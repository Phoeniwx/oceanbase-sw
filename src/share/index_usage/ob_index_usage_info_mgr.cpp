#include "ob_index_usage_info_mgr.h"
#include "lib/thread/thread_mgr.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_page_manager.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/random/ob_random.h"
#include "lib/utility/ob_macro_utils.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h" // ObTenantConfigGuard
#include "share/ob_errno.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "observer/omt/ob_multi_tenant.h"

#define USING_LOG_PREFIX SERVER
using namespace oceanbase::common;

namespace oceanbase {
namespace share {

const char *OB_INDEX_USAGE_MANAGER = "hIndexUsageMgr";

void ObIndexUsageOp::operator()(
    common::hash::HashMapPair<ObIndexUsageKey, ObIndexUsageInfo> &data) {
  if (op_mode_ == ObIndexUsageOpMode::UPDATE) {
    data.second.ref_count++;
    data.second.exec_count++;
    data.second.last_used_time = ObTimeUtility::current_time();

  } else if (op_mode_ == ObIndexUsageOpMode::RESET) {
    old_info_ = data.second;
    data.second.reset();
  }
}

ObIndexUsageInfoMgr::ObIndexUsageInfoMgr()
    : is_inited_(false), index_usage_map_(), report_task_(), allocator_(MTL_ID()) {}

ObIndexUsageInfoMgr::~ObIndexUsageInfoMgr() {}

int ObIndexUsageInfoMgr::mtl_init(ObIndexUsageInfoMgr *&index_usage_mgr) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(index_usage_mgr->init())) {
    LOG_WARN("ObIndexUsageInfoMgr init failed", K(ret));
  }
  return ret;
}

int ObIndexUsageInfoMgr::init() {
  int ret = OB_SUCCESS;
  const ObMemAttr attr(MTL_ID(), OB_INDEX_USAGE_MANAGER);
  if (!is_user_tenant(MTL_ID())) {
    // only init for user tenant
  } else if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(index_usage_map_.create(DEFAULT_MAX_HASH_BUCKET_CNT, attr))) {
    LOG_WARN("create hash map failed", K(ret));
  } else if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(),
                                     OB_MALLOC_NORMAL_BLOCK_SIZE, attr))) {
    LOG_WARN("init allocator failed", K(ret));
  } else {
    report_task_.sql_proxy_ = GCTX.sql_proxy_;
    report_task_.mgr_ = this;
    is_inited_ = true;
  }
  return ret;
}

void ObIndexUsageInfoMgr::destroy() {
  if (is_inited_) {
    // cancel report task
    if (report_task_.is_inited_) {
      bool is_exist = true;
      if (TG_TASK_EXIST(MTL(omt::ObSharedTimer*)->get_tg_id(), report_task_, is_exist)==OB_SUCCESS && is_exist) {
        TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), report_task_);
        TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), report_task_);
        report_task_.is_inited_ = false;
      }
    }
    // destroy hashmap
    index_usage_map_.destroy();
  }
}

int ObIndexUsageInfoMgr::start(){
  int ret = OB_SUCCESS;
  if (is_inited_) {
    if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(),
                                 report_task_, 
                                 INDEX_USAGE_REPORT_INTERVAL,
                                 true))) {
      LOG_WARN("failed to schedule index usage report task", K(ret));
    } else {
      report_task_.is_inited_ = true;
    }
  }
  return ret;
}

void ObIndexUsageInfoMgr::stop(){
  if (OB_LIKELY(report_task_.is_inited_)) {
    TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), report_task_);
  }
}

void ObIndexUsageInfoMgr::wait(){
  if (OB_LIKELY(report_task_.is_inited_)) {
    TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), report_task_);
  }
}

int ObIndexUsageInfoMgr::update(const uint64_t tenant_id,
                                const uint64_t table_id,
                                const uint64_t index_table_id) {
  int ret = OB_SUCCESS;
  if (!GCONF._iut_enable || !is_inited_) {
    // do nothing
  } else if (GCONF._iut_max_entries <= index_usage_map_.size()) {
    ret = OB_ERROR;
    LOG_WARN("index usage hashmap reach max entries", K(ret));
  } else {
    ObIndexUsageKey key(tenant_id, table_id, index_table_id);
    ObIndexUsageOp update_op(ObIndexUsageOpMode::UPDATE);
    if (OB_SUCC(index_usage_map_.atomic_refactored(key, update_op))) {
      // key exists, update success
    } else if (OB_LIKELY(ret == OB_HASH_NOT_EXIST)) {
      // key not exist, insert new one
      ObIndexUsageInfo new_info(index_table_id);
      if (OB_FAIL(index_usage_map_.set_or_update(key, new_info, update_op))) {
        LOG_WARN("failed to set or update index-usage map", K(ret));
      }
    } else {
      LOG_WARN("failed to update index-usage map", K(ret));
    }
  }
  return ret;
}

/*
1. sample ObIndexUsageInfo and call @update_func to process them by batch size SAMPLE_BATCH_SIZE
2. check deleted index and call @del_func to del them
*/
int ObIndexUsageInfoMgr::sample(const UpdateFunc& update_func, const DelFunc& del_func) {
  int ret = OB_SUCCESS;

  if (is_inited_) {
    schema::ObMultiVersionSchemaService* schema_service = MTL(ObTenantSchemaService*)->get_schema_service();
    const char *iut_mode = GCONF._iut_stat_collection_type;
    int64_t map_size = index_usage_map_.size();
    int64_t sample_count = map_size;
    if (STRCASECMP(iut_mode, "SAMPLE") == 0) {
      sample_count = sample_count * (SAMPLE_RATIO * 1.0 / 100);
    }
    ObIndexUsageOp reset_op(ObIndexUsageOpMode::RESET);

    ObIndexUsagePairList pair_list(allocator_);
    int64_t index = 0;
    for (ObIndexUsageHashMap::iterator it = index_usage_map_.begin();
        it != index_usage_map_.end(); it++, index++) {
      if (pair_list.size()>=SAMPLE_BATCH_SIZE) {
        // process a batch
        if (OB_FAIL(update_func(pair_list))) {
          LOG_WARN("flush index usage batch failed", K(ret));
        }
        pair_list.reset();
      }
      bool exist = true;
      if (OB_SUCC(schema_service->check_table_exist(it->first.tenant_id, it->first.table_id, 0, exist)) && !exist) {
        // delete index not exist
        if (OB_FAIL(del_func(it->first)) || OB_FAIL(del(it->first))) {
          LOG_WARN("del index usage failed", K(ret), "index_id", it->first.index_table_id);
          break;
        } 
        continue;
      }
      if (sample_count < map_size &&
          common::ObRandom::rand(0, map_size - index) <= sample_count) {
        continue;
      }
      // retrive info and reset info atomicly
      index_usage_map_.atomic_refactored(it->first, reset_op);
      ObIndexUsagePair pair;
      pair.init(it->first, reset_op.retrive_info());
      pair_list.push_back(pair);
      sample_count--;
    }
    //process last batch
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(update_func(pair_list))) {
      LOG_WARN("flush index usage batch failed", K(ret));
    }
    pair_list.destroy();
  }
  
  return ret;
}

int ObIndexUsageInfoMgr::del(ObIndexUsageKey &key) {
  int ret = OB_SUCCESS;
  if (is_inited_ && OB_FAIL(index_usage_map_.erase_refactored(key))) {
    LOG_WARN("failed to erase index usage key", K(ret));
  }
  return ret;
}

} // namespace share
} // namespace oceanbase