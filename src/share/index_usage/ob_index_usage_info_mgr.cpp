/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#include "ob_index_usage_info_mgr.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_tenant_schema_service.h"

#define USING_LOG_PREFIX SERVER
using namespace oceanbase::common;

namespace oceanbase {
namespace share {

const char *OB_INDEX_USAGE_MANAGER = "IndexUsageMgr";

void ObIndexUsageOp::operator()(common::hash::HashMapPair<ObIndexUsageKey, ObIndexUsageInfo> &data) {
  if (op_mode_ == ObIndexUsageOpMode::UPDATE) {
    data.second.ref_count_++;
    data.second.exec_count_++;
    data.second.last_used_time_ = ObTimeUtility::current_time();

  } else if (op_mode_ == ObIndexUsageOpMode::RESET) {
    old_info_ = data.second;
    data.second.reset();
  }
}

ObIndexUsageInfoMgr::ObIndexUsageInfoMgr()
    : is_inited_(false), tenant_id_(MTL_ID()), index_usage_map_(), report_task_(), allocator_(MTL_ID()) {}

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
  } else if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(), OB_MALLOC_NORMAL_BLOCK_SIZE, attr))) {
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
      if (TG_TASK_EXIST(MTL(omt::ObSharedTimer *)->get_tg_id(), report_task_, is_exist) == OB_SUCCESS && is_exist) {
        TG_CANCEL_TASK(MTL(omt::ObSharedTimer *)->get_tg_id(), report_task_);
        TG_WAIT_TASK(MTL(omt::ObSharedTimer *)->get_tg_id(), report_task_);
        report_task_.is_inited_ = false;
      }
    }
    // destroy hashmap
    index_usage_map_.destroy();
  }
}

int ObIndexUsageInfoMgr::start() {
  int ret = OB_SUCCESS;
  if (is_inited_) {
    if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer *)->get_tg_id(), report_task_, INDEX_USAGE_REPORT_INTERVAL, true))) {
      LOG_WARN("failed to schedule index usage report task", K(ret));
    } else {
      report_task_.is_inited_ = true;
    }
  }
  return ret;
}

void ObIndexUsageInfoMgr::stop() {
  if (OB_LIKELY(report_task_.is_inited_)) {
    TG_CANCEL_TASK(MTL(omt::ObSharedTimer *)->get_tg_id(), report_task_);
  }
}

void ObIndexUsageInfoMgr::wait() {
  if (OB_LIKELY(report_task_.is_inited_)) {
    TG_WAIT_TASK(MTL(omt::ObSharedTimer *)->get_tg_id(), report_task_);
  }
}

int ObIndexUsageInfoMgr::update(const uint64_t tenant_id, const uint64_t table_id, const uint64_t index_table_id) {
  int ret = OB_SUCCESS;
  if (!is_inited_ || !is_enabled()) {
    // do nothing
  } else {
    ObIndexUsageKey key(tenant_id, table_id, index_table_id);
    ObIndexUsageOp update_op(ObIndexUsageOpMode::UPDATE);
    if (tenant_id == OB_INVALID_TENANT_ID || table_id == OB_INVALID_ID || index_table_id == OB_INVALID_ID) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("usage of invalid ids", K(ret));
    } else if (is_inner_object_id(table_id)) {
      // do nothing
    } else if (OB_SUCC(index_usage_map_.atomic_refactored(key, update_op))) {
      // key exists, update success
    } else if (OB_LIKELY(ret == OB_HASH_NOT_EXIST)) {
      // key not exist, insert new one
      ObIndexUsageInfo new_info(index_table_id);
      new_info.exec_count_ = 1;
      if (get_iut_entries() <= index_usage_map_.size()) {
        LOG_WARN("index usage hashmap reaches max entries", K(ret));
      } else if (OB_FAIL(index_usage_map_.set_or_update(key, new_info, update_op))) {
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
int ObIndexUsageInfoMgr::sample(const UpdateFunc &update_func, const DelFunc &del_func) {
  int ret = OB_SUCCESS;

  if (is_inited_ && is_enabled()) {
    int64_t map_size = index_usage_map_.size();
    int64_t sample_count = map_size;
    if (is_sample_mode()) {
      sample_count = sample_count * (SAMPLE_RATIO * 1.0 / 100);
    }
    if (sample_count < SAMPLE_BATCH_SIZE) {
      sample_count = SAMPLE_BATCH_SIZE;
    }
    ObIndexUsageOp reset_op(ObIndexUsageOpMode::RESET);
    ObIndexUsagePairList pair_list(allocator_);
    int64_t index = 0;

    for (ObIndexUsageHashMap::iterator it = index_usage_map_.begin(); OB_SUCC(ret) && index < map_size; it++, index++) {
      bool exist = true;
      if (OB_SUCC(check_table_exists(it->first.tenant_id_, it->first.index_table_id_, exist)) && !exist) {
        // delete index not exist
        if (OB_FAIL(del_func(it->first)) || OB_FAIL(del(it->first))) {
          LOG_WARN("del index usage failed", K(ret), "index_id", it->first.index_table_id_);
        }
      } else if (sample_count < map_size - index && common::ObRandom::rand(0, map_size - index) > sample_count) {
        // sample skip
      } else {
        // retrive info and reset info atomicly
        index_usage_map_.atomic_refactored(it->first, reset_op);
        ObIndexUsagePair pair;
        pair.init(it->first, reset_op.retrive_info());
        pair_list.push_back(pair);
        sample_count--;
      }
      // clear batch at last
      if (pair_list.size() >= SAMPLE_BATCH_SIZE || index >= map_size - 1) {
        if (OB_FAIL(update_func(pair_list))) {
          LOG_WARN("flush index usage batch failed", K(ret));
        }
        pair_list.reset();
      }
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

int ObIndexUsageInfoMgr::check_table_exists(uint64_t tenant_id, uint64_t table_id, bool &exist) {
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema_guard", K(ret));
  } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id, table_id, exist))) {
    LOG_WARN("failed to check table exist", K(ret));
  }
  return ret;
}

bool ObIndexUsageInfoMgr::is_enabled() {
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  return tenant_config.is_valid() && tenant_config->_iut_enable;
}

bool ObIndexUsageInfoMgr::is_sample_mode() {
  bool mode = true;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (OB_UNLIKELY(!tenant_config.is_valid())) {
    int ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get tenant config, use default", K(ret));
  } else {
    mode = tenant_config->_iut_stat_collection_type.get_value_string().case_compare("SAMPLE") == 0;
  }
  return mode;
}

int64_t ObIndexUsageInfoMgr::get_iut_entries() {
  int64_t entries = 30000;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (OB_UNLIKELY(!tenant_config.is_valid())) {
    int ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get tenant config, use default", K(ret));
  } else {
    entries = tenant_config->_iut_max_entries.get();
  }
  return entries;
}

} // namespace share
} // namespace oceanbase