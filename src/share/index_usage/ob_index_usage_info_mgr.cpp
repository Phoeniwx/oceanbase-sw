#include "ob_index_usage_info_mgr.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_page_manager.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_errno.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/omt/ob_tenant_config_mgr.h" // ObTenantConfigGuard
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <algorithm>

#define USING_LOG_PREFIX SERVER
using namespace oceanbase::common;

namespace oceanbase {
namespace share {

const char *OB_INDEX_USAGE_MANAGER = "hIndexUsageMgr";

void ObIndexUsageOp::operator() (common::hash::HashMapPair<ObIndexUsageKey*, ObIndexUsageInfo*> &data) {
  if (op_mode_==ObIndexUsageOpMode::UPDATE) {
    ret_code_ = OB_SUCCESS;
    data.second->ref_count++;
    data.second->last_used_time = time(NULL);
    
  } else if (op_mode_==ObIndexUsageOpMode::RESET) {
    ret_code_ = OB_SUCCESS;
    if (OB_ISNULL(old_info_)) {
      ret_code_ = OB_NULL_CHECK_ERROR;
    } else {
      old_info_->data_table_id = data.second->data_table_id;
      old_info_->ref_count = data.second->ref_count;
      old_info_->last_used_time = data.second->last_used_time;
      data.second->ref_count = 0;
    }  
  }
  is_called_ = true;  
}

ObIndexUsageInfoMgr::ObIndexUsageInfoMgr()
    : is_inited_(false),
      is_destroying_(false),
      index_usage_map_(),
      init_lock_(),
      destory_lock_(),
      allocator_(MTL_ID()) 
{}

ObIndexUsageInfoMgr::~ObIndexUsageInfoMgr() {}

int ObIndexUsageInfoMgr::mtl_init(ObIndexUsageInfoMgr *&index_usage_mgr) {
  int ret = OB_SUCCESS;
  index_usage_mgr = OB_NEW(ObIndexUsageInfoMgr, ObMemAttr(MTL_ID(), OB_INDEX_USAGE_MANAGER));
  if (OB_ISNULL(index_usage_mgr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for ObIndexUsageInfoMgr", K(ret));
  } else if (OB_FAIL(index_usage_mgr->init())) {
    LOG_WARN("ObIndexUsageInfoMgr init failed", K(ret));
  }
  return ret;
}

void ObIndexUsageInfoMgr::mtl_destroy(ObIndexUsageInfoMgr *&index_usage_mgr) {
  if (nullptr != index_usage_mgr) {
    LOG_INFO("mtl destory ObIndexUsageInfoMgr", K(MTL_ID()));
    index_usage_mgr->destory();
    common::ob_delete(index_usage_mgr);
    index_usage_mgr = nullptr;
  }
  
}

int ObIndexUsageInfoMgr::init() {
  int ret = OB_SUCCESS;
  const ObMemAttr attr(MTL_ID(), OB_INDEX_USAGE_MANAGER);
  SpinWLockGuard guard(init_lock_);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(index_usage_map_.create(DEFAULT_MAX_HASH_BUCKET_CNT, attr))) {
    LOG_WARN("create hash map failed", K(ret));
  } else if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(), OB_MALLOC_NORMAL_BLOCK_SIZE, attr))) {
    LOG_WARN("init allocator failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObIndexUsageInfoMgr::destory() {
  {
    SpinWLockGuard guard(destory_lock_);
    is_destroying_ = true;
  }
  for (ObIndexUsageHashMap::iterator it=index_usage_map_.begin(); it != index_usage_map_.end(); it++) {
    allocator_.free(it->first);
    allocator_.free(it->second);
  }
  index_usage_map_.destroy();
}

int ObIndexUsageInfoMgr::update(const int64_t database_id, const int64_t tenant_id, const int64_t index_table_id) {
  int ret = OB_SUCCESS;
  if (!GCONF._iut_enable) {
    return ret;
  } else if (GCONF._iut_max_entries <= index_usage_map_.size()) {
    ret = OB_ERROR;
    LOG_WARN("index usage hashmap reach max entries", K(ret));
    return ret;
  } else if (is_destroying()) {
    ret = OB_ERROR;
    LOG_WARN("index usage mgr is destroying", K(ret));
    return ret;
  }
  ObIndexUsageKey temp_key(database_id, tenant_id, index_table_id);
  ObIndexUsageOp update_op(ObIndexUsageOpMode::UPDATE);
  ObIndexUsageKey* new_key = nullptr;
  ObIndexUsageInfo* new_info = nullptr;
  SpinRLockGuard guard(destory_lock_);
  // suppose key exists, update it directory
  if (OB_FAIL(index_usage_map_.atomic_refactored(&temp_key, update_op))) {
    // key not exist, insert one
    if (ret == OB_HASH_NOT_EXIST) {
      if (OB_FAIL(alloc_new_record(temp_key, new_key, new_info))) {
        LOG_WARN("alloc memory for new index-usage record failed", K(ret));
      } else if (OB_FAIL(index_usage_map_.set_or_update(new_key, new_info, update_op))) {
        LOG_WARN("failed to set or update index-usage", K(ret));
      } 
      // free new key if update is called
      if (update_op.is_called() && OB_SUCCESS==update_op.get_ret()) {
        allocator_.free(new_key);
        allocator_.free(new_info);
      }
    } else {
      LOG_WARN("failed to update index-usage key", K(ret));
    }
  }
  return ret;
}

// sample ObIndexUsageInfo to pair_list, you need to free ObIndexUsageInfo* in pair_list later
int ObIndexUsageInfoMgr::sample(common::ObList<ObIndexUsagePair>& pair_list) {
  int ret = OB_SUCCESS;
  const char* iut_mode = GCONF._iut_stat_collection_type;
  int64_t map_size = index_usage_map_.size();
  int64_t sample_count = index_usage_map_.size();
  if (strcmp(iut_mode, "SAMPLE") == 0) {
    sample_count = sample_count * (SAMPLE_RATIO/100);
  }
  std::srand(static_cast<uint32_t>(time(NULL)));
  ObIndexUsageOp reset_op(ObIndexUsageOpMode::RESET);
  int64_t index = 0;
  for (ObIndexUsageHashMap::iterator it=index_usage_map_.begin(); it != index_usage_map_.end(); it++, index++) {
    if (sample_count < map_size && rand()%(map_size-index)>=sample_count) {
      continue;
    }
    sample_count--;
    void* buf = allocator_.alloc(sizeof(ObIndexUsageInfo));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for old info", K(ret));
      break;
    }
    ObIndexUsageInfo* old_info = new(buf) ObIndexUsageInfo();
    reset_op.set_old_info(old_info);
    index_usage_map_.atomic_refactored(it->first, reset_op);
    ObIndexUsagePair pair;
    pair.init(it->first, old_info);
    pair_list.push_back(pair);
  }
  return ret;
}

int ObIndexUsageInfoMgr::del(ObIndexUsageKey* key) {
  int ret = OB_SUCCESS;
  if (is_destroying()) {
    int ret = OB_ERROR;
    LOG_WARN("index usage mgr is destroying", K(ret));
    return ret;
  }
  SpinRLockGuard guard(destory_lock_);
  ObIndexUsageInfo *const * info_ptr_ptr = index_usage_map_.get(const_cast<ObIndexUsageKey *const>(key));
  if (OB_ISNULL(info_ptr_ptr)) {
    LOG_WARN("index usage key not exists");
  } else {
    if (OB_FAIL(index_usage_map_.erase_refactored(const_cast<ObIndexUsageKey *const>(key)))) {
      LOG_WARN("failed to erase index usage key", K(ret));
    } else {
      if (OB_NOT_NULL(*info_ptr_ptr)) {
        allocator_.free(*info_ptr_ptr);
        allocator_.free(key);
      }  
    }
  }
  return ret;
}

int ObIndexUsageInfoMgr::alloc_new_record(const ObIndexUsageKey& temp_key, ObIndexUsageKey* key, ObIndexUsageInfo* info){
  int ret = OB_SUCCESS;
  void* buf_key = allocator_.alloc(sizeof(ObIndexUsageKey));
  void* buf_info = allocator_.alloc(sizeof(ObIndexUsageInfo));
  if (OB_ISNULL(buf_key) || OB_ISNULL(buf_info)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc index-usage memory", K(ret));
  } else {
    key = new(buf_key) ObIndexUsageKey(temp_key.database_id, temp_key.tenant_id, temp_key.index_table_id);
    info = new(buf_key) ObIndexUsageInfo(temp_key.index_table_id, 0, time(NULL));
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(buf_key)){
      allocator_.free(buf_key);
    }
    if (OB_NOT_NULL(buf_info)){
      allocator_.free(buf_info);
    }
  }
  return ret;
}

void ObIndexUsageInfoMgr::release_node(ObIndexUsageInfo *info) {
  if (OB_NOT_NULL(info)) {
    allocator_.free(info);
    info = nullptr;
  }
}

bool ObIndexUsageInfoMgr::is_destroying() {
  SpinRLockGuard guard(destory_lock_);
  return is_destroying_;
}

}
}