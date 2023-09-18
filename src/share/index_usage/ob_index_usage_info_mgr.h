
#ifndef SRC_SHARE_INDEX_USAGE_OB_INDEX_USAGE_INFO_MGR_H_
#define SRC_SHARE_INDEX_USAGE_OB_INDEX_USAGE_INFO_MGR_H_

#include <cstdint>
#include <ctime>
#include <mutex>
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_cascad_member.h"
#include "lib/list/ob_list.h"

namespace oceanbase {
namespace share {

enum ObIndexUsageOpMode
{
  UPDATE, // for update haspmap
  RESET // for reset hashmap
};

struct ObIndexUsageKey {
  ObIndexUsageKey(int64_t database_id, int64_t tenant_id, int64_t index_table_id)
      : database_id(database_id), tenant_id(tenant_id), index_table_id(index_table_id){}
  ~ObIndexUsageKey(){}

  uint64_t hash() const
  {
    uint64_t hash_value = common::murmurhash(&database_id, 3*sizeof(int64_t), 0);
    return hash_value;
  }
  inline int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  bool operator==(const ObIndexUsageKey &other) const {
    return database_id == other.database_id && tenant_id == other.tenant_id && index_table_id == other.index_table_id;
  }

  int64_t database_id;
  int64_t tenant_id;
  int64_t index_table_id;
};

struct ObIndexUsageInfo {
  ObIndexUsageInfo(int64_t data_table_id, int64_t ref_count, time_t last_used_time)
      : data_table_id(data_table_id), ref_count(ref_count), last_used_time(last_used_time){}
  ObIndexUsageInfo(){}
  ~ObIndexUsageInfo(){}
  int64_t data_table_id;
  int64_t ref_count;
  time_t last_used_time;
};

// callback for update or reset map value
class ObIndexUsageOp {
public:
  explicit ObIndexUsageOp(ObIndexUsageOpMode mode, ObIndexUsageInfo* old_info=nullptr) 
    : op_mode_(mode), is_called_(false), ret_code_(common::OB_SUCCESS), old_info_(old_info) {}
  virtual ~ObIndexUsageOp(){}
  void operator() (common::hash::HashMapPair<ObIndexUsageKey*, ObIndexUsageInfo*> &data);
  void set_old_info(ObIndexUsageInfo* old_info) {old_info_ = old_info;}
  bool is_called() {return is_called_;}
  int get_ret() {return ret_code_;}
private:
  ObIndexUsageOpMode op_mode_;
  bool is_called_;
  int ret_code_;
  ObIndexUsageInfo* old_info_;
  DISALLOW_COPY_AND_ASSIGN(ObIndexUsageOp);
};

class ObIndexUsageInfoMgr final
{
typedef common::hash::ObHashMap<ObIndexUsageKey*, ObIndexUsageInfo*> ObIndexUsageHashMap;
typedef common::hash::HashMapPair<const ObIndexUsageKey*, ObIndexUsageInfo*> ObIndexUsagePair;
static const int64_t SAMPLE_RATIO = 50;	// 采样模式下的采样比例，50 表示 50%
static const int64_t DEFAULT_MAX_HASH_BUCKET_CNT = 10000;

public:
  static int mtl_init(ObIndexUsageInfoMgr *&index_usage_mgr);
  static void mtl_destroy(ObIndexUsageInfoMgr *&index_usage_mgr);
  ObIndexUsageInfoMgr();
  ~ObIndexUsageInfoMgr();

public:
  int init(); 	// 申请map内存，在创建索引之后调用
  void destory();	// 释放map，在observer析构时调用
  int update(const int64_t database_id, const int64_t tenant_id, const int64_t index_table_id);
  int get(const ObIndexUsageKey &key, const ObIndexUsageInfo &value); // get之后增量数据重置
  int del(ObIndexUsageKey* key);
  int sample(common::ObList<ObIndexUsagePair>& pair_list);
  int alloc_new_record(const ObIndexUsageKey& temp_key, ObIndexUsageKey* key, ObIndexUsageInfo* info);
private:
  bool is_inited_;
  ObIndexUsageHashMap index_usage_map_;
  common::SpinRWLock spin_lock_;
  common::ObFIFOAllocator allocator_;
};

}
}
#endif