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
#ifndef OCEANBASE_SHARE_OB_INDEX_USAGE_INFO_MGR_H_
#define OCEANBASE_SHARE_OB_INDEX_USAGE_INFO_MGR_H_

#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/function/ob_function.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/list/ob_list.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/task/ob_timer.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace share
{

class ObIndexUsageInfoMgr;

enum ObIndexUsageOpMode {
  UPDATE, // for update haspmap
  RESET   // for reset hashmap
};

struct ObIndexUsageKey {
  ObIndexUsageKey(uint64_t tenant_id, uint64_t table_id, uint64_t index_table_id)
      : tenant_id(tenant_id), table_id(table_id), index_table_id(index_table_id)
  {
  }

  ObIndexUsageKey() {}
  ~ObIndexUsageKey() {}

  uint64_t hash() const
  {
    uint64_t hash_value = 0;
    hash_value = common::murmurhash(&tenant_id, sizeof(uint64_t), hash_value);
    hash_value = common::murmurhash(&table_id, sizeof(uint64_t), hash_value);
    hash_value = common::murmurhash(&index_table_id, sizeof(uint64_t), hash_value);
    return hash_value;
  }
  inline int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  bool operator==(const ObIndexUsageKey &other) const
  {
    return tenant_id == other.tenant_id && table_id == other.table_id && index_table_id == other.index_table_id;
  }

  int64_t tenant_id;
  int64_t table_id; // main table id
  int64_t index_table_id;
};

/* strcut stores increment stasitic data*/
struct ObIndexUsageInfo {
  ObIndexUsageInfo(uint64_t index_table_id)
      : index_table_id(index_table_id), ref_count(0), access_count(0), exec_count(0), rows_returned(0),
        start_used_time(ObTimeUtility::current_time()), last_used_time(start_used_time)
  {
  }
  ObIndexUsageInfo() {}
  ~ObIndexUsageInfo() {}

  void reset()
  {
    ref_count = 0;
    access_count = 0;
    exec_count = 0;
    rows_returned = 0;
  }

  uint64_t index_table_id;
  int64_t ref_count;
  int64_t access_count;
  int64_t exec_count;
  int64_t rows_returned;
  int64_t start_used_time;
  int64_t last_used_time;
};

typedef common::hash::HashMapPair<ObIndexUsageKey, ObIndexUsageInfo> ObIndexUsagePair;
typedef common::ObList<ObIndexUsagePair, common::ObFIFOAllocator> ObIndexUsagePairList;

class ObIndexUsageReportTask : public common::ObTimerTask {
  friend ObIndexUsageInfoMgr;

public:
  static const int64_t INDEX_USAGE_REPORT_INTERVAL = 15 * 60 * 1000L * 1000L; // 15min
public:
  ObIndexUsageReportTask();
  virtual ~ObIndexUsageReportTask(){};

private:
  virtual void runTimerTask();
  int storage_index_usage(const ObIndexUsagePairList &info_list);
  int del_index_usage(const ObIndexUsageKey &key);

private:
  bool is_inited_;
  ObIndexUsageInfoMgr *mgr_;
  common::ObMySQLProxy *sql_proxy_; // 写入内部表需要 sql proxy
};

// callback for update or reset map value
class ObIndexUsageOp final {
public:
  explicit ObIndexUsageOp(ObIndexUsageOpMode mode) : op_mode_(mode), old_info_() {}
  virtual ~ObIndexUsageOp() {}
  void operator()(common::hash::HashMapPair<ObIndexUsageKey, ObIndexUsageInfo> &data);
  const ObIndexUsageInfo &retrive_info() { return old_info_; }

private:
  ObIndexUsageOpMode op_mode_;
  ObIndexUsageInfo old_info_;
  DISALLOW_COPY_AND_ASSIGN(ObIndexUsageOp);
};

class ObIndexUsageInfoMgr final {

  typedef common::hash::ObHashMap<ObIndexUsageKey, ObIndexUsageInfo, common::hash::ReadWriteDefendMode>
      ObIndexUsageHashMap;
  static const int64_t SAMPLE_RATIO = 50; // 采样模式下的采样比例，50 表示 50%
  static const int64_t DEFAULT_MAX_HASH_BUCKET_CNT = 3000;
  static const int32_t SAMPLE_BATCH_SIZE = 100;
  static const int64_t INDEX_USAGE_REPORT_INTERVAL = 15 * 60 * 1000L * 1000L; // 15min

public:
  typedef common::ObFunction<int(ObIndexUsagePairList &)> UpdateFunc;
  typedef common::ObFunction<int(ObIndexUsageKey &)> DelFunc;

  static int mtl_init(ObIndexUsageInfoMgr *&index_usage_mgr);
  ObIndexUsageInfoMgr();
  ~ObIndexUsageInfoMgr();

public:
  int start(); // start timer task
  void stop();
  void wait();
  int init();     // 申请map内存，在创建索引之后调用
  void destroy(); // 释放map，在observer析构时调用
  int update(const uint64_t tenant_id, const uint64_t table_id, const uint64_t index_table_id);
  int del(ObIndexUsageKey &key);
  int sample(const UpdateFunc &update_func, const DelFunc &del_func); // 采样哈希表
  void release_node(ObIndexUsageInfo *info);                          // 用于task释放内存

private:
  bool is_inited_;
  ObIndexUsageHashMap index_usage_map_;
  ObIndexUsageReportTask report_task_;
  common::ObFIFOAllocator allocator_;
};

} // namespace share
} // namespace oceanbase
#endif