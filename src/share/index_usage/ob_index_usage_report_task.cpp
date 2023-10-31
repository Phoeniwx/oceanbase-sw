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
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/rc/ob_tenant_base.h"

#define USING_LOG_PREFIX SERVER
namespace oceanbase {
using namespace common;
namespace share {
const char *OB_INDEX_USAGE_REPORT_TASK = "IndexUsageReportTask";
#define INSERT_INDEX_USAGE_HEAD_SQL                                                                                    \
  "INSERT INTO %s"                                                                                                     \
  "(tenant_id,table_id,object_id,name,owner,total_"                                                                    \
  "access_count,total_rows_returned,"                                                                                  \
  "total_exec_count,start_used,last_used,last_flush_time) VALUES"
#define INSERT_INDEX_USAGE_ON_DUPLICATE_END_SQL                                                                        \
  " ON DUPLICATE KEY UPDATE "                                                                                          \
  "total_access_count=total_access_count+VALUES(total_access_count),"                                                    \
  "total_rows_returned=total_rows_returned+VALUES(total_rows_returned),"                                               \
  "total_exec_count=total_exec_count+VALUES(total_exec_count),"                                                        \
  "start_used=VALUES(start_used),last_used=VALUES(last_used),"                                                         \
  "last_flush_time=VALUES(last_flush_time)"
ObIndexUsageReportTask::ObIndexUsageReportTask() : is_inited_(false), sql_proxy_(nullptr) {}

void ObIndexUsageReportTask::runTimerTask() {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index usage mgr not init", K(ret), K(MTL_ID()));
  } else {
    ObIndexUsageInfoMgr::UpdateFunc update_func;
    ObIndexUsageInfoMgr::DelFunc del_func;
    update_func.assign([this](ObIndexUsagePairList &info_list) { return this->storage_index_usage(info_list); });
    del_func.assign([this](ObIndexUsageKey &key) { return this->del_index_usage(key); });
    if (OB_FAIL(mgr_->sample(update_func, del_func))) {
      LOG_WARN("index usage sample failed", K(ret), K(MTL_ID()));
    } else if (OB_FAIL(mgr_->check_disable(update_func, del_func))) {
      LOG_WARN("index usage check disable failed", K(ret), K(MTL_ID()));
    }
  }
}

int ObIndexUsageReportTask::storage_index_usage(const ObIndexUsagePairList &info_list) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", K(ret));
  } else if (!info_list.empty()) {
    ObSqlString insert_update_sql;
    insert_update_sql.append_fmt(INSERT_INDEX_USAGE_HEAD_SQL, OB_ALL_INDEX_USAGE_INFO_TNAME);
    for (ObIndexUsagePairList::const_iterator it = info_list.begin(); it != info_list.end(); it++) {
      insert_update_sql.append_fmt("(%lu,%lu,%lu,'','',%lu,%lu,%lu,usec_to_time(%lu),usec_to_time(%lu),now(6)),",
                                   it->first.tenant_id_, it->first.table_id_, it->first.index_table_id_,
                                   it->second.access_count_, it->second.rows_returned_, it->second.exec_count_,
                                   it->second.start_used_time_, it->second.last_used_time_);
    }
    insert_update_sql.set_length(insert_update_sql.length() - 1);
    insert_update_sql.append(INSERT_INDEX_USAGE_ON_DUPLICATE_END_SQL);
    int64_t affected_rows;
    if (OB_FAIL(sql_proxy_->write(MTL_ID(), insert_update_sql.ptr(), affected_rows))) {
      LOG_WARN("insert update sql error", K(ret), K(insert_update_sql));
    }
  }
  return ret;
}

int ObIndexUsageReportTask::del_index_usage(const ObIndexUsageKey &key) {
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObDMLExecHelper exec(*sql_proxy_, MTL_ID());
  int64_t affected_rows = 0;

  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", K(ret));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", key.tenant_id_)) ||
             OB_FAIL(dml.add_pk_column("table_id", key.table_id_)) ||
             OB_FAIL(dml.add_pk_column("object_id", key.index_table_id_))) {
    LOG_WARN("dml add column failed", K(ret));
  } else if (OB_FAIL(exec.exec_delete(OB_ALL_INDEX_USAGE_INFO_TNAME, dml, affected_rows))) {
    LOG_WARN("del sql exec error", K(ret), K(key));
  }
  return ret;
}
} // namespace share
} // namespace oceanbase

#undef INSERT_INDEX_USAGE_HEAD_SQL
#undef INSERT_INDEX_USAGE_ON_DUPLICATE_END_SQL
