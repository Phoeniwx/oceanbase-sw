#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "ob_index_usage_info_mgr.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/rc/ob_tenant_base.h"

#define USING_LOG_PREFIX SERVER
namespace oceanbase {
using namespace common;
namespace share {
const char *OB_INDEX_USAGE_REPORT_TASK = "IndexUsageReportTask";
#define INSERT_INDEX_USAGE_HEAD_SQL                                            \
  "INSERT INTO %s"                                                               \
  "(tenant_id,table_id,object_id,name,owner,total_"                            \
  "access_count,total_rows_returned,"                                          \
  "total_exec_count,start_used,last_used,last_flush_time) VALUES"
#define INSERT_INDEX_USAGE_ON_DUPLICATE_END_SQL                                \
  " ON DUPLICATE KEY UPDATE "                                                  \
  "total_access_count=total_access_count+VALUES(total_exec_count),"            \
  "total_rows_returned=total_rows_returned+VALUES(total_rows_returned),"       \
  "total_exec_count=total_exec_count+VALUES(total_exec_count),"                \
  "start_used=VALUES(start_used),last_used=VALUES(last_used),"                 \
  "last_flush_time=VALUES(last_flush_time)"
ObIndexUsageReportTask::ObIndexUsageReportTask()
    : is_inited_(false), sql_proxy_(nullptr) {}

void ObIndexUsageReportTask::runTimerTask() {
  int ret = OB_SUCCESS;

  ObIndexUsageInfoMgr::UpdateFunc update_func;
  ObIndexUsageInfoMgr::DelFunc del_func;
  update_func.assign([this](ObIndexUsagePairList& info_list){
    return this->storage_index_usage(info_list);
  });
  del_func.assign([this](ObIndexUsageKey& key){
    return this->del_index_usage(key);
  });
  if (OB_FAIL(mgr_->sample(update_func, del_func))) {
    LOG_WARN("index usage sample failed", K(ret), K(MTL_ID()));
  }
  
  // common::ObArray<uint64_t> tenant_ids;
  // omt::ObMultiTenant *omt = GCTX.omt_;
  // if (OB_ISNULL(omt)) {
  //   ret = OB_ERR_UNEXPECTED;
  //   STORAGE_LOG(WARN, "unexpected error, omt is nullptr", K(ret));
  // } else if (OB_FAIL(omt->get_mtl_tenant_ids(tenant_ids))) {
  //   STORAGE_LOG(WARN, "fail to get_mtl_tenant_ids", K(ret));
  // }

  // for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.size(); i++) {
  //   const uint64_t &tenant_id = tenant_ids.at(i);
  //   MTL_SWITCH(tenant_id) {
  //     if (OB_FAIL(storage_index_usage(tenant_id))) {
  //       STORAGE_LOG(WARN, "failed to count tenant's slog", K(ret));
  //     }
  //   }
  //   else {
  //     if (OB_TENANT_NOT_IN_SERVER == ret) {
  //       ret = OB_SUCCESS;
  //       STORAGE_LOG(INFO, "tenant is stopped, ignore", K(tenant_id));
  //     } else {
  //       STORAGE_LOG(WARN, "fail to switch tenant", K(ret), K(tenant_id));
  //     }
  //   }
  // }
}

int ObIndexUsageReportTask::storage_index_usage(const ObIndexUsagePairList& info_list) {
  int ret = OB_SUCCESS;
  if (!info_list.empty()) {
    ObSqlString insert_update_sql;
    insert_update_sql.append_fmt(INSERT_INDEX_USAGE_HEAD_SQL, OB_ALL_INDEX_USAGE_INFO_TNAME);
    for (ObIndexUsagePairList::const_iterator it = info_list.begin(); it!=info_list.end(); it++) {
      insert_update_sql.append_fmt(
        "(%lu,%lu,%lu,'','',%lu,%lu,%lu,usec_to_time(%lu),usec_to_time(%lu),now(6)),",
        it->first.tenant_id, it->first.table_id, it->first.index_table_id,
        it->second.access_count, it->second.rows_returned, it->second.exec_count, 
        it->second.start_used_time, it->second.last_used_time);
    }
    insert_update_sql.set_length(insert_update_sql.length()-1);
    insert_update_sql.append(INSERT_INDEX_USAGE_ON_DUPLICATE_END_SQL);
    int64_t affected_rows;
    if (OB_FAIL(sql_proxy_->write(insert_update_sql.ptr(), affected_rows))) {
      LOG_WARN("insert update sql error", K(ret));
    }
  }
  return ret;
}

int ObIndexUsageReportTask::del_index_usage(const ObIndexUsageKey& key) {
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObDMLExecHelper exec(*sql_proxy_, common::OB_SYS_TENANT_ID);
  int64_t affected_rows = 0;

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", key.tenant_id))
           ||OB_FAIL(dml.add_pk_column("table_id", key.table_id))
           ||OB_FAIL(dml.add_pk_column("object_id", key.index_table_id))) {
    LOG_WARN("dml add column failed", K(ret));
  } else if (OB_FAIL(exec.exec_delete(OB_ALL_INDEX_USAGE_INFO_TNAME, dml, affected_rows))) {
    LOG_WARN("del sql exec error", K(ret));
  }
  return ret;
}

int ObIndexUsageReportTask::storage_index_usage(const uint64_t tenant_id) {
  int ret = OB_SUCCESS;
  // ObIndexUsageInfoMgr *mgr = MTL(ObIndexUsageInfoMgr *);

  // // todo: write data
  // common::ObList<ObIndexUsageInfoMgr::ObIndexUsagePair, common::ObFIFOAllocator>
  //     result_list(allocator_);

  // int64_t affected_rows = 0;
  // mgr->sample(result_list);
  // if (!result_list.empty()) {
  //   ObSqlString insert_update_sql;
  //   int count = 0;
  //   int batch_size = 1000;
  //   for (common::ObList<ObIndexUsageInfoMgr::ObIndexUsagePair,
  //                       common::ObFIFOAllocator>::iterator it =
  //            result_list.begin();
  //        it != result_list.end(); ++it) {
  //     count++;
  //     if (count > 1) {
  //       insert_update_sql.append(",");
  //     } else {
  //       insert_update_sql.append(INSERT_INDEX_USAGE_HEAD_SQL);
  //     }
  //     insert_update_sql.append_fmt(
  //         "(%lu,%lu,%lu,'','',0,0,%lu,now(6),usec_to_time(%ld),now(6))",
  //         it->first.tenant_id, it->first.index_table_id, it->first.database_id,
  //         it->second.ref_count, it->second.last_used_time);

  //     // mgr->release_node(&it->second);
  //     if (count % batch_size == 0) {
  //       insert_update_sql.append(INSERT_INDEX_USAGE_ON_DUPLICATE_END_SQL);
  //       if (OB_FAIL(
  //               sql_proxy_->write(insert_update_sql.ptr(), affected_rows))) {
  //         LOG_WARN("insert update sql error", K(ret));
  //       }
  //       insert_update_sql.reset();
  //     }
  //   }
  //   if (!insert_update_sql.empty()) {
  //     insert_update_sql.append(INSERT_INDEX_USAGE_ON_DUPLICATE_END_SQL);
  //     if (OB_FAIL(sql_proxy_->write(OB_SYS_TENANT_ID, insert_update_sql.ptr(),
  //                                   affected_rows))) {
  //       LOG_WARN("insert update sql error", K(ret));
  //     }
  //     insert_update_sql.reset();
  //   }
  // }
  // result_list.destroy();
  return ret;
}
} // namespace share
} // namespace oceanbase

#undef INSERT_INDEX_USAGE_HEAD_SQL
#undef INSERT_INDEX_USAGE_ON_DUPLICATE_END_SQL
