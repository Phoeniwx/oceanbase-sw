#include "ob_index_usage_report_task.h"
#include "lib/ob_errno.h"
#include "share/rc/ob_tenant_base.h"
#include "ob_index_usage_info_mgr.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema.h"

#define USING_LOG_PREFIX SERVER
using namespace oceanbase::common;

namespace oceanbase
{
  namespace share
  {

#define INSERT_INDEX_USAGE_SQL "INSERT INTO __all_index_usage_info(tenant_id,table_id,object_id,name,owner,total_exec_count,start_used,last_used,last_flush_time) VALUES"
#define INSERT_INDEX_USAGE_ON_DUPLICATE_SQL " ON DUPLICATE UPDATE name=VALUES(name),owner=VALUES(owner),total_exec_count=total_exec_count + VALUES(total_exec_count),last_flush_time=VALUES(last_flush_time)"
    ObIndexUsageReportTask::ObIndexUsageReportTask()
        : is_inited_(false), allocator_(MTL_ID()), sql_proxy_(nullptr) {}

    int ObIndexUsageReportTask::init(common::ObMySQLProxy &sql_proxy)
    {
      int ret = OB_SUCCESS;
      if (!is_inited_)
      {
        is_inited_ = true;
        sql_proxy_ = &sql_proxy;
      }
      return ret;
    }

    void ObIndexUsageReportTask::destroy()
    {
      sql_proxy_ = NULL;
    }

    void ObIndexUsageReportTask::runTimerTask()
    {
      ObIndexUsageInfoMgr *mgr = MTL(ObIndexUsageInfoMgr *);
      // todo: write data
      common::ObList<ObIndexUsageInfoMgr::ObIndexUsagePair, common::ObFIFOAllocator> result_list(allocator_);

      int64_t affected_rows = 0;
      mgr->sample(result_list);
      if (result_list.size() > 0)
      {
        ObSqlString insert_update_sql;
        int count = 0;
        int batch_size = 1000;
        for (common::ObList<ObIndexUsageInfoMgr::ObIndexUsagePair, common::ObFIFOAllocator>::iterator it = result_list.begin();
             it != result_list.end(); ++it)
        {
          count++;
          if (count > 1)
          {
            insert_update_sql.append(",");
          }
          else
          {
            insert_update_sql.append(INSERT_INDEX_USAGE_SQL);
          }
          insert_update_sql.append_fmt("(%lu,%lu,%lu,'%.*s','%.*s',%lu,now(6),usec_to_time(%ld),now(6))",
                                       it->first->tenant_id, it->first->index_table_id, it->first->database_id,
                                       name.length(), name.ptr(),
                                       owner.length(), owner.ptr(),
                                       it->second->ref_count, 
                                       it->second->last_used_time);

          mgr->release_node(it->second);
          if (count % batch_size == 0)
          {
            insert_update_sql.append(INSERT_INDEX_USAGE_ON_DUPLICATE_SQL);
            sql_proxy_->write(insert_update_sql.ptr(), affected_rows);
            insert_update_sql.reset();
          }
        }
        if (!insert_update_sql.empty())
        {
          insert_update_sql.append(INSERT_INDEX_USAGE_ON_DUPLICATE_SQL);
          sql_proxy_->write(,insert_update_sql.ptr(), affected_rows);
          insert_update_sql.reset();
        }
        result_list.destroy();
      }
    }
  }
}

#undef INSERT_INDEX_USAGE_SQL
#undef INSERT_INDEX_USAGE_ON_DUPLICATE_SQL
