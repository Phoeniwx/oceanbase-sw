#include "ob_index_usage_report_task.h"
#include "lib/ob_errno.h"
#include "share/rc/ob_tenant_base.h"
#include "ob_index_usage_info_mgr.h"

#define USING_LOG_PREFIX SERVER
using namespace oceanbase::common;

namespace oceanbase
{
  namespace share
  {

    ObIndexUsageReportTask::ObIndexUsageReportTask()
        : is_inited_(false), allocator_(MTL_ID()), sql_proxy_(nullptr) {}

    int ObIndexUsageReportTask::init(common::ObMySQLProxy &sql_proxy)
    {
      int ret = OB_SUCCESS;
      // todo: init
      return ret;
    }

    void ObIndexUsageReportTask::destroy()
    {
      // todo: destroy
    }

    void ObIndexUsageReportTask::runTimerTask()
    {

      ObIndexUsageInfoMgr *mgr = MTL(ObIndexUsageInfoMgr *);
      
      // todo: write data
      common::ObList<ObIndexUsageInfoMgr::ObIndexUsagePair> result_list(allocator_);

      mgr->sample(result_list);

      mgr->release_node(result_list.begin()->second);
      
      //mgr->sample()


      result_list.destroy();
      //result_list.begin()
    }
  }
}