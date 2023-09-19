#include "ob_index_usage_report_task.h"
#include "lib/ob_errno.h"
#include "share/rc/ob_tenant_base.h"

#define USING_LOG_PREFIX SERVER
using namespace oceanbase::common;

namespace oceanbase {
namespace share {

ObIndexUsageReportTask::ObIndexUsageReportTask()
    : is_inited_(false), sql_proxy_(nullptr) {}

int ObIndexUsageReportTask::init(common::ObMySQLProxy &sql_proxy) {
  int ret = OB_SUCCESS;
  // todo: init
  return ret; 
}

void ObIndexUsageReportTask::destroy() {
  // todo: destroy
}

void ObIndexUsageReportTask::runTimerTask() {
  ObIndexUsageInfoMgr* mgr =  MTL(ObIndexUsageInfoMgr*);
  // todo: write data

}

}
}