#ifndef SRC_SHARE_INDEX_USAGE_OB_INDEX_USAGE_REPORT_TASK_H_
#define SRC_SHARE_INDEX_USAGE_OB_INDEX_USAGE_REPORT_TASK_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/ob_define.h"

namespace oceanbase {
namespace share {

class ObIndexUsageReportTask : public common::ObTimerTask
{
public:
  static const int64_t INDEX_USAGE_TASK_PERIOD = 15* 60 * 1000L * 1000L; // 15min
public:
  ObIndexUsageReportTask();
  virtual ~ObIndexUsageReportTask() {};
  int init(common::ObMySQLProxy &sql_proxy);
  void destroy();

private:
  virtual void runTimerTask();
private:
  bool is_inited_;
  common::ObMySQLProxy *sql_proxy_;	// 写入内部表需要 sql proxy
};
}
}

#endif