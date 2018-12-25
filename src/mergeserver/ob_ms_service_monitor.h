
#ifndef OB_MS_SERVICE_MONITOR_H_
#define OB_MS_SERVICE_MONITOR_H_

#include "common/ob_statistics.h"
#include "common/ob_common_stat.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerServiceMonitor : public common::ObStatManager
    {
    public:
      ObMergerServiceMonitor(const int64_t timestamp);
      virtual ~ObMergerServiceMonitor();
    private:
      int64_t startup_timestamp_;
    };
  }
}


#endif //OB_MS_SERVICE_MONITOR_H_
