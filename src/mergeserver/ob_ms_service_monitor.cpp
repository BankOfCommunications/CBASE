
#include "ob_ms_service_monitor.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;


ObMergerServiceMonitor::ObMergerServiceMonitor(const int64_t timestamp)
  : ObStatManager(OB_MERGESERVER)
{
  startup_timestamp_ = timestamp;
  set_id2name(common::OB_STAT_MERGESERVER, common::ObStatSingleton::ms_map, common::MERGESERVER_STAT_MAX);
  set_id2name(common::OB_STAT_COMMON, common::ObStatSingleton::common_map, common::COMMON_STAT_MAX);
  set_id2name(common::OB_STAT_SQL, common::ObStatSingleton::sql_map, common::SQL_STAT_MAX);
  set_id2name(common::OB_STAT_OBMYSQL, common::ObStatSingleton::obmysql_map, common::OBMYSQL_STAT_MAX);
}

ObMergerServiceMonitor::~ObMergerServiceMonitor()
{

}

