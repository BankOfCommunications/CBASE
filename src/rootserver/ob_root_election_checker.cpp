/*
* Version: $ObPaxos V0.1$
*
* Authors:
*  pangtianze <pangtianze@ecnu.cn>
*
* Date:
*  20150609
*
*  - check lease to decide whether or not that rs needs raise new election
*
*/
#include "ob_root_election_checker.h"
#include "ob_root_election_node_mgr.h"
#include "common/ob_define.h"
#include "tbsys.h"

using namespace rootserver;
using namespace common;
ObRsCheckRunnable::ObRsCheckRunnable(ObRootElectionNodeMgr& rs_election_mgr)
  :rs_node_mgr_(rs_election_mgr)
{
}
ObRsCheckRunnable::~ObRsCheckRunnable()
{
}
void ObRsCheckRunnable::run(tbsys::CThread* thread, void* arg)
{
  UNUSED(thread);
  UNUSED(arg);
  TBSYS_LOG(INFO, "[NOTICE] rs check thread start, waiting [10s]");
  const int wait_second = 10;
  for (int64_t i = 0; i < wait_second && !_stop; i++)
  {
    sleep(1);
  }
  TBSYS_LOG(INFO, "[NOTICE] rs check working");
  while (!_stop)
  {
    //mod pangtianze [Paxos rs_election] 20170628:b
    int64_t begt = tbsys::CTimeUtil::getTime();
    rs_node_mgr_.check_lease();
    int64_t endt = tbsys::CTimeUtil::getTime();
    if (endt - begt > 500 * 1000) //
    {
        TBSYS_LOG(WARN, " real check lease interval[%ld]", endt - begt);
    }
    usleep(CHECK_INTERVAL_US);
  }
  TBSYS_LOG(INFO, "[NOTICE] rs check thread exit");
}
