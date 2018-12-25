/*
* Version: $ObPaxos V0.1$
*
* Authors:
*  pangtianze <pangtianze@ecnu.cn>
*
* Date:
*  20150611
*
*  - heartbeat from rs to rs
*
*/
#ifndef _OB_RS_HEARTBEAT_RUNNABLE_H
#define _OB_RS_HEARTBEAT_RUNNABLE_H 1

#include "tbsys.h"
#include "ob_root_election_node_mgr.h"
namespace oceanbase
{
  namespace rootserver
  {
    class ObRsHeartbeatRunnable: public tbsys::CDefaultRunnable
    {
      public:
        ObRsHeartbeatRunnable(ObRootElectionNodeMgr &rs_node_mgr);
        virtual ~ObRsHeartbeatRunnable();
        virtual void run(tbsys::CThread* thread, void* arg);
      private:
        // disallow copy
        ObRsHeartbeatRunnable(const ObRsHeartbeatRunnable &other);
        ObRsHeartbeatRunnable& operator=(const ObRsHeartbeatRunnable &other);
      private:
        static const int64_t CHECK_INTERVAL_US = 100000LL; // 100ms
        // data members
        ObRootElectionNodeMgr &rs_node_mgr_;
    };
  } // end namespace rootserver
} // end namespace oceanbase

#endif // OB_RS_HEARTBEAT_RUNNABLE_H
