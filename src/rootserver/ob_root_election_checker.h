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
#ifndef OB_ROOT_ELECTION_CHECKER_H
#define OB_ROOT_ELECTION_CHECKER_H

#include <tbsys.h>

namespace oceanbase
{
  namespace rootserver
  {
    class ObRootElectionNodeMgr;
    class ObRsCheckRunnable: public tbsys::CDefaultRunnable
    {
      public:
        ObRsCheckRunnable();
        ObRsCheckRunnable(ObRootElectionNodeMgr& rs_election_mgr);
        virtual ~ObRsCheckRunnable();
        virtual void run(tbsys::CThread* thread, void* arg);
      private:
        // disallow copy
        ObRsCheckRunnable(const ObRsCheckRunnable &other);
        ObRsCheckRunnable& operator=(const ObRsCheckRunnable &other);
      private:
        // data members
        ObRootElectionNodeMgr &rs_node_mgr_;
        static const int64_t CHECK_INTERVAL_US = 50000LL; // 50ms
    };    
  } // end namespace rootserver
} // end namespace oceanbase
#endif // OB_ROOT_ELECTION_CHECKER_H
