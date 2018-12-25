/*
* Version: $ObPaxos V0.1$
*
* Authors:
*  pangtianze <pangtianze@ecnu.cn>
*
* Date:
*  20150707
*
*  - for ms/cs/ups to manage all rootserver info
*
*/
#include "common/ob_define.h"
#include "common/ob_server.h"
#include <tbsys.h>
#include "Mutex.h"
#include "common/ob_spin_lock.h"
namespace oceanbase
{
  namespace common
  {
    class ObRsAddressMgr
    {
      public:
        ObRsAddressMgr();
        ~ObRsAddressMgr();
        inline void set_master_rs_idx(const int32_t rs_idx)
        {
          common::ObSpinLockGuard guard(lock_);
          master_rs_idx_ = rs_idx;
        }
        int32_t get_master_rs_idx()
        {
          common::ObSpinLockGuard guard(lock_);
          return master_rs_idx_;
        }
        inline void init_current_idx()
        {
          common::ObSpinLockGuard guard(lock_);
          current_idx_ = master_rs_idx_;
        }
        /**
         * @brief get current rootserver by index
         * @return
         */
        inline common::ObServer get_current_rs()
        {
          common::ObSpinLockGuard guard(lock_);
          ObServer server;
          return current_idx_ < 0 ? server : rootservers_[current_idx_];
        }
        /**
         * @brief add_rs
         * @param rs
         * @return index of master rs
         */
        int32_t find_rs_index(const common::ObServer &addr);
        /**
         * @brief update local rs_array
         * @param rs_array
         * @param count
         * @return
         */
        int update_all_rs(const common::ObServer *rs_array, const int32_t count);
        void reset();
        int get_master_rs(common::ObServer& master);
        void set_master_rs(const common::ObServer &master);
        int32_t add_rs(const common::ObServer &rs);
        common::ObServer next_rs();
        //add pangtianze [Paxos rs_election] 20170630
      private:
        bool list_equal(const common::ObServer *rs_array, const int32_t count);        
        //add:e
      private:
        mutable tbsys::CThreadMutex rs_mutex_;
        common::ObSpinLock lock_;
        ObServer rootservers_[OB_MAX_RS_COUNT]; //store all rootservers info
        int32_t server_count_; //rootserver count
        int32_t current_idx_; //current index of rootserver for register
        int32_t master_rs_idx_;// array index of master rs,
    };
  }
}
#ifndef OB_RS_ADDRESS_MGR_H
#define OB_RS_ADDRESS_MGR_H

#endif // OB_RS_ADDRESS_MGR_H
