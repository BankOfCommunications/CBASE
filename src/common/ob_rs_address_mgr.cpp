/*
* Version: $ObPaxos V0.1$
*
* Authors:
*  pangtianze <pangtianze@ecnu.cn>
*
* Date:
*  20150609
*
*  - message struct from rs to rs
*
*/
#include "common/ob_rs_address_mgr.h"
using namespace oceanbase::common;

namespace oceanbase
{
  namespace common
  {
    ObRsAddressMgr::ObRsAddressMgr()
      :server_count_(0),current_idx_(OB_INVALID_INDEX),master_rs_idx_(OB_INVALID_INDEX)
    {
    }
    ObRsAddressMgr::~ObRsAddressMgr()
    {
    }
    void ObRsAddressMgr::reset()
    {
      for (int32_t i = 0; i < OB_MAX_RS_COUNT; i++)
      {
        rootservers_[i].reset();
      }
      server_count_ = 0;
    }
    int32_t ObRsAddressMgr::find_rs_index(const common::ObServer &addr)
    {
       int32_t ret = -1;
       for (int32_t i = 0; i< OB_MAX_RS_COUNT; i++)
       {
         if (rootservers_[i] == addr)
         {
           ret = i;
           break;
         }
       }
       return ret;
    }
    int ObRsAddressMgr::get_master_rs(common::ObServer& master)
    {
      int ret = OB_SUCCESS;
      tbsys::CThreadGuard guard(&rs_mutex_);
      int32_t master_idx = get_master_rs_idx();
      if (master_idx < 0)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "failed to find master rootserver");
      }
      else
      {
        master = rootservers_[master_idx];
      }
      return ret;
    }
    void ObRsAddressMgr::set_master_rs(const common::ObServer &master)
    {
      int32_t idx = OB_INVALID_INDEX;
      tbsys::CThreadGuard guard(&rs_mutex_);
      if (-1 == (idx = find_rs_index(master)))
      {
         idx = add_rs(master);
         if (idx == OB_INVALID_INDEX)
         {
           TBSYS_LOG(WARN, "add rootserver failed, rs=%s", master.to_cstring());
         }
         else
         {          
           set_master_rs_idx(idx);
         }
      }
      else
      {
        set_master_rs_idx(idx);
      }
    }
    int32_t ObRsAddressMgr::add_rs(const common::ObServer &rs)
    {
        int32_t idx = OB_INVALID_INDEX;
        bool rs_exist = false;
        for (int32_t i = 0; i < OB_MAX_RS_COUNT; i++)
        {
          if (rootservers_[i] == rs)
          {
            idx = i;
            rs_exist = true;
            break;
          }
        }
        if (!rs_exist)
        {
          for (int32_t i = 0; i < OB_MAX_RS_COUNT; i++)
          {
            if (!rootservers_[i].is_valid())
            {
              rootservers_[i] = rs;
              idx = i;
              server_count_++;
              break;
            }
          }
        }
        return idx;
    }

    ObServer ObRsAddressMgr::next_rs()
    {
      tbsys::CThreadGuard guard(&rs_mutex_);
      common::ObSpinLockGuard guard_spin(lock_);
      ObServer svr;
      if (server_count_ <= 0 )
      {
        current_idx_ = -1;
      }
      if (current_idx_ >= 0)
      {
        current_idx_ = (current_idx_ + 1) % server_count_;
        svr = rootservers_[current_idx_];
      }
      return svr;
    }
    int ObRsAddressMgr::update_all_rs(const common::ObServer *rs_array, const int32_t count)
    {
      int ret = OB_SUCCESS;
      int32_t master_idx = OB_INVALID_INDEX;
      tbsys::CThreadGuard guard(&rs_mutex_);
      //add pangtianze [Paxos] 20170630:b
      if (NULL == rs_array || 0 == count)
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "unexpected error, ret=%d", ret);
      }
      else if (list_equal(rs_array, count))
      {
        TBSYS_LOG(DEBUG, "rootserver list do not change, rs num=%d", count);
        for (int i = 0; i < count; i++)
        {
           TBSYS_LOG(DEBUG, "list[%d], rs=[%s]", i, rootservers_[i].to_cstring());
        }
      }
      else
      {
      //add:e
        TBSYS_LOG(INFO, "begin update rootserver list");
        ObServer tmp_server;
        if (master_rs_idx_ >= 0)
        {
           tmp_server = rootservers_[master_rs_idx_];
        }
        //reset rootserver array
        reset();
        //server_count_ = count;
        for (int32_t i = 0; i < count; i++)
        {
          //mod pangtianze [Paxos rs_election] 20170710:b
          //rootservers_[i] = rs_array[i];
          if (OB_INVALID_INDEX == add_rs(rs_array[i]))
          {
            TBSYS_LOG(WARN, "add rootserver failed, rs=%s", rs_array[i].to_cstring());
          }
          else
          {
             TBSYS_LOG(INFO, "rs list, rs[%d]=%s", i, rootservers_[i].to_cstring());
          }
        }
        if (-1 == (master_idx = find_rs_index(tmp_server)))
        {
          master_idx = 0;
        }
        if (OB_INVALID_INDEX == master_idx)
        {
           TBSYS_LOG(ERROR, "master index in rs array is invalid");
        }
        else
        {
          set_master_rs_idx(master_idx);
          TBSYS_LOG(INFO, "end update rootserver list, count=%d, master_rs=%s",
                    server_count_, rootservers_[master_rs_idx_].to_cstring());
        }
      }
      //mod:e
      return ret;
    }
    //add pangtianze [Paxos re_election] 201706
    bool ObRsAddressMgr::list_equal(const common::ObServer *rs_array, const int32_t count)
    {
      bool ret = true;
      if (server_count_ != count)
      {
        ret = false;
      }
      else
      {
        for (int i = 0; i < server_count_; i++)
        {
          if (!(rootservers_[i] == rs_array[i]))
          {
            ret = false;
            break;
          }
        }
      }
      return ret;
    }
    //add:e
  }
}
