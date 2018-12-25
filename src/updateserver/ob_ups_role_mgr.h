/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 */
#ifndef OCEANBASE_UPDATESERVER_OB_UPS_ROLE_MGR_H_
#define OCEANBASE_UPDATESERVER_OB_UPS_ROLE_MGR_H_

#include <stdint.h>
#include "common/ob_atomic.h"
#include <tbsys.h> 
#include "ob_ups_utils.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace updateserver
  {
    /// @brief ObUpsRoleMgr管理了进程的角色和状态 
    class ObUpsRoleMgr
    {
      public:
        enum Role
        {
          MASTER = 1,
          SLAVE = 2,
          STANDALONE = 3, // used for test
        };
        enum State
        {
          REPLAYING_LOG = 1,
          ACTIVE = 2,
          FATAL = 3,
          STOP = 4,
        };

      public:
        ObUpsRoleMgr()
        {
          role_ = SLAVE;
          state_ = REPLAYING_LOG;
        }

        virtual ~ObUpsRoleMgr() { }

        /// @brief 获取Role
        inline Role get_role() const {return role_;}

        /// @brief 修改Role
        inline void set_role(const Role role)
        {
          atomic_exchange(reinterpret_cast<uint32_t*>(&role_), role);
          TBSYS_LOG(INFO, "set_role=%s state=%s", get_role_str(), get_state_str());
        }

        /// 获取State
        inline State get_state() const {return state_;}

        /// 修改State
        inline void set_state(const State state)
        {
          atomic_exchange(reinterpret_cast<uint32_t*>(&state_), state);
          if (FATAL == state)
          {
            set_client_mgr_err(OB_IN_FATAL_STATE);
            TBSYS_LOG(ERROR, "stop client_mgr, enter FATAL state.");
          }
          // else if (STOP == state)
          // {
          //   set_client_mgr_err(OB_IN_STOP_STATE);
          //   TBSYS_LOG(WARN, "stop client_mgr, enter STOP state.");
          // }
          TBSYS_LOG(INFO, "set_state=%s role=%s", get_state_str(), get_role_str());
        }

        inline const char* get_role_str() const
        {
          switch (role_)
          {
            case MASTER:       return "MASTER";
            case SLAVE:        return "SLAVE";
            case STANDALONE:   return "STANDALONE";
            default:           return "UNKNOWN";
          }
        }

        inline const char* get_state_str() const
        {
          switch (state_)
          {
            case FATAL:             return "FATAL";
            case REPLAYING_LOG:     return "REPLAYING_LOG";
            case ACTIVE:            return "ACTIVE";
            case STOP:              return "STOP";
            default:                return "UNKNOWN";
          }
        }
        inline bool is_master() const
        {
          return (role_ == ObUpsRoleMgr::MASTER) && (state_ == ObUpsRoleMgr::ACTIVE);
        }
        int serialize(char* buf, const int64_t len, int64_t& pos) const {
          int err = OB_SUCCESS;
          if (OB_SUCCESS != (err = serialization::encode_i64(buf, len, pos, role_))
              || OB_SUCCESS != (err = serialization::encode_i64(buf, len, pos, state_)))
          {
            TBSYS_LOG(ERROR, "ups_role_mgr.serialize(buf=%p[%ld], pos=%ld)=>%d", buf, len, pos, err);
          }
          return err;
        }
        int deserialize(const char* buf, const int64_t len, int64_t& pos) {
          int err = OB_SUCCESS;
          if (OB_SUCCESS != (err = serialization::decode_i64(buf, len, pos, (int64_t*)&role_))
              || OB_SUCCESS != (err = serialization::decode_i64(buf, len, pos, (int64_t*)&state_)))
          {
            TBSYS_LOG(ERROR, "ups_role_mgr.deserialize(buf=%p[%ld], pos=%ld)=>%d", buf, len, pos, err);
          }
          return err;
        }
        
      private:
        Role role_;
        State state_;
    };
  } // end namespace updateserver
} // end namespace oceanbase

#endif // OCEANBASE_UPDATESERVER_OB_UPS_ROLE_MGR_H_
