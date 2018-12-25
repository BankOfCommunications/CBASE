/**
 * Authors: lbzhong
 */

#ifndef OCEANBASE_UPDATESERVER_CLOG_STATUS_H_
#define OCEANBASE_UPDATESERVER_CLOG_STATUS_H_
#include "tbsys.h"
#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_base_client.h"
#include "common/ob_client_manager.h"
#include "common/ob_packet_factory.h"
#include "common/ob_result.h"
#include "ob_ups_role_mgr.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace updateserver
  {
    struct ClogStatus
    {
      int32_t self_ip_;
      int32_t self_port_;
      ObUpsRoleMgr::State state_;
      int32_t master_rs_ip_;
      int32_t master_rs_port_;
      int32_t master_ups_ip_;
      int32_t master_ups_port_;
      int64_t lease_;
      int64_t lease_remain_;

      ObUpsRoleMgr::Role role_;
      int64_t send_receive_;
      int64_t commit_replay_;
      int64_t log_term_;
      int64_t flush_;
      int64_t commit_point_;

      ClogStatus();
      int serialize(char* buf, int64_t len, int64_t& pos) const;
      int deserialize(const char* buf, int64_t len, int64_t& pos);
      int to_string(char* buf, const int64_t len) const;

      void reset();

      inline void set_lease_remain(const int64_t lease_remain)
      {
        if(lease_remain > 0)
        {
          lease_remain_ = lease_remain;
        }
        else
        {
          lease_remain_ = 0;
        }
      }

      inline void trim_buff(char* str, const int32_t len) const
      {
        char *tmp = new char[len];
        snprintf(tmp, len, "%s                    ", str);
        snprintf(str, len, "%s", tmp);
        delete [] tmp;
      }

      inline void get_host_self(char* buffer, const int32_t size, const bool need_format = false) const
      {
        if(need_format)
        {
          memset(buffer, 0, size);
        }
        snprintf(buffer, size, "%d.%d.%d.%d:%d",
            (self_ip_ & 0xFF),
            (self_ip_ >> 8) & 0xFF,
            (self_ip_ >> 16) & 0xFF,
            (self_ip_ >> 24) & 0xFF,
            self_port_);
        if(need_format)
        {
          trim_buff(buffer, size);
        }
      }

      inline void get_state(char* buffer, const int32_t size, const bool need_format = false) const
      {
        if(need_format)
        {
          memset(buffer, 0, size);
        }
        switch (state_)
        {
          case ObUpsRoleMgr::FATAL:
            snprintf(buffer, size, "Fatal");
            break;
          case ObUpsRoleMgr::REPLAYING_LOG:
            snprintf(buffer, size, "Replay");
            break;
          case ObUpsRoleMgr::ACTIVE:
            snprintf(buffer, size, "Active");
            break;
          case ObUpsRoleMgr::STOP:
            snprintf(buffer, size, "Stop");
            break;
          default:
            snprintf(buffer, size, "Timeout");
            break;
        }
        if(need_format)
        {
          trim_buff(buffer, size);
        }
      }

      inline void get_timeout_state(char* buffer, const int32_t size) const
      {
        memset(buffer, 0, size);
        snprintf(buffer, size, "Timeout");
        trim_buff(buffer, size);
      }

      inline void get_host_rs(char* buffer, const int32_t size, const bool need_format = false) const
      {
        if(need_format)
        {
          memset(buffer, 0, size);
        }
        snprintf(buffer, size, "%d.%d.%d.%d:%d",
            (master_rs_ip_ & 0xFF),
            (master_rs_ip_ >> 8) & 0xFF,
            (master_rs_ip_ >> 16) & 0xFF,
            (master_rs_ip_ >> 24) & 0xFF,
            master_rs_port_);
        if(need_format)
        {
          trim_buff(buffer, size);
        }
      }

      inline void get_host_ups(char* buffer, const int32_t size, const bool need_format = false) const
      {
        if(need_format)
        {
          memset(buffer, 0, size);
        }
        snprintf(buffer, size, "%d.%d.%d.%d:%d",
            (master_ups_ip_ & 0xFF),
            (master_ups_ip_ >> 8) & 0xFF,
            (master_ups_ip_ >> 16) & 0xFF,
            (master_ups_ip_ >> 24) & 0xFF,
            master_ups_port_);
        if(need_format)
        {
          trim_buff(buffer, size);
        }
      }
      inline void get_lease(char* buffer, const int32_t size, const bool need_format = false) const
      {
        if(need_format)
        {
          memset(buffer, 0, size);
        }
        snprintf(buffer, size, "%ld", lease_);
        if(need_format)
        {
          trim_buff(buffer, size);
        }
      }
      inline void get_lease_remain(char* buffer, const int32_t size, const bool need_format = false) const
      {
        if(need_format)
        {
          memset(buffer, 0, size);
        }
        snprintf(buffer, size, "%ld", lease_remain_);
        if(need_format)
        {
          trim_buff(buffer, size);
        }
      }
      inline void get_role(char* buffer, const int32_t size, const bool need_format = false) const
      {
        if(need_format)
        {
          memset(buffer, 0, size);
        }
        switch (role_)
        {
          case ObUpsRoleMgr::MASTER:
            snprintf(buffer, size, "Master");
            break;
          case ObUpsRoleMgr::SLAVE:
            snprintf(buffer, size, "Slave");
            break;
          case ObUpsRoleMgr::STANDALONE:
            snprintf(buffer, size, "Alone");
            break;
          default:
            snprintf(buffer, size, "Unkown");
            break;
        }
        if(need_format)
        {
          trim_buff(buffer, size);
        }
      }
      inline void get_send_receive(char* buffer, const int32_t size, const bool need_format = false) const
      {
        if(need_format)
        {
          memset(buffer, 0, size);
        }
        snprintf(buffer, size, "%ld", send_receive_);
        if(need_format)
        {
          trim_buff(buffer, size);
        }
      }
      inline void get_commit_replay(char* buffer, const int32_t size, const bool need_format = false) const
      {
        if(need_format)
        {
          memset(buffer, 0, size);
        }
        snprintf(buffer, size, "%ld", commit_replay_);
        if(need_format)
        {
          trim_buff(buffer, size);
        }
      }
      inline void get_log_term(char* buffer, const int32_t size, const bool need_format = false) const
      {
        if(need_format)
        {
          memset(buffer, 0, size);
        }
        snprintf(buffer, size, "%ld", log_term_);
        if(need_format)
        {
          trim_buff(buffer, size);
        }
      }
      inline void get_flush(char* buffer, const int32_t size, const bool need_format = false) const
      {
        if(need_format)
        {
          memset(buffer, 0, size);
        }
        snprintf(buffer, size, "%ld", flush_);
        if(need_format)
        {
          trim_buff(buffer, size);
        }
      }
      inline void get_commit_point(char* buffer, const int32_t size, const bool need_format = false) const
      {
        if(need_format)
        {
          memset(buffer, 0, size);
        }
        snprintf(buffer, size, "%ld", commit_point_);
        if(need_format)
        {
          trim_buff(buffer, size);
        }
      }
    };
  } // end namespace updateserver
} // end namespace oceanbase

#endif // OCEANBASE_UPDATESERVER_CLOG_STATUS_H_
