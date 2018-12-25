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
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#include "ob_ups_clog_status.h"

namespace oceanbase
{
  namespace updateserver
  {
    const char* obi_slave_stat_to_str(const ObiSlaveStat slave_stat)
    {
      const char* str = NULL;
      static const char* obi_slave_stat[] = {"INVALID_SLAVE", "FOLLOWED_SLAVE", "STANDALONE_SLAVE", "UNKNOWN_SLAVE"};
      if (slave_stat <= MIN_SLAVE_STAT || slave_stat >= MAX_SLAVE_STAT)
      {
        str = "unknown_slave";
      }
      else
      {
        str = obi_slave_stat[slave_stat];
      }
      return str;
    }

    const char* get_role_as_str(const int64_t role)
    {
      const char* str = NULL;
      const char* names[] = {"master_master", "master_slave", "slave_master", "slave_slave",
                             "master_master", "master_slave", "slave_master", "slave_slave"};
      if (role >= 0 && role < (int64_t)ARRAYSIZEOF(names))
      {
        str = names[role];
      }
      else
      {
        str = "unknown";
        TBSYS_LOG(WARN, "unknown role[%ld]", role);
      }
      return str;
    }

    const char* get_clog_src_name(const int64_t role)
    {
      const char* str = NULL;
      const char* names[] = {"self", "master_master", "master_master", "slave_master",
                             "self", "master_master", "lsync", "slave_master"};
      if (role >= 0 && role < (int64_t)ARRAYSIZEOF(names))
      {
        str = names[role];
      }
      else
      {
        str = "unknown";
        TBSYS_LOG(WARN, "unknown role[%ld]", role);
      }
      return str;
    }

    int get_role_as_int(const ObiRole::Role obi_role, const ObUpsRoleMgr::Role role, const ObiSlaveStat obi_slave = FOLLOWED_SLAVE)
    {
      return (obi_slave == STANDALONE_SLAVE? 4:0) + (obi_role == ObiRole::MASTER? 0: 2) + (role == ObUpsRoleMgr::MASTER ? 0: 1);
    }

    int ObUpsCLogStatus::serialize(char* buf, int64_t len, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      if (NULL == buf || pos < 0 || pos > len)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if(OB_SUCCESS != serialization::encode_i64(buf, len, pos, obi_slave_stat_)
              || OB_SUCCESS != slave_sync_type_.serialize(buf, len, pos)
              || OB_SUCCESS != obi_role_.serialize(buf, len, pos)
              || OB_SUCCESS != role_mgr_.serialize(buf, len, pos)
              || OB_SUCCESS != rs_.serialize(buf, len, pos)
              || OB_SUCCESS != self_.serialize(buf, len, pos)
              || OB_SUCCESS != ups_master_.serialize(buf, len, pos)
              || OB_SUCCESS != inst_ups_master_.serialize(buf, len, pos)
              || OB_SUCCESS != lsync_.serialize(buf, len, pos)
              || OB_SUCCESS != serialization::encode_i64(buf, len, pos, last_frozen_version_)
              || OB_SUCCESS != replayed_cursor_.serialize(buf, len, pos)
              || OB_SUCCESS != replay_switch_.serialize(buf, len, pos)
              || OB_SUCCESS != serialization::encode_i64(buf, len, pos, max_log_id_replayable_)
              || OB_SUCCESS != serialization::encode_i64(buf, len, pos, master_log_id_)
              || OB_SUCCESS != serialization::encode_i64(buf, len, pos, next_submit_log_id_)
              || OB_SUCCESS != serialization::encode_i64(buf, len, pos, next_commit_log_id_)
              || OB_SUCCESS != serialization::encode_i64(buf, len, pos, next_flush_log_id_)
              || OB_SUCCESS != serialization::encode_i64(buf, len, pos, last_barrier_log_id_)
              || OB_SUCCESS != serialization::encode_i64(buf, len, pos, wait_trans_)
              || OB_SUCCESS != serialization::encode_i64(buf, len, pos, wait_commit_)
              || OB_SUCCESS != serialization::encode_i64(buf, len, pos, wait_response_))
      {
        err = OB_SERIALIZE_ERROR;
        TBSYS_LOG(ERROR, "clog_status.serialize(buf=%p[%ld], pos=%ld)=>%d", buf, len, pos, err);
      }
      return err;
    }

    int ObUpsCLogStatus::deserialize(const char* buf, int64_t len, int64_t& pos)
    {
      int err = OB_SUCCESS;
      if (NULL == buf || pos < 0 || pos > len)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if(OB_SUCCESS != serialization::decode_i64(buf, len, pos, (int64_t*)&obi_slave_stat_)
              || OB_SUCCESS != slave_sync_type_.deserialize(buf, len, pos)
              || OB_SUCCESS != obi_role_.deserialize(buf, len, pos)
              || OB_SUCCESS != role_mgr_.deserialize(buf, len, pos)
              || OB_SUCCESS != rs_.deserialize(buf, len, pos)
              || OB_SUCCESS != self_.deserialize(buf, len, pos)
              || OB_SUCCESS != ups_master_.deserialize(buf, len, pos)
              || OB_SUCCESS != inst_ups_master_.deserialize(buf, len, pos)
              || OB_SUCCESS != lsync_.deserialize(buf, len, pos)
              || OB_SUCCESS != serialization::decode_i64(buf, len, pos, (int64_t*)&last_frozen_version_)
              || OB_SUCCESS != replayed_cursor_.deserialize(buf, len, pos)
              || OB_SUCCESS != replay_switch_.deserialize(buf, len, pos)
              || OB_SUCCESS != serialization::decode_i64(buf, len, pos, (int64_t*)&max_log_id_replayable_)
              || OB_SUCCESS != serialization::decode_i64(buf, len, pos, (int64_t*)&master_log_id_)
              || OB_SUCCESS != serialization::decode_i64(buf, len, pos, (int64_t*)&next_submit_log_id_)
              || OB_SUCCESS != serialization::decode_i64(buf, len, pos, (int64_t*)&next_commit_log_id_)
              || OB_SUCCESS != serialization::decode_i64(buf, len, pos, (int64_t*)&next_flush_log_id_)
              || OB_SUCCESS != serialization::decode_i64(buf, len, pos, (int64_t*)&last_barrier_log_id_)
              || OB_SUCCESS != serialization::decode_i64(buf, len, pos, (int64_t*)&wait_trans_)
              || OB_SUCCESS != serialization::decode_i64(buf, len, pos, (int64_t*)&wait_commit_)
              || OB_SUCCESS != serialization::decode_i64(buf, len, pos, (int64_t*)&wait_response_))
      {
        err = OB_DESERIALIZE_ERROR;
        TBSYS_LOG(ERROR, "clog_status.deserialize(buf=%p[%ld], pos=%ld)=>%d", buf, len, pos, err);
      }
      return err;
    }
    
    int ObUpsCLogStatus::to_str(char* buf, const int64_t len, const bool detail /*=true*/) const
    {
      int err = OB_SUCCESS;
      int64_t count = 0;
      int64_t count2 = 0;
      char rs_addr[OB_MAX_SERVER_ADDR_SIZE];
      char self_addr[OB_MAX_SERVER_ADDR_SIZE];
      char ups_master_addr[OB_MAX_SERVER_ADDR_SIZE];
      char inst_ups_master_addr[OB_MAX_SERVER_ADDR_SIZE];
      char lsync_addr[OB_MAX_SERVER_ADDR_SIZE];
      char* clog_src_addr_array[] = {self_addr, ups_master_addr, inst_ups_master_addr, ups_master_addr,
                                     self_addr, ups_master_addr, lsync_addr, ups_master_addr};
      int64_t role = get_role_as_int(obi_role_.get_role(), role_mgr_.get_role(), obi_slave_stat_);
      const char* clog_src_addr = (role >= 0 && role < (int)ARRAYSIZEOF(clog_src_addr_array)) ? clog_src_addr_array[role]: "unknown";
      if (!rs_.to_string(rs_addr, sizeof(rs_addr)) || !self_.to_string(self_addr, sizeof(self_addr))
          || !ups_master_.to_string(ups_master_addr, sizeof(ups_master_addr)) || !inst_ups_master_.to_string(inst_ups_master_addr, sizeof(inst_ups_master_addr))
          || !lsync_.to_string(lsync_addr, sizeof(lsync_addr)))
      {
        err = OB_ERR_UNEXPECTED;
      }
      else if (0 >= (count = snprintf(buf, len,
                                      "role=%s_%s:%s[%s], replay_switch=%ld[%s]\n"
                                      "frozen_version=%ld, cursor=%ld[%ld+%ld]->%ld<-%ld[%s:%s]\n"
                                      "next_submit=%ld, next_commit=%ld, next_flush=%ld, last_barrier=%ld\n"
                                      "wait_trans=%ld, wait_commit=%ld, wait_response=%ld\n"
                                      ,
                                      obi_role_.get_role_str(), role_mgr_.get_role_str(), role_mgr_.get_state_str(),
                                      slave_sync_type_.get_type_str(),
                                      replay_switch_.get_seq(), (replay_switch_.get_seq()%4) ? "replay_thread_stopped": "replay_thread_running",
                                      last_frozen_version_, 
                                      replayed_cursor_.log_id_, replayed_cursor_.file_id_, replayed_cursor_.offset_,
                                      max_log_id_replayable_, master_log_id_,
                                      get_clog_src_name(role), clog_src_addr,
                                      next_submit_log_id_, next_commit_log_id_, next_flush_log_id_, last_barrier_log_id_,
                                      wait_trans_, wait_commit_, wait_response_))
                     || count > len)
      {
        err = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(WARN, "snprintf(buf=%p[%ld]): count=%ld", buf, len, count);
      }
      else if (detail && 0 >= (count2 = snprintf(buf + count, len - count,
                                                 "rs=%s, self=%s, ups_master=%s, inst_ups_master=%s, lsync=%s\n",
                                                 rs_addr, self_addr, ups_master_addr, inst_ups_master_addr,
                                                 lsync_addr))
                               && count2 > len - count)
      {
        err = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(WARN, "snprintf(buf=%p[%ld]): count=%ld", buf + count, len - count, count2);
      }
      return err;
    }
  }; // end namespace updateserver
}; // end namespace oceanbase
