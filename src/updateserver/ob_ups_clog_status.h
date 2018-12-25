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
#ifndef __OB_UPDATESERVER_OB_UPS_CLOG_STATUS_H__
#define __OB_UPDATESERVER_OB_UPS_CLOG_STATUS_H__
#include "common/ob_log_cursor.h"
#include "common/ob_switch.h"
#include "common/ob_server.h"
#include "common/ob_obi_role.h"
#include "ob_ups_role_mgr.h"
#include "ob_obi_slave_stat.h"
#include "ob_slave_sync_type.h"

namespace oceanbase
{
  namespace updateserver
  {
    struct ObUpsCLogStatus
    {
      ObUpsCLogStatus(): obi_slave_stat_(UNKNOWN_SLAVE), slave_sync_type_(),
                      obi_role_(), role_mgr_(), 
                      rs_(), self_(), ups_master_(), inst_ups_master_(), lsync_(),
                      last_frozen_version_(0),  replay_switch_(),
                         replayed_cursor_(), max_log_id_replayable_(0), master_log_id_(0),
                         next_submit_log_id_(), next_commit_log_id_(), next_flush_log_id_(), last_barrier_log_id_(),
                         wait_trans_(), wait_commit_(), wait_response_()
                         
      {}
      ~ObUpsCLogStatus() {}
      int serialize(char* buf, int64_t len, int64_t& pos) const;
      int deserialize(const char* buf, int64_t len, int64_t& pos);
      int to_str(char* buf, const int64_t len, bool detail=true) const;
      ObiSlaveStat obi_slave_stat_;
      ObSlaveSyncType slave_sync_type_;
      common::ObiRole obi_role_;
      ObUpsRoleMgr role_mgr_;
      common::ObServer rs_;
      common::ObServer self_;
      common::ObServer ups_master_;
      common::ObServer inst_ups_master_;
      common::ObServer lsync_;
      int64_t last_frozen_version_;
      ObSwitch replay_switch_;
      ObLogCursor replayed_cursor_;
      int64_t max_log_id_replayable_;
      int64_t master_log_id_;
      int64_t next_submit_log_id_;
      int64_t next_commit_log_id_;
      int64_t next_flush_log_id_;
      int64_t last_barrier_log_id_;
      int64_t wait_trans_;
      int64_t wait_commit_;
      int64_t wait_response_;
    };
  }; // end namespace updateserver
}; // end namespace oceanbase

#endif /* __OB_UPDATESERVER_OB_UPS_CLOG_STATUS_H__ */
