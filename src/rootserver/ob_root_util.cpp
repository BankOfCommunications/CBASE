/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_root_util.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_root_util.h"
#include <tbsys.h>
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

int ObRootUtil::delete_tablets(ObRootRpcStub &rpc_stub, const ObChunkServerManager &server_manager, 
                               ObTabletReportInfoList &delete_list, const int64_t timeout)
{
  static common::ObTabletReportInfoList delete_msg;
  static tbsys::CThreadMutex delete_msg_lock;
  int ret = OB_SUCCESS;

  tbsys::CThreadGuard guard(&delete_msg_lock);

  common::ObServer cs;
  const ObServerStatus *server_status = NULL; //add zhaoqiong [fix bug for RootTable Management]:20160104
  ObTabletReportInfo delete_tablet;
  ObTabletReportInfo* tablets = const_cast<ObTabletReportInfo*>(delete_list.get_tablet());
  int32_t server_idx = -1;
  do
  {
    delete_msg.reset();
    server_idx = -1;
    for (int i = 0; i < delete_list.get_tablet_size(); ++i)
    {
      if (-1 != tablets[i].tablet_location_.chunkserver_.get_port())
      {
        if (-1 == server_idx
            || (-1 != server_idx && server_idx == tablets[i].tablet_location_.chunkserver_.get_port()))
        {
          if (-1 == server_idx)
          {
            // send delete msg to this server
            server_idx = tablets[i].tablet_location_.chunkserver_.get_port();
            cs = server_manager.get_cs(server_idx);
            server_status = server_manager.get_server_status(server_idx); //add zhaoqiong [fix bug for RootTable Management] 20160104
          }
          tablets[i].tablet_location_.chunkserver_ = cs;
          if (OB_SUCCESS != (ret = delete_msg.add_tablet(tablets[i])))
          {
            TBSYS_LOG(ERROR, "add tablet error");
            break;
          }
          // mark this element as used
          tablets[i].tablet_location_.chunkserver_.set_port(-1);
        }
      }
    } // end for
    if (0 < delete_msg.get_tablet_size())
    {
      const ObTabletReportInfo* del_tablets = delete_msg.get_tablet();
      for (int i = 0; i < delete_msg.get_tablet_size(); ++i)
      {
        TBSYS_LOG(INFO, "try to delete tablet replica, cs=%s tablet=%s version=%ld row_count=%ld size=%ld crc=%ld i=%d", 
                  to_cstring(cs), to_cstring(del_tablets[i].tablet_info_.range_),
                  del_tablets[i].tablet_location_.tablet_version_, 
                  del_tablets[i].tablet_info_.row_count_, del_tablets[i].tablet_info_.occupy_size_,
                  del_tablets[i].tablet_info_.crc_sum_, i);
      }
      //add zhaoqiong [fix bug for RootTable Management]:20160104:b
      if ( server_status == NULL || server_status->status_ != ObServerStatus::STATUS_SERVING)
      {
        TBSYS_LOG(WARN, "failed to delete tablets msg, cs=%s, server_status is not serving ", to_cstring(cs));
      }
      //add:e
      else if (OB_SUCCESS != (ret = rpc_stub.delete_tablets(cs, delete_msg, timeout)))
      {
        TBSYS_LOG(WARN, "failed to send delete tablets msg, err=%d cs=%s", ret, to_cstring(cs));
      }
      else
      {
        TBSYS_LOG(INFO, "delete tablet replicas from cs, count=%ld cs=%s", 
                  delete_msg.get_tablet_size(), to_cstring(cs));
      }
    }
  //mod zhaoqiong [fix bug for RootTable Management]:20160104:b
  //} while(-1 != server_idx && OB_SUCCESS == ret);
  } while(-1 != server_idx);
  //mod:e
  return ret;
}
     
