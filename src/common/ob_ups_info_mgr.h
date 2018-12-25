

/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_ups_info_mgr.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_UPS_INFO_MGR_H
#define _OB_UPS_INFO_MGR_H

#include "common/ob_ups_info.h"
#include "common/ob_ups_info_mgr_rpc_stub.h"

namespace oceanbase
{
  namespace common
  {
    class ObUpsInfoMgr
    {
    public:
      ObUpsInfoMgr(const ObServerType type, const ObServer& root_server, int64_t rpc_timeout);
      ~ObUpsInfoMgr();
    
    public:
      // warning: rpc_buff should be only used by rpc stub for reset
      // param  @rpc_buff rpc send and response buff
      //        @rpc_frame client manger for network interaction
      int init(ObUpsInfoMgrRpcStub* ups_info_mgr_rpc_stub);

      //get master ups from ups list
      int get_master_ups(ObServer& master_ups, const bool is_update_list, bool& is_updated);

      //get ups list
      int get_ups_list(ObUpsList& ups_list, const bool is_update_list, bool& is_updated);

      // fetch update server list from root server
      int fetch_update_server_list(int32_t& count, bool& is_updated);

    private:
      bool check_inner_stat(void) const;
      
    private:
      tbsys::CRWLock ups_info_mgr_lock_;   //update info mgr rw lock
      ObServerType server_type_;
      ObServer update_server_;              //master updateserver addr
      ObServer root_server_;                //root server
      ObUpsList update_server_list_;        // update server list for read
      int64_t rpc_timeout_;                         // rpc call timeout
      uint64_t cur_finger_print_;                   // server list finger print
      ObUpsInfoMgrRpcStub* ups_info_mgr_rpc_stub_;
    };
  }
}

#endif /* _OB_UPS_INFO_MGR_H */


