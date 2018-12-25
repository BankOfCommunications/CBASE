/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_trigger_handler.h,  12/12/2012 06:44:12 PM xiaochu Exp $
 * 
 * Author:  
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:  
 *   handle trigger_table updates mutator, notify the target with trigger event
 * 
 */
#ifndef __OB_UPDATESERVER_OB_TRIGGER_HANDLER_H__
#define __OB_UPDATESERVER_OB_TRIGGER_HANDLER_H__

#include "common/ob_server.h"
#include "ob_ups_mutator.h"
#include "ob_session_mgr.h"

namespace oceanbase
{
  namespace common
  {
    class ObTriggerMsg;
    class ObDdlTriggerMsg;//add zhaoqiong [Schema Manager] 20150327:b
  };

  namespace updateserver
  {
    class ObUpsMutator;
    class ObUpsRpcStub;
    class ObUpsRoleMgr;
    class ObTriggerHandler;

    class ObTriggerCallback : public ISessionCallback
    {
      public:
        ObTriggerCallback    ();
        int init             (ObTriggerHandler * handler);
        virtual int cb_func  (const bool rollback, void *data,
                              BaseSessionCtx &session);
      protected:
        ObTriggerHandler * handler_;
        bool               init_;
    };

    class ObTriggerHandler
    {
      public:
        ObTriggerHandler     ();
        virtual ~ObTriggerHandler();
        int init             (common::ObServer &rootserver,
                              ObUpsRpcStub *rpc_stub,
                              ObUpsRoleMgr *role_mgr);
        int handle_trigger   (ObUpsMutator &mutator) const;
        int handle_trigger   (common::ObTriggerMsg &msg) const;
        //add zhaoqiong [Schema Manager] 20150327:b
        /**
         * @brief send ddl trigger info to rs
         */
        int handle_trigger   (common::ObDdlTriggerMsg &msg) const;
        //add:e
        //add pangtianze [Paxos rs_election] 20150825:b
        inline void set_rootserver(const common::ObServer rs)
        {
          rootserver_ = rs;
        }
        //add:e
        ObTriggerCallback * get_trigger_callback();
      private:
        int next_value_      (ObUpsMutator &mutator, int64_t &value) const;
        static const int64_t RPC_TIMEOUT = 1 * 1000 * 1000; // 1s
        ObTriggerCallback    callback_;
        common::ObServer     rootserver_;
        ObUpsRpcStub *       rpc_stub_;
        ObUpsRoleMgr *       role_mgr_;
    };
  }; // end namespace updateserver
}; // end namespace updateserver

 
#endif // __OB_UPDATESERVER_OB_TRIGGER_HANDLER_H__
