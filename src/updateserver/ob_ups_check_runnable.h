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

#ifndef OCEANBASE_UPDATESERVER_OB_UPS_CHECK_RUNNABLE_H_
#define OCEANBASE_UPDATESERVER_OB_UPS_CHECK_RUNNABLE_H_

#include "common/ob_check_runnable.h"
#include "common/ob_obi_role.h"
namespace oceanbase
{
  namespace updateserver
  {
    class ObUpsCheckRunnable : public common::ObCheckRunnable
    {
      public:
        ObUpsCheckRunnable();
        virtual ~ObUpsCheckRunnable();
        int init(common::ObiRole *obi_role, common::ObRoleMgr *role_mgr,
            const int64_t check_period, common::ObCommonRpcStub* rpc_stub = NULL,
            common::ObServer* master = NULL, common::ObServer* slave_addr = NULL);
        int set_master_addr(common::ObServer *master);
        virtual void run(tbsys::CThread* thread, void * arg);
        inline void set_lease_off()
        {
          lease_on_ = false;
        }
      private:
        common::ObiRole *obi_role_;
    };
  }
}
#endif
