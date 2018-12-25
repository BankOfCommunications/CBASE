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

#ifndef OCEANBASE_UPDATESERVER_OB_UPS_KEEP_ALIVE_H
#define OCEANBASE_UPDATESERVER_OB_UPS_KEEP_ALIVE_H

#include "common/ob_obi_role.h"
#include "ob_ups_slave_mgr.h"

namespace oceanbase
{
  namespace updateserver
  {
    class ObUpsKeepAlive : public tbsys::CDefaultRunnable
    {
      public:
        ObUpsKeepAlive();
        virtual ~ObUpsKeepAlive();
        int init(ObUpsSlaveMgr *slave_mgr, ObRoleMgr *role_mgr, ObiRole *obi_role, const int64_t keep_alive_tiemout);
        void set_is_registered_to_ups(const bool is_regisetered_to_ups);
        void renew_keep_alive_time();
        void clear();
        virtual void run(tbsys::CThread * thread, void * arg);
        static const int64_t DEFAULT_CHECK_PERIOD =  100 * 1000;
        static const int64_t DEFAULT_KEEP_ALIVE_SEND_PERIOD = 10;

      private:
        ObUpsSlaveMgr * slave_mgr_;
        ObRoleMgr * role_mgr_;
        ObiRole *obi_role_;
        int64_t last_keep_alive_time_;
        int64_t keep_alive_timeout_;
        bool is_registered_to_ups_;
    };
  }
}
#endif
