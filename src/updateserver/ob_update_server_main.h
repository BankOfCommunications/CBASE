/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_update_server_main.h,v 0.1 2010/09/28 13:25:38 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_OB_UPDATE_SERVER_MAIN_H__
#define __OCEANBASE_CHUNKSERVER_OB_UPDATE_SERVER_MAIN_H__

#include "common/base_main.h"
#include "common/ob_shadow_server.h"
#include "common/ob_config_manager.h"
#include "common/ob_version.h"
#include "ob_update_server.h"
#include "ob_update_reload_config.h"
#include "ob_update_server_config.h"

namespace oceanbase
{
  namespace updateserver
  {
    class ObUpdateServerMain : public common::BaseMain
    {
      static const int SIG_RESET_MEMORY_LIMIT = 34;
      static const int SIG_INC_WORK_THREAD = 35;
      static const int SIG_DEC_WORK_THREAD = 36;
      static const int SIG_START_STRESS = 37;
      protected:
        ObUpdateServerMain();

      protected:
        virtual int do_work();
        virtual void do_signal(const int sig);
        virtual void print_version();

      public:
        static ObUpdateServerMain* get_instance();
      public:
        const ObUpdateServer& get_update_server() const
        {
          return server_;
        }

        ObUpdateServer& get_update_server()
        {
          return server_;
        }

      private:
        ObUpdateReloadConfig ups_reload_config_;
        ObUpdateServerConfig ups_config_;
        ObConfigManager config_mgr_;
        ObUpdateServer server_;
        common::ObShadowServer shadow_server_;
    };
  }
}

#endif //__OB_UPDATE_SERVER_MAIN_H__

