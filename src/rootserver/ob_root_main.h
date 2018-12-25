/*===============================================================
 *   (C) 2007-2010 Taobao Inc.
 *
 *
 *   Version: 0.1 2010-09-26
 *
 *   Authors:
 *          daoan(daoan@taobao.com)
 *
 *
 ================================================================*/
#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_MAIN_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_MAIN_H_
#include "common/ob_packet_factory.h"
#include "common/base_main.h"
#include "common/ob_define.h"
#include "common/ob_config_manager.h"
#include "ob_root_worker.h"
#include "ob_root_reload_config.h"
#include "ob_root_server_config.h"

namespace oceanbase
{
  namespace rootserver
  {
    class ObRootMain : public common::BaseMain
    {
      public:
        static common::BaseMain* get_instance();
        int do_work();
        void do_signal(const int sig);
      private:
        virtual void print_version();
        ObRootMain();
        ObRootServerConfig rs_config_;
        ObRootReloadConfig rs_reload_config_;
        common::ObConfigManager config_mgr_;
        ObRootWorker worker;
    };
  }
}
#endif
