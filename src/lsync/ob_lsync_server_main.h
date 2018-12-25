/**
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_lsync_server_main.h
 *
 * Authors:
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *
 */

#ifndef __OCEANBASE_LSYNC_OB_LSYNC_SERVER_MAIN_H__
#define __OCEANBASE_LSYNC_OB_LSYNC_SERVER_MAIN_H__

#include "common/base_main.h"
#include "ob_lsync_server_param.h"
#include "ob_lsync_server.h"

namespace oceanbase
{
  namespace lsync
  {
    const static int SIG_INC_LOG_LEVEL = SIGUSR1;
    const static int SIG_DEC_LOG_LEVEL = SIGUSR2;
    class ObLsyncServerMain : public common::BaseMain
    {
      protected:
        ObLsyncServerMain() : common::BaseMain(), app_name_(NULL) {}
        ~ObLsyncServerMain(){}

      protected:
        virtual int do_work();
        virtual void do_signal(const int sig);
        virtual void parse_cmd_line(const int argc,  char *const argv[]);
        virtual void print_usage(const char *prog_name);
        virtual void print_version();

      public:
        static ObLsyncServerMain* get_instance();
      public:
        const ObLsyncServer& get_lsync_server() const
        {
          return server_;
        }

        ObLsyncServer& get_lsync_server()
        {
          return server_;
        }

      private:
        const char* app_name_;
        ObLsyncServerParam cmd_line_param_;
        ObLsyncServerParam param_;
        ObLsyncServer server_;
    };
  }
}

#endif //__OCEANBASE_LSYNC_OB_LSYNC_SERVER_MAIN_H__

