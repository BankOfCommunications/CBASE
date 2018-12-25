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
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_UPDATESERVER_UPS_MON_H_
#define OCEANBASE_UPDATESERVER_UPS_MON_H_
//add peiouya [OB_PING_SET_PRI_RT] 20141208:b
#include <sched.h>
//add 20141208:e
#include "tbsys.h"
#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_base_client.h"
#include "common/ob_client_manager.h"
#include "common/ob_packet_factory.h"
#include "common/ob_result.h"

namespace oceanbase
{
  namespace updateserver
  {
    class UpsMon
    {
    public:

      static UpsMon* get_instance();

      virtual ~UpsMon();
      virtual int start(const int argc, char *argv[]);
    protected:
      UpsMon();
      virtual void do_signal(const int sig);
      void add_signal_catched(const int sig);
      static UpsMon* instance_;
      virtual void print_usage(const char *prog_name);

      virtual int parse_cmd_line(const int argc, char *const argv[]);
      virtual int do_work();
      virtual int ping_ups();
      virtual int set_sync_limit();
      virtual int set_new_vip();
    private:
      static void sign_handler(const int sig);

    private:
      common::ObServer ups_server_;
      int64_t ping_timeout_us_;
      int64_t new_sync_limit_;
      bool set_sync_limit_;
      int32_t new_vip_;

    public:
      //modify peiouya [OB_PING_TIME_OUT] 20141029:b
      //static const int64_t DEFAULT_PING_TIMEOUT_US = 1000000;
      static const int64_t DEFAULT_PING_TIMEOUT_US = 3000000;
      //modify 20141029:e
      static const char* DEFAULT_LOG_LEVEL;
    }; // end class UpsMon

  } // end namespace updateserver
} // end namespace oceanbase

#endif // OCEANBASE_UPDATESERVER_UPS_MON_H_
