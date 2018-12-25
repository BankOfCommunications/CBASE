/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-23 15:55:21 fufeng.syd>
 * Version: $Id$
 * Filename: ob_update_reload_config.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#ifndef _OB_UPDATE_RELOAD_CONFIG_H_
#define _OB_UPDATE_RELOAD_CONFIG_H_

#include "common/ob_reload_config.h"

namespace oceanbase {
  namespace updateserver {
    /* forward declearation */
    class ObUpdateServer;

    class ObUpdateReloadConfig
      : public common::ObReloadConfig
    {
      public:
        ObUpdateReloadConfig();
        virtual ~ObUpdateReloadConfig();

        int operator() ();

        void set_update_server(ObUpdateServer &update_server);

      private:
        ObUpdateServer *update_server_;
    };
  } // end of namespace updateserver
} // end of namespace oceanbase

#endif /* _OB_UPDATE_RELOAD_CONFIG_H_ */
