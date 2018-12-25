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
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#include "ob_server_getter.h"

namespace oceanbase
{
  namespace common
  {
    int ObStoredServer::set_server(const ObServer& server)
    {
      int err = OB_SUCCESS;
      server_lock_.wrlock();
      server_ = server;
      server_lock_.unlock();
      return err;
    }

    int64_t ObStoredServer::get_type() const
    {
      return FETCH_SERVER;
    }

    int ObStoredServer::get_server(ObServer& server) const
    {
      int err = OB_SUCCESS;
      server_lock_.rdlock();
      server = server_;
      server_lock_.unlock();
      return err;
    }
  }; // end namespace common
}; // end namespace oceanbase
