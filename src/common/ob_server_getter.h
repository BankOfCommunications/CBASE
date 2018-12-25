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
#ifndef __OB_COMMON_OB_SERVER_GETTER_H__
#define __OB_COMMON_OB_SERVER_GETTER_H__
#include "ob_server.h"
#include "ob_spin_rwlock.h"

namespace oceanbase
{
  namespace common
  {
    enum ServerType
    {
      ANY_SERVER = 0,
      LSYNC_SERVER = 1,
      FETCH_SERVER = 2,
    };
    class IObServerGetter
    {
      public:
        IObServerGetter() {}
        virtual ~IObServerGetter() {}
        virtual int64_t get_type() const  = 0;
        virtual int get_server(ObServer& server) const  = 0;
    };

    class ObStoredServer : public IObServerGetter
    {
      public:
        ObStoredServer() {}
        virtual ~ObStoredServer() {}
        virtual int64_t get_type() const;
        virtual int get_server(ObServer& server) const;
        int set_server(const ObServer& server);        
      private:
        ObServer server_;
        mutable SpinRWLock server_lock_;
    };
  }; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_SERVER_GETTER_H__ */
