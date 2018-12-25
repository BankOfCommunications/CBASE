/*
 *   (C) 2007-2010 Taobao Inc.
 *
 *
 *   Version: 0.1 2010-8-12
 *
 *   Authors:
 *      qushan <qushan@taobao.com>
 *      zian <yunliang.shi@alipay.com>
 *
 */
#ifndef OCEANBASE_PROXYSERVER_PROXYSERVERMAIN_H_
#define OCEANBASE_PROXYSERVER_PROXYSERVERMAIN_H_

#include "common/base_main.h"
#include "common/ob_version.h"
#include "ob_proxy_server.h"
#include "ob_proxy_server_config.h"

namespace oceanbase
{
  namespace proxyserver
  {
    class ObProxyServerMain : public common::BaseMain 
    {
      protected:
        ObProxyServerMain();
      protected:
        virtual int do_work();
        virtual void do_signal(const int sig);
      public:
        static ObProxyServerMain* get_instance();
      public:
        const ObProxyServer& get_proxy_server() const { return server_ ; }
        ObProxyServer& get_proxy_server() { return server_ ; }
      protected:
        virtual void print_version();
      private:
        ObProxyServer server_;
        ObProxyServerConfig proxy_config_;
    };


#define THE_PROXY_SERVER ObProxyServerMain::get_instance()->get_proxy_server()

  } // end namespace proxyserver
} // end namespace oceanbase


#endif 

