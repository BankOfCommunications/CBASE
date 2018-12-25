#ifndef BASE_SERVER_H_
#define BASE_SERVER_H_

#include "tbsys.h"
#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "common/ob_packet_factory.h"
#include "common/ob_base_server.h"
#include "common/ob_single_server.h"

namespace oceanbase
{
  namespace tools
  {
    class BaseServer : public common::ObSingleServer
    {
      public:
        /// init
        virtual int initialize();
        /// task dispatch
        virtual int do_request(common::ObPacket * base_packet) = 0;

        /// client manager
        common::ObClientManager * get_client(void)
        {
          return &client_manager_;
        }

        /// rpc buffer
        common::ThreadSpecificBuffer * get_buffer(void)
        {
          return &rpc_buffer_;
        }
      protected:
        common::ThreadSpecificBuffer rpc_buffer_;

      private:
        common::ObClientManager client_manager_;
    };

    class BaseServerRunner : public tbsys::Runnable
    {
      public:
        BaseServerRunner(BaseServer & server) : server_(server)
        {
        }

        virtual ~BaseServerRunner()
        {
          server_.stop();
        }

        virtual void run(tbsys::CThread *thread, void *arg)
        {
          UNUSED(thread);
          UNUSED(arg);
          int ret = server_.start();
          if (ret != common::OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "server start failed:ret[%d]", ret);
          }
        }

      private:
        BaseServer & server_;
    };
  }
}


#endif // BASE_SERVER_H_
