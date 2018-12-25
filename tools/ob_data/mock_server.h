#ifndef MOCK_SERVER_H_
#define MOCK_SERVER_H_

#include "tbsys.h"
#include "tbnet.h"

#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "common/ob_packet_factory.h"
#include "common/ob_base_server.h"
#include "common/ob_single_server.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace mergeserver
  {
    class MockServer : public ObSingleServer
    {
      public:
        virtual int initialize()
        {
          // not set port
          set_dev_name("bond0");
          set_thread_count(10);
          set_batch_process(false);
          set_packet_factory(&factory_);
          set_default_queue_size(100);
          set_packet_factory(&factory_);
          client_manager_.initialize(get_transport(), get_packet_streamer());
          ObSingleServer::initialize();
          return OB_SUCCESS;
        }

        virtual int do_request(ObPacket* base_packet) = 0;
        int set_self(const char* dev_name, const int32_t port);
      public:
        ThreadSpecificBuffer response_packet_buffer_;
      protected:
        ObPacketFactory factory_;
        ObClientManager client_manager_;
        ObServer self_;
    };

    class MockServerRunner : public tbsys::Runnable
    {
      public:
        MockServerRunner(MockServer & server) : mock_server_(server)
      {
      }

        virtual ~MockServerRunner()
        {
          mock_server_.stop();
        }

        virtual void run(tbsys::CThread *thread, void *arg)
        {
          UNUSED(thread);
          UNUSED(arg);
          mock_server_.start();
        }

      private:
        MockServer & mock_server_;
    };
  }
}



#endif // MOCK_SERVER_H_
