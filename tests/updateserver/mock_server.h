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
  namespace updateserver
  {
    class MockServer : public ObSingleServer
    {
    public:
      virtual int initialize()
      {
        // not set port
        set_dev_name("bond0");
        set_thread_count(1);
        set_batch_process(false);
        set_packet_factory(&factory_);
        set_default_queue_size(100);
        set_packet_factory(&factory_);
        client_manager_.initialize(get_transport(), get_packet_streamer());
        ObSingleServer::initialize();
        return OB_SUCCESS;
      }

      virtual int do_request(ObPacket* base_packet) = 0;
    public:
      ThreadSpecificBuffer response_packet_buffer_;
    private:
      ObPacketFactory factory_;
      ObClientManager client_manager_;
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
        mock_server_.start();
      }
    
    private:
      MockServer & mock_server_;
    };
  }
}



#endif // MOCK_SERVER_H_
