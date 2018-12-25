#ifndef MOCK_ROOT_SERVER_H_ 
#define MOCK_ROOT_SERVER_H_

#include "mock_server.h"

namespace oceanbase
{
  namespace updateserver
  {
    namespace test
    {
      class MockRootServer : public MockServer
      {
        public:
          static const int32_t ROOT_SERVER_PORT = 13110;

        public:
          int initialize();

          // dispatcher process 
          int do_request(ObPacket* base_packet);

        private:
          // schema changed
          int handle_waiting_job_done(ObPacket * ob_packet);

          // get update server
          int handle_get_updater(ObPacket * ob_packet);

          // fetch schema
          int handle_fetch_schema(ObPacket * ob_packet);

          // server register
          int handle_register_server(ObPacket * ob_packet);

          // scan root table
          int handle_scan_root(ObPacket * ob_packet);

          int handle_get_root(ObPacket * ob_packet);
      };
    }
  }
}


#endif //MOCK_ROOT_SERVER_H_

