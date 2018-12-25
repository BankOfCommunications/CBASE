#ifndef MOCK_ROOT_SERVER_H_ 
#define MOCK_ROOT_SERVER_H_

#include "mock_server.h"

namespace oceanbase
{
  namespace mergeserver
  {
    namespace test
    {
      class MockRootServer : public MockServer
      {
      public:
        static const int32_t ROOT_SERVER_PORT = 12340;

      public:
        int initialize();

        // dispatcher process 
        int do_request(ObPacket* base_packet);

      private:
        // get update server vip
        int handle_get_updater(ObPacket * ob_packet);

        // get update server list
        int handle_get_ups_list(ObPacket * ob_packet);

        // fetch schema
        int handle_fetch_schema(ObPacket * ob_packet);

        // fetch schema version
        int handle_fetch_schema_version(ObPacket * ob_packet);

        // server register
        int handle_register_server(ObPacket * ob_packet);

        // get root table
        int handle_get_root(ObPacket * ob_packet);
      };
    }
  }
}


#endif //MOCK_ROOT_SERVER_H_

