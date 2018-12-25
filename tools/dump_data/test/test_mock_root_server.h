#ifndef MOCK_ROOT_SERVER_H_ 
#define MOCK_ROOT_SERVER_H_

#include "base_server.h"

namespace oceanbase
{
  namespace tools 
  {
    class MockRootServer : public BaseServer
    {
      public:
        static const int32_t ROOT_SERVER_PORT = 22340;

      public:
        int initialize();

        // dispatcher process 
        int do_request(common::ObPacket* base_packet);

      private:
        // fetch schema
        int handle_fetch_schema(common::ObPacket * ob_packet);

        // get root table
        int handle_get_root(common::ObPacket * ob_packet);
    };
  }
}

#endif //MOCK_ROOT_SERVER_H_

