#ifndef MOCK_UPDATE_SERVER_H_ 
#define MOCK_UPDATE_SERVER_H_

#include "mock_server.h"


namespace oceanbase
{
  namespace sql 
  {
    namespace test
    {
      class MockUpdateServer : public MockServer
      {
      public:
        static const int32_t UPDATE_SERVER_PORT = 12343;
        int initialize();
        int do_request(ObPacket* base_packet);

      private:
        // get table cell
        int handle_get_table(ObPacket * ob_packet);

        // scan table row
        int handle_scan_table(ObPacket * ob_packet);

        // mutate table row
        int handle_mutate_table(ObPacket * ob_packet);

        int handle_mock_get(ObPacket * ob_packet);
        int handle_mock_scan(ObPacket * ob_packet);

        // get frozen version
        int handle_frozen_version(ObPacket * ob_packet);
      };
    }
  }
}


#endif //MOCK_UPDATE_SERVER_H_

