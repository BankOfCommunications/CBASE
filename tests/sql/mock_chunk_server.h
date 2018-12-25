#ifndef MOCK_CHUNK_SERVER_H_ 
#define MOCK_CHUNK_SERVER_H_

#include "mock_server.h"


namespace oceanbase
{
  namespace sql 
  {
    namespace test
    {
      class MockChunkServer: public MockServer
      {
      public:
        static const int32_t CHUNK_SERVER_PORT = 12341;
        int initialize();
        int do_request(ObPacket* base_packet);
      
      private:
        // get table cell
        int handle_get_table(ObPacket * ob_packet);
        
        // scan table row
        int handle_scan_table(ObPacket * ob_packet);

        // do mock request
        int handle_mock_get(ObPacket *ob_packet);
        int handle_mock_scan(ObPacket *ob_packet);
      };
    }
  }
}


#endif //MOCK_CHUNK_SERVER_H_

