#ifndef MOCK_MERGE_SERVER_H_
#define MOCK_MERGE_SERVER_H_

#include "mock_server.h"
#include "mergeserver/ob_merge_server_params.h"
namespace oceanbase
{
  namespace mergeserver
  {
    class MockMergeServer : public MockServer
    {
      public:
        MockMergeServer();
        ~MockMergeServer();
        // called before server start
        virtual int initialize();
        virtual int do_request(ObPacket * base_packet);
      private:
        int ms_scan(const int64_t start_time,const int32_t version,const int32_t channel_id,tbnet::Connection* connection,common::ObDataBuffer& in_buffer,common::ObDataBuffer& out_buffer,const int64_t timeout_us);
      private:
        ObMergeServerParams ms_params_;

    };
  }
}
#endif
