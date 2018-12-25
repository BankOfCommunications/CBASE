
#ifndef OCEANBASE_RPC_CLIENT_H_
#define OCEANBASE_RPC_CLIENT_H_

#include <tbnet.h>
#include "ob_packet.h"
#include "ob_server.h"

namespace oceanbase
{
  namespace common
  {
    class ObScanParam;
    class ObScanner;
    // not thread safe
    class ObRpcClient
    {
    public:
      ObRpcClient(const ObServer & server);
      virtual ~ObRpcClient();
    public:
      int init(void);
      // only for ups scan
      int scan(const ObScanParam & param, const int64_t timeout, ObScanner & result);
    private:
      int connect(void);
      void close(void);
      int write(void);
      int read(const int64_t timeout);
    private:
      int request(const int64_t retry_times, const int64_t timeout);
      int construct_request(const int64_t timeout, const ObScanParam & param);
      int parse_response(ObScanner & result, int & code);
      int parse_packet(ObScanner & result, int & code);
    private:
      static const int64_t MAX_ADDR_LEN = 64;
      static const int64_t MAX_PARAM_LEN = 2 * 1024 * 1024L;
      static const int64_t DEFAULT_RETRY_INTERVAL = 30;
    private:
      bool init_;
      bool ready_;
      int32_t retry_times_;
      int64_t retry_interval_;
      ObServer server_;
      ObPacket rpc_packet_;
      tbnet::DataBuffer request_buffer_;
      ObPacket response_packet_;
      tbnet::DataBuffer response_buffer_;
      char * param_buffer_;
      tbnet::DefaultPacketStreamer stream_;
      tbnet::Socket socket_; 
    };
  }
}

#endif //OCEANBASE_RPC_CLIENT_H_
