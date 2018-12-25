#ifndef RPC_STUB_H_
#define RPC_STUB_H_

#include "common/ob_scan_param.h"
#include "common/ob_scanner.h"
#include "common/thread_buffer.h"
#include "common/ob_server.h"
#include "common/ob_result.h"
#include "common/ob_client_manager.h"
#include "common/ob_packet.h"
#include "common/ob_schema.h"
#include "common/ob_mutator.h"

const char* ob_server_to_string(const oceanbase::common::ObServer& server);
namespace oceanbase
{
  namespace tools
  {
    class RpcStub
    {
    public:
      RpcStub(const common::ObClientManager * client, const common::ThreadSpecificBuffer * buffer);
      virtual ~RpcStub();

    public:
      static const int64_t DEFAULT_VERSION = 1;

      /// scan data by merge server
      int scan(const common::ObServer & server, const int64_t timeout,
          common::ObScanner & result);

      /// del all data
      int del(const common::ObServer & server, const int64_t timeout,
           common::ObScanner & result);
      int del(const common::ObServer & server, const int64_t timeout, 
	   const common::ObMutator & mutator);

    public:
      /// get update server
      int get_update_server(const common::ObServer & server, const int64_t timeout,
          common::ObServer & update_server);

      /// get merge server
      int get_merge_server(const common::ObServer & server, const int64_t timeout,
          common::ObServer list[], const int64_t size, int32_t & count);

    private:
      /// check inner stat
      bool check_inner_stat(void) const;

      /// get rpc buffer
      int get_rpc_buffer(common::ObDataBuffer & data_buffer) const;

    public:
      /// scan one server
      int scan(const common::ObServer & server, const int64_t timeout,
        const common::ObScanParam & param, common::ObScanner & result);

    private:
      const common::ObClientManager * frame_;
      const common::ThreadSpecificBuffer * buffer_;
    };

    inline bool RpcStub::check_inner_stat(void) const
    {
      return ((NULL != frame_) && (NULL != buffer_));
    }
  }
}


#endif // RPC_STUB_H_

