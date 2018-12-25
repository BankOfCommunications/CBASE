#ifndef OCEANBASE_ASYNC_RPC_STUB_H_
#define OCEANBASE_ASYNC_RPC_STUB_H_

#include "common/ob_define.h"

namespace oceanbase
{
  namespace sql 
  {
    class ObSqlGetParam;
    class ObSqlScanParam;
  }
  namespace common
  {
    class ObServer;
    class ObDataBuffer;
    class ObGetParam;
    class ObScanParam;
    class ObClientManager;
    class ThreadSpecificBuffer;
  }

  namespace mergeserver
  {
    class ObMergerRpcEvent;
    class ObMsSqlRpcEvent;
    class ObMsSqlGetRequest;
    class ObMergerAsyncRpcStub
    {
    public:
      ObMergerAsyncRpcStub();
      virtual ~ObMergerAsyncRpcStub();
    
    public: 
      /// init with new multiple client manager
      int init(const common::ThreadSpecificBuffer * rpc_buffer, 
          const common::ObClientManager * rpc_frame);
    
    private:
      /// check inner stat
      virtual bool check_inner_stat(void) const;

      // get rpc buffer for send request
      int get_rpc_buffer(common::ObDataBuffer & data_buffer) const;
    
    public:
      // default cmd version
      static const int32_t DEFAULT_VERSION = 1;
      // for heartbeat cmd version
      static const int32_t NEW_VERSION = 2;
      // max server addr to string len
      static const int64_t MAX_SERVER_LEN = 64;

      /// send get reqeust for collect result
      virtual int get(const int64_t timeout, const common::ObServer & server,
          const common::ObGetParam & get_param, ObMergerRpcEvent & result) const;

      /// send scan request for collect result
      virtual int scan(const int64_t timeout, const common::ObServer & server,
          const common::ObScanParam & scan_param, ObMergerRpcEvent & result) const;

      virtual int get_session_next(const int64_t timeout, const common::ObServer & server, 
        const int64_t session_id, const int32_t req_type, ObMergerRpcEvent & result)const;

      /// send sql scan request for collect result
      virtual int scan(const int64_t timeout, const common::ObServer & server,
          const sql::ObSqlScanParam & scan_param, ObMsSqlRpcEvent & result) const;
      
      virtual int get(const int64_t timeout, const common::ObServer & server,
          const sql::ObSqlGetParam & get_param, ObMsSqlRpcEvent & result) const;

      virtual int get_session_next(const int64_t timeout, const common::ObServer & server, 
        const int64_t session_id, const int32_t req_type, ObMsSqlRpcEvent & result)const;


      const common::ObClientManager *get_client_manager()const
      {
        return rpc_frame_;
      }

    private:
      /// thread buffer for send packet serialize
      const common::ThreadSpecificBuffer * rpc_buffer_;   // rpc thread buffer
      const common::ObClientManager * rpc_frame_;         // rpc frame for send request
    };

    inline bool ObMergerAsyncRpcStub::check_inner_stat(void) const
    {
      // check server and packet version
      return ((NULL != rpc_buffer_) && (NULL != rpc_frame_)); 
    }
  }
}


#endif //OCEANBASE_ASYNC_RPC_STUB_H_
