////===================================================================
 //
 // ob_log_rpc_stub.h liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-05-23 by Yubai (yubai.lk@alipay.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 // 
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_LIBOBLOG_RPC_STUB_H_
#define  OCEANBASE_LIBOBLOG_RPC_STUB_H_

#include "common/ob_define.h"
#include "common/ob_rpc_stub.h"
#include "common/ob_schema.h"
#include "updateserver/ob_fetched_log.h"
#include "common/ob_result.h"

namespace oceanbase
{
  namespace liboblog
  {
    class IObLogRpcStub
    {
      public:
        virtual ~IObLogRpcStub() {};

      public:
        virtual int init() = 0;

        virtual void destroy() = 0;

        virtual int fetch_log(const common::ObServer &ups,
            const updateserver::ObFetchLogReq& req,
            updateserver::ObFetchedLog& log,
            const int64_t timeout,
            common::ObResultCode &rc) = 0;

        virtual int fetch_schema(const common::ObServer &rs,
            common::ObSchemaManagerV2& schema,
            const int64_t timeout) = 0;
        virtual int get_max_log_seq(const common::ObServer &ups, const int64_t timeout, int64_t &max_log_id) = 0;
    };

    class ObLogRpcStub : public IObLogRpcStub, public common::ObRpcStub
    {
      public:
        ObLogRpcStub();
        ~ObLogRpcStub();
      public:
        int init();
        void destroy();
        int fetch_log(const common::ObServer &ups,
            const updateserver::ObFetchLogReq& req,
            updateserver::ObFetchedLog& log,
            const int64_t timeout,
            common::ObResultCode &rc);
        int fetch_schema(const common::ObServer &rs,
            common::ObSchemaManagerV2& schema,
            const int64_t timeout);
        int get_max_log_seq(const common::ObServer &ups, const int64_t timeout, int64_t &max_log_id);
      public:
        static bool fetch_log_errno_sensitive(const int error);
      private:
        bool inited_;
        common::ThreadSpecificBuffer buffer_;
        common::ObClientManager client_;
        easy_io_t *eio_;
        easy_io_handler_pt client_handler_;
    };
  }
}

#endif //OCEANBASE_LIBOBLOG_RPC_STUB_H_

