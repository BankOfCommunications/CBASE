#ifndef _OB_CLIENT_H_
#define _OB_CLIENT_H_

#include "rpc_stub.h"
#include "base_client.h"
#include "common/ob_range.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "common/ob_scanner.h"
#include "common/ob_server.h"
namespace oceanbase
{
  namespace tools
  {
    class ObClient:public BaseClient
    {
    public:
      ObClient(const common::ObServer & server)
      {
        rpc_ = new(std::nothrow) RpcStub(get_client(), get_buffer());
        root_server_ = server;
        ms_num_ = 0;
        scan_timeout_ = 5000000;
        retry_times_ = 3;
      }
      //
      virtual ~ObClient()
      {
        if (rpc_ != NULL)
        {
          delete rpc_;
          rpc_ = NULL;
        }
      }
    public:
      /// exe delete rows
      int del(const common::ObString & table_name, const common::ObRange & range, int64_t & row_count);
      int scan(const common::ObString & table_name, const common::ObRange & range, common::ObScanner & ob_scanner);

    public:
      /// prepare get merge server list and master update server addr
      int prepare();

    public:
      void setMergeserver(const common::ObServer & server);
    private:
      int32_t retry_times_;
      RpcStub * rpc_;
      common::ObServer root_server_;
      const static int64_t MAX_COUNT = 8;
      common::ObMemBuffer range_buffer_;
      common::ObServer merge_server_[MAX_COUNT];
      common::ObStringBuf string_buf_;
      int32_t ms_num_;
      int64_t scan_timeout_;
      common::ObServer update_server_;
    };
  }
}

#endif //_OB_CLIENT_H_

