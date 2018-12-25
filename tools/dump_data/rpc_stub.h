#ifndef RPC_STUB_H_
#define RPC_STUB_H_

#include "common/ob_scan_param.h"
#include "common/ob_scanner.h"
#include "common/thread_buffer.h"
#include "common/ob_server.h"
#include "common/ob_result.h"
#include "common/ob_get_param.h"
#include "common/ob_client_manager.h"
#include "common/ob_packet.h"
#include "common/ob_schema.h"
#include "base_server.h"
#include "task_info.h"

namespace oceanbase
{
  namespace tools
  {
    class BaseServer;
    class RpcStub
    {
    public:
      RpcStub(common::ObClientManager * client, common::ThreadSpecificBuffer * buffer);
      virtual ~RpcStub();

    public:
      static const int64_t DEFAULT_VERSION = 1;
      // init base server
      void set_base_server(BaseServer * server) {server_ = server;}
      /// fetch task
      int fetch_task(const common::ObServer & server, const int64_t timeout,
        TaskCounter & count, TaskInfo & task);

      /// report task
      int report_task(const common::ObServer & server, const int64_t timeout,
        const int64_t result_code, const char * file_name, const TaskInfo & task);

      /// get data
      int get(const common::ObServer & server, const int64_t timeout,
        const common::ObGetParam & param, common::ObScanner & result);

      /// scan data
      int scan(const int64_t index, const TabletLocation & list, const int64_t timeout,
        const common::ObScanParam & param, common::ObScanner & result);

      /// get memtable version
      int get_version(const common::ObServer & server, const int64_t timeout,
        int64_t & version);

      /// get table schema
      int get_schema(const common::ObServer & server, const int64_t timeout,
        const int64_t version, common::ObSchemaManagerV2 & schema);

      /// get update server
      int get_update_server(const common::ObServer & server, const int64_t timeout,
        common::ObServer & update_server);

      /// finish response
      int response_finish(const int ret_code, const common::ObPacket * packet);

      /// fetch response
      int response_fetch(const int ret_code, const TaskCounter & couter,
        const TaskInfo & task, common::ObPacket * packet);

    private:
      /// check inner stat
      bool check_inner_stat(void) const;

      /// get rpc buffer
      int get_rpc_buffer(common::ObDataBuffer & data_buffer) const;

      /// scan one server
      int scan(const common::ObServer & server, const int64_t timeout,
        const common::ObScanParam & param, common::ObScanner & result);

    private:
      BaseServer * server_;
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

