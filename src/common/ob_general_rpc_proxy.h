#ifndef OB_COMMON_GENERAL_RS_RPC_PROXY_H_
#define OB_COMMON_GENERAL_RS_RPC_PROXY_H_

#include "tbsys.h"
#include "common/ob_rowkey.h"
#include "common/ob_server.h"
#include "common/ob_schema_service.h"
#include "common/ob_strings.h"
namespace oceanbase
{
  namespace common
  {
    class ObString;
    class ObServer;
    class ObNewRange;
    class ObRowkey;
    class ObSchemaManagerV2;
  };

  namespace common
  {
    // root server rpc proxy
    class ObGeneralRpcStub;
    class ObTabletLocationList;
    class ObTabletLocationCache;
    class ObGeneralRootRpcProxy
    {
    public:
      ObGeneralRootRpcProxy(const int64_t retry_times, const int64_t timeout,
          const common::ObServer & root);
      virtual ~ObGeneralRootRpcProxy();

    public:
      // retry interval time
      static const int64_t RETRY_INTERVAL_TIME = 20; // 20 ms usleep
      static const int64_t CREATE_DROP_TABLE_TIME_OUT = 5 * 1000 * 1000; // 5s
      ///
      int init(ObGeneralRpcStub * rpc_stub);
      
        // scan tablet location through root_server rpc call
      // param  @table_id table id of root table
      //        @row_key row key included in tablet range
      //        @location tablet location
      virtual int scan_root_table(ObTabletLocationCache * cache,
          const uint64_t table_id, const common::ObRowkey & row_key,
          const common::ObServer & server, ObTabletLocationList & location);
      //add pangtianze [Paxos rs_election] 20150716:b
      void set_general_root_server(const common::ObServer &server)
      {
        root_server_ = server;
      }
      //add:e

    private:
      // check inner stat
      bool check_inner_stat(void) const;
      // ok find a tablet item
      void find_tablet_item(const uint64_t table_id, const common::ObRowkey & row_key,
          const common::ObRowkey & start_key, const common::ObRowkey & end_key,
          const common::ObServer & addr, common::ObNewRange & range, bool & find,
          ObTabletLocationList & list, ObTabletLocationList & location);

    private:
      int64_t rpc_timeout_;                         // rpc call timeout
      int64_t rpc_retry_times_;                     // rpc retry times
      const ObGeneralRpcStub * rpc_stub_;            // rpc stub bottom module
      common::ObServer root_server_;                // root server
    };

    inline bool ObGeneralRootRpcProxy::check_inner_stat(void) const
    {
      return (rpc_stub_ != NULL);
    }
  }
}

#endif //OB_COMMON_GENERAL_RS_RPC_PROXY_H_
