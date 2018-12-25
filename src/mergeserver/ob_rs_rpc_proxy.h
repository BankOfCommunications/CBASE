#ifndef _OB_MERGER_RS_RPC_PROXY_H_
#define _OB_MERGER_RS_RPC_PROXY_H_

#include "tbsys.h"
#include "common/ob_rowkey.h"
#include "common/ob_server.h"
#include "common/ob_schema_service.h"
#include "common/location/ob_tablet_location_list.h"
#include "common/ob_strings.h"
#include "common/ob_general_rpc_proxy.h"
#include "common/ob_obi_role.h"

namespace oceanbase
{
  namespace common
  {
    class ObString;
    class ObServer;
    class ObNewRange;
    class ObRowkey;
    class ObSchemaManagerV2;
    class ObMergerSchemaManager;
    class ObTabletLocationList;
    class ObTabletLocationCache;
    class ObGeneralRpcStub;
  };

  namespace mergeserver
  {
    // root server rpc proxy
    class ObMergerRootRpcProxy : public common::ObGeneralRootRpcProxy
    {
    public:
      ObMergerRootRpcProxy(const int64_t retry_times, const int64_t timeout,
          const common::ObServer & root);
      virtual ~ObMergerRootRpcProxy();

    public:
      // retry interval time
      static const int64_t RETRY_INTERVAL_TIME = 20; // 20 ms usleep
	  //modify peiouya 20140730:b
	  //修改创建表的超时时间为600s，即10分钟
      //static const int64_t CREATE_DROP_TABLE_TIME_OUT = 5 * 1000 * 1000; // 5s
	  static const int64_t CREATE_DROP_TABLE_TIME_OUT = 600 * 1000 * 1000; // 600s
	  //mod 20140730:e
      ///
      int init(common::ObGeneralRpcStub *rpc_stub);

      // merge server heartbeat with root server
      // param  @merge_server localhost merge server addr
      int async_heartbeat(const common::ObServer & merge_server, const int32_t sql_port,
          const bool is_listen_ms = false);

      // fetch newest schema
      int fetch_newest_schema(common::ObMergerSchemaManager * schema_manager,
          const common::ObSchemaManagerV2 ** manager);

      // fetch current schema version
      int fetch_schema_version(int64_t & timestamp);

      // create table
      int create_table(bool if_not_exists, const common::TableSchema & table_schema);
      // drop tables
      int drop_table(bool if_exists, const common::ObStrings & tables);
      // truncate tables
      int truncate_table(bool if_exists, const common::ObStrings & tables,
                         const common::ObString & user, const common::ObString & comment); //add zhaoqiong [Truncate Table]:20160318
      // alter table
      //add wenghaixing[secondary index drop index]20141222
      int drop_index(const common::ObStrings & indexs);
      //add e
      int alter_table(const common::AlterTableSchema & alter_schema);
      int fetch_master_ups(const ObServer &rootserver, ObServer & master_ups);
      //add pangtianze [Paxos rs_election] 20150716:b
      inline void set_root_server(const common::ObServer &server)
      {
        root_server_ = server;
        set_general_root_server(server);
      }
      //add:e
    public:
      // scan tablet location through root_server rpc call
      // param  @table_id table id of root table
      //        @row_key row key included in tablet range
      //        @location tablet location
      virtual int scan_root_table(ObTabletLocationCache * cache,
          const uint64_t table_id, const common::ObRowkey & row_key,
          const common::ObServer & server, ObTabletLocationList & location);
      int set_obi_role(const ObServer &rs, const int64_t timeout, const ObiRole &obi_role);
      int get_obi_role(const int64_t timeout_us, const common::ObServer& root_server, common::ObiRole &obi_role) const;
      int set_master_rs_vip_port_to_cluster(const ObServer &rs, const int64_t timeout, const char *new_master_ip, const int32_t new_master_port);
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
      const common::ObGeneralRpcStub *rpc_stub_;    // rpc stub bottom module
      common::ObServer root_server_;
    };

    inline bool ObMergerRootRpcProxy::check_inner_stat(void) const
    {
      return (rpc_stub_ != NULL);
    }
  }
}

#endif //_OB_MERGER_RS_RPC_PROXY_H_
