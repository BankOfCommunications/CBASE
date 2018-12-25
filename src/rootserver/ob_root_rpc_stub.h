#ifndef OCEANBASE_ROOT_RPC_STUB_H_
#define OCEANBASE_ROOT_RPC_STUB_H_

#include "common/ob_fetch_runnable.h"
#include "common/ob_common_rpc_stub.h"
#include "common/ob_server.h"
#include "common/ob_schema.h"
#include "common/ob_range.h"
#include "common/ob_tablet_info.h"
#include "common/ob_tablet_info.h"
#include "common/ob_rs_ups_message.h"
#include "common/ob_data_source_desc.h"
#include "common/ob_list.h"
#include "common/ob_range2.h"
#include "ob_chunk_server_manager.h"
#include "common/ob_new_scanner.h"
#include "sql/ob_sql_scan_param.h"
#include "ob_daily_merge_checker.h"
//add pangtianze [Paxos rs_election] 20150611:b
#include "common/ob_rs_rs_message.h"
#include "common/roottable/ob_tablet_meta_table_row.h"
//add:e

namespace oceanbase
{
  namespace rootserver
  {
    class ObDataSourceProxyList;

    class ObRootRpcStub : public common::ObCommonRpcStub
    {
      public:
        ObRootRpcStub();
        virtual ~ObRootRpcStub();
        int init(const common::ObClientManager *client_mgr, common::ThreadSpecificBuffer* tsbuffer);
        // synchronous rpc messages
        //mod by pangtianze [Paxos rs_election] 20160926:b
        //virtual int slave_register(const common::ObServer& master, const common::ObServer& slave_addr, common::ObFetchParam& fetch_param, const int64_t timeout);
        virtual int slave_register(const common::ObServer& master, const common::ObServer& slave_addr, common::ObFetchParam& fetch_param, int64_t &paxos_num, int64_t &quorum_scale, const int64_t timeout);
        //mod:e
        virtual int set_obi_role(const common::ObServer& ups, const common::ObiRole& role, const int64_t timeout_us);
        virtual int switch_schema(const common::ObServer& server, const common::ObSchemaManagerV2& schema_manager, const int64_t timeout_us);
        //add zhaoqiong [Schema Manager] 20150327:b
        /**
         * @brief send schema_mutator to other server
         */
        virtual int switch_schema_mutator(const common::ObServer& server, const common::ObSchemaMutator& schema_mutator, const int64_t timeout_us);
        //add:e

        //add zhaoqiong [truncate table] 20160125:b
        virtual int truncate_table(const common::ObServer& server, const common::ObMutator& mutator, const int64_t timeout_us);
        //add:e
        virtual int migrate_tablet(const common::ObServer& src_cs, const common::ObDataSourceDesc& desc, const int64_t timeout_us);
        virtual int create_tablet(const common::ObServer& cs, const common::ObNewRange& range, const int64_t mem_version, const int64_t timeout_us);
        virtual int delete_tablets(const common::ObServer& cs, const common::ObTabletReportInfoList &tablets, const int64_t timeout_us);
        virtual int get_last_frozen_version(const common::ObServer& ups, const int64_t timeout_us, int64_t &frozen_version);
        virtual int get_obi_role(const common::ObServer& master, const int64_t timeout_us, common::ObiRole &obi_role);
        virtual int get_boot_state(const common::ObServer& master, const int64_t timeout_us, bool &boot_ok);
        virtual int revoke_ups_lease(const common::ObServer &ups, const int64_t lease, const common::ObServer& master, const int64_t timeout_us);
        virtual int import_tablets(const common::ObServer& cs, const uint64_t table_id, const int64_t version, const int64_t timeout_us);
        //add pangtianze [Paxos ups_replication] 20150603:b
        virtual int get_ups_max_log_seq_and_term(const common::ObServer& ups, uint64_t &max_log_seq, int64_t &ups_term, const int64_t timeout_us);
        //add:e
        //add pangtianze [Paxos rs_election] 20150731:b
        virtual int send_election_priority(const common::ObServer target_rs, const int64_t priority, const int64_t timeout_us);
        virtual int set_rs_leader(const common::ObServer target_rs);
        //add:e
        //add pangtianze [Paxos rs_election] 20160919:b
        /**
         * @brief fetch_ups_role
         * @param ups
         * @param role, 2 means slave, 1 means master
         * @return
         */
        virtual int get_ups_role(const common::ObServer &ups, int32_t &role, const int64_t timeout_us);
        virtual int ping_server(const common::ObServer &rs, const int64_t timeout_us);
        //add:e
        //add pangtianze [Paxos rs_election] 20170228:b
        /**
         * @brief sync refresh_rs_group_to_server
         * @param target_server
         * @param servers
         * @param server_count
         * @return
         */
        virtual int send_rs_list_to_server(const ObServer &target_server, const ObServer *servers, const int32_t server_count);
        //add:e
        virtual int get_ups_max_log_seq(const common::ObServer& ups, uint64_t &max_log_seq, const int64_t timeout_us);
        virtual int shutdown_cs(const common::ObServer& cs, bool is_restart, const int64_t timeout_us);
        virtual int get_row_checksum(const common::ObServer& server, const int64_t data_version, const uint64_t table_id, ObRowChecksum &row_checksum, int64_t timeout_us);

        virtual int get_split_range(const common::ObServer& ups, const int64_t timeout_us,
             const uint64_t table_id, const int64_t frozen_version, common::ObTabletInfoList &tablets);
        virtual int table_exist_in_cs(const common::ObServer &cs, const int64_t timeout_us,
            const uint64_t table_id, bool &is_exist_in_cs);
        // asynchronous rpc messages
        //add wenghaixing [secondary index static_index_build] 20150317
        virtual int index_signal_to_cs(const common::ObServer& cs, uint64_t tid, int64_t hist_width
                                       );
        //add e
        //add wenghaixing [secondary index static_index_build.heartbeat]20150528
        virtual int heartbeat_to_cs_with_index(const ObServer &cs,
                                               const int64_t lease_time,
                                               const int64_t frozen_mem_version,
                                               const int64_t schema_version,
                                               const int64_t config_version,
                                               const IndexBeat beat,
                                               const int64_t build_index_version = OB_INVALID_VERSION,
                                               const bool is_last_finished = false
                                               );
        //add e
        //add wenghaixing [secondary index.cluster]20150630
        virtual int get_init_index_from_ups(const ObServer ups, const int64_t timeout, const int64_t data_version, ObArray<uint64_t> *list);
        //add e
        virtual int heartbeat_to_cs(const common::ObServer& cs,
                                    const int64_t lease_time,
                                    const int64_t frozen_mem_version,
                                    const int64_t schema_version,
                                    const int64_t config_version
									);
        virtual int heartbeat_to_ms(const common::ObServer& ms,
                                    const int64_t lease_time,
                                    const int64_t frozen_mem_version,
                                    const int64_t schema_version,
                                    const common::ObiRole &role,
                                    const int64_t privilege_version,
                                    const int64_t config_version
                                    );

        virtual int grant_lease_to_ups(const common::ObServer& ups, common::ObMsgUpsHeartbeat &msg);
		//add pangtianze [Paxos rs_election] 20150611:b
        virtual int grant_lease_to_rs(const common::ObServer &rs,
                                      const common::ObMsgRsHeartbeat &msg,
                                      const common::ObServer *servers,
                                      const int32_t server_count);
        virtual int send_rs_heartbeat_resp(const common::ObServer &rs,
                                      common::ObMsgRsHeartbeatResp &msg);
        //add:e
        //add pangtianze [Paxos rs_election] 20150616:b
        virtual int send_vote_request(const common::ObServer &rs, common::ObMsgRsRequestVote &msg);
        //add:e
        //add pangtianze [Paxos rs_election] 20150618:b
        /**
         * @brief send_leader_broadcast
         * @param rs : sender rs
         * @param msg
         * @param force : if true, means target rs must follow sender rs
         * @return
         */
        virtual int send_leader_broadcast(const common::ObServer &rs, common::ObMsgRsLeaderBroadcast &msg, const bool force);
        //add:e
        //add pangtianze [Paxos rs_election] 20150814:b
        virtual int send_vote_request_resp(const common::ObServer &rs, common::ObMsgRsRequestVoteResp &msg);
        virtual int send_leader_broadcast_resp(const common::ObServer &rs, common::ObMsgRsLeaderBroadcastResp &msg);
        //add:e
        //add pangtianze [Paxos rs_election] 20150629:b
        virtual int rs_init_first_meta(const common::ObServer &rs, const ObTabletMetaTableRow first_meta_row, const int64_t timeout_us);
        //add:e
        //add pangtianze [Paxos rs_switch] 20170208:b
        /**
         * @brief send a message to server to make it leader timeout, then it can re-regist to master rs
         *        only can be used when rs slave switching to master!
         * @param server, rs_leader
         * @return
         */
        virtual int force_server_regist(const common::ObServer &rs_master, const common::ObServer &server);
        //add:e
        virtual int request_report_tablet(const common::ObServer& chunkserver);
        virtual int execute_sql(const common::ObServer& ms,
                                const common::ObString sql, int64_t timeout);
        virtual int request_cs_load_bypass_tablet(const common::ObServer& chunkserver,
            const common::ObTableImportInfoList &import_info, const int64_t timeout_us);
        virtual int request_cs_delete_table(const common::ObServer& chunkserver, const uint64_t table_id, const int64_t timeout_us);
        virtual int fetch_ms_list(const common::ObServer& rs, common::ObArray<common::ObServer> &ms_list, const int64_t timeout_us);

        // fetch range table from datasource, uri is used when datasouce need uri info to generate range table(e.g. datasource proxy server)
        virtual int fetch_range_table(const common::ObServer& data_source, const common::ObString& table_name,
            common::ObList<common::ObNewRange*>& range_table , common::ModuleArena& allocator, int64_t timeout);
        virtual int fetch_range_table(const common::ObServer& data_source, const common::ObString& table_name, const common::ObString& uri,
            common::ObList<common::ObNewRange*>& range_table ,common::ModuleArena& allocator, int64_t timeout);
        virtual int fetch_proxy_list(const common::ObServer& ms, const common::ObString& table_name,
            const int64_t cluster_id, ObDataSourceProxyList& proxy_list, int64_t timeout);
        virtual int fetch_slave_cluster_list(const common::ObServer& ms, const common::ObServer& master_rs,
            common::ObServer* slave_cluster_rs, int64_t& rs_count, int64_t timeout);
        virtual int fetch_master_cluster_id_list(const common::ObServer &ms,
            common::ObIArray<int64_t> &cluster_ids, const int64_t timeout);
        virtual int import(const common::ObServer& rs, const common::ObString& table_name,
            const uint64_t table_id, const common::ObString& uri, const int64_t start_time, const int64_t timeout);
        virtual int get_import_status(const common::ObServer& rs, const common::ObString& table_name,
            const uint64_t table_id, int32_t& status, const int64_t timeout);
        virtual int set_import_status(const common::ObServer& rs, const common::ObString& table_name,
            const uint64_t table_id, const int32_t status, const int64_t timeout);
        virtual int notify_switch_schema(const common::ObServer& rs, const int64_t timeout);
        //add bingo [Paxos datasource __all_cluster] 20170407:b
        virtual int fetch_cluster_flow_list(const common::ObServer &ms,
            int32_t *cluster_flows, const int64_t timeout);
        //add:e
        //add bingo [Paxos datasource __all_cluster] 20170710:b
        virtual int fetch_master_cluster_ids(const common::ObServer &ms,
            common::ObIArray<int64_t> &cluster_ids, const int64_t timeout);
        //add:e
        //add bingo [Paxos datasource __all_cluster] 20170710:b
        virtual int fetch_cluster_port_list(const common::ObServer &ms,
            int64_t *cluster_ports, const int64_t timeout);
        //add:e
        //add pangtianze [Paxos rs_election] 20161010:b
        int send_change_config_request(const common::ObServer &rs, common::ObMsgRsChangePaxosNum &msg, const int64_t timeout);
        int send_change_ups_quorum_scale_request(const common::ObServer &rs, common::ObMsgRsNewQuorumScale &msg, const int64_t timeout);
        //add:e
        //add lbzhong [Paxos Cluster.Balance] 201607014:b
        int is_cluster_exist(bool &is_exist, int64_t &rs_port, const common::ObServer &ms,
                             const int64_t timeout, const int32_t cluster_id);
        //add:e
        //add bingo [Paxos rs_election] 20161009
        int get_priority_from_rs(const common::ObServer& rs, const int64_t timeout_us, int32_t &priority);
        //add:e
        //add pangtianze [Paxos rs_election] 20150706:b
        int serialize_servers(common::ObDataBuffer& data_buffer, const common::ObServer *servers, const int32_t server_count);
        //add:e
        //add bingo [Paxos Cluster.Flow.UPS] 20170405:b
        int send_sync_is_strong_consistent_req(const common::ObServer &rs, const int64_t timeout_us, const int64_t is_strong_consistent);
        //add:e

      private:
        typedef std::pair<int64_t, common::ObServer> ClusterInfo;
        typedef common::ObIArray<ClusterInfo> ClusterInfoArray;
        //add bingo [Paxos datasource __all_cluster] 20170407:b
        typedef std::pair<int64_t, int64_t> ClusterFlowInfo;
        typedef common::ObIArray<ClusterFlowInfo> ClusterFlowInfoArray;
        int fetch_cluster_flow_info(ClusterFlowInfoArray &clusterFlows, const common::ObServer &ms,
            const int64_t timeout);
        int fill_cluster_flow_info(common::ObNewScanner& scanner, ClusterFlowInfoArray &clusterFlows);
        //add:e
        int fetch_cluster_info(ClusterInfoArray &clusters, const common::ObServer &ms,
            const int64_t timeout, const common::ObiRole &role);
        //add bingo [Paxos datasource __all_cluster] 20170710:b
        typedef common::ObIArray<int64_t> ClusterIDInfoArray;
        int fetch_cluster_id(ClusterIDInfoArray &clusters, const common::ObServer &ms,
            const int64_t timeout, const common::ObiRole &role);
        int fill_cluster_id_info(common::ObNewScanner& scanner, ClusterIDInfoArray &clusterIDs);
        //add:e
        //add bingo [Paxos datasource __all_cluster] 20170714:b
        typedef std::pair<int64_t, int64_t> ClusterPortInfo;
        typedef common::ObIArray<ClusterPortInfo> ClusterPortsArray;
        int fetch_cluster_port_info(ClusterPortsArray &clusterPorts, const common::ObServer &ms,
            const int64_t timeout);
        int fill_cluster_port_info(common::ObNewScanner& scanner, ClusterFlowInfoArray &clusterPorts);
        //add:e
        int fill_proxy_list(ObDataSourceProxyList& proxy_list, common::ObNewScanner& scanner);
        int fill_cluster_info(common::ObNewScanner& scanner, ClusterInfoArray &clusters);
        int get_thread_buffer_(common::ObDataBuffer& data_buffer);

      private:
        static const int32_t DEFAULT_VERSION = 1;
        common::ThreadSpecificBuffer *thread_buffer_;
    };
  } /* rootserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_ROOT_RPC_STUB_H_ */
