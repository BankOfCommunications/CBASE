/*===============================================================
 *   (C) 2007-2010 Taobao Inc.
 *
 *
 *   Version: 0.1 2010-09-26
 *
 *   Authors:
 *          daoan(daoan@taobao.com)
 *
 *
 ================================================================*/
#ifndef OCEANBASE_ROOTSERVER_ROOT_WORKER_H_
#define OCEANBASE_ROOTSERVER_ROOT_WORKER_H_
#include "common/ob_version.h"
#include "common/ob_define.h"
#include "common/ob_base_server.h"
#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "common/ob_fetch_runnable.h"
#include "common/ob_role_mgr.h"
#include "common/ob_slave_mgr.h"
#include "common/ob_check_runnable.h"
#include "common/ob_packet_queue_thread.h"
#include "common/ob_packet.h"
#include "common/ob_packet_factory.h"
#include "common/ob_timer.h"
#include "common/ob_ms_list.h"
#include "common/ob_config_manager.h"
#include "common/ob_rs_rs_message.h"
#include "rootserver/ob_root_server2.h"
#include "rootserver/ob_root_rpc_stub.h"
#include "rootserver/ob_root_sql_proxy.h"
#include "rootserver/ob_root_log_replay.h"
#include "rootserver/ob_root_log_manager.h"
#include "rootserver/ob_root_stat.h"
#include "rootserver/ob_root_fetch_thread.h"
#include "rootserver/ob_root_server_config.h"
#include "rootserver/ob_root_inner_table_task.h"
//add wenghaixing [secondary index static_index_build]20150317
#include "rootserver/ob_index_monitor.h"
//add e
//add pangtianze [Paxos rs_election] 20150612:b
#include "rootserver/ob_root_election_checker.h"
#include "ob_root_election_node_mgr.h"
//add:e

namespace oceanbase
{
  namespace common
  {
    class ObDataBuffer;
    class ObServer;
    class ObScanner;
    class ObTabletReportInfoList;
    class ObGetParam;
    class ObScanParam;
    class ObRange;
    class ObTimer;
    class MsList;
    class ObGeneralRpcStub;
  }
  using common::ObConfigManager;
  namespace rootserver
  {
    class ObRootWorker :public common::ObBaseServer, public common::ObPacketQueueHandler
    {
      public:
        ObRootWorker(ObConfigManager &config_mgr, ObRootServerConfig &rs_config);
        virtual ~ObRootWorker();

        /**
         * handle packet received from network
         * push packet into queue
         * @param packet   packet to handle
         * @return int     return OB_SUCCESS if packet pushed, else return OB_ERROR
         */
        int handlePacket(common::ObPacket* packet);
        //tbnet::IPacketHandler::HPRetCode handlePacket(easy_request_t *connection, tbnet::Packet *packet);
        int handleBatchPacket(common::ObPacketQueue &packetQueue);
        bool handlePacketQueue(common::ObPacket *packet, void *args);
        int initialize();
        int start_service();
        void wait_for_queue();
        void destroy();

        int create_eio();

        bool start_merge();
        //add pangtianze [Paxos rs_election] 20150814:b
        int submit_leader_broadcast_task(const bool force);
        int submit_vote_request_task();
        //add:e

        int submit_check_task_process();
        int submit_delete_tablets_task(const common::ObTabletReportInfoList& delete_list);
        int schedule_after_restart_task(const int64_t delay,bool repeate = false);
        int submit_restart_task();
        int set_io_thread_count(int io_thread_num);
        //add huangjianwei [Paxos rs_switch] 20160726:b
        int submit_get_server_status_task();
        //add:e
        //add wenghaixing [secondary index static_index_build]20150317
        int submit_index_task();
        //add e
        //add liuxiao [secondary index 20150410] 20150410
        bool check_create_index_over();
        //add e
        //add liumz, [bugfix: cchecksum too large]20161109:b
        int clean_checksum_info(const int64_t max_draution_of_version,const int64_t current_version);
        //add:e
        ObRootLogManager* get_log_manager();
        common::ObRoleMgr* get_role_manager();
        common::ThreadSpecificBuffer::Buffer* get_rpc_buffer() const;
        virtual ObRootRpcStub& get_rpc_stub();
        virtual ObGeneralRpcStub& get_general_rpc_stub();

        ObRootServer2& get_root_server();
        ObConfigManager& get_config_mgr();
        ObRootServerConfig& get_config() const;
        //add bingo [Paxos Cluster.Balance] 20161024:b
        ObRootSQLProxy& get_sql_proxy();
        //add:e
        //add pangtianze [Paxos rs_election] 20150717:b
        MsList& get_ms_list_task();
        //add:e
        //add pangtianze [Paxos rs_election] 20150813:b
        common::ObPacketQueueThread& get_election_queue();
        //add:e

        int send_obi_role(common::ObiRole obi_role);
        common::ObClientManager* get_client_manager();
        int64_t get_network_timeout();
        common::ObServer get_rs_master();
        common::ThreadSpecificBuffer* get_thread_buffer();
        //add wenghaixing [secondary index static_index_build]20150318
        inline ObIndexMonitor* get_monitor()
        {
          return &index_monitor_;
        }
        //add e
	    //add liumz, [secondary index version management]20160413:b
        inline int64_t get_build_index_version()
        {
          return  index_monitor_.get_start_version();
        }
        inline bool is_last_finished()
        {
          return  index_monitor_.is_last_finished();
        }
        //add:e
		//add pangtianze [Paxos rs_election] 20150713:b
        inline common::ObServer get_self_rs()
        {
           return self_addr_;
        }
        //add:e
        //add pangtianze [Paxos rs_election] 20150619:b
        int switch_to_slave();
        inline void set_rt_master(const common::ObServer &rs_master)
        {
          common::ObSpinLockGuard guard(rt_master_lock_);
          rt_master_ = rs_master;
        }
        //add:e
        //add pangtianze [Paxos rs_switch] 20170208:b
        inline ObRsGetServerStatusTask* get_server_status_task()
        {
            return &get_server_status_task_;
        }
        //add:e
        //add pangtianze [Paxos rs_switch] 20170208:b
        inline bool is_switching_to_master()
        {
            return ObRoleMgr::MASTER == role_mgr_.get_role() && ObRoleMgr::ACTIVE != role_mgr_.get_state();
        }
        //add:e
      private:
        int start_as_master();
        int start_as_slave();
        //add pangtianze [Paxos rs_election] 20150701:b
        int start_as_master_leader();
        //add:e
        //add pangtianze [Paxos rs_election] 20150619:b
        int start_as_slave_follower();
        int switch_to_master();
        //add:e
        template <class Queue>
          int submit_async_task_(const common::PacketCode pcode, Queue &qthread, int32_t task_queue_size,
              const common::ObDataBuffer *data_buffer = NULL,
              const common::ObPacket *packet = NULL);
        template <class Queue>
          int submit_async_task_(const common::PacketCode pcode, Queue &qthread, int32_t task_queue_size,
              const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req,
              const uint32_t channel_id, const int64_t timeout);

        // notice that in_buff can not be const.
		//modify peiouya [Get_masterups_and_timestamp] 20141017:b
		/*
	    int rt_get_update_server_info(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff,
            bool use_inner_port = false);
		*/
        int rt_get_update_server_info(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff,
            bool use_inner_port = false, bool is_get_ups_settimestamp = false);
		//modify 20141017:e
        //add huangjianwei [Paxos rs_switch] 20160727:b
        int rt_get_server_status(const int32_t version, common::ObDataBuffer& in_buff,easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e
        //add pangtianze [Paxos rs_switch] 20170208:b
        /**
         * @brief rt_force_server_regist, only used in rs switch to master
         * @return
         */
        int rt_force_server_regist();
        const char * server_to_cstr(const ObString &svr_str);
        //add:e
        int rt_get_merge_delay_interval(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_get(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_scan(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_sql_scan(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_after_restart(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_write_schema_to_file(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_change_table_id(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_fetch_schema(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add zhaoqiong [Schema Manager] 20150327:b
        /**
         * @brief rs response other server fetch_schema_next request
         */
        int rt_fetch_schema_next(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e
        int rt_fetch_schema_version(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_report_tablets(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add liumz, [secondary index static_index_build] 20150320:b
        int rt_report_histograms(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e
        //add liuxiao [secondary index] 20150401
        int rt_get_old_tablet_column_checksum(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add e
        //add wenghaixing [secondary index cluster.p2]20150630
        int rt_get_index_process_info(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add e
        int rt_waiting_job_done(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_register(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_register_ms(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_migrate_over(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_report_capacity_info(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_heartbeat(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_heartbeat_ms(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        int rt_dump_cs_info(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_fetch_stats(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_check_tablet_merged(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add wenghaixing[secondary index] 20141110
        int rt_create_index_command(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add e

        int rt_split_tablet(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rs_check_root_table(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_ping(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_slave_quit(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_update_server_report_freeze(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id,
            common::ObDataBuffer& out_buff);

        int rt_slave_register(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_renew_lease(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_grant_lease(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_get_obi_role(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_get_boot_state(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_set_obi_role_to_slave(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_set_obi_role(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_get_last_frozen_version(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_force_cs_to_report(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_admin(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_change_log_level(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_stat(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rs_dump_cs_tablet_info(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_ups_heartbeat_resp(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_ups_register(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_ups_slave_failure(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_get_ups(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_set_ups_config(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_change_ups_master(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_get_cs_list(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_get_ms_list(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_get_proxy_list(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_cs_import_tablets(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_shutdown_cs(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_restart_cs(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_cs_delete_tablets(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_delete_tablets(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_create_table(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_force_create_table(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_force_drop_table(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_alter_table(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_drop_table(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add zhaoqiong [Truncate Table]:20160318:b
        int rt_truncate_table(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e
        //add wenghaixing [secondary index drop index]20141223
        int rt_drop_index(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add e
        int rt_execute_sql(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_handle_trigger_event(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add zhaoqiong [Schema Manager] 20150327:b
        /**
         * @brief handle ddl_trigger_event
         *      (create table, drop table, alter table)
         */
        int rt_handle_ddl_trigger_event(const int32_t version, common::ObDataBuffer& in_buff,easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e
        //add pangtianze [Paxos rs_election] 20150612:b
        int rt_send_vote_request(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_send_leader_broadcast(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_handle_rs_heartbeat(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_handle_rs_heartbeat_resp(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_handle_vote_request(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_handle_vote_request_resp(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_handle_leader_broadcast(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_handle_leader_broadcast_resp(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        void set_rs_heartbeat_res(ObMsgRsHeartbeatResp &hb_resp);
        //add:e
        //add pangtianze [Paxos rs_election] 20150629:b
        int rt_slave_init_first_meta_row(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e
        //add pangtianze [Paxos rs_election] 20150709:b
        int rt_slave_handle_register(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e
        //add pangtianze [Paxos rs_election] 20150731:b
        int rt_set_election_priority(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_set_my_election_priority(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e
        //add pangtianze [Paxos rs_election] 20150813:b
        int rt_rs_admin_set_rs_leader(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_set_rs_leader(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e
        int rt_set_config(const int32_t version,
            common::ObDataBuffer& in_buff, easy_request_t* req,
            const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_get_config(const int32_t version,
            common::ObDataBuffer& in_buff, easy_request_t* req,
            const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //bypass
        int rt_check_task_process(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        int rt_prepare_bypass_process(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_start_bypass_process(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_cs_delete_table_done(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rs_cs_load_bypass_sstable_done(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        int rt_get_row_checksum(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        int rt_start_import(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_import(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_start_kill_import(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_get_import_status(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_set_import_status(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_notify_switch_schema(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        //add chujiajia [Paxos rs_election] 20151102:b
        int rt_handle_change_paxos_num_request(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_handle_new_ups_quorum_scale_request(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_rs_admin_change_rs_paxos_number(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_rs_admin_change_ups_quorum_scale(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e
        //add lbzhong [Paxos Cluster.Balance] 201607020:b
        int rs_dump_balancer_info(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_get_replica_num(const int32_t version,
            common::ObDataBuffer& in_buff, easy_request_t* req,
            const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e
        //add bingo [Paxos table replica] 20170620:b
        int rt_get_table_replica_num(const int32_t version,
            common::ObDataBuffer& in_buff, easy_request_t* req,
            const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e
        //add bingo [Paxos sstable info to rs log] 20170614:b
        int rs_dump_sstable_info(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e
        //add lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
        int rt_get_cluster_ups(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e
        //add bingo [Paxos rs_election] 20161009
        int rt_master_get_all_priority(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_get_priority(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e
        //add bingo [Paxos Cluser.Balance] 20161020:b
        int rt_set_master_cluster_id(const int32_t version,
            common::ObDataBuffer& in_buff, easy_request_t* req,
            const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e
        //add bingo [Paxos __all_cluster] 20170714:b
        int set_new_master_cluster(const int32_t old_master_cluster_id, const int32_t new_master_cluster_id);
        //add:e
        //add bingo [Paxos rs restart] 20170221:b
        bool is_exist(common::ObServer *servers, common::ObServer server);
        //add:e
        //add bingo [Paxos rs management] 20170301:b
        int rt_get_rs_leader(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:es
        //add bingo [Paxos Cluster.Flow.UPS] 20170405:
        int rt_sync_is_strong_consistent(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e

      private:
        int do_stat(int stat_key, char *buf, const int64_t buf_len, int64_t& pos);
        int get_obi_role_from_master();
        int get_boot_state_from_master();
        int do_admin_with_return(int admin_cmd);
        int do_admin_without_return(int admin_cmd);
        int slave_register_(common::ObFetchParam& fetch_param);
        int rt_slave_write_log(const int32_t version, common::ObDataBuffer& in_buffer, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buffer);
        int rt_get_master_ups_config(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_set_master_ups_config(const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_get_master_obi_rs(const int32_t version, common::ObDataBuffer &in_buff, easy_request_t *req, const uint32_t channel_id, common::ObDataBuffer &out_buff);
      protected:
        const static int64_t ASYNC_TASK_TIME_INTERVAL = 5000 * 1000;
        ObConfigManager &config_mgr_;
        ObRootServerConfig &config_;
        bool is_registered_;
        ObRootServer2 root_server_;
        //add pangtianze [Paxos rs_election] 20150619:b
        common::ObPacketQueueThread election_thread_queue_;
        //add:e
        common::ObPacketQueueThread read_thread_queue_;
        common::ObPacketQueueThread write_thread_queue_;
        common::ObPacketQueueThread log_thread_queue_;
        common::ThreadSpecificBuffer my_thread_buffer;
        common::ObClientManager client_manager;
        common::ObServer rt_master_;
        common::ObServer self_addr_;
        common::ObRoleMgr role_mgr_;
        common::ObSlaveMgr slave_mgr_;
        common::ObCheckRunnable check_thread_;
        ObRootFetchThread fetch_thread_;
        ObRootSQLProxy sql_proxy_;
        ObRootRpcStub rt_rpc_stub_;
        common::ObGeneralRpcStub general_rpc_stub_;
        ObRootLogReplay log_replay_thread_;
        ObRootLogManager log_manager_;
        ObRootStatManager stat_manager_;
        int64_t schema_version_;
        MsList ms_list_task_;
        ObRootInnerTableTask inner_table_task_;
        ObRsAfterRestartTask after_restart_task_;
        //add huangjianwei [Paxos rs_switch] 20160727:b
        ObRsGetServerStatusTask get_server_status_task_;
        //add:e
        ObTimer timer_;
        //add wenghaixing [secondary index static_index_build]20150317
        ObIndexMonitor index_monitor_;
        //add e
		
        //add pangtianze [Paxos rs_election] 20150626:b
        common::ObSpinLock rt_master_lock_;
        //add:e
        //add jinty [Paxos Cluster.Balance] 20160809:b
        oceanbase::common::ObLocalMsList local_ms_for_slave_rs_;
        //add e
    };
    //add pangtianze [Paxos rs_election] 20150717:b
    inline MsList& ObRootWorker::get_ms_list_task()
    {
      return ms_list_task_;
    }
    //add:e
    //add pangtianze [Paxos rs_election] 20150813:b
    inline common::ObPacketQueueThread& ObRootWorker::get_election_queue()
    {
      return election_thread_queue_;
    }
    //add:e
    inline ObRootServer2& ObRootWorker::get_root_server()
    {
      return root_server_;
    }
    inline ObConfigManager& ObRootWorker::get_config_mgr()
    {
      return config_mgr_;
    }

    inline ObRootServerConfig& ObRootWorker::get_config() const
    {
      return config_;
    }
    //add bingo [Paxos Cluster.Balance] 20161024:b
    inline ObRootSQLProxy& ObRootWorker::get_sql_proxy()
    {
      return sql_proxy_;
    }
    //add:e
    inline ObRootRpcStub& ObRootWorker::get_rpc_stub()
    {
      return rt_rpc_stub_;
    }

    inline ObGeneralRpcStub& ObRootWorker::get_general_rpc_stub()
    {
      return general_rpc_stub_;
    }

    //add liuxiao [secondary index 20150410] 20150410
    inline bool ObRootWorker::check_create_index_over()
    {
      return (index_monitor_.check_create_index_over());
    }
    //add e

  }
}

#endif
