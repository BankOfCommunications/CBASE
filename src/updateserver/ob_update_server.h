/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_update_server.h,v 0.1 2010/09/28 13:39:26 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_OB_UPDATE_SERVER_H__
#define __OCEANBASE_CHUNKSERVER_OB_UPDATE_SERVER_H__

#include "easy_io_struct.h"
#include "common/ob_single_server.h"
#include "common/ob_packet_factory.h"
#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "common/batch_packet_queue_thread.h"
#include "ob_ups_role_mgr.h"
#include "common/ob_log_writer.h"
#include "common/ob_fetch_runnable.h"
#include "common/ob_packet_queue_thread.h"
#include "common/priority_packet_queue_thread.h"
#include "common/ob_rs_ups_message.h"
#include "common/ob_cur_time.h"
#include "ob_ups_slave_mgr.h"
#include "common/ob_perm_components.h"
#include "common/ob_login_mgr.h"
#include "common/ob_pointer_array.h"
#include "common/ob_schema_manager.h"
#include "common/ob_new_scanner.h"
#include "common/ob_new_scanner_helper.h"
#include "common/ob_general_rpc_stub.h"
#include "common/location/ob_tablet_location_list.h"
#include "common/location/ob_tablet_location_cache_proxy.h"
#include "common/ob_ms_list.h"
#include "common/ob_config_manager.h"
#include "ob_ups_timer_task.h"
#include "ob_ups_rpc_stub.h"
#include "ob_update_server_config.h"
#include "ob_ups_table_mgr.h"
#include "ob_ups_log_mgr.h"
#include "ob_ups_replay_runnable.h"
#include "ob_ups_stat.h"
#include "ob_sstable_mgr.h"
#include "ob_transfer_sstable_query.h"
#include "ob_ups_fetch_runnable.h"
#include "ob_ups_cache.h"
#include "ob_obi_slave_stat.h"
#include "ob_slave_sync_type.h"
#include "ob_trans_executor.h"
#include "ob_trigger_handler.h"
#include "ob_util_interface.h"
#include "common/ob_trace_id.h"
#include "common/ob_shadow_server.h"
//add zhaoqiong [Schema Manager] 20150327:b
#include "common/roottable/ob_scan_helper_impl.h"
#include "common/ob_schema_service_impl.h"
#include "ob_big_log_writer.h"
//add:e
//add pangtianze [Paxos rs_election] 20150708:b
#include "common/ob_rs_address_mgr.h"
//add:e

namespace oceanbase
{
  namespace updateserver
  {
    class ObClientWrapper;
    typedef common::ObPointerArray<common::ObScanner, common::BatchPacketQueueThread::MAX_BATCH_NUM> ScannerArray;

    struct ObSafeOnceGuard
    {
      ObSafeOnceGuard(int64_t timeout=-1): count_(0), cocurrent_count_(1), timeout_(timeout), last_launch_time_(0)
      {}
      ~ObSafeOnceGuard()
      {}
      bool launch_authorize()
      {
        bool ret = true;
        int64_t cocurrent_count = 1;
        int64_t cur_time = tbsys::CTimeUtil::getTime();
        if (last_launch_time_ > 0 && timeout_ > 0 && last_launch_time_ + timeout_ < cur_time)
        {
          cocurrent_count = ++cocurrent_count_;
        }
        if (__sync_fetch_and_add(&count_, 1) >= cocurrent_count)
        {
          done();
          ret = false;
        }
        return ret;
      }
      void launch_success()
      {
        last_launch_time_ = tbsys::CTimeUtil::getTime();
      }
      void launch_fail()
      {}
      void done()
      {
        cocurrent_count_ = __sync_fetch_and_add(&count_, -1);
      }
      int64_t count_;
      int64_t cocurrent_count_;
      int64_t timeout_;
      int64_t last_launch_time_;
    };
    class UpsWarmUpDuty : public common::ObTimerTask
    {
      static const int64_t MAX_DUTY_IDLE_TIME = 2000L * 1000L * 1000L; // 2000s
      public:
        UpsWarmUpDuty();
        virtual ~UpsWarmUpDuty();
        virtual void runTimerTask();
        bool drop_start();
        void drop_end();
        void finish_immediately();
      public:
        static int64_t get_warm_up_time();
        static int64_t get_warm_up_step_interval();
      private:
        int64_t duty_start_time_;
        int64_t cur_warm_up_percent_;
        volatile uint64_t duty_waiting_;
    };

    class ObUpdateServer;
    class ObUpsLogServerGetter: public IObServerGetter
    {
      public:
        ObUpsLogServerGetter(): ups_(NULL) {}
        virtual ~ObUpsLogServerGetter() {}
        int init(ObUpdateServer* ups);
        virtual int64_t get_type() const;
        virtual int get_server(ObServer& server) const;
      private:
        ObUpdateServer* ups_;
    };
    class ObPrefetchLogTaskSubmitter: public IObAsyncTaskSubmitter
    {
      public:
        struct Task
        {
          Task(): launch_time_(0) {}
          ~Task() {}
          int64_t launch_time_;
        };
      public:
        ObPrefetchLogTaskSubmitter(): running_task_num_(0), prefetch_timeout_(0), last_launch_time_(0), last_done_time_(0), ups_(NULL) {}
        virtual ~ObPrefetchLogTaskSubmitter() {}
        int init(int64_t prefetch_timeout, ObUpdateServer* ups);
        virtual int done(Task& task);
        virtual int submit_task(void* arg);
      private:
        int64_t running_task_num_;
        int64_t prefetch_timeout_;
        int64_t last_launch_time_;
        int64_t last_done_time_;
        ObUpdateServer* ups_;
    };

    class TransExecutor;
    class ZombieKiller;  //add hongchen [LOCK_WAIT_TIMEOUT] 20161209
    class ObUpdateServer
      : public common::ObBaseServer, public ObPacketQueueHandler, public common::IBatchPacketQueueHandler, public ObUtilInterface
    {
      public:
        const static int64_t RESPONSE_RESERVED_US = 60 * 1000 * 1000;
      friend class TransExecutor;
      friend class ZombieKiller;  //add hongchen [LOCK_WAIT_TIMEOUT] 20161209
      public:
        ObUpdateServer(common::ObConfigManager &config_mgr,
                       ObUpdateServerConfig& config, common::ObShadowServer& shadow_server);
        ~ObUpdateServer();
      private:
        DISALLOW_COPY_AND_ASSIGN(ObUpdateServer);
      public:
        //tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection *connection, tbnet::Packet *packet);
        int handleBatchPacket(ObPacketQueue &packetQueue);
        /** packet queue handler */
        int handlePacket(ObPacket *packet);
        bool handlePacketQueue(ObPacket *packet, void *args);
        bool handleBatchPacketQueue(const int64_t batch_num, ObPacket** packets, void* args);

        /** called before start server */
        virtual int initialize();
        virtual void wait_for_queue();
        virtual void destroy();
        void cleanup();

        virtual int start_service();
        virtual void stop();
        void req_stop();
        int wait_until_log_sync();
        bool is_master_lease_valid() const;
        virtual void on_ioth_start();
      public:
        const common::ObClientManager& get_client_manager() const
        {
          return client_manager_;
        }
        ObUpsRpcStub& get_ups_rpc_stub();
        int renew_master_inst_ups();

        common::ThreadSpecificBuffer::Buffer* get_rpc_buffer() const;
        const ObUpdateServerConfig& get_param() const
        {
          return config_;
        }

        ObUpsCache& get_ups_cache()
        {
          return ups_cache_;
        }

        ObUpsTableMgr& get_table_mgr()
        {
          return table_mgr_;
        }

      //add shili [LONG_TRANSACTION_LOG]  20160926:b
      ObBigLogWriter& get_big_log_writer()
      {
        return big_log_writer_;
      }
       ;
      //add e
        common::BatchPacketQueueThread& get_write_thread_queue()
        {
          return write_thread_queue_;
        }

        bool get_replay_checksum_flag() { return config_.replay_checksum_flag; }
        ObUpsLogMgr& get_log_mgr()
        {
          return log_mgr_;
        }

        const common::ObServer& get_self()
        {
          return self_addr_;
        }
        const common::ObServer& get_root_server()
        {
          return root_server_;
        }
        //add pangtianze [Paxos rs_election] 20150708:b
        inline void set_root_server(const common::ObServer &rs)
        {
          common::ObSpinLockGuard guard(rs_lock_);          
          root_server_ = rs; //set rootserver_ in updateserver::ObTriggerHandler
          trigger_handler_.set_rootserver(rs);
          ms_list_task_.set_rs(rs); //set rs_ in common::MsList
          //add pangtianze [Paxos rs_election] 20161124
          //dump to config file
          char ip_tmp[OB_MAX_SERVER_ADDR_SIZE] = "";
          rs.ip_to_string(ip_tmp,sizeof(ip_tmp));
          common::ObServer old_config_server;
          bool res = old_config_server.set_ipv4_addr(config_.root_server_ip, (int32_t)config_.root_server_port);
          if (!res)
          {
              TBSYS_LOG(ERROR, "root server address invalid: %s:%s",
                        config_.root_server_ip.str(),
                        config_.root_server_port.str());
          }
          if (old_config_server != rs)
          {
              config_.root_server_ip.set_value(ip_tmp);
              config_.root_server_port = rs.get_port();
              config_mgr_.dump2file();
              TBSYS_LOG(DEBUG, "update rootserver ip and port to config file [%s:%s]",
                 config_.root_server_ip.str(),
                 config_.root_server_port.str());
          }
          //add:e

        }
        //add:e
        const common::ObServer& get_ups_log_master();
        const common::ObServer & get_master_inst_rootserver()
        {
          return master_inst_root_server_;
        }

        int set_io_thread_count(int32_t io_thread_count);

        inline ObUpsSlaveMgr& get_slave_mgr()
        {
          return slave_mgr_;
        }

        inline ObUpsRoleMgr& get_role_mgr()
        {
          return role_mgr_;
        }

        inline const ObUpsRoleMgr& get_role_mgr() const
        {
          return role_mgr_;
        }

        inline common::ObiRole& get_obi_role()
        {
          return obi_role_;
        }

        inline const ObiRole& get_obi_role() const
        {
          return obi_role_;
        }
        inline SSTableMgr &get_sstable_mgr()
        {
          return sstable_mgr_;
        }
        inline ObTriggerHandler &get_trigger_handler()
        {
          return trigger_handler_;
        }
        inline ObTransferSSTableQuery &get_sstable_query()
        {
          return sstable_query_;
        }
        inline common::ObPermTable &get_perm_table()
        {
          return perm_table_cache_;
        }

        inline ObiSlaveStat get_obi_slave_stat() const
        {
          return obi_slave_stat_;
        }

        inline common::ObServer get_lsync_server() const
        {
          common::ObServer ret;
          server_lock_.rdlock();
          ret = lsync_server_;
          server_lock_.unlock();
          return ret;
        }

        inline void set_lsync_server(const char* ip, const int32_t port)
        {
          server_lock_.wrlock();
          lsync_server_.set_ipv4_addr(ip, port);
          server_lock_.unlock();
        }

        inline void set_lsync_server(const int32_t ip, const int32_t port)
        {
          server_lock_.wrlock();
          lsync_server_.set_ipv4_addr(ip, port);
          server_lock_.unlock();
        }

        inline TransExecutor &get_trans_executor()
        {
          return trans_executor_;
        }
        //add wangdonghui [ups_log_replication_optimize] 20161009 :b
        inline int64_t get_wait_sync_time()
        {
          return config_.wait_slave_sync_time;
        }
        inline int64_t get_wait_sync_type()
        {
          return config_.wait_slave_sync_type;
        }
        inline int64_t get_keep_alive_interval()
        {
          return config_.keep_alive_interval;
        }
        inline int64_t get_delete_ups_wait_time()
        {
          return config_.delete_ups_wait_time;
        }

        //add :e
        void set_log_sync_delay_stat_param();
        void set_log_replay_thread_param();

        int sync_update_schema(const bool always_try, const bool write_log, bool only_core_tables);

        int submit_major_freeze();

        int submit_auto_freeze();

        int submit_handle_frozen();

        int submit_report_freeze();

        int submit_switch_skey();

        int submit_force_drop();

        int submit_delay_drop(const bool for_immediately = false);

        int submit_immediately_drop();
        int submit_replay_commit_log();
        //add wangjiahao [Paxos] 20150719 :b
        int submit_replay_tmp_log();
        //add :e
        int submit_prefetch_remote_log(ObPrefetchLogTaskSubmitter::Task& task);
        int submit_switch_schema(CommonSchemaManagerWrapper& schema_mgr);
        //add zhaoqiong [Schema Manager] 20150327:b
        /**
         * @brief read queue submit OB_SWITCH_SCHEMA_MUTATOR task to write queue
         */
        int submit_switch_schema_mutator(ObSchemaMutator& schema_mutator);
        //add:e
        int submit_grant_keep_alive();
        int submit_lease_task();
        int submit_check_keep_alive();
        int submit_fake_write_for_keep_alive();
        int submit_update_schema();
        int submit_kill_zombie();
        int submit_check_sstable_checksum(const uint64_t sstable_id, const uint64_t checksum);

        void schedule_warm_up_duty(const bool for_immediately = false);
        int submit_load_bypass(const common::ObPacket *packet);
        int submit_check_cur_version();

        void apply_conf();
        int response_result_(int32_t ret_code, int32_t cmd_type, int32_t func_version,
                             easy_request_t* req, const uint32_t channel_id, int64_t receive_ts = 0, const char *ret_string = NULL);
        //add wangdonghui [ups_replication] 20170323 :b:e
        int response_result1_(int32_t ret_code, int32_t cmd_type, int32_t func_version, easy_request_t* req,
                              const uint32_t channel_id, int64_t message_residence_time_us, const char *ret_string = NULL);


      private:
        int start_threads();
        ///@fn switch form master_slave/slave_master/slave_slave to master_master
        int switch_to_master_master();
        ///@fn deal with switch case interval master_slave/slave_master/slave_slave
        int slave_change_role(const bool is_obi_change, const bool is_role_chenge);
        ///@fn switch form master_master to master_slave/slave_master/slave_slave
        int master_switch_to_slave(const bool is_obi_change, const bool is_role_change);
        int reregister_followed(const common::ObServer &master);
        int reregister_standalone();
        int reregister_to_rootserver();
        //int set_schema();
        int set_timer_major_freeze();
        int set_timer_kill_zombie();
        int set_timer_handle_fronzen();
        int set_timer_refresh_lsync_addr();
        int set_timer_switch_skey();
        int set_timer_grant_keep_alive();
        int set_timer_check_keep_alive();
        int set_timer_check_lease();
        int set_timer_time_update();
        int init_slave_log_mgr();
        int slave_report_quit();

        int register_and_start_fetch(const common::ObServer &master, uint64_t &replay_point);
        //add
        int register_and_continue_fetch(const ObServer &master);
        int register_to_master_ups(const ObServer &master);
        int start_standalone_();
        int set_fetch_thread(const ObUpsFetchParam &fetch_param);

        int set_self_(const char* dev_name, const int32_t port);

        int slave_register_followed(const ObServer &master, ObUpsFetchParam & fetch_param, uint64_t &max_log_seq);
        //add:
        int ups_update_lease(const common::ObMsgUpsHeartbeat &hb);
        int ups_revoke_lease(const common::ObMsgRevokeLease &revoke_info);
        bool is_lease_valid() const;
        bool get_sync_state();
        bool can_serve_read_req(const bool is_consistency_read, const int64_t query_version);
        int init_fetch_thread(const common::ObFetchParam &fetch_param);
        int response_result(int32_t ret_code, ObPacket &pkt);
        int response_result(int32_t ret_code, const char *ret_string, ObPacket &pkt);
        int response_trans_id(int32_t ret_code, ObPacket &pkt, common::ObTransID &id, ObDataBuffer &out_buffer);
        int response_scanner(int32_t ret_code, ObPacket &pkt, common::ObScanner &scanner, ObDataBuffer &out_buffer);
        int response_scanner(int32_t ret_code, ObPacket &pkt, common::ObNewScanner &new_scanner, ObDataBuffer &out_buffer);
        int response_scanner(int32_t ret_code, ObPacket &pkt, common::ObCellNewScanner &new_scanner, ObDataBuffer &out_buffer);
        int response_buffer(int32_t ret_code, ObPacket &pkt, common::ObDataBuffer &buffer, const char *ret_string = NULL);
      private:
        //add:
        int start_timer_schedule();
        int return_not_master(const int32_t version, easy_request_t* req,
            const uint32_t channel_id, const int32_t packet_code);

        int ups_rs_lease(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int ups_rs_revoke_lease(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int ob_login(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int ups_switch_skey();
        int check_keep_alive_();
        int grant_keep_alive_();
        int check_lease_();
        void set_heartbeat_res(ObMsgUpsHeartbeatResp &hb_res);
        int register_to_rootserver(const uint64_t log_seq_id);
        void set_register_msg(const uint64_t log_id, ObMsgUpsRegister &msg_register);
        int set_obi_role(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        ///slave_master ups send fetch param to slave_slave
        int slave_set_fetch_param(const int32_t version, common::ObDataBuffer& in_buff,
                    easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int ups_slave_write_log(const int32_t version, common::ObDataBuffer& in_buff,
                                easy_request_t* req, const uint32_t channel_id,
                                //add chujiajia [Paxos ups_replication] 20160107:b
                                const int64_t cmt_log_id,
                                //add:e
                                common::ObDataBuffer& out_buff);
        int ups_fetch_log_for_slave(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff, common::ObPacket* packet);

        int ups_fill_log_cursor_for_slave(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int ups_get_clog_status(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add lbzhong [Clog Monitor] 20151218:b
        int clog_monitor_get_ups_list(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int clog_monitor_get_clog_status(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e
        int ups_set_sync_limit(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int ups_get_clog_cursor(const int32_t version,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int ups_get_clog_master(const int32_t version, easy_request_t* req,
                                const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int ups_get_log_sync_delay_stat(const int32_t version, easy_request_t* req,
                                        const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int ups_get_clog_stat(const int32_t version, easy_request_t* req,
                              const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int ups_sql_scan(const int32_t version, common::ObDataBuffer& in_buff,
                         easy_request_t* req, const uint32_t channel_id,
                         common::ObDataBuffer& out_buff);
        int ups_ping(const int32_t version, easy_request_t* req, const uint32_t channel_id);
        int ups_new_get(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff,
            const int64_t start_time, const int64_t packet_timewait, const int32_t priority);
        int ups_new_scan(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff,
            const int64_t start_time, const int64_t packet_timewait, const int32_t priority);

        int ups_get(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff,
            const int64_t start_time, const int64_t packet_timewait, const int32_t priority);
        int ups_scan(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff,
            const int64_t start_time, const int64_t packet_timewait, const int32_t priority);
        int ups_preprocess(const int32_t version, const int32_t packet_code, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff,
            const int64_t start_time, const int64_t packet_timewait);
        int ups_slave_register(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int ups_slave_quit(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        //add zhaoqiong [fixed for Backup]:20150811:b
        int ups_backup_register(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e
        int ups_dump_text_memtable(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id);
        int ups_dump_text_schemas(const int32_t version,
            easy_request_t* req, const uint32_t channel_id);
        int ups_force_fetch_schema(const int32_t version,
            easy_request_t* req, const uint32_t channel_id);
        int ups_reload_conf(const int32_t version,
            easy_request_t* req, const uint32_t channel_id);
        int ups_renew_lease(const int32_t version, common::ObDataBuffer& in_buf,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int ups_grant_lease(const int32_t version, common::ObDataBuffer& in_buf,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int ups_change_vip(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id);
        int ups_memory_watch(const int32_t version,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int ups_memory_limit_set(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int ups_priv_queue_conf_set(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int ups_clear_active_memtable(const int32_t version,
            easy_request_t* req, const uint32_t channel_id);
        int ups_switch_commit_log(const int32_t version,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int ups_get_last_frozen_version(const int32_t version,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int ups_get_slave_info(const int32_t version,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add pangtianze [Paxos ups_replication] 20150604:b
        int ups_rs_get_max_log_seq_and_term(const int32_t version,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e
        //add pangtianze [Paxos rs_election] 20160919:b
        int ups_rs_get_ups_role(const int32_t version,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e
        //add pangtianze [Paxos rs_election] 20170228:b
        int ups_refresh_rs_list(const int32_t version, common::ObDataBuffer& in_buff,
                           easy_request_t* req, const uint32_t channel_id);
         //add:e
        int ups_rs_get_max_log_seq(const int32_t version,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int ups_get_table_time_stamp(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int ups_enable_memtable_checksum(const int32_t version, easy_request_t* req, const uint32_t channel_id);
        int ups_disable_memtable_checksum(const int32_t version, easy_request_t* req, const uint32_t channel_id);
        int ups_fetch_stat_info(const int32_t version,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int ups_get_schema(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add zhaoqiong [Truncate Table]:20160318:b
        int ups_check_incremental_data_range(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e
        //add zhaoqiong [Schema Manager] 20150327:b
        /**
         * @brief ups respond cs fetch schema next request
         */
        int ups_get_schema_next(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add:e
		//add wenghaixing [secondary index.cluster]20150630
        int ups_get_init_index(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        //add e
        int ups_get_sstable_range_list(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        int ups_start_transaction(const MemTableTransType type, UpsTableMgrTransHandle& handle);
        int ups_apply(const bool using_id, UpsTableMgrTransHandle& handle, common::ObDataBuffer& in_buff, common::ObScanner *scanner);
        int ups_end_transaction(common::ObPacket** packets, ScannerArray &scanner_array, const int64_t start_idx,
            const int64_t last_idx, UpsTableMgrTransHandle& handle, int32_t last_err_code);

        int ups_freeze_memtable(const int32_t version, common::ObPacket *packet_orig, common::ObDataBuffer& in_buff, const int pcode);
        int ups_switch_schema(const int32_t version, common::ObPacket *packet_orig, common::ObDataBuffer &in_buf);
        //add zhaoqiong [Schema Manager] 20150327:b
        /**
         * @brief ups receive schema mutator, apply mutator to local schema
         */
        int ups_switch_schema_mutator(const int32_t version, common::ObPacket *packet_orig, common::ObDataBuffer &in_buf);
        int ups_switch_tmp_schema();
        //add:e
        int ups_create_memtable_index();
        int ups_drop_memtable(const int32_t version, easy_request_t* req, const uint32_t channel_id);
        int ups_delay_drop_memtable(const int32_t version, easy_request_t* req, const uint32_t channel_id);
        int ups_immediately_drop_memtable(const int32_t version, easy_request_t* req, const uint32_t channel_id);
        int ups_drop_memtable();
        int ups_load_bypass(const int32_t version,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff, const int pcode);
        int ups_check_cur_version();
        int ups_commit_check_sstable_checksum(ObDataBuffer &buffer);
        int ups_get_bloomfilter(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int ups_store_memtable(const int32_t version, common::ObDataBuffer &in_buf,
            easy_request_t* req, const uint32_t channel_id);
        int ups_erase_sstable(const int32_t version, easy_request_t* req, const uint32_t channel_id);
        int ups_handle_frozen();
        int ups_load_new_store(const int32_t version, easy_request_t* req, const uint32_t channel_id);
        int ups_reload_all_store(const int32_t version, easy_request_t* req, const uint32_t channel_id);
        int ups_froce_report_frozen_version(const int32_t version, easy_request_t* req, const uint32_t channel_id);
        int ups_reload_store(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id);
        int ups_umount_store(const int32_t version, common::ObDataBuffer& in_buff,
            easy_request_t* req, const uint32_t channel_id);
        int ups_change_log_level(const int32_t version, common::ObDataBuffer& in_buff,
                                 easy_request_t* req, const uint32_t channel_id);
        int ups_stop_server(const int32_t version, common::ObDataBuffer& in_buff,
                                 easy_request_t* req, const uint32_t channel_id);
        int ob_malloc_stress(const int32_t version, common::ObDataBuffer& in_buff,
                             easy_request_t* req, const uint32_t channel_id);
        int ups_set_config(const int32_t version, common::ObDataBuffer& in_buff,
                           easy_request_t* req, const uint32_t channel_id);
        int ups_get_config(const int32_t version, easy_request_t* req,
                           const uint32_t channel_id, common::ObDataBuffer& out_buff);


        int ups_handle_fake_write_for_keep_alive();
        //int response_result_(int32_t ret_code, int32_t cmd_type, int32_t func_version,
                             //easy_request_t* req, const uint32_t channel_id, int64_t receive_ts = 0, const char *ret_string = NULL);
        int response_scanner_(int32_t ret_code, const common::ObScanner &scanner,
            int32_t cmd_type, int32_t func_version,
            easy_request_t* req, const uint32_t channel_id,
            common::ObDataBuffer& out_buff);
        int response_fetch_param_(int32_t ret_code, const ObUpsFetchParam& fetch_param,
            const int64_t log_id, int32_t cmd_type, int32_t func_version,
            easy_request_t* req, const uint32_t channel_id,
            common::ObDataBuffer& out_buff);
        int response_lease_(int32_t ret_code, const common::ObLease& lease,
            int32_t cmd_type, int32_t func_version,
            easy_request_t* req, const uint32_t channel_id,
            common::ObDataBuffer& out_buff);
        template <class T>
        int response_data_(int32_t ret_code, const T &data,
                          int32_t cmd_type, int32_t func_version,
                          easy_request_t* req, const uint32_t channel_id,
                           common::ObDataBuffer& out_buff, const int64_t receive_ts=0, const int32_t* priority = NULL, const char *ret_string = NULL);
        int low_priv_speed_control_(const int64_t scanner_size);

        template <class Queue>
        int submit_async_task_once_(ObSafeOnceGuard& guard, const PacketCode pcode, Queue& qthread, int32_t task_queue_size);
        template <class Queue>
        int submit_async_task_(const common::PacketCode pcode, Queue &qthread, int32_t &task_queue_size,
                              const common::ObDataBuffer *data_buffer = NULL,
                              const common::ObPacket *packet = NULL);
        template <class Queue>
        int submit_async_task_(const common::PacketCode pcode, Queue &qthread, int32_t task_queue_size,
            const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req,
            const uint32_t channel_id, const int64_t timeout);

        int report_frozen_version_();
        int replay_commit_log_();
        //add wangjiahao [Paxos ups_replication] 20150719 :b
        int replay_tmp_log_();
        //add :e
        int prefetch_remote_log_(ObDataBuffer& in_buf);
        int check_frozen_version();
        //mod zhaoqiong [Schema Manager] 20150520:b
        //int do_async_update_whole_schema();
        int do_async_update_whole_schema(bool force_fetch_schema = false);
        //mod:e
        //add zhaoqiong [Schema Manager] 20150327:b
        /**
         * @brief fetch schema mutator from system table,
         *      apply to local schema => get latest schema
         * @return
         */
        int do_async_update_schema();
        //add:e

        //add
        //add wangdonghui [ups_log_replication_optimize] 20161009 :b
        int ups_push_wait_flush(TransExecutor::Task* task);
        //add :e
        int slave_ups_receive_keep_alive(const int32_t version, easy_request_t* req, const uint32_t channel_id);
        int ups_clear_fatal_status(const int32_t version, easy_request_t* req, const uint32_t channel_id);
      public:
        int ui_deserialize_mutator(ObDataBuffer& buffer, ObMutator &mutator);
      private:
        static const int MY_VERSION = 1;
        static const int HEARTBEAT_VERSION = 2;
        static const int32_t RPC_BUFFER_SIZE = 1024*1024*2; //2MB
        static const int32_t DEFAULT_TASK_READ_QUEUE_SIZE = 1000;
        static const int32_t DEFAULT_TASK_WRITE_QUEUE_SIZE = 1000;
        static const int32_t DEFAULT_TASK_PREPROCESS_QUEUE_SIZE = 1000;
        static const int32_t DEFAULT_TASK_LEASE_QUEUE_SIZE = 100;
        static const int32_t DEFAULT_TASK_LOG_QUEUE_SIZE = 100;
        static const int32_t DEFAULT_STORE_THREAD_QUEUE_SIZE = 100;
        static const int64_t DEFAULT_REQUEST_TIMEOUT_RESERVE = 100 * 1000; // 100ms
        static const int64_t DEFAULT_NETWORK_TIMEOUT = 1000 * 1000;
        static const int64_t DEFAULT_USLEEP_INTERVAL = 10 * 1000;
        static const int64_t DEFAULT_GET_OBI_ROLE_INTERVAL = 2000 * 1000;
        static const int64_t LEASE_ON = 1;
        static const int64_t LEASE_OFF = 0;

        static const int64_t DEFAULT_SLAVE_QUIT_TIMEOUT = 1000 * 1000; // 1s
        static const int64_t DEFAULT_CHECK_LEASE_INTERVAL = 50 * 1000;
        static const int64_t CHECK_KEEP_ALIVE_PERIOD = 50 * 1000;
        static const int64_t GRANT_KEEP_ALIVE_PERIOD = 50 * 1000;

        static const int64_t DEFAULT_GET_OBJ_ROLE_INTEGER = 1000 * 1000;
        static const int64_t SEG_STAT_TIMES = 1000;
        static const int64_t SEG_STAT_SIZE = 100; // 100MB
        static const int64_t DEFAULT_CHANGE_MASTER_TIMEOUT = 3000 * 1000; //3s
        static const int64_t DEFAULT_LEASE_TIMEOUT_IN_ADVANCE = 100 * 1000; //100ms
        static const int64_t DEFAULT_WAIT_FOR_ROLE_TIMEOUT = 180 * 1000 * 1000; //180s
        static const int64_t DEFAULT_BUFFER_LENGTH = 512;
        static const int64_t DEFAULT_RESET_ASYNC_TASK_COUNT_TIMEOUT = 5 * 1000 * 1000;
      private:
        static const int64_t RPC_RETRY_TIMES = 3;             // rpc retry times used by client wrapper
        static const int64_t RPC_TIMEOUT = 2 * 1000L * 1000L; // rpc timeout used by client wrapper

      private:
        ObUpdateServerConfig &config_;
        common::ObConfigManager &config_mgr_;
        common::ObPacketFactory packet_factory_;
        common::ObClientManager client_manager_;
        ObUpsRpcStub ups_rpc_stub_;
        common::ThreadSpecificBuffer rpc_buffer_;
        common::ThreadSpecificBuffer my_thread_buffer_;

        //tbnet::PacketQueueThread read_thread_queue_; // for read task
        common::PriorityPacketQueueThread read_thread_queue_; // for read task
        //add lbzhong[Clog Monitor] 20151218:b
        common::ObPacketQueueThread clog_thread_queue_; // for clog status
        //add:e
        common::BatchPacketQueueThread write_thread_queue_; // for write task
        common::ObPacketQueueThread lease_thread_queue_; // for lease
        common::ObPacketQueueThread store_thread_; // store sstable
        ObUpsFetchRunnable fetch_thread_;
        ObUpsReplayRunnable log_replay_thread_;
        ObUpsWaitFlushRunnable wait_flush_thread_;//add wangdonghui [Ups_replication] 20161009 :b:e
        //common::ObCheckRunnable check_thread_;
        //ObUpsCheckRunnable check_thread_;
        int32_t read_task_queue_size_;
        int32_t write_task_queue_size_;
        int32_t lease_task_queue_size_;
        int32_t log_task_queue_size_;
        int32_t store_thread_queue_size_;
        int32_t preprocess_task_queue_size_;

        ObUpsCache ups_cache_;
        ObUpsLogMgr log_mgr_;
        ObUpsTableMgr table_mgr_;
        common::ObUserTable user_table_cache_;
        common::ObSKeyTable skey_table_cache_;
        common::ObPermTable perm_table_cache_;
        ObUpsRoleMgr role_mgr_;
        common::ObiRole settled_obi_role_;
        common::ObiRole obi_role_;
        ObiSlaveStat obi_slave_stat_;
        common::ObServer root_server_;
        common::ObServer ups_master_;
        common::ObServer self_addr_;
        common::ObServer ups_inst_master_;
        common::ObServer lsync_server_;
        ObUpsSlaveMgr slave_mgr_;
        mutable common::SpinRWLock server_lock_;
        ObUpsLogServerGetter ups_log_server_getter_;
        ObPrefetchLogTaskSubmitter prefetch_log_task_submitter_;
        ObReplayLogSrc replay_log_src_;
        UpsStatMgr stat_mgr_;
        SSTableMgr sstable_mgr_;
        ObTransferSSTableQuery sstable_query_;
        MajorFreezeDuty major_freeze_duty_; // 仅master调度
        HandleFrozenDuty handle_frozen_duty_;
        SwitchSKeyDuty switch_skey_duty_; // 仅master调度
        UpsWarmUpDuty warm_up_duty_;
        TimeUpdateDuty time_update_duty_;
        ObUpsCheckKeepAliveTask check_keep_alive_duty_;
        ObUpsGrantKeepAliveTask grant_keep_alive_duty_;
        KillZombieDuty kill_zombie_duty_;
        ObUpsLeaseTask ups_lease_task_;
        common::ObTimer timer_;
        common::ObTimer config_timer_;
        common::MsList ms_list_task_;
        //ObCommitLogReceiver clog_receiver_;
        //ObUpsFetchLsync fetch_lsync_;
        ObSafeOnceGuard grant_keep_alive_guard_;
        ObSafeOnceGuard check_keep_alive_guard_;
        ObSafeOnceGuard check_lease_guard_;

        int64_t start_trans_timestamp_;
        //bool is_first_log_;

        bool is_log_mgr_start_;
        tbsys::CThreadMutex mutex_;
        int64_t lease_timeout_in_advance_;
        int64_t lease_expire_time_us_;
        //int64_t last_keep_alive_time_;
        int64_t keep_alive_valid_interval_;
        int64_t ups_renew_reserved_us_;
        common::ObServer master_inst_root_server_;
        volatile int64_t schema_version_;
        volatile uint64_t schema_lock_;
        ObTriggerHandler trigger_handler_;
        TransExecutor trans_executor_;
        ObLogReplayWorker replay_worker_;
        ObAsyncLogApplier log_applier_;
        common::ObShadowServer& shadow_server_;
		
        //add zhaoqiong [Schema Manager] 20150327:b
        //used for ups get schema info from system table
        oceanbase::common::ObLocalMsList ms_;
        oceanbase::common::ObScanHelperImpl *scan_helper_;
        common::ObSchemaServiceImpl *schema_service_;
        int64_t fetch_schema_timestamp_;
        //add:e
        //add pangtianze [Paxos rs_election] 20150708:b
        common::ObRsAddressMgr rs_mgr_;
        common::ObSpinLock rs_lock_;
        //add:e
        //add chujiajia [Paxos rs_election] 20151217:b
        common::ObSpinLock change_quorum_scale_lock_;
        //add:e
		//add shili [LONG_TRANSACTION_LOG]  20160926:b
        ObBigLogWriter big_log_writer_;
        //add e
        //add wangdonghui [ups_replication] 20170315 :b
        int64_t switch_time_;//for test
        bool can_receive_log_;//when rs is electing master UPS. slave ups cannot receive log
        //add :e
    };
  }
}

#endif //__OB_UPDATE_SERVER_H__

