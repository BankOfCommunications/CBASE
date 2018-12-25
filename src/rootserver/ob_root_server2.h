/*===============================================================
 *   (C) 2007-2010 Taobao Inc.
 *
 *
 *   Version: 0.1 2010-09-26
 *
 *   Authors:
 *          daoan(daoan@taobao.com)
 *          maoqi(maoqi@taobao.com)
 *          xielun.szd(xielun@alipay.com)
 *
 *
 ================================================================*/
#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_SERVER2_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_SERVER2_H_
#include <tbsys.h>

#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_array.h"
#include "common/ob_string.h"
#include "common/ob_scan_param.h"
#include "common/ob_get_param.h"
#include "common/ob_obi_role.h"
#include "common/ob_ups_info.h"
#include "common/ob_schema_table.h"
#include "common/ob_schema_service.h"
#include "common/ob_table_id_name.h"
#include "common/ob_array.h"
#include "common/ob_list.h"
#include "common/ob_bypass_task_info.h"
#include "common/ob_timer.h"
#include "common/ob_tablet_info.h"
#include "common/ob_trigger_event.h"
#include "common/ob_trigger_msg.h"
#include "ob_root_server_state.h"
#include "common/roottable/ob_root_table_service.h"
#include "common/roottable/ob_first_tablet_entry_meta.h"
#include "common/roottable/ob_scan_helper_impl.h"
#include "common/ob_schema_service.h"
#include "common/ob_spin_lock.h"
#include "common/ob_strings.h"
#include "ob_chunk_server_manager.h"
#include "ob_root_table2.h"
#include "ob_root_log_worker.h"
#include "ob_root_async_task_queue.h"
#include "ob_ups_manager.h"
#include "ob_ups_heartbeat_runnable.h"
#include "ob_ups_check_runnable.h"
#include "ob_root_balancer.h"
#include "ob_root_balancer_runnable.h"
#include "ob_root_ddl_operator.h"
#include "ob_daily_merge_checker.h"
#include "ob_heartbeat_checker.h"
#include "ob_root_server_config.h"
#include "ob_root_ms_provider.h"
#include "ob_rs_after_restart_task.h"
//add huangjianwei [Paxos Cluster.Balance] 20160729:b
#include "ob_rs_get_server_status_task.h"
//add:e
#include "ob_schema_service_ms_provider.h"
#include "ob_schema_service_ups_provider.h"
#include "rootserver/ob_root_operation_helper.h"
#include "rootserver/ob_root_timer_task.h"
//add wenghaixing [secondary index col checksum.h]20141210
#include "common/ob_column_checksum.h"
//add e
//add wenghaixing [seocndary index static_index_build]20150318
#include "rootserver/ob_index_monitor.h"
//add e
//add liumz, [seocndary index static_index_build] 20150326:b
#include "common/ob_tablet_histogram_report_info.h"
//add:e
//add pangtianze [Paxos rs_election] 20150612:b
#include "ob_rs_heartbeat_runnable.h"
#include "ob_root_election_node_mgr.h"
#include "ob_root_election_checker.h"
//add:e
//add lbzhong [Paxos Cluster.Balance] 201607014:b
#include "ob_root_cluster_manager.h"
//add:e

class ObBalanceTest;
class ObBalanceTest_test_n_to_2_Test;
class ObBalanceTest_test_timeout_Test;
class ObBalanceTest_test_rereplication_Test;
class ObBalanceTest_test_n_to_2_with_faulty_dest_cs_Test;
class ObDeleteReplicasTest_delete_in_init_Test;
class ObDeleteReplicasTest_delete_when_rereplication_Test;
class ObDeleteReplicasTest_delete_when_report_Test;
class ObBalanceTest_test_shutdown_servers_Test;
class ObRootServerTest;
namespace oceanbase
{
  namespace common
  {
    class ObSchemaManagerV2;
    class ObRange;
    class ObTabletInfo;
    class ObTabletLocation;
    class ObScanner;
    class ObCellInfo;
    class ObTabletReportInfoList;
    class ObTableIdNameIterator;
    class ObConfigManager;
    struct TableSchema;
    //add liumz, [secondary index static_index_build] 20150324:b
    class ObTabletHistogramTable;
    class ObTabletHistogramManager;
    //add:e
  }
  namespace rootserver
  {
    class ObBootstrap;
    class ObRootTable2;
    class ObRootServerTester;
    class ObRootWorker;
    class ObRootServer2;
    // 参见《OceanBase自举流程》
    class ObBootState
    {
      public:
        enum State
        {
          OB_BOOT_NO_META = 0,
          OB_BOOT_OK = 1,
          OB_BOOT_STRAP = 2,
          OB_BOOT_RECOVER = 3,
        };
      public:
        ObBootState();
        bool is_boot_ok() const;
        void set_boot_ok();
        void set_boot_strap();
        void set_boot_recover();
        bool can_boot_strap() const;
        bool can_boot_recover() const;
        const char* to_cstring() const;
        ObBootState::State get_boot_state() const;
      private:
        mutable common::ObSpinLock lock_;
        State state_;
    };

    class ObRootServer2
    {
      public:
        friend class ObBootstrap;
        static const int64_t DEFAULT_SAFE_CS_NUMBER = 2;
        static const char* ROOT_TABLE_EXT;
        static const char* CHUNKSERVER_LIST_EXT;
        static const char* LOAD_DATA_EXT;
        static const char* SCHEMA_FILE_NAME;
        static const char* TMP_SCHEMA_LOCATION;
      public:
        ObRootServer2(ObRootServerConfig& config);
        virtual ~ObRootServer2();

        bool init(const int64_t now, ObRootWorker* worker);
        int start_master_rootserver();
        int init_first_meta();
        int init_boot_state();
        void start_threads();
        void stop_threads();
        void start_merge_check();
        //add liumz, [paxos static index]20170606:b
        int create_hist_managers(ObTabletHistogramManager** managers, const int64_t size);
        int create_hist_tables(ObTabletHistogramTable** tables, const int64_t size);
        int destroy_hist_managers(ObTabletHistogramManager** managers, const int64_t size);
        int destroy_hist_tables(ObTabletHistogramTable** tables, const int64_t size);
        //add:e

        //add pangtianze [Paxos rs_election] 20150624
        void start_rs_heartbeat();
        void start_election_checker();
        /**
         * @brief for rs_admin to set leader only when first starting
         * @param addr
         */
        int set_leader_when_starting();
        void set_rt_master(const ObServer &rs_master);
        //add:e
        //add wangdonghui [paxos daily merge] 20170510 :b
        void set_last_frozen_time(const int64_t last_frozen_time);
        //add :e
        //add jinty [Paxos Cluster.Balance] 20160708:b
        //to scan inner table for rs slave
        void init_schema_service_scan_helper(common::ObLocalMsList *ms_list_for_rs_slave);
        //add e
        //add chujiajia [Paoxs rs_election] 20151231:b
        inline void set_paxos_num(int64_t new_num)
        {
          common::ObSpinLockGuard guard(change_paxos_num_lock_);
          config_.rs_paxos_number = new_num;
        }
        inline void set_quorum_scale(int64_t new_num)
        {
          common::ObSpinLockGuard guard(change_quorum_scale_lock_);
          config_.ups_quorum_scale = new_num;
        }
        //add:e
        //add huangjianwei [Paxos rs_switch] 20160729:b
        inline common::ObSchemaService* get_schema_service()
        {
          return schema_service_;
        }
        //add:e
        //add pangtianze [Paxos] 20170419:b
        inline ObSchemaServiceMsProvider * get_schema_service_ms_provider()
        {
            return schema_service_ms_provider_;
        }
        //add:e
        //add pangtianze [Paxos rs_election] 20150619:b
        void stop_merge_check();
        int parse_string_to_ips(const char *str, ObRootElectionNode *other_rs);
        //add:e
        //add pangtianze [Paxos rs_election] 20150629:b
        int slave_init_first_meta(const ObTabletMetaTableRow &first_meta_row);
        int get_rs_master(common::ObServer &rs);
        //add:e
        //add pangtianze [Paxos rs_election] 20150701:b
        int force_leader_broadcast();
        bool check_most_rs_registered();
        int get_rs_leader(common::ObServer &rs);
        //add:e
        //add chujiajia [Paxos rs_election] 20151210:b
        int add_rootserver_register_info(const common::ObServer &rs);
        //add:e
        //add pangtianze [Paxos rs_election] 20150731:b
        int update_rs_array(const common::ObServer *array, const int32_t count);
        int set_election_priority(const common::ObServer target_rs, const int32_t priority);
        //add:e
        //add pangtianze [Paxos rs_election] 20150813:b
        ObRootWorker* get_worker();
        //add:e
        int delete_tablets(const common::ObTabletReportInfoList& to_delete_list);

        // oceanbase bootstrap
        int boot_strap();
        //add liuxiao [secondary index col checksum] 20150320
        int boot_strap_for_create_all_cchecksum_info();
        //add e
        //add liuxiao [secondary index col checksum] 20150316
        int clean_old_checksum(int64_t current_version);
        //mod liumz, [paxos static index]20170626:b
        //int check_column_checksum(const int64_t index_table_id, bool &is_right);
        //int get_cchecksum_info(const ObNewRange new_range,const int64_t required_version,ObString& cchecksum);
        int check_column_checksum(const int32_t cluster_id, const int64_t index_table_id, bool &is_right);
        int get_cchecksum_info(const ObNewRange new_range,const int64_t required_version,const int32_t cluster_id,ObString& cchecksum);
        //mod:e
        //add e
        //add liuxiao [secondary index 20150410] 20150410
        bool check_create_index_over();
        //add e
        //add liumz, [bugfix: cchecksum too large]20161109:b
        int clean_old_checksum_v2(int64_t current_version);
        //add:e
        int do_bootstrap(ObBootstrap & bootstrap);
        int boot_recover();
        ObBootState* get_boot();
        ObBootState::State get_boot_state() const;
        int slave_boot_strap();
        int start_notify_switch_schema();
        int notify_switch_schema(bool only_core_tables, bool force_update = false);
        //add zhaoqiong [Schema Manager] 20150327:b
        /**
         * @brief send schema_mutator to all ms and cs
         *        used after ddl operation
         */
        int notify_switch_schema(ObSchemaMutator &schema_mutator);
        //add:e
        void set_privilege_version(const int64_t privilege_version);
        // commit update inner table task
        void commit_task(const ObTaskType type, const common::ObRole role, const common::ObServer & server, int32_t sql_port,
                         const char* server_version, const int32_t cluster_role = 0);
        // for monitor info
        int64_t get_table_count(void) const;
        void get_tablet_info(int64_t & tablet_count, int64_t & row_count, int64_t & date_size) const;
        int change_table_id(const int64_t table_id, const int64_t new_table_id=0);
        int change_table_id(const ObString& table_name, const uint64_t new_table_id);
        
        // for bypass
        // start_import is called in master cluster by rs_admin, this method will call import on all clusters
        int start_import(const ObString& table_name, const uint64_t table_id, ObString& uri);
        int import(const ObString& table_name, const uint64_t table_id, ObString& uri, const int64_t start_time);
        // start_kill_import is called in master cluster by rs_admin
        int start_kill_import(const ObString& table_name, const uint64_t table_id);
        // used by master cluster to check slave cluster's import status
        int get_import_status(const ObString& table_name, const uint64_t table_id, ObLoadDataInfo::ObLoadDataStatus& status);
        int set_import_status(const ObString& table_name, const uint64_t table_id, const ObLoadDataInfo::ObLoadDataStatus status);

        bool is_loading_data() const;

        int write_schema_to_file();
        //mod zhaoqiong [Schema Manager] 20150327:b
        //rs should use value from __all_ddl_operation as local schema timestamp
//        int trigger_create_table(const uint64_t table_id = 0);
//        int trigger_drop_table(const uint64_t table_id);
//        int force_create_table(const uint64_t table_id);
//        int force_drop_table(const uint64_t table_id);
        int trigger_create_table(const uint64_t table_id, int64_t schema_version);
        int trigger_drop_table(const uint64_t table_id, int64_t schema_version);
        int force_create_table(const uint64_t table_id,int64_t schema_version);
        int force_drop_table(const uint64_t table_id,int64_t schema_version);
        //mod:e
        //add zhaoqiong [Schema Manager] 20150327:b
        int trigger_alter_table(const uint64_t table_id, int64_t schema_version);
        //add:e
        //add pangtianze [Paxos rs_election] 20150612:b
        int receive_rs_heartbeat(const common::ObMsgRsHeartbeat hb);
        int get_rs_term(int64_t &term);
        ObRootElectionNode::ObRsNodeRole get_rs_node_role();
        //add:e
        //add pangtianze [Paxos rs_election] 20170302:b
        bool is_in_vote_phase();
        bool is_in_broadcast_phase();
        //add:e
        //add pangtianze [Paxos rs_election] 20150619:b
        int switch_master_to_slave();
        //add:e
        int check_schema();
        /*
         * 从本地读取新schema, 判断兼容性
         */
        //int switch_schema(const int64_t time_stamp, common::ObArray<uint64_t> &deleted_tables);
        /*
         * 切换过程中, update server冻结内存表 或者chunk server 进行merge等耗时操作完成
         * 发送消息调用此函数
         */
        int waiting_job_done(const common::ObServer& server, const int64_t frozen_mem_version);
        /*
         * chunk server register
         * @param out status 0 do not start report 1 start report
         */
        int regist_chunk_server(const common::ObServer& server, const char* server_version, int32_t& status, int64_t timestamp = -1);
        /*
         * merge server register
         * @param out status 0 do not start report 1 start report
         */
        int regist_merge_server(const common::ObServer& server, const int32_t sql_port, const bool is_listen_ms,
            const char* server_version, int64_t timestamp = -1);
        /*
         * chunk server更新自己的磁盘情况信息
         */
        int update_capacity_info(const common::ObServer& server, const int64_t capacity, const int64_t used);
        /*
         * 迁移完成操作
         */
        const common::ObServer& get_self() { return my_addr_; }
       ObRootBalancer* get_balancer() { return balancer_; }

       virtual int migrate_over(const int32_t result, const ObDataSourceDesc& desc,
           const int64_t occupy_size, const uint64_t crc_sum, const uint64_t row_checksum, const int64_t row_count);
        //add zhaoqiong [Schema Manager] 20150327:b
        /**
          * @brief get schema_mutator from system table
          * @param [out] schema_mutator
          * @param [out] table_count
          */
        int get_schema_mutator(common::ObSchemaMutator& schema_mutator, int64_t & table_count);
        void set_schema_version(int64_t schema_version) {schema_timestamp_ = schema_version;}
        //add:e
        /// if (force_update = true && get_only_core_tables = false) then read new schema from inner table
        int get_schema(const bool froce_update, bool get_only_core_tables, common::ObSchemaManagerV2& out_schema);
        int64_t get_schema_version() const;
        //mod chujiajia[Paxos rs_election]20151012:b
        //const ObRootServerConfig& get_config() const;
        ObRootServerConfig& get_config() const;
        //mod:e
        //add wangdonghui [paxos daily merge] 20170510 :b
        int64_t get_last_frozen_time() const
        {
            return  last_frozen_time_;
        }
        //add :e
        ObConfigManager* get_config_mgr();
        int64_t get_privilege_version() const;
        int get_max_tablet_version(int64_t &version) const;
        int64_t get_config_version() const;
        int64_t get_alive_cs_number();
        common::ObSchemaManagerV2* get_ini_schema() const;
        ObRootRpcStub& get_rpc_stub();
        int fetch_mem_version(int64_t &mem_version);
        int create_empty_tablet(common::TableSchema &tschema, common::ObArray<common::ObServer> &cs);
        //add wenghaixing [secondary index static_index_build] 20150317
        int force_cs_create_index(uint64_t tid, int64_t hist_width, int64_t start_time);
        //add e

        int get_table_id_name(common::ObTableIdNameIterator *table_id_name, bool& only_core_tables);
        int get_table_schema(const uint64_t table_id, common::TableSchema &table_schema);
        int find_root_table_key(const uint64_t table_id, const common::ObString& table_name, const int32_t max_key_len,
            const common::ObRowkey& key, common::ObScanner& scanner
                                //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
                                , const int32_t cluster_id
                                //add:e
                                );

        int find_root_table_key(const common::ObGetParam& get_param, common::ObScanner& scanner
                                //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
                                , const int32_t cluster_id = 0
                                //add:e
                                );
        int find_monitor_table_key(const common::ObGetParam& get_param, common::ObScanner& scanner);
        int find_session_table_key(const common::ObGetParam& get_param, common::ObScanner& scanner);
        int find_statement_table_key(const common::ObGetParam& get_param, common::ObScanner& scanner);
        int find_root_table_range(const common::ObScanParam& scan_param, common::ObScanner& scanner);
        virtual int report_tablets(const common::ObServer& server, const common::ObTabletReportInfoList& tablets, const int64_t time_stamp);
        //add liumz, [secondary index static_index_build] 20150320:b
        virtual int report_histograms(const common::ObServer& server, const common::ObTabletHistogramReportInfoList & tablets, const int64_t time_stamp);
        int check_create_local_index_done(const uint64_t index_tid, bool &is_finished);
        int fill_all_samples();
        /*
         * split global index range and write it into root table
         */
        int write_global_index_range(const int64_t sample_num);
        int fill_tablet_info_list_by_server_index(ObIndexTabletInfoList *tablet_info_list[], const int32_t list_size, const ObTabletInfo &tablet_info, int32_t *server_index, const int32_t copy_count, const int32_t replica_order);
        int write_tablet_info_list_to_rt(ObIndexTabletInfoList **tablet_info_list, const int32_t list_size);
        int check_create_global_index_done(const uint64_t index_tid, bool &is_finished) const;
        int get_tablet_manager(ObTabletInfoManager *& tablet_manager_) const;
        int modify_index_stat(const uint64_t index_tid, const IndexStatus stat);
        int modify_index_stat(const ObArray<uint64_t> &index_tid_list, const IndexStatus stat);
        int trigger_balance_index(const uint64_t index_tid);
        //add:e
        //add liumz, [secondary index static_index_build] 20150630:b
        int check_index_init_changed(const uint64_t index_tid, bool &is_changed);
        //add:e
        //add wenghaixing [secondary index.cluster]20150629
        //mod liumz, [paxos static index]20170626:b
        //int modify_index_process_info(const uint64_t index_tid, const IndexStatus stat);
        int modify_index_process_info(const int32_t cluster_id, const uint64_t index_tid, const IndexStatus stat);
        //mod:e
        int fetch_init_index(const int64_t version, ObArray<uint64_t> *list);
        //add e
        //add wenghaixing [secondary index.cluster]20150629
        int fetch_index_stat(const int64_t &index_tid, const int64_t &cluster_id, int64_t &status);
        //add e
        //add liumz, [paxos static index]20170626:b
        int get_index_stat(const uint64_t &index_tid, const int64_t &cluster_num, IndexStatus &status);
        //add:e
        int receive_hb(const common::ObServer& server, const int32_t sql_port, const bool is_listen_ms, const common::ObRole role
                       //add pangtianze [Paxos rs_election] 20170420:b
                       ,const bool valid_flag = true
                       //add:e
                );
        common::ObServer get_update_server_info(bool use_inner_port) const;
        //add peiouya [Get_masterups_and_timestamp] 20141017:b
        int64_t get_ups_set_time(const common::ObServer &addr);
        //add 20141017:e
        int add_range_for_load_data(const common::ObList<common::ObNewRange*> &range);
        int load_data_fail(const uint64_t new_table_id);
        int get_table_id(const ObString table_name, uint64_t& table_id);
        int load_data_done(const ObString table_name, const uint64_t old_table_id);
        int get_master_ups(common::ObServer &ups_addr, bool use_inner_port);
        int table_exist_in_cs(const uint64_t table_id, bool &is_exist);
        int create_tablet(const common::ObTabletInfoList& tablets);
        uint64_t get_table_info(const common::ObString& table_name, int32_t& max_row_key_length) const;
        int get_table_info(const common::ObString& table_name, uint64_t& table_id, int32_t& max_row_key_length);

        int64_t get_time_stamp_changing() const;
        int64_t get_lease() const;
        int get_server_index(const common::ObServer& server) const;
        int get_cs_info(ObChunkServerManager* out_server_manager) const;
        // get task queue
        ObRootAsyncTaskQueue * get_task_queue(void);
        //mod chujiajia[Paxos rs_election] 20151019:b
        //const ObChunkServerManager &get_server_manager(void) const;
        ObChunkServerManager &get_server_manager(void);
        //mod:e
        //add chujiajia [Paxos rs_election] 20151110:b
        inline ObUpsManager *get_ups_manager()
        {
          return ups_manager_;
        }
        //add:e
        void clean_daily_merge_tablet_error();
        void set_daily_merge_tablet_error(const char* msg_buff, const int64_t len);
        char* get_daily_merge_error_msg();
        bool is_daily_merge_tablet_error() const;
        void print_alive_server() const;
        bool is_master() const;
        //add pangtianze [Paxos rs_election] 20170717:b
        bool is_slave() const;
        //add:e
        //add chujiajia [Paxos rs_election] 20151210:b
        common::ObRoleMgr::Role get_rs_role();
        //add:e
        common::ObFirstTabletEntryMeta* get_first_meta();
        //add liumz, [secondary index static_index_build] 20150601:b
        int get_rt_tablet_info(const int32_t meta_index, const ObTabletInfo *&tablet_info) const;
        //add:e
        //add liumz, [secondary index static_index_build] 20150320:b
        int get_root_meta(const common::ObTabletInfo &tablet_info, ObRootTable2::const_iterator &meta);
        int get_root_meta(const int32_t meta_index, ObRootTable2::const_iterator &meta);
        //add:e
        //add liumz, [secondary index static_index_build] 20150601:b
        int get_root_meta_index(const common::ObTabletInfo &tablet_info, int32_t &meta_index);
        //add:e
        void dump_root_table() const;
        //add liumz, [secondary index static_index_build] 20150601:b
        void dump_root_table(const int32_t index) const;
        //add:e
        bool check_root_table(const common::ObServer &expect_cs) const;
        int dump_cs_tablet_info(const common::ObServer & cs, int64_t &tablet_num) const;
        void dump_unusual_tablets() const;
        int check_tablet_version(const int64_t tablet_version, const int64_t safe_count, bool &is_merged) const;
        //add liumz, [secondary index static_index_build] 20150529:b
        int check_tablet_version_v3(const uint64_t table_id, const int64_t tablet_version, const int64_t safe_count, bool &is_merged) const;
        //add:e
        int use_new_schema();
        // dump current root table and chunkserver list into file
        int do_check_point(const uint64_t ckpt_id);
        // recover root table and chunkserver list from file
        int recover_from_check_point(const int server_status, const uint64_t ckpt_id);
        int receive_new_frozen_version(int64_t rt_version, const int64_t frozen_version,
            const int64_t last_frozen_time, bool did_replay);
        int report_frozen_memtable(const int64_t frozen_version, const int64_t last_frozen_time,bool did_replay);
        int get_last_frozen_version_from_ups(const bool did_replay);
        // 用于slave启动过程中的同步
        void wait_init_finished();
        const common::ObiRole& get_obi_role() const;
        int request_cs_report_tablet();
        int set_obi_role(const common::ObiRole& role);
        int get_master_ups_config(int32_t &master_master_ups_read_percent
                                  //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                                  //, int32_t &slave_master_ups_read_percent
                                  //del:e
                                  ) const;
        const common::ObUpsList &get_ups_list() const;
        int set_ups_list(const common::ObUpsList &ups_list);
        int do_stat(int stat_key, char *buf, const int64_t buf_len, int64_t& pos);
        int register_ups(const common::ObServer &addr, int32_t inner_port, int64_t log_seq_num, int64_t lease, const char *server_version_);
        //mod chujiajia [Paxos rs_election] 20151229:b
        //int receive_ups_heartbeat_resp(const common::ObServer &addr, ObUpsStatus stat, const common::ObiRole &obi_role);
        int receive_ups_heartbeat_resp(const common::ObServer &addr, ObUpsStatus stat, const common::ObiRole &obi_role, const int64_t &quorum_scale);
        //mod:e
        //add pangtianze [Paxos rs_election] 20150613:b
        ObRootElectionNodeMgr* get_rs_node_mgr() const;
        /**
         * @brief set current time after received such rs hb resp
         * @param [in] hb message
         * @param [in] received_time
         * @return
         */
        int receive_rs_heartbeat_resp(const common::ObMsgRsHeartbeatResp &hb_resp, const int64_t received_time);
        //add:e
        //add pangtianze [Paxos rs_election] 20161010:b
        int receive_change_paxos_num_request(const common::ObMsgRsChangePaxosNum change_paxos_num);
        int receive_new_quorum_scale_request(const ObMsgRsNewQuorumScale new_quorum_scale);
        bool check_all_rs_registered();
        //add:e
        //add pangtianze [Paxos rs_election] 20150618:b
        /**
         * @brief receive_rs_leader_broadcast
         * @param addr
         * @param lease
         * @param rs_term
         * @param is_force : if true, must follow broadcast sender rs
         * @return
         */
        int receive_rs_leader_broadcast(const common::ObMsgRsLeaderBroadcast lb,
                                        common::ObMsgRsLeaderBroadcastResp &lb_resq,
                                        const bool is_force);
        int receive_rs_vote_request(const ObMsgRsRequestVote rv, ObMsgRsRequestVoteResp &rv_resp);
        //add:e
        //add pangtianze [Paxos rs_election] 20150817:b
        int receive_rs_vote_request_resp(const common::ObMsgRsRequestVoteResp &rv_resp);
        int receive_rs_leader_broadcast_resp(const common::ObMsgRsLeaderBroadcastResp &lb_resp);
        int send_rs_vote_request();
        int send_rs_leader_broadcast();
        //add:e
        //add pangtianze [Paxos rs_election] 20170228:b
        ///refresh rootserver list in ups/cs/ms nsync
        int sync_refresh_rs_list();
        int refresh_list_to_all();
        int refresh_list_to_ms_and_cs(ObServer *servers, const int32_t server_count);
        //add:e
        int ups_slave_failure(const common::ObServer &addr, const common::ObServer &slave_addr);
        int get_ups_list(common::ObUpsList &ups_list
                         //add lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                         , const int32_t cluster_id = 0
                         //add:e
                         );
        int set_ups_config(const common::ObServer &ups, int32_t ms_read_percentage, int32_t cs_read_percentage);
        int set_ups_config(int32_t read_master_master_ups_percentage
                           //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                           //, int32_t read_slave_master_ups_percentage
                           //del:e
                           //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
                           ,const int32_t is_strong_consistent = 0
                           //add:e
                           );
        int change_ups_master(const common::ObServer &ups, bool did_force);
        int serialize_cs_list(char* buf, const int64_t buf_len, int64_t& pos) const;
        int serialize_ms_list(char* buf, const int64_t buf_len, int64_t& pos
                              //add lbzhong [Paxos Cluster.Flow.MS] 201607027:b
                              , const int32_t cluster_id = 0
                              //add:e
                              ) const;
        int serialize_proxy_list(char* buf, const int64_t buf_len, int64_t& pos) const;
        int grant_eternal_ups_lease();
        int cs_import_tablets(const uint64_t table_id, const int64_t tablet_version);
        /// force refresh the new schmea manager through inner table scan
        int refresh_new_schema(int64_t & table_count, int64_t version = 0);
        //add zhaoqiong [Schema Manager] 20150327:b
        /**
         * @brief refresh schema after trigger ddl operation
         * @param [out] table_count
         * @param [out] schema_mutator
         * @return
         */
        int refresh_new_schema(int64_t & table_count, ObSchemaMutator &schema_mutator);
        //add:e
        int switch_ini_schema();
        //mod zhaoqiong [Schema Manager] 20150327:b
        //int renew_user_schema(int64_t & table_count);
        int renew_user_schema(int64_t & table_count, int64_t version = 0);
        //mod:e
        int renew_core_schema(void);
        void dump_schema_manager();
        void dump_migrate_info() const; // for monitor
        int shutdown_cs(const common::ObArray<common::ObServer> &servers, enum ShutdownOperation op);
        void restart_all_cs();
        void cancel_restart_all_cs();
        int cancel_shutdown_cs(const common::ObArray<common::ObServer> &servers, enum ShutdownOperation op);
        void reset_hb_time();
        int remove_replica(const bool did_replay, const common::ObTabletReportInfo &replica);
        int delete_tables(const bool did_replay, const common::ObArray<uint64_t> &deleted_tables);
        int delete_replicas(const bool did_replay, const common::ObServer & cs, const common::ObTabletReportInfoList & replicas);
        int create_table(const bool if_not_exists, const common::TableSchema &tschema);
        int alter_table(common::AlterTableSchema &tschema);
        int drop_tables(const bool if_exists, const common::ObStrings &tables);
        //add zhaoqiong [Truncate Table]:20160318:b
        int truncate_tables(const bool if_exists, const common::ObStrings &tables, const common::ObString &user, const common::ObString & comment);
        int truncate_one_table( const common::ObMutator & mutator);
        //add:e
        //add wenghaixing [secondary index drop index]20141223
        int drop_index(const common::ObStrings &tables);
        //add e
        //add wenghaixing [secondary index drop table_with_index]20150121
        inline ObSchemaManagerV2* get_local_schema()
        {
          return schema_manager_for_cache_;
        }
        //add e
        //add wenghaixing [secondary index static_index_build]20150528
        void reset_index_beat();
        IndexBeat get_index_beat();
        //add e
        //add liumz, [secondary index version management]20160413:b
        int64_t get_build_index_version();
        bool is_last_finished();
        //add:e
        //add liumz, [secondary index static_index_build] 20150320:b
        int reuse_hist_table(const uint64_t index_tid);
        int clean_hist_table();
        //add:e
        int64_t get_last_frozen_version() const;

        //for bypass process begin
        ObRootOperationHelper* get_bypass_operation();
        bool is_bypass_process();
        int set_bypass_flag(const bool flag);
        int get_new_table_id(uint64_t &max_table_id, ObBypassTaskInfo &table_name_id);
        int clean_root_table();
        int slave_clean_root_table();
        void set_bypass_version(const int64_t version);
        int unlock_frozen_version();
        int lock_frozen_version();
        int prepare_bypass_process(common::ObBypassTaskInfo &table_name_id);
        virtual int start_bypass_process(common::ObBypassTaskInfo &table_name_id);
        int check_bypass_process();
        const char* get_bypass_state()const;
        int cs_delete_table_done(const common::ObServer &cs,
            const uint64_t table_id, const bool is_succ);
        int delete_table_for_bypass();
        int cs_load_sstable_done(const common::ObServer &cs,
            const common::ObTableImportInfoList &table_list, const bool is_load_succ);
        virtual int bypass_meta_data_finished(const OperationType type, ObRootTable2 *root_table,
            ObTabletInfoManager *tablet_manager, common::ObSchemaManagerV2 *schema_mgr);
        int use_new_root_table(ObRootTable2 *root_table, ObTabletInfoManager *tablet_manager);
        int switch_bypass_schema(common::ObSchemaManagerV2 *schema_mgr, common::ObArray<uint64_t> &delete_tables);
        int64_t get_frozen_version_for_cs_heartbeat() const;
        //for bypass process end
        /// check the table exist according the local schema manager
        int check_table_exist(const common::ObString & table_name, bool & exist);
        int delete_dropped_tables(int64_t & table_count);
        void after_switch_to_master();
        int after_restart();
        int make_checkpointing();
        ObRootMsProvider & get_ms_provider() { return ms_provider_; }

        //table_id is OB_INVALID_ID , get all table's row_checksum
        int get_row_checksum(const int64_t tablet_version, const uint64_t table_id, ObRowChecksum &row_checksum);
        //add lbzhong [Paxos Cluster.Balance] 20160708:b
        void get_alive_cluster(bool *is_cluster_alive);
        bool is_cluster_alive(const int32_t cluster_id);
        bool is_cluster_alive_with_cs(const int32_t cluster_id);
        int set_master_cluster_id(const int32_t cluster_id);
        int32_t get_master_cluster_id() const;
        int get_cluster_tablet_replicas_num(int32_t *replicas_num) const;
        int load_cluster_replicas_num();
        void dump_balancer_info(const int64_t table_id);
        //add:e
        //add bingo [Paxos Cluster.Balance] 20161024:b
        void print_master_cluster_id(char *buf, const int64_t buf_len, int64_t& pos);
        //add:e
        //add bingo [Paxos Cluster.Balance] 20161118:b
        void get_alive_cluster_with_cs(bool *is_cluster_alive_with_cs);
        //add:e
        //add liumz, [paxos static index]20170626:b
        void get_alive_cluster_with_cs(bool *is_cluster_alive_with_cs) const;
        int get_alive_cluster_num_with_cs();
        //add:e
        //add bingo [Paxos Cluster.Balance] 20170407:b
        void get_alive_cluster_with_ms_and_cs(bool *is_cluster_alive_with_ms_cs);
        //add:e
        //add bingo [Paxos set_boot_ok] 20170315:b
        int set_boot_ok();
        //add:e

        friend class ObRootServerTester;
        friend class ObRootLogWorker;
        friend class ObDailyMergeChecker;
        friend class ObHeartbeatChecker;
        friend class ::ObBalanceTest;
        friend class ::ObBalanceTest_test_n_to_2_Test;
        friend class ::ObBalanceTest_test_timeout_Test;
        friend class ::ObBalanceTest_test_rereplication_Test;
        friend class ::ObBalanceTest_test_n_to_2_with_faulty_dest_cs_Test;
        friend class ::ObDeleteReplicasTest_delete_in_init_Test;
        friend class ::ObDeleteReplicasTest_delete_when_rereplication_Test;
        friend class ::ObDeleteReplicasTest_delete_when_report_Test;
        friend class ::ObBalanceTest_test_shutdown_servers_Test;
        friend class ::ObRootServerTest;
        friend class ObRootReloadConfig;
        //add huangjianwei [Paxos rs_switch] 20160728:b
        friend class ObRootInnerTableTask;
        //add:e
      private:
        bool async_task_queue_empty()
        {
          return seq_task_queue_.size() == 0;
        }
        int after_boot_strap(ObBootstrap & bootstrap);

        /*  add liumz, [secondary index static_index_build] 20150320:b
         *  æ”¶åˆ°ç»Ÿè®¡ä¿¡æ¯æ±‡æŠ¥åŽè°ƒç”?
         */
        //mod liumz, [paxos static index]20170626:b
        //int got_reported_for_histograms(const common::ObTabletHistogramReportInfoList& tablets, const int server_index,
        //    const int64_t frozen_mem_version, const bool for_bypass = false, const bool is_replay_log = false);
        int got_reported_for_histograms(const common::ObTabletHistogramReportInfoList& tablets,const int32_t cluster_id, const int server_index,
            const int64_t frozen_mem_version, const bool for_bypass = false, const bool is_replay_log = false);
        //mod:e
        //add:e

        /*
         * 收到汇报消息后调用
         */
        int got_reported(const common::ObTabletReportInfoList& tablets, const int server_index,
            const int64_t frozen_mem_version, const bool for_bypass = false, const bool is_replay_log = false);
        /*
         * 旁路导入时，创建新range的时候使用
         */
        int add_range_to_root_table(const ObTabletReportInfoList &tablets, const bool is_replay_log = false);
        /*
         * 处理汇报消息, 直接写到当前的root table中
         * 如果发现汇报消息中有对当前root table的tablet的分裂或者合并
         * 要调用采用写拷贝机制的处理函数
         */
        int got_reported_for_query_table(const common::ObTabletReportInfoList& tablets,
            const int32_t server_index, const int64_t frozen_mem_version, const bool for_bypass = false);
        /*
         * 写拷贝机制的,处理汇报消息
         */
        int got_reported_with_copy(const common::ObTabletReportInfoList& tablets,
            const int32_t server_index, const int64_t have_done_index, const bool for_bypass = false);

        int create_new_table(const bool did_replay, const common::ObTabletInfo& tablet,
            const common::ObArray<int32_t> &chunkservers, const int64_t mem_version);
        int slave_batch_create_new_table(const common::ObTabletInfoList& tablets,
            int32_t** t_server_index, int32_t* replicas_num, const int64_t mem_version);
        void get_available_servers_for_new_table(int* server_index, int32_t expected_num, int32_t &results_num);
        int get_deleted_tables(const common::ObSchemaManagerV2 &old_schema,
            const common::ObSchemaManagerV2 &new_schema, common::ObArray<uint64_t> &deleted_tables);
        int make_out_cell_all_server(ObCellInfo& out_cell, ObScanner& scanner,
            const int32_t max_row_count) const;
        //add zhaoqiong [Schema Manager] 20150520:b
        //add refresh record to __all_ddl_operation, before refresh or after rs restart
        int renew_schema_version();
        //add :e


        /*
         * 生成查询的输出cell
         */
        int make_out_cell(common::ObCellInfo& out_cell, ObRootTable2::const_iterator start,
            ObRootTable2::const_iterator end, common::ObScanner& scanner, const int32_t max_row_count,
            const int32_t max_key_len
            //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
            , const int32_t cluster_id = 0
            //add:e
            ) const;

        // stat related functions
        void do_stat_start_time(char *buf, const int64_t buf_len, int64_t& pos);
        void do_stat_local_time(char *buf, const int64_t buf_len, int64_t& pos);
        void do_stat_common(char *buf, const int64_t buf_len, int64_t& pos);
        void do_stat_schema_version(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_mem(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_table_num(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_tablet_num(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_cs(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_ms(char* buf, const int64_t buf_len, int64_t &pos);
        //add bingo [Paxos rs_election] 20161012
        void do_stat_rs(char* buf, const int64_t buf_len, int64_t &pos);
        //add:e
        void do_stat_ups(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_all_server(char* buf, const int64_t buf_len, int64_t &pos);
        //add bingo [Paxos rs_admin all_server_in_clusters] 20170612:b
        void do_stat_all_server_in_clusters(char* buf, const int64_t buf_len, int64_t &pos);
        //add:e
        //add pangtianze [Paxos rs_election] 20150731:b
        void do_stat_rs_leader(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_paxos_changed(char* buf, const int64_t buf_len, int64_t &pos);
        //add:e
        //add chujiajia [Paxos rs_election] 20151230:b
        void do_stat_quorum_changed(char* buf, const int64_t buf_len, int64_t &pos);
        //add:e
        //add jintianyang [paxos test] 20160530:b
        void do_stat_ups_leader(char* buf, const int64_t buf_len, int64_t &pos);
        //add:e
        //add bingo [Paxos Cluster.Balance] 20161024:b
        void do_stat_master_cluster_id(char* buf, const int64_t buf_len, int64_t &pos);
        //add:e
        void do_stat_frozen_time(char* buf, const int64_t buf_len, int64_t &pos);
        int64_t get_stat_value(const int32_t index);
        void do_stat_cs_num(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_ms_num(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_merge(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_unusual_tablets_num(char* buf, const int64_t buf_len, int64_t &pos);

        void switch_root_table(ObRootTable2 *rt, ObTabletInfoManager *ti);
        int switch_schema_manager(const common::ObSchemaManagerV2 & schema_manager);
        /*
         * 在一个tabelt的各份拷贝中, 寻找合适的备份替换掉
         */
        int write_new_info_to_root_table(
            const common::ObTabletInfo& tablet_info, const int64_t tablet_version, const int32_t server_index,
            ObRootTable2::const_iterator& first, ObRootTable2::const_iterator& last, ObRootTable2 *p_root_table);

        //add liumz, [secondary index static_index_build] 20150629:b
        int modify_init_index();
        int modify_index_stat_amd();//after merge done
        //add:e
        //add liumz, [secondary index static_index_build] 20150529:b
        int modify_staging_index();
        //add:e
        //add liumz, [secondary index static_index_build] 20151208:b
        int modify_staging_index_process_info();
        //add:e
        bool check_all_tablet_safe_merged(void) const;
        int create_root_table_for_build();
        DISALLOW_COPY_AND_ASSIGN(ObRootServer2);
        int get_rowkey_info(const uint64_t table_id, common::ObRowkeyInfo &info) const;
        int select_cs(const int64_t select_num, common::ObArray<std::pair<common::ObServer, int32_t> > &chunkservers);
      private:
        int try_create_new_tables(int64_t fetch_version);
        int try_create_new_table(int64_t frozen_version, const uint64_t table_id);
        int check_tablets_legality(const common::ObTabletInfoList &tablets);
        int split_table_range(const int64_t frozen_version, const uint64_t table_id,
            common::ObTabletInfoList &tablets);
        int create_table_tablets(const uint64_t table_id, const common::ObTabletInfoList & list);
        int create_tablet_with_range(const int64_t frozen_version,
            const common::ObTabletInfoList& tablets);
        int create_empty_tablet_with_range(const int64_t frozen_version,
            ObRootTable2 *root_table, const common::ObTabletInfo &tablet,
            int32_t& created_count, int* t_server_index);
        //add wenghaixing [secondary index drop index]20141223
        int drop_one_index(const common::ObString &table_name,bool &refresh);
        //add e
        /// for create and delete table xielun.szd
        int drop_one_table(const bool if_exists, const common::ObString & table_name, bool & refresh);
        /// force sync schema to all servers include ms\cs\master ups
        int force_sync_schema_all_servers(const common::ObSchemaManagerV2 &schema);
        //add zhaoqiong [Schema Manager] 20150327:b
        /**
         * @brief send schema_mutator to all ms and cs
         *        used after ddl operation
         */
        int force_sync_schema_mutator_all_servers(const common::ObSchemaMutator &schema_mutator);
        //add:e
        int force_heartbeat_all_servers(void);
        int get_ms(common::ObServer& ms_server);
        //add wenghaixing [secondary index static_index_build]20150528

        void set_index_beat(uint64_t index_tid, IndexStatus status, int64_t hist_width);
        //add e
      private:
        static const int MIN_BALANCE_TOLERANCE = 1;
        static const int MAX_HIST_TABLE_NUM = OB_MAX_COPY_COUNT;//add liumz, [paxos static index]20170626

        common::ObClientHelper client_helper_;
        ObRootServerConfig &config_;
        ObRootWorker* worker_; //who does the net job
        ObRootLogWorker* log_worker_;

        // cs & ms manager
        ObChunkServerManager server_manager_;
        mutable tbsys::CRWLock server_manager_rwlock_;

        mutable tbsys::CThreadMutex root_table_build_mutex_; //any time only one thread can modify root_table
        ObRootTable2* root_table_;
        ObTabletInfoManager* tablet_manager_;
        //add liumz, [secondary index static_index_build] 20150320:b
        //common::ObTabletHistogramTable* hist_table_;//store data table's tablet meta info and index table's histogram meta info
        //common::ObTabletHistogramManager* hist_manager_;//store cs reported histograms when building local index
        common::ObTabletHistogramTable* hist_table_[MAX_HIST_TABLE_NUM];//store data table's tablet meta info and index table's histogram meta info
        common::ObTabletHistogramManager* hist_manager_[MAX_HIST_TABLE_NUM];//store cs reported histograms when building local index
        mutable tbsys::CThreadMutex hist_table_mutex_;
        //add:e
        mutable tbsys::CRWLock root_table_rwlock_; //every query root table should rlock this
        common::ObTabletReportInfoList delete_list_;
        bool have_inited_;
        bool first_cs_had_registed_;
        volatile bool receive_stop_;

        mutable tbsys::CThreadMutex frozen_version_mutex_;
        int64_t last_frozen_mem_version_;
        int64_t last_frozen_time_;
        int64_t next_select_cs_index_;
        int64_t time_stamp_changing_;

        common::ObiRole obi_role_;        // my role as oceanbase instance
        common::ObServer my_addr_;

        time_t start_time_;
        // ups related
        ObUpsManager *ups_manager_;
        ObUpsHeartbeatRunnable *ups_heartbeat_thread_;
        ObUpsCheckRunnable *ups_check_thread_;
        //add pangtianze [Paxos rs_election] 20150612:b
        ObRootElectionNodeMgr *rs_node_mgr_;
        ObRsHeartbeatRunnable *rs_heartbeat_thread_;
        ObRsCheckRunnable *rs_election_checker_;
        //add:e
        //add chujiajia [Paxos rs_election] 20151225:b
        int32_t count_rs_receive_quorum_scale_;
        common::ObSpinLock change_quorum_scale_lock_;
        common::ObSpinLock change_paxos_num_lock_;
        //add:e
        // balance related
        ObRootBalancer *balancer_;
        ObRootBalancerRunnable *balancer_thread_;
        ObRestartServer *restart_server_;
        // schema service
        int64_t schema_timestamp_;
        int64_t privilege_timestamp_;
        common::ObSchemaService *schema_service_;
        common::ObScanHelperImpl *schema_service_scan_helper_;
        ObSchemaServiceMsProvider *schema_service_ms_provider_;
        ObSchemaServiceUpsProvider *schema_service_ups_provider_;
        // new root table service
        mutable tbsys::CThreadMutex rt_service_wmutex_;
        common::ObFirstTabletEntryMeta *first_meta_;
        common::ObRootTableService *rt_service_;
        // sequence async task queue
        ObRootAsyncTaskQueue seq_task_queue_;
        ObDailyMergeChecker merge_checker_;
        ObHeartbeatChecker heart_beat_checker_;
        // trigger tools
        ObRootMsProvider ms_provider_;
        common::ObTriggerEvent root_trigger_;
        // ddl operator
        tbsys::CThreadMutex mutex_lock_;
        ObRootDDLOperator ddl_tool_;
        ObBootState boot_state_;
        //to load local schema.ini file, only use the first_time
        common::ObSchemaManagerV2* local_schema_manager_;
        //used for cache
        common::ObSchemaManagerV2 * schema_manager_for_cache_;
        mutable tbsys::CRWLock schema_manager_rwlock_;

        ObRootServerState state_;  //RS state
        int64_t bypass_process_frozen_men_version_; //æ—è·¯å¯¼å…¥çŠ¶æ?ä¸‹ï¼Œå¯ä»¥å¹¿æ’­çš„frozen_version
        ObRootOperationHelper operation_helper_;
        ObRootOperationDuty operation_duty_;
        common::ObTimer timer_;
        //add wenghaixing [secondary index static_index_build]20150528
        //to change index monitor by heartbeat with cs,we add index_tid to heart beat,signal cs to build index
        IndexBeat index_beat_;
        mutable tbsys::CRWLock index_beat_rwlock_;
        //add e        
        //add lbzhong [Paxos Cluster.Balance] 201607014:b
        ObRootClusterManager cluster_mgr_;
        mutable tbsys::CRWLock cluster_mgr_rwlock_;
        //add:e
    };
  }
}

#endif
