/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *
 *  Version: 1.0 : 2010/08/22
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 */
#ifndef OCEANBASE_CHUNKSERVER_CHUNKSERVICE_H_
#define OCEANBASE_CHUNKSERVER_CHUNKSERVICE_H_

#include "common/ob_define.h"
#include "common/thread_buffer.h"
#include "common/ob_timer.h"
#include "ob_schema_task.h"
#include "easy_io.h"
#include "common/ob_cur_time.h"
#include "common/ob_ms_list.h"
#include "sql/ob_sql_read_param.h"
//add zhaoqiong [Schema Manager] 20150327:b
#include "common/ob_common_rpc_stub.h"
#include "common/roottable/ob_scan_helper_impl.h"
#include "common/ob_schema_service_impl.h"
//add:e
//add wenghaixing [secondary index static_index_build] 20150318
#include "common/ob_schema_service.h"
//add e
//add pangtianze [Paxos rs_election] 20150708:b
#include "common/ob_rs_address_mgr.h"
//add:e
#ifdef GTEST
#define private public
#define protected public
#endif

namespace oceanbase
{
  namespace common
  {
    class ObDataBuffer;
    class ObSchemaManager;
    class ObSchemaManagerV2;
    class ObMergerSchemaManager;
  }
  namespace chunkserver
  {
    class ObTablet;
    class ObChunkServer;
    class ObQueryService;
    class ObSqlQueryService;
    //add huangjianwei [Paxos rs_election] 20160517:b
    static const int64_t RETRY_INTERVAL_TIME = 100 * 1000; // usleep 100 ms
    //add:e

    class ObChunkService
    {
      public:
        ObChunkService();
        ~ObChunkService();
      public:
        int initialize(ObChunkServer* chunk_server);
        int start();
        int destroy();
        int schedule_report_tablet();

      public:
        int do_request(
            const int64_t receive_time,
            const int32_t packet_code,
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_time = 0);
      private:
        // warning: fetch schema interval can not be too long
        // because of the heartbeat handle will block tbnet thread
        static const int64_t FETCH_SCHEMA_INTERVAL = 30 * 1000;

        int cs_get(
            const int64_t start_time, 
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_time);

        int cs_batch_get(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_tablet_read(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_time);

        int cs_fetch_data(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_sql_scan(
            const int64_t start_time,
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_time);

        int cs_sql_get(
            const int64_t start_time,
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_time);

        int cs_sql_read(
            const int64_t start_time,
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_time,
            sql::ObSqlReadParam *sql_read_param_ptr);

        int cs_scan(
            const int64_t start_time, 
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_time);

        int cs_drop_old_tablets(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);
         //add wenghaixing [secondary index static_index_build] 20150318
        int cs_static_index(
                const int32_t version,
                const int32_t channel_id,
                easy_request_t* req,
                common::ObDataBuffer& in_buffer,
                common::ObDataBuffer& out_buffer);
        int gen_index_table_list(int64_t& count,uint64_t* list);
        int gen_static_index(int64_t count,uint64_t* list);
        //add e
        //add wenghaixing [secondary index static_index_build.exceptional_handle] 20150409
        int cs_recieve_wok(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);
        //add e
        //add wenghaixing [secondary index static_index_build.heartbeat]20150528
        int handle_index_beat(IndexBeat beat);
        //add e
        //add liumz, [secondary index version management]20160413:b
        int handle_index_beat_v2(IndexBeat beat, int64_t frozen_version, int64_t build_index_version, bool is_last_finished);
        //add:e
        //add pangtianze [Paxos rs_switch] 20170208:b
        int cs_force_regist(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);
        //add:e
        //add pangtianze [Paxos rs_election] 20170301:b
        int cs_refresh_rs_list(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);
        //add:e
        int cs_heart_beat(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_create_tablet(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_accept_schema(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);
        //add zhaoqiong [Schema Manager] 20150327:b

        /**
         * @brief cs accept schema_mutator from RS, apply mutator
         * @param version: mutator version on schema
         */
        int cs_accept_schema_mutator(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);
        //add:e

        int cs_load_tablet(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_delete_tablets(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_get_migrate_dest_loc(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_dump_tablet_image(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_fetch_stats(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_start_gc(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_reload_conf(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_show_param(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_stop_server(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_force_to_report_tablet(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);
        int cs_change_log_level(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_check_tablet(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_merge_tablets(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_send_file(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_sync_all_images(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_load_bypass_sstables(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_delete_table(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        //add liumz, [secondary index delete global index sstable]20160621:b
        int cs_delete_global_index(uint64_t table_id);
        //add:e

        int cs_fetch_sstable_dist(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_set_config(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_get_config(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_get_bloom_filter(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_time);

  
        int cs_install_disk(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_uninstall_disk(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_show_disk(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);


      private:
        class LeaseChecker : public common::ObTimerTask
        {
          public:
            LeaseChecker(ObChunkService* service) : service_(service) {}
            ~LeaseChecker() {}
          public:
            virtual void runTimerTask();
          private:
            ObChunkService* service_;
        };

        class DiskChecker : public common::ObTimerTask
        {
          public:
            DiskChecker(ObChunkService* service) : service_(service) {}
          public:
            virtual void runTimerTask();
          private:
            ObChunkService* service_;
        };

        class StatUpdater : public common::ObTimerTask
        {
          public:
            StatUpdater () : pre_request_count_(0),current_request_count_(0), run_count_(0) {}
            ~StatUpdater() {}
          public:
            virtual void runTimerTask();
          private:
            int64_t pre_request_count_;
            int64_t current_request_count_;
            int64_t run_count_;
        };

        class MergeTask : public common::ObTimerTask
        {
          public:
            MergeTask (ObChunkService* service)
              : frozen_version_(0), task_scheduled_(false), service_(service)  {}
          public:
            inline int64_t get_last_frozen_version() const { return frozen_version_; }
            void set_frozen_version(int64_t version) { frozen_version_ = version; }
            inline bool is_scheduled() const { return task_scheduled_; }
            inline void set_scheduled() { task_scheduled_ = true; }
            inline void unset_scheduled() { task_scheduled_ = false; }
            virtual void runTimerTask();
          private:
            int64_t frozen_version_;
            bool task_scheduled_;
            ObChunkService* service_;
        };

        //add wenghaixing [secondary index static_index_build]20150302
        class IndexTask : public common::ObTimerTask
        {
          public:
            IndexTask(ObChunkService* service)
              :task_scheduled_(false),service_(service){}
          public:
            inline bool is_scheduled() const { return task_scheduled_;}
            inline void set_scheduled() { task_scheduled_ = true;}
            inline void unset_scheduled() { task_scheduled_ = false;}
            int set_schedule_idx_tid(uint64_t table_id);
            int set_schedule_time(uint64_t tid, int64_t start_time);
            void set_hist_width(int64_t hist_width);
            bool get_round_end();
            uint64_t get_schedule_idx_tid();
            //add wenghaixing [secondary index static_index_build.stop_mission]20150728
            void try_stop_mission(uint64_t index_tid);
            //add e
            virtual void runTimerTask();
          private:
            bool task_scheduled_;
            ObChunkService* service_;
        };

        class IndexCheckTask :public common::ObTimerTask
        {
          public:
            IndexCheckTask(ObChunkService* service)
              :task_scheduled_(false),service_(service){}
          public:
            inline bool is_scheduled()const {return task_scheduled_;}
            inline void set_scheduled() { task_scheduled_ = true;}
            inline void unset_scheduled() { task_scheduled_ = false;}
            virtual void runTimerTask();
          private:
            bool task_scheduled_;
            ObChunkService* service_;

        };
        //add e

        class FetchUpsTask : public common::ObTimerTask
        {
          public:
            FetchUpsTask (ObChunkService* service) : service_(service) {}
          public:
            virtual void runTimerTask();
          private:
            ObChunkService* service_;
        };

        class ReportTabletTask : public common::ObTimerTask
        {
          public:
            ReportTabletTask(ObChunkService* service) 
              : service_(service), wait_seconds_(1), report_version_(0) {}
            int64_t get_next_wait_useconds();
            void start_retry_round();
          public:
            virtual void runTimerTask();
          private:
            ObChunkService* service_;
            int64_t wait_seconds_;
            int64_t report_version_;
        };

      private:
        int check_compress_lib(const char* compress_name_buf);
        int load_tablets();
        //mod pangtianze [Paxos rs_election] 20170228:b
        //int register_self();
        //int register_self_busy_wait(int32_t &status);
        int register_self(bool is_first = false);
        int register_self_busy_wait(int32_t &status, bool is_first = false);
        //mod:e
        int report_tablets_busy_wait();
        int fetch_schema_busy_wait(common::ObSchemaManagerV2 *schema);
        bool is_valid_lease();
        bool have_frozen_table(const ObTablet& tablet,const int64_t ups_data_version) const;
        int check_update_data(ObTablet& tablet,int64_t ups_data_version,bool& release_tablet);
        int get_sql_query_service(ObSqlQueryService *&service);
        int get_query_service(ObQueryService *&service);
        int reset_internal_status(bool release_table = true);
        int fetch_update_server_list();
        int get_master_master_update_server();
		//add liumz, [secondary index version management]20160413:b
        inline void set_build_index_version(int64_t index_version) {build_index_version_ = index_version;}
        inline int64_t get_build_index_version() {return build_index_version_;}
        //add:e
        //add pangtianze [Paxos bugfix: coredump, alloc memory for array 'servers' error] 20161214:b
        void reset_rs_servers()
        {
          for (int32_t i = 0; i < OB_MAX_RS_COUNT; i++)
          {
            rs_servers_[i].reset();
          }
        }
        //add:e
      //add pangtianze [Paxos rs_election] 20150710:b
      public:
        inline common::ObRsAddressMgr & get_rs_mgr()
        {
          return rs_mgr_;
        }
        inline common::MsList & get_ms_list_task()
        {
          return ms_list_task_;
        }
        //add:e
      private:
        DISALLOW_COPY_AND_ASSIGN(ObChunkService);
        ObChunkServer* chunk_server_;
        bool inited_;
        bool service_started_;
        bool in_register_process_;
        int64_t service_expired_time_;
        volatile uint32_t migrate_task_count_;
        volatile uint32_t scan_tablet_image_count_;

        common::ObTimer timer_;
        DiskChecker disk_checker_;
        LeaseChecker lease_checker_;
        StatUpdater  stat_updater_;
        MergeTask    merge_task_;
        //add wenghaixing [secondary index static_index_build] 20150302
        IndexTask index_task_;
        IndexCheckTask index_check_task_;
        //add e                
        FetchUpsTask fetch_ups_task_;
        ObMergerSchemaTask fetch_schema_task_;
        ReportTabletTask report_tablet_task_;
        common::TimeUpdateDuty time_update_duty_;
        common::MsList ms_list_task_;
        common::ThreadSpecificBuffer query_service_buffer_;
        tbsys::CRWLock del_tablet_rwlock_;
        //add zhaoqiong [Schema Manager] 20150327:b
        //for get schema mutator from sys table
        oceanbase::common::ObLocalMsList ms_;
        oceanbase::common::ObCommonRpcStub *common_rpc_;
        oceanbase::common::ObScanHelperImpl *scan_helper_;
        common::ObSchemaServiceImpl *schema_service_;
        //add:e
		int64_t build_index_version_;//add liumz, [secondary index version management]20160413
        //add pangtianze [Paxos rs_election] 20150707:b
        common::ObRsAddressMgr rs_mgr_;
        //add:e
        //add pangtianze [Paxos bugfix: coredump, alloc memory for array 'servers' error] 20161214:b
        common::ObServer rs_servers_[OB_MAX_RS_COUNT];
        //add:e
    };


  } // end namespace chunkserver
} // end namespace oceanbase


#endif //OCEANBASE_CHUNKSERVER_CHUNKSERVICE_H_

