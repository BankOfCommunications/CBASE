#ifndef OCEANBASE_TESTS_ROOTSERVER_ROOT_SERVER_TESTER_H_
#define OCEANBASE_TESTS_ROOTSERVER_ROOT_SERVER_TESTER_H_
#include "rootserver/ob_root_server2.h"
#include "rootserver/ob_root_worker.h"
namespace oceanbase
{
  using namespace common;
  namespace rootserver
  {
    class ObRootServerTester
    {
      public:
      explicit ObRootServerTester(ObRootServer2* root_server):
        root_server_(root_server)
      {
      }
      //void init_root_table_by_report(){ root_server_->init_root_table_by_report();}
      ObChunkServerManager& get_server_manager(){ return root_server_->server_manager_;}
      //int64_t& get_lease_duration(){return (int64_t&)root_server_->config_.flag_cs_lease_duration_us_.get();}
      common::ObSchemaManagerV2*& get_schema_manager(){return root_server_->local_schema_manager_;}

      ObRootTable2*& get_root_table_for_query(){return root_server_->root_table_;}
      //ObRootTable2*& root_table_for_build(){return root_server_->root_table_for_build_;}
      //const char* get_schema_file_name(){return root_server_->config_.flag_schema_filename_.get();}
      bool& get_have_inited(){return root_server_->have_inited_;}
      //int64_t get_time_stamp_changing() { return root_server_->time_stamp_changing_;}
      ObRootLogWorker* get_log_worker() { return root_server_->log_worker_; }
      //int get_server_status() { return root_server_->server_status_; }
      //void set_server_status(int status) { root_server_->server_status_ = status; }
      //void set_master(bool is_master) { root_server_->is_master_ = is_master; }
      //int64_t& get_one_safe_duration() { return (int64_t&)root_server_->config_.flag_safe_lost_one_duration_seconds_.get();}
      //int64_t& get_wait_init_time() { return (int64_t&)root_server_->config_.flag_safe_wait_init_duration_seconds_.get();}
      void stop_thread()
      {
        root_server_->heart_beat_checker_.stop();
        root_server_->heart_beat_checker_.wait();
        TBSYS_LOG(DEBUG, "heart beat checker stoped");
        root_server_->balancer_thread_->stop();
        root_server_->balancer_thread_->wait();
        TBSYS_LOG(DEBUG, "balancer stoped");
        //root_server_->root_table_modifier_.stop();
        //root_server_->root_table_modifier_.wait();
        TBSYS_LOG(DEBUG, "table modifier stoped");
      }
      ObRootServer2* root_server_;
    };

    class ObRootWorkerForTest : public ObRootWorker
    {
      public:
        ObRootWorkerForTest(common::ObConfigManager &config_mgr, ObRootServerConfig &rs_config)
          : ObRootWorker(config_mgr, rs_config)
        {
          start_new_send_times = unload_old_table_times =0;
        }
        virtual ~ObRootWorkerForTest()
        {
        }
        int cs_start_merge(const common::ObServer& server, const int64_t time_stamp, const int32_t init_flag){
          UNUSED(server);
          UNUSED(time_stamp);
          UNUSED(init_flag);
          start_new_send_times++;
          return OB_SUCCESS;
        }
        int up_freeze_mem(const common::ObServer& server, const int64_t time_stamp){
          UNUSED(server);
          UNUSED(time_stamp);
          start_new_send_times++;
          return OB_SUCCESS;
        }
        int unload_old_table(const common::ObServer& server, const int64_t time_stamp, const int64_t remain_time){
          UNUSED(server);
          UNUSED(time_stamp);
          UNUSED(remain_time);
          unload_old_table_times++;
          return OB_SUCCESS;
        }
        int cs_create_tablet(const common::ObServer& server, const common::ObNewRange& range) {
          UNUSED(server);
          UNUSED(range);
          return OB_SUCCESS;
        }
        int hb_to_cs(const common::ObServer& server, const int64_t lease_time) {
          UNUSED(server);
          UNUSED(lease_time);
          return OB_SUCCESS;
        }
        virtual int cs_migrate(const common::ObNewRange& range,
                          const common::ObServer& src_server, const common::ObServer& dest_server, bool keep_src)
        {
          TBSYS_LOG(INFO, "will do cs_migrate");
          char t1[100];
          char t2[100];
          src_server.to_string(t1, 100);
          dest_server.to_string(t2, 100);
          range.hex_dump(TBSYS_LOG_LEVEL_INFO);
          TBSYS_LOG(INFO, "src server = %s dest server = %s keep src = %d", t1, t2, keep_src);
          //root_server_.migrate_over(range, src_server, dest_server, keep_src, 1);
          //root_server_.print_alive_server();
          return OB_SUCCESS;
        }
        void change_schema_test(ObRootServer2* root_server, const int64_t time_stamp, const int32_t init_flag)
        {
          UNUSED(root_server);
          UNUSED(time_stamp);
          UNUSED(init_flag);
          //schemaChanger* tt = new schemaChanger(root_server, time_stamp, init_flag);
          //tt->start();
        }
        int async_sql(const common::ObString &sqlstr, const common::ObString &table_name, int64_t try_times)
        {
          UNUSED(sqlstr);
          UNUSED(table_name);
          UNUSED(try_times);
          TBSYS_LOG(INFO, "async sql. sql_str=%s, table_name=%s", to_cstring(sqlstr), to_cstring(table_name));
          return OB_SUCCESS;
        }
        int start_new_send_times;
        int unload_old_table_times;
        ObRootServer2* get_root_server(){return &root_server_;}
        //ObRootServerConfig &get_config(){return config_;}
    };

    class ObRootServer2ForTest : public ObRootServer2
    {
      public:
        ObRootServer2ForTest(ObRootServerConfig& config)
          :ObRootServer2(config)
        {
          has_been_called_ = false;
        }

        int migrate_over(const ObNewRange& range, const common::ObServer& src_server, const common::ObServer& dest_server, const bool keep_src, const int64_t tablet_version)
        {
          UNUSED(range);
          UNUSED(src_server);
          UNUSED(dest_server);
          UNUSED(keep_src);
          UNUSED(tablet_version);
          has_been_called_ = true;
          return OB_SUCCESS;
        }

        int report_tablets(const ObServer& server, const ObTabletReportInfoList& tablets, const int64_t time_stamp)
        {
          UNUSED(server);
          UNUSED(tablets);
          UNUSED(time_stamp);
          has_been_called_ = true;
          return OB_SUCCESS;
        }

        bool has_been_called_;
    };

  }
}
#endif
