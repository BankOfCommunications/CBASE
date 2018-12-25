/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*
*
*   Version: 0.1 2010-09-19
*
*   Authors:
*          ruohai(ruohai@taobao.com)
*
*
================================================================*/
#ifndef OCEANBASE_ROOT_SERVER_LOG_WORKER_H
#define OCEANBASE_ROOT_SERVER_LOG_WORKER_H
#include "common/ob_server.h"
#include "common/ob_range.h"
#include "common/ob_tablet_info.h"
#include "common/ob_log_entry.h"
#include "common/ob_ups_info.h"
#include "common/roottable/ob_tablet_meta_table_row.h"
#include "ob_chunk_server_manager.h"
namespace oceanbase
{
  namespace rootserver
  {
    class ObRootLogManager;
    class ObRootServer2;
    class ObRootBalancer;

    class ObRootLogWorker
    {
      public:
        ObRootLogWorker();

      public:
        void set_root_server(ObRootServer2* root_server);
        void set_balancer(ObRootBalancer* balancer);
        void set_log_manager(ObRootLogManager* log_manager);
        int apply(common::LogCommand cmd, const char* log_data, const int64_t& data_len);

      uint64_t get_cur_log_file_id();
      uint64_t get_cur_log_seq();

      public:
        int sync_schema(const int64_t timestamp);
        int regist_cs(const common::ObServer& server, const char* server_version, const int64_t timestamp);
        int regist_ms(const common::ObServer& server, int32_t sql_port, const char* server_version, const int64_t timestamp);
        int regist_lms(const common::ObServer& server, int32_t sql_port, const char* server_version, const int64_t timestamp);
        int server_is_down(const common::ObServer& server, const int64_t timestamp);
        int report_cs_load(const common::ObServer& server, const int64_t capacity, const int64_t used);
        int cs_migrate_done(const int32_t result, const ObDataSourceDesc& desc,
            const int64_t occupy_size, const uint64_t crc_sum, const uint64_t row_checksum, const int64_t row_count);
        int report_tablets(const common::ObServer& server, const common::ObTabletReportInfoList& tablets, const int64_t timestamp);
        int add_range_for_load_data(const common::ObTabletReportInfoList& tablets);
        int remove_table(const common::ObArray<uint64_t> &deleted_tables);
        int add_load_table(const ObString& table_name, const uint64_t table_id, const uint64_t old_table_id,
            const common::ObString& uri, const int64_t start_time, const int64_t tablet_version);
        int set_load_table_status(const uint64_t table_id, const int32_t status, const int64_t end_time);
        int remove_replica(const common::ObTabletReportInfo & replica);
        int delete_replicas(const common::ObServer& server, const common::ObTabletReportInfoList& replicas);
        int add_new_tablet(const common::ObTabletInfo tablet, const common::ObArray<int32_t> &chunkservers, const int64_t mem_version);
        int batch_add_new_tablet(const common::ObTabletInfoList& tablets,
            int** server_indexs, int* count, const int64_t mem_version);
        int cs_merge_over(const common::ObServer& server, const int64_t timestamp);

        int set_frozen_version_for_brodcast(const int64_t bypass_version);
        int sync_us_frozen_version(const int64_t frozen_version, const int64_t last_frozen_time);
        int set_ups_list(const common::ObUpsList &ups_list);
        int init_first_meta(const common::ObTabletMetaTableRow &first_meta_row);

        int clean_root_table();
        int got_config_version(int64_t config_version);
      private:
        int log_server(const common::LogCommand cmd, const common::ObServer& server);
        int log_server_with_ts(const common::LogCommand cmd, const common::ObServer& server, const char* server_version, const int64_t timestamp);
        int flush_log(const common::LogCommand cmd, const char* log_buffer, const int64_t& serialize_size);

      public:
        int do_check_point(const char* log_data, const int64_t& log_length);
        int do_set_bypass_version(const char* log_data, const int64_t& log_length);
        int do_schema_sync(const char* log_data, const int64_t& log_length);
        int do_cs_regist(const char* log_data, const int64_t& log_length);
        int do_ms_regist(const char* log_data, const int64_t& log_length);
        int do_lms_regist(const char* log_data, const int64_t& log_length);
        int do_server_down(const char* log_data, const int64_t& log_length);
        int do_cs_load_report(const char* log_data, const int64_t& log_length);
        int do_cs_migrate_done(const char* log_data, const int64_t& log_length);

        int do_init_first_meta_row(const char* log_data, const int64_t& log_length);
        int do_report_tablets(const char* log_data, const int64_t& log_length);
        int do_add_range_for_load_data(const char* log_data, const int64_t& log_length);
        int do_remove_replica(const char* log_data, const int64_t& log_length);
        int do_delete_replicas(const char* log_data, const int64_t& log_length);
        int do_remove_table(const char* log_data, const int64_t& log_length);
        int do_add_load_table(const char* log_data, const int64_t& log_length);
        int do_set_load_table_status(const char* log_data, const int64_t& log_length);

        int do_add_new_tablet(const char* log_data, const int64_t& log_length);
        int do_batch_add_new_tablet(const char* log_data, const int64_t& log_length);
        int do_create_table_done();

        int do_begin_balance();
        int do_balance_done();

        int do_cs_merge_over(const char* log_data, const int64_t& log_length);
        int do_sync_frozen_version(const char* log_data, const int64_t& log_length);
        int do_sync_frozen_version_and_time(const char* log_data, const int64_t& log_length);
        int do_set_ups_list(const char* log_data, const int64_t& log_length);

        int do_got_config_version(const char* log_data, const int64_t& log_length);
        int do_clean_root_table(const char* log_data, const int64_t& log_length);

        void exit();

      private:
        ObRootServer2* root_server_;
        ObRootBalancer* balancer_;
        ObRootLogManager* log_manager_;
    };
  } /* rootserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_ROOT_SERVER_LOG_WORKER_H */
