/**
 * (C) 2007-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_OPERATION_HELPER_H
#define OCEANBASE_ROOTSERVER_OB_ROOT_OPERATION_HELPER_H
#include "common/ob_schema.h"
#include "common/ob_define.h"
#include "rootserver/ob_root_table2.h"
#include "rootserver/ob_tablet_info_manager.h"
#include "rootserver/ob_root_server_config.h"
#include "rootserver/ob_root_rpc_stub.h"
#include "rootserver/ob_root_operation_data.h"
namespace oceanbase
{
  namespace common
  {
    class ObBypassTaskInfo;
  }
  namespace rootserver
  {
    class ObRootServer2;
    class ObRootOperationHelper
    {
      public:
      enum OperationProcess
      {
        INIT = -1,
        STARTED = 0,
        LOAD_SSTABLE = 1,
        REPORT_TABLET = 2,
        SORT_ROOT_TABLE = 3,
        DELETE_TABLE = 4,
      };
      public:
      ObRootOperationHelper();
      ~ObRootOperationHelper();
      int init(const ObRootServer2 *root_server, const ObRootServerConfig *config,
          const ObRootRpcStub *rpc_stub, const ObChunkServerManager *server_manager);
      ObSchemaManagerV2 *get_schema_manager();
      ObRootTable2 *get_root_table();
      int start_operation(const common::ObSchemaManagerV2 *schema_mgr,
          const ObBypassTaskInfo &table_name_id,
          const int64_t frozen_version);
      int report_tablets(const ObTabletReportInfoList& tablets,
          const int32_t server_index, const int64_t frozen_mem_version);
      int waiting_job_done(const int index);
      int set_delete_table(common::ObArray<uint64_t> &delete_tables);
      int cs_load_sstable_done(const int index,
          const common::ObTableImportInfoList &table_list, const bool is_load_succ);
      void delete_tables_done(const int index,
          const uint64_t table_id, const bool is_succ);
      int end_bypass_process();
      int check_process(OperationType &type);
      const char* print_process();
      //for test
      void set_process(OperationProcess process);
      OperationProcess get_process();
      private:
      int check_load_sstable_process();
      int check_report_tablet_process();
      int check_delete_tables_process();
      int check_root_table();
      int clean_root_table(const ObSchemaManagerV2 *schema_mgr);
      int import_all_table(const int64_t frozen_version,
          const ObSchemaManagerV2 *schema_mgr,
          const ObBypassTaskInfo &table_name_id);
      int bypass_process(const int64_t frozen_version,
          const ObSchemaManagerV2 *schema_mgr,
          const ObBypassTaskInfo &table_name_id);
      int request_cs_load_bypass_table(const int64_t frozen_version,
          const common::ObBypassTaskInfo &table_name_id,
          const int64_t expect_cs = -1);
      private:
      OperationProcess process_;
      ObRootServer2 *root_server_;
      int64_t start_time_us_;
      int64_t delete_index_;
      int64_t done_count_;
      int64_t total_count_;
      ObRootServerConfig *config_;
      ObRootRpcStub *rpc_stub_;
      ObChunkServerManager *server_manager_;
      ObRootOperationData bypass_data_;
    };
  }
}

#endif
