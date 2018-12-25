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
#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_OPERATION_DATA_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_OPERATION_DATA_H_
#include "common/ob_bypass_task_info.h"
#include "rootserver/ob_root_table_operation.h"
#include "rootserver/ob_rs_schema_operation.h"
namespace oceanbase
{
  namespace rootserver
  {
    class ObRootOperationData
    {
      public:
        ObRootOperationData();
        ~ObRootOperationData();
        void init(const ObRootServerConfig *config);
        void reset_data();
        void destroy_data();
        OperationType get_operation_type();
        void set_operation_type(const OperationType &type);
        ObSchemaManagerV2 *get_schema_manager();
        ObRootTable2 *get_root_table();
        ObTabletInfoManager *get_tablet_info_manager();
        int generate_root_table();
        int init_schema_mgr(const common::ObSchemaManagerV2 *schema_mgr);
        int generate_bypass_data(const common::ObSchemaManagerV2 *schema_mgr);
        int change_table_schema(const common::ObSchemaManagerV2 *schema_mgr,
            const char* table_name, const uint64_t new_table_id);
        int report_tablets(const ObTabletReportInfoList& tablets,
            const int32_t server_index, const int64_t frozen_mem_version);
        int set_delete_table(common::ObArray<uint64_t> &delete_tables);
        int check_root_table(common::ObTabletReportInfoList &delete_list);
        common::ObArray<uint64_t>& get_delete_table();
      private:
        OperationType operation_type_;
        common::ObArray<uint64_t> delete_tables_;
        ObRootTableOperation root_table_;
        ObRsSchemaOperation schema_helper_;
    };
  }
}

#endif


