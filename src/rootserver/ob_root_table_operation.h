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
#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_TABLE_OPERATION_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_TABLE_OPERATION_H_
#include "ob_root_table2.h"
#include "ob_tablet_info_manager.h"
#include "common/ob_schema.h"
#include "ob_root_server_config.h"
namespace oceanbase
{
  namespace rootserver
  {
    class ObRootTableOperation
    {
      public:
        ObRootTableOperation();
        ~ObRootTableOperation();
        void init(const ObRootServerConfig *config);
        void set_schema_manager(const common::ObSchemaManagerV2 *schema_mgr);
        ObRootTable2 *get_root_table();
        ObTabletInfoManager *get_tablet_info_manager();
        void reset_root_table();
        int report_tablets(const ObTabletReportInfoList& tablets,
            const int32_t server_index, const int64_t frozen_mem_version);
        int waiting_job_done();
        int generate_root_table();
        int check_root_table(ObTabletReportInfoList &delete_list);
        void destroy_data();
      private:
        ObRootServerConfig *config_;
        //保存所有旁路导入汇报的tablet
        ObRootTable2 *new_root_table_;
        ObTabletInfoManager* tablet_manager_;
        //整理roottable时所需的
        ObRootTable2 *root_table_tmp_;
        ObTabletInfoManager* tablet_manager_tmp_;
        const common::ObSchemaManagerV2 *schema_manager_;
    };
  }
}

#endif
