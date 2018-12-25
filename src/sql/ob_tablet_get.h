/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_get.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#ifndef _OB_TABLET_GET_H
#define _OB_TABLET_GET_H 1

#include "ob_tablet_read.h"
#include "ob_sstable_get.h"
#include "ob_tablet_get_fuse.h"
#include "ob_project.h"
#include "ob_limit.h"
#include "ob_filter.h"
#include "ob_table_rename.h"
#include "sql/ob_sql_get_param.h"
#include "chunkserver/ob_tablet_manager.h"
#include "ob_tablet_direct_join.h"

namespace oceanbase
{
  namespace sql
  {
    class ObTabletGet : public ObTabletRead
    {
      public:
        int create_plan(const ObSchemaManagerV2 &schema_mgr);
        int set_tablet_manager(chunkserver::ObTabletManager *tablet_manager);

        inline void set_sql_get_param(const ObSqlGetParam &sql_get_param);
        void reset();
        void reuse();
        int64_t to_string(char* buf, const int64_t buf_len) const;
        virtual ObPhyOperatorType get_type() const { return PHY_TABLET_GET; }

      private:
        int need_incremental_data(ObArray<uint64_t> &basic_columns,
                                  ObTabletJoin::TableJoinInfo &table_join_info,
                                  int64_t start_data_version,
                                  int64_t end_data_version);

        //del zhaoqiong [Truncate Table]:20170519:b
//        int need_incremental_data(ObArray<uint64_t> &basic_columns,
//                                  ObTabletJoin::TableJoinInfo &table_join_info,
//                                  ObVersionRange *range); //add zhaoqiong [Truncate Table]:20160318
        //del:e
        bool check_inner_stat() const;
        int add_action_flag_column(ObProject &project);

      private:
        chunkserver::ObTabletManager *tablet_manager_;
        ObSSTableGet op_sstable_get_;
        ObGetParam get_param_;
        const ObSqlGetParam *sql_get_param_;
        ObRowDesc ups_mget_row_desc_;

        ObUpsMultiGet op_ups_multi_get_;
        ObTabletGetFuse op_tablet_get_fuse_;
        ObTabletDirectJoin op_tablet_join_;
        ObProject op_project_;
        ObTableRename op_rename_;
    };

    void ObTabletGet::set_sql_get_param(const ObSqlGetParam &sql_get_param)
    {
      sql_get_param_ = &sql_get_param;
    }
  }
}

#endif /* _OB_TABLET_GET_H */
