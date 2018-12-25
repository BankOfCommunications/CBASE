/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 *         ob_tablet_merger.h is for what ...
 *
 *  Version: $Id: ob_tablet_merger.h 12/25/2012 02:46:43 PM qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#ifndef OB_CHUNKSERVER_OB_TABLET_MERGER_V2_H_
#define OB_CHUNKSERVER_OB_TABLET_MERGER_V2_H_

#include "common/ob_define.h"
#include "compactsstablev2/ob_compact_sstable_writer.h"
#include "compactsstablev2/ob_sstable_schema.h"
#include "ob_tablet_merger_v1.h"
#include "ob_tablet_merge_filter.h"
#include "sql/ob_tablet_scan.h"

namespace oceanbase
{
  namespace common
  {
    class ObTableSchema;
  }
  namespace sql
  {
    class ObProject;
    class ObSqlScanParam;
  }

  namespace chunkserver
  {
    class ObTabletManager;
    class ObTablet;
    class ObChunkMerge;

    class ObTabletMergerV2 : public ObTabletMerger
    {
      public:
        ObTabletMergerV2(ObChunkMerge& chunk_merge, ObTabletManager& manager);
        ~ObTabletMergerV2() {}

        virtual int init();
        virtual int merge(ObTablet *tablet, int64_t frozen_version);
        //add wenghaixing [secondary index static_index_build.merge]20150422
        virtual int stop_invalid_index_tablet_merge(ObTablet *tablet, bool &invalid){UNUSED(tablet);UNUSED(invalid);return OB_SUCCESS;}
        //add e
      private:
        DISALLOW_COPY_AND_ASSIGN(ObTabletMergerV2);

        int prepare_merge(ObTablet *tablet, int64_t frozen_version);
        int init_sstable_writer(const common::ObTableSchema & table_schema, 
            const ObTablet* tablet, const int64_t frozen_version);
        
        int create_new_sstable();
        int create_hard_link_sstable();
        int finish_sstable(const bool is_sstable_split, const bool is_tablet_unchanged);

        int build_sstable_schema(const uint64_t table_id, compactsstablev2::ObSSTableSchema& sstable_schema);
        int build_project(const compactsstablev2::ObSSTableSchema& schema, sql::ObProject& project);
        int build_project(const compactsstablev2::ObSSTableSchemaColumnDef* def, const int64_t size, sql::ObProject& project);
        int build_sql_scan_param(const compactsstablev2::ObSSTableSchema& schema, sql::ObSqlScanParam& scan_param);

        int wait_aio_buffer() const;
        int reset();
        int open();
        int do_merge();

        int build_extend_info(const bool is_tablet_unchanged, ObTabletExtendInfo& extend_info);
        int build_new_tablet(const bool is_tablet_unchanged, ObTablet* &tablet);
        int cleanup_uncomplete_sstable_files();

      private:
        compactsstablev2::ObSSTableSchema sstable_schema_;

        // for write merged new sstable
        compactsstablev2::ObCompactSSTableWriter writer_;
        // data filter, discard in merge process.
        ObTabletMergerFilter tablet_merge_filter_;

        // for read tablet data..
        sql::ObSqlScanParam sql_scan_param_;
        sql::ObTabletScan tablet_scan_;
        sql::ObSSTableScan op_sstable_scan_;
        sql::ObUpsScan op_ups_scan_;
        sql::ObUpsMultiGet op_ups_multi_get_;
    };
  } /* chunkserver */
} /* oceanbase */

#endif //OB_CHUNKSERVER_OB_TABLET_MERGER_V2_H_


