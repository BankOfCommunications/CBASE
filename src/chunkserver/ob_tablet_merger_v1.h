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


#ifndef OB_CHUNKSERVER_OB_TABLET_MERGER_V1_H_
#define OB_CHUNKSERVER_OB_TABLET_MERGER_V1_H_

#include "common/ob_define.h"
#include "sstable/ob_sstable_writer.h"
#include "sstable/ob_sstable_reader.h"
#include "ob_get_scan_proxy.h"
#include "ob_get_cell_stream_wrapper.h"
#include "ob_query_agent.h"
#include "ob_tablet_merge_filter.h"
//add wenghaixing [secondary index col checksum.h]20141210
#include "common/ob_column_checksum.h"
//add e

namespace oceanbase
{
  namespace common
  {
    class ObTableSchema;
    class ObInnerCellInfo;
  }
  namespace sstable
  {
    class ObSSTableSchema;
  }
  namespace chunkserver
  {
    class ObTabletManager;
    class ObTablet;
    class ObChunkMerge;

    class ObTabletMerger
    {
      public:
        static const int64_t DEFAULT_ESTIMATE_ROW_COUNT = 256*1024LL;
      public:
        ObTabletMerger(ObChunkMerge& chunk_merge, ObTabletManager& manager);
        virtual ~ObTabletMerger();

        virtual int init();
        virtual int merge(ObTablet *tablet, int64_t frozen_version) = 0;
        //add wenghaixing [secondary index static_index_build.merge]20150422
        virtual int stop_invalid_index_tablet_merge(ObTablet* tablet, bool &invalid) = 0;
        //add e
        int update_meta(ObTablet* old_tablet,
            const common::ObVector<ObTablet*> & tablet_array, const bool sync_meta);
        int gen_sstable_file_location(const int32_t disk_no,
            sstable::ObSSTableId& sstable_id, char* path, const int64_t path_size);
        int64_t calc_tablet_checksum(const int64_t checksum);
        int check_tablet(const ObTablet* tablet);
        //add zhuyanchao[secondary index static_data_build]20150401
        int update_meta_index(ObTablet* tablet,const bool sync_meta);
        //add e

      protected:
        ObChunkMerge& chunk_merge_;
        ObTabletManager& manager_;

        ObTablet* old_tablet_;            // merge or import tablet object.
        int64_t frozen_version_;          // merge or import tablet new version.
        sstable::ObSSTableId sstable_id_; // current generate new sstable id
        char path_[common::OB_MAX_FILE_NAME_LENGTH]; // current generate new sstable path
        common::ObVector<ObTablet *> tablet_array_;  //generate new tablet objects.
    };

    class ObTabletMergerV1 : public ObTabletMerger
    {
      public:
        ObTabletMergerV1(ObChunkMerge& chunk_merge, ObTabletManager& manager);
        ~ObTabletMergerV1() {}

        int init();
        int merge(ObTablet *tablet,int64_t frozen_version);
        //add liuxiao [secondary index col checksum]20150330
        //获取列校验和
        inline char* get_column_checksum()
        {
          return column_checksum_.get_str();
        }
        //add e
      private:
        int reset();

        DISALLOW_COPY_AND_ASSIGN(ObTabletMergerV1);

        enum RowStatus
        {
          ROW_START = 0,
          ROW_GROWING = 1,
          ROW_END = 2
        };

        bool is_table_need_join(const uint64_t table_id);
        int fill_sstable_schema(const common::ObTableSchema& common_schema,
          sstable::ObSSTableSchema& sstable_schema);
        int update_range_start_key();
        int update_range_end_key();
        int create_new_sstable();
        //add by zhaoqiong [bugfix: create table][schema sync]20160106:b
        int create_empty_tablet_and_upgrade(ObTablet *& new_tablet, int64_t frozen_version);
        //add:e
        int create_hard_link_sstable(int64_t& sstable_size);
        int finish_sstable(const bool is_tablet_unchanged, ObTablet*& new_tablet);
        int report_tablets(ObTablet* tablet_list[],int32_t tablet_size);
        bool maybe_change_sstable() const;
        //add liuxiao [secondary index col checksum] 20150330
        int calc_tablet_col_checksum(const sstable::ObSSTableRow& row,char* column_checksum);
        //add e
        //add liuxiao [secondary index col checksum] 20150409
        int cons_row_desc(ObRowDesc &desc);
        int cons_row_desc_local_tablet(ObRowDesc &rowkey_desc,ObRowDesc &index_desc,uint64_t tid);
        int cons_row_desc_without_virtual(ObRowDesc &rowkey_desc,ObRowDesc &index_desc,uint64_t &max_data_table_cid,uint64_t tid);
        int calc_tablet_col_checksum_index_local_tablet(const sstable::ObSSTableRow& row, ObRowDesc rowkey_desc, ObRowDesc index_desc, char *column_checksum);
        int calc_tablet_col_checksum_index_index_tablet(const sstable::ObSSTableRow& row, ObRowDesc rowkey_desc, ObRowDesc index_desc, char *column_checksum,const uint64_t max_data_table_cid);
        //add e
        //add liuxiao [secondary index col checksum] 20150407
        //判断索引表信息
        int set_has_index_or_is_index(const uint64_t table_id);
        //add e
        //add wenghaixing [secondary index static_index_build.merge]20150422
        virtual int stop_invalid_index_tablet_merge(ObTablet* tablet, bool &invalid);
        //add e
        int fill_scan_param(const uint64_t column_group_id);
        int wait_aio_buffer() const;
        int reset_local_proxy() const;
        int merge_column_group(
            const int64_t column_group_idx,
            const uint64_t column_group_id,
            int64_t& split_row_pos,
            const int64_t max_sstable_size,
            const bool is_need_join,
            bool& is_tablet_splited,
            bool& is_tablet_unchanged
            );

        int save_current_row(const bool current_row_expired);
        int check_row_count_in_column_group();
        void reset_for_next_column_group();


      private:
        common::ObRowkey split_rowkey_;
        common::ObMemBuf split_rowkey_buffer_;
        common::ObMemBuf last_rowkey_buffer_;

        common::ObInnerCellInfo*     cell_;
        const common::ObTableSchema* new_table_schema_;

        sstable::ObSSTableRow    row_;
        sstable::ObSSTableSchema sstable_schema_;
        sstable::ObSSTableWriter writer_;
        common::ObNewRange new_range_;

        int64_t current_sstable_size_;
        int64_t row_num_;
        int64_t pre_column_group_row_num_;

        common::ObString path_string_;
        common::ObString compressor_string_;

        ObGetScanProxy           cs_proxy_;
        common::ObScanParam      scan_param_;

        ObGetCellStreamWrapper    ms_wrapper_;
        ObQueryAgent          merge_join_agent_;
        ObTabletMergerFilter tablet_merge_filter_;
        //add liuxiao [secondary index col checksum]20150330
        col_checksum column_checksum_;
        //add e
        //add liuxiao [secondary index col checksum]20150407
        bool has_index_or_is_index_;//ÊÇ·ñÊÇË÷Òý±í»òÊý¾Ý±í
        bool is_data_table_;//ÊÇ·ñÊÇÊý¾Ý±í
        int64_t index_column_num;//Ë÷ÒýµÄÁÐÊý£¨²»°üÀ¨storing£©
        bool is_have_available_index_;//ÊÇ·ñÓÐ¿ÉÓÃµÄË÷Òý±í
        bool is_or_have_init_index_;
        ObRowDesc rowkey_desc_;
        ObRowDesc index_desc_;
        uint64_t max_data_table_cid_;

        bool is_static_truncated_; /*add zhaoqiong [Truncate Table]:20160318*/
        //add e
    };
  } /* chunkserver */
} /* oceanbase */

#endif //OB_CHUNKSERVER_OB_TABLET_MERGER_V1_H_


