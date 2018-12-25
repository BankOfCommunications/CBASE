/**
 * (C) 2010-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_sstable_merger.h for merge multi-sstable. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SSTABLE_OB_SSTABLE_MERGER_H_
#define OCEANBASE_SSTABLE_OB_SSTABLE_MERGER_H_

#include "ob_sstable_writer.h"
#include "ob_sstable_reader.h"
#include "ob_seq_sstable_scanner.h"

namespace oceanbase 
{
  namespace sstable 
  {
    class ObSSTableMerger
    {
      public:
        ObSSTableMerger();
        ~ObSSTableMerger();

        int merge_sstables(const common::ObString& sstable_path,
          ObBlockCache& block_cache, ObBlockIndexCache& block_index_cache,
          ObSSTableReader* readers[], const int64_t reader_size, 
          const common::ObNewRange& new_range);

        void cleanup();

        inline const ObSSTableTrailer& get_sstable_trailer() const
        {
          return writer_.get_trailer();
        }

        inline int64_t get_sstable_size() const
        {
          return current_sstable_size_;
        }

      private:
        void reset();
        int check_readers_param(ObSSTableReader* readers[], const int64_t reader_size);
        int create_new_sstable();
        int finish_sstable(const common::ObNewRange& new_range);
        int save_current_row();
        int check_row_count_in_column_group();
        void reset_for_next_column_group();
        int fill_scan_param(const uint64_t column_group_id);
        int merge_one_column_group(const int64_t column_group_idx,
          const uint64_t column_group_id, ObSSTableReader* readers[], 
          const int64_t reader_size);
        int merge_column_groups(ObSSTableReader* readers[], 
          const int64_t reader_size, const common::ObNewRange& new_range);

      private:
        enum RowStatus
        {
          ROW_START = 0,
          ROW_GROWING = 1,
          ROW_END = 2
        };

      private:
        DISALLOW_COPY_AND_ASSIGN(ObSSTableMerger);

        ObBlockCache* block_cache_;
        ObBlockIndexCache* block_index_cache_;

        char             path_[common::OB_MAX_FILE_NAME_LENGTH];
        common::ObString sstable_path_;
        common::ObCellInfo* cell_;
        ObSSTableRow    row_;
        const ObSSTableSchema* sstable_schema_;
        const ObSSTableTrailer* trailer_;
        ObSSTableWriter writer_;

        int64_t current_sstable_size_;
        int64_t row_num_;
        int64_t pre_column_group_row_num_;

        common::ObScanParam scan_param_;
        ObSeqSSTableScanner seq_scanner_;
    };
  } // namespace oceanbase::sstable
} // namespace Oceanbase

#endif // OCEANBASE_SSTABLE_OB_SSTABLE_MERGER_H_
