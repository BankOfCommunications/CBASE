/**
 *  (C) 2010-2011 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  ob_sstable_block_scanner.h is for what ...
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 *        
 */
#ifndef OCEANBASE_SSTABLE_OB_SSTABLE_BLOCK_SCANNER_V3_H_
#define OCEANBASE_SSTABLE_OB_SSTABLE_BLOCK_SCANNER_V3_H_

#include <string>
#include <tblog.h>
#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_common_param.h"
#include "common/ob_rowkey.h"
#include "common/ob_row.h"
#include "common/ob_range2.h"
#include "sstable/ob_sstable_block_reader.h"
#include "ob_multi_cg_scanner.h"

namespace oceanbase
{
  namespace sstable
  {
    class ObSimpleColumnIndexes;
    class ObSSTableScanParam;
  }
  namespace sql
  {
    class ObSSTableBlockScanner : public ObRowkeyIterator
    {
      public:
        ObSSTableBlockScanner(const sstable::ObSimpleColumnIndexes& column_indexes);
        ~ObSSTableBlockScanner();


        int get_next_row(const common::ObRowkey* &row_key, const common::ObRow *&row_value);

        /**
         * @param [in] range scan range(start key, end key, border flag).
         * @param [in] block_data_buf sstable block data buffer.
         * @param [in] block_data_len size of %block_data_buf
         * @param [out] need_looking_forward scan reach the end
         * @param not_exit_col_ret_nop [in] whether return nop if column
         *                             doesn't exit  
         * @return OB_SUCCESS on success, otherwise failed.
         */
        int set_scan_param(
            const common::ObRowDesc& row_desc,
            const sstable::ObSSTableScanParam & scan_param,
            const sstable::ObSSTableBlockReader::BlockDataDesc& data_desc,
            const sstable::ObSSTableBlockReader::BlockData& block_data, 
            bool &need_looking_forward);

        int get_row_desc(const common::ObRowDesc *&row_desc) const;

      private:
        typedef sstable::ObSSTableBlockReader::const_iterator const_iterator;
        typedef sstable::ObSSTableBlockReader::iterator iterator;
      private:
        int store_and_advance_row();
        void next_row();
        bool start_of_block();
        bool end_of_block();

        int initialize(const bool is_reverse_scan, 
            const bool is_full_row_scan,
            const int64_t store_style, 
            const bool not_exit_col_ret_nop);

        int locate_start_pos(const common::ObNewRange& range,
            const_iterator& start_iterator, bool& need_looking_forward);
        int locate_end_pos(const common::ObNewRange& range,
            const_iterator& last_iterator, bool& need_looking_forward);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObSSTableBlockScanner);

      private:
        int64_t initialize_status_;
        int64_t sstable_data_store_style_;
        bool    is_reverse_scan_;
        bool    is_full_row_scan_;
        bool    not_exit_col_ret_nop_;

        uint64_t current_column_count_;
        int64_t not_null_column_count_;

        const_iterator row_cursor_;
        const_iterator row_start_index_;
        const_iterator row_last_index_;
        
        common::ObRowkey current_rowkey_;
        common::ObObj current_ids_[common::OB_MAX_COLUMN_NUMBER];
        common::ObObj current_columns_[common::OB_MAX_COLUMN_NUMBER];
        common::ObRow current_row_;

        sstable::ObSSTableBlockReader reader_;
        const sstable::ObSimpleColumnIndexes& query_column_indexes_;
    };
  }//end namespace sstable
}//end namespace oceanbase

#endif //OCEANBASE_SSTABLE_OB_SSTABLE_BLOCK_SCANNER_V3_H_
