/**
 *  (C) 2010-2011 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  ob_seq_sstablet_scanner.h is for what ...
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 *        
 */
#ifndef OCEANBASE_SSTABLE_SEQ_SSTABLE_SCANNER_H_
#define OCEANBASE_SSTABLE_SEQ_SSTABLE_SCANNER_H_


#include "common/ob_iterator.h"
#include "common/ob_array_helper.h"
#include "ob_sstable_scanner.h"

namespace oceanbase 
{ 
  namespace common
  {
    class ObScanParam;
    class ObCellInfo;
  }

  namespace sstable 
  {
    class ObSSTableReader;
    class ObBlockCache;
    class ObBlockIndexCache;

    class ObSeqSSTableScanner : public common::ObIterator
    {
      public:
        ObSeqSSTableScanner();
        virtual ~ObSeqSSTableScanner();

        int add_sstable_reader(ObSSTableReader* reader);
        int set_scan_param(const common::ObScanParam& param, ObBlockCache& block_cache,
                           ObBlockIndexCache& block_index_cache, bool not_exit_col_ret_nop = false);
        void cleanup();
      public:
        // Moves the cursor to next cell.
        // @return OB_SUCCESS if sucess, OB_ITER_END if iter ends, or other error code
        virtual int next_cell();
        // Gets the current cell.
        virtual int get_cell(common::ObCellInfo** cell);
        virtual int get_cell(common::ObCellInfo** cell, bool* is_row_changed);
      private:
        static const int32_t MAX_SSTABLE_SCANNER_NUM = 64;
        static const int32_t NOT_START = 0;
        static const int32_t PROGRESS = 1;
        static const int32_t END_OF_DATA = 2;
        static const int32_t ERROR_STATUS = 3;

        int32_t cur_iter_idx_;
        int32_t cur_iter_status_;
        bool not_exit_col_ret_nop_; //whether return nop if columns doesn't exit

        ObBlockCache* block_cache_;
        ObBlockIndexCache* block_index_cache_;

        const common::ObScanParam* scan_param_;
        ObSSTableScanner sstable_scanner_;

        ObSSTableReader* sstable_readers_[MAX_SSTABLE_SCANNER_NUM];
        common::ObArrayHelper<ObSSTableReader*> sstable_array_;
    };

  } // end namespace sstable
} // end namespace oceanbase


#endif //OCEANBASE_SSTABLE_SEQ_SSTABLE_SCANNER_H_

