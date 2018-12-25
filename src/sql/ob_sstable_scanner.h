/**
 * (C) 2010-2011 Taobao Inc.
 *  
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_sstable_scanner.h for what ... 
 *  
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *   qushan <qushan@taobao.com>
 *     modified at 2010/10/8
 *     use new ObSSTableBlockScanner.
 *
 */
#ifndef OCEANBASE_SQL_OB_SSTABLE_SCANNER_V2_H_
#define OCEANBASE_SQL_OB_SSTABLE_SCANNER_V2_H_

#include "common/ob_iterator.h"
#include "common/ob_merger.h"
#include "sstable/ob_sstable_scan_param.h"
#include "sstable/ob_scan_column_indexes.h"
#include "ob_column_group_scanner.h"
#include "ob_multi_cg_scanner.h"

using namespace oceanbase::common;
using namespace oceanbase::sstable;

namespace oceanbase
{
  namespace common
  {
    class ObRowkeyInfo;
  }

  namespace sstable
  {
    class ObSSTableReader;
    class ObSSTableSchema;
  }

  namespace sql
  {
    class ScanContext;

    class ObSSTableScanner : public ObRowkeyIterator
    {
    public:
      ObSSTableScanner();
      ~ObSSTableScanner();

      inline int get_next_row(const common::ObRowkey* &row_key, const common::ObRow *&row_value)
      {
        int ret = OB_SUCCESS;
        if (end_of_data_)
        {
          ret = OB_ITER_END;
        }
        else if (1 == column_group_size_)
        {
          ret = column_group_scanner_.get_next_row(row_key, row_value);
        }
        else
        {
          ret = merger_.get_next_row(row_key, row_value);
        }
        return ret;
      }

      int get_row_desc(const common::ObRowDesc *&row_desc) const;

      /**
       * set scan parameters like input query range, sstable object.
       *
       * @param context input scan param, query range, columns, table, etc. 
       * @param sstable_reader corresponds sstable for scanning.
       * @param block_cache block cache
       * @param block_index_cache block index cache 
       *  
       * @return 
       *  OB_SUCCESS on success otherwise on failure. 
       */
      int set_scan_param(
          const sstable::ObSSTableScanParam& scan_param, 
          const ScanContext* scan_context);

      /**
       * cleanup internal objects, this method must be 
       * invoke after scan done when reuse ObSSTableScanner.
       */
      void cleanup();

      //add whx, [bugfix: free memory]20161030
      void clear();

    private:
      bool column_group_exists(const uint64_t* group_array, 
          const int64_t group_size, const uint64_t group_id) const;
      bool is_columns_in_one_group(const sstable::ObSSTableScanParam &scan_param, 
        const sstable::ObSSTableSchema* schema, const uint64_t group_id);

      /**
       * translate column ids in %ObScanParam to 
       * column group array.
       *
       * @param scan_param input query column ids
       * @param sstable_reader corresponds sstable.
       * @param [out] group_array translate result.
       * @param [out] group_size size of group_array
       *
       * @return OB_SUCCESS on success otherwise on failure.
       */
      int trans_input_column_id(const sstable::ObSSTableScanParam& scan_param, 
          const sstable::ObSSTableSchema* schema, uint64_t *group_array, int64_t& group_size);

      /**
       * special case while column_id is 0, means we get whole row.
       * helper function used by trans_input_column_id.
       */
      int trans_input_whole_row(const sstable::ObSSTableScanParam& scan_param, 
          const sstable::ObSSTableSchema* schema, uint64_t* group_array, int64_t& group_size);

      /**
       * store input scan parameters by translate 
       * ObScanParam to ObSSTableScanParam.
       * check some border flags like min , max value.
       */
      int trans_input_scan_range(const sstable::ObSSTableScanParam& scan_param) ;

      /**
       * call by set_scan_param, before next_cell or get_cell
       * initialize can reset status of itself, 
       */
      int initialize(const ScanContext* scan_context, 
          const sstable::ObSSTableScanParam& scan_param);

      int set_column_group_scanner(const uint64_t *group_array, const int64_t group_size, 
          const common::ObRowkeyInfo* rowkey_info);
      int set_mult_column_group_scanner(const uint64_t *group_array, const int64_t group_size, 
          const common::ObRowkeyInfo* rowkey_info);
      int set_single_column_group_scanner(const uint64_t column_group_id, 
          const common::ObRowkeyInfo* rowkey_info);

      /**
       * reset all members of scanner.
       * cache objects, scan_param_, base class members;
       * and reset column group scanner
       */
      void destroy_internal_scanners();

      /**
       * have no sstable in scan. 
       */
      int build_row_desc_from_scan_param(const sstable::ObSSTableScanParam& scan_param);
      bool is_empty_sstable() const;

    private:
      common::ObRowkeyInfo rowkey_info_;        // compatible rowkey info from external
      const ScanContext* scan_context_;         // reader_, caches;

      sstable::ObSSTableScanParam scan_param_;  // input scan param (columns);
      char* internal_scanner_obj_ptr_;          // for multi column group scanners
      int64_t column_group_size_;               // column group count
      bool end_of_data_;                        // have no sstable in tablet.

      ObMultiColumnGroupScanner merger_;
      ObColumnGroupScanner column_group_scanner_;
      common::ObRowDesc row_desc_;
    };
  }//end namespace sstable
}//end namespace oceanbase

#endif
