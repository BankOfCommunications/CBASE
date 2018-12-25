/**
 *  (C) 2010-2011 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  ob_sstable_scan_param.h is for what ...
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        
 */



#ifndef OCEANBASE_SSTABLE_OB_SSTABLE_SCAN_PARAM_H_
#define OCEANBASE_SSTABLE_OB_SSTABLE_SCAN_PARAM_H_

#include "common/ob_scan_param.h"
#include "common/ob_array_helper.h"
#include "common/ob_read_common_data.h"
#include "common/ob_rowkey.h"
#include "common/ob_range2.h"

namespace oceanbase
{
  namespace sstable
  {
    class ObSSTableScanParam : public common::ObReadParam
    {
      public:
        ObSSTableScanParam();
        //ObSSTableScanParam(const common::ObScanParam &param);
        ~ObSSTableScanParam();
        inline ObSSTableScanParam & operator=(const common::ObScanParam &param)
        {
          assign(param);
          return *this;
        }
        inline const common::ObNewRange& get_range() const { return range_; }
        inline common::ObNewRange& get_range() { return range_; }
        inline void set_range(const common::ObNewRange& range) { range_ = range; }
        inline uint64_t get_table_id() const { return range_.table_id_; }
        inline const uint64_t* const get_column_id() const { return column_ids_; }

        inline void set_read_mode(const common::ScanFlag::SyncMode mode) 
        { 
          scan_flag_.read_mode_ = mode & common::ScanFlag::SF_MASK_READ_MODE; 
        }
        inline common::ScanFlag::SyncMode get_read_mode() const 
        { 
          return static_cast<common::ScanFlag::SyncMode>(scan_flag_.read_mode_); 
        }

        inline void set_scan_direction(const common::ScanFlag::Direction dir) 
        { 
          scan_flag_.direction_ = dir & common::ScanFlag::SF_MASK_DIRECTION; 
        }
        inline common::ScanFlag::Direction get_scan_direction() const 
        { 
          return static_cast<common::ScanFlag::Direction>(scan_flag_.direction_); 
        }

        inline void set_not_exit_col_ret_nop(const bool nop) { scan_flag_.not_exit_col_ret_nop_ = nop; }
        inline bool is_not_exit_col_ret_nop() const { return scan_flag_.not_exit_col_ret_nop_; }

        inline void set_full_row_scan(const bool full) { scan_flag_.full_row_scan_ = full; }
        inline bool is_full_row_scan() const { return scan_flag_.full_row_scan_; }

        inline void set_daily_merge_scan(const bool full) { scan_flag_.daily_merge_scan_ = full; }
        inline bool is_daily_merge_scan() const { return scan_flag_.daily_merge_scan_; }

        inline void set_rowkey_column_count(const int16_t count) { scan_flag_.rowkey_column_count_ = count; }
        inline int16_t get_rowkey_column_count() const { return scan_flag_.rowkey_column_count_; }

        inline void set_scan_flag(const common::ScanFlag& flag) { scan_flag_ = flag; }
        inline const common::ScanFlag get_scan_flag() const { return scan_flag_; }

        inline int64_t get_column_id_size() const 
        { 
          return column_id_list_.get_array_index(); 
        }
        inline const common::ObArrayHelper<uint64_t> & get_column_id_list() const 
        { 
          return column_id_list_; 
        }

        inline int add_column(uint64_t column_id) 
        { 
          return column_id_list_.push_back(column_id) 
            ? common::OB_SUCCESS : common::OB_SIZE_OVERFLOW; 
        }

        inline bool is_reverse_scan() const
        {
          return scan_flag_.direction_ == common::ScanFlag::BACKWARD;
        }
        inline bool is_sync_read() const
        {
          return scan_flag_.read_mode_ == common::ScanFlag::SYNCREAD;
        }

        inline bool is_full_row_columns() const
        {
          return column_id_list_.get_array_index() == 1 && column_ids_[0] == 0;
        }

        template <typename Param> int assign(const Param& param);
        void reset();
        bool is_valid() const;
        int64_t to_string(char* buf, const int64_t buf_len) const;

      private:
        common::ObNewRange range_;
        common::ScanFlag scan_flag_;
        uint64_t column_ids_[common::OB_MAX_COLUMN_NUMBER];
        common::ObArrayHelper<uint64_t> column_id_list_;
    };

    template <typename Param>
    int ObSSTableScanParam::assign(const Param& param)
    {
      int ret = common::OB_SUCCESS;


      int64_t column_id_size = param.get_column_id_size();
      if (column_id_size > common::OB_MAX_COLUMN_NUMBER)
      {
        TBSYS_LOG(WARN, "column_id_size=%ld too large", column_id_size);
        ret = common::OB_SIZE_OVERFLOW;
      }
      else
      {
        // copy base class members
        set_is_result_cached(param.get_is_result_cached());
        set_version_range(param.get_version_range());
        scan_flag_ = param.get_scan_flag();
        // copy range, donot copy start_key_ & end_key_, 
        // suppose they will not change in scan process.
        range_ = *param.get_range();
        range_.table_id_ = param.get_table_id();
        const uint64_t* const column_id_begin = param.get_column_id();
        memcpy(column_ids_, column_id_begin, column_id_size * sizeof(uint64_t));
        column_id_list_.init(common::OB_MAX_COLUMN_NUMBER, column_ids_, column_id_size);
      }
      return ret;
    }

  }
}


#endif // OCEANBASE_SSTABLE_OB_SSTABLE_SCAN_PARAM_H_
