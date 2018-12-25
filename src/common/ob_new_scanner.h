/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *     - some work details if you want
 *   yanran <yanran.hfs@taobao.com> 2010-10-27
 *     - new serialization format(ObObj composed, extented version)
 */

#ifndef OCEANBASE_COMMON_OB_NEW_SCANNER_H_
#define OCEANBASE_COMMON_OB_NEW_SCANNER_H_

#include "ob_define.h"
#include "ob_read_common_data.h"
#include "ob_row_iterator.h"
#include "ob_rowkey.h"
#include "ob_range2.h"
#include "page_arena.h"
#include "ob_string_buf.h"
#include "ob_object.h"
#include "ob_row.h"
#include "ob_row_store.h"

namespace oceanbase
{
  namespace common
  {
    class ObNewScanner : public ObRowIterator
    {
      public:
        static const int64_t DEFAULT_MAX_SERIALIZE_SIZE = OB_MAX_PACKET_LENGTH - OB_MAX_ROW_KEY_LENGTH * 2 - 1024;
      public:
        ObNewScanner();
        virtual ~ObNewScanner();

        virtual void clear();
        virtual void reuse();

        int64_t set_mem_size_limit(const int64_t limit);

        /// @brief add a row in the ObNewScanner
        /// @retval OB_SUCCESS successful
        /// @retval OB_SIZE_OVERFLOW memory limit is exceeded if add this row
        /// @retval otherwise error
        int add_row(const ObRow &row);

        int add_row(const ObRowkey &rowkey, const ObRow &row);

        /// @retval OB_ITER_END iterate end
        /// @retval OB_SUCCESS go to the next cell succ
        int get_next_row(ObRow &row);

        int get_next_row(const ObRowkey *&rowkey, ObRow &row);

        /* @brief set default row desc which will auto filt into row
         * when invoke get_next_row if row desc of the given parameter
         * row is NULL. */
        void set_default_row_desc(const ObRowDesc *row_desc) { default_row_desc_ = row_desc; }

        bool is_empty() const;
        /// after deserialization, get_last_row_key can retreive the last row key of this scanner
        /// @param [out] row_key the last row key
        int get_last_row_key(ObRowkey &row_key) const;
        int set_last_row_key(ObRowkey &row_key);
        int serialize_meta_param(char * buf, const int64_t buf_len, int64_t & pos) const;
        int deserialize_meta_param(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj);
        NEED_SERIALIZE_AND_DESERIALIZE;

      public:
        /// indicating the request was fullfiled, must be setted by cs and ups
        /// @param [in] is_fullfilled  缓冲区不足导致scanner中只包含部分结果的时候设置为false
        /// @param [in] fullfilled_row_num -# when getting, this means processed position in GetParam
        ///                                 -# when scanning, this means row number fullfilled
        int set_is_req_fullfilled(const bool &is_fullfilled, const int64_t fullfilled_row_num);
        int get_is_req_fullfilled(bool &is_fullfilled, int64_t &fullfilled_row_num) const;

        /// when response scan request, range must be setted
        /// the range is the seviced range of this tablet or (min, max) in updateserver
        /// @param [in] range
        int set_range(const ObNewRange &range);
        /// same as above but do not deep copy keys of %range, just set the reference.
        /// caller ensures keys of %range remains reachable until ObNewScanner serialized.
        int set_range_shallow_copy(const ObNewRange &range);
        int get_range(ObNewRange &range) const;

        inline void set_data_version(const int64_t version)
        {
          data_version_ = version;
        }

        inline int64_t get_data_version() const
        {
          return data_version_;
        }

        /* 获取数据占用的空间总大小（包括暂未使用的缓冲区) */
        inline int64_t get_used_mem_size() const
        {
          return row_store_.get_used_mem_size();
        }
        /** 获取实际数据的大小
        */
        inline int64_t get_size() const
        {
          return cur_size_counter_;
        }

        inline int64_t get_row_num() const
        {
          return cur_row_num_;
        }

        int64_t to_string(char* buffer, const int64_t length) const;
        void dump(void) const;
        void dump_all(int log_level) const; 


      private:
        int deserialize_basic_(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj);

        int deserialize_table_(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj);

        static int deserialize_int_(const char* buf, const int64_t data_len, int64_t& pos,
            int64_t &value, ObObj &last_obj);

        static int deserialize_varchar_(const char* buf, const int64_t data_len, int64_t& pos,
            ObString &value, ObObj &last_obj);

        static int deserialize_int_or_varchar_(const char* buf, const int64_t data_len, int64_t& pos,
            int64_t& int_value, ObString &varchar_value, ObObj &last_obj);

        int64_t get_meta_param_serialize_size() const;

      protected:
        ObRowStore row_store_;
        int64_t cur_size_counter_;
        int64_t mem_size_limit_;
        int64_t data_version_;
        bool has_range_;
        bool is_request_fullfilled_;
        int64_t  fullfilled_row_num_;
        int64_t cur_row_num_;
        ObNewRange range_;
        ObRowkey last_row_key_;
        ModulePageAllocator mod_;
        mutable ModuleArena rowkey_allocator_;
        const ObRowDesc* default_row_desc_;        
    };

    class ObCellNewScanner : public ObNewScanner
    {
      public: 
        ObCellNewScanner() 
          : row_desc_(NULL),
          last_table_id_(OB_INVALID_ID),
          ups_row_valid_(false),
          is_size_overflow_(false)
        { 
        }
        ~ObCellNewScanner() { }
      public:
        // compatible with ObScanner's interface;
        int add_cell(const ObCellInfo &cell_info, 
            const bool is_compare_rowkey = true, const bool is_rowkey_change = false); 
        int finish();
        void clear();
        void reuse();
        void set_row_desc(const ObRowDesc & row_desc);
        int set_is_req_fullfilled(const bool &is_fullfilled, const int64_t fullfilled_row_num);
        
        //do not need rollback
        inline int rollback()
        {
          return OB_SUCCESS;
        }

      private:
        int add_current_row();

      private:
        const ObRowDesc* row_desc_;
        uint64_t last_table_id_;
        ObUpsRow ups_row_;
        ObStringBuf str_buf_;
        ObRowkey cur_rowkey_;
        bool ups_row_valid_;
        bool is_size_overflow_;
    };

  } /* common */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_COMMON_OB_NEW_SCANNER_H_ */
