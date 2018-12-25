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

#ifndef OCEANBASE_COMMON_OB_SCANNER_H_
#define OCEANBASE_COMMON_OB_SCANNER_H_

#include "ob_define.h"
#include "ob_read_common_data.h"
#include "ob_iterator.h"
#include "ob_object.h"
#include "ob_rowkey.h"
#include "ob_range2.h"
#include "page_arena.h"

namespace oceanbase
{
  namespace common
  {
    class ObSchemaManagerV2;
    class ObScanner : public ObIterator, public ObInnerIterator
    {
      static const int64_t DEFAULT_MAX_SERIALIZE_SIZE = OB_MAX_PACKET_LENGTH - OB_MAX_ROW_KEY_LENGTH * 2 - 1024;
      static const int64_t DEFAULT_MEMBLOCK_SIZE = 512 * 1024;
      struct MemBlock
      {
        int64_t memory_size;
        int64_t rollback_pos;
        int64_t cur_pos;
        MemBlock *next;
        char memory[0];
        explicit MemBlock(const int64_t size) : memory_size(size), next(NULL)
        {
          reset();
        };
        void reset()
        {
          rollback_pos = 0;
          cur_pos = 0;
        };
      };
    public:
      enum ID_NAME_STATUS
      {
        UNKNOWN = 0,
        ID = 1,
        NAME = 2
      };
      /// only column name/id and value is stored, when table name/id or row key are the same.
      /// when iterating through stored table, current table name/id and row key should be recorded.
      class TableReader
      {
        public:
          TableReader();
          virtual ~TableReader();

          inline TableReader& operator=(const TableReader& other)
          {
            if (this != &other)
            {
              cur_table_name_ = other.cur_table_name_;
              cur_table_id_ = other.cur_table_id_;
              id_name_type_ = other.id_name_type_;
              is_row_changed_ = other.is_row_changed_;
              memcpy(cur_rowkey_obj_array_, other.cur_rowkey_obj_array_, sizeof(ObObj)*OB_MAX_ROWKEY_COLUMN_NUMBER);
              cur_row_key_.assign(cur_rowkey_obj_array_, other.cur_row_key_.get_obj_cnt());
            }
            return *this;
          }

          /// @brief get a complete ObCellInfo from the buffer
          /// iterate a series of cells, store the current table and row
          /// reconstruct a complete ObCellInfo with the current column and the table and row info
          /// @param [out] cell_info output complete ObCellInfo
          /// @param [out] last_obj the last read ObObj
          int read_cell(const ObSchemaManagerV2* schema_manager, const char *buf, const int64_t data_len, int64_t &pos,
                ObCellInfo &cell_info, ObObj &last_obj);

          /// @brief get current table name
          /// namely the last read table name
          inline const ObString& get_cur_table_name() const
          {
            return cur_table_name_;
          }

          /// @brief get current table id
          /// namely the last read table id
          inline uint64_t get_cur_table_id() const
          {
            return cur_table_id_;
          }

          /// @brief get current row key
          /// namely the last read row key
          inline const ObRowkey& get_cur_row_key() const
          {
            return cur_row_key_;
          }

          inline bool is_row_changed() const
          {
            return is_row_changed_;
          }

        private:
          ObString cur_table_name_;
          uint64_t cur_table_id_;
          ObRowkey cur_row_key_;
          int64_t id_name_type_;
          bool is_row_changed_;
          ObObj cur_rowkey_obj_array_[OB_MAX_ROWKEY_COLUMN_NUMBER];
      };

      class Iterator
      {
        public:
          Iterator();
          virtual ~Iterator();
          Iterator(const Iterator &other);
          Iterator(const ObScanner *scanner, const ObScanner::MemBlock *mem_block,
                            int64_t size_counter = 0, int64_t cur_pos = 0);
        public:
          int get_cell(ObCellInfo &cell_info);
          int get_cell(ObCellInfo **cell_info, bool *is_row_changed = NULL);
          Iterator &operator ++ ();
          Iterator operator ++ (int);
          bool operator == (const Iterator &other) const;
          bool operator != (const Iterator &other) const;
          Iterator &operator= (const Iterator &other);
        protected:
          bool is_iterator_end_();
          void update_cur_mem_block_();
          int get_cell_(ObCellInfo &cell_info, int64_t &next_pos);
          int get_cell_(const char *data, const int64_t data_len, const int64_t cur_pos,
              ObCellInfo &cell_info, int64_t &next_pos);
        protected:
          friend class ObScanner;
          const ObScanner *scanner_;
          const ObScanner::MemBlock *cur_mem_block_;
          int64_t cur_pos_;
          int64_t next_pos_;
          int64_t iter_size_counter_;
          TableReader reader_;
          ObCellInfo cur_cell_info_;
          bool new_row_cell_;
	        bool is_end_;
          ObObj last_obj_;
      };

      // RowIterator实现有一个前提假设，在读取cell的时候不能遇到多个Memblock，
      // 当一行数据横跨两个Memblock时会出现异常。
      // 目前ObScanner的实现不会有这个问题，因为每次申请的Mmeblock都是2MB，
      // 并且ObScanner当前内存最大限制是2MB
      class RowIterator : public Iterator
      {
        public:
          RowIterator();
          virtual ~RowIterator();
          RowIterator(const RowIterator &other);
          //RowIterator(const Iterator &other);
          RowIterator(const ObScanner *scanner, const ObScanner::MemBlock *mem_block,
                            int64_t size_counter = 0, int64_t cur_pos = 0);
        public:
          int get_row(ObCellInfo **cell_info, int64_t *num);
          RowIterator &operator ++ ();
          RowIterator operator ++ (int);
          bool operator == (const ObScanner::RowIterator &other) const;
          bool operator != (const ObScanner::RowIterator &other) const;
          RowIterator &operator= (const RowIterator &other);
          //RowIterator &operator= (const Iterator &other);
        protected:
          friend class ObScanner;
          bool is_iterator_end_();
          void update_cur_mem_block_();
          int get_row_();
          ObObj cur_rowkey_objs_[OB_MAX_ROWKEY_COLUMN_NUMBER];
          ObCellInfo cur_row_[OB_MAX_COLUMN_NUMBER];
          int64_t row_cell_num_;
          int64_t row_start_pos_;
          int64_t row_iter_size_counter_;
      };

      public:
        ObScanner();
        virtual ~ObScanner();

        void reset();
        void clear();

        int64_t set_mem_size_limit(const int64_t limit);

        /// @brief add a cell in the ObScanner
        /// @retval OB_SUCCESS successful
        /// @retval OB_SIZE_OVERFLOW memory limit is exceeded if add this cell
        /// @retval otherwise error
        int add_cell(const ObCellInfo &cell_info, const bool is_compare_rowkey = true, 
            const bool is_row_changed = false);

        int rollback();

        // for translate binary rowkey to obj array rowkey while deserialize.
        // for translate obj array rowkey to binary rowkey while serialize.
        inline void set_compatible_schema(const ObSchemaManagerV2* sm) 
        {
          schema_manager_ = sm;
        }

        // if is_binary_rowkey_format_ is true, serialize obj array rowkey to binary
        // format rowkey for compatibile with old clients.
        inline void set_binary_rowkey_format(const bool is_binary_rowkey_format)
        {
          is_binary_rowkey_format_ = is_binary_rowkey_format;
        }

        NEED_SERIALIZE_AND_DESERIALIZE;

      private:
        /// append a new cell, the cell is serialized into inner memory buffer
        /// after serialize this cell, check mem_size_limit, return OB_SIZE_OVERFLOW when exceeded
        int append_serialize_(const ObCellInfo &cell_info, const bool is_compare_rowkey = true, 
            const bool is_row_changed = false);

        int serialize_table_name_or_id_(char* buf, const int64_t buf_len, int64_t &pos,
            const ObCellInfo &cell_info);

        int serialize_row_key_(char* buf, const int64_t buf_len, int64_t &pos,
            const ObCellInfo &cell_info);

        int serialize_column_(char* buf, const int64_t buf_len, int64_t &pos,
            const ObCellInfo &cell_info);

        int serialize_rowkey_in_binary_format(char* buf, const int64_t buf_len, int64_t &pos, 
            const uint64_t table_id, const ObString& table_name, const ObRowkey& rowkey) const;

        /// alocate a new memblock to store ObCellInfo
        int get_memblock_(const int64_t size);

        /// when deserialization, iterate all ObCellInfo and get the size
        int64_t get_valid_data_size_(const char* data, const int64_t data_len, int64_t &pos, ObObj &last_obj);
      private:
        // BASIC_PARAM_FIELD 
        int serialize_basic_param(char * buf, const int64_t buf_len, int64_t & pos) const;
        int deserialize_basic_param(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj);
        int64_t get_basic_param_serialize_size(void) const;

        // MEAT_PARAM_FIELD
        int serialize_meta_param(char * buf, const int64_t buf_len, int64_t & pos) const;
        int deserialize_meta_param(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj);
        int64_t get_meta_param_serialize_size(void) const;

        // TABLE_PARAM_FIELD
        int serialize_table_param(char * buf, const int64_t buf_len, int64_t & pos) const;
        int deserialize_table_param(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj);
        int64_t get_table_param_serialize_size(void) const;

        // END_PARAM_FIELD
        int serialize_end_param(char * buf, const int64_t buf_len, int64_t & pos) const;
        int deserialize_end_param(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj);
        int64_t get_end_param_serialize_size(void) const;

      public:
        static int deserialize_int_(const char* buf, const int64_t data_len, int64_t& pos,
            int64_t &value, ObObj &last_obj);

        static int deserialize_varchar_(const char* buf, const int64_t data_len, int64_t& pos,
            ObString &value, ObObj &last_obj);

        static int deserialize_int_or_varchar_(const char* buf, const int64_t data_len, int64_t& pos,
            int64_t& int_value, ObString &varchar_value, ObObj &last_obj);

      public:
        Iterator begin() const;
        RowIterator row_begin() const;
        Iterator end() const;
        RowIterator row_end() const;
      public:
        int reset_iter();
        int reset_row_iter();

        /// @retval OB_ITER_END iterate end
        /// @retval OB_SUCCESS go to the next cell succ
        int next_cell();

        int get_cell(oceanbase::common::ObCellInfo** cell);
        int get_cell(oceanbase::common::ObCellInfo** cell, bool* is_row_changed);
        int get_cell(oceanbase::common::ObInnerCellInfo** cell);
        int get_cell(oceanbase::common::ObInnerCellInfo** cell, bool* is_row_changed);

        int next_row();
        int get_row(ObCellInfo **cell_info, int64_t *num);

        bool is_empty() const;

        /// after deserialization, get_last_row_key can retreive the last row key of this scanner
        /// @param [out] row_key the last row key
        int get_last_row_key(ObRowkey &row_key) const;

      public:
        /// indicating the request was fullfiled, must be setted by cs and ups
        /// @param [in] is_fullfilled  缓冲区不足导致scanner中只包含部分结果的时候设置为false
        /// @param [in] fullfilled_item_num -# when getting, this means processed position in GetParam
        ///                                 -# when scanning, this means row number fullfilled
        int set_is_req_fullfilled(const bool &is_fullfilled, const int64_t fullfilled_item_num);
        int get_is_req_fullfilled(bool &is_fullfilled, int64_t &fullfilled_item_num) const;

        /// when response scan request, range must be setted
        /// the range is the seviced range of this tablet or (min, max) in updateserver
        /// @param [in] range
        int set_range(const ObNewRange& range);
        /// same as above but do not deep copy keys of %range, just set the reference.
        /// caller ensures keys of %range remains reachable until ObScanner serialized.
        int set_range_shallow_copy(const ObNewRange& range);
        int get_range(ObNewRange& range) const;

        inline void set_data_version(const int64_t version)
        {
          data_version_ = version;
        }

        inline int64_t get_data_version() const
        {
          return data_version_;
        }

        inline int64_t get_size() const
        {
          return cur_size_counter_;
        }

        inline int64_t get_cell_num() const
        {
          return cur_cell_num_;
        }

        inline int64_t get_row_num() const
        {
          return cur_row_num_;
        }

        inline int64_t get_whole_result_row_num() const
        {
          return whole_result_row_num_;
        }

        inline void set_whole_result_row_num(int64_t new_whole_result_row_num)
        {
          whole_result_row_num_ = new_whole_result_row_num;
        }

        inline void set_id_name_type(const ID_NAME_STATUS type)
        {
          id_name_type_ = type;
        }

        inline ID_NAME_STATUS get_id_name_type() const
        {
          return (ID_NAME_STATUS)id_name_type_;
        }
        // debug info
        void dump(void) const;
        void dump_all(int log_level) const; 
      private:
        MemBlock *head_memblock_;
        MemBlock *cur_memblock_;
        MemBlock *rollback_memblock_;
        int64_t cur_size_counter_;
        int64_t rollback_size_counter_;
        ObString cur_table_name_;
        uint64_t cur_table_id_;
        ObRowkey cur_row_key_;
        ObString tmp_table_name_;
        uint64_t tmp_table_id_;
        ObRowkey tmp_row_key_;
        bool has_row_key_flag_;
        int64_t mem_size_limit_;
        Iterator iter_;
        RowIterator row_iter_;
        int64_t cur_row_num_;
        int64_t cur_cell_num_;
        int64_t rollback_row_num_;
        int64_t rollback_cell_num_;
        mutable ObRowkey rollback_row_key_;
        mutable ObRowkey last_row_key_;
        bool first_next_;
        bool first_next_row_;
        ObNewRange range_;
        int64_t data_version_;
        bool has_range_;
        bool is_request_fullfilled_;
        int64_t  fullfilled_item_num_;
        int64_t id_name_type_;
        int64_t whole_result_row_num_;
        ObInnerCellInfo ugly_inner_cell_;

        const ObSchemaManagerV2* schema_manager_;
        bool is_binary_rowkey_format_;  // old binary rowkey format
        ModulePageAllocator mod_;
        mutable ModuleArena rowkey_allocator_;
    };
    typedef ObScanner::Iterator ObScannerIterator;
    inline bool ObScannerIterator::is_iterator_end_()
    {
      return(true == is_end_ || NULL == scanner_
          || 0 == scanner_->cur_size_counter_
          || iter_size_counter_ >= scanner_->cur_size_counter_);
    }
  } /* common */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_COMMON_OB_SCANNER_H_ */
