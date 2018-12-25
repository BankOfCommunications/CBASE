/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_get_param.h for define get param 
 *
 * Authors: 
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_GET_PARAM_H_
#define OCEANBASE_COMMON_GET_PARAM_H_

#include "ob_common_param.h"
#include "ob_rowkey.h"
#include "page_arena.h"
#include "common/ob_transaction.h"

namespace oceanbase
{
  namespace common
  {
    class ObSchemaManagerV2; 

    class ObGetParam : public ObReadParam
    {
      static const int64_t DEFAULT_CELLS_BUF_SIZE = 64 * 1024; // 16KB
      static const int64_t DEFAULT_CELL_CAPACITY  = 512;
      static const int64_t MAX_CELL_CAPACITY  = OB_MAX_PACKET_LENGTH / sizeof(ObCellInfo);

    public:
      static const int64_t MAX_CELLS_PER_ROW = OB_MAX_COLUMN_NUMBER;
      struct ObRowIndex 
      {
        int32_t offset_;  //index of the cell in cells list
        int32_t size_;    //cells count of this row
      };
      static const char * OB_GET_ALL_COLUMN_NAME_CSTR; 
      static const ObString OB_GET_ALL_COLUMN_NAME;

    public:
      explicit ObGetParam(const bool deep_copy_args = false);

      /**
       * this constructor is only used for ups to get one row data 
       * from transfer sstable. the cell info must be with column id 
       * 0, table id and row key. user must ensure the cell info is 
       * legal, otherwise it will fail to use this get param to get
       * row data from transfer sstable. 
       */
      explicit ObGetParam(const ObCellInfo& cell_info, const bool deep_copy_args = false);
      virtual ~ObGetParam ();
      
      void set_deep_copy_args(const bool deep_copy_args);
      int copy(const ObGetParam& other);
      /**
       * add one cell into get param
       * 
       * @param cell_info the cell to add
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR or OB_INVALID_ARGUMENT
       */
      int add_cell(const ObCellInfo& cell_info);

      /**
       * add only one cell in get parameter, this function is only be 
       * used for ups to get one row 
       * 
       * @param cell_info the cell to add
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR or OB_INVALID_ARGUMENT
       */
      int add_only_one_cell(const ObCellInfo& cell_info);

      /**
       * get the number of cells in get parameter 
       * 
       * @return int64_t return the cells count
       */
      inline int64_t get_cell_size() const 
      {
        return cell_size_; 
      }

      /**
       * random access the cell in get parameter with index
       * 
       * @param index the index of cell to access
       * 
       * @return ObCellInfo* the current cell at this index
       */
      inline ObCellInfo* operator[](int64_t index) const 
      {
        ObCellInfo* cell = NULL;
        if (index >= 0 && index < get_cell_size())
        {
          cell = &cell_list_[index];
        }
        return cell;
      }

      /// clear the cell list and row index for free the allocated memory
      inline void destroy(void)
      {
        if (NULL != cell_list_ && cell_list_ != &fixed_cell_)
        {
          allocator_.free(reinterpret_cast<char*>(cell_list_));
          cell_list_ = NULL;
        }

        if (NULL != row_index_ && row_index_ != &fixed_row_index_)
        {
          allocator_.free(reinterpret_cast<char*>(row_index_));
          row_index_ = NULL;
        }
      }

      /**
       * rollback the last row, this function is used by merge server 
       * to ensure there is not part row in get parameter 
       *  
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR 
       */
      int rollback();

      /**
       * rollback the rows with cells count, this function will skip 
       * the last "count" cells
       * 
       * @param count cells count to skip at the end of get parameter
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR 
       */
      int rollback(const int64_t count);

      /**
       * reset get parameter, easier to reuse it.
       */
      void reset(bool deep_copy_args = false);

      /**
       * get how mang rows in get parameter 
       * 
       * @return int64_t the rows count in get parameter
       */
      inline int64_t get_row_size() const 
      {
        return row_size_; 
      }

      /**
       * get the row index array in get parameter, just use for 
       * chunkserver to optimize get by row 
       * 
       * @return const ObRowIndex* the row index array in get 
       *         parameter
       */
      inline const ObRowIndex* get_row_index() const 
      {
        return row_index_; 
      }

      NEED_SERIALIZE_AND_DESERIALIZE;

      /// get readable scan param info
      int64_t to_string(char *buf, int64_t buf_size) const;
      void print_memory_usage(const char* msg) const;

      inline void set_compatible_schema(const ObSchemaManagerV2* sm) { schema_manager_ = sm; }
      inline bool is_binary_rowkey_format() const { return is_binary_rowkey_format_; }
      //add zhujun [transaction read uncommit]2016/3/24
      inline int set_trans_id(ObTransID trans_id) { trans_id_=trans_id;return OB_SUCCESS; }
      inline ObTransID get_trans_id() const { return trans_id_;}
      //add:e
    private:
      int copy_cell_info(const ObCellInfo & cell_info, ObCellInfo &stored_cell);
      /**
       * allocate memory for cell_list_ and row_index_ if necessary 
       *  
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int init();
      int expand();

      /**
       * accorrding to the cell offset in cell list, find which row 
       * the cell belong to, return the index in row index array, we 
       * can ensure the row index array is in ascending order by cell 
       * offset, so we use binary search 
       * 
       * @param cell_offset cell offset in cell list
       * 
       * @return int64_t the row index which the cell belong to, if 
       *         not found, return -1
       */
      int64_t find_row_index(const int64_t cell_offset) const;

      /**
       * check whether the table name, table id, column name and 
       * column id of the specified cell are legal, and this function 
       * will set the "id_only_" flag 
       * 
       * @param cell_info cell to check
       * @param is_first if it's the first cell in cell list
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int check_name_and_id(const ObCellInfo& cell_info, bool is_first);

      //common type serialize and deserialzie helper
      int serialize_flag(char* buf, const int64_t buf_len, int64_t& pos, 
        const int64_t flag) const;
      int serialize_int(char* buf, const int64_t buf_len, int64_t& pos,
        const int64_t value) const;
      int deserialize_int(const char* buf, const int64_t buf_len, int64_t& pos,
        int64_t& value) const;
      int64_t get_obj_serialize_size(const int64_t value, bool is_ext) const;
      int64_t get_obj_serialize_size(const ObString& str) const;

      //name or id serialize, deserialize helper
      int serialize_name_or_id(char* buf, const int64_t buf_len, 
        int64_t& pos, const ObString name,
        const uint64_t id, bool is_field) const;
      int deserialize_name_or_id(const char* buf, const int64_t data_len, 
        int64_t& pos, ObString& name,
        uint64_t& id);
      int64_t get_serialize_name_or_id_size(const ObString name,
        const uint64_t id,
        bool is_field) const;

      //serialize and deserialize table info(table field, table name or table id)
      int serialize_table_info(char* buf, const int64_t buf_len, 
        int64_t& pos, const ObString table_name,
        const uint64_t table_id) const;
      int deserialize_table_info(const char* buf, const int64_t buf_len, 
        int64_t& pos, ObString& table_name,
        uint64_t& table_id);
      int64_t get_serialize_table_info_size(const ObString table_name,
        const uint64_t table_id) const;

      //serialize column info(column name or column id)
      int serialize_column_info(char* buf, const int64_t buf_len, 
        int64_t& pos, const ObString column_name,
        const uint64_t column_id) const;
      int64_t get_serialize_column_info_size(const ObString column_name,
        const uint64_t column_id) const;

      //serialize, deserialize row key
      int serialize_rowkey(char* buf, const int64_t buf_len, 
        int64_t& pos, const ObRowkey& row_key) const;
      int deserialize_rowkey(const char* buf, const int64_t data_len, 
        int64_t& pos, ObRowkey& row_key);
      int64_t get_serialize_rowkey_size(const ObRowkey& row_key) const;
      int deserialize_binary_rowkey(const char* buf, const int64_t data_len, 
          int64_t& pos, const ObRowkeyInfo& rowkey_info, ObRowkey& row_key);

      //basic parameter serialize and deserialize
      int serialize_basic_field(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize_basic_field(const char* buf, const int64_t data_len, int64_t& pos);
      int64_t get_basic_field_serialize_size(void) const;

      //cells serialize and deserialize
      bool is_table_change(ObCellInfo& cell, ObString& table_name, 
        uint64_t& table_id) const;
      bool is_rowkey_change(ObCellInfo& cell, ObRowkey& row_key) const;
      int serialize_cells(char* buf, const int64_t buf_len, int64_t& pos) const;
      int64_t get_cells_serialize_size(void) const;

    private:
      DISALLOW_COPY_AND_ASSIGN(ObGetParam);
      bool deep_copy_args_;

      bool single_cell_;
      ObCellInfo fixed_cell_;
      ObRowIndex fixed_row_index_;

      ObCellInfo* cell_list_;   //cell array to store all cells
      int64_t cell_size_;       //cell count in cell list
      int64_t cell_capacity_;   //cell list capacity

      ObString prev_table_name_;//previous table name, for check if row key change
      uint64_t prev_table_id_;  //previous table id, for check if row key change
      ObRowkey prev_rowkey_;    //previous row key, for check if row key change
      ObRowIndex* row_index_;   //row index, for index cell list by row
      int32_t row_size_;        //how many row item in row index  
      bool id_only_;            //whether only id(table id, column id) in cell

      bool is_binary_rowkey_format_;  // old binary rowkey format
      const ObSchemaManagerV2* schema_manager_; // rowkey compatible information get from schema

      ModulePageAllocator mod_;
      ModuleArena allocator_;
      //add zhujun [transaction read uncommit]2016/3/24
      ObTransID trans_id_;
	  //add:e
      //add lbzhong [Update rowkey] 20160509:b
      bool has_new_row_;
      //add:e
    };
  } /* common */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_COMMON_GET_PARAM_H_ */
