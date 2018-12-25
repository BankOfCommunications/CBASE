/**
 *  (C) 2010-2011 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  ob_sstable_block_reader.h is for what ...
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 *        
 */
#ifndef OCEANBASE_SSTABLE_SSTABLE_BLOCK_READER_H_
#define OCEANBASE_SSTABLE_SSTABLE_BLOCK_READER_H_

#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_object.h"
#include "ob_sstable_block_builder.h"
#include "ob_sstable_row_cache.h"

namespace oceanbase 
{ 
  namespace common
  {
    class ObRowkey;
    class ObRowkeyInfo;
    class ObRow;
  }
  namespace sstable 
  {

    class ObSSTableSchema;
    class ObSimpleColumnIndexes;


    class ObSSTableBlockReader
    {
      public:
        ObSSTableBlockReader();
        ~ObSSTableBlockReader();
      public:
        struct BlockData
        {
          char* internal_buffer_;
          int64_t internal_bufsiz_;
          const char* data_buffer_;
          int64_t data_bufsiz_;

          BlockData() 
            :  internal_buffer_(NULL), internal_bufsiz_(0), 
            data_buffer_(NULL),  data_bufsiz_(0)
          {
          }

          BlockData(char* ib, const int64_t ibsz, 
              const char* db, const int64_t dbsz)
            : internal_buffer_(ib), internal_bufsiz_(ibsz), 
            data_buffer_(db),  data_bufsiz_(dbsz)
          {
          }

          inline bool available() const 
          {
            return NULL != internal_buffer_ && 0 < internal_bufsiz_ 
              && NULL != data_buffer_ &&  0 < data_bufsiz_;
          }
        };

        struct BlockDataDesc
        {
          const common::ObRowkeyInfo* rowkey_info_;
          int64_t rowkey_column_count_;
          int64_t store_style_;
          BlockDataDesc() 
            : rowkey_info_(NULL), rowkey_column_count_(0), store_style_(0) 
          {
          }
          BlockDataDesc(
              const common::ObRowkeyInfo* rowkey_info,
              const int64_t rk_col_count,
              const int64_t store_style)
            : rowkey_info_(rowkey_info), 
            rowkey_column_count_(rk_col_count), 
            store_style_(store_style) 
          {
          }
          inline bool available() const 
          {
            bool ret = true;
            if (0 == rowkey_column_count_)
            {
              ret = (NULL != rowkey_info_);
            }
            else if (0 > rowkey_column_count_)
            {
              ret = false;
            }
            return ret;
          }
        };

        struct IndexEntryType
        {
          int32_t offset_;
          int32_t size_;
        };
        typedef IndexEntryType* iterator;
        typedef const IndexEntryType* const_iterator;
        typedef int RowFormat;
      public:
        class Compare
        {
          public:
            Compare(const ObSSTableBlockReader& reader) : reader_(reader) {}
            bool operator()(const IndexEntryType& index, const common::ObRowkey& key);
          private:
            const ObSSTableBlockReader& reader_;
        };
      public:
        int deserialize(const BlockDataDesc & block_data_desc, const BlockData& block_data);
        int reset();

        /**
         * get rowkey of sstable row
         * @param index row iterator
         * @param [out] key rowkey at %index
         */
        int get_row_key(const_iterator index, common::ObRowkey& key) const;


        /**
         * get whole row content of sstable row, includes rowkey and all columns value
         * @param format sstable row format @see RowFormat
         * @param index row iterator
         * @param [out] key rowkey at %index
         * @param [out] ids SPARSE format stored column id array.
         * @param [out] values columns value object array.
         * @param [in,out] column_count columns count.
         */
        int get_row(const RowFormat format, const_iterator index,
            common::ObRowkey& key, common::ObObj* ids, 
            common::ObObj* values, int64_t& column_count, bool is_scan = true) const;

        /**
         * get user request columns;
         * @param [in] format sstable row format @see RowFormat
         * @param [in] index row iterator
         * @param [in] is_full_row_columns, query all columns in row?
         * @param [in] query_columns user query columns
         * @param [out] key rowkey at %index
         * @param [out] value columns value object array.
         */
        int get_row(const RowFormat format, const_iterator index,
            const bool is_full_row_columns,
            const ObSimpleColumnIndexes& query_columns, 
            common::ObRowkey& key, common::ObRow& value) const;

        const_iterator lower_bound(const common::ObRowkey& key);
        const_iterator find(const common::ObRowkey& key);

        inline const_iterator begin() const { return index_begin_; }
        inline const_iterator end() const { return index_end_; }
        inline int get_row_count() const { return header_.row_count_; }

        inline const BlockDataDesc & get_block_data_desc() const { return block_data_desc_; }
        //WARNING: this function must be called after deserialize()
        int get_cache_row_value(const_iterator index, 
          ObSSTableRowCacheValue& row_value) const;

      private:
        /**
         * @param [in] format: dense  ObObjs array;
         *                     sparse Id(ObInt) + col(ObObj) array;
         * @param [in] index: row index
         * @param [out] ids: if format is sparse, ids return all id objects;
         * @param [out] values: return all column values array.
         * @param [in,out] count: input max count of ids and values,
         * and return actually in row.
         *
         */
        int get_objs(const RowFormat format, const_iterator index, int64_t& pos,
            common::ObObj* ids, common::ObObj* values, int64_t& column_count) const;

        int get_row_key(const_iterator index, common::ObRowkey& key, int64_t &pos) const;
        static int deserialize_sstable_rowkey(const char* buf, const int64_t data_len, common::ObString& key);

        int get_dense_row(const_iterator index,
            const ObSimpleColumnIndexes& query_columns, 
            common::ObRowkey& key, common::ObRow& value) const;
        int get_dense_full_row(const_iterator index,
            common::ObRowkey& key, common::ObRow& value) const;
        int get_sparse_row(const_iterator index,
            const ObSimpleColumnIndexes& query_columns, 
            common::ObRowkey& key, common::ObRow& value) const;

      private:
          /** 
           * index_begin_ , index_end_
           * point to block internal index array
           */
          const_iterator index_begin_;
          const_iterator index_end_;
          /**
           * point to row data array
           * every row represents as:
           *   -----------------------------------------------------------------------------------------
           *   ITEM NAME | rowkey length | rowkey byte stream | ObObj vals[0] | vals[column_count-1] |
           *   -----------------------------------------------------------------------------------------
           *   ITEM SIZE | 2bytes        | rowkey length      | length of all values is index[row_index].size
           *   -----------------------------------------------------------------------------------------
           */
          const char* data_begin_;
          const char* data_end_;
          ObSSTableBlockHeader header_;

          BlockDataDesc block_data_desc_;
          mutable common::ObObj rowkey_buf_array_[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
    };

  } // end namespace sstable
} // end namespace oceanbase


#endif //OCEANBASE_SSTABLE_SSTABLE_BLOCK_READER_H_

