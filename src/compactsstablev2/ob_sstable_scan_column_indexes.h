#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_SCAN_COLUMN_INDEXES_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_SCAN_COLUMN_INDEXES_H_

#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/utility.h"

namespace oceanbase 
{ 
  namespace compactsstablev2
  {
    class ObSSTableScanColumnIndexes
    {
      public:
        enum ColumnType
        {
          Normal = 0,
          NotExist = 1,
          Rowkey = 2
        };

      public:
        struct Column
        {
          ColumnType type_;  //Normal/NotExist/Rowkey
          int64_t index_;    //在sstable schema中，
                             //相对于rowkey/rowvalue起始位置的偏移
          uint64_t id_;      //column id

          Column()
            : type_(Normal),
              index_(0),
              id_(0)
          {
          }

          Column(const ColumnType type, const int64_t index, const uint64_t id)
            : type_(type),
              index_(index),
              id_(id)
          {
          }
        };

      public:
        static const int32_t COLUMN_ITEM_SIZE
          = static_cast<int32_t>(sizeof(Column));

      public:
        ObSSTableScanColumnIndexes(const int64_t column_size = common::OB_MAX_COLUMN_NUMBER)
          : column_info_(NULL),
            column_info_size_(column_size),
            column_cnt_(0)
        {
        }

        ~ObSSTableScanColumnIndexes()
        {
          if (NULL != column_info_)
          {
            common::ob_free(column_info_);
          }
        }

        inline void reset()
        {
          //reset不重置column_info_size_
          column_cnt_ = 0;
        }

        int add_column_id(const ColumnType type, const int64_t index, 
            const uint64_t column_id);

        int get_column(const int64_t index, Column& column) const;

        inline int64_t get_column_count() const 
        { 
          return column_cnt_;
        }

        inline const Column* get_column_info() const
        {
          return column_info_;
        }

        int64_t to_string(char* buf, const int64_t buf_len) const;

      private:
        Column* column_info_;
        int64_t column_info_size_;
        int64_t column_cnt_;
    };
  }//end namespace compactsstablev2
}//end namespace oceanbase

#endif
