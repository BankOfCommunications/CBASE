#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_BLOCK_READER_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_BLOCK_READER_H_

#include "common/ob_define.h"
#include "common/ob_compact_cell_iterator.h"
#include "common/ob_rowkey.h"
#include "common/ob_tsi_factory.h"
#include "sstable/ob_sstable_row_cache.h"
#include "ob_sstable_store_struct.h"

namespace oceanbase
{
  namespace compactsstablev2
  {
    class ObSSTableBlockReader
    {
    public:
      //block表示(row index buf + row data buf)
      //row index buf包含(row offset, row size),
      //在sstable文件中，只有row offset
      struct BlockData
      {
        //解析后的row index的存储位置(空间在ObSSTableBlockScanner内申请，
        //当不足时在ObSSTableBlockReader内申请
        char* internal_buf_;         //index buf
        int64_t internal_buf_size_;  //index size
        const char* data_buf_;       //block buf
        int64_t data_buf_size_;      //block size

        BlockData()
          : internal_buf_(NULL),
            internal_buf_size_(0),
            data_buf_(NULL),
            data_buf_size_(0)
        {
        }

        BlockData(char* ib, const int64_t ib_sz,
            const char* db, const int64_t db_sz)
          : internal_buf_(ib), internal_buf_size_(ib_sz),
            data_buf_(db), data_buf_size_(db_sz)
        {
        }

        bool available() const
        {
          return (NULL != internal_buf_) && (0 < internal_buf_size_)
            && (NULL != data_buf_) && (0 < data_buf_size_);
        }
      };

      //单条row index(offset, size)
      struct RowIndexItemType
      {
        int32_t offset_;   //row开始offset
        int32_t size_;     //row size
        //int32_t key_offset_;
        //int32_t key_size_;
        //int32_t value_offset_;
        //int32_t value_size_;
      };

      //重载lower_bound的比较类
      //根据row index找到对应的rowkey然后与要比较的key进行比较
      class Compare
      {
      public:
        Compare(const ObSSTableBlockReader& block_reader) 
          : block_reader_(block_reader)
        {
        }

        inline bool operator()(const RowIndexItemType& index, 
            const common::ObRowkey& key)
        {
          bool ret = false;
          int ret1 = common::OB_SUCCESS;
          common::ObRowkey row_key;
          if (common::OB_SUCCESS != (ret1 = block_reader_.get_row_key(
                  &index, row_key)))
          {
            TBSYS_LOG(WARN, "block reader get row key error:ret1=%d," \
                "index.offset_=%d,index.size_=%d",
                ret1, index.offset_, index.size_);
          }
          else
          {
            ret = (row_key.compare(key) < 0);
          }
          return ret;
        }
      
      private:
        const ObSSTableBlockReader& block_reader_;
      };

    public:
      typedef RowIndexItemType* iterator;
      typedef const RowIndexItemType* const_iterator;

    public:
      //sstable中row index size(offset)
      static const int32_t ROW_INDEX_ITEM_SIZE 
        = static_cast<int32_t>(sizeof(ObSSTableBlockRowIndex));       
      static const int32_t BLOCK_HEADER_SIZE 
        = static_cast<int32_t>(sizeof(ObSSTableBlockHeader));
      static const int32_t INTERNAL_ROW_INDEX_ITEM_SIZE 
        = static_cast<int32_t>(sizeof(RowIndexItemType));

    public:
      ObSSTableBlockReader()
        : data_begin_(NULL),
          data_end_(NULL),
          index_begin_(NULL),
          index_end_(NULL),
          row_store_type_(common::INVALID_COMPACT_STORE_TYPE)
      {
        memset(&block_header_, 0, sizeof(block_header_));
      }

      ~ObSSTableBlockReader()
      {
        index_begin_ = NULL;
        index_end_ = NULL;
        data_begin_ = NULL;
        data_end_ = NULL;
      }

      inline int reset()
      {
        int ret = common::OB_SUCCESS;
        index_begin_ = NULL;
        index_end_ = NULL;
        data_begin_ = NULL;
        data_end_ = NULL;
        row_store_type_ = common::INVALID_COMPACT_STORE_TYPE;
        memset(&block_header_, 0, sizeof(block_header_));
        return ret;
      }

      int init(const BlockData& data, 
          const common::ObCompactStoreType& row_store_type);
      
      int get_row(const_iterator index, 
          common::ObCompactCellIterator& row) const;

      int get_row_key(const_iterator index, common::ObRowkey& key) const;

      inline ObSSTableBlockReader::const_iterator lower_bound(
          const common::ObRowkey& key)
      {
        return std::lower_bound(index_begin_, index_end_, key, Compare(*this));
      }

      inline const_iterator begin_index() const
      {
        return index_begin_;
      }

      inline const_iterator end_index() const
      {
        return index_end_;
      }

      int get_cache_row_value(const_iterator index,
          sstable::ObSSTableRowCacheValue& row_value) const;

      inline const ObSSTableBlockHeader* get_block_header() const
      {
        return &block_header_;
      }
    
    private:
      int get_row_key(common::ObCompactCellIterator& row, 
          common::ObRowkey& key) const;

      inline const char* find_row(const_iterator index) const
      {
        return (data_begin_ + index->offset_);
      }
          
    private:
      ObSSTableBlockHeader block_header_;
      const char* data_begin_;              //contain the block header
      const char* data_end_;
      const_iterator index_begin_;
      const_iterator index_end_;
      common::ObCompactStoreType row_store_type_;
      mutable common::ObObj rowkey_buf_array_[common::OB_MAX_ROWKEY_COLUMN_NUMBER]; //用于rowkey比较的临时Obj数组
    };
  }//end namespace compactsstablev2
}//end namesapce oceanbase
#endif
