#ifndef OCEANBASE_SSTABLE_BLOCK_BUILDER_H_
#define OCEANBASE_SSTABLE_BLOCK_BUILDER_H_

#include <tbsys.h>
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/ob_compact_cell_writer.h"
#include "common/ob_row.h"
#include "common/ob_rowkey.h"
#include "ob_sstable_store_struct.h"
#include "common/ob_compact_store_type.h"

class TestSSTableBlockBuilder_construction_Test;
class TestSSTableBlockBuilder_add_row1_Test;
class TestSSTableBlockBuilder_add_row2_Test;
class TestSSTableBlockBuilder_build_block1_Test;
class TestSSTableBlockBuilder_reset1_Test;

namespace oceanbase
{
  namespace compactsstablev2
  {
    class ObSSTableBlockBuilder
    {
    public:
      friend class ::TestSSTableBlockBuilder_construction_Test;
      friend class ::TestSSTableBlockBuilder_add_row1_Test;
      friend class ::TestSSTableBlockBuilder_add_row2_Test;
      friend class ::TestSSTableBlockBuilder_build_block1_Test;
      friend class ::TestSSTableBlockBuilder_reset1_Test;

    public:
      static const int64_t BLOCK_ROW_BUFFER_SIZE = 2 * 1024 * 1024;
      static const int64_t BLOCK_ROW_INDEX_BUFFER_SIZE = 64 * 1024;
      static const int64_t BLOCK_ROW_INDEX_SIZE
        = sizeof(ObSSTableBlockRowIndex);
      static const int64_t SSTABLE_BLOCK_HEADER_SIZE
        = sizeof(ObSSTableBlockHeader);
      static const int64_t SSTABLE_BLOCK_BUF_ALIGN_SIZE = 1024;
      
    public:
      ObSSTableBlockBuilder(
          common::ObCompactStoreType row_store_type = common::DENSE_SPARSE)
        : row_store_type_(row_store_type),
          row_buf_(NULL), 
          row_length_(0),
          row_buf_size_(0),
          row_index_buf_(NULL), 
          row_index_length_(0),
          row_index_buf_size_(0)
      {
        int ret = common::OB_SUCCESS;
        if (common::OB_SUCCESS != (ret = reset()))
        {
          TBSYS_LOG(WARN, "faile to reset:ret=%d", ret);
        }
        else
        {
          //success:do nothing
        }
      }

      ~ObSSTableBlockBuilder()
      {
        clear();
      }

      int reset();
      
      inline void clear()
      {
        if (NULL != row_buf_)
        {
          free_mem(row_buf_);
        }

        if (NULL != row_index_buf_)
        {
          free_mem(row_index_buf_);
        }
      }

      int add_row(const common::ObRowkey& row_key, 
          const common::ObRow& row_value);

      int add_row(const common::ObRow& row);

      inline int32_t get_row_count() const
      {
        return block_header_.row_count_;
      }

      int build_block(char*& buf, int64_t& size);

      inline int64_t get_block_size() const
      {
        //计算大小时，row索引数要加1
        return (row_length_ + row_index_length_ + BLOCK_ROW_INDEX_SIZE);
      }
      
    private:
      int add_row_index(const ObSSTableBlockRowIndex& row_index);

      inline char* get_cur_row_ptr()
      {
        return row_buf_ + row_length_;
      }

      inline char* get_cur_row_index_ptr()
      {
        return row_index_buf_ + row_index_length_;
      }

      inline int64_t get_row_remain() const
      {
        return (row_buf_size_ - row_length_);
      }

      inline int64_t get_row_index_remain() const
      {
        return (row_index_buf_size_ - row_index_length_);
      }

      int alloc_mem(char*& buf, const int64_t size);

      inline void free_mem(void* buf)
      {
        common::ob_free(buf);
        buf = NULL;
      }

   public:
      inline void set_row_store_type(
          const common::ObCompactStoreType row_store_type)
      {
        row_store_type_ = row_store_type;
      }

    private:     
      ObSSTableBlockHeader block_header_;
      common::ObCompactStoreType row_store_type_;

      char* row_buf_;
      int64_t row_length_;
      int64_t row_buf_size_;

      char* row_index_buf_;
      int64_t row_index_length_;
      int64_t row_index_buf_size_;
    };
  }//end namespace compactsstablev2
}//end namespace oceanbase
#endif

