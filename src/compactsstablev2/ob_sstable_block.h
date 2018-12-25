#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_BLOCK_H_
#define OCEANBASE_COMAPCTSSTABLEV2_OB_SSTABLE_BLOCK_H_

#include <tbsys.h>
#include "common/ob_define.h"
#include "common/ob_compact_cell_iterator.h"
#include "ob_sstable_block_builder.h"

namespace oceanbase
{
  namespace compactsstablev2
  {
    class ObSSTableBlock
    {
    public:
      ObSSTableBlock()
      {
      }

      ~ObSSTableBlock()
      {
      }

      inline void reset()
      {
        block_builder_.reset();
      }

      inline void clear()
      {
        block_builder_.clear();
      }

      inline int64_t get_block_size() const
      {
        return block_builder_.get_block_size();
      }

      inline int add_row(const common::ObRowkey& row_key, 
          const common::ObRow& row_value)
      {
        int ret = common::OB_SUCCESS;
        if (common::OB_SUCCESS != (ret = block_builder_.add_row(
                row_key, row_value)))
        {
          TBSYS_LOG(WARN, "add_row error:ret=%d,row_key=%s,row_value=%s", 
              ret, to_cstring(row_key), to_cstring(row_value));
        }
        return ret;
      }

      inline int add_row(const common::ObRow& row)
      {
        int ret = common::OB_SUCCESS;
        if (common::OB_SUCCESS != (ret = block_builder_.add_row(row)))
        {
          TBSYS_LOG(WARN, "add_row error:ret=%d,row=%s",
              ret, to_cstring(row));
        }
        return ret;
      }

      inline int build_block(char*& buf, int64_t& length)
      {
        int ret = common::OB_SUCCESS;
        if (common::OB_SUCCESS != (ret = block_builder_.build_block(
                buf, length)))
        {
          TBSYS_LOG(WARN, "build block error:ret=%d", ret);
        }
        return ret;
      }

      inline void set_row_store_type(
          const common::ObCompactStoreType row_store_type)
      {
        block_builder_.set_row_store_type(row_store_type);
      }

      inline int32_t get_row_count()
      {
        return block_builder_.get_row_count();
      }
      
    private:    
      ObSSTableBlockBuilder block_builder_;
    };
  }
}
#endif

