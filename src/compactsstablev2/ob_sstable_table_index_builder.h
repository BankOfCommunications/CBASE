#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_TABLE_INDEX_BUILDER_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_TABLE_INDEX_BUILDER_H_

#include <tbsys.h>
#include "common/ob_define.h"
#include "common/ob_compact_cell_writer.h"
#include "common/ob_compact_store_type.h"
#include "ob_sstable_buffer.h"
#include "ob_sstable_store_struct.h"

namespace oceanbase
{
  namespace compactsstablev2
  {
    class ObSSTableTableIndexBuilder
    {
    public:
      static const int64_t OB_SSTABLE_TABLE_INDEX_SIZE = sizeof(ObSSTableTableIndex);

    public:
      ObSSTableTableIndexBuilder()
      {
      }

      ~ObSSTableTableIndexBuilder()
      {
      }

      inline void reset()
      {
        buf_.reset();
      }

      inline void clear()
      {
        buf_.clear();
      }

      inline int64_t get_length() const
      {
        return buf_.get_length();
      }

      inline int64_t get_buf_block_count() const
      {
        return buf_.get_block_count();
      }
    
      int add_item(const ObSSTableTableIndex& item);

      inline int build_table_index(char* const buf, const int64_t buf_size,
          int64_t& length)
      {
        int ret = common::OB_SUCCESS;

        if (common::OB_SUCCESS != (ret = buf_.get_data(buf, buf_size, length)))
        {
          TBSYS_LOG(WARN, "get data error");
        }

        return ret;
      }
      
      inline int build_table_index(const char*& buf, int64_t& length)
      {
        int ret = common::OB_SUCCESS;

        if (common::OB_SUCCESS != (ret = buf_.get_data(buf, length)))
        {
          TBSYS_LOG(WARN, "get data error");
        }

        return ret;
      }

    private:
      ObSSTableBuffer buf_;
    };
  }
}
#endif

