#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_TABLE_SCHEMA_BUILDER_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_TABLE_SCHEMA_BUILDER_H_

#include <tbsys.h>
#include "common/ob_define.h"
#include "ob_sstable_buffer.h"
#include "ob_sstable_store_struct.h"

namespace oceanbase
{
  namespace compactsstablev2
  {
    class ObSSTableTableSchemaBuilder
    {
    public:
      static const int64_t OB_SSTABLE_TABLE_SCHEMA_ITEM_SIZE = sizeof(ObSSTableTableSchemaItem);

    public:
      ObSSTableTableSchemaBuilder()
      {
      }

      ~ObSSTableTableSchemaBuilder()
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

      int add_item(const ObSSTableTableSchemaItem& item);

      inline int build_table_schema(char* const buf, const int64_t buf_size,
          int64_t& length)
      {
        int ret = common::OB_SUCCESS;

        if (common::OB_SUCCESS != (ret = buf_.get_data(buf, buf_size, length)))
        {
          TBSYS_LOG(WARN, "buf get data error:ret=%d,buf=%p,buf_size=%ld,"
              "length=%ld", ret, buf, buf_size, length);
        }

        return ret;
      }

      inline int build_table_schema(const char*& buf, int64_t& length)
      {
        int ret = common::OB_SUCCESS;

        if (common::OB_SUCCESS != (ret = buf_.get_data(buf, length)))
        {
          TBSYS_LOG(WARN, "buf get data error:ret=%d,buf=%p,length=%ld",
              ret, buf, length);
        }

        return ret;
      }
      
    private:
      ObSSTableBuffer buf_;
    };
  }
}
#endif

