#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_BLOCK_GETTER_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_BLOCK_GETTER_H_

#include "common/ob_string.h"
#include "common/ob_common_param.h"
#include "common/ob_rowkey.h"
#include "common/ob_compact_cell_iterator.h"
#include "ob_sstable_block_reader.h"
#include "sstable/ob_sstable_row_cache.h"

namespace oceanbase
{
  namespace compactsstablev2
  {
    class ObSSTableBlockGetter
    {
    public:
      static const int64_t DEFAULT_INDEX_BUF_SIZE = 256 * 1024;
      typedef ObSSTableBlockReader::const_iterator const_iterator;
      typedef ObSSTableBlockReader::iterator iterator;

    public:
      ObSSTableBlockGetter()
        : inited_(false), 
          row_cursor_(NULL)
      {
      }

      ~ObSSTableBlockGetter()
      {
      }
  
      inline int get_row(common::ObCompactCellIterator*& row)
      {
        int ret = common::OB_SUCCESS;
        row = &row_;
        return ret;
      }
  
      int init(const common::ObRowkey& row_key,
          const ObSSTableBlockReader::BlockData& block_data,
          const common::ObCompactStoreType& row_store_type);

      inline int get_cache_row_value(
          sstable::ObSSTableRowCacheValue& row_value)
      {
        return block_reader_.get_cache_row_value(row_cursor_, row_value);
      }

    private:
      inline int load_current_row(const_iterator row_index)
      {
        int ret = common::OB_SUCCESS;
        if (common::OB_SUCCESS != (ret = block_reader_.get_row(
                row_index, row_)))
        {
          TBSYS_LOG(WARN, "block get row error");
          ret = common::OB_ERROR;
        }
        return ret;
      }
  
    private:
      bool inited_;
      ObSSTableBlockReader block_reader_;
      const_iterator row_cursor_;
      common::ObCompactCellIterator row_;
    };
  }//end namespace sstable
}//end namespace oceanbase

#endif
