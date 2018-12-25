#include "ob_sstable_block_getter.h"
    
using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    int ObSSTableBlockGetter::init(const common::ObRowkey& row_key,
        const ObSSTableBlockReader::BlockData& block_data,
        const ObCompactStoreType& row_store_type)
    {
      int ret = OB_SUCCESS;

      block_reader_.reset();
      if (OB_SUCCESS != (ret = block_reader_.init(
              block_data, row_store_type)))
      {
        TBSYS_LOG(WARN, "block reader init error");
      }
      else
      {
        ObSSTableBlockReader::const_iterator iterator;
        iterator = block_reader_.lower_bound(row_key);
        ObRowkey key;
        if (iterator == block_reader_.end_index())
        {
          ret = OB_BEYOND_THE_RANGE;
        }
        else if (OB_SUCCESS != (ret = block_reader_.get_row_key(
                iterator, key)))
        {
          TBSYS_LOG(WARN, "get row key error");
        }
        else 
        {
          if (0 != key.compare(row_key))
          {
            ret = OB_BEYOND_THE_RANGE;
          }
          else if (OB_SUCCESS != (ret = load_current_row(iterator)))
          {
            TBSYS_LOG(WARN, "load current row error");
          }
          else
          {
            row_cursor_ = iterator;
          }
        }
      }

      return ret;
    }
  }//end namespace sstable
}//end namespace oceanbase
