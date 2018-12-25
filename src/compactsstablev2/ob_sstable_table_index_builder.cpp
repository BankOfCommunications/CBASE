#include "ob_sstable_table_index_builder.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    int ObSSTableTableIndexBuilder::add_item(const ObSSTableTableIndex& item)
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = buf_.add_item(&item, static_cast<int64_t>(OB_SSTABLE_TABLE_INDEX_SIZE))))
      {
        TBSYS_LOG(WARN, "buf add item error:item.block_data_offset_=%ld,"
            "item.block_endkey_offset_=%ld,index_size=%ld",
            item.block_data_offset_, item.block_endkey_offset_,
            OB_SSTABLE_TABLE_INDEX_SIZE);
      }
      
      return ret;
    }
  }//end namespace compactsstablev2
}//end namespace oceanbase

