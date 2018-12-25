#include "ob_sstable_table_schema_builder.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    int ObSSTableTableSchemaBuilder::add_item(const ObSSTableTableSchemaItem& item)
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = buf_.add_item(&item, static_cast<int64_t>(OB_SSTABLE_TABLE_SCHEMA_ITEM_SIZE))))
      {
        TBSYS_LOG(WARN, "buf add item error:ret=%d,item.table_id_=%lu,"
            "item.column_id_=%u,item.rowkey_seq_=%u,item.column_attr_=%u,"
            "item.column_value_type_=%u"
            "OB_SSTABLE_TABLE_SCHEMA_ITEM_SIZE", ret, item.table_id_,
            item.column_id_, item.rowkey_seq_, item.column_attr_,
            item.column_value_type_);
      }

      return ret;
    }
  }//end namespace compactsstablev2
}//end namespace oceanbase

