#include "ob_sstable.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    int ObSSTable::finish_one_table()
    {
      int ret = OB_SUCCESS;
      ObSSTableTableSchemaItem item;
      uint64_t table_id = table_index_.table_id_;
      const ObSSTableSchemaColumnDef* def = NULL;
      int64_t size = 0;
      memset(&item, 0, sizeof(item));

      if (NULL == (def = table_schema_.get_table_schema(table_id, 
              true, size)))
      {
        TBSYS_LOG(WARN, "get table schema error");
        ret = OB_ERROR;
      }
      else
      {
        for (int64_t i = 0; i < size; i ++)
        {
          item.table_id_ = table_id;
          item.column_id_ = static_cast<uint32_t>(def[i].column_id_);
          item.rowkey_seq_ = static_cast<uint16_t>(def[i].rowkey_seq_);
          item.column_attr_ = 0;
          item.column_value_type_ = static_cast<uint16_t>(def[i].column_value_type_);
          for (int64_t j = 0; j < 3; j ++)
          {
            item.reserved16_[j] = 0;
          }

          if (OB_SUCCESS != (ret = table_schema_builder_.add_item(item)))
          {
            TBSYS_LOG(WARN, "add item error:ret=%d", ret);
            break;
          }
          else
          {
            table_index_.column_count_ ++;
            table_index_.columns_in_rowkey_ ++;
            sstable_header_.schema_array_unit_count_ ++;
          }
        }
      }

      if (OB_SUCCESS != ret)
      {
      }
      else if (NULL == (def = table_schema_.get_table_schema(table_id,
              false, size)))
      {
      }
      else
      {
        for (int64_t i = 0; i < size; i ++)
        {
          item.table_id_ = table_id;
          item.column_id_ = static_cast<uint16_t>(def[i].column_id_);
          item.rowkey_seq_ = static_cast<uint16_t>(def[i].rowkey_seq_);
          item.column_attr_ = 0;
          item.column_value_type_ = static_cast<uint16_t>(def[i].column_value_type_);
          for (int64_t j = 0; j < 3; j++)
          {
            item.reserved16_[j] = 0;
          }

          if (OB_SUCCESS != (ret = table_schema_builder_.add_item(item)))
          {
            TBSYS_LOG(WARN, "add item error:ret=%d", ret);
            break;
          }
          else
          {
            sstable_header_.schema_array_unit_count_ ++;
            table_index_.column_count_ ++;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = table_index_builder_.add_item(table_index_)))
        {
          TBSYS_LOG(WARN, "table index builder add item error:ret=%d", ret);
        }
        else
        {
          sstable_header_.table_count_ ++;
        }
      }

      return ret;
    }

    int ObSSTable::check_row(const ObRowkey& rowkey, const ObRow& rowvalue)
    {
      int ret = OB_SUCCESS;

      if (DENSE_DENSE == sstable_header_.row_store_type_)
      {
        uint64_t table_id = table_index_.table_id_;
        if (OB_SUCCESS != (ret = table_schema_.check_row(table_id,
                rowkey, rowvalue)))
        {
          char rowkey_buf[1024];
          char rowvalue_buf[1024];
          rowkey.to_string(rowkey_buf, 1024);
          rowvalue.to_string(rowvalue_buf, 1024);
          TBSYS_LOG(WARN, "table schema check row fail:table_id=%lu,"
              "rowkey=%s,rowvalue=%s", table_id, rowkey_buf, rowvalue_buf);
        }
      }

      return ret;
    }

    int ObSSTable::check_row(const common::ObRow& row)
    {
      int ret = OB_SUCCESS;

      if (DENSE_DENSE == sstable_header_.row_store_type_)
      {
        uint64_t table_id = table_index_.table_id_;
        if (OB_SUCCESS != (ret = table_schema_.check_row(table_id, row)))
        {
          char buf[1024];
          row.to_string(buf, 1024);
          TBSYS_LOG(WARN, "table schema check row fail:table_id=%lu,row=%s", 
              table_id, buf);
        }
      }

      return ret;
    }
  }//end namepace compactsstablev2
}//end namespace oceanbase

