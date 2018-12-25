
#include "sstable/ob_sstable_writer.h"

#define TABLE_ID 1001

using namespace oceanbase;
using namespace common;
using namespace sstable;

#define OB_STR(str) (ObString(0, (int32_t)strlen(str), str))

int main()
{

  ob_init_memory_pool();

  ObSSTableWriter writer;
  ObSSTableSchema schema;

  ObSSTableSchemaColumnDef column_def;

  column_def.table_id_ = TABLE_ID;
  column_def.column_group_id_ = 0;
  column_def.rowkey_seq_ = 1;
  column_def.column_name_id_ = 2;
  column_def.column_value_type_ = ObVarcharType;

  schema.add_column_def(column_def);

  column_def.table_id_ = TABLE_ID;
  column_def.column_group_id_ = 0;
  column_def.rowkey_seq_ = 0;
  column_def.column_name_id_ = 3;
  column_def.column_value_type_ = ObIntType;

  schema.add_column_def(column_def);

  writer.create_sstable(schema, OB_STR("./tmp/kkkk"), OB_STR("lzo_1.0"), 1);

  ObObj rowkey;
  rowkey.set_varchar(OB_STR("guorui"));

  ObObj value;
  value.set_int(3);

  ObSSTableRow row;
  row.set_table_id(TABLE_ID);
  row.set_column_group_id(0);
  row.add_obj(rowkey);
  row.add_obj(value);

  int64_t space_usage = 0;
  writer.append_row(row, space_usage);

  //row.clear();
  //row.set_table_id(TABLE_ID);
  //row.set_column_group_id(0);

  //writer.append_row(row, space_usage);
  
  int64_t offset = 0;
  writer.close_sstable(offset);
}

