#include <gtest/gtest.h>
#include "common/ob_schema.h"
#include "common/ob_malloc.h"
#include "common/ob_define.h"
#include "common/ob_schema_table.h"
#include "stdlib.h"

using namespace oceanbase;
using namespace oceanbase::common;


bool str_equal(const char* expected, ObObj value)
{
  if(ObVarcharType != value.get_type())
  {
    return false;
  }

  ObString str;
  value.get_varchar(str);
  return !strncmp(expected, str.ptr(), str.length());
}

bool int_equal(int64_t expected, ObObj value)
{
  if(ObIntType != value.get_type())
  {
    return false;
  }

  int64_t tmp = 0;
  value.get_int(tmp);
  return expected == tmp;
}

TEST(TestSchemaTable, dump_schema_manager)
{
  tbsys::CConfig c1;
  ObSchemaManagerV2 *schema_manager = new ObSchemaManagerV2();
  ASSERT_EQ(true, schema_manager->parse_from_file("./test_schema_table.ini", c1));
  schema_manager->print_info();

  ObObj value;

  const ObTableSchema* table = schema_manager->get_table_schema(1001);

  for(uint32_t i=0;i<ARRAYSIZEOF(TABLE_OR_MAP);i++)
  {
    TABLE_OR_MAP[i].func_(*table, value);
    const char* col = TABLE_OR_MAP[i].column_name_;

    if(!strcmp(col, "table_name"))
    {
      ASSERT_EQ(true, str_equal("collect_info", value));
    }
    else if(!strcmp(col, "table_id"))
    {
      ASSERT_EQ(true, int_equal(1001, value));
    }
    else if(!strcmp(col, "table_type"))
    {
      ASSERT_EQ(true, int_equal(1, value));
    }
    else if(!strcmp(col, "rowkey_len"))
    {
      ASSERT_EQ(true, int_equal(17, value));
    }
    else if(!strcmp(col, "rowkey_is_fixed_len"))
    {
      ASSERT_EQ(true, int_equal(1, value));
    }
    else if(!strcmp(col, "rowkey_split"))
    {
      ASSERT_EQ(true, int_equal(8, value));
    }
    else if(!strcmp(col, "compressor"))
    {
      ASSERT_EQ(true, str_equal("none", value));
    }
    else if(!strcmp(col, "max_column_id"))
    {
      ASSERT_EQ(true, int_equal(22, value));
    }
  }

  const ObColumnSchemaV2* column = schema_manager->get_column_schema(1001, 2);
  for(uint32_t i=0;i<ARRAYSIZEOF(COLUMN_OR_MAP);i++)
  {
    COLUMN_OR_MAP[i].func_(*schema_manager, *column, value);
    const char* col = COLUMN_OR_MAP[i].column_name_;
    if(!strcmp(col, "table_name"))
    {
      ASSERT_EQ(true, str_equal("collect_info", value));
    }
    else if(!strcmp(col, "table_id"))
    {
      ASSERT_EQ(true, int_equal(1001, value));
    }
    else if(!strcmp(col, "column_name"))
    {
      ASSERT_EQ(true, str_equal("user_nicker", value));
    }
    else if(!strcmp(col, "column_id"))
    {
      ASSERT_EQ(true, int_equal(2, value));
    }
    else if(!strcmp(col, "data_type"))
    {
      ASSERT_EQ(true, int_equal((int64_t)ObVarcharType, value));
    }
    else if(!strcmp(col, "data_length"))
    {
      ASSERT_EQ(true, int_equal(32, value));
    }
    else if(!strcmp(col, "column_group_id"))
    {
      ASSERT_EQ(true, int_equal(11, value));
    }
  }

  column = schema_manager->get_column_schema(1001, 13);
  for(uint32_t i=0;i<ARRAYSIZEOF(JOININFO_OR_MAP);i++)
  {
    JOININFO_OR_MAP[i].func_(*schema_manager, *column, value);
    const char* col = JOININFO_OR_MAP[i].column_name_;
    if(!strcmp(col, "left_table_name"))
    {
      ASSERT_EQ(true, str_equal("collect_info", value));
    }
    else if(!strcmp(col, "left_table_id"))
    {
      ASSERT_EQ(true, int_equal(1001, value));
    }
    else if(!strcmp(col, "left_column_name"))
    {
      ASSERT_EQ(true, str_equal("owner_id", value));
    }
    else if(!strcmp(col, "left_column_id"))
    {
      ASSERT_EQ(true, int_equal(13, value));
    }
    else if(!strcmp(col, "start_pos"))
    {
      ASSERT_EQ(true, int_equal(8, value));
    }
    else if(!strcmp(col, "end_pos"))
    {
      ASSERT_EQ(true, int_equal(16, value));
    }
    else if(!strcmp(col, "right_table_name"))
    {
      ASSERT_EQ(true, str_equal("item_info", value));
    }
    else if(!strcmp(col, "right_table_id"))
    {
      ASSERT_EQ(true, int_equal(1002, value));
    }
    else if(!strcmp(col, "right_column_name"))
    {
      ASSERT_EQ(true, str_equal("owner_id", value));
    }
    else if(!strcmp(col, "right_column_id"))
    {
      ASSERT_EQ(true, int_equal(4, value));
    }
  }

}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
