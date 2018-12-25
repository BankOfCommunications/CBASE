#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>

#include "common/ob_malloc.h"
#include "common/ob_schema.h"
#include "common/ob_cond_info.h"
#include "common/ob_common_param.h"
#include "ob_param_decoder.h"
#include "../common/test_rowkey_helper.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

static CharArena allocator_;
int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestParamDecoder: public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};


TEST_F(TestParamDecoder, test_decode_org_cell)
{
  ObSchemaManagerV2 schema(100);
  tbsys::CConfig config;
  EXPECT_TRUE(schema.parse_from_file("schema.ini", config));
  ObCellInfo cell;
  ObCellInfo out_cell;
  char * no_name = (char*)"test_not_exist";
  char * tablename = (char*)"collect_info";
  char * columnname = (char*)"item_price";
  ObString table_name;
  ObString column_name;
  // table name not exist and column not exist
  table_name.assign(no_name, static_cast<int32_t>(strlen(no_name)));
  column_name.assign(no_name, static_cast<int32_t>(strlen(no_name)));
  cell.table_name_ = table_name;
  cell.column_name_ = column_name;
  EXPECT_TRUE(OB_SUCCESS != ObParamDecoder::decode_org_cell(cell, schema, out_cell));

  // table name not exist but column exist
  table_name.assign(no_name, static_cast<int32_t>(strlen(no_name)));
  column_name.assign(columnname, static_cast<int32_t>(strlen(columnname)));
  cell.table_name_ = table_name;
  cell.column_name_ = column_name;
  EXPECT_TRUE(OB_SUCCESS != ObParamDecoder::decode_org_cell(cell, schema, out_cell));

  // table name exist but column not exist
  table_name.assign(tablename, static_cast<int32_t>(strlen(tablename)));
  column_name.assign(no_name, static_cast<int32_t>(strlen(no_name)));
  cell.table_name_ = table_name;
  cell.column_name_ = column_name;
  EXPECT_TRUE(OB_SUCCESS != ObParamDecoder::decode_org_cell(cell, schema, out_cell));

  // table name exist and column exist
  table_name.assign(tablename, static_cast<int32_t>(strlen(tablename)));
  column_name.assign(columnname, static_cast<int32_t>(strlen(columnname)));
  cell.table_name_ = table_name;
  cell.column_name_ = column_name;
  EXPECT_TRUE(OB_SUCCESS == ObParamDecoder::decode_org_cell(cell, schema, out_cell));
}


TEST_F(TestParamDecoder, test_decode_cond_cell)
{
  ObSchemaManagerV2 schema(100);
  tbsys::CConfig config;
  EXPECT_TRUE(schema.parse_from_file("schema.ini", config));
  ObCondInfo cond;
  ObCellInfo out_cell;
  char * no_name = (char*)"test_not_exist";
  char * tablename = (char*)"collect_info";
  char * columnname = (char*)"item_price";
  char * rowkey = (char*)"rowkey_test";
  ObString table_name;
  ObString column_name;
  ObRowkey row_key;

  // NORMAL TYPE
  // table name not exist and column not exist
  table_name.assign(no_name, static_cast<int32_t>(strlen(no_name)));
  column_name.assign(no_name, static_cast<int32_t>(strlen(no_name)));
  row_key = make_rowkey(rowkey, &allocator_);
  ObObj obj;
  obj.set_int(1234);
  cond.set(NE, table_name, row_key, column_name, obj);
  EXPECT_TRUE(OB_SUCCESS != ObParamDecoder::decode_cond_cell(cond, schema, out_cell));

  // table name not exist but column exist
  table_name.assign(no_name, static_cast<int32_t>(strlen(no_name)));
  column_name.assign(columnname, static_cast<int32_t>(strlen(columnname)));
  cond.set(NE, table_name, row_key, column_name, obj);
  EXPECT_TRUE(OB_SUCCESS != ObParamDecoder::decode_cond_cell(cond, schema, out_cell));

  // table name exist but column not exist
  table_name.assign(tablename, static_cast<int32_t>(strlen(tablename)));
  column_name.assign(no_name, static_cast<int32_t>(strlen(no_name)));
  cond.set(NE, table_name, row_key, column_name, obj);
  EXPECT_TRUE(OB_SUCCESS != ObParamDecoder::decode_cond_cell(cond, schema, out_cell));

  // table name exist and column exist
  table_name.assign(tablename, static_cast<int32_t>(strlen(tablename)));
  column_name.assign(columnname, static_cast<int32_t>(strlen(columnname)));
  cond.set(NE, table_name, row_key, column_name, obj);
  EXPECT_TRUE(OB_SUCCESS == ObParamDecoder::decode_cond_cell(cond, schema, out_cell));

  // EXIST TYPE
  // table name not exist
  table_name.assign(no_name, static_cast<int32_t>(strlen(no_name)));
  row_key = make_rowkey(rowkey, &allocator_);
  cond.set(table_name, row_key, false);
  EXPECT_TRUE(OB_SUCCESS != ObParamDecoder::decode_cond_cell(cond, schema, out_cell));
  cond.set(table_name, row_key, true);
  EXPECT_TRUE(OB_SUCCESS != ObParamDecoder::decode_cond_cell(cond, schema, out_cell));

  // table name exist
  table_name.assign(tablename, static_cast<int32_t>(strlen(tablename)));
  cond.set(table_name, row_key, true);
  EXPECT_TRUE(OB_SUCCESS == ObParamDecoder::decode_cond_cell(cond, schema, out_cell));
  cond.set(table_name, row_key, false);
  EXPECT_TRUE(OB_SUCCESS == ObParamDecoder::decode_cond_cell(cond, schema, out_cell));
}

