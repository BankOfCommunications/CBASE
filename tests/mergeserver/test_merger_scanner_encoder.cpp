#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>

#include "common/ob_malloc.h"
#include "common/ob_scanner.h"
#include "common/ob_get_param.h"
#include "ob_ms_scanner_encoder.h"
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

class TestScannerEncoder: public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};

TEST_F(TestScannerEncoder, encode)
{
  // set get_param cell
  ObGetParam get_param;
  ObGetParam err_param;
  const int64_t count = 50;
  ObCellInfo cell;
  ObString table_name;
  ObRowkey row_key;
  ObString column_name;
  char * table = (char*)"table";
  char * rowkey = (char*)"rowkey";
  char * column = (char*)"column";
  table_name.assign(table, static_cast<int32_t>(strlen(table)));
  row_key = make_rowkey(rowkey, &allocator_);
  column_name.assign(column, static_cast<int32_t>(strlen(column)));
  for (int64_t i = 0; i < count; ++i)
  {
    cell.reset();
    cell.table_name_ = table_name;
    cell.row_key_ = row_key;
    cell.column_name_ = column_name;
    EXPECT_TRUE(OB_SUCCESS == get_param.add_cell(cell));
    if (0 == i)
    {
      continue;
    }
    EXPECT_TRUE(OB_SUCCESS == err_param.add_cell(cell));
  }
  // ups return scanner
  ObScanner temp_result;
  ObScanner err_result;
  uint64_t table_id = 1234;
  uint64_t column_id = 1234;
  ObObj obj;
  for (int64_t i = 0; i < count; ++i)
  {
    cell.reset();
    cell.table_id_= table_id;
    cell.row_key_ = row_key;
    cell.column_id_ = column_id;
    obj.set_int(i);
    cell.value_ = obj;
    EXPECT_TRUE(OB_SUCCESS == temp_result.add_cell(cell));
    if (0 == i)
    {
      continue;
    }
    EXPECT_TRUE(OB_SUCCESS == err_result.add_cell(cell));
  }

  ObScanner result;
  // encode
  EXPECT_TRUE(OB_SUCCESS == ObMergerScannerEncoder::encode(get_param, temp_result, result));
  result.clear();
  EXPECT_TRUE(OB_SUCCESS != ObMergerScannerEncoder::encode(err_param, temp_result, result));
  result.clear();
  EXPECT_TRUE(OB_SUCCESS != ObMergerScannerEncoder::encode(get_param, err_result, result));
  result.clear();
  EXPECT_TRUE(OB_SUCCESS == ObMergerScannerEncoder::encode(err_param, err_result, result));
  result.clear();
  EXPECT_TRUE(OB_SUCCESS == err_result.add_cell(cell));
  EXPECT_TRUE(OB_SUCCESS == ObMergerScannerEncoder::encode(get_param, err_result, result));
  result.clear();
  EXPECT_TRUE(OB_SUCCESS != ObMergerScannerEncoder::encode(err_param, err_result, result));
}

