#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>

#include "common/ob_schema.h"
#include "common/ob_malloc.h"
#include "common/location/ob_ms_cache_table.h"
#include "../common/test_rowkey_helper.h"

using namespace std;
using namespace oceanbase::common;
static CharArena allocator_;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestCacheTable: public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};

TEST_F(TestCacheTable, test_new_compare)
{
  ObNewRange range;
  range.set_whole_range();
  range.table_id_ = 100;
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.set_inclusive_end();
  int ret = -1;
  ObCBtreeTable<int, int>::MapKey key;
  ObRowkey rowkey;
  char * cmp_key = (char*)"100";
  rowkey = make_rowkey(cmp_key, &allocator_);
  ret = key.compare_range_with_key(99, rowkey, range);
  EXPECT_TRUE(ret < 0);
  ret = key.compare_range_with_key(100, rowkey, range);
  EXPECT_TRUE(ret == 0);
  ret = key.compare_range_with_key(101, rowkey, range);
  EXPECT_TRUE(ret > 0);

  // min value
  ret = key.compare_range_with_key(99, ObRowkey::MIN_ROWKEY, range);
  EXPECT_TRUE(ret < 0);
  ret = key.compare_range_with_key(100, ObRowkey::MIN_ROWKEY, range);
  EXPECT_TRUE(ret == 0);
  ret = key.compare_range_with_key(101, ObRowkey::MIN_ROWKEY, range);
  EXPECT_TRUE(ret > 0);

  // max value
  ret = key.compare_range_with_key(99, ObRowkey::MAX_ROWKEY, range);
  EXPECT_TRUE(ret < 0);
  ret = key.compare_range_with_key(100, ObRowkey::MAX_ROWKEY, range);
  EXPECT_TRUE(ret == 0);
  ret = key.compare_range_with_key(101, ObRowkey::MAX_ROWKEY, range);
  EXPECT_TRUE(ret > 0);
}

TEST_F(TestCacheTable, test_compare)
{
  char temp[100];
  char temp_end[100];
  ObNewRange range;
  sprintf(temp, "%d", 1001);
  sprintf(temp_end, "%d", 100100);
  range.start_key_ = make_rowkey(temp, &allocator_);
  range.end_key_ = make_rowkey(temp_end, &allocator_);
  range.table_id_ = 100;

  ObCBtreeTable<int, int>::MapKey key;
  ObRowkey rowkey;
  // bad case bug
  char * cmp_key = (char*)"100";
  rowkey = make_rowkey(cmp_key, &allocator_);
  ObBorderFlag row_key_flag;
  int ret = key.compare_range_with_key(100, rowkey, range);
  // bug fix EXPECT_TRUE(ret == 0);
  EXPECT_TRUE(ret < 0);

  // good case
  cmp_key = (char*)"10010";
  rowkey = make_rowkey(cmp_key, &allocator_);
  ret = key.compare_range_with_key(100, rowkey, range);
  EXPECT_TRUE(ret == 0);

  cmp_key = (char*)"101";
  rowkey = make_rowkey(cmp_key, &allocator_);
  ret = key.compare_range_with_key(100, rowkey, range);
  EXPECT_TRUE(ret > 0);

  cmp_key = (char*)"1001002";
  rowkey = make_rowkey(cmp_key, &allocator_);
  ret = key.compare_range_with_key(100, rowkey, range);
  EXPECT_TRUE(ret > 0);

  // bad case bug
  cmp_key = (char*)"10010002";
  rowkey = make_rowkey(cmp_key, &allocator_);
  ret = key.compare_range_with_key(100, rowkey, range);
  // buf fix EXPECT_TRUE(ret == 0);
  EXPECT_TRUE(ret > 0);
}


