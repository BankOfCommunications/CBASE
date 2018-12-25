/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_fuse_test.cpp 
 *
 * Authors:
 *   Jianming <jianming.cjq@taobao.com>
 *
 */

#include "sql/ob_tablet_scan_fuse.h"
#include <gtest/gtest.h>
#include "ob_fake_table.h"
#include "ob_fake_ups_scan.h"
#include "ob_fake_sstable_scan.h"
#include "ob_file_table.h"
#include "common/utility.h"

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::test;

class ObTabletFuseTest: public ::testing::Test
{
  public:
    ObTabletFuseTest();
    virtual ~ObTabletFuseTest();
    virtual void SetUp();
    virtual void TearDown();
    void test(const char* sstable_file, const char *ups_file, const char *result_file);

  private:
    
};

void ObTabletFuseTest::test(const char* sstable_file, const char *ups_file, const char *result_file)
{
    
  const ObRow *row = NULL;
  const ObRow *result_row = NULL;
  const ObObj *value = NULL;
  const ObObj *result_value = NULL;
  uint64_t table_id = 0;
  uint64_t column_id = 0;

  ObFakeSSTableScan sstable(sstable_file);
  ObFakeUpsScan ups_scan(ups_file);

  ObTabletScanFuse tablet_fuse;
  OK(tablet_fuse.set_sstable_scan(&sstable));
  OK(tablet_fuse.set_incremental_scan(&ups_scan));

  ObFileTable result(result_file);

  OK(tablet_fuse.open());
  OK(result.open());

  int err = OB_SUCCESS;
  int64_t count = 0;
  const ObRowkey *last_rowkey = NULL;
  const ObRowkey *result_rowkey = NULL;

  while(OB_SUCCESS == err)
  {
    err = tablet_fuse.get_next_row(row);
    if(OB_SUCCESS != err)
    {
      ASSERT_TRUE(OB_ITER_END == err);
      break;
    }

    OK(tablet_fuse.get_last_rowkey(last_rowkey));

    OK(result.get_next_row(result_rowkey, result_row));
    count ++;

    for(int64_t i=0;i<row->get_column_num();i++)
    {
      OK(row->raw_get_cell(i, value, table_id, column_id));
      OK(result_row->get_cell(table_id, column_id, result_value));

      if((*last_rowkey) != (*result_rowkey))
      {
        printf("last_rowkey:%s\n", to_cstring(*last_rowkey));
        printf("result_rowkey:%s\n", to_cstring(*result_rowkey));
        ASSERT_TRUE(false);
      }

      if( *value != *result_value )
      {
        printf("row:[%ld], column[%ld]===========\n", count, i);
        printf("row: %s\n", print_obj(*value));
        printf("result: %s\n", print_obj(*result_value));
      }

      ASSERT_TRUE((*value) == (*result_value));
    }
  }


  OK(result.close());
  OK(tablet_fuse.close());
}

ObTabletFuseTest::ObTabletFuseTest()
{
}

ObTabletFuseTest::~ObTabletFuseTest()
{
}

void ObTabletFuseTest::SetUp()
{
}

void ObTabletFuseTest::TearDown()
{
}

TEST_F(ObTabletFuseTest, big_data_test)
{
  system("python gen_test_data.py fuse");
  test("tablet_fuse_test_data/big_sstable.ini", "tablet_fuse_test_data/big_ups_scan.ini", "tablet_fuse_test_data/big_result.ini");
}

TEST_F(ObTabletFuseTest, basic_test)
{
  test("tablet_fuse_test_data/sstable.ini", "tablet_fuse_test_data/ups_scan.ini", "tablet_fuse_test_data/result.ini");
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

