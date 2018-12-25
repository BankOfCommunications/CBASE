/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_test.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "common/ob_malloc.h"
#include "common/utility.h"
#include "sql/ob_multiple_scan_merge.h"
#include "sql/ob_multiple_get_merge.h"
#include "common/ob_action_flag.h"
#include <gtest/gtest.h>

using namespace oceanbase;
using namespace common;
using namespace sql;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

#define TABLE_ID 1001

struct Row
{
  int64_t rowkey;
  bool is_delete;
  const char * col1;
  const char * col2;
  bool is_add;
  int64_t col3;
};

class ObChildTest : public ObPhyOperator 
{
  public:
    ObChildTest();
    int open();
    int close();
    int set_child(int32_t child_idx, ObPhyOperator &child)
    {
      UNUSED(child_idx);
      UNUSED(child);
      return OB_SUCCESS;
    }

    int get_row_desc(const ObRowDesc *&row_desc) const
    {
      row_desc = &row_desc_;
      return OB_SUCCESS;
    }

    virtual int get_next_row(const ObRow *&row);
    void set_row_array(Row *row_array, int64_t row_count)
    {
      row_array_ = row_array;
      row_count_ = row_count;
    }

    int64_t to_string(char *buf, int64_t buf_len) const
    {
      UNUSED(buf);
      UNUSED(buf_len);
      return 0;
    }

  private:
    ObRow ups_row_;
    int64_t count_;
    int64_t row_count_;
    Row *row_array_;
    ObRowDesc row_desc_;
};

ObChildTest::ObChildTest()
  :count_(0),
  row_count_(0),
  row_array_(NULL)
{
}

int ObChildTest::open()
{
  count_ = 0;
  for (int64_t i=0;i<4;i++)
  {
    row_desc_.add_column_desc(TABLE_ID, OB_APP_MIN_COLUMN_ID + i);
  }
  row_desc_.set_rowkey_cell_count(1);
  row_desc_.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
  ups_row_.set_row_desc(row_desc_);
  return OB_SUCCESS;
}

int ObChildTest::close()
{
  return OB_SUCCESS;
}

int ObChildTest::get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj cell;

  if (count_ >= row_count_)
  {
    return OB_ITER_END;
  }

  if (row_array_[count_].is_delete)
  {
    cell.set_ext(ObActionFlag::OP_NEW_ADD);
  }
  else
  {
    cell.set_ext(ObActionFlag::OP_VALID);
  }
  ups_row_.set_cell( OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, cell);

  cell.set_int(row_array_[count_].rowkey);
  ups_row_.set_cell(TABLE_ID, OB_APP_MIN_COLUMN_ID + 0, cell);

  cell.set_varchar(ObString::make_string(row_array_[count_].col1));
  ups_row_.set_cell(TABLE_ID, OB_APP_MIN_COLUMN_ID + 1, cell);

  cell.set_varchar(ObString::make_string(row_array_[count_].col2));
  ups_row_.set_cell(TABLE_ID, OB_APP_MIN_COLUMN_ID + 2, cell);

  cell.set_int(row_array_[count_].col3, row_array_[count_].is_add);
  ups_row_.set_cell(TABLE_ID, OB_APP_MIN_COLUMN_ID + 3, cell);

  row = &ups_row_;
  count_ ++;
  return ret;
}


class ObMultipleMergeTest: public ::testing::Test
{
  public:
    ObMultipleMergeTest();
    virtual ~ObMultipleMergeTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObMultipleMergeTest(const ObMultipleMergeTest &other);
    ObMultipleMergeTest& operator=(const ObMultipleMergeTest &other);
  protected:
    // data members
};

ObMultipleMergeTest::ObMultipleMergeTest()
{
}

ObMultipleMergeTest::~ObMultipleMergeTest()
{
}

void ObMultipleMergeTest::SetUp()
{
}

void ObMultipleMergeTest::TearDown()
{
}

bool row_equal(const ObRow &row1, const ObRow &row2)
{
  const ObRowDesc *row_desc = row1.get_row_desc();
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  const ObObj *cell1 = NULL;
  const ObObj *cell2 = NULL;

  if (NULL == row_desc)
  {
    return false;
  }

  for (int64_t i=0;i<row_desc->get_column_num();i++)
  {
    row_desc->get_tid_cid(i, table_id, column_id);
    row1.get_cell(table_id, column_id, cell1);
    row2.get_cell(table_id, column_id, cell2);
    if ( *cell1 != *cell2 )
    {
      return false;
    }
  }

  return true;
}

TEST_F(ObMultipleMergeTest, ob_multiple_get_merge)
{
  ObChildTest child1;
  ObChildTest child2;
  ObChildTest result;

  const ObRow *row = NULL;
  const ObRow *result_row = NULL;
  ObMultipleGetMerge merge;

  Row table1[] = 
  {
    {3, true, "oceanbase", "oceanbase", false, 4 },
    {1, true, "oceanbase", "oceanbase", false, 4 },
    {2, true,  "oceanbase", "oceanbase", false, 4 }
  };

  Row table2[] = 
  {
    {3, true, "kkk", "oceanbase", false, 4 },
    {1, true, "oceanbase", "oceanbase", false, 4 },
    {2, true, "oceanbase", "oceanbase", false, 4 },
  };

  Row result_table[] = 
  {
    {3, true, "kkk", "oceanbase", false, 4 },
    {1, true, "oceanbase", "oceanbase", false, 4 },
    {2, true,  "oceanbase", "oceanbase", false, 4 }
  };

  int table1_size = (int)(sizeof(table1) / sizeof(Row));
  int table2_size = (int)(sizeof(table2) / sizeof(Row));
  int result_size = (int)(sizeof(result_table) / sizeof(Row));

  child1.set_row_array(table1, table1_size);
  child2.set_row_array(table2, table2_size);
  result.set_row_array(result_table, result_size);

  merge.set_child(0, child1);
  merge.set_child(1, child2);

  OK(merge.open());
  OK(result.open());

  for (int i=0;i<result_size;i++)
  {
    OK(merge.get_next_row(row));
    OK(result.get_next_row(result_row));
    ASSERT_TRUE( row_equal(*row, *result_row) ) <<
      "row" << to_cstring(*row) << std::endl << 
      "result_row" << to_cstring(*result_row) << std::endl;
  }

  merge.close();
  result.close();

}

TEST_F(ObMultipleMergeTest, basic_test)
{
  ObChildTest child1;
  ObChildTest child2;
  ObChildTest child3;
  ObChildTest child4;
  ObChildTest result;

  Row table1[] = 
  {
    {1, true, "oceanbase", "oceanbase", false, 4 },
    {2, true, "oceanbase", "oceanbase", false, 4 },
    {3, true,  "oceanbase", "oceanbase", false, 4 }
  };

  Row table2[] = 
  {
    {3, true, "kkk", "oceanbase", false, 4 },
    {5, true, "oceanbase", "oceanbase", false, 4 },
    {6, true, "oceanbase", "oceanbase", false, 4 },
    {7, false, "oceanbase", "oceanbase", true, 4 }
  };

  Row table3[] = 
  {
    {3, true, "kkk", "oceanbase", false, 4 },
    {3, true, "zzz", "oceanbase", false, 4 },
    {5, true, "jjjj", "oceanbase", false, 4 },
    {6, false, "oceanbase", "oceanbase", true, 4 },
    {7, true, "oceanbase", "oceanbase", true, 4 },
    {8, false, "oceanbase", "oceanbase", true, 4 }
  };

  Row result_table[] = 
  {
    {1, true, "oceanbase", "oceanbase", false, 4 },
    {2, true, "oceanbase", "oceanbase", false, 4 },
    {3, true, "zzz", "oceanbase", false, 4 },
    {5, true, "jjjj", "oceanbase", false, 4 },
    {6, true, "oceanbase", "oceanbase", false, 8 },
    {7, true, "oceanbase", "oceanbase", true, 4 },
    {8, false, "oceanbase", "oceanbase", true, 4 }
  };


  int table1_size = (int)(sizeof(table1) / sizeof(Row));
  int table2_size = (int)(sizeof(table2) / sizeof(Row));
  int table3_size = (int)(sizeof(table3) / sizeof(Row));
  int result_size = (int)(sizeof(result_table) / sizeof(Row));

  child1.set_row_array(table1, table1_size);
  child2.set_row_array(table2, table2_size);
  child3.set_row_array(table3, table3_size);
  child4.set_row_array(table3, 0);
  result.set_row_array(result_table, result_size);

  const ObRow *row = NULL;
  const ObRow *result_row = NULL;

  ObMultipleScanMerge merge;
  merge.set_child(0, child1);
  merge.set_child(1, child2);
  merge.set_child(2, child3);
  merge.set_child(3, child4);

  OK(merge.open());
  OK(result.open());

  for (int i=0;i<result_size;i++)
  {
    OK(merge.get_next_row(row));
    OK(result.get_next_row(result_row));
    ASSERT_TRUE( row_equal(*row, *result_row) ) <<
      "row" << to_cstring(*row) << std::endl << 
      "result_row" << to_cstring(*result_row) << std::endl;
  }

  merge.close();
  result.close();
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

