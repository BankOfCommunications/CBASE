/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_fuse_test.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include "common/ob_row_fuse.h"
#include "common/ob_ups_row.h"
#include "common/ob_row.h"
#include "common/ob_row_desc.h"

using namespace oceanbase;
using namespace common;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

namespace test
{
  class ObRowFuseTest: public ::testing::Test
  {
    public:
      ObRowFuseTest();
      virtual ~ObRowFuseTest();
      virtual void SetUp();
      virtual void TearDown();
    private:
      // disallow copy
      ObRowFuseTest(const ObRowFuseTest &other);
      ObRowFuseTest& operator=(const ObRowFuseTest &other);
    protected:
      // data members
  };

  ObRowFuseTest::ObRowFuseTest()
  {
  }

  ObRowFuseTest::~ObRowFuseTest()
  {
  }

  void ObRowFuseTest::SetUp()
  {
  }

  void ObRowFuseTest::TearDown()
  {
  }

  TEST_F(ObRowFuseTest, assign)
  {
    uint64_t TABLE_ID = 1000;

    ObRowDesc row_desc;
    for(int i=0;i<8;i++)
    {
      row_desc.add_column_desc(TABLE_ID, OB_APP_MIN_COLUMN_ID + i);
    }

    ObRow row;
    row.set_row_desc(row_desc);
    ObObj value;

    for(int i=0;i<8;i++)
    {
      value.set_int(i);
      OK(row.raw_set_cell(i, value));
    }

    ObRowDesc ups_row_desc;
    for(int i=0;i<4;i++)
    {
      ups_row_desc.add_column_desc(TABLE_ID, OB_APP_MIN_COLUMN_ID + i);
    }

    ObUpsRow ups_row;
    ups_row.set_row_desc(ups_row_desc);

    for(int i=0;i<4;i++)
    {
      value.set_ext(ObActionFlag::OP_NOP);
      OK(ups_row.raw_set_cell(i, value));
    }

    ups_row.set_is_delete_row(true);
    OK(ObRowFuse::assign(ups_row, row));

    const ObObj *cell = NULL;
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;

    for(int i=0;i<4;i++)
    {
      OK(row.raw_get_cell(i, cell, table_id, column_id));
      ASSERT_EQ(ObNullType, cell->get_type());
    }
  }

  TEST_F(ObRowFuseTest, basic_join_test2)
  {
    uint64_t TABLE_ID = 1000;

    ObRowDesc row_desc;
    for(int i=0;i<8;i++)
    {
      row_desc.add_column_desc(TABLE_ID, OB_APP_MIN_COLUMN_ID + i);
    }

    ObRow row;
    row.set_row_desc(row_desc);
    ObObj value;

    for(int i=0;i<8;i++)
    {
      value.set_int(i);
      row.raw_set_cell(i, value);
    }

    ObRowDesc ups_row_desc;
    for(int i=0;i<4;i++)
    {
      ups_row_desc.add_column_desc(TABLE_ID, OB_APP_MIN_COLUMN_ID + i);
    }

    ObUpsRow ups_row;
    ups_row.set_row_desc(ups_row_desc);

    ups_row.set_is_delete_row(true);

    for(int i=0;i<4;i++)
    {
      value.set_ext(ObActionFlag::OP_NOP);
      ups_row.raw_set_cell(i, value);
    }

    ObRow result;
    OK(ObRowFuse::join_row(&ups_row, &row, &result));

    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;

    const ObObj *cell = NULL;

    for(int i=0;i<4;i++)
    {
      OK(result.raw_get_cell(i, cell, table_id, column_id));
      ASSERT_EQ(ObNullType, cell->get_type());
    }

    for(int i=4;i<8;i++)
    {
      int64_t int_value = 0;
      OK(result.raw_get_cell(i, cell, table_id, column_id));
      cell->get_int(int_value);
      ASSERT_EQ(int_value, i);
    }


    for(int i=0;i<4;i++)
    {
      value.set_int(i+100);
      ups_row.raw_set_cell(i, value);
    }

    OK(ObRowFuse::join_row(&ups_row, &row, &result));
    for(int i=0;i<4;i++)
    {
      int64_t int_value = 0;
      OK(result.raw_get_cell(i, cell, table_id, column_id));
      cell->get_int(int_value);
      ASSERT_EQ(int_value, i+100);
    }

    for(int i=4;i<8;i++)
    {
      int64_t int_value = 0;
      OK(result.raw_get_cell(i, cell, table_id, column_id));
      cell->get_int(int_value);
      ASSERT_EQ(int_value, i);
    }

    ups_row_desc.add_column_desc(TABLE_ID, OB_APP_MIN_COLUMN_ID + 10);
    ASSERT_TRUE(OB_SUCCESS != ObRowFuse::join_row(&ups_row, &row, &result));
  }


  void init_row(ObRow &row, ObRow &result_row, int64_t column_num)
  {
    ObObj value;

    for (int i = 0; i < column_num; i ++)
    {
      value.set_int(i, true);
      row.raw_set_cell(i, value);
    }

    for (int i = 0; i < column_num; i ++)
    {
      value.set_int(i + 1);
      result_row.raw_set_cell(i, value);
    }
  }

  #define MY_ASSERT_TRUE(expr) \
    if (!(expr)) \
    { \
      TBSYS_LOG(WARN, "assert fail"); \
      return false; \
    }

  void set_flag(ObRow &row, int64_t action_flag)
  {
    ObObj cell;
    cell.set_ext(action_flag);
    row.set_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, cell);
  }

  enum ResultState
  {
    NOT_CHANGE,
    COPY,
    APPLY,
    ALL_NULL
  };

  bool check_result(const ObRow &row, enum ResultState state, int64_t column_num)
  {
    int64_t int_value = 0;
    const ObRowDesc *row_desc = row.get_row_desc();
    const ObObj *cell = NULL;
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;

    if (NULL == row_desc)
    {
      return false;
    }
    for (int64_t i = row_desc->get_rowkey_cell_count(); i < column_num; i ++)
    {
      row.raw_get_cell(i, cell, table_id, column_id);
      cell->get_int(int_value);
      switch (state)
      {
        case NOT_CHANGE:
          MY_ASSERT_TRUE( int_value == i + 1 );
          break;
        case COPY:
          MY_ASSERT_TRUE( int_value == i );
          break;
        case APPLY:
          MY_ASSERT_TRUE( int_value == i * 2 + 1 );
          break;
        case ALL_NULL:
          MY_ASSERT_TRUE( ObNullType == cell->get_type() );
      }
    }
    for (int i = 0; i < row_desc->get_rowkey_cell_count(); i ++)
    {
      row.raw_get_cell(i, cell, table_id, column_id);
      cell->get_int(int_value);
      MY_ASSERT_TRUE( int_value == i + 1 );
    }
    return true;
  }

  //test ObRowFuse.fuse_row
  TEST_F(ObRowFuseTest, fuse_row_test)
  {
    ObRow row;
    ObRow result_row;

    ObRowDesc row_desc;
    ObRowDesc result_row_desc;
    const uint64_t TABLE_ID = 1001;
    const int64_t COLUMN_NUM = 8;
    const ObObj *result_action_flag_cell = NULL;
    bool is_row_empty = false;

    row_desc.set_rowkey_cell_count(2);
    result_row_desc.set_rowkey_cell_count(2);

    for (int i = 0; i < COLUMN_NUM; i ++)
    {
      row_desc.add_column_desc(TABLE_ID, OB_APP_MIN_COLUMN_ID + i);
      result_row_desc.add_column_desc(TABLE_ID, OB_APP_MIN_COLUMN_ID + i);
    }
    row_desc.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);

    row.set_row_desc(row_desc);
    result_row.set_row_desc(result_row_desc);

    //测试fuse_sstable_row的情况
    /*
     * after init row
     * row:            +1 +2 +3 +4 +5 +6 +7 +8
     * result_row:      2  3  4  5  6  7  8  9
     */
    init_row(row, result_row, COLUMN_NUM);
    set_flag(row, ObActionFlag::OP_NEW_ADD);
    ObRowFuse::fuse_row(row, result_row, is_row_empty, false);
    ASSERT_TRUE(check_result(result_row, COPY, COLUMN_NUM)) << to_cstring(row) << std::endl << to_cstring(result_row) << std::endl;
    ASSERT_TRUE(!is_row_empty);

    init_row(row, result_row, COLUMN_NUM);
    set_flag(row, ObActionFlag::OP_DEL_ROW);
    ObRowFuse::fuse_row(row, result_row, is_row_empty, false);
    ASSERT_TRUE(check_result(result_row, ALL_NULL, COLUMN_NUM));
    ASSERT_TRUE(is_row_empty);

    init_row(row, result_row, COLUMN_NUM);
    set_flag(row, ObActionFlag::OP_ROW_DOES_NOT_EXIST);
    is_row_empty = true;
    ObRowFuse::fuse_row(row, result_row, is_row_empty, false);
    ASSERT_TRUE(check_result(result_row, NOT_CHANGE, COLUMN_NUM));
    ASSERT_TRUE(is_row_empty);

    init_row(row, result_row, COLUMN_NUM);
    set_flag(row, ObActionFlag::OP_VALID);
    ObRowFuse::fuse_row(row, result_row, is_row_empty, false);
    ASSERT_TRUE(check_result(result_row, APPLY, COLUMN_NUM));
    ASSERT_TRUE(!is_row_empty);


    //测试fuse_ups_row的情况
    result_row_desc.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
    result_row.get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, result_action_flag_cell);

    init_row(row, result_row, COLUMN_NUM);
    set_flag(row, ObActionFlag::OP_NEW_ADD);
    set_flag(result_row, ObActionFlag::OP_DEL_ROW);
    ObRowFuse::fuse_row(row, result_row, is_row_empty, true);
    ASSERT_TRUE(check_result(result_row, COPY, COLUMN_NUM));
    ASSERT_TRUE( ObActionFlag::OP_NEW_ADD == result_action_flag_cell->get_ext() );
    ASSERT_TRUE(!is_row_empty);

    init_row(row, result_row, COLUMN_NUM);
    set_flag(row, ObActionFlag::OP_DEL_ROW);
    set_flag(result_row, ObActionFlag::OP_NEW_ADD);
    ObRowFuse::fuse_row(row, result_row, is_row_empty, true);
    ASSERT_TRUE(check_result(result_row, NOT_CHANGE, COLUMN_NUM));
    ASSERT_TRUE( ObActionFlag::OP_DEL_ROW == result_action_flag_cell->get_ext() );
    ASSERT_TRUE(!is_row_empty);

    init_row(row, result_row, COLUMN_NUM);
    set_flag(row, ObActionFlag::OP_NEW_ADD);
    set_flag(result_row, ObActionFlag::OP_NEW_ADD);
    ObRowFuse::fuse_row(row, result_row, is_row_empty, true);
    ASSERT_TRUE(check_result(result_row, COPY, COLUMN_NUM)) << to_cstring(row) << std::endl << to_cstring(result_row) << std::endl;
    ASSERT_TRUE( ObActionFlag::OP_NEW_ADD == result_action_flag_cell->get_ext() ) << "result_action flag" << result_action_flag_cell->get_ext() << std::endl;
    ASSERT_TRUE(!is_row_empty);

    init_row(row, result_row, COLUMN_NUM);
    set_flag(row, ObActionFlag::OP_NEW_ADD);
    set_flag(result_row, ObActionFlag::OP_ROW_DOES_NOT_EXIST);
    ObRowFuse::fuse_row(row, result_row, is_row_empty, true);
    ASSERT_TRUE(check_result(result_row, COPY, COLUMN_NUM));
    ASSERT_TRUE( ObActionFlag::OP_NEW_ADD == result_action_flag_cell->get_ext() );
    ASSERT_TRUE(!is_row_empty);

    init_row(row, result_row, COLUMN_NUM);
    set_flag(row, ObActionFlag::OP_ROW_DOES_NOT_EXIST);
    set_flag(result_row, ObActionFlag::OP_DEL_ROW);
    ObRowFuse::fuse_row(row, result_row, is_row_empty, true);
    ASSERT_TRUE(check_result(result_row, NOT_CHANGE, COLUMN_NUM));
    ASSERT_TRUE( ObActionFlag::OP_DEL_ROW == result_action_flag_cell->get_ext() );
    ASSERT_TRUE(!is_row_empty);

  }

  //test ObRowFuse.apply_row
  TEST_F(ObRowFuseTest, apply_row_test)
  {
    ObRow row;
    ObRow result_row;

    ObRowDesc row_desc;
    const uint64_t TABLE_ID = 1001;
    const int64_t COLUMN_NUM = 8;
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;
    int64_t int_value = 0;
    const ObObj *cell = NULL;

    row_desc.set_rowkey_cell_count(2);

    for (int i = 0; i < COLUMN_NUM; i ++)
    {
      row_desc.add_column_desc(TABLE_ID, OB_APP_MIN_COLUMN_ID + i);
    }
    row_desc.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);

    row.set_row_desc(row_desc);
    result_row.set_row_desc(row_desc);

    init_row(row, result_row, COLUMN_NUM);
    set_flag(row, ObActionFlag::OP_DEL_ROW);
    set_flag(result_row, ObActionFlag::OP_ROW_DOES_NOT_EXIST);
    ObRowFuse::apply_row(row, result_row, true);

    for (int64_t i = row_desc.get_rowkey_cell_count(); i < COLUMN_NUM; i++)
    {
      result_row.raw_get_cell(i, cell, table_id, column_id);
      cell->get_int(int_value);
      ASSERT_EQ(int_value, i);
    }

    for (int i = 0; i < row_desc.get_rowkey_cell_count(); i ++)
    {
      result_row.raw_get_cell(i, cell, table_id, column_id);
      cell->get_int(int_value);
      ASSERT_EQ(int_value, i + 1);
    }

    result_row.get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, cell);
    ASSERT_TRUE( ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell->get_ext() );

    init_row(row, result_row, COLUMN_NUM);
    set_flag(row, ObActionFlag::OP_DEL_ROW);
    set_flag(result_row, ObActionFlag::OP_ROW_DOES_NOT_EXIST);
    ObRowFuse::apply_row(row, result_row, false);

    for (int64_t i = row_desc.get_rowkey_cell_count(); i < COLUMN_NUM; i++)
    {
      result_row.raw_get_cell(i, cell, table_id, column_id);
      cell->get_int(int_value);
      ASSERT_EQ(int_value, i * 2 + 1);
    }

    for (int i = 0; i < row_desc.get_rowkey_cell_count(); i ++)
    {
      result_row.raw_get_cell(i, cell, table_id, column_id);
      cell->get_int(int_value);
      ASSERT_EQ(int_value, i + 1);
    }

    result_row.get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, cell);
    ASSERT_TRUE( ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell->get_ext() );
  }

  TEST_F(ObRowFuseTest, basic_join_test)
  {
    ObRowDesc row_desc;
    uint64_t table_id = 1000;
    for(int i=0;i<8;i++)
    {
      row_desc.add_column_desc(table_id, OB_APP_MIN_COLUMN_ID + i);
    }

    ObUpsRow ups_row;
    ups_row.set_row_desc(row_desc);
    ups_row.set_is_delete_row(false);

    ObObj cell;
    for(int i=0;i<8;i++)
    {
      cell.set_int(i, true);
      ups_row.raw_set_cell(i, cell);
    }

    ObRow row;
    row.set_row_desc(row_desc);

    for(int i=0;i<8;i++)
    {
      cell.set_int(i);
      row.raw_set_cell(i, cell);
    }

    ObRow result;

    ObRowFuse::join_row(&ups_row, &row, &result);

    const ObObj *result_cell = NULL;
    uint64_t result_table_id = 0;
    uint64_t result_column_id = 0;
    int64_t int_value = 0;
    for(int i=0;i<8;i++)
    {
      result.raw_get_cell(i, result_cell, result_table_id, result_column_id);
      result_cell->get_int(int_value);
      ASSERT_EQ(i * 2, int_value);
    }

    ups_row.set_is_delete_row(true);

    ObRowFuse::join_row(&ups_row, &row, &result);

    for(int i=0;i<8;i++)
    {
      result.raw_get_cell(i, result_cell, result_table_id, result_column_id);
      result_cell->get_int(int_value);
      ASSERT_EQ(i, int_value);
    }

    for(int i=0;i<8;i++)
    {
      cell.set_int(i, false);
      ups_row.raw_set_cell(i, cell);
    }

    ups_row.set_is_delete_row(false);
    ObRowFuse::join_row(&ups_row, &row, &result);

    for(int i=0;i<8;i++)
    {
      result.raw_get_cell(i, result_cell, result_table_id, result_column_id);
      result_cell->get_int(int_value);
      ASSERT_EQ(i, int_value);
    }
  }

  TEST_F(ObRowFuseTest, basic_fuse_test)
  {
    ObRowDesc row_desc;
    ObRowDesc sstable_row_desc;
    uint64_t table_id = 1000;
    for(int i=0;i<8;i++)
    {
      row_desc.add_column_desc(table_id, OB_APP_MIN_COLUMN_ID + i);
      sstable_row_desc.add_column_desc(table_id, OB_APP_MIN_COLUMN_ID + i);
    }
    sstable_row_desc.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);

    ObUpsRow ups_row;
    ups_row.set_row_desc(row_desc);
    ups_row.set_is_delete_row(false);

    ObObj cell;
    for(int i=0;i<8;i++)
    {
      cell.set_int(i, true);
      ups_row.raw_set_cell(i, cell);
    }

    ObRow row;
    row.set_row_desc(sstable_row_desc);


    for(int i=0;i<8;i++)
    {
      cell.set_int(i);
      row.raw_set_cell(i, cell);
    }

    ObRow result;

    set_flag(row, ObActionFlag::OP_ROW_DOES_NOT_EXIST);
    ObRowFuse::fuse_row(&ups_row, &row, &result);

    const ObObj *result_cell = NULL;
    uint64_t result_table_id = 0;
    uint64_t result_column_id = 0;
    int64_t int_value = 0;
    for(int i=0;i<8;i++)
    {
      result.raw_get_cell(i, result_cell, result_table_id, result_column_id);
      result_cell->get_int(int_value);
      ASSERT_EQ(i, int_value) << to_cstring(result);
    }

    ups_row.set_is_delete_row(true);

    ObRowFuse::fuse_row(&ups_row, &row, &result);

    for(int i=0;i<8;i++)
    {
      result.raw_get_cell(i, result_cell, result_table_id, result_column_id);
      result_cell->get_int(int_value);
      ASSERT_EQ(i, int_value);
    }

    for(int i=0;i<8;i++)
    {
      cell.set_int(i, false);
      ups_row.raw_set_cell(i, cell);
    }

    ups_row.set_is_delete_row(false);
    ObRowFuse::fuse_row(&ups_row, &row, &result);

    for(int i=0;i<8;i++)
    {
      result.raw_get_cell(i, result_cell, result_table_id, result_column_id);
      result_cell->get_int(int_value);
      ASSERT_EQ(i, int_value);
    }
  }
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
