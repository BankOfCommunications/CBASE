/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_project_test.cpp,  06/04/2012 05:47:34 PM xiaochu Exp $
 *
 * Author:
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:
 *   Test ObProject class
 *
 */
#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include "common/ob_string_buf.h"
#include "sql/ob_sql_expression.h"
#include "common/ob_row.h"
#include "sql/ob_project.h"
#include "ob_fake_table.h"


using namespace oceanbase::sql;
using namespace oceanbase::sql::test;
using namespace oceanbase::common;

class ObPhyOperatorStub : public ObPhyOperator
{
  public:
    ObPhyOperatorStub(){}
    ~ObPhyOperatorStub(){}

    int set_child(int32_t child_idx, ObPhyOperator &child_operator)
    {
      UNUSED(child_idx);
      UNUSED(child_operator);
      return OB_ERROR;
    }

    int open()
    {
      return OB_SUCCESS;
    }

    int close()
    {
      return OB_SUCCESS;
    }

    int get_next_row(const ObRow *&row)
    {
      ObRowDesc row_desc;
      ObObj obj_a, obj_b;

      row_desc.add_column_desc(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 2);
      row_desc.add_column_desc(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 3);
      obj_a.set_int(19);
      obj_b.set_int(2);
      row_.set_row_desc(row_desc);
      row_.set_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 2, obj_a);
      row_.set_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 3, obj_b);

      row = &row_;
      return OB_SUCCESS;
    }

    int64_t to_string(char* buf, const int64_t buf_len) const
    {
      UNUSED(buf);
      UNUSED(buf_len);
      TBSYS_LOG(INFO, "ObPhyOperatorStub to string called");
      return OB_SUCCESS;
    }

    ObRow row_;

};

class ObProjectTest: public ::testing::Test
{
  public:
    ObProjectTest();
    virtual ~ObProjectTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObProjectTest(const ObProjectTest &other);
    ObProjectTest& operator=(const ObProjectTest &other);
  private:
    // data members
};

ObProjectTest::ObProjectTest()
{
}

ObProjectTest::~ObProjectTest()
{
}

void ObProjectTest::SetUp()
{
}

void ObProjectTest::TearDown()
{
}

TEST_F(ObProjectTest, one_column_basic_test)
{
  int ret = OB_SUCCESS;
  ObSqlExpression p, p2;
  ExprItem item_a, item_b, item_add, item_c;
  ObObj obj_a, obj_b;

  item_a.type_ = T_REF_COLUMN;
  item_b.type_ = T_REF_COLUMN;
  item_add.type_ = T_OP_ADD;
  item_c.type_ = T_REF_COLUMN;
  item_a.value_.cell_.tid = test::ObFakeTable::TABLE_ID;
  item_a.value_.cell_.cid = OB_APP_MIN_COLUMN_ID + 4;
  item_b.value_.cell_.tid = test::ObFakeTable::TABLE_ID;
  item_b.value_.cell_.cid = OB_APP_MIN_COLUMN_ID + 5;
  item_add.value_.int_ = 2;
  item_c.value_.cell_.tid = test::ObFakeTable::TABLE_ID;
  item_c.value_.cell_.cid = OB_APP_MIN_COLUMN_ID + 7;

  p.add_expr_item(item_a);
  p.add_expr_item(item_b);
  p.add_expr_item(item_add);

  p.set_tid_cid(1000, 1);
  p.add_expr_item_end();

  p2.add_expr_item(item_c);
  p2.set_tid_cid(1000, 2);
  p2.add_expr_item_end();

  ObProject project;
  const ObRow *next_row_p = NULL;
  ObSingleChildPhyOperator &single_op = project;
  ObFakeTable input_table;

  input_table.set_row_count(1000);
  ASSERT_EQ(OB_SUCCESS , single_op.set_child(0, input_table));
  ASSERT_EQ(OB_SUCCESS , project.add_output_column(p));
  ASSERT_EQ(OB_SUCCESS , project.add_output_column(p2));

  ASSERT_EQ(OB_SUCCESS ,project.open());
  while(OB_SUCCESS == (ret = project.get_next_row(next_row_p)))
  {
    ASSERT_EQ(true, NULL != next_row_p);
    const ObObj *result = NULL;
    const ObObj *result2 = NULL;
    ASSERT_EQ(OB_SUCCESS, next_row_p->get_cell(1000, 1, result));
    ASSERT_EQ(true, NULL != result);
    ASSERT_EQ(OB_SUCCESS, next_row_p->get_cell(1000, 2, result2));
    ASSERT_EQ(true, NULL != result2);

    int64_t re, re2;
    ASSERT_EQ(OB_SUCCESS, result->get_int(re));
    ASSERT_EQ(OB_SUCCESS, result2->get_int(re2));
    ASSERT_EQ(re, re2);
    //TBSYS_LOG(DEBUG, "re=%d, re2=%d", re, re2);
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(OB_SUCCESS , project.close());
}




TEST_F(ObProjectTest, serialize_test)
{
  int ret = OB_SUCCESS;
  ObSqlExpression p, p2;
  ExprItem item_a, item_b, item_add, item_c;
  ObObj obj_a, obj_b;

  item_a.type_ = T_REF_COLUMN;
  item_b.type_ = T_REF_COLUMN;
  item_add.type_ = T_OP_ADD;
  item_c.type_ = T_REF_COLUMN;
  item_a.value_.cell_.tid = test::ObFakeTable::TABLE_ID;
  item_a.value_.cell_.cid = OB_APP_MIN_COLUMN_ID + 4;
  item_b.value_.cell_.tid = test::ObFakeTable::TABLE_ID;
  item_b.value_.cell_.cid = OB_APP_MIN_COLUMN_ID + 5;
  item_add.value_.int_ = 2;
  item_c.value_.cell_.tid = test::ObFakeTable::TABLE_ID;
  item_c.value_.cell_.cid = OB_APP_MIN_COLUMN_ID + 7;

  p.add_expr_item(item_a);
  p.add_expr_item(item_b);
  p.add_expr_item(item_add);

  p.set_tid_cid(1000, 1);
  p.add_expr_item_end();

  p2.add_expr_item(item_c);
  p2.set_tid_cid(1000, 2);
  p2.add_expr_item_end();

  ObProject project;
  ObProject project_deserialized;
  const ObRow *next_row_p = NULL;
  ObSingleChildPhyOperator &single_op = project;
  ObFakeTable input_table;

  input_table.set_row_count(1000);
  ASSERT_EQ(OB_SUCCESS , single_op.set_child(0, input_table));
  ASSERT_EQ(OB_SUCCESS , project.add_output_column(p));
  ASSERT_EQ(OB_SUCCESS , project.add_output_column(p2));

  const int64_t buf_len = 100;
  char buf[buf_len];
  int64_t pos = 0;
  TBSYS_LOG(WARN, "go!!");
  ASSERT_EQ(OB_SUCCESS, project.serialize(buf, buf_len, pos));
  TBSYS_LOG(WARN, "homeo!! %ld", pos);

  ASSERT_TRUE(pos > 0);
  int64_t data_len = pos;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS , project_deserialized.deserialize(buf, data_len, pos));
  ASSERT_EQ(OB_SUCCESS , project_deserialized.set_child(0, input_table));

  ASSERT_EQ(OB_SUCCESS ,project_deserialized.open());
  while(OB_SUCCESS == (ret = project_deserialized.get_next_row(next_row_p)))
  {
    ASSERT_EQ(true, NULL != next_row_p);
    const ObObj *result = NULL;
    const ObObj *result2 = NULL;
    ASSERT_EQ(OB_SUCCESS, next_row_p->get_cell(1000, 1, result));
    ASSERT_EQ(true, NULL != result);
    ASSERT_EQ(OB_SUCCESS, next_row_p->get_cell(1000, 2, result2));
    ASSERT_EQ(true, NULL != result2);

    int64_t re, re2;
    ASSERT_EQ(OB_SUCCESS, result->get_int(re));
    ASSERT_EQ(OB_SUCCESS, result2->get_int(re2));
    ASSERT_EQ(re, re2);
    //TBSYS_LOG(DEBUG, "re=%d, re2=%d", re, re2);
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(OB_SUCCESS , project_deserialized.close());
}






int main(int argc, char **argv)
{

  TBSYS_LOGGER.setLogLevel("INFO");
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
