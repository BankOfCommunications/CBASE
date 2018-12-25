/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_filter_test.cpp,  06/04/2012 05:47:34 PM xiaochu Exp $
 *
 * Author:
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:
 *   Test ObFilter class
 *
 */
#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include "common/ob_string_buf.h"
#include "sql/ob_sql_expression.h"
#include "common/ob_row.h"
#include "sql/ob_filter.h"
#include "ob_fake_table.h"


using namespace oceanbase::sql;
using namespace oceanbase::sql::test;
using namespace oceanbase::common;

class ObPhyOperatorStub : public ObPhyOperator
{
  public:
    ObPhyOperatorStub()
      : pos_(0)
    {
      memset(data, 0, sizeof(data));
      data[0][0] = 1, data[0][1] = 9;
      data[1][0] = 2, data[1][1] = 8;
      data[2][0] = 3, data[2][1] = 7;
      data[3][0] = 4, data[3][1] = 6;
      data[4][0] = 5, data[4][1] = 5;
      data[5][0] = 6, data[5][1] = 4;
      data[6][0] = 7, data[6][1] = 3;
      data[7][0] = 8, data[7][1] = 2;
      data[8][0] = 9, data[8][1] = 1;
      data[9][0] = 0, data[9][1] = 0;
    }

    ~ObPhyOperatorStub(){}
    virtual ObPhyOperatorType get_type() const { return PHY_INVALID; }

    void reset() {}
    void reuse() {}
    int get_eq_row_count()
    {
      return 2;
    }

    int get_lt_row_count()
    {
      // data[x][0] < data[x][1]
      return 4;
    }

    int get_ne_row_count()
    {
      return 8;
    }

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
    virtual int get_row_desc(const ObRowDesc *&row_desc) const {row_desc=NULL;return OB_NOT_IMPLEMENT;}
    int get_next_row(const ObRow *&row)
    {
      ObRowDesc row_desc;
      ObObj obj_a, obj_b;

      if (pos_ == 10)
      {
        return OB_ITER_END;
      }

      row_desc.add_column_desc(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+2);
      row_desc.add_column_desc(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+3);
      obj_a.set_int(data[pos_][0]);
      obj_b.set_int(data[pos_][1]);
      row_.set_row_desc(row_desc);
      row_.set_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+2, obj_a);
      row_.set_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+3, obj_b);
      row = &row_;
      pos_++;

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
private:
    int data[10][2];
    int pos_;
};

class ObFilterTest: public ::testing::Test
{
  public:
    ObFilterTest();
    virtual ~ObFilterTest();
    virtual void SetUp();
    virtual void TearDown();
    int gen_sql_expression(ObSqlExpression &p, ObItemType type);
  private:
    // disallow copy
    ObFilterTest(const ObFilterTest &other);
    ObFilterTest& operator=(const ObFilterTest &other);
  private:
    // data members
};

ObFilterTest::ObFilterTest()
{
}

ObFilterTest::~ObFilterTest()
{
}

void ObFilterTest::SetUp()
{
}

void ObFilterTest::TearDown()
{
}

int ObFilterTest::gen_sql_expression(ObSqlExpression &p, ObItemType type)
{
  ExprItem item_a, item_b, item_add;
  ObObj obj_a, obj_b;

  item_a.type_ = T_REF_COLUMN;
  item_b.type_ = T_REF_COLUMN;
  item_add.type_ = type;
  item_a.value_.cell_.tid = test::ObFakeTable::TABLE_ID;
  item_a.value_.cell_.cid = OB_APP_MIN_COLUMN_ID+2;
  item_b.value_.cell_.tid = test::ObFakeTable::TABLE_ID;
  item_b.value_.cell_.cid = OB_APP_MIN_COLUMN_ID+3;
  item_add.value_.int_ = 2;

  p.add_expr_item(item_a);
  p.add_expr_item(item_b);
  p.add_expr_item(item_add);

  p.set_tid_cid(1000, 1);
  p.add_expr_item_end();

  return OB_SUCCESS;
}

TEST_F(ObFilterTest, one_filter_eq_basic_test)
{
  ObFilter filter;
  ObSingleChildPhyOperator &single_op = filter;
  ObPhyOperatorStub input_table;
  const ObRow *next_row_p = NULL;
  int count = 0;

  ObSqlExpression *p = ObSqlExpression::alloc();

  single_op.set_child(0, input_table);
  gen_sql_expression(*p, T_OP_EQ);
  filter.add_filter(p);
  filter.open();

  while(OB_ITER_END != filter.get_next_row(next_row_p))
  {
    ASSERT_EQ(true, NULL != next_row_p);
    const ObObj *result1 = NULL;
    const ObObj *result2 = NULL;
    int64_t re1 = 0, re2=1;

    next_row_p->get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+2, result1);
    ASSERT_EQ(true, NULL != result1);
    ASSERT_EQ(OB_SUCCESS, result1->get_int(re1));
    next_row_p->get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+3, result2);
    ASSERT_EQ(true, NULL != result2);
    ASSERT_EQ(OB_SUCCESS, result2->get_int(re2));
    ASSERT_EQ(re1, re2);

    next_row_p = NULL;
    TBSYS_LOG(INFO, "find a matched cell. v1=%ld, v2=%ld", re1, re2);
    count++;
  }
  ASSERT_EQ(count, input_table.get_eq_row_count());
  filter.close();
}

TEST_F(ObFilterTest, one_filter_neq_basic_test)
{
  ObFilter filter;
  ObSingleChildPhyOperator &single_op = filter;
  ObPhyOperatorStub input_table;
  const ObRow *next_row_p = NULL;
  int count = 0;

  ObSqlExpression *p = ObSqlExpression::alloc();
  ObStringBuf buf;

  single_op.set_child(0, input_table);
  gen_sql_expression(*p, T_OP_NE);
  filter.add_filter(p);
  filter.open();


  while(OB_ITER_END != filter.get_next_row(next_row_p))
  {
    ASSERT_EQ(true, NULL != next_row_p);
    const ObObj *result1 = NULL;
    const ObObj *result2 = NULL;
    int64_t re1 = 0, re2=1;

    next_row_p->get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+2, result1);
    ASSERT_EQ(true, NULL != result1);
    ASSERT_EQ(OB_SUCCESS, result1->get_int(re1));
    next_row_p->get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+3, result2);
    ASSERT_EQ(true, NULL != result2);
    ASSERT_EQ(OB_SUCCESS, result2->get_int(re2));
    ASSERT_NE(re1, re2);

    next_row_p = NULL;
    TBSYS_LOG(INFO, "find a matched cell. v1=%ld, v2=%ld", re1, re2);
    count++;
  }
  ASSERT_EQ(count, input_table.get_ne_row_count());
  filter.close();
}


//////////////////////////////////////////////////////////////

TEST_F(ObFilterTest, one_filter_eq_basic_fake_table_serialize_test)
{
  ObFilter filter, filter_serialize;
  ObSingleChildPhyOperator &single_op = filter;
  ObFakeTable input_table;
  const ObRow *next_row_p = NULL;
  int count = 0;

  ObSqlExpression *p = ObSqlExpression::alloc();

  const int64_t buf_len = 1024;
  char buf[buf_len];
  int64_t data_len = 0;
  int64_t pos = 0;

  input_table.set_row_count(100);

  single_op.set_child(0, input_table);
  gen_sql_expression(*p, T_OP_EQ);
  filter.add_filter(p);
  filter.open();

  while(OB_ITER_END != filter.get_next_row(next_row_p))
  {
    ASSERT_EQ(true, NULL != next_row_p);
    const ObObj *result1 = NULL;
    const ObObj *result2 = NULL;
    int64_t re1 = 0, re2=1;

    /* for(i = 0; i <100; i++): idx%2 == idx%3 */
    next_row_p->get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+2, result1);
    ASSERT_EQ(true, NULL != result1);
    ASSERT_EQ(OB_SUCCESS, result1->get_int(re1));
    next_row_p->get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+3, result2);
    ASSERT_EQ(true, NULL != result2);
    ASSERT_EQ(OB_SUCCESS, result2->get_int(re2));
    ASSERT_EQ(re1, re2);

    next_row_p = NULL;
    TBSYS_LOG(INFO, "find a matched cell. v1=%ld, v2=%ld", re1, re2);
    count++;
  }
  ASSERT_EQ(count, 34);
  filter.close();

  // use deserialized filter to test again
  ASSERT_EQ(OB_SUCCESS, filter.serialize(buf, buf_len, pos));
  data_len = pos;
  pos = 0;

  ObFakeTable input_table2;
  input_table2.set_row_count(100);
  count = 0;
  next_row_p = NULL;
  ASSERT_EQ(OB_SUCCESS, filter_serialize.deserialize(buf, data_len, pos));
  ASSERT_EQ(pos, data_len);
  filter_serialize.set_child(0, input_table2);
  ASSERT_EQ(OB_SUCCESS, filter_serialize.open());
  while(OB_ITER_END != filter_serialize.get_next_row(next_row_p))
  {
    ASSERT_EQ(true, NULL != next_row_p);
    const ObObj *result1 = NULL;
    const ObObj *result2 = NULL;
    int64_t re1 = 0, re2=1;

    /* for(i = 0; i <100; i++): idx%2 == idx%3 */
    next_row_p->get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+2, result1);
    ASSERT_EQ(true, NULL != result1);
    ASSERT_EQ(OB_SUCCESS, result1->get_int(re1));
    next_row_p->get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+3, result2);
    ASSERT_EQ(true, NULL != result2);
    ASSERT_EQ(OB_SUCCESS, result2->get_int(re2));
    ASSERT_EQ(re1, re2);

    next_row_p = NULL;
    TBSYS_LOG(INFO, "find a matched cell. v1=%ld, v2=%ld", re1, re2);
    count++;
  }
  ASSERT_EQ(count, 34);
  ASSERT_EQ(OB_SUCCESS, filter_serialize.close());

}


TEST_F(ObFilterTest, one_filter_eq_basic_fake_table_test)
{
  ObFilter filter;
  ObSingleChildPhyOperator &single_op = filter;
  ObFakeTable input_table;
  const ObRow *next_row_p = NULL;
  int count = 0;

  ObSqlExpression *p = ObSqlExpression::alloc();
  ObStringBuf buf;

  input_table.set_row_count(100);

  single_op.set_child(0, input_table);
  gen_sql_expression(*p, T_OP_EQ);
  filter.add_filter(p);
  filter.open();

  while(OB_ITER_END != filter.get_next_row(next_row_p))
  {
    ASSERT_EQ(true, NULL != next_row_p);
    const ObObj *result1 = NULL;
    const ObObj *result2 = NULL;
    int64_t re1 = 0, re2=1;

    /* for(i = 0; i <100; i++): idx%2 == idx%3 */
    next_row_p->get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+2, result1);
    ASSERT_EQ(true, NULL != result1);
    ASSERT_EQ(OB_SUCCESS, result1->get_int(re1));
    next_row_p->get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+3, result2);
    ASSERT_EQ(true, NULL != result2);
    ASSERT_EQ(OB_SUCCESS, result2->get_int(re2));
    ASSERT_EQ(re1, re2);

    next_row_p = NULL;
    TBSYS_LOG(INFO, "find a matched cell. v1=%ld, v2=%ld", re1, re2);
    count++;
  }
  ASSERT_EQ(count, 34);
  filter.close();
}

TEST_F(ObFilterTest, one_filter_neq_basic_fake_table_test)
{
  ObFilter filter;
  ObSingleChildPhyOperator &single_op = filter;
  ObFakeTable input_table;
  const ObRow *next_row_p = NULL;
  int count = 0;

  ObSqlExpression *p = ObSqlExpression::alloc();
  ObStringBuf buf;

  input_table.set_row_count(100);

  single_op.set_child(0, input_table);
  gen_sql_expression(*p, T_OP_NE);
  filter.add_filter(p);
  filter.open();


  while(OB_ITER_END != filter.get_next_row(next_row_p))
  {
    ASSERT_EQ(true, NULL != next_row_p);
    const ObObj *result1 = NULL;
    const ObObj *result2 = NULL;
    int64_t re1 = 0, re2=1;

    /* for(i = 0; i <100; i++): idx%2 == idx%3 */
    next_row_p->get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+2, result1);
    ASSERT_EQ(true, NULL != result1);
    ASSERT_EQ(OB_SUCCESS, result1->get_int(re1));
    next_row_p->get_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID+3, result2);
    ASSERT_EQ(true, NULL != result2);
    ASSERT_EQ(OB_SUCCESS, result2->get_int(re2));
    ASSERT_NE(re1, re2);

    next_row_p = NULL;
    TBSYS_LOG(INFO, "find a matched cell. v1=%ld, v2=%ld", re1, re2);
    count++;
  }
  ASSERT_EQ(count, 66);
  filter.close();
}


int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
