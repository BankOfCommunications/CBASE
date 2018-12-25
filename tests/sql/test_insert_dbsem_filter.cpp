#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include "ob_malloc.h"
#include "sql/ob_insert_dbsem_filter.h"
#include "sql/ob_values.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace common;
using namespace sql;

void build_values(const int64_t row_count, ObRowDesc &row_desc, ObExprValues &values1, ObValues &values2)
{
  for (int64_t i = 0; i < row_count; i++)
  {
    ObRow row;
    row.set_row_desc(row_desc);
    for (int64_t j = 0; j < row_desc.get_column_num(); j++)
    {
      uint64_t table_id = OB_INVALID_ID;
      uint64_t column_id = OB_INVALID_ID;
      row_desc.get_tid_cid(j, table_id, column_id);
      ObObj obj;
      obj.set_int(i + j);
      row.set_cell(table_id, column_id, obj); 

      ObSqlExpression se;
      se.set_tid_cid(table_id, column_id);
      ExprItem ei;
      ei.type_ = T_INT;
      ei.value_.int_ = i + j;
      se.add_expr_item(ei);
      //ei.type_ = T_REF_COLUMN;
      //ei.value_.cell_.tid = table_id;
      //ei.value_.cell_.cid = column_id;
      //se.add_expr_item(ei);
      se.add_expr_item_end();

      values1.add_value(se);
    }
    //values1.add_values(row);
    values2.add_values(row);
  }
}

bool equal(const ObRowDesc &rd1, const ObRowDesc &rd2)
{
  bool bret = false;
  if (rd1.get_column_num() == rd2.get_column_num())
  {
    bret = true;
    for (int64_t i = 0; i < rd1.get_column_num(); i++)
    {
      uint64_t tid1 = OB_INVALID_ID;
      uint64_t cid1 = OB_INVALID_ID;
      uint64_t tid2 = OB_INVALID_ID;
      uint64_t cid2 = OB_INVALID_ID;
      rd1.get_tid_cid(i, tid1, cid1);
      rd2.get_tid_cid(i, tid2, cid2);
      if (tid1 != tid2
          || cid1 != cid2)
      {
        bret = false;
        break;
      }
    }
  }
  return bret;
}

bool equal(const ObRow &r1, const ObRow &r2)
{
  bool bret = false;
  const ObRowDesc *rd1 = r1.get_row_desc();
  const ObRowDesc *rd2 = r2.get_row_desc();
  if (equal(*rd1, *rd2))
  {
    bret = true;
    for (int64_t i = 0; i < rd1->get_column_num(); i++)
    {
      const ObObj *c1 = NULL;
      uint64_t tid1 = OB_INVALID_ID;
      uint64_t cid1 = OB_INVALID_ID;
      const ObObj *c2 = NULL;
      uint64_t tid2 = OB_INVALID_ID;
      uint64_t cid2 = OB_INVALID_ID;
      r1.raw_get_cell(i, c1, tid1, cid1);
      r2.raw_get_cell(i, c2, tid2, cid2);
      if (*c1 != *c2
          || tid1 != tid2
          || cid1 != cid2)
      {
        bret = false;
        break;
      }
    }
  }
  return bret;
}

TEST(TestObInsertDBSemFilter, iter_succ)
{
  ObInsertDBSemFilter dbi;
  ObRowDesc row_desc;
  row_desc.add_column_desc(1001, 16);
  row_desc.add_column_desc(1001, 17);
  row_desc.add_column_desc(1001, 101);
  row_desc.add_column_desc(1001, 102);
  ObRowDescExt row_desc_ext;
  ObObj type;
  type.set_int(0);
  row_desc_ext.add_column_desc(1001, 16, type);
  row_desc_ext.add_column_desc(1001, 17, type);
  row_desc_ext.add_column_desc(1001, 101, type);
  row_desc_ext.add_column_desc(1001, 102, type);
  ObValues cc;
  dbi.get_values().set_row_desc(row_desc, row_desc_ext);
  cc.set_row_desc(row_desc);
  build_values(10, row_desc, dbi.get_values(), cc);

  ObValues empty_values;
  dbi.set_child(0, empty_values);

  int ret = dbi.open();
  EXPECT_EQ(OB_SUCCESS, ret);

  cc.open();
  const ObRow *row1 = NULL;
  const ObRow *row2 = NULL;
  while (OB_SUCCESS == cc.get_next_row(row1))
  {
    ret = dbi.get_next_row(row2);
    EXPECT_EQ(OB_SUCCESS, ret);

    EXPECT_EQ(true, equal(*row1, *row2));
  }

  ret = cc.get_next_row(row1);
  EXPECT_EQ(OB_ITER_END, ret);

  ret = dbi.get_next_row(row2);
  EXPECT_EQ(OB_ITER_END, ret);

  ret = dbi.close();
  EXPECT_EQ(OB_SUCCESS, ret);
}

TEST(TestObInsertDBSemFilter, child_not_empty)
{
  ObInsertDBSemFilter dbi;
  ObRowDesc row_desc;
  row_desc.add_column_desc(1001, 16);
  row_desc.add_column_desc(1001, 17);
  row_desc.add_column_desc(1001, 101);
  row_desc.add_column_desc(1001, 102);
  ObRowDescExt row_desc_ext;
  ObObj type;
  type.set_int(0);
  row_desc_ext.add_column_desc(1001, 16, type);
  row_desc_ext.add_column_desc(1001, 17, type);
  row_desc_ext.add_column_desc(1001, 101, type);
  row_desc_ext.add_column_desc(1001, 102, type);
  ObValues cc;
  dbi.get_values().set_row_desc(row_desc, row_desc_ext);
  cc.set_row_desc(row_desc);
  build_values(10, row_desc, dbi.get_values(), cc);

  dbi.set_child(0, cc);

  int ret = dbi.open();
  EXPECT_EQ(OB_ERR_PRIMARY_KEY_DUPLICATE, ret);

  const ObRow *row = NULL;
  ret = dbi.get_next_row(row);
  EXPECT_EQ(OB_ITER_END, ret);

  ret = dbi.close();
  EXPECT_EQ(OB_SUCCESS, ret);
}

TEST(TestObInsertDBSemFilter, iter_succ_after_fail)
{
  ObInsertDBSemFilter dbi;
  ObRowDesc row_desc;
  row_desc.add_column_desc(1001, 16);
  row_desc.add_column_desc(1001, 17);
  row_desc.add_column_desc(1001, 101);
  row_desc.add_column_desc(1001, 102);
  ObRowDescExt row_desc_ext;
  ObObj type;
  type.set_int(0);
  row_desc_ext.add_column_desc(1001, 16, type);
  row_desc_ext.add_column_desc(1001, 17, type);
  row_desc_ext.add_column_desc(1001, 101, type);
  row_desc_ext.add_column_desc(1001, 102, type);
  ObValues cc;
  dbi.get_values().set_row_desc(row_desc, row_desc_ext);
  cc.set_row_desc(row_desc);
  build_values(10, row_desc, dbi.get_values(), cc);

  dbi.set_child(0, cc);

  int ret = dbi.open();
  EXPECT_EQ(OB_ERR_PRIMARY_KEY_DUPLICATE, ret);

  const ObRow *row = NULL;
  ret = dbi.get_next_row(row);
  EXPECT_EQ(OB_ITER_END, ret);

  ret = dbi.close();
  EXPECT_EQ(OB_SUCCESS, ret);

  //////////////////////////////////////////////////

  ObValues empty_values;
  dbi.clear();
  dbi.get_values().get_values().clear();
  dbi.set_child(0, empty_values);

  dbi.get_values().set_row_desc(row_desc, row_desc_ext);
  cc.set_row_desc(row_desc);
  build_values(10, row_desc, dbi.get_values(), cc);

  ret = dbi.open();
  EXPECT_EQ(OB_SUCCESS, ret);

  cc.open();
  const ObRow *row1 = NULL;
  const ObRow *row2 = NULL;
  while (OB_SUCCESS == cc.get_next_row(row1))
  {
    ret = dbi.get_next_row(row2);
    EXPECT_EQ(OB_SUCCESS, ret);

    EXPECT_EQ(true, equal(*row1, *row2));
  }

  ret = cc.get_next_row(row1);
  EXPECT_EQ(OB_ITER_END, ret);

  ret = dbi.get_next_row(row2);
  EXPECT_EQ(OB_ITER_END, ret);

  ret = dbi.close();
  EXPECT_EQ(OB_SUCCESS, ret);
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv); 
  int ret = RUN_ALL_TESTS();
  return ret;
}

