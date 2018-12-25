#include "gtest/gtest.h"
#include "common/ob_compact_cell_util.h"
#include "common/ob_compact_cell_writer.h"
#include "common/ob_compact_cell_iterator.h"
#include "common/ob_malloc.h"
#include "common/ob_string.h"

using namespace oceanbase;
using namespace common;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

TEST(TestObCompactCell, decimal)
{
  char buf[1024];
  ObCompactCellWriter writer;
  writer.init(buf, 1024, DENSE);

  ObNumber number1;
  number1.from("1323232323323288888");

  ObObj value;
  value.set_decimal(number1);

  ObObj clone_value;

  writer.append(1, value, &clone_value);

  ObNumber n1;
  clone_value.get_decimal(n1);

  ASSERT_TRUE(number1 == n1);

  ObCompactCellIterator iterator;
  iterator.init(buf);

  uint64_t column_id = OB_INVALID_ID;
  ObObj get_value;

  ObNumber number2;
  number2.from("321");

  value.set_decimal(number2);
  writer.append(2, value, &clone_value);

  OK(iterator.next_cell());
  OK(iterator.get_cell(column_id, get_value));

  get_value.get_decimal(n1);
  ASSERT_TRUE(number1 == n1);

  ObNumber n2;

  OK(iterator.next_cell());
  OK(iterator.get_cell(column_id, get_value));

  get_value.get_decimal(n2);
  ASSERT_TRUE(number2 == n2);
}

TEST(TestObCompactCell, row_end)
{
  char buf[1024];
  ObCompactCellWriter writer;
  writer.init(buf, 1024, DENSE);

  OK(writer.row_finish());
  OK(writer.row_finish());
  OK(writer.row_finish());
  OK(writer.row_finish());

  ObCompactCellIterator iter;
  iter.init(buf, DENSE);

  const ObObj *value = NULL;
  bool is_row_finished = false;
  ObString row;

  OK(iter.next_cell());
  OK(iter.get_cell(value, &is_row_finished, &row));
  ASSERT_TRUE(is_row_finished);
  ASSERT_EQ(1, row.length());

  OK(iter.next_cell());
  OK(iter.get_cell(value, &is_row_finished, &row));
  ASSERT_TRUE(is_row_finished);
  ASSERT_EQ(1, row.length());
}
 
TEST(TestObCompactCell, DENSE_DENSE)
{
  char buf[1024];
  ObCompactCellWriter writer;
  writer.init(buf, 1024, DENSE_DENSE);

  ObObj value;
  for(int64_t j=0;j<5;j++)
  {
    for(int64_t i=0;i<5;i++)
    {
      value.set_int(i);
      OK(writer.append(value));
    }
    OK(writer.row_finish());
    for(int64_t i=0;i<5;i++)
    {
      value.set_int(i);
      writer.append(value);
    }
    OK(writer.row_finish());
  }

  ObCompactCellIterator iter;
  iter.init(buf, DENSE_DENSE);

  uint64_t column_id = 0;
  int64_t int_value = 0;
  bool is_row_finished = false;
  for(int64_t j=0;j<5;j++)
  {
    for(int64_t i=0;i<5;i++)
    {
      OK(iter.next_cell());
      OK(iter.get_cell(column_id, value));
      OK(value.get_int(int_value));
      ASSERT_EQ(i, int_value);
      OK(value.get_int(int_value));
      ASSERT_EQ(i, int_value);
      ASSERT_EQ(OB_INVALID_ID, column_id);
    }

    ObString row;
    ObCompactCellIterator iter2;

    OK(iter.next_cell());
    OK(iter.get_cell(column_id, value, &is_row_finished, &row));
    ASSERT_TRUE(is_row_finished);
    iter2.init(row, DENSE);
    for(int64_t i=0;i<5;i++)
    {
      OK(iter2.next_cell());
      OK(iter2.get_cell(column_id, value));
      OK(value.get_int(int_value));
      ASSERT_EQ(i, int_value);
      ASSERT_EQ(OB_INVALID_ID, column_id);
    }

    for(int64_t i=0;i<5;i++)
    {
      OK(iter.next_cell());
      OK(iter.get_cell(column_id, value));
      OK(value.get_int(int_value));
      ASSERT_EQ(i, int_value);
      OK(value.get_int(int_value));
      ASSERT_EQ(i, int_value);
      ASSERT_EQ(OB_INVALID_ID, column_id);
    }

    OK(iter.next_cell());
    OK(iter.get_cell(column_id, value, &is_row_finished, &row));
    ASSERT_TRUE(is_row_finished);
    OK(iter.get_cell(column_id, value, &is_row_finished, &row));
    ASSERT_TRUE(is_row_finished);

    iter2.init(row, DENSE);
    for(int64_t i=0;i<5;i++)
    {
      OK(iter2.next_cell());
      OK(iter2.get_cell(column_id, value));
      OK(value.get_int(int_value));
      ASSERT_EQ(i, int_value);
      ASSERT_EQ(OB_INVALID_ID, column_id);
    }
    OK(iter2.next_cell());
    OK(iter2.get_cell(column_id, value, &is_row_finished, &row));
    ASSERT_TRUE(is_row_finished);

    ASSERT_EQ(OB_ITER_END, iter2.next_cell());

  }
}

TEST(TestObCompactCell, DENSE_SPARSE)
{
  char buf[1024];
  ObCompactCellWriter writer;
  writer.init(buf, 1024, DENSE_SPARSE);

  ObObj value;
  for(int64_t j=0;j<5;j++)
  {
    for(int64_t i=0;i<5;i++)
    {
      value.set_int(i);
      OK(writer.append(value));
    }
    OK(writer.row_finish());
    for(int64_t i=0;i<5;i++)
    {
      value.set_int(i);
      writer.append(1, value);
    }
    OK(writer.row_finish());
  }

  ObCompactCellIterator iter;
  iter.init(buf, DENSE_SPARSE);

  uint64_t column_id = 0;
  int64_t int_value = 0;
  bool is_row_finished = false;
  for(int64_t j=0;j<5;j++)
  {
    for(int64_t i=0;i<5;i++)
    {
      OK(iter.next_cell());
      OK(iter.get_cell(column_id, value));
      OK(value.get_int(int_value));
      ASSERT_EQ(i, int_value);
      ASSERT_EQ(OB_INVALID_ID, column_id);
    }

    ObString row;
    ObCompactCellIterator iter2;

    OK(iter.next_cell());
    OK(iter.get_cell(column_id, value, &is_row_finished, &row));
    ASSERT_TRUE(is_row_finished);

    iter2.init(row, DENSE);
    for(int64_t i=0;i<5;i++)
    {
      OK(iter2.next_cell());
      OK(iter2.get_cell(column_id, value));
      OK(value.get_int(int_value));
      ASSERT_EQ(i, int_value);
      ASSERT_EQ(OB_INVALID_ID, column_id);
    }


    for(int64_t i=0;i<5;i++)
    {
      OK(iter.next_cell());
      OK(iter.get_cell(column_id, value));
      OK(value.get_int(int_value));
      ASSERT_EQ(i, int_value);
      ASSERT_EQ(1u, column_id);
    }

    OK(iter.next_cell());
    OK(iter.get_cell(column_id, value, &is_row_finished, &row));
    ASSERT_TRUE(is_row_finished);

    iter2.init(row, SPARSE);
    for(int64_t i=0;i<5;i++)
    {
      OK(iter2.next_cell());
      OK(iter2.get_cell(column_id, value));
      OK(value.get_int(int_value));
      ASSERT_EQ(i, int_value);
      ASSERT_EQ(1u, column_id);
    }
    OK(iter2.next_cell());
    OK(iter2.get_cell(column_id, value, &is_row_finished, &row));
    ASSERT_TRUE(is_row_finished);

    ASSERT_EQ(OB_ITER_END, iter2.next_cell());
  }
}

TEST(TestObCompactCell, DENSE)
{
  char buf[1024];
  ObCompactCellWriter writer;
  writer.init(buf, 1024, DENSE);

  ObObj value;
  for(int64_t j=0;j<5;j++)
  {
    for(int64_t i=0;i<10;i++)
    {
      value.set_int(i+j);
      OK(writer.append(value));
    }
    OK(writer.row_finish());
  }

  ObCompactCellIterator iter;
  iter.init(buf, DENSE);

  uint64_t column_id = 0;
  int64_t int_value = 0;
  bool is_row_finished = false;
  for(int64_t j=0;j<5;j++)
  {
    for(int64_t i=0;i<10;i++)
    {
      OK(iter.next_cell());
      OK(iter.get_cell(column_id, value));
      OK(value.get_int(int_value));
      ASSERT_EQ(i+j, int_value);
      ASSERT_EQ(OB_INVALID_ID, column_id);
    }
    OK(iter.next_cell());
    OK(iter.get_cell(column_id, value, &is_row_finished));
    ASSERT_TRUE(is_row_finished);
  }
}

TEST(TestObCompactCell, writer_varchar)
{
  char buf[1024];
  ObCompactCellWriter writer;
  writer.init(buf, 1024);

  char *str = const_cast<char*>("oceanbase");
  ObString varchar;
  varchar.assign_ptr(str, (int32_t)strlen(str));

  ObObj value;
  value.set_varchar(varchar);

  ObObj clone_value;

  writer.append(1, value, &clone_value);

  ObString varchar2;
  clone_value.get_varchar(varchar2);

  printf("varchar:%.*s\n", varchar2.length(), varchar2.ptr());

  ASSERT_TRUE(varchar == varchar2);
}

TEST(TestObCompactCell, writer)
{
  char buf[1024];
  ObCompactCellWriter writer;
  writer.init(buf, 1024);

  uint64_t column_id = 3;
  ObObj value;

  value.set_int(32);
  OK(writer.append(column_id, value));

  value.set_int(13888);
  OK(writer.append(column_id, value));

  OK(writer.row_finish());

  value.set_int(888);
  OK(writer.append(column_id, value));
  OK(writer.row_finish());

  ObCompactCellIterator iter;
  ObString str;
  int64_t int_value = 0;
  bool is_row_finished = false;

  printf("writer.size() %ld\n", writer.size());

  str.assign_ptr(buf, (int32_t)writer.size());
  iter.init(str);

  uint64_t column_id_get = 0;
  ObObj value_get;

  OK(iter.next_cell());
  OK(iter.get_cell(column_id_get, value_get));

  value_get.get_int(int_value);
  ASSERT_EQ(32, int_value);

  OK(iter.next_cell());
  OK(iter.get_cell(column_id_get, value_get, &is_row_finished));

  ASSERT_FALSE(is_row_finished);

  value_get.get_int(int_value);
  ASSERT_EQ(13888, int_value);


  ObString row;

  OK(iter.next_cell());
  OK(iter.get_cell(column_id_get, value_get, &is_row_finished, &row));

  //ASSERT_TRUE(is_row_finished);
  //ASSERT_EQ(row.ptr(), str.ptr());
  //ASSERT_EQ(row.length(), str.length());
  //ASSERT_EQ(OB_ITER_END, iter.next_cell());

  OK(iter.next_cell());
  OK(iter.get_cell(column_id_get, value_get));

  value_get.get_int(int_value);
  ASSERT_EQ(888, int_value);

  OK(iter.next_cell());
  OK(iter.get_cell(column_id_get, value_get, &is_row_finished, &row));

  ASSERT_EQ(OB_ITER_END, iter.next_cell());

  iter.init(row);
  OK(iter.next_cell());
  OK(iter.get_cell(column_id_get, value_get));

  value_get.get_int(int_value);
  ASSERT_EQ(888, int_value);
}

TEST(TestObCompactCell, util)
{
  ObObj value;
  value.set_int(32);

  int ret = get_compact_cell_size(value);
  ASSERT_EQ(4, ret);

  ret = get_compact_cell_max_size(value.get_type());
  ASSERT_EQ(11, ret);

  ObString varchar;
  char *str = const_cast<char*>("oceanbase");
  varchar.assign_ptr(str, static_cast<int32_t>(strlen(str)));
  value.set_varchar(varchar);

  ret = get_compact_cell_size(value);
  ASSERT_EQ(5 + (int64_t)strlen(str), ret);


  value.set_ext(ObActionFlag::OP_DEL_ROW);
  ret = get_compact_cell_size(value);
  ASSERT_EQ(1, ret);

  ret = get_compact_cell_max_size(value.get_type());
  ASSERT_EQ(1, ret);
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
