#include "sql/ob_merge_except.h"
#include "ob_fake_table.h"
#include "common/ob_row.h"
#include <gtest/gtest.h>
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::test;

class ObFakeTable2 : public ObFakeTable
{
  public:
    typedef int (ObFakeTable2::*generate_data_strategy)(const ObRow *& row, int row_index);
    ObFakeTable2();
    virtual ~ObFakeTable2();
    virtual int get_next_row(const ObRow *&row);
    virtual int open();
    void set_column_count(int column_count);
    void set_generate_data_strategy(generate_data_strategy strategy);
    int generate_data1(const ObRow *&row, int row_index);
    int generate_data2(const ObRow *&row, int row_index);
    int set_name(const char *name);
  private:
    int column_count_;
    generate_data_strategy strategy_;
    int cur_row_index_;
    const char *name_;
};
ObFakeTable2::~ObFakeTable2()
{
}
int ObFakeTable2::set_name(const char *name)
{
  name_ = name;
  return OB_SUCCESS;
}
// 1,1,2,2,3,7
int ObFakeTable2::generate_data1(const ObRow *& row, int row_index)
{
  int ret = OB_SUCCESS;
  if (row_index > row_count_)
  {
    ret = OB_ITER_END;
  }
  else
  {
    ObObj cell;
    if (row_index == 1 || row_index == 2)
    {
      cell.set_int(1);
      if (OB_SUCCESS != (ret = curr_row_.set_cell(table_id_, OB_APP_MIN_COLUMN_ID + 1 , cell)))
      {
        TBSYS_LOG(WARN, "failed to set cell");
      }
      else
      {
        row = &curr_row_;
      }
    }
    else if (row_index == 3 || row_index == 4)
    {
      cell.set_int(2);
      if (OB_SUCCESS != (ret = curr_row_.set_cell(table_id_, OB_APP_MIN_COLUMN_ID + 1, cell)))
      {
        TBSYS_LOG(WARN, "failed to set cell");
      }
      else
      {
        row = &curr_row_;
      }
    }
    else if (row_index == 5)
    {
      cell.set_int(3);
      if (OB_SUCCESS != (ret = curr_row_.set_cell(table_id_, OB_APP_MIN_COLUMN_ID + 1, cell)))
      {
        TBSYS_LOG(WARN, "failed to set cell");
      }
      else
      {
        row = &curr_row_;
      }
    }
    else if (row_index == 6)
    {
      cell.set_int(7);
      if (OB_SUCCESS != (ret = curr_row_.set_cell(table_id_, OB_APP_MIN_COLUMN_ID + 1, cell)))
      {
        TBSYS_LOG(WARN, "failed to set cell");
      }
      else
      {
        row = &curr_row_;
      }
    }
    else
    {
      ret = OB_ITER_END;
    }
  }
  return ret;
}
// 1,1,3,4,4,5
int ObFakeTable2::generate_data2(const ObRow *& row, int row_index)
{
  int ret = OB_SUCCESS;
  if (row_index > row_count_)
  {
    ret = OB_ITER_END;
  }
  else
  {
    ObObj cell;
    if (row_index == 1 || row_index == 2)
    {
      cell.set_int(1);
      if (OB_SUCCESS != (ret = curr_row_.set_cell(table_id_, OB_APP_MIN_COLUMN_ID + 1, cell)))
      {
        TBSYS_LOG(WARN, "failed to set cell, table_id_=%lu", table_id_);
      }
      else
      {
        row = &curr_row_;
      }
    }
    else if (row_index == 3)
    {
      cell.set_int(3);
      if (OB_SUCCESS != (ret = curr_row_.set_cell(table_id_, OB_APP_MIN_COLUMN_ID + 1, cell)))
      {
        TBSYS_LOG(WARN, "failed to set cell, table_id_=%lu", table_id_);
      }
      else
      {
        row = &curr_row_;
      }
    }
    else if (row_index == 4 || row_index == 5)
    {
      cell.set_int(4);
      if (OB_SUCCESS != (ret = curr_row_.set_cell(table_id_, OB_APP_MIN_COLUMN_ID + 1, cell)))
      {
        TBSYS_LOG(WARN, "failed to set cell, table_id_=%lu", table_id_);
      }
      else
      {
        row = &curr_row_;
      }
    }
    else if (row_index == 6)
    {
      cell.set_int(5);
      if (OB_SUCCESS != (ret = curr_row_.set_cell(table_id_, OB_APP_MIN_COLUMN_ID + 1, cell)))
      {
        TBSYS_LOG(WARN, "failed to set cell, table_id_=%lu", table_id_);
      }
      else
      {
        row = &curr_row_;
      }
    }
    else
    {
      ret = OB_ITER_END;
    }
  }
  return ret;
}
ObFakeTable2::ObFakeTable2():column_count_(0), strategy_(NULL),cur_row_index_(1)
{
}
void ObFakeTable2::set_generate_data_strategy(generate_data_strategy strategy)
{
  strategy_ = strategy;
}
void ObFakeTable2::set_column_count(int column_count)
{
  column_count_ = column_count;
}
int ObFakeTable2::get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  if (strategy_ != NULL)
  {
    ret = (this->*(this->ObFakeTable2::strategy_))(row, cur_row_index_);
    if (OB_SUCCESS == ret)
    {
      cur_row_index_++;
    }
    else if (OB_ITER_END == ret)
    {
      TBSYS_LOG(DEBUG, "iter end");
    }
    else
    {
      TBSYS_LOG(DEBUG, "failed to get next row,ret=%d",ret);
    }
  }
  else
  {
    ret = OB_ERROR;
  }
  return ret;
}
int ObFakeTable2::open()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0;i < column_count_;++i)
  {
    if (OB_SUCCESS != (ret = row_desc_.add_column_desc(table_id_, OB_APP_MIN_COLUMN_ID + i + 1)))
    {
      TBSYS_LOG(WARN, "failed to add column desc, ret=%d", ret);
      break;
    }
  }
  curr_row_.set_row_desc(row_desc_);
  return ret;
}
TEST(ObExceptAll, test4)
{
  const uint64_t left_table_id = 1001;
  const uint64_t right_table_id = 2001;

  // empty
  ObFakeTable2 first_query_result;
  const char *name1 = "fake table1";
  first_query_result.set_name(name1);
  first_query_result.set_row_count(0);
  first_query_result.set_table_id(left_table_id);
  first_query_result.set_column_count(1);
  first_query_result.set_generate_data_strategy(&ObFakeTable2::generate_data1);


  // 1
  ObFakeTable2 second_query_result;
  const char *name2 = "fake table2";
  second_query_result.set_name(name2);
  second_query_result.set_row_count(1);
  second_query_result.set_table_id(right_table_id);
  second_query_result.set_column_count(1);
  second_query_result.set_generate_data_strategy(&ObFakeTable2::generate_data2);

  ObMergeExcept except_all;
  except_all.set_distinct(false);
  ASSERT_EQ(OB_SUCCESS, except_all.set_child(0, first_query_result));
  ASSERT_EQ(OB_SUCCESS, except_all.set_child(1, second_query_result));
  ASSERT_EQ(OB_SUCCESS, except_all.open());
  const ObRow *row = NULL;

  ASSERT_EQ(OB_ITER_END, except_all.get_next_row(row));
  ASSERT_EQ(OB_ITER_END, except_all.get_next_row(row));
  ASSERT_EQ(OB_ITER_END, except_all.get_next_row(row));

}
TEST(ObExceptAll, test3)
{
  const uint64_t left_table_id = 1001;
  const uint64_t right_table_id = 2001;

  // empty
  ObFakeTable2 first_query_result;
  const char *name1 = "fake table1";
  first_query_result.set_name(name1);
  first_query_result.set_row_count(0);
  first_query_result.set_table_id(left_table_id);
  first_query_result.set_column_count(1);
  first_query_result.set_generate_data_strategy(&ObFakeTable2::generate_data1);


  // empty
  ObFakeTable2 second_query_result;
  const char *name2 = "fake table2";
  second_query_result.set_name(name2);
  second_query_result.set_row_count(0);
  second_query_result.set_table_id(right_table_id);
  second_query_result.set_column_count(1);
  second_query_result.set_generate_data_strategy(&ObFakeTable2::generate_data2);

  ObMergeExcept except_all;
  except_all.set_distinct(false);
  ASSERT_EQ(OB_SUCCESS, except_all.set_child(0, first_query_result));
  ASSERT_EQ(OB_SUCCESS, except_all.set_child(1, second_query_result));
  ASSERT_EQ(OB_SUCCESS, except_all.open());
  const ObRow *row = NULL;
  ASSERT_EQ(OB_ITER_END, except_all.get_next_row(row));
  ASSERT_EQ(OB_ITER_END, except_all.get_next_row(row));
  ASSERT_EQ(OB_ITER_END, except_all.get_next_row(row));

}
TEST(ObExceptAll, test2)
{
  const uint64_t left_table_id = 1001;
  const uint64_t right_table_id = 2001;
  // 1,1,2,2,3,7
  ObFakeTable2 first_query_result;
  const char *name1 = "fake table1";
  first_query_result.set_name(name1);
  first_query_result.set_row_count(6);
  first_query_result.set_table_id(left_table_id);
  first_query_result.set_column_count(1);
  first_query_result.set_generate_data_strategy(&ObFakeTable2::generate_data1);


  // 1,1,3,4,4,5
  ObFakeTable2 second_query_result;
  const char *name2 = "fake table2";
  second_query_result.set_name(name2);
  second_query_result.set_row_count(6);
  second_query_result.set_table_id(right_table_id);
  second_query_result.set_column_count(1);
  second_query_result.set_generate_data_strategy(&ObFakeTable2::generate_data2);

  ObMergeExcept except_all;
  except_all.set_distinct(false);
  ASSERT_EQ(OB_SUCCESS, except_all.set_child(0, first_query_result));
  ASSERT_EQ(OB_SUCCESS, except_all.set_child(1, second_query_result));
  ASSERT_EQ(OB_SUCCESS, except_all.open());
  const ObRow *row = NULL;
  const ObObj *cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  int64_t int_value = -1;
  //2,2,7
  ASSERT_EQ(OB_SUCCESS, except_all.get_next_row(row));
  ASSERT_EQ(OB_SUCCESS, row->raw_get_cell(0, cell, table_id, column_id));
  ASSERT_EQ(OB_SUCCESS, cell->get_int(int_value));
  ASSERT_EQ(2, int_value);
  ASSERT_EQ(left_table_id, table_id);
  uint64_t c_id = static_cast<uint64_t> (OB_APP_MIN_COLUMN_ID + 1);
  ASSERT_EQ(c_id, column_id);

  ASSERT_EQ(OB_SUCCESS, except_all.get_next_row(row));
  ASSERT_EQ(OB_SUCCESS, row->raw_get_cell(0, cell, table_id, column_id));
  ASSERT_EQ(OB_SUCCESS, cell->get_int(int_value));
  ASSERT_EQ(2, int_value);
  ASSERT_EQ(left_table_id, table_id);
  ASSERT_EQ(c_id, column_id);

  ASSERT_EQ(OB_SUCCESS, except_all.get_next_row(row));
  ASSERT_EQ(OB_SUCCESS, row->raw_get_cell(0, cell, table_id, column_id));
  ASSERT_EQ(OB_SUCCESS, cell->get_int(int_value));
  ASSERT_EQ(7, int_value);
  ASSERT_EQ(left_table_id, table_id);
  ASSERT_EQ(c_id, column_id);

  ASSERT_EQ(OB_ITER_END, except_all.get_next_row(row));
  ASSERT_EQ(OB_ITER_END, except_all.get_next_row(row));

}
TEST(ObUnionDistinct, test3)
{
  const uint64_t left_table_id = 1001;
  const uint64_t right_table_id = 2001;
  // empty
  ObFakeTable2 first_query_result;
  const char *name1 = "fake table1";
  first_query_result.set_name(name1);
  first_query_result.set_row_count(0);
  first_query_result.set_table_id(left_table_id);
  first_query_result.set_column_count(1);
  first_query_result.set_generate_data_strategy(&ObFakeTable2::generate_data1);


  // 1
  ObFakeTable2 second_query_result;
  const char *name2 = "fake table2";
  second_query_result.set_name(name2);
  second_query_result.set_row_count(1);
  second_query_result.set_table_id(right_table_id);
  second_query_result.set_column_count(1);
  second_query_result.set_generate_data_strategy(&ObFakeTable2::generate_data2);

  ObMergeExcept except_distinct;
  except_distinct.set_distinct(true);
  ASSERT_EQ(OB_SUCCESS, except_distinct.set_child(0, first_query_result));
  ASSERT_EQ(OB_SUCCESS, except_distinct.set_child(1, second_query_result));
  ASSERT_EQ(OB_SUCCESS, except_distinct.open());
  const ObRow *row = NULL;

  ASSERT_EQ(OB_ITER_END,except_distinct.get_next_row(row));
  ASSERT_EQ(OB_ITER_END,except_distinct.get_next_row(row));
  ASSERT_EQ(OB_ITER_END,except_distinct.get_next_row(row));
}
TEST(ObUnionDistinct, test2)
{
  const uint64_t left_table_id = 1001;
  const uint64_t right_table_id = 2001;
  // 1
  ObFakeTable2 first_query_result;
  const char *name1 = "fake table1";
  first_query_result.set_name(name1);
  first_query_result.set_row_count(1);
  first_query_result.set_table_id(left_table_id);
  first_query_result.set_column_count(1);
  first_query_result.set_generate_data_strategy(&ObFakeTable2::generate_data1);


  // empty
  ObFakeTable2 second_query_result;
  const char *name2 = "fake table2";
  second_query_result.set_name(name2);
  second_query_result.set_row_count(0);
  second_query_result.set_table_id(right_table_id);
  second_query_result.set_column_count(1);
  second_query_result.set_generate_data_strategy(&ObFakeTable2::generate_data2);

  ObMergeExcept except_distinct;
  except_distinct.set_distinct(true);
  ASSERT_EQ(OB_SUCCESS, except_distinct.set_child(0, first_query_result));
  ASSERT_EQ(OB_SUCCESS, except_distinct.set_child(1, second_query_result));
  ASSERT_EQ(OB_SUCCESS, except_distinct.open());
  const ObRow *row = NULL;
  const ObObj *cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  int64_t int_value = -1;

  // 1
  ASSERT_EQ(OB_SUCCESS, except_distinct.get_next_row(row));
  ASSERT_EQ(OB_SUCCESS, row->raw_get_cell(0, cell, table_id, column_id));
  ASSERT_EQ(OB_SUCCESS, cell->get_int(int_value));
  ASSERT_EQ(1, int_value);
  ASSERT_EQ(left_table_id, table_id);
  uint64_t c_id = static_cast<uint64_t> (OB_APP_MIN_COLUMN_ID + 1);
  ASSERT_EQ(c_id, column_id);

  ASSERT_EQ(OB_ITER_END,except_distinct.get_next_row(row));
  ASSERT_EQ(OB_ITER_END,except_distinct.get_next_row(row));
  ASSERT_EQ(OB_ITER_END,except_distinct.get_next_row(row));
}
TEST(ObUnionDistinct, test1)
{
  const uint64_t left_table_id = 1001;
  const uint64_t right_table_id = 2001;
  // 1,1,2,2,3,7
  ObFakeTable2 first_query_result;
  const char *name1 = "fake table1";
  first_query_result.set_name(name1);
  first_query_result.set_row_count(6);
  first_query_result.set_table_id(left_table_id);
  first_query_result.set_column_count(1);
  first_query_result.set_generate_data_strategy(&ObFakeTable2::generate_data1);


  // 1,1,3,4,4,5
  ObFakeTable2 second_query_result;
  const char *name2 = "fake table2";
  second_query_result.set_name(name2);
  second_query_result.set_row_count(6);
  second_query_result.set_table_id(right_table_id);
  second_query_result.set_column_count(1);
  second_query_result.set_generate_data_strategy(&ObFakeTable2::generate_data2);

  ObMergeExcept except_distinct;
  except_distinct.set_distinct(true);
  ASSERT_EQ(OB_SUCCESS, except_distinct.set_child(0, first_query_result));
  ASSERT_EQ(OB_SUCCESS, except_distinct.set_child(1, second_query_result));
  ASSERT_EQ(OB_SUCCESS, except_distinct.open());
  const ObRow *row = NULL;
  const ObObj *cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  int64_t int_value = -1;

  // 2,7
  ASSERT_EQ(OB_SUCCESS, except_distinct.get_next_row(row));
  ASSERT_EQ(OB_SUCCESS, row->raw_get_cell(0, cell, table_id, column_id));
  ASSERT_EQ(OB_SUCCESS, cell->get_int(int_value));
  ASSERT_EQ(2, int_value);
  ASSERT_EQ(left_table_id, table_id);
  uint64_t c_id = static_cast<uint64_t> (OB_APP_MIN_COLUMN_ID + 1);
  ASSERT_EQ(c_id, column_id);

  ASSERT_EQ(OB_SUCCESS, except_distinct.get_next_row(row));
  ASSERT_EQ(OB_SUCCESS, row->raw_get_cell(0, cell, table_id, column_id));
  ASSERT_EQ(OB_SUCCESS, cell->get_int(int_value));
  ASSERT_EQ(7, int_value);
  ASSERT_EQ(left_table_id, table_id);
  ASSERT_EQ(c_id, column_id);

  ASSERT_EQ(OB_ITER_END, except_distinct.get_next_row(row));
  ASSERT_EQ(OB_ITER_END, except_distinct.get_next_row(row));
}
int main(int argc, char* argv[])
{
  TBSYS_LOGGER.setLogLevel("DEBUG");
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
