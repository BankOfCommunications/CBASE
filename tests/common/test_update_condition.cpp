/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: test_update_condition.cpp,v 0.1 2011/04/02 10:43:01 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include <iostream>
#include <sstream>
#include <algorithm>
#include <gtest/gtest.h>
#include "tblog.h"
#include "test_helper.h"
#include "ob_update_condition.h"

using namespace std;
using namespace oceanbase::common;

int init_mem_pool()
{
  ob_init_memory_pool(2 * 1024L * 1024L);
  return 0;
}
static int init_mem_pool_ret = init_mem_pool();

namespace oceanbase
{
namespace tests
{
namespace common
{

class TestUpdateCondition : public ::testing::Test
{
public:
  virtual void SetUp()
  {

  }

  virtual void TearDown()
  {

  }
};

TEST_F(TestUpdateCondition, test_empty_update_condition)
{
  ObUpdateCondition update_condition;
  int64_t count = update_condition.get_count();
  EXPECT_EQ(0, count);
  char buf[1024];
  int64_t pos = 0;
  int err = update_condition.serialize(buf, sizeof(buf), pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(0, pos);

  ObUpdateCondition new_condition;
  int64_t new_pos = 0;
  err = new_condition.deserialize(buf, pos, new_pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(0, new_condition.get_count());
  EXPECT_EQ(0, new_pos);
}

TEST_F(TestUpdateCondition, test_single_cell_condition)
{
  ObUpdateCondition update_condition;
  char table_name_str[128];
  strcpy(table_name_str, "table_name");
  char row_key_str[128];
  strcpy(row_key_str, "row_key");
  char column_name_str[128];
  strcpy(column_name_str, "column_name");
  ObCondInfo cond_info;
  cond_info.table_name_.assign(table_name_str, strlen(table_name_str));
  cond_info.row_key_.assign(row_key_str, strlen(row_key_str));
  cond_info.column_name_.assign(column_name_str, strlen(column_name_str));
  cond_info.op_type_ = EQ;
  cond_info.value_.set_int(100);

  int err = update_condition.add_cond(cond_info.table_name_, cond_info.row_key_,
      cond_info.column_name_, cond_info.op_type_, cond_info.value_);
  EXPECT_EQ(0, err);
  EXPECT_EQ(1, update_condition.get_count());
  check_cond_info(cond_info, *update_condition[0]);

  char buf[64 * 1024];
  int64_t pos = 0;
  err = update_condition.serialize(buf, sizeof(buf), pos);
  EXPECT_EQ(0, err);

  ObUpdateCondition new_condition;
  int64_t new_pos = 0;
  err = new_condition.deserialize(buf, pos, new_pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(pos, new_pos);
  EXPECT_EQ(1, new_condition.get_count());
  check_cond_info(cond_info, *new_condition[0]);

  // deserialize again
  new_pos = 0;
  err = new_condition.deserialize(buf, pos, new_pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(pos, new_pos);
  EXPECT_EQ(1, new_condition.get_count());
  check_cond_info(cond_info, *new_condition[0]);
}

TEST_F(TestUpdateCondition, test_single_row_exist_condition)
{
  ObUpdateCondition update_condition;
  char table_name_str[128];
  strcpy(table_name_str, "table_name");
  char row_key_str[128];
  strcpy(row_key_str, "row_key");
  char column_name_str[128];
  strcpy(column_name_str, "column_name");
  ObCondInfo cond_info;
  cond_info.table_name_.assign(table_name_str, strlen(table_name_str));
  cond_info.row_key_.assign(row_key_str, strlen(row_key_str));
  cond_info.column_name_.assign(column_name_str, strlen(column_name_str));
  cond_info.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);

  int err = update_condition.add_cond(cond_info.table_name_, cond_info.row_key_, false);
  EXPECT_EQ(0, err);
  EXPECT_EQ(1, update_condition.get_count());
  check_exist_cond_info(cond_info, *update_condition[0]);

  char buf[64 * 1024];
  int64_t pos = 0;
  err = update_condition.serialize(buf, sizeof(buf), pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(pos, update_condition.get_serialize_size());

  ObUpdateCondition new_condition;
  int64_t new_pos = 0;
  err = new_condition.deserialize(buf, pos, new_pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(pos, new_pos);
  EXPECT_EQ(1, new_condition.get_count());
  check_exist_cond_info(cond_info, *new_condition[0]);
  EXPECT_EQ(pos, new_condition.get_serialize_size());

  // deserialize again
  new_pos = 0;
  err = new_condition.deserialize(buf, pos, new_pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(pos, new_pos);
  EXPECT_EQ(1, new_condition.get_count());
  check_exist_cond_info(cond_info, *new_condition[0]);
  EXPECT_EQ(pos, new_condition.get_serialize_size());
}

TEST_F(TestUpdateCondition, test_multiple_conditions)
{
  static const int64_t COND_NUM = 100;
  ObCondInfo cond_info[COND_NUM];
  char row_key_str[COND_NUM][64];
  char column_name_str[COND_NUM][64];
  char table_name_str[COND_NUM][64];
  int exist_arr[COND_NUM];

  for (int64_t i = 0; i < COND_NUM; ++i)
  {
    sprintf(row_key_str[i], "row_key%ld", i);
    sprintf(table_name_str[i], "table_name%ld", i);
    sprintf(column_name_str[i], "column_name%ld", i);

    exist_arr[i] = rand() % 3;
    if (0 == exist_arr[i])
    {
      cond_info[i].table_name_.assign(table_name_str[i], strlen(table_name_str[i]));
      cond_info[i].row_key_.assign(row_key_str[i], strlen(row_key_str[i]));
      cond_info[i].column_name_.assign(column_name_str[i], strlen(column_name_str[i]));
      cond_info[i].op_type_ = EQ;
      cond_info[i].value_.set_int(rand());
    }
    else if (1 == exist_arr[i])
    {
      cond_info[i].table_name_.assign(table_name_str[i], strlen(table_name_str[i]));
      cond_info[i].row_key_.assign(row_key_str[i], strlen(row_key_str[i]));
      cond_info[i].value_.set_ext(ObActionFlag::OP_ROW_EXIST);
    }
    else
    {
      cond_info[i].table_name_.assign(table_name_str[i], strlen(table_name_str[i]));
      cond_info[i].row_key_.assign(row_key_str[i], strlen(row_key_str[i]));
      cond_info[i].value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
    }
  }

  int err = 0;
  ObUpdateCondition update_condition;
  for (int64_t i = 0; i < COND_NUM; ++i)
  {
    if (0 == exist_arr[i])
    {
      err = update_condition.add_cond(cond_info[i].table_name_, cond_info[i].row_key_,
          cond_info[i].column_name_, cond_info[i].op_type_, cond_info[i].value_);
    }
    else if (1 == exist_arr[i])
    {
      err = update_condition.add_cond(cond_info[i].table_name_, cond_info[i].row_key_, true);
    }
    else
    {
      err = update_condition.add_cond(cond_info[i].table_name_, cond_info[i].row_key_, false);
    }

    EXPECT_EQ(0, err);
  }

  EXPECT_EQ(COND_NUM, update_condition.get_count());
  for (int64_t i = 0; i < COND_NUM; ++i)
  {
    if (0 == exist_arr[i])
    {
      check_cond_info(cond_info[i], *update_condition[i]);
    }
    else
    {
      check_exist_cond_info(cond_info[i], *update_condition[i]);
    }
  }

  char* buf = new char[1024 * 1024];
  int64_t pos = 0;
  err = update_condition.serialize(buf, 1024 * 1024, pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(pos, update_condition.get_serialize_size());

  ObUpdateCondition new_condition;
  int64_t new_pos = 0;
  err = new_condition.deserialize(buf, pos, new_pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(pos, new_pos);
  EXPECT_EQ(pos, new_condition.get_serialize_size());

  EXPECT_EQ(COND_NUM, new_condition.get_count());
  for (int64_t i = 0; i < COND_NUM; ++i)
  {
    if (0 == exist_arr[i])
    {
      check_cond_info(cond_info[i], *new_condition[i]);
    }
    else
    {
      check_exist_cond_info(cond_info[i], *new_condition[i]);
    }
  }

  delete[] buf;
}

} // end namespace common
} // end namespace tests
} // end namespace oceanbase


int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

