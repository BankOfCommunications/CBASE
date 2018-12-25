/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: test_ring_buffer.cpp,v 0.1 2011/04/28 11:59:32 chuanhui Exp $
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
#include "ob_ring_buffer.h"
#include "ob_malloc.h"

using namespace std;
using namespace oceanbase::common;

static int __init_memory_pool__ = ob_init_memory_pool();

namespace oceanbase
{
namespace tests
{
namespace common
{

class TestRingBuffer : public ::testing::Test
{
public:
  virtual void SetUp()
  {

  }

  virtual void TearDown()
  {

  }
};

TEST_F(TestRingBuffer, test_push_pop)
{
  char task_buf[1024];
  const char* TASK_CONTENT = "This is task";
  strcpy(task_buf, TASK_CONTENT);
  void* ret_ptr = NULL;

  ObRingBuffer ring_buffer;
  ring_buffer.init();
  int err = ring_buffer.push_task(task_buf, strlen(task_buf) + 1, ret_ptr);
  EXPECT_EQ(0, err);
  EXPECT_TRUE(ret_ptr != NULL && 0 == strcmp((char*) ret_ptr, task_buf));
  err = ring_buffer.pop_task(ret_ptr);
  EXPECT_EQ(0, err);
}

TEST_F(TestRingBuffer, test_push_pop_large_task)
{
  int64_t BLOCK_SIZE = 4 * 1024L * 1024L - sizeof(RingTask);
  char *task_buf = new char[BLOCK_SIZE];
  int64_t task_len = BLOCK_SIZE;

  const char* TASK_CONTENT = "This is task";
  strcpy(task_buf, TASK_CONTENT);
  void* ret_ptr = NULL;

  ObRingBuffer ring_buffer;
  ring_buffer.init();
  int err = 0;

  for (int64_t i = 0; i < 10; ++i)
  {
    err = ring_buffer.push_task(task_buf, task_len, ret_ptr);
    EXPECT_EQ(0, err);
    EXPECT_TRUE(ret_ptr != NULL && 0 == strcmp((char*) ret_ptr, task_buf));
    err = ring_buffer.pop_task(ret_ptr);
    EXPECT_EQ(0, err);
    err = ring_buffer.pop_task(ret_ptr);
    EXPECT_NE(0, err);
  }

  delete [] task_buf;
}

TEST_F(TestRingBuffer, test_push_too_large_task)
{
  int64_t BLOCK_SIZE = 4 * 1024L * 1024L;
  char *task_buf = new char[BLOCK_SIZE];
  int64_t task_len = BLOCK_SIZE;

  ObRingBuffer ring_buffer;
  ring_buffer.init();
  int err = 0;
  void* ret_ptr = NULL;

  err = ring_buffer.push_task(task_buf, task_len, ret_ptr);
  EXPECT_NE(0, err);
  delete [] task_buf;
}

TEST_F(TestRingBuffer, test_pre_process_one_task)
{
  char task_buf[1024];
  const char* TASK_CONTENT = "This is task";
  strcpy(task_buf, TASK_CONTENT);
  void* ret_ptr = NULL;

  ObRingBuffer ring_buffer;
  ring_buffer.init();
  int err = ring_buffer.push_task(task_buf, strlen(task_buf) + 1, ret_ptr);
  EXPECT_EQ(0, err);
  EXPECT_TRUE(ret_ptr != NULL && 0 == strcmp((char*) ret_ptr, task_buf));

  char got_task_buf[1024];
  int64_t got_task_len = 0;
  err = ring_buffer.get_next_preprocess_task(got_task_buf, sizeof(got_task_buf), got_task_len);
  EXPECT_EQ(0, err);
  EXPECT_TRUE(got_task_len == (int64_t) (strlen(task_buf) + 1) && 0 == strcmp(got_task_buf, task_buf));

  err = ring_buffer.get_next_preprocess_task(got_task_buf, sizeof(got_task_buf), got_task_len);
  EXPECT_EQ(OB_NEED_RETRY, err);

  err = ring_buffer.pop_task(ret_ptr);
  EXPECT_EQ(0, err);
}

TEST_F(TestRingBuffer, test_pre_process_multi_tasks)
{
  const int64_t task_num = 10;
  int err = OB_SUCCESS;
  void* ret_ptr = NULL;

  char task_buf[task_num][100];
  const char* TASK_CONTENT = "This is task";
  for (int64_t i = 0; i < task_num; ++i)
  {
    sprintf(task_buf[i], "%s: %ld", TASK_CONTENT, i);
  }

  ObRingBuffer ring_buffer;
  ring_buffer.init();
  for (int64_t i = 0; i < task_num; ++i)
  {
    err = ring_buffer.push_task(task_buf[i], strlen(task_buf[i]) + 1, ret_ptr);
    EXPECT_EQ(0, err);
    EXPECT_TRUE(ret_ptr != NULL && 0 == strcmp((char*) ret_ptr, task_buf[i]));
  }

  char got_task_buf[1024];
  int64_t got_task_len = 0;
  for (int64_t i = 0; i < task_num; ++i)
  {
    err = ring_buffer.get_next_preprocess_task(got_task_buf, sizeof(got_task_buf), got_task_len);
    EXPECT_EQ(0, err);
    EXPECT_TRUE(got_task_len == (int64_t) (strlen(task_buf[i]) + 1) && 0 == strcmp(got_task_buf, task_buf[i]));
  }

  err = ring_buffer.get_next_preprocess_task(got_task_buf, sizeof(got_task_buf), got_task_len);
  EXPECT_EQ(OB_NEED_RETRY, err);

  // push a task
  err = ring_buffer.push_task(task_buf[0], strlen(task_buf[0]) + 1, ret_ptr);
  EXPECT_EQ(0, err);
  EXPECT_TRUE(ret_ptr != NULL && 0 == strcmp((char*) ret_ptr, task_buf[0]));

  // get next preprocess task
  err = ring_buffer.get_next_preprocess_task(got_task_buf, sizeof(got_task_buf), got_task_len);
  EXPECT_EQ(0, err);
  EXPECT_TRUE(got_task_len == (int64_t) (strlen(task_buf[0]) + 1) && 0 == strcmp(got_task_buf, task_buf[0]));

  err = ring_buffer.get_next_preprocess_task(got_task_buf, sizeof(got_task_buf), got_task_len);
  EXPECT_EQ(OB_NEED_RETRY, err);
}

TEST_F(TestRingBuffer, test_pre_process_with_fast_pop)
{
  const int64_t task_num = 10;
  int err = OB_SUCCESS;
  void* ret_ptr = NULL;

  char task_buf[task_num][100];
  void* pop_ptr[task_num];
  const char* TASK_CONTENT = "This is task";
  for (int64_t i = 0; i < task_num; ++i)
  {
    sprintf(task_buf[i], "%s: %ld", TASK_CONTENT, i);
  }

  ObRingBuffer ring_buffer;
  ring_buffer.init();
  for (int64_t i = 0; i < task_num; ++i)
  {
    err = ring_buffer.push_task(task_buf[i], strlen(task_buf[i]) + 1, ret_ptr);
    EXPECT_EQ(0, err);
    EXPECT_TRUE(ret_ptr != NULL && 0 == strcmp((char*) ret_ptr, task_buf[i]));
    pop_ptr[i] = ret_ptr;
  }

  for (int64_t i = 0; i < task_num / 2; ++i)
  {
    err = ring_buffer.pop_task(pop_ptr[i]);
    EXPECT_EQ(0, err);
  }

  char got_task_buf[1024];
  int64_t got_task_len = 0;
  for (int64_t i = task_num / 2; i < task_num; ++i)
  {
    err = ring_buffer.get_next_preprocess_task(got_task_buf, sizeof(got_task_buf), got_task_len);
    EXPECT_EQ(0, err);
    EXPECT_TRUE(got_task_len == (int64_t) (strlen(task_buf[i]) + 1) && 0 == strcmp(got_task_buf, task_buf[i]));
  }

  err = ring_buffer.get_next_preprocess_task(got_task_buf, sizeof(got_task_buf), got_task_len);
  EXPECT_EQ(OB_NEED_RETRY, err);

  // push a task
  err = ring_buffer.push_task(task_buf[0], strlen(task_buf[0]) + 1, ret_ptr);
  EXPECT_EQ(0, err);
  EXPECT_TRUE(ret_ptr != NULL && 0 == strcmp((char*) ret_ptr, task_buf[0]));

  // get next preprocess task
  err = ring_buffer.get_next_preprocess_task(got_task_buf, sizeof(got_task_buf), got_task_len);
  EXPECT_EQ(0, err);
  EXPECT_TRUE(got_task_len == (int64_t) (strlen(task_buf[0]) + 1) && 0 == strcmp(got_task_buf, task_buf[0]));

  err = ring_buffer.get_next_preprocess_task(got_task_buf, sizeof(got_task_buf), got_task_len);
  EXPECT_EQ(OB_NEED_RETRY, err);
}

TEST_F(TestRingBuffer, test_pre_process_large_task)
{
  const int64_t task_num = 10;
  int err = OB_SUCCESS;
  void* ret_ptr = NULL;

  char* task_buf[task_num];
  const int64_t BLOCK_SIZE = 4 * 1024L * 1024L - sizeof(RingTask);
  int64_t task_len = BLOCK_SIZE;
  const char* TASK_CONTENT = "This is task";
  for (int64_t i = 0; i < task_num; ++i)
  {
    task_buf[i] = new char[BLOCK_SIZE];
    sprintf(task_buf[i], "%s: %ld", TASK_CONTENT, i);
  }

  ObRingBuffer ring_buffer;
  ring_buffer.init();
  for (int64_t i = 0; i < task_num; ++i)
  {
    err = ring_buffer.push_task(task_buf[i], task_len, ret_ptr);
    EXPECT_EQ(0, err);
    EXPECT_TRUE(ret_ptr != NULL && 0 == strcmp((char*) ret_ptr, task_buf[i]));
  }

  char* got_task_buf = new char[BLOCK_SIZE];
  int64_t got_task_len = 0;
  for (int64_t i = 0; i < task_num; ++i)
  {
    err = ring_buffer.get_next_preprocess_task(got_task_buf, BLOCK_SIZE, got_task_len);
    EXPECT_EQ(0, err);
    EXPECT_TRUE(got_task_len == task_len && 0 == strcmp(got_task_buf, task_buf[i]));
  }

  err = ring_buffer.get_next_preprocess_task(got_task_buf, BLOCK_SIZE, got_task_len);
  EXPECT_EQ(OB_NEED_RETRY, err);

  // push a task
  err = ring_buffer.push_task(task_buf[0], task_len, ret_ptr);
  EXPECT_EQ(0, err);
  EXPECT_TRUE(ret_ptr != NULL && 0 == strcmp((char*) ret_ptr, task_buf[0]));

  // get next preprocess task
  err = ring_buffer.get_next_preprocess_task(got_task_buf, BLOCK_SIZE, got_task_len);
  EXPECT_EQ(0, err);
  EXPECT_TRUE(got_task_len == task_len && 0 == strcmp(got_task_buf, task_buf[0]));

  err = ring_buffer.get_next_preprocess_task(got_task_buf, BLOCK_SIZE, got_task_len);
  EXPECT_EQ(OB_NEED_RETRY, err);

  for (int64_t i = 0; i < task_num; ++i)
  {
    delete[] task_buf[i];
  }

  delete[] got_task_buf;
}

} // end namespace common
} // end namespace tests
} // end namespace oceanbase


int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

