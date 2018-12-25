/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  getquerytest.cpp,  02/01/2013 12:29:54 PM xiaochu Exp $
 * 
 * Author:  
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:  
 *   get query test
 * 
 */

#include "get_query_test.h"
#include <pthread.h>
#include "tbsys.h"
#include "ob_sql_db_creater.h"
#include "ob_sql_reader.h"
#include "ob_sql_writer.h"

ObGetQueryTest::ObGetQueryTest()
{
}

ObGetQueryTest::~ObGetQueryTest()
{
}

int ObGetQueryTest::init()
{
    //SqlClient client_;
    //GetQueryParams param_;
    read_thread_cnt_ = 10;
    used_thread_cnt_ = 0;
    memset(threads_, 0, sizeof(pthread_t) * MAX_THREAD_COUNT);
    stop_ = false;

    // init param
    param_.write_idx_ = 1;
    param_.batch_write_count_ = 100;
    param_.expect_count_ = 10000000;

    // init database
    ObSqlDbCreater creater;
    creater.open();
    creater.create();
    creater.close();

    return 0; 
}

int ObGetQueryTest::start()
{
  init();
  // note: share global g_row_count_;
  // start write thread
  start_write_thread();
  // start read thread
  start_read_threads(read_thread_cnt_);
  return 0;
}

void ObGetQueryTest::loop()
{
  while(!stop_)
  {
    sleep(1);
  }
  TBSYS_LOG(INFO, "get query test stopped. bye");
}

void ObGetQueryTest::stop()
{
  stop_ = true;
}

void ObGetQueryTest::start_read_threads(int thread_num)
{
  int ret;
  for (int i = 0; i < thread_num; i++)
  {
    ret = start_thread(read_thread_routine, this);
    if (0 != ret)
    {
      TBSYS_LOG(WARN, "fail to start write thread. ret=%d", ret);
    }
  }
}

void ObGetQueryTest::start_write_thread()
{
  int ret = start_thread(write_thread_routine, this);
  if (0 != ret)
  {
    TBSYS_LOG(WARN, "fail to start write thread. ret=%d", ret);
  }
}

int ObGetQueryTest::start_thread(void *(*routine)(void*), void *param)
{
  int ret = pthread_create(&threads_[used_thread_cnt_], NULL, routine, param);
  used_thread_cnt_++;
  if (0 != ret)
  {
    TBSYS_LOG(WARN, "fail to create thread");
  }
  return ret;
}

void *ObGetQueryTest::read_thread_routine(void *param)
{
  TBSYS_LOG(INFO, "read thread routine");
  ObGetQueryTest * query = reinterpret_cast<ObGetQueryTest *>(param);
  ObSqlReader reader;
  reader.open();
  while (true)
  {
    if ( query->param_.write_idx_ > 1)
    {
      const int64_t max_idx = query->param_.write_idx_;
      // random read [0, max_idx) row
      int ret = reader.read(max_idx);
      if (0 != ret)
      {
        TBSYS_LOG(WARN, "fail to read data. exit");
        query->stop();
        break;
      }
    }
  }
  reader.close();
  return NULL;
}

void *ObGetQueryTest::write_thread_routine(void *param)
{
  TBSYS_LOG(INFO, "write thread routine");
  ObGetQueryTest * query = reinterpret_cast<ObGetQueryTest *>(param);
  ObSqlWriter writer;
  writer.open();  
  int64_t anti_counter = 10; 
  int64_t start_idx = query->param_.write_idx_;
  while (start_idx < query->param_.expect_count_ )
  {
    const int64_t idx = writer.write(start_idx, query->param_.batch_write_count_);
    if (0 != idx)
    {
      TBSYS_LOG(WARN, "fail to write row[%ld]. exit", idx);
      query->stop();
      break;
    }
    query->param_.write_idx_ += query->param_.batch_write_count_;
    start_idx = query->param_.write_idx_;
    if (0 == anti_counter--)
    {
      TBSYS_LOG(INFO, "wrote row:%ld", query->param_.write_idx_);
      anti_counter = 10; //query->param_.batch_write_count_;
    }
  }
  writer.close();  
  TBSYS_LOG(INFO, "exit write thread");
  return NULL;
}
