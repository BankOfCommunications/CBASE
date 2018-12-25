/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  getquerytest.h,  02/01/2013 12:30:17 PM xiaochu Exp $
 * 
 * Author:  
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:  
 *   get query test
 * 
 */

#include <pthread.h>
#include <stdint.h>

struct GetQueryParam
{
  int64_t write_idx_;
  int64_t expect_count_;
  int64_t batch_write_count_;
};

class ObGetQueryTest {
  public:
    ObGetQueryTest();
    ~ObGetQueryTest();
    int start();
    void stop();
    void loop();
  public:
    GetQueryParam param_;
  private:
    int init();
    void start_read_threads(int thread_num);
    void start_write_thread();
    int start_thread(void *(*routine)(void*), void *param);
  private:
    static void *read_thread_routine(void *);
    static void *write_thread_routine(void *);
  private:
    //SqlClient client_;
    const static int MAX_THREAD_COUNT = 20;
    int used_thread_cnt_;
    int read_thread_cnt_;
    pthread_t threads_[MAX_THREAD_COUNT];
    bool stop_;
};
