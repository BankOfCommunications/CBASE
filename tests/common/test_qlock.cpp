/**
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_ob_log_cache.cpp
 *
 * Authors:
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *
 */

#include "gtest/gtest.h"
#include "common/ob_malloc.h"
#include "updateserver/ob_log_buffer.h"
#include "common/thread_buffer.h"
#include "qlock.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;

namespace oceanbase
{
  namespace test
  {
    struct Config
    {
      static int64_t MAX_N_DATA_ITEMS;
      int64_t max_num_items;
      int64_t n_thread;
      Config()
      {
        max_num_items = MAX_N_DATA_ITEMS;
        n_thread = 2;
      }
    };
    int64_t Config::MAX_N_DATA_ITEMS = 1<<20;

    struct Node
    {
      Node(): lock_(), x(0), y(0), n_call_(0), n_check_(0), n_set_(0), n_set2_(0), n_err_(0), n_dead_lock_(0) {}
      ~Node() {}
      static const bool use_lock_ = true;
      QLock lock_;
      int64_t x;
      int64_t y;
      int64_t n_call_;
      int64_t n_check_;
      int64_t n_set_;
      int64_t n_set2_;
      int64_t n_err_;
      int64_t n_dead_lock_;
      int check(int32_t uid){
        int err = OB_SUCCESS;
        int unlock_err = OB_EAGAIN;
        bool is_locked = true;
        __sync_fetch_and_add(&n_call_, 1);
        if (use_lock_ && OB_SUCCESS != (err = lock_.try_shared_lock(uid)))
        {
          is_locked = false;
        }
        else if (x != y)
        {
          err = OB_INVALID_DATA;
          TBSYS_LOG(ERROR, "check(uid=%d): ERROR", uid);
        }
        if (OB_SUCCESS == err)
        {
          __sync_fetch_and_add(&n_check_, 1);
        }
        else if (OB_EAGAIN != err)
        {
          __sync_fetch_and_add(&n_err_, 1);
        }
        while(use_lock_ && is_locked && OB_EAGAIN == (unlock_err = lock_.try_shared_unlock(uid)))
          ;
        return (OB_SUCCESS == err && is_locked) ? unlock_err: err;
      }
      int set(int32_t uid, int64_t id){
        int err = OB_SUCCESS;
        int unlock_err = OB_EAGAIN;
        bool is_locked = true;
        __sync_fetch_and_add(&n_call_, 1);
        if (use_lock_ && OB_SUCCESS != (err = lock_.try_exclusive_lock(uid)))
        {
          is_locked = false;
          //TBSYS_LOG(ERROR, "lock.try_exclusive_lock(uid=%ld)=>%d", uid, err);
        }
        else if (use_lock_ && OB_SUCCESS != (err = lock_.wait_shared_lock_release(uid)))
        {
          TBSYS_LOG(ERROR, "lock.wait_shared_lock_release(uid=%d)=>%d", uid, err);
        }
        else if (x != y)
        {
          err = OB_INVALID_DATA;
          TBSYS_LOG(ERROR, "check(uid=%d): ERROR", uid);
        }
        else
        {
          x = y = id;
        }
        if (OB_SUCCESS == err)
        {
          __sync_fetch_and_add(&n_set_, 1);
        }
        else if (OB_EAGAIN != err)
        {
          __sync_fetch_and_add(&n_err_, 1);
        }
        while(use_lock_&& is_locked && OB_EAGAIN == (unlock_err = lock_.try_exclusive_unlock(uid)))
          ;
        return (OB_SUCCESS == err && is_locked) ? unlock_err: err;
      }
      int set2(int32_t uid, int64_t id){
        int err = OB_SUCCESS;
        int unlock_err = OB_EAGAIN;
        bool is_locked_shared = true;
        bool is_locked_exclusive = true;
        __sync_fetch_and_add(&n_call_, 1);
        if (use_lock_ && OB_SUCCESS != (err = lock_.try_shared_lock(uid)))
        {
          is_locked_shared = false;
          is_locked_exclusive = false;
        }
        else if (use_lock_ && OB_SUCCESS != (err = lock_.share2exclusive_lock(uid)))
        {
          is_locked_exclusive = false;
        }
        else if (use_lock_ && OB_SUCCESS != (err = lock_.wait_shared_lock_release(uid)))
        {}
        else
        {
          x = y = id;
        }
        if (OB_DEAD_LOCK == err)
        {
          err = OB_EAGAIN;
          __sync_fetch_and_add(&n_dead_lock_, 1);
        }
        if (OB_SUCCESS == err)
        {
          __sync_fetch_and_add(&n_set2_, 1);
        }
        else if (OB_EAGAIN != err)
        {
          __sync_fetch_and_add(&n_err_, 1);
        }
        while(use_lock_ && is_locked_exclusive && OB_EAGAIN == (unlock_err = lock_.try_exclusive_unlock(uid)))
          ;
        while(use_lock_ && is_locked_shared && !is_locked_exclusive && OB_EAGAIN == (unlock_err = lock_.try_shared_unlock(uid)))
          ;
        return (OB_SUCCESS == err) ? unlock_err: err;
      }
      void report(int64_t uid)
      {
        TBSYS_LOG(INFO, "uid[%ld]: check=%ld, set=%ld, set2=%ld, total=%ld, n_err=%ld, n_dead_lock=%ld", uid, n_check_, n_set_, n_set2_, n_call_, n_err_, n_dead_lock_);
      }
    };
    class ObQLockTest: public ::testing::Test, public tbsys::CDefaultRunnable, public Config {
      public:
        ObQLockTest() {}
        ~ObQLockTest() {}
      protected:
        int err;
        Node node;
      protected:
        virtual void SetUp(){
          srandom(static_cast<int32_t>(time(NULL)));
          err = OB_SUCCESS;
        }
        virtual void TearDown(){
          inspect(true);
        }
        void inspect(bool verbose=false){
          UNUSED(verbose);
          fprintf(stderr, "ObQLockTest{n_thread=%ld max_num_items=%ld}\n",
                  n_thread, max_num_items);
        }

        int do_work(tbsys::CThread *thread, int64_t idx){
          int err = OB_SUCCESS;
          int64_t start_id = 1;
          int32_t uid = (int32_t)idx;
          UNUSED(thread);
          for (start_id = 0; !_stop && OB_SUCCESS == err && start_id < max_num_items; start_id++)
          {
            switch(random()%3)
            {
              case 0:
                err = node.check(uid);
                break;
              case 1:
                err = node.set(uid, start_id);
                break;
              case 2:
                err = node.set2(uid, start_id);
                break;
              default:
                err = OB_ERR_UNEXPECTED;
            }
            if (OB_EAGAIN == err)
            {
              err = OB_SUCCESS;
            }
          }
          if (OB_SUCCESS != err)
          {
            stop();
          }
          node.report(idx);
          return err;
        }

        void run(tbsys::CThread *thread, void* arg){
          ASSERT_EQ(OB_SUCCESS, do_work(thread, (int64_t)arg + 1));
        }
    };
    TEST_F(ObQLockTest, Inspect){
      inspect(true);
    }

    TEST_F(ObQLockTest, Shared){
      QLock lock;
      ASSERT_EQ(OB_SUCCESS, lock.try_shared_lock(1));
      ASSERT_EQ(OB_SUCCESS, lock.try_exclusive_lock(2));
      ASSERT_EQ(OB_EAGAIN, lock.try_wait_shared_lock_release(2));
      ASSERT_EQ(OB_SUCCESS, lock.try_exclusive_unlock(2));
      ASSERT_EQ(OB_SUCCESS, lock.try_shared_lock(3));
      ASSERT_EQ(OB_SUCCESS, lock.try_shared_unlock(1));
      ASSERT_EQ(OB_SUCCESS, lock.try_shared_unlock(3));
      ASSERT_EQ(OB_SUCCESS, lock.try_exclusive_lock(2));
    }

    TEST_F(ObQLockTest, Exclusive){
      QLock lock;
      ASSERT_EQ(OB_SUCCESS, lock.try_exclusive_lock(1));
      ASSERT_EQ(OB_EAGAIN, lock.try_exclusive_lock(2));
      ASSERT_EQ(OB_EAGAIN, lock.try_shared_lock(3));
      ASSERT_EQ(OB_LOCK_NOT_MATCH, lock.try_exclusive_unlock(2));
      ASSERT_EQ(OB_SUCCESS, lock.try_exclusive_unlock(1));
      ASSERT_EQ(OB_SUCCESS, lock.try_shared_lock(3));
    }

    TEST_F(ObQLockTest, CoCurrent){
      setThreadCount((int)n_thread);
      start();
      wait();
      ASSERT_EQ(0, node.n_err_);
    }
  } // end namespace updateserver
} // end namespace oceanbase

using namespace oceanbase::test;

int main(int argc, char** argv)
{
  int err = OB_SUCCESS;
  int n_data_items_shift = 0;
  TBSYS_LOGGER.setLogLevel("INFO");
  if (OB_SUCCESS != (err = ob_init_memory_pool()))
    return err;
  ::testing::InitGoogleTest(&argc, argv);
  if(argc >= 2)
  {
    TBSYS_LOGGER.setLogLevel(argv[1]);
  }
  if(argc >= 3)
  {
    n_data_items_shift = atoi(argv[2]);
    if(n_data_items_shift > 4)
      Config::MAX_N_DATA_ITEMS = 1 << n_data_items_shift;
  }
  return RUN_ALL_TESTS();
}
