/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * test_ob_ps_store.cpp is for what ...
 *
 * Version: ***: test_ob_ps_store.cpp  Mon Jul 29 13:24:09 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#include "common/ob_define.h"
#include "common/ob_string.h"
#include "sql/ob_ps_store.h"
#include "common/hash/ob_hashutils.h"
#include "thread.h"
#include "gtest/gtest.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace tbsys;

namespace oceanbase
{
  namespace tests
  {
    namespace sql
    {

      class TestObPsStore: public ::testing::Test
      {
      public:
        static const int32_t sqlnum = 40000;
      public:
        TestObPsStore()
        {

        }

        ~TestObPsStore()
        {

        }

        int init()
        {
          int ret = OB_SUCCESS;
          ret = ps_store_.init(hash::cal_next_prime(512));
          EXPECT_EQ(OB_SUCCESS, ret);
          ret = ObPsStoreItemGFactory::get_instance()->init();
          EXPECT_EQ(OB_SUCCESS, ret);
          map = &ps_store_.ps_store_;
          for (int i = 0; i<sqlnum; ++i)
          {
            sqlstrs[i] = new char[100];
            memset(sqlstrs[i], 0, 100);
            snprintf(sqlstrs[i], 100, "select * from test where c%d = ?", i);
          }
          return ret;
        }

        virtual void SetUp()
        {
          int ret = init();
          ASSERT_EQ(OB_SUCCESS, ret);
        }

        virtual void TearDown(){}

        void test_get();
        void test_mt_get();
        void test_get_plan();
        void test_mt_get_plan();
        void test_remove_plan();
        void test_mt_remove_plan();

        ObPsStore ps_store_;
        ObPsStore::SqlPlanMap *map;
        char *sqlstrs[sqlnum];
      };

      void TestObPsStore::test_get()
      {
        int ret = OB_SUCCESS;
        for (int i = 0; i<sqlnum; ++i)
        {
          ObString sql((ObString::obstr_size_t)strlen(sqlstrs[i]),(ObString::obstr_size_t)strlen(sqlstrs[i]), sqlstrs[i]);
          ASSERT_EQ(i, map->size());

          ObPsStoreItem *item = NULL;
          //new item when get first
          ret = ps_store_.get(sql, item);
          item->set_status(PS_ITEM_VALID);
          ASSERT_EQ(OB_SUCCESS, ret);
          ASSERT_TRUE(NULL != item);
          ASSERT_EQ(1, item->get_ps_count());
          ASSERT_EQ(i+1, map->size());

          //use old item
          ret = ps_store_.get(sql, item);
          ASSERT_EQ(OB_SUCCESS, ret);
          ASSERT_EQ(PS_ITEM_VALID, item->get_status());
          ASSERT_EQ(2, item->get_ps_count());
          ASSERT_EQ(i+1, map->size());
        }
      }

      TEST_F(TestObPsStore, get)
      {
        test_get();
      }

      void do_get(TestObPsStore *psstore)
      {
        int ret = OB_SUCCESS;
        ObPsStoreItem *item = NULL;
        for (int i = 0; i < TestObPsStore::sqlnum; ++i)
        {
          ObString sql((ObString::obstr_size_t)strlen(psstore->sqlstrs[i]),(ObString::obstr_size_t)strlen(psstore->sqlstrs[i]), psstore->sqlstrs[i]);
          ret = psstore->ps_store_.get(sql, item);
          EXPECT_EQ(OB_SUCCESS, ret);
          if (OB_SUCCESS == ret)
          {
            item->unlock();
            if (PS_ITEM_VALID == item->get_status())
            {
              EXPECT_TRUE(item->get_ps_count()>1);
            }
            else if (PS_ITEM_INVALID == item->get_status())
            {
              while(true && OB_SUCCESS == ret)
              {
                if (item->is_valid())
                {
                  ret = OB_SUCCESS;
                  break;
                }
                else
                {
                  if (0 == item->try_wlock())
                  {
                    item->set_status(PS_ITEM_VALID);
                    item->unlock();
                    break;
                  }
                  else
                  {
                    TBSYS_LOG(DEBUG, "item %p wlock failed", item);
                  }
                }
              }
            }
          }
          else
          {
            TBSYS_LOG(ERROR, "Get item failed ret=%d, sql is %.*s", ret, sql.length(), sql.ptr());
          }
        }
      }

      void do_get_plan(TestObPsStore *psstore)
      {
        int ret = OB_SUCCESS;
        ObPsStore &ps_store_ = psstore->ps_store_;
        ObSQLIdMgr *id_mgr = ps_store_.get_id_mgr();
        ObPsStoreItem *item = NULL;
        ObSQLIdMgr::SqlIdMap::iterator iter = id_mgr->get_map()->begin();
        int count = 0;
        for(; iter!=id_mgr->get_map()->end(); iter++)
        {
          count++;
          ret = ps_store_.get_plan(iter->second, item);
          EXPECT_EQ(OB_SUCCESS, ret);
          EXPECT_EQ(100, item->get_ps_count());
          EXPECT_EQ(PS_ITEM_VALID, item->get_status());
          EXPECT_TRUE(0!=item->try_wlock());
          item->unlock();
        }
        EXPECT_EQ(40000, count);
      }

      void do_remove_plan(TestObPsStore *psstore)
      {
        int ret = OB_SUCCESS;
        ObPsStore &ps_store_ = psstore->ps_store_;
        ObSQLIdMgr *id_mgr = ps_store_.get_id_mgr();
        ObPsStoreItem *item = NULL;
        ObSQLIdMgr::SqlIdMap::iterator iter = id_mgr->get_map()->begin();
        int count = 0;
        for(; iter!=id_mgr->get_map()->end(); iter++)
        {
          count++;
          ret = ps_store_.get_plan(iter->second, item);
          EXPECT_EQ(OB_SUCCESS, ret);
          EXPECT_TRUE(100>=item->get_ps_count());
          EXPECT_TRUE(0<=item->get_ps_count());
          EXPECT_EQ(PS_ITEM_VALID, item->get_status());
          EXPECT_TRUE(0!=item->try_wlock());
          item->unlock();
          ret = ps_store_.remove_plan(item->get_sql_id());
          EXPECT_EQ(OB_SUCCESS, ret);
        }
        EXPECT_EQ(40000, count);
      }

      void do_mix(TestObPsStore *psstore)
      {
        int ret = OB_ERROR;
        ObPsStoreItem *item = NULL;
        for (int i = 0; i < TestObPsStore::sqlnum; ++i)
        {
          ObString sql((ObString::obstr_size_t)strlen(psstore->sqlstrs[i]),(ObString::obstr_size_t)strlen(psstore->sqlstrs[i]), psstore->sqlstrs[i]);
          ret = psstore->ps_store_.get(sql, item);
          EXPECT_EQ(OB_SUCCESS, ret);
          if (OB_SUCCESS == ret)
          {
            item->unlock();
            if (PS_ITEM_VALID == item->get_status())
            {
              //EXPECT_TRUE(item->get_ps_count()>1);
            }
            else if (PS_ITEM_INVALID == item->get_status())
            {
              while(true && OB_SUCCESS == ret)
              {
                if (item->is_valid())
                {
                  ret = OB_SUCCESS;
                  break;
                }
                else
                {
                  if (0 == item->try_wlock())
                  {
                    item->set_status(PS_ITEM_VALID);
                    item->unlock();
                    break;
                  }
                  else
                  {
                    //TBSYS_LOG(ERROR, "item %p wlock failed", item);
                  }
                }
              }
            }

            EXPECT_TRUE(NULL != item);
            EXPECT_EQ(PS_ITEM_VALID, item->get_status());
            ////do execute
            uint64_t sqlid = item->get_sql_id();
            item = NULL;
            ret = psstore->ps_store_.get_plan((int64_t)sqlid, item);
            EXPECT_EQ(OB_SUCCESS, ret);
            item->unlock();
            //do close
            ret = psstore->ps_store_.remove_plan((int64_t)sqlid);
            EXPECT_EQ(OB_SUCCESS, ret);
          }
          else
          {
            TBSYS_LOG(ERROR, "Get item failed ret=%d, sql is %.*s", ret, sql.length(), sql.ptr());
          }
        }
      }

      class GetRunnable:public tbsys::CDefaultRunnable
      {
      public:
        void run(tbsys::CThread *thread, void *arg)
        {
          TestObPsStore *tstore = reinterpret_cast<TestObPsStore*>(arg);
          TBSYS_LOG(DEBUG, "start thread %p", thread);
          do_get(tstore);
        }
      };

      class GetPlanRunnable:public tbsys::CDefaultRunnable
      {
      public:
        void run(tbsys::CThread *thread, void *arg)
        {
          TestObPsStore *tstore = reinterpret_cast<TestObPsStore*>(arg);
          TBSYS_LOG(DEBUG, "start thread %p", thread);
          do_get_plan(tstore);
        }
      };

      class RemovePlanRunnable:public tbsys::CDefaultRunnable
      {
      public:
        void run(tbsys::CThread *thread, void *arg)
        {
          TestObPsStore *tstore = reinterpret_cast<TestObPsStore*>(arg);
          TBSYS_LOG(DEBUG, "start thread %p", thread);
          do_remove_plan(tstore);
        }
      };

      class MixRunnable:public tbsys::CDefaultRunnable
      {
      public:
        void run(tbsys::CThread *thread, void *arg)
        {
          TestObPsStore *tstore = reinterpret_cast<TestObPsStore*>(arg);
          TBSYS_LOG(DEBUG, "start thread %p", thread);
          do_mix(tstore);
        }
      };


      TEST_F(TestObPsStore, mt_get)
      {
        GetRunnable runner;
        CThread threads[100];
        for (int i = 0; i < 100; ++i)
        {
          threads[i].start(&runner, this);
        }
        ObPsStoreItem *item = NULL;
        int ret = OB_ERROR;
        for (int i = 0; i < 100; ++i)
        {
          threads[i].join();
        }
        ObSQLIdMgr *id_mgr = ps_store_.get_id_mgr();
        ObSQLIdMgr::SqlIdMap::iterator iter = id_mgr->get_map()->begin();
        for(; iter!=id_mgr->get_map()->end(); iter++)
        {
          ret = ps_store_.get_plan(iter->second, item);
          EXPECT_EQ(OB_SUCCESS, ret);
          EXPECT_EQ(100, item->get_ps_count());
          EXPECT_EQ(PS_ITEM_VALID, item->get_status());
          item->unlock();
        }
      }

      TEST_F(TestObPsStore, get_plan)
      {
        GetRunnable runner;
        GetPlanRunnable  gprunner;
        CThread threads[100];
        
        for (int i = 0; i < 100; ++i)
        {
          threads[i].start(&runner, this);
        }
        ObPsStoreItem *item = NULL;
        int ret = OB_ERROR;
        for (int i = 0; i < 100; ++i)
        {
          threads[i].join();
        }
        ObSQLIdMgr *id_mgr = ps_store_.get_id_mgr();
        ObSQLIdMgr::SqlIdMap::iterator iter = id_mgr->get_map()->begin();
        for(; iter!=id_mgr->get_map()->end(); iter++)
        {
          ret = ps_store_.get_plan(iter->second, item);
          EXPECT_EQ(OB_SUCCESS, ret);
          EXPECT_EQ(100, item->get_ps_count());
          EXPECT_EQ(PS_ITEM_VALID, item->get_status());
          item->unlock();
        }

        //get plan
        CThread gthreads[256];
        for (int i=0; i< 256; ++i)
        {
          gthreads[i].start(&gprunner, this);
        }
        for (int i = 0; i < 256; ++i)
        {
          gthreads[i].join();
        }

        iter = id_mgr->get_map()->begin();
        for(; iter!=id_mgr->get_map()->end(); iter++)
        {
          ret = ps_store_.get_plan(iter->second, item);
          EXPECT_EQ(OB_SUCCESS, ret);
          EXPECT_EQ(100, item->get_ps_count());
          EXPECT_EQ(PS_ITEM_VALID, item->get_status());
          item->unlock();
        }
      }

      TEST_F(TestObPsStore, remove_plan)
      {
        GetRunnable runner;
        RemovePlanRunnable  rprunner;
        CThread threads[100];
        for (int i = 0; i < 100; ++i)
        {
          threads[i].start(&runner, this);
        }
        ObPsStoreItem *item = NULL;
        int ret = OB_ERROR;
        for (int i = 0; i < 100; ++i)
        {
          threads[i].join();
        }
        ObSQLIdMgr *id_mgr = ps_store_.get_id_mgr();
        ObSQLIdMgr::SqlIdMap::iterator iter = id_mgr->get_map()->begin();
        for(; iter!=id_mgr->get_map()->end(); iter++)
        {
          ret = ps_store_.get_plan(iter->second, item);
          EXPECT_EQ(OB_SUCCESS, ret);
          EXPECT_EQ(100, item->get_ps_count());
          EXPECT_EQ(PS_ITEM_VALID, item->get_status());
          item->unlock();
        }

        //remove plan
        CThread rthreads[100];
        for (int i=0; i< 100; ++i)
        {
          rthreads[i].start(&rprunner, this);
        }
        for (int i = 0; i < 100; ++i)
        {
          rthreads[i].join();
        }
        EXPECT_EQ(40000, id_mgr->get_map()->size());
        EXPECT_EQ(0, ps_store_.count());
      }

      TEST_F(TestObPsStore, mix)
      {
        MixRunnable runner;
        CThread threads[256];
        for (int i = 0; i < 256; ++i)
        {
          threads[i].start(&runner, this);
        }
        for (int i = 0; i < 256; ++i)
        {
          threads[i].join();
        }

        EXPECT_EQ(0, ps_store_.count());
      }
    }
  }
}


void sigdump(int s)
{
  void *array[10];
  UNUSED(s);
  size_t size;
  char **strings;
  size_t i;

  size = backtrace (array, 10);
  strings = backtrace_symbols (array, (int)size);
  TBSYS_LOG(ERROR, "Obtained %zd stack frames\n", size);
  for (i = 0; i < size; i++)
    TBSYS_LOG(ERROR,"%s\n", strings[i]);
  free (strings);
  exit(0);
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  //signal(SIGSEGV, (__sighandler_t)sigdump);
  TBSYS_LOGGER.setLogLevel("ERROR");
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
