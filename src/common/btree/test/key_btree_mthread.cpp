#include <limits.h>
#include <test_key.h>
#include <key_btree.h>
#include <gtest/gtest.h>

/** * 多线程测试程序 */
namespace oceanbase
{
  namespace common
  {
    typedef  KeyBtree<CollectInfo, int32_t> CollectBtree;

#define CollectInfo_TEST_COUNT 500000
#define MAX_ID 1000000

    static int stop_ = 0;
    volatile int64_t remove_count = 0;
    volatile int64_t search_count = 0;
    volatile int64_t put_count = 0;
    volatile int64_t rsearch_count = 0;
    int64_t start_time = 0;

    void print_count(CollectBtree *btree)
    {
      int64_t end_time =  getTime();
      end_time -= start_time;
      end_time /= 1000000;
      if (end_time > 0)
      {
        fprintf(stderr, "%d sea: %ld(%ld), rsea: %ld(%ld), put: %ld(%ld), remove: %ld(%ld), object: %ld, %d\n",
                (int)time(NULL), search_count, search_count / end_time, rsearch_count, rsearch_count / end_time,
                put_count, put_count / end_time, remove_count, remove_count / end_time,
                btree->get_object_count(), btree->get_node_count());
      }
    }

    void *m_thread_read_1(void *args)
    {
      CollectBtree *btree = reinterpret_cast<CollectBtree*>(args);
      while(stop_ -- > 0)
      {
        CollectInfo key;
        int value;
        for(int32_t i = 0; i < MAX_ID; i++)
        {
          key.set_value(i);
          btree->get(key, value);
          if (i % (CollectInfo_TEST_COUNT * 5) == 0)
          {
            print_count(btree);
            if (atomic_add_return(CollectInfo_TEST_COUNT, &search_count) >= MAX_ID * 10)
            {
              stop_ = -1;
              break;
            }
          }
        }
      }
      return (void*) NULL;
    }

    void *m_thread_read_2(void *args)
    {
      CollectBtree *btree = reinterpret_cast<CollectBtree*>(args);
      while(stop_ == 0)
      {
        BtreeReadHandle handle;
        int32_t ret = btree->get_read_handle(handle);
        if (ERROR_CODE_OK != ret) continue;
        int start_index = rand() % MAX_ID;
        int end_index = rand() % MAX_ID;

        CollectInfo fromkey, tokey;
        fromkey.set_value(start_index);
        tokey.set_value(end_index);

        // range search
        int32_t value = 0;
        btree->set_key_range(handle, &fromkey, 0, &tokey, 0);
        while(ERROR_CODE_OK == btree->get_next(handle, value))
        {
        }
        atomic_add_return(1, &rsearch_count);
      }
      return (void*) NULL;
    }
    void *m_thread_write(void *args)
    {
      CollectBtree *btree = reinterpret_cast<CollectBtree*>(args);
      CollectInfo key;
      int32_t j;
      for(int32_t i = 0; i < MAX_ID; i++)
      {
        key.set_value(i);
        btree->put(key, i + 1);
        if (i % CollectInfo_TEST_COUNT == 0)
        {
          atomic_add_return(CollectInfo_TEST_COUNT, &put_count);
          print_count(btree);
        }
      }
      while(stop_ == 0)
      {
        for(int32_t i = 0; i < CollectInfo_TEST_COUNT; i++)
        {
          j = (rand() & MAX_ID);
          key.set_value(j);
          btree->put(key, i + 1);
        }
        atomic_add_return(CollectInfo_TEST_COUNT, &put_count);
        print_count(btree);
        stop_ = 1;
      }
      return (void*) NULL;
    }

    void *m_thread_remove(void *args)
    {
      CollectBtree *btree = reinterpret_cast<CollectBtree*>(args);
      CollectInfo key;
      int32_t j;
      while(stop_ == 0)
      {
        for(int32_t i = 0; i < CollectInfo_TEST_COUNT; i++)
        {
          j = (rand() & MAX_ID);
          key.set_value(j);
          btree->remove(key);
        }
        atomic_add_return(CollectInfo_TEST_COUNT, &remove_count);
      }
      return (void*) NULL;
    }

    TEST(DISABLED_KeyBtreeMThread, read_write_remove)
    {
      srand(time(NULL));
      CollectBtree btree(COLLECTINFO_KEY_MAX_SIZE);
      btree.set_write_lock_enable(1);

      int cnt = 0;
      int rtc1 = 4;
      int rtc2 = 0;
      int wtc = 1;
      int dtc = 1;
      pthread_t tids[rtc1+rtc2+wtc+dtc];

      start_time = getTime();
      //m_thread_write((void*)&btree);
      stop_ = 100;
      start_time = getTime();
      while(cnt < rtc1)
      {
        pthread_create(&tids[cnt], NULL, m_thread_read_1, &btree);
        cnt ++;
      }
      while(cnt < rtc1 + rtc2)
      {
        pthread_create(&tids[cnt], NULL, m_thread_read_2, &btree);
        cnt ++;
      }
      while(cnt < rtc1 + rtc2 + wtc)
      {
        pthread_create(&tids[cnt], NULL, m_thread_write, &btree);
        cnt ++;
      }
      while(cnt < rtc1 + rtc2 + wtc + dtc)
      {
        pthread_create(&tids[cnt], NULL, m_thread_remove, &btree);
        cnt ++;
      }
      for(int i = 0; i < cnt; i++)
      {
        pthread_join(tids[i], NULL);
      }
      print_count(&btree);
      fprintf(stderr, "normal exit, time: %ld\n", getTime() - start_time);
      fflush(stderr);
    }

  } // end namespace common
} // end namespace oceanbase
