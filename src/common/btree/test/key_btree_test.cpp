#include <limits.h>
#include <test_key.h>
#include <key_btree.h>
#include <gtest/gtest.h>

/**
 * 测试key_btree
 */
namespace oceanbase
{
  namespace common
  {
#define TEST_COUNT 10000
    static int stop_ = 0;
    typedef KeyBtree<TestKey, int32_t> StringBtree;

    static inline int32_t get_next_id(int32_t &value)
    {
      value = (rand() & 0x7FFFFFFF);
      return 0;
    }

    TEST(KeyBtreeTest, get)
    {
      StringBtree btree(TEST_KEY_MAX_SIZE);
      {
        TestKey key;
        int32_t ret;
        int32_t success = 0;
        int32_t value = 0;

        EXPECT_EQ(0, btree.get_object_count());
        for(int32_t i = 0; i < TEST_COUNT; i++)
        {
          key.set_value(i);
          ret = btree.put(key, i + 1);
          if (ret == ERROR_CODE_OK)
          {
            success ++;
          }
        }
        EXPECT_EQ(success, btree.get_object_count());

        BtreeBaseHandle handle;
        key.set_value(0);
        ret = btree.get(handle, key, value);
        EXPECT_EQ(ret, ERROR_CODE_NOT_FOUND);
        btree.get_read_handle(handle);
        ret = btree.get(handle, key, value);
        EXPECT_EQ(ret, ERROR_CODE_OK);
        EXPECT_EQ(1, value);
        key.set_value(2);
        ret = btree.get(handle, key, value);
        EXPECT_EQ(ret, ERROR_CODE_OK);
        EXPECT_EQ(3, value);
      }
      btree.clear();
    }

    TEST(KeyBtreeTest, put)
    {
      StringBtree btree(TEST_KEY_MAX_SIZE);
      {
        TestKey key;
        int32_t ret, j;
        int32_t success = 0;
        int32_t value = 0;

        for(int32_t i = 0; i < TEST_COUNT; i++)
        {
          if (get_next_id(j)) break;
          key.set_value(j);
          ret = btree.put(key, i + 1);
          if (ret == ERROR_CODE_OK)
          {
            success ++;
          }
        }
        EXPECT_EQ(success, btree.get_object_count());
        ret = btree.put(key, 1);
        EXPECT_EQ(ret, ERROR_CODE_KEY_REPEAT);
        ret = btree.put(key, 20, true);
        EXPECT_EQ(ret, ERROR_CODE_OK);
        EXPECT_EQ(btree.get(key, value), ERROR_CODE_OK);
        EXPECT_TRUE(value == 20);
        EXPECT_EQ(success, btree.get_object_count());
      }
      btree.clear();
    }

    TEST(KeyBtreeTest, remove)
    {
      StringBtree btree(TEST_KEY_MAX_SIZE);
      {
        TestKey key;
        int32_t ret, j;
        int32_t *ids = (int32_t*)malloc(TEST_COUNT * sizeof(int32_t));
        int32_t success = 0;
        int value;

        for(int32_t i = 0; i < TEST_COUNT; i++)
        {
          if (get_next_id(j)) break;
          ids[i] = j;
          key.set_value(j);
          if (btree.put(key, i + 1) == ERROR_CODE_OK)
          {
            success ++;
          }
        }
        key.set_value(0x80000000);
        ASSERT_TRUE(btree.put(key, 0x8000001) == ERROR_CODE_OK);
        btree.remove(key, value);
        ASSERT_TRUE(value == 0x8000001);
        // remove
        for(int32_t i = 0; i < TEST_COUNT - 10; i++)
        {
          key.set_value(ids[i]);
          ret = btree.remove(key);
          if (ret == ERROR_CODE_OK)
          {
            success --;
          }
        }
        free(ids);
        EXPECT_EQ(success, btree.get_object_count());
      }
      btree.clear();
    }

    TEST(KeyBtreeTest, search)
    {
      StringBtree btree(TEST_KEY_MAX_SIZE);
      {
        TestKey key;
        int32_t j;
        int32_t *ids = (int32_t*)malloc(TEST_COUNT * sizeof(int32_t));

        for(int32_t i = 0; i < TEST_COUNT; i++)
        {
          if (get_next_id(j)) break;
          ids[i] = j;
          key.set_value(j);
          btree.put(key, j + 1);
        }

        // search
        int32_t value = 0;
        int32_t success = 0;
        for(int32_t i = 0; i < TEST_COUNT; i++)
        {
          key.set_value(ids[i]);
          btree.get(key, value);
          if (value == ids[i] + 1) success ++;
        }
        free(ids);
        EXPECT_EQ(success, TEST_COUNT);
      }
      btree.clear();
    }

    TEST(KeyBtreeTest, insert_batch)
    {
      StringBtree btree(TEST_KEY_MAX_SIZE);
      {
        BtreeWriteHandle handle;

        TestKey key;
        int32_t ret, j;
        int32_t success = 0;
        int32_t value = 0;

        ret = btree.get_write_handle(handle);
        ASSERT_TRUE(ERROR_CODE_OK == ret);
        for(int32_t i = 0; i < TEST_COUNT; i++)
        {
          if (get_next_id(j)) break;
          key.set_value(j);
          ret = btree.put(handle, key, i + 1);
          if (ret == ERROR_CODE_OK)
          {
            success ++;
          }
        }
        handle.end();
        EXPECT_EQ(success, btree.get_object_count());
        {
          BtreeWriteHandle handle1;
          btree.get_write_handle(handle1);
          for(int32_t i = 0; i < TEST_COUNT; i++)
          {
            if (get_next_id(j)) break;
            key.set_value(j);
            btree.put(handle1, key, i + 1);
          }
          key.set_value(1000);
          btree.put(handle1, key, 1000);
          btree.get(handle1, key, value);
          EXPECT_EQ(1000, value);
          handle1.rollback();
        }
        EXPECT_EQ(success, btree.get_object_count());
      }
      btree.clear();
    }

    TEST(KeyBtreeTest, range_search)
    {
      StringBtree btree(TEST_KEY_MAX_SIZE);
      {
        BtreeReadHandle handle;

        TestKey key;
        int32_t j;
        int32_t *ids = (int32_t*)malloc(TEST_COUNT * sizeof(int32_t));
        int insert_total = 0;

        for(int32_t i = 0; i < TEST_COUNT; i++)
        {
          if (get_next_id(j)) break;
          ids[i] = j;
          key.set_value(j);
          if (ERROR_CODE_OK == btree.put(key, i + 1))
          {
            insert_total ++;
          }
        }

        // range search
        int32_t value = 0;
        int32_t success = 0;
        int32_t ret = btree.get_read_handle(handle);
        ASSERT_TRUE(ERROR_CODE_OK == ret);
        btree.set_key_range(handle, btree.get_min_key(), 0, btree.get_max_key(), 0);
        while(ERROR_CODE_OK == btree.get_next(handle, value))
        {
          success ++;
        }
        EXPECT_EQ(success, insert_total);

        success = 0;
        BtreeReadHandle handle1;
        btree.get_read_handle(handle1);
        btree.set_key_range(handle1, btree.get_max_key(), 1, btree.get_min_key(), 1);
        if (ERROR_CODE_OK == btree.get_next(handle1, key, value))
        {
          success ++;
        }
        while(ERROR_CODE_OK == btree.get_next(handle1, value))
        {
          success ++;
        }
        EXPECT_EQ(success, insert_total);
        free(ids);
      }
      btree.clear();
    }

    void *mthread_read_1(void *args)
    {
      StringBtree *btree = reinterpret_cast<StringBtree*>(args);
      while(stop_ == 0)
      {
        TestKey key;
        int value;
        int success = 0;
        int ret = 0;
        for(int32_t i = 0; i < TEST_COUNT; i++)
        {
          key.set_value(i * 11);
          ret = btree->get(key, value);
          if (value == i + 1)
          {
            success ++;
          }
          value = 0;
        }
        EXPECT_EQ(success, TEST_COUNT);
      }
      return (void*) NULL;
    }
    void *mthread_read_2(void *args)
    {
      StringBtree *btree = reinterpret_cast<StringBtree*>(args);
      int32_t max_value = 1;
      while(max_value < 11 * TEST_COUNT)
        max_value *= 2;
      max_value --;
      while(stop_ == 0)
      {
        BtreeReadHandle handle;
        int32_t ret = btree->get_read_handle(handle);
        EXPECT_EQ(ERROR_CODE_OK, ret);

        TestKey fromkey, tokey;
        fromkey.set_value(0);
        tokey.set_value(max_value);

        // range search
        int32_t value = 0;
        int32_t success = 0;
        btree->set_key_range(handle, &fromkey, 0, &tokey, 0);
        while(ERROR_CODE_OK == btree->get_next(handle, value))
        {
          success ++;
        }
        EXPECT_GE(success, TEST_COUNT);
      }
      return (void*) NULL;
    }
    void *mthread_write(void *args)
    {
      StringBtree *btree = reinterpret_cast<StringBtree*>(args);
      TestKey key;
      int32_t j, ret;

      int success = 0;
      for(int32_t i = 0; i < TEST_COUNT * 3; i++)
      {
        j = (i * 11) + 1;
        key.set_value(j);
        ret = btree->put(key, i + 1);
        if (ret == ERROR_CODE_OK)
        {
          success ++;
        }
      }
      stop_ = 1;
      return (void*) NULL;
    }
    void *mthread_remove(void *args)
    {
      StringBtree *btree = reinterpret_cast<StringBtree*>(args);
      TestKey key;
      int32_t j, ret;

      int success = 0;
      for(int32_t i = 0; i < TEST_COUNT * 3; i++)
      {
        j = (i * 11) + 1;
        key.set_value(j);
        ret = btree->remove(key);
        if (ret == ERROR_CODE_OK)
        {
          success ++;
        }
      }
      return (void*) NULL;
    }

    TEST(KeyBtreeTest, read_write_mthread)
    {
      StringBtree btree(TEST_KEY_MAX_SIZE);
      btree.set_write_lock_enable(1);
      TestKey key;
      int32_t new_value = 0;
      int32_t success = 0;

      for(int32_t i = 0; i < TEST_COUNT; i++)
      {
        new_value = (i * 11);
        key.set_value(new_value);
        if (btree.put(key, i + 1) == ERROR_CODE_OK)
        {
          success ++;
        }
      }
      EXPECT_EQ(success, TEST_COUNT);

      int cnt = 0;
      int rtc1 = 1;
      int rtc2 = 1;
      int wtc = 1;
      int dtc = 1;
      pthread_t tids[rtc1+rtc2+wtc+dtc];

      stop_ = 0;
      while(cnt < rtc1)
      {
        pthread_create(&tids[cnt], NULL, mthread_read_1, &btree);
        cnt ++;
      }
      while(cnt < rtc1 + rtc2)
      {
        pthread_create(&tids[cnt], NULL, mthread_read_2, &btree);
        cnt ++;
      }
      while(cnt < rtc1 + rtc2 + wtc)
      {
        pthread_create(&tids[cnt], NULL, mthread_write, &btree);
        cnt ++;
      }
      while(cnt < rtc1 + rtc2 + wtc + dtc)
      {
        pthread_create(&tids[cnt], NULL, mthread_remove, &btree);
        cnt ++;
      }
      for(int i = 0; i < cnt; i++)
      {
        pthread_join(tids[i], NULL);
      }
      btree.clear();
    }

    TEST(KeyBtreeTest, small_key)
    {
      KeyBtree<char*, int32_t> btree(sizeof(char*));
      char *base = (char*)0x1000;
      int success = 0;
      for(int32_t i = 0; i < TEST_COUNT; i++)
      {
        if (btree.put(base + i, i + 1) == ERROR_CODE_OK)
          success ++;
      }
      ASSERT_TRUE(success == TEST_COUNT);
      int value = 0;
      btree.get(base, value);
      ASSERT_TRUE(value == 1);
      btree.clear();
    }

    TEST(KeyBtreeTest, range_search_int)
    {
      KeyBtree<int64_t, char*> btr(sizeof(int64_t));
      if (true)
      {
        BtreeWriteHandle handle;
        btr.get_write_handle(handle);
        btr.put(handle, 1, NULL);
        btr.put(handle, 2, NULL);
        btr.put(handle, 3, NULL);
        btr.put(handle, 4, NULL);
        handle.end();
      }

      int64_t start = 4;
      int64_t end = 99;
      int64_t key;
      char *value;
      {
        BtreeReadHandle handle;
        btr.get_read_handle(handle);
        btr.set_key_range(handle, &start, 0, &end, 1);
        key = 0;
        EXPECT_EQ(btr.get_next(handle, key, value), ERROR_CODE_OK);
        EXPECT_EQ(key, start);
        EXPECT_EQ(btr.get_next(handle, key, value), ERROR_CODE_NOT_FOUND);
      }
      {
        BtreeReadHandle handle;
        btr.get_read_handle(handle);
        btr.set_key_range(handle, &start, 0, &end, 0);
        key = 0;
        EXPECT_EQ(btr.get_next(handle, key, value), ERROR_CODE_OK);
        EXPECT_EQ(key, start);
        EXPECT_EQ(btr.get_next(handle, key, value), ERROR_CODE_NOT_FOUND);
      }
      btr.clear();
    }

    TEST(KeyBtreeTest, put_overwrite)
    {
      KeyBtree<KeyInfo, int> btr(sizeof(KeyInfo));
      KeyInfo k1(10, 1);
      KeyInfo k2(10, 2);
      KeyInfo key(1, 1);
      int value, old_value;
      EXPECT_EQ(btr.put(k1, 100), ERROR_CODE_OK);
      EXPECT_EQ(btr.put(k2, 101, true), ERROR_CODE_OK);
      {
        BtreeReadHandle handle;
        btr.get_read_handle(handle);
        btr.set_key_range(handle, btr.get_min_key(), 0, btr.get_max_key(), 0);
        EXPECT_EQ(btr.get_next(handle, key, value), ERROR_CODE_OK);
        EXPECT_EQ(value, 101);
        EXPECT_EQ(key.b_, 1);
      }
      // overwrite key
      EXPECT_EQ(btr.put(k2, 102, old_value, true, true), ERROR_CODE_OK);
      {
        BtreeReadHandle handle;
        btr.get_read_handle(handle);
        btr.set_key_range(handle, btr.get_min_key(), 0, btr.get_max_key(), 0);
        EXPECT_EQ(btr.get_next(handle, key, value), ERROR_CODE_OK);
        EXPECT_EQ(old_value, 101);
        EXPECT_EQ(value, 102);
        EXPECT_EQ(key.b_, 2);
      }
    }
  } // end namespace common
} // end namespace oceanbase
