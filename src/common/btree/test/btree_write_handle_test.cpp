#include <limits.h>
#include <btree_write_handle.h>
#include <btree_base.h>
#include <gtest/gtest.h>
#include <test_key.h>
#include <key_btree.h>

/**
 * 测试BtreeReadHandle
 */
namespace oceanbase
{
  namespace common
  {
    typedef KeyBtree<TestKey, int32_t> StringBtree;
    TEST(BtreeWriteHandleTest, put)
    {
      StringBtree btree(TEST_KEY_MAX_SIZE);
      BtreeWriteHandle handle;
      TestKey key;
      int32_t ret;
      key.set_value(100);

      btree.get_write_handle(handle);
      ret = btree.put(handle, key, 100);
      EXPECT_EQ(0, btree.get_object_count());
      EXPECT_EQ(1, handle.get_object_count());

      // 第二代writehandle
      BtreeWriteHandle *handle2 = handle.get_write_handle();
      ASSERT_TRUE(NULL != handle2);
      key.set_value(101);
      ret = btree.put(*handle2, key, 101);
      EXPECT_EQ(1, handle.get_object_count());
      EXPECT_EQ(2, handle2->get_object_count());
      BtreeWriteHandle *handle3 = handle.get_write_handle();
      EXPECT_TRUE(NULL == handle3);
      handle2->end();
      handle3 = handle.get_write_handle();
      ASSERT_TRUE(NULL != handle3);
      key.set_value(102);
      ret = btree.put(handle, key, 102);
      EXPECT_EQ(ret, ERROR_CODE_FAIL);
      ret = btree.put(*handle3, key, 102);
      EXPECT_EQ(ret, ERROR_CODE_OK);
      EXPECT_EQ(2, handle.get_object_count());
      EXPECT_EQ(3, handle3->get_object_count());
      handle3->rollback();
      handle3->end();
      EXPECT_EQ(2, handle.get_object_count());
      btree.clear();
    }

    TEST(BtreeWriteHandleTest, rollback)
    {
      StringBtree btree(TEST_KEY_MAX_SIZE);
      TestKey key;
      int32_t ret;
      key.set_value(100);
      int32_t success_cnt = 0;

      // 1
      {
        BtreeWriteHandle handle;
        btree.get_write_handle(handle);
        ret = btree.put(handle, key, 100);
        EXPECT_EQ(ret, ERROR_CODE_OK);
        handle.rollback();
      }
      EXPECT_EQ(0, btree.get_object_count());
      EXPECT_EQ(0, btree.get_alloc_count());

      // 2
      {
        BtreeWriteHandle handle;
        btree.get_write_handle(handle);
        ret = btree.put(handle, key, 100);
        EXPECT_EQ(ret, ERROR_CODE_OK);
        BtreeWriteHandle *h1 = handle.get_write_handle();
        key.set_value(101);
        ret = btree.put(*h1, key, 100);
        EXPECT_EQ(ret, ERROR_CODE_OK);
        handle.rollback();
      }
      EXPECT_EQ(0, btree.get_object_count());
      EXPECT_EQ(0, btree.get_alloc_count());

      // 3
      {
        BtreeWriteHandle handle;
        btree.get_write_handle(handle);
        key.set_value(100);
        ret = btree.put(handle, key, 100);
        EXPECT_EQ(ret, ERROR_CODE_OK);
        BtreeWriteHandle *h1 = handle.get_write_handle();
        key.set_value(101);
        ret = btree.put(*h1, key, 100);
        EXPECT_EQ(ret, ERROR_CODE_OK);
        h1->rollback();
        success_cnt ++;
      }
      EXPECT_EQ(success_cnt, btree.get_object_count());
      EXPECT_EQ(2, btree.get_alloc_count());

      // 4
      {
        BtreeWriteHandle handle;
        btree.get_write_handle(handle);
        key.set_value(400);
        ret = btree.put(handle, key, 400);
        EXPECT_EQ(ret, ERROR_CODE_OK);
        BtreeWriteHandle *h1 = handle.get_write_handle();
        key.set_value(401);
        ret = btree.put(*h1, key, 400);
        EXPECT_EQ(ret, ERROR_CODE_OK);
        success_cnt += 2;
      }
      EXPECT_EQ(success_cnt, btree.get_object_count());
      EXPECT_EQ(3, btree.get_alloc_count());
      btree.clear();
    }
    TEST(BtreeWriteHandleTest, two_get_write_handle)
    {
      StringBtree btree(TEST_KEY_MAX_SIZE);
      TestKey key;
      int32_t ret;
      key.set_value(100);
      int32_t i = 0;
      int32_t value = 0;
      int32_t cnt = 1000;
      int32_t rcnt = 0;
      int32_t base_value = 10000;
      int32_t skip = 10;
      {
        BtreeWriteHandle handle;
        btree.get_write_handle(handle);
        for(i = 0; i < cnt; i++)
        {
          //key.set_value(i + 10000);
          //ret = btree.put(handle, key, i+base_value);
          //EXPECT_EQ(ret, ERROR_CODE_OK);
          BtreeWriteHandle *h1 = handle.get_write_handle();
          if (h1 == NULL) continue;
          key.set_value(i + 1000);
          ret = btree.put(*h1, key, i + base_value);
          EXPECT_EQ(ret, ERROR_CODE_OK);
          if (i % skip == 0)
          {
            h1->rollback();
            rcnt ++;
          }
          h1->end();
        }
        handle.end();
      }
      EXPECT_EQ(cnt - rcnt, btree.get_object_count());
      for(i = 0; i < cnt; i++)
      {
        if (i % skip == 0) continue;
        key.set_value(i + 1000);
        ret = btree.get(key, value);
        EXPECT_EQ(ret, ERROR_CODE_OK);
        EXPECT_EQ(value, i + base_value);
      }
      btree.clear();
    }
    TEST(BtreeWriteHandleTest, rollback_write_handle)
    {
      StringBtree btree(TEST_KEY_MAX_SIZE);
      int cnt = 5;
      TestKey key;
      int i, j, skip = 2;
      int ret;
      int rcnt = 0;
      int base_value = 0x1ffff;
      for(j = 0; j < cnt; j++)
      {
        BtreeWriteHandle handle;
        btree.get_write_handle(handle);
        for(i = 0; i < cnt; i++)
        {
          BtreeWriteHandle *h1 = handle.get_write_handle();
          if (h1 == NULL) continue;
          key.set_value(j * cnt + i + 1000);
          ret = btree.put(*h1, key, i + base_value);
          EXPECT_EQ(ret, ERROR_CODE_OK);
          if (i % skip == 0)
          {
            h1->rollback();
            rcnt ++;
          }
          h1->end();
        }
        if (j % skip == 0)
        {
          handle.rollback();
        }
      }
      btree.clear();
    }
    TEST(BtreeWriteHandleTest, key_write_handle)
    {
      BtreeDefaultAlloc kalloc;
      BtreeDefaultAlloc nalloc;
      StringBtree btree(TEST_KEY_MAX_SIZE, &kalloc, &nalloc);
      TestKey key;
      key.set_value(1000);
      for(int i = 0; i < 10000; i++)
      {
        btree.put(key, i);
        btree.remove(key);
      }
      EXPECT_EQ(0, kalloc.get_use_count());
      EXPECT_EQ(2, nalloc.get_use_count());
      btree.clear();
    }
  } // end namespace common
} // end namespace oceanbase
