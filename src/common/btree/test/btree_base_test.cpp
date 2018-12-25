#include <limits.h>
#include <btree_read_handle_new.h>
#include <id_btree.h>
#include <id_key_btree.h>
#include <key_btree.h>
#include <test_key.h>
#include <gtest/gtest.h>

namespace oceanbase
{
  namespace common
  {
    TEST(BtreeBaseTest, get_read_handle)
    {
      int64_t value;
      uint64_t key = 1;
      IdBtree<int64_t> btree;
      {
        BtreeBaseHandle handle;
        EXPECT_EQ(btree.get_read_handle(handle), ERROR_CODE_OK);
        EXPECT_EQ(btree.get_object_count(), 0);
        EXPECT_EQ(btree.get_node_count(), 0);
        EXPECT_EQ(btree.get(key, value), ERROR_CODE_NOT_FOUND);

        // 加一些数据
        btree.put(key, 1);

        // 再get_read_handle
        EXPECT_EQ(btree.get_read_handle(handle), ERROR_CODE_FAIL);
        // 再get_read_handle, 一个handle只能用一次
        EXPECT_EQ(btree.get_read_handle(handle), ERROR_CODE_FAIL);
      }
      btree.clear();
    }

    TEST(BtreeBaseTest, get_write_handle)
    {
      IdBtree<int64_t> btree;
      // 加一些数据
      uint64_t key;
      int64_t value;
      btree.put(key, 1);
      EXPECT_TRUE(key == 0);
      EXPECT_EQ(btree.get(key, value), ERROR_CODE_OK);
      EXPECT_TRUE(value == 1);
      btree.put(key, 20);
      EXPECT_TRUE(key == 1);
      EXPECT_EQ(btree.get(key, value), ERROR_CODE_OK);
      EXPECT_TRUE(value == 20);
      EXPECT_EQ(btree.get(key + 1, value), ERROR_CODE_NOT_FOUND);
      btree.clear();
    }

    int read_range(KeyBtree<int64_t, int64_t> *btree, int64_t from, int32_t fe, int64_t to, int32_t te)
    {
      BtreeReadHandle handle;
      int cnt = 0;
      int64_t key;
      int64_t value;
      if (btree->get_read_handle(handle) == ERROR_CODE_OK)
      {
        btree->set_key_range(handle, &from, fe, &to, te);
        while(btree->get_next(handle, key, value) == ERROR_CODE_OK)
        {
          EXPECT_EQ(key + 1, value);
          cnt ++;
        }
        EXPECT_EQ(btree->get_next(handle, value), ERROR_CODE_NOT_FOUND);
      }
      return cnt;
    }
    void read_range_ex(int line, KeyBtree<int64_t, int64_t> *btree, int64_t from, int64_t to, int e1, int e2, int e3, int e4)
    {
      int cnt;
      cnt = read_range(btree, from, 0, to, 0);
      EXPECT_EQ(cnt, e1) << "==> 0,0 Line:" << line;
      cnt = read_range(btree, from, 1, to, 0);
      EXPECT_EQ(cnt, e2) << "==> 1,0 Line:" << line;
      cnt = read_range(btree, from, 0, to, 1);
      EXPECT_EQ(cnt, e3) << "==> 0,1 Line:" << line;
      cnt = read_range(btree, from, 1, to, 1);
      EXPECT_EQ(cnt, e4) << "==> 1,1 Line:" << line;

      cnt = read_range(btree, to, 0, from, 0);
      EXPECT_EQ(cnt, e1) << "<== 0,0 Line:" << line;
      cnt = read_range(btree, to, 0, from, 1);
      EXPECT_EQ(cnt, e2) << "<== 0,1 Line:" << line;
      cnt = read_range(btree, to, 1, from, 0);
      EXPECT_EQ(cnt, e3) << "<== 1,0 Line:" << line;
      cnt = read_range(btree, to, 1, from, 1);
      EXPECT_EQ(cnt, e4) << "<== 1,1 Line:" << line;
    }

#define XT(x,y) (int64_t)(0x1000000000000000L*x+y)
    TEST(BtreeBaseTest, range_read)
    {
      KeyBtree<int64_t, int64_t> btree(sizeof(int64_t));
      {
        BtreeReadHandle handle;
        int64_t key;
        int64_t value;
        EXPECT_EQ(btree.get(key, value), ERROR_CODE_NOT_FOUND);
        EXPECT_EQ(btree.get_next(handle, value), ERROR_CODE_NOT_FOUND);
        // 加一些数据
        {
          BtreeWriteHandle handle;
          btree.get_write_handle(handle);
          for(int i = 1; i <= 10; i ++)
          {
            key = XT(i, 0);
            btree.put(handle, key, key + 1);
          }
        }
        EXPECT_EQ(btree.get_object_count(), 10);

        read_range_ex(__LINE__, &btree, XT(0, 8), XT(0, 9),   0, 0, 0, 0);
        read_range_ex(__LINE__, &btree, XT(0, 9), XT(0, 9),   0, 0, 0, 0);
        read_range_ex(__LINE__, &btree, XT(0, 9), XT(1, 0),  1, 1, 0, 0);
        read_range_ex(__LINE__, &btree, XT(1, 0), XT(1, 0),  1, 0, 0, 0);
        read_range_ex(__LINE__, &btree, XT(1, 0), XT(1, 1),  1, 0, 1, 0);
        read_range_ex(__LINE__, &btree, XT(1, 1), XT(1, 1),  0, 0, 0, 0);
        read_range_ex(__LINE__, &btree, XT(1, 1), XT(2, 0),  1, 1, 0, 0);
        read_range_ex(__LINE__, &btree, XT(1, 0), XT(2, 0),  2, 1, 1, 0);
        read_range_ex(__LINE__, &btree, XT(1, 0), XT(10, 0),  10, 9, 9, 8);
        read_range_ex(__LINE__, &btree, XT(9, 9), XT(10, 0),  1, 1, 0, 0);
        read_range_ex(__LINE__, &btree, XT(10, 0), XT(10, 0),  1, 0, 0, 0);
        read_range_ex(__LINE__, &btree, XT(10, 0), XT(10, 1),  1, 0, 1, 0);
        read_range_ex(__LINE__, &btree, XT(10, 1), XT(10, 1),  0, 0, 0, 0);
        read_range_ex(__LINE__, &btree, XT(10, 1), XT(10, 2),  0, 0, 0, 0);
        read_range_ex(__LINE__, &btree, XT(10, 1), XT(12, 0),  0, 0, 0, 0);
        read_range_ex(__LINE__, &btree, XT(0, 0), XT(12, 0),  10, 10, 10, 10);
      }
      btree.clear();
    }

    TEST(BtreeBaseTest, batch_write)
    {
      IdBtree<int64_t> btree;
      uint64_t key;
      {
        BtreeWriteHandle handle;
        btree.get_write_handle(handle);
        for(int i = 0; i < 300; i++)
        {
          btree.put(handle, key, i);
        }
        for(int i = 0; i <= static_cast<int>(key); i++)
        {
          btree.remove(handle, i);
        }
        for(int i = 0; i < 300; i++)
        {
          btree.put(handle, key, i);
        }
        for(int i = static_cast<int>(key); i > static_cast<int>(key - 300); i--)
        {
          btree.remove(handle, i);
        }
      }
      EXPECT_EQ(btree.get_object_count(), 0);
      btree.clear();
    }

    TEST(BtreeBaseTest, id_key_btree)
    {
      IdKeyBtree<TestKey, int64_t> btree(TEST_KEY_MAX_SIZE);
      TestKey key;
      uint64_t id;
      int64_t value, value1;
      // 加一些数据
      {
        BtreeWriteHandle handle;
        btree.get_write_handle(handle);
        for(int i = 0; i < 40; i++)
        {
          key.set_value(i);
          btree.put(handle, key, i + 1, id);
          EXPECT_EQ(id, static_cast<uint64_t>(i));
        }
        for(int i = 20; i < 30; i++)
        {
          key.set_value(i);
          EXPECT_EQ(btree.remove(handle, key), ERROR_CODE_OK);
        }
        for(int i = 30; i < 40; i++)
        {
          id = i;
          EXPECT_EQ(btree.remove(handle, id), ERROR_CODE_OK);
        }
      }
      EXPECT_EQ(btree.get_object_count(), 20);

      // get
      id = 10;
      key.set_value(id);
      EXPECT_EQ(btree.get(key, value), ERROR_CODE_OK);
      EXPECT_EQ(btree.get(id, value1), ERROR_CODE_OK);
      EXPECT_EQ(value, value1);

      // remove
      EXPECT_EQ(btree.remove(key), ERROR_CODE_OK);
      EXPECT_EQ(btree.remove(id), ERROR_CODE_NOT_FOUND);
      btree.clear();
    }

    TEST(BtreeBaseTest, clear)
    {
      IdKeyBtree<TestKey, int64_t> btree(TEST_KEY_MAX_SIZE);
      TestKey key;
      uint64_t id;
      int cnt = 10;
      {
        key.set_value(cnt);
        btree.put(key, cnt + 1, id);
        BtreeWriteHandle handle;
        btree.get_write_handle(handle);
        for(int i = 0; i < cnt; i++)
        {
          key.set_value(i);
          btree.put(handle, key, i + 1, id);
          EXPECT_EQ(id, static_cast<uint64_t>(i + 1));
        }
      }
      EXPECT_EQ(cnt + 1, btree.get_object_count());
      btree.clear();
      EXPECT_EQ(0, btree.get_object_count());
      btree.clear();
      EXPECT_EQ(2, btree.get_alloc_count());
      btree.clear();
      EXPECT_EQ(2, btree.get_alloc_count());
      cnt = 1000;
      {
        key.set_value(cnt);
        BtreeWriteHandle handle;
        btree.get_write_handle(handle);
        for(int i = 0; i < cnt; i++)
        {
          key.set_value(i + 10000);
          btree.put(handle, key, i + 1, id);
        }
      }
      EXPECT_EQ(cnt, btree.get_object_count());
      btree.clear();
      EXPECT_EQ(0, btree.get_object_count());
      btree.clear();
      EXPECT_EQ(2, btree.get_alloc_count());
      btree.clear();
      EXPECT_EQ(2, btree.get_alloc_count());
      btree.clear();
    }
    TEST(BtreeBaseTest, destroy)
    {

      KeyBtree<TestKey, int64_t> btree(TEST_KEY_MAX_SIZE);
      TestKey key;
      int cnt = 100;
      for(int n = 0; n < 3; n++)
      {
        for(int i = 0; i < cnt; i++)
        {
          key.set_value(i);
          btree.put(key, i + 1);
        }
        EXPECT_EQ(btree.get_object_count(), cnt);
        BtreeReadHandle handle;
        btree.get_read_handle(handle);
        btree.destroy();
        EXPECT_EQ(btree.get_alloc_memory(), 0);
      }
    }
  } // end namespace common
} // end namespace oceanbase
