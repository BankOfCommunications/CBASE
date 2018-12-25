#include <limits.h>
#include <btree_default_alloc.h>
#include <btree_node.h>
#include <gtest/gtest.h>

namespace oceanbase
{
  namespace common
  {
    int64_t key_compare_func_test(const char *a, const char *b)
    {
      return strcmp(a, b);
    }

    BtreeNode *allocNode()
    {
      int32_t size = sizeof(BtreeNode) + sizeof(BtreeKeyValuePair) * CONST_NODE_OBJECT_COUNT;
      BtreeNode *ret = reinterpret_cast<BtreeNode*>(malloc(size));
      memset(ret, 0, size);
      return ret;
    }

    TEST(BtreeNodeTest, init)
    {
      BtreeNode *node = allocNode();
      EXPECT_EQ(node->init(1), ERROR_CODE_OK);
      EXPECT_EQ(node->init(0), ERROR_CODE_FAIL);
      node->set_deleted(1000);
      EXPECT_EQ(node->is_deleted(), 1);
      node->set_deleted(0);
      EXPECT_EQ(node->is_deleted(), 0);
      node->set_read_only(1000);
      EXPECT_EQ(node->is_read_only(), 1);
      node->set_read_only(0);
      EXPECT_EQ(node->is_read_only(), 0);
      free(node);
      BtreeNode node1;
      EXPECT_EQ(node1.get_size(), 0);
    }

    TEST(BtreeNodeTest, add_pair)
    {
      BtreeNode *node = allocNode();
      node->init(CONST_NODE_OBJECT_COUNT);
      EXPECT_EQ(node->add_pair(-1, "A01", reinterpret_cast<char*>(1), NULL), ERROR_CODE_FAIL);
      EXPECT_EQ(node->add_pair(1, "A01", reinterpret_cast<char*>(1), NULL), ERROR_CODE_FAIL);
      EXPECT_EQ(node->add_pair(0, "A01", reinterpret_cast<char*>(1), NULL), ERROR_CODE_OK);
      EXPECT_EQ(node->add_pair(1, "A02", reinterpret_cast<char*>(2), NULL), ERROR_CODE_OK);
      EXPECT_EQ(node->add_pair(2, "A03", reinterpret_cast<char*>(3), NULL), ERROR_CODE_OK);
      EXPECT_EQ(node->add_pair(3, "A04", reinterpret_cast<char*>(4), NULL), ERROR_CODE_OK);
      EXPECT_EQ(node->add_pair(4, "A05", reinterpret_cast<char*>(5), NULL), ERROR_CODE_OK);
      EXPECT_EQ(node->get_size(), 5);

      int64_t flag;
      int32_t pos = node->find_pos("A04", key_compare_func_test, flag);
      EXPECT_EQ(pos, 3);
      EXPECT_EQ(flag, 0);

      BtreeKeyValuePair *key = node->get_pair(2);
      EXPECT_EQ(key->value_, reinterpret_cast<char*>(3));
      EXPECT_TRUE(node->get_pair(5) == NULL);

      key = node->get_next_pair(2);
      EXPECT_EQ(key->value_, reinterpret_cast<char*>(4));
      EXPECT_TRUE(node->get_next_pair(4) == NULL);
      EXPECT_TRUE(node->get_next_pair(-1) == NULL);

      node->set_read_only(1);
      EXPECT_EQ(node->add_pair(4, "A006", reinterpret_cast<char*>(20), NULL), ERROR_CODE_FAIL);
      node->set_read_only(0);

      // 把node写满
      for(int32_t i = 5; i < CONST_NODE_OBJECT_COUNT; i++)
      {
        EXPECT_EQ(node->add_pair(i, "", reinterpret_cast<char*>(i + 1), NULL), ERROR_CODE_OK);
      }
      EXPECT_EQ(node->add_pair(4, "A006", reinterpret_cast<char*>(20), NULL), ERROR_CODE_FAIL);
      free(node);
    }

    TEST(BtreeNodeTest, remove_pair)
    {
      BtreeNode *node = allocNode();
      node->init(CONST_NODE_OBJECT_COUNT);
      for(int32_t i = 0; i < CONST_NODE_OBJECT_COUNT; i++)
      {
        EXPECT_EQ(node->add_pair(i, "", reinterpret_cast<char*>(i + 1), NULL), ERROR_CODE_OK);
      }
      BtreeKeyValuePair pair;
      EXPECT_EQ(node->remove_pair(-1, pair), ERROR_CODE_FAIL);
      EXPECT_EQ(node->remove_pair(CONST_NODE_OBJECT_COUNT, pair), ERROR_CODE_FAIL);
      node->set_read_only(1);
      EXPECT_EQ(node->remove_pair(0, pair), ERROR_CODE_FAIL);
      free(node);
    }

    TEST(BtreeNodeTest, set_pair)
    {
      BtreeNode *node = allocNode();
      node->init(CONST_NODE_OBJECT_COUNT);
      EXPECT_EQ(node->set_pair(-1, NULL, NULL, 0, NULL), ERROR_CODE_FAIL);
      EXPECT_EQ(node->set_pair(0, NULL, NULL, 0, NULL), ERROR_CODE_FAIL);

      EXPECT_EQ(node->add_pair(0, reinterpret_cast<char*>(19), reinterpret_cast<char*>(20), NULL), ERROR_CODE_OK);
      EXPECT_EQ(node->set_pair(0, NULL, reinterpret_cast<char*>(21), 2, NULL), ERROR_CODE_OK);
      BtreeKeyValuePair *key = node->get_pair(0);
      EXPECT_EQ(key->key_, reinterpret_cast<char*>(19));
      EXPECT_EQ(key->value_, reinterpret_cast<char*>(21));
      EXPECT_EQ(node->set_pair(0, reinterpret_cast<char*>(1), reinterpret_cast<char*>(2), 3, NULL), ERROR_CODE_OK);
      key = node->get_pair(0);
      EXPECT_EQ(key->key_, reinterpret_cast<char*>(1));
      EXPECT_EQ(key->value_, reinterpret_cast<char*>(2));
      EXPECT_EQ(node->set_pair(0, reinterpret_cast<char*>(3), NULL, 1, NULL), ERROR_CODE_OK);
      key = node->get_pair(0);
      EXPECT_EQ(key->key_, reinterpret_cast<char*>(3));
      EXPECT_EQ(key->value_, reinterpret_cast<char*>(2));

      node->set_read_only(1);
      EXPECT_EQ(node->set_pair(0, reinterpret_cast<char*>(4), NULL, 1, NULL), ERROR_CODE_FAIL);
      free(node);
    }

    TEST(BtreeNodeTest, move_pair_out)
    {
      BtreeNode *node = allocNode();
      BtreeNode *next_node = allocNode();
      node->init(CONST_NODE_OBJECT_COUNT);
      next_node->init(CONST_NODE_OBJECT_COUNT);

      node->set_read_only(1);
      EXPECT_EQ(node->move_pair_out(next_node, 1), ERROR_CODE_FAIL);
      node->set_read_only(0);
      next_node->set_read_only(1);
      EXPECT_EQ(node->move_pair_out(next_node, 1), ERROR_CODE_FAIL);
      next_node->set_read_only(0);

      EXPECT_EQ(node->move_pair_out(next_node, 0), ERROR_CODE_FAIL);
      EXPECT_EQ(node->move_pair_out(next_node, 1), ERROR_CODE_FAIL);

      for(int32_t i = 0; i < CONST_NODE_OBJECT_COUNT - 1; i++)
      {
        node->add_pair(i, reinterpret_cast<char*>(i), reinterpret_cast<char*>(i), NULL);
        next_node->add_pair(i, reinterpret_cast<char*>(i), reinterpret_cast<char*>(i), NULL);
      }
      EXPECT_EQ(node->move_pair_out(next_node, 2), ERROR_CODE_FAIL);
      EXPECT_EQ(node->move_pair_out(next_node, 1), ERROR_CODE_OK);
      free(node);
      free(next_node);
    }

    TEST(BtreeNodeTest, move_pair_in)
    {
      BtreeNode *node = allocNode();
      BtreeNode *next_node = allocNode();
      node->init(CONST_NODE_OBJECT_COUNT);
      next_node->init(CONST_NODE_OBJECT_COUNT);

      node->set_read_only(1);
      EXPECT_EQ(node->move_pair_in(next_node, 1), ERROR_CODE_FAIL);
      node->set_read_only(0);
      next_node->set_read_only(1);
      EXPECT_EQ(node->move_pair_in(next_node, 1), ERROR_CODE_FAIL);
      next_node->set_read_only(0);

      EXPECT_EQ(node->move_pair_in(next_node, 0), ERROR_CODE_FAIL);
      EXPECT_EQ(node->move_pair_in(next_node, 1), ERROR_CODE_FAIL);

      for(int32_t i = 0; i < CONST_NODE_OBJECT_COUNT - 1; i++)
      {
        node->add_pair(i, reinterpret_cast<char*>(i), reinterpret_cast<char*>(i), NULL);
        next_node->add_pair(i, reinterpret_cast<char*>(i), reinterpret_cast<char*>(i), NULL);
      }
      EXPECT_EQ(node->move_pair_in(next_node, 2), ERROR_CODE_FAIL);
      EXPECT_EQ(node->move_pair_in(next_node, 1), ERROR_CODE_OK);
      free(node);
      free(next_node);
    }

    TEST(BtreeNodeTest, merge_pair_in)
    {
      BtreeNode *node = allocNode();
      BtreeNode *next_node = allocNode();
      node->init(CONST_NODE_OBJECT_COUNT);
      next_node->init(CONST_NODE_OBJECT_COUNT);

      node->set_read_only(1);
      EXPECT_EQ(node->merge_pair_in(next_node), ERROR_CODE_FAIL);
      node->set_read_only(0);

      int32_t i = 0;
      for(i = 0; i < CONST_NODE_OBJECT_COUNT / 2 + 1; i++)
      {
        node->add_pair(i, reinterpret_cast<char*>(i), reinterpret_cast<char*>(i), NULL);
        next_node->add_pair(i, reinterpret_cast<char*>(i), reinterpret_cast<char*>(i), NULL);
      }
      EXPECT_EQ(node->merge_pair_in(next_node), ERROR_CODE_FAIL);
      BtreeKeyValuePair pair;
      next_node->remove_pair(0, pair);
      if (CONST_NODE_OBJECT_COUNT % 2 == 0) node->remove_pair(0, pair);
      EXPECT_EQ(node->merge_pair_in(next_node), ERROR_CODE_OK);
      free(node);
      free(next_node);
    }

    TEST(BtreeNodeTest, key_refcount)
    {
      BtreeNode *node = allocNode();
      node->init(CONST_NODE_OBJECT_COUNT);
      BtreeDefaultAlloc allocator;
      char *key = allocator.alloc();
      int32_t *addr = reinterpret_cast<int32_t*>(key);
      (*addr) = 1;
      node->add_pair(0, key, reinterpret_cast<char*>(1), NULL);
      node->inc_key_refcount();
      EXPECT_EQ(*addr, 1);
      node->dec_key_refcount(&allocator);
      EXPECT_EQ(*addr, 1);

      node->set_leaf(1);
      node->inc_key_refcount();
      EXPECT_EQ(*addr, 2);

      node->dec_key_refcount(&allocator);
      EXPECT_EQ(*addr, 1);
      node->dec_key_refcount(&allocator);
#ifndef OCEAN_BASE_BTREE_USE_SYS_MALLOC
      EXPECT_EQ(*addr, 0);
#endif

      node->set_sequence(1);
      EXPECT_EQ(node->get_sequence(), 1);
      free(node);
    }
  } // end namespace common
} // end namespace oceanbase
