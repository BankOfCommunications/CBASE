#include <limits.h>
#include <btree_default_alloc.h>
#include <btree_node.h>
#include <btree_root_pointer.h>
#include <gtest/gtest.h>

/**
 * 测试BtreeRootPointer
 */
namespace oceanbase
{
  namespace common
  {
    TEST(BtreeRootPointerTest, add_node_list)
    {
      BtreeDefaultAlloc allocator;
      allocator.init(BtreeArrayList::NODE_SIZE);
      BtreeRootPointer *proot = (BtreeRootPointer*) allocator.alloc();
      proot->init(&allocator);
      proot->add_node_list(NULL);
      BtreeNode *node;

      int max_copy_node_count_ = (BtreeArrayList::NODE_SIZE - sizeof(BtreeRootPointer)) / sizeof(void*);
      int total_size = max_copy_node_count_ + BtreeArrayList::NODE_COUNT_PRE_PAGE * 2;
      for(int i = 0; i < total_size; i++)
      {
        node = (BtreeNode*) allocator.alloc();
        node->set_sequence(i + 1);
        proot->add_node_list(node);
      }
      EXPECT_EQ(proot->get_copy_node_count(), total_size);

      // 重复add_node_list
      BtreeNodeList list;
      int32_t ret = 0;
      proot->node_list_first(list);
      while((node = proot->node_list_next(list)) != NULL)
      {
        if (node->get_sequence() == list.pos)
          ret ++;
      }
      EXPECT_EQ(ret, total_size);
      EXPECT_EQ(allocator.get_use_count(), total_size + 3);

      for(int i = 0; i < 4; i++)
      {
        ret = 0;
        proot->node_list_first(list);
        proot->node_list_reset();
        // 取出每个node
        while((node = proot->node_list_next(list)) != NULL)
        {
          if (list.pos % 2)
          {
            proot->add_node_list(node);
            ret ++;
          }
          else
          {
            allocator.release(reinterpret_cast<char*>(node));
            total_size --;
          }
        }
        EXPECT_EQ(proot->get_copy_node_count(), ret);
      }
      proot->release_node_list(0, NULL);
      EXPECT_EQ(allocator.get_use_count(), 1);
      allocator.release(reinterpret_cast<char*>(proot));
    }

    TEST(BtreeRootPointerTest, remove_from_list)
    {
      BtreeDefaultAlloc allocator;
      allocator.init(BtreeArrayList::NODE_SIZE);
      BtreeRootPointer *proot[3];
      BtreeRootPointer *tail = NULL, *head = NULL;
      for(int i = 0; i < 3; i++)
      {
        proot[i] = (BtreeRootPointer*) allocator.alloc();
        proot[i]->init(&allocator);
        proot[i]->add_tail(head, tail);
        head = proot[i];
      }
      EXPECT_TRUE(head == proot[2]);
      EXPECT_TRUE(tail == proot[0]);
      // 删除
      proot[1]->remove_from_list(tail);
      EXPECT_TRUE(head == proot[2]);
      EXPECT_TRUE(tail == proot[0]);
      proot[2]->remove_from_list(tail);
      EXPECT_TRUE(tail == proot[0]);
      proot[0]->remove_from_list(tail);
      EXPECT_TRUE(tail == NULL);
      for(int i = 0; i < 3; i++)
      {
        allocator.release(reinterpret_cast<char*>(proot[i]));
      }
    }
  } // end namespace common
} // end namespace oceanbase
