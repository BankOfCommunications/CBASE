#include "btree_define.h"
#include "btree_alloc.h"
#include "btree_array_list.h"
#include "btree_node.h"

namespace oceanbase
{
  namespace common
  {
    const int32_t BtreeArrayList::NODE_SIZE = (CONST_NODE_OBJECT_COUNT * sizeof(BtreeKeyValuePair) + sizeof(BtreeNode));
    const int32_t BtreeArrayList::NODE_COUNT_PRE_PAGE = (NODE_SIZE / sizeof(void*) - 1);

    BtreeArrayList::BtreeArrayList()
    {
      init(NODE_SIZE);
    }

    /**
     * 初始化
     */
    void BtreeArrayList::init(int32_t struct_size)
    {
      assert(struct_size <= NODE_SIZE);
      max_copy_node_count_ = static_cast<int32_t>((NODE_SIZE - struct_size) / sizeof(void*));

      copy_node_count_ = 0;
      copy_list_start_ = NULL;
      copy_list_current_ = NULL;
      copy_list_end_ = NULL;

#ifdef OCEAN_BTREE_CHECK
      bf.clear();
#endif
    }

    /**
     * 增加到node进来
     */
    void BtreeArrayList::add_node_list(BtreeAlloc *node_allocator, int32_t
#ifdef OCEAN_BTREE_CHECK
                                       unique
#endif
                                       , void *node)
    {
      OCEAN_BTREE_CHECK_TRUE(node, "node is null.");
      OCEAN_BTREE_CHECK_TRUE(node_allocator, "node is null.");
      OCEAN_BTREE_CHECK_TRUE(copy_node_count_ >= 0, "copy_node_count_: %d", copy_node_count_);

      if (NULL != node && copy_node_count_ >= 0 && node_allocator)
      {
#ifdef OCEAN_BTREE_CHECK
        if (unique && bf.exist(node))
        {
          void *tn = NULL;
          BtreeNodeList tlist;
          node_list_first(tlist);

          // 取出每个node
          while((tn = node_list_next(tlist)) != NULL)
          {
            if (tn == node)
            {
              OCEAN_BTREE_CHECK_TRUE(0, "node is repeat.");
              abort();
            }
          }
        }
#endif
        // 放在copy_node_上就可以
        if (copy_node_count_ < max_copy_node_count_)
        {
          copy_node_[copy_node_count_] = node;
        }
        else if (copy_list_current_ != NULL && copy_list_current_ > copy_list_end_)
        {
          (*copy_list_current_) = node;
          copy_list_current_ --;
        }
        else
        {
          void **pdata = NULL;
          char* list = NULL;
          // 存在以前的链
          if (NULL != copy_list_current_)
            list = reinterpret_cast<char*>(*copy_list_current_);
          else
            list = reinterpret_cast<char*>(copy_list_start_);
          // 分配一个新的链
          if (NULL == list)
          {
            list = node_allocator->alloc();
            OCEAN_BTREE_CHECK_TRUE(list, "list is null.");
            if (NULL != copy_list_current_)
              (*copy_list_current_) = reinterpret_cast<void*>(list);
            else
              copy_list_start_ = reinterpret_cast<void**>(list);
          }
          if (NULL != list)
          {
            // 插入node
            pdata = reinterpret_cast<void**>(list);
            copy_list_current_ = pdata + NODE_COUNT_PRE_PAGE;
            copy_list_end_ = pdata;
            (*copy_list_current_) = node;
            copy_list_current_ --;
          }
        }

        copy_node_count_ ++;
#ifdef OCEAN_BTREE_CHECK
        bf.set(node);
#endif
      }
    }

    /**
     * 得到初始值
     */
    void BtreeArrayList::node_list_first(BtreeNodeList &list)
    {
      list.pos = 0;
      list.node_count = copy_node_count_;
      list.start = copy_list_start_;
      list.end = copy_list_current_;
      if (list.start)
        list.current = list.start + NODE_COUNT_PRE_PAGE;
      else
        list.current = NULL;
      if ((NULL != list.start && NULL == (*list.start)) || (NULL == list.end))   // 最后一个链
      {
        list.start = list.end;
      }
    }

    /**
     * 得到node_list中的next
     */
    void *BtreeArrayList::node_list_next(BtreeNodeList &list)
    {
      void *node = NULL;
      // 如果还RootPointer块上
      if (list.pos >= 0 && list.pos < list.node_count && list.pos < max_copy_node_count_)
      {
        node = copy_node_[list.pos];
        list.pos ++;
        // 下一个链
      }
      else if (list.pos >= 0 && NULL != list.start &&
               NULL != list.current && list.current > list.start)
      {
        OCEAN_BTREE_CHECK_TRUE(list.pos < list.node_count,
                               "cnt: %d %d", list.pos, list.node_count);
        node = (*list.current);
        OCEAN_BTREE_CHECK_TRUE(node, "node is null.");
        list.current --;
        list.pos ++;
        // 下一链接
        if (list.current == list.end)
        {
          list.start = NULL;
        }
        else if (list.start != list.end && list.current == list.start)
        {
          list.start = reinterpret_cast<void**>(*list.start);
          if (list.start)
          {
            list.current = list.start + NODE_COUNT_PRE_PAGE;
            if (NULL != list.start && NULL == (*list.start))   // 最后一个链
            {
              list.start = list.end;
            }
          }
        }
      }
      return node;
    }

    /**
     * 清空copy_list, 不对list上node进行释放, CLEAR_LIST_ONLY
     */
    void BtreeArrayList::clear_list_only(BtreeAlloc *node_allocator)
    {
      OCEAN_BTREE_CHECK_TRUE(node_allocator, "node_allocator is null.");
      void **pdata_next;
      void **pdata = copy_list_start_;
      while(NULL != pdata)
      {
        pdata_next = reinterpret_cast<void**>(*pdata);
        node_allocator->release(reinterpret_cast<char*>(pdata));
        // next
        pdata = pdata_next;
      }
      copy_list_start_ = NULL;
      copy_list_end_ = NULL;
      copy_list_current_ = NULL;
      copy_node_count_ = 0;
#ifdef OCEAN_BTREE_CHECK
      bf.clear();
#endif
    }

    /**
     * reset copy list
     */
    void BtreeArrayList::node_list_reset()
    {
      copy_list_end_ = NULL;
      copy_list_current_ = NULL;
      copy_node_count_ = 0;
    }

    /**
     * 对后面的空的list进行回收
     */
    void BtreeArrayList::shrink_list(BtreeAlloc *node_allocator)
    {
      void **pdata_next = NULL;
      void **pdata = NULL;
      if (copy_list_end_)
        pdata = reinterpret_cast<void**>(*copy_list_end_);
      else
        pdata = copy_list_start_;

      // 遍历每个链
      while(NULL != pdata)
      {
        pdata_next = reinterpret_cast<void**>(*pdata);
        if (node_allocator)
        {
          node_allocator->release(reinterpret_cast<char*>(pdata));
        }
#ifdef OCEAN_BTREE_CHECK
        bf.del(pdata);
#endif
        // next
        pdata = pdata_next;
      }

      // 设置为null
      if (copy_list_end_)
        (*copy_list_end_) = NULL;
      else
        copy_list_start_ = NULL;
    }

#ifdef OCEAN_BTREE_CHECK
    void BtreeArrayList::delete_bf(void *node)
    {
      bf.del(node);
    }
#endif

    /**
     * copy node数量
     */
    int32_t BtreeArrayList::get_copy_node_count()
    {
      return copy_node_count_;
    }
  } // end namespace common
} // end namespace oceanbase
