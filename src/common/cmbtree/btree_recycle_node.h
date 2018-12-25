/*
 * Copyright (C) 2012-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   Fusheng Han <yanran.hfs@taobao.com>
 *     - Data structure used for memory recycling
 */
#ifndef OCEANBASE_COMMON_CMBTREE_BTREE_RECYCLE_NODE_H_
#define OCEANBASE_COMMON_CMBTREE_BTREE_RECYCLE_NODE_H_

#include "btree_define.h"
#include "btree_node.h"

namespace oceanbase
{
  namespace common
  {
    namespace cmbtree
    {
      template<class K, class V>
      class BtreeRecycleNode
      {
        public:
          typedef BtreeBaseNode<K, V>           TBaseNode;
          typedef BtreeRecycleNode<K, V>        TRecycleNode;
        public:
          BtreeRecycleNode();
          ~BtreeRecycleNode();

          /**
           * @brief 增加引用计数
           */
          void     inc_ref();

          /**
           * @brief 减少引用计数
           */
          void     dec_ref();

          /**
           * @brief 获得引用计数
           */
          int64_t  get_ref() const;

          /**
           * @brief 添加新的待回收节点
           */
          int32_t  add_node(TBaseNode * new_node);

          /**
           * @brief 获取待回收链表的头节点
           */
          TBaseNode * get_node_list();

          /**
           * @brief 设置next指向的位置
           */
          int32_t  set_next(TRecycleNode * next);

          /**
           * @brief 得到next值
           */
          BtreeRecycleNode<K, V> * get_next();

          bool     is_valid() const;

          void     set_invalid();

          void     set_valid();

          int64_t  get_node_num();

        protected:
          TRecycleNode * volatile next_;   /// 指向下一个RecycleNode
          int64_t             node_num_;   /// number of node to be recycled
          int64_t             ref_;        /// 引用计数
          TBaseNode *         node_list_;  /// 待回收的节点列表
          /**
           * This variable is a little tricky.
           * After a recycle node is freed, it still have chances be referenced.
           * And the is_valid_ is the sign for indicating this recycle node
           * is not a valid one.
           */
          bool                is_valid_;
      };

      template<class K, class V>
      BtreeRecycleNode<K, V>::BtreeRecycleNode()
        // ref_ can not be initialized to zero
        // due to other thread may still use an old RecycleNode
        // with the same memory location
        : next_(NULL), node_num_(0), node_list_(NULL), is_valid_(true)
      {
      }

      template<class K, class V>
      BtreeRecycleNode<K, V>::~BtreeRecycleNode()
      {
      }

      template<class K, class V>
      void BtreeRecycleNode<K, V>::inc_ref()
      {
        CMBTREE_ATOMIC_INC(&ref_);
      }

      template<class K, class V>
      void BtreeRecycleNode<K, V>::dec_ref()
      {
        CMBTREE_ATOMIC_DEC(&ref_);
      }

      template<class K, class V>
      int64_t BtreeRecycleNode<K, V>::get_ref() const
      {
        return ref_;
      }

      template<class K, class V>
      int32_t BtreeRecycleNode<K, V>::add_node(TBaseNode * new_node)
      {
        int32_t ret = ERROR_CODE_OK;
        if (UNLIKELY(NULL == new_node))
        {
          CMBTREE_LOG(ERROR, "add_node new_node is null");
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          //TBaseNode * old_val = NULL;
          //do
          //{
          //  old_val = node_list_;
          //  new_node->set_next(old_val);
          //}
          //while (old_val != CMBTREE_ATOMIC_CAS(&node_list_, old_val, new_node));
          new_node->set_next(node_list_);
          node_list_ = new_node;
          node_num_++;
        }
        return ret;
      }

      template<class K, class V>
      typename BtreeRecycleNode<K, V>::TBaseNode *
          BtreeRecycleNode<K, V>::get_node_list()
      {
        return node_list_;
      }

      template<class K, class V>
      int32_t BtreeRecycleNode<K, V>::set_next(TRecycleNode * next)
      {
        int32_t ret = ERROR_CODE_OK;
        if (UNLIKELY(NULL == next))
        {
          CMBTREE_LOG(ERROR, "set_next next is null");
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          //CMBTREE_ATOMIC_STORE(&next_, next, __ATOMIC_ACQ_REL);
          next_ = next;
        }
        return ret;
      }

      template<class K, class V>
      BtreeRecycleNode<K, V> * BtreeRecycleNode<K, V>::get_next()
      {
        return const_cast<BtreeRecycleNode<K, V> *>(next_);
      }

      template<class K, class V>
      bool BtreeRecycleNode<K, V>::is_valid() const
      {
        return is_valid_;
      }

      template<class K, class V>
      void BtreeRecycleNode<K, V>::set_invalid()
      {
        is_valid_ = false;
      }

      template<class K, class V>
      void BtreeRecycleNode<K, V>::set_valid()
      {
        is_valid_ = true;
      }

      template<class K, class V>
      int64_t BtreeRecycleNode<K, V>::get_node_num()
      {
        return node_num_;
      }

    } // end namespace cmbtree
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_CMBTREE_BTREE_RECYCLE_NODE_H_
