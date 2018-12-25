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
 *     - Recycle pool
 */
#ifndef OCEANBASE_COMMON_CMBTREE_BTREE_RECYCLE_POOL_H_
#define OCEANBASE_COMMON_CMBTREE_BTREE_RECYCLE_POOL_H_

#include <pthread.h>
#include "btree_define.h"
#include "btree_recycle_node.h"
#include "btree_node.h"
#include "btree_mem_pool.h"
#include "btree_tid.h"
#include "qlock.h"

namespace oceanbase
{
  namespace common
  {
    namespace cmbtree
    {
      template<class K, class V, class Alloc>
      class BtreeRecyclePool
      {
        public:
          typedef BtreeBaseNode<K, V>           TBaseNode;
          typedef BtreeInterNode<K, V>          TInterNode;
          typedef BtreeLeafNode<K, V>           TLeafNode;
          typedef BtreeRecycleNode<K, V>        TRecycleNode;
          typedef BtreeRecyclePool<K, V, Alloc> TRecyclePool;
          typedef BtreeMemPool<K, V, Alloc>     TMemPool;
        public:
          BtreeRecyclePool(TMemPool & node_alloc, TMemPool & recycle_alloc);

          ~BtreeRecyclePool();

          int32_t  init                          ();

          void     destroy                       ();

          /**
           * @brief 添加新的RecycleNode
           */
          int32_t  add_recycle_node              (TRecycleNode * recycle_node);

          TRecycleNode * inc_ref_and_get_cur_recycle_node();

          /**
           * @brief 回收RecycleNode
           */
          int32_t  free_recycle_node             ();

          int64_t  get_recycle_node_num          () const;

          int64_t  get_btree_node_num            () const;

          int64_t  get_reserved_size             () const;

        protected:
          int32_t  do_free_                      (int64_t max_reserved_num);
          int32_t  free_node_                    (TRecycleNode * node);

          static const int64_t DEFAULT_BTREE_NODE_ALARM_NUM= 10000000;
          static const int64_t DEFAULT_LOCK_TIME           = 10000;
          static const int64_t DEFAULT_LOCK_RESERVED_NUM   = 20;
          static const int64_t DEFAULT_LOCK_BTREE_NODE_NUM = 1000;
          static const int64_t DEFAULT_TRY_RESERVED_NUM    = 80;
          static const int64_t DEFAULT_MAX_RESERVED_NUM    = 100;

        protected:
          int64_t             recycle_node_num_;
          int64_t             btree_node_num_;
          TRecycleNode * volatile cur_recycle_node_;/// 当前的RecycleNode
          TRecycleNode *      oldest_recycle_node_; /// 最老的RecycleNode
          QLock               mutex_;               /// 回收操作互斥锁
          TMemPool &          node_alloc_;          /// Btree内存分配器
          TMemPool &          recycle_alloc_;       /// 回收池的内存分配器
          bool                init_;
      };

      template<class K, class V, class Alloc>
      BtreeRecyclePool<K, V, Alloc>::BtreeRecyclePool(TMemPool & node_alloc,
          TMemPool & recycle_alloc)
          : recycle_node_num_(0), btree_node_num_(0), cur_recycle_node_(NULL),
            oldest_recycle_node_(NULL), node_alloc_(node_alloc),
            recycle_alloc_(recycle_alloc), init_(false)
      {
      }

      template<class K, class V, class Alloc>
      BtreeRecyclePool<K, V, Alloc>::~BtreeRecyclePool()
      {
        destroy();
      }

      template<class K, class V, class Alloc>
      int32_t BtreeRecyclePool<K, V, Alloc>::init()
      {
        int32_t ret = ERROR_CODE_OK;
        if (init_)
        {
          CMBTREE_LOG(ERROR, "BtreeRecyclePool has initialized");
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else if (NULL == cur_recycle_node_ && NULL == oldest_recycle_node_)
        {
          cur_recycle_node_ = oldest_recycle_node_
              = new (recycle_alloc_.alloc_block_with_initial_memset()) TRecycleNode();
          if (UNLIKELY(NULL == cur_recycle_node_))
          {
            CMBTREE_LOG(ERROR, "Btree initialized error: alloc failed!");
          }
          else
          {
            init_ = true;
            recycle_node_num_ = 1;
          }
        }
        else
        {
          CMBTREE_LOG(ERROR, "Internal Error: cur_recycle_node_ = %p "
              "oldest_recycle_node_ = %p",
              cur_recycle_node_, oldest_recycle_node_);
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      void BtreeRecyclePool<K, V, Alloc>::destroy()
      {
        if (init_)
        {
          TBSYS_LOG(DEBUG, "remained_recycle_node = %ld", get_recycle_node_num());
          do_free_(-1);
          init_ = false;
        }
      }

      template<class K, class V, class Alloc>
      int32_t BtreeRecyclePool<K, V, Alloc>::add_recycle_node(
          TRecycleNode * recycle_node)
      {
        int32_t ret = ERROR_CODE_OK;
        if (UNLIKELY(!init_))
        {
          CMBTREE_LOG(ERROR, "BtreeRecyclePool has not been initialized");
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          if (UNLIKELY(NULL == recycle_node))
          {
            CMBTREE_LOG(ERROR, "add_recycle_node recycle_node is null");
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            //recycle_node->set_next(NULL);
            TRecycleNode * pre_recycle_node = NULL;
            do
            {
              pre_recycle_node = const_cast<TRecycleNode *>(cur_recycle_node_);
            }
            while (LIKELY(pre_recycle_node
                != CMBTREE_ATOMIC_CAS(&cur_recycle_node_, pre_recycle_node, recycle_node)));
            pre_recycle_node->set_next(recycle_node);
            CMBTREE_ATOMIC_INC(&recycle_node_num_);
            CMBTREE_ATOMIC_ADD(&btree_node_num_, recycle_node->get_node_num());
          }
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      typename BtreeRecyclePool<K, V, Alloc>::TRecycleNode *
          BtreeRecyclePool<K, V, Alloc>::inc_ref_and_get_cur_recycle_node()
      {
        TRecycleNode * ret = NULL;
        if (UNLIKELY(!init_))
        {
          CMBTREE_LOG(ERROR, "BtreeRecyclePool has not been initialized");
        }
        else
        {
          do
          {
            TRecycleNode * cur = const_cast<TRecycleNode *>(cur_recycle_node_);
            if (UNLIKELY(NULL == cur))
            {
              CMBTREE_LOG(ERROR, "cur_recycle_node_ is NULL");
              break;
            }
            else
            {
              cur->inc_ref();
              if (LIKELY(cur->is_valid()))
              {
                ret = cur;
              }
              else
              {
                cur->dec_ref();
              }
            }
          }
          while (NULL == ret);
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      int32_t BtreeRecyclePool<K, V, Alloc>::free_recycle_node()
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;
        CMBTREE_CHECK_FALSE(btree_node_num_ > DEFAULT_BTREE_NODE_ALARM_NUM,
            "Fatal error: btree node in recycle pool is more than %ld",
            DEFAULT_BTREE_NODE_ALARM_NUM);
        if (UNLIKELY(!init_))
        {
          CMBTREE_LOG(ERROR, "BtreeRecyclePool has not been initialized");
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else if (UNLIKELY(node_alloc_.get_reserved_block_num() < DEFAULT_LOCK_RESERVED_NUM
            && btree_node_num_ > DEFAULT_LOCK_BTREE_NODE_NUM))
        {
          err = mutex_.exclusive_lock(BtreeTID::gettid(),
              tbsys::CTimeUtil::getTime() + DEFAULT_LOCK_TIME);
          if (LIKELY(ERROR_CODE_OK == err))
          {
            ret = do_free_(DEFAULT_MAX_RESERVED_NUM);
            mutex_.exclusive_unlock(BtreeTID::gettid());
          }
        }
        else if (node_alloc_.get_reserved_block_num() < DEFAULT_TRY_RESERVED_NUM)
        {
          err = mutex_.try_exclusive_lock(BtreeTID::gettid());
          if (LIKELY(ERROR_CODE_OK == err))
          {
            ret = do_free_(DEFAULT_MAX_RESERVED_NUM);
            mutex_.exclusive_unlock(BtreeTID::gettid());
          }
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      int64_t BtreeRecyclePool<K, V, Alloc>::get_recycle_node_num() const
      {
        return recycle_node_num_;
      }

      template<class K, class V, class Alloc>
      int64_t BtreeRecyclePool<K, V, Alloc>::get_btree_node_num() const
      {
        return btree_node_num_;
      }

      template<class K, class V, class Alloc>
      int64_t BtreeRecyclePool<K, V, Alloc>::get_reserved_size() const
      {
        return btree_node_num_ * node_alloc_.get_block_size()
               + recycle_node_num_ * recycle_alloc_.get_block_size();
      }

      template<class K, class V, class Alloc>
      int32_t  BtreeRecyclePool<K, V, Alloc>::do_free_(int64_t max_reserved_num)
      {
        int32_t ret = ERROR_CODE_OK;
        int counter = 0;
        while (UNLIKELY(ERROR_CODE_OK == ret
                        && (-1 == max_reserved_num
                            || node_alloc_.get_reserved_block_num()
                               < max_reserved_num)
                        && NULL != oldest_recycle_node_
                        && oldest_recycle_node_ != cur_recycle_node_
                        && oldest_recycle_node_->get_ref() == 0
                        && oldest_recycle_node_->get_next() != NULL))
        {
          CMBTREE_CHECK_TRUE(oldest_recycle_node_->get_ref() >= 0,
              "Fatal error: reference count is less than 0");
          oldest_recycle_node_->set_invalid();
          if (UNLIKELY(oldest_recycle_node_->get_ref() != 0))
          {
            oldest_recycle_node_->set_valid();
          }
          else
          {
            counter++;
            int64_t node_num = oldest_recycle_node_->get_node_num();
            ret = free_node_(oldest_recycle_node_);
            if (UNLIKELY(ERROR_CODE_OK != ret))
            {
              CMBTREE_LOG(ERROR, "free_node_ error, err = %d", ret);
            }
            else
            {
              CMBTREE_ATOMIC_SUB(&btree_node_num_, node_num);
              TRecycleNode * to_be_freed = oldest_recycle_node_;
              oldest_recycle_node_ = oldest_recycle_node_->get_next();
              CMBTREE_ATOMIC_DEC(&recycle_node_num_);
              to_be_freed->~BtreeRecycleNode<K, V>();
              recycle_alloc_.free_block(to_be_freed);
            }
          }
        }
        if (UNLIKELY(-1 == max_reserved_num && ERROR_CODE_OK == ret))
        {
          if (NULL != cur_recycle_node_)
          {
            CMBTREE_ATOMIC_SUB(&btree_node_num_, cur_recycle_node_->get_node_num());
            free_node_(const_cast<TRecycleNode *>(cur_recycle_node_));
            CMBTREE_ATOMIC_DEC(&recycle_node_num_);
            cur_recycle_node_->~BtreeRecycleNode<K, V>();
            recycle_alloc_.free_block(const_cast<TRecycleNode *>(cur_recycle_node_));
            cur_recycle_node_ = oldest_recycle_node_ = NULL;
          }
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      int32_t  BtreeRecyclePool<K, V, Alloc>::free_node_(TRecycleNode * node)
      {
        int32_t ret = ERROR_CODE_OK;
        TBaseNode * p = node->get_node_list();
        while (NULL != p)
        {
          TBaseNode * next = p->get_next();
          if (p->is_leaf())
          {
            reinterpret_cast<TLeafNode *>(p)->~BtreeLeafNode<K, V>();
          }
          else
          {
            reinterpret_cast<TInterNode *>(p)->~BtreeInterNode<K, V>();
          }
          node_alloc_.free_block(p);
          p = next;
        }
        return ret;
      }

    } // end namespace cmbtree
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_CMBTREE_BTREE_RECYCLE_POOL_H_
