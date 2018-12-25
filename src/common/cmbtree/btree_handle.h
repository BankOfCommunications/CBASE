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
 *     - Handler used for manipulating Btree
 */
#ifndef OCEANBASE_COMMON_CMBTREE_BTREE_HANDLE_H_
#define OCEANBASE_COMMON_CMBTREE_BTREE_HANDLE_H_

#include "btree_define.h"
#include "btree_recycle_node.h"
#include "btree_recycle_pool.h"

namespace oceanbase
{
  namespace common
  {
    namespace cmbtree
    {
      template<class K, class V>
      class BtreeBaseHandle
      {
        public:
          typedef BtreeRecycleNode<K, V>        TRecycleNode;
        public:
          BtreeBaseHandle();
          virtual ~BtreeBaseHandle();

          /**
           * @brief 设置RecycleNode
           */
          int32_t  set_recycle_node(TRecycleNode * node);

          /**
           * @brief 减引用计数
           */
          void     release_recycle_node();

        protected:
          TRecycleNode * recycle_node_;      /// RecycleNode
      };

      template<class K, class V>
      class BtreeGetHandle : public BtreeBaseHandle<K, V>
      {
      };

      template<class K, class V, class Alloc>
      class BtreePutHandle : public BtreeBaseHandle<K, V>
      {
        public:
          typedef BtreeRecyclePool<K, V, Alloc> TRecyclePool;
        public:
          BtreePutHandle();
          virtual ~BtreePutHandle();

          int32_t  set_recycle_pool    (TRecyclePool * recycle_pool);

        protected:
          TRecyclePool * recycle_pool_;
      };

      template<class B>
      class BtreeScanHandle
        : public BtreeBaseHandle<typename B::TKey, typename B::TValue>
      {
          friend class
            BtreeBase<typename B::TKey, typename B::TValue, typename B::TAlloc>;
        public:
          typedef BtreeBaseNode<typename B::TKey, typename B::TValue> TBaseNode;
        public:
          BtreeScanHandle();
          virtual ~BtreeScanHandle();
          void     reset();

        protected:
          typename B::TKey    start_key_, end_key_;
          int32_t             start_exclude_, end_exclude_;
          TBaseNode *         node_[CONST_MAX_DEPTH];      /// 路径上的节点
          int16_t             node_pos_[CONST_MAX_DEPTH];  /// 路将上的节点内位置
          bool                forcast_compare_[CONST_MAX_DEPTH];
          int16_t             node_length_;                /// 节点的个数
          int64_t             found_;
          bool                begin_flag_;                 /// 刚刚开始的标志
          bool                finish_flag_;                /// 扫描结束标志
          bool                forward_direction_;          /// 是否前向扫描
          bool                init_;                       /// 是否初始化过
          bool                is_set_;                     /// 是否设置过扫描范围
      };

      template<class K, class V>
      BtreeBaseHandle<K, V>::BtreeBaseHandle()
        : recycle_node_(NULL)
      {
      }

      template<class K, class V>
      BtreeBaseHandle<K, V>::~BtreeBaseHandle()
      {
        release_recycle_node();
      }

      template<class K, class V>
      int32_t BtreeBaseHandle<K, V>::set_recycle_node(
          TRecycleNode * node)
      {
        int32_t ret = ERROR_CODE_OK;
        if (UNLIKELY(NULL == node))
        {
          CMBTREE_LOG(ERROR, "set_recycle_node node is null");
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          recycle_node_ = node;
        }
        return ret;
      }

      template<class K, class V>
      void BtreeBaseHandle<K, V>::release_recycle_node()
      {
        if (NULL != recycle_node_)
        {
          recycle_node_->dec_ref();
          recycle_node_ = NULL;
        }
      }

      template<class K, class V, class Alloc>
      BtreePutHandle<K, V, Alloc>::BtreePutHandle()
          : recycle_pool_(NULL)
      {
      }

      template<class K, class V, class Alloc>
      BtreePutHandle<K, V, Alloc>::~BtreePutHandle()
      {
        BtreeBaseHandle<K, V>::release_recycle_node();
        if (NULL != recycle_pool_)
        {
          int32_t err = recycle_pool_->free_recycle_node();
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "free_recycle_node error, err = %d", err);
          }
        }
      }

      template<class K, class V, class Alloc>
      int32_t BtreePutHandle<K, V, Alloc>::set_recycle_pool(TRecyclePool * recycle_pool)
      {
        int32_t ret = ERROR_CODE_OK;
        if (UNLIKELY(NULL == recycle_pool))
        {
          CMBTREE_LOG(ERROR, "set_recycle_pool recycle_pool is null");
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          recycle_pool_ = recycle_pool;
        }
        return ret;
      }

      template<class B>
      BtreeScanHandle<B>::BtreeScanHandle()
        : node_length_(0), init_(false), is_set_(false)
      {
      }

      template<class B>
      BtreeScanHandle<B>::~BtreeScanHandle()
      {
      }

      template<class B>
      void BtreeScanHandle<B>::reset()
      {
        this->release_recycle_node();
        init_   = false;
        is_set_ = false;
      }

    } // end namespace cmbtree
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_CMBTREE_BTREE_HANDLE_H_
