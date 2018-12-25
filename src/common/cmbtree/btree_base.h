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
 *     - CMBtree (Concurrently Modifiable B+ Tree) Implementation
 */
#ifndef OCEANBASE_COMMON_CMBTREE_BTREE_BASE_H_
#define OCEANBASE_COMMON_CMBTREE_BTREE_BASE_H_

#include "btree_define.h"
#include "btree_handle.h"
#include "btree_node.h"
#include "btree_recycle_node.h"
#include "btree_recycle_pool.h"
#include "btree_read_param.h"
#include "btree_write_param.h"
#include "btree_mem_pool.h"
#include "btree_counter.h"

namespace oceanbase
{
  namespace common
  {
    namespace cmbtree
    {
      /**
       * BTree 基础类, 对BTree结构的创建, 搜索, 以及copy on write
       */
      template<class K, class V, class Alloc>
      class BtreeBase
      {
        public:
          typedef K                             TKey;
          typedef V                             TValue;
          typedef Alloc                         TAlloc;
          typedef BtreeBaseNode<K, V>           TBaseNode;
          typedef BtreeInterNode<K, V>          TInterNode;
          typedef BtreeLeafNode<K, V>           TLeafNode;
          typedef BtreeWriteParam<K, V>         TWriteParam;
          typedef BtreeRecycleNode<K, V>        TRecycleNode;
          typedef BtreeRecyclePool<K, V, Alloc> TRecyclePool;
          typedef BtreeBaseHandle<K, V>         TBaseHandle;
          typedef BtreeGetHandle<K, V>          TGetHandle;
          typedef BtreePutHandle<K, V, Alloc>   TPutHandle;
          typedef BtreeScanHandle<BtreeBase<K, V, Alloc> > TScanHandle;
          typedef BtreeMemPool<K, V, Alloc>     TMemPool;
          typedef BtreeCounter<Alloc>           TCounter;

        public:
          /**
           * 构造函数
           */
          BtreeBase();

          BtreeBase(Alloc & alloc);

          /**
           * 析构函数
           */
          virtual ~BtreeBase();

          /**
           * 把<key, value>加到btree中
           *
           * @param   key     put的主键
           * @param   value   put的值
           * @param   overwrite 是否覆盖
           * @return  OK:     成功,  ERROR_CODE_KEY_REPEAT: key已经存在了
           */
          int32_t  put                           (const K & key, const V & value,
                                                  const bool overwrite = false);

          /**
           * 获得key对应的value
           * 如果key不存在，返回错误吗ERROR_CODE_NOT_FOUND
           */
          int32_t  get                           (const K & key, V & value);

          int32_t  get_min_key                   (K & key);

          int32_t  get_max_key                   (K & key);

          int32_t  get_scan_handle               (TScanHandle & handle);

          int32_t  set_key_range                 (TScanHandle & handle,
                                                  const K & start_key,
                                                  int32_t start_exclude,
                                                  const K & end_key,
                                                  int32_t end_exclude);

          int32_t  get_next                      (TScanHandle & handle,
                                                  K & key,
                                                  V & value);

          int64_t  get_object_count              ();

          int64_t  get_alloc_memory              ();

          /**
           * @brief Reserved memory by CMBtree
           */
          int64_t  get_reserved_memory           ();

          /**
           * 清除Btree已有的数据
           * <b>注意：这个函数同时只能由一个线程来调用，多线程不安全。
           * 同时不允许还有其他读操作和写操作。</b>
           */
          void     clear                         ();

          int32_t  init                          ();

          int32_t  reset                         ();

          /**
           * 释放Btree占用的所有内存
           * <b>注意：这个函数同时只能由一个线程来调用，多线程不安全。
           * 同时不允许还有其他读操作和写操作。</b>
           */
          void     destroy                       ();

          void     dump_mem_info                 ();

#ifdef CMBTREE_DEBUG
          /**
           * 打印出来.
           */
          void     print                         ();
#endif

        protected:

          /**
           * 得到Get操作Handle
           */
          int32_t  get_get_handle_               (TGetHandle & handle);

          /**
           * 得到Put操作Handle
           */
          int32_t  get_put_handle_               (TPutHandle & handle);

          /**
           * @brief 插入<key, value>对
           *
           * 执行步骤如图：
           *      put_pair_ ------------> put_pair_into_empty_tree_
           *        |        树为空时
           *        |
           *        | 树不为空时
           *        |
           *        V
           *      put_pair_into_leaf_node_
           *        |
           *        +------------------> put_pair_direct_into_leaf_node_
           *        |  叶子节点不满时
           *        |
           *        | 叶子节点满时
           *        |
           *        V
           *      split_leaf_node_and_put_pair_
           *        |
           *        V
           *   +->add_tri_into_inter_node_
           *   |    |
           *   |    +------------------> add_tri_direct_into_inter_node_
           *   |    |  中间节点不满时
           *   |    |
           *   |    | 中间节点满时
           *   |    |
           *   |    V
           *   +--split_inter_node_and_add_tri_
           */
          int32_t  put_pair_                     (const K & key, const V & value,
                                                  const bool overwrite,
                                                  TRecycleNode * recycle_node);

          /**
           * @brief 空树时，建立根节点并插入数据
           */
          int32_t  put_pair_into_empty_tree_     (const K & key, const V & value);

          /**
           * @brief 在非叶子节点的指定位置替换指针值
           */
          int32_t  set_ptr_into_inter_node_      (int16_t level,
                                                  const TBaseNode * node,
                                                  TWriteParam & param);
          /**
           * @brief 分裂非叶子节点并且插入新值
           */
          int32_t  split_inter_node_and_add_tri_(TInterNode * inter,
                                                 int16_t level, int16_t pos,
                                                 const K & key,
                                                 const TBaseNode * node1,
                                                 const TBaseNode * node2,
                                                 TWriteParam & param,
                                                 TRecycleNode * recycle_node);
          /**
           * @brief 非叶子节点不满的时候直接插入新值
           */
          int32_t  add_tri_direct_into_inter_node_(TInterNode * inter,
                                                   int16_t level, int16_t pos,
                                                   const K & key,
                                                   const TBaseNode * node1,
                                                   const TBaseNode * node2,
                                                   TWriteParam & param,
                                                   TRecycleNode * recycle_node);
          /**
           * @brief 非叶子节点插入新值
           */
          int32_t  add_tri_into_inter_node_      (int16_t level,
                                                  const K & key,
                                                  const TBaseNode * node1,
                                                  const TBaseNode * node2,
                                                  TWriteParam & param,
                                                  TRecycleNode * recycle_node);
          /**
           * @brief 分裂叶子节点并且插入新值
           */
          int32_t  split_leaf_node_and_put_pair_ (TLeafNode * leaf,
                                                  int16_t level, int16_t pos,
                                                  const K & key, const V & value,
                                                  TWriteParam & param,
                                                  TRecycleNode * recycle_node);
          /**
           * @brief 叶子节点不满的时候直接插入新值
           */
          int32_t  put_pair_direct_into_leaf_node_(TLeafNode * leaf,
                                                   int16_t level, int16_t pos,
                                                   const K & key, const V & value,
                                                   TWriteParam & param,
                                                   TRecycleNode * recycle_node);
          /**
           * @brief 叶子节点插入新值
           */
          int32_t  put_pair_into_leaf_node_      (const K & key,
                                                  const V & value,
                                                  TWriteParam & param,
                                                  TRecycleNode * recycle_node);
          /**
           * @brief 搜索路径
           */
          int32_t  get_children_                 (const K & key,
                                                  TWriteParam & param);

          /**
           * @brief 将param里所有节点使用shared_unlock函数解锁
           */
          int32_t  shared_unlock_param_          (TWriteParam & param);

          /**
           * @brief 释放以node为根的子树
           */
          void     destroy_                      (TBaseNode * node);

          int32_t  get_start_path_               (TScanHandle & handle);

          int32_t  step_forward_                 (TScanHandle & handle,
                                                  int16_t level);

          int32_t  step_backward_                (TScanHandle & handle,
                                                  int16_t level);

        private:
          CMBTREE_DISALLOW_COPY_AND_ASSIGN(BtreeBase);

        protected:
          TBaseNode * volatile root_;     /// Root节点
          BtreeDefaultAlloc   default_alloc_;     /// 默认内存分配器
          Alloc &             alloc_;
          TMemPool            node_alloc_;        /// Btree内存分配器
          TMemPool            recycle_alloc_;     /// 回收池内存分配器
          TRecyclePool        recycle_pool_;
          TCounter            object_counter_;
          bool                init_;              /// 初始化标记
      };

      template<class K, class V, class Alloc>
      BtreeBase<K, V, Alloc>::BtreeBase()
          : root_(NULL),
            alloc_(default_alloc_),
            node_alloc_(std::max(sizeof(TLeafNode),
                                 sizeof(TInterNode)),
                        alloc_),
            recycle_alloc_(sizeof(TRecycleNode), alloc_),
            recycle_pool_(node_alloc_, recycle_alloc_),
            object_counter_(),
            init_(false)
      {
      }

      template<class K, class V, class Alloc>
      BtreeBase<K, V, Alloc>::BtreeBase(Alloc & alloc)
          : root_(NULL),
            alloc_(alloc),
            node_alloc_(std::max(sizeof(TLeafNode),
                                 sizeof(TInterNode)),
                        alloc_),
            recycle_alloc_(sizeof(TRecycleNode), alloc_),
            recycle_pool_(node_alloc_, recycle_alloc_),
            object_counter_(),
            init_(false)
      {
      }

      template<class K, class V, class Alloc>
      BtreeBase<K, V, Alloc>::~BtreeBase()
      {
        //destroy_(root_);
        //root_ = NULL;
        destroy();
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::put(const K & key, const V & value,
          const bool overwrite/* = false*/)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;
        if (UNLIKELY(!init_))
        {
          CMBTREE_LOG(ERROR, "Btree has not been initialized");
          ret = ERROR_CODE_FAIL;
        }
        else
        {
          TPutHandle handle;
          err = get_put_handle_(handle);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "get_put_handle error, err = %d", err);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            TRecycleNode * recycle_node
                = new (recycle_alloc_.alloc_block_with_initial_memset())
                  TRecycleNode();
            if (UNLIKELY(NULL == recycle_node))
            {
              CMBTREE_LOG(ERROR, "alloc TRecycleNode failed");
              ret = ERROR_CODE_ALLOC_FAIL;
            }
            else
            {
              do
              {
                ret = put_pair_(key, value, overwrite, recycle_node);
              }
              while (UNLIKELY(ERROR_CODE_NEED_RETRY == ret));
              err = recycle_pool_.add_recycle_node(recycle_node);
              if (UNLIKELY(ERROR_CODE_OK != err))
              {
                CMBTREE_LOG(ERROR, "add_recycle_node error, err = %d", err);
              }
            }
          }
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::get(const K & key, V & value)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;
        if (UNLIKELY(!init_))
        {
          CMBTREE_LOG(ERROR, "Btree has not been initialized");
          ret = ERROR_CODE_FAIL;
        }
        else
        {
          TGetHandle handle;
          err = get_get_handle_(handle);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "get_get_handle error, err = %d", err);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            TBaseNode * proot = NULL;
            int16_t pos = 0;
            int64_t found = 0;
            do
            {
              if (UNLIKELY(NULL == proot))
              {
                if (UNLIKELY(NULL == root_))
                {
                  ret = ERROR_CODE_NOT_FOUND;
                }
                else
                {
                  proot = root_;
                }
              }
              else
              {
                TInterNode * last_node = reinterpret_cast<TInterNode *>(proot);
                err = last_node->get_ptr(pos, proot);
                if (UNLIKELY(ERROR_CODE_OK != err))
                {
                  CMBTREE_LOG(ERROR, "get_ptr error, err = %d, pos = %d",
                      err, pos);
                  ret = ERROR_CODE_NOT_EXPECTED;
                }
              }
              if (LIKELY(ERROR_CODE_OK == ret))
              {
                err = proot->find_pos(key, pos, found);
                if (UNLIKELY(ERROR_CODE_OK != err))
                {
                  CMBTREE_LOG(ERROR, "find_pos return error, err = %d, "
                      "pos = %d, found = %ld", err, pos, found);
                  ret = ERROR_CODE_NOT_EXPECTED;
                }
                else if (LIKELY(!proot->is_leaf()))
                {
                  // pos+1是所需要找寻的子树的根节点位置
                  ++pos;
                }
              }
            }
            while (ERROR_CODE_OK == ret && !proot->is_leaf());
            if (LIKELY(ERROR_CODE_OK == ret))
            {
              if (found != 0)
              {
                ret = ERROR_CODE_NOT_FOUND;
              }
              else
              {
                err = reinterpret_cast<TLeafNode *>(proot)->get_value(pos, value);
                if (UNLIKELY(ERROR_CODE_OK != err))
                {
                  CMBTREE_LOG(ERROR, "get_value error, err = %d pos = %d",
                      err, pos);
                  ret = ERROR_CODE_NOT_EXPECTED;
                }
              }
            }
          }
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::get_min_key(K & key)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;
        if (UNLIKELY(!init_))
        {
          CMBTREE_LOG(ERROR, "Btree has not been initialized");
          ret = ERROR_CODE_FAIL;
        }
        else
        {
          TGetHandle handle;
          err = get_get_handle_(handle);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "get_get_handle error, err = %d", err);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            TBaseNode * proot = NULL;
            do
            {
              if (UNLIKELY(NULL == proot))
              {
                if (UNLIKELY(NULL == root_))
                {
                  ret = ERROR_CODE_NOT_FOUND;
                }
                else
                {
                  proot = root_;
                }
              }
              else
              {
                TInterNode * last_node = reinterpret_cast<TInterNode *>(proot);
                err = last_node->get_ptr(0, proot);
                if (UNLIKELY(ERROR_CODE_OK != err))
                {
                  CMBTREE_LOG(ERROR, "get_ptr error, err = %d", err);
                  ret = ERROR_CODE_NOT_EXPECTED;
                }
              }
            }
            while (ERROR_CODE_OK == ret && !proot->is_leaf());
            if (LIKELY(ERROR_CODE_OK == ret))
            {
              err = proot->get_key(0, key);
              if (UNLIKELY(ERROR_CODE_OK != err))
              {
                CMBTREE_LOG(ERROR, "get_key error, err = %d", err);
                ret = ERROR_CODE_NOT_EXPECTED;
              }
            }
          }
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::get_max_key(K & key)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;
        if (UNLIKELY(!init_))
        {
          CMBTREE_LOG(ERROR, "Btree has not been initialized");
          ret = ERROR_CODE_FAIL;
        }
        else
        {
          TGetHandle handle;
          err = get_get_handle_(handle);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "get_get_handle error, err = %d", err);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            TBaseNode * proot = NULL;
            do
            {
              if (UNLIKELY(NULL == proot))
              {
                if (UNLIKELY(NULL == root_))
                {
                  ret = ERROR_CODE_NOT_FOUND;
                }
                else
                {
                  proot = root_;
                }
              }
              else
              {
                TInterNode * last_node = reinterpret_cast<TInterNode *>(proot);
                err = last_node->get_ptr(proot->get_size(), proot);
                if (UNLIKELY(ERROR_CODE_OK != err))
                {
                  CMBTREE_LOG(ERROR, "get_ptr error, err = %d, get_size = %d",
                      err, proot->get_size());
                  ret = ERROR_CODE_NOT_EXPECTED;
                }
              }
            }
            while (ERROR_CODE_OK == ret && !proot->is_leaf());
            if (LIKELY(ERROR_CODE_OK == ret))
            {
              err = proot->get_key(static_cast<int16_t>(proot->get_size() - 1), key);
              if (UNLIKELY(ERROR_CODE_OK != err))
              {
                CMBTREE_LOG(ERROR, "get_key error, err = %d get_size = %d",
                    err, proot->get_size());
                ret = ERROR_CODE_NOT_EXPECTED;
              }
            }
          }
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::get_scan_handle(TScanHandle & handle)
      {
        int32_t ret = ERROR_CODE_OK;
        if (UNLIKELY(!init_))
        {
          CMBTREE_LOG(ERROR, "Btree has not been initialized");
          ret = ERROR_CODE_FAIL;
        }
        else
        {
          if (UNLIKELY(handle.init_))
          {
            CMBTREE_LOG(ERROR, "handle has been initialized");
            ret = ERROR_CODE_FAIL;
          }
          else
          {
            ret = handle.set_recycle_node(
                recycle_pool_.inc_ref_and_get_cur_recycle_node());
            if (UNLIKELY(ERROR_CODE_OK != ret))
            {
              CMBTREE_LOG(ERROR, "acquire_recycle_node error");
            }
            else
            {
              handle.init_ = true;
            }
          }
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::set_key_range(TScanHandle & handle,
          const K & start_key, int32_t start_exclude,
          const K & end_key, int32_t end_exclude)
      {
        int ret = ERROR_CODE_OK;
        if (UNLIKELY(!init_))
        {
          CMBTREE_LOG(ERROR, "Btree has not been initialized");
          ret = ERROR_CODE_FAIL;
        }
        else
        {
          if (UNLIKELY(!handle.init_))
          {
            CMBTREE_LOG(ERROR, "handle has not been initialized");
            ret = ERROR_CODE_FAIL;
          }
          else
          {
            handle.start_key_     = start_key;
            handle.end_key_       = end_key;
            handle.start_exclude_ = start_exclude;
            handle.end_exclude_   = end_exclude;
            handle.node_length_   = 0;
            handle.found_         = 0;
            handle.begin_flag_    = true;
            handle.finish_flag_   = false;
            handle.forward_direction_ = (start_key - end_key <= 0);
            handle.is_set_        = true;
            int err = get_start_path_(handle);
            if (UNLIKELY(ERROR_CODE_OK != err))
            {
              CMBTREE_LOG(ERROR, "get_start_path_ error, err = %d", err);
              ret = ERROR_CODE_NOT_EXPECTED;
            }
          }
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::get_next(TScanHandle & handle, K & key, V & value)
      {
        int ret = ERROR_CODE_OK;
        int err = ERROR_CODE_OK;
        bool got = false;
        if (UNLIKELY(!init_))
        {
          CMBTREE_LOG(ERROR, "Btree has not been initialized");
          ret = ERROR_CODE_FAIL;
        }
        else if (UNLIKELY(!handle.is_set_))
        {
          CMBTREE_LOG(ERROR, "handle has not been set");
          ret = ERROR_CODE_FAIL;
        }
        else
        {
          if (UNLIKELY(handle.begin_flag_))
          {
            handle.begin_flag_ = false;
            if ((handle.found_ == 0 && !handle.start_exclude_)
                || handle.found_ != 0)
            {
              int16_t index = static_cast<int16_t>(handle.node_length_ - 1);
              TLeafNode * leaf
                  = reinterpret_cast<TLeafNode *>(handle.node_[index]);
              int16_t pos = handle.node_pos_[index];
              err = leaf->get_key(pos, key);
              if (UNLIKELY(ERROR_CODE_OK != err))
              {
                CMBTREE_LOG(ERROR, "get_key error, err = %d pos = %d",
                    err, pos);
                ret = ERROR_CODE_NOT_EXPECTED;
              }
              else if (handle.forward_direction_)
              {
                if (handle.forcast_compare_[index]
                    || key - handle.end_key_ < 0
                    || (handle.end_key_ - key == 0 && !handle.end_exclude_))
                {
                  err = leaf->get_value(pos, value);
                  if (UNLIKELY(ERROR_CODE_OK != err))
                  {
                    CMBTREE_LOG(ERROR, "get_value error, err = %d pos = %d",
                        err, pos);
                    ret = ERROR_CODE_NOT_EXPECTED;
                  }
                }
                else
                {
                  handle.finish_flag_ = true;
                  ret = ERROR_CODE_NOT_FOUND;
                }
              }
              else
              {
                if (/*handle.forcast_compare_[index]
                    || */key - handle.end_key_ > 0
                    || (handle.end_key_ - key == 0 && !handle.end_exclude_))
                {
                  leaf->get_value(pos, value);
                  if (UNLIKELY(ERROR_CODE_OK != err))
                  {
                    CMBTREE_LOG(ERROR, "get_value error, err = %d pos = %d",
                        err, pos);
                    ret = ERROR_CODE_NOT_EXPECTED;
                  }
                }
                else
                {
                  handle.finish_flag_ = true;
                  ret = ERROR_CODE_NOT_FOUND;
                }
              }
              got = true;
            }
          }
          if (LIKELY(!got))
          {
            if (handle.forward_direction_)
            {
              err = step_forward_(handle, static_cast<int16_t>(handle.node_length_ - 1));
            }
            else
            {
              err = step_backward_(handle, static_cast<int16_t>(handle.node_length_ - 1));
            }
            if (UNLIKELY(ERROR_CODE_NOT_FOUND == err))
            {
              ret = err;
            }
            else if (UNLIKELY(ERROR_CODE_OK != err))
            {
              CMBTREE_LOG(ERROR, "step_forward_ error");
              ret = ERROR_CODE_NOT_EXPECTED;
            }
            else if (handle.finish_flag_)
            {
              ret = ERROR_CODE_NOT_FOUND;
            }
            else
            {
              int16_t index = static_cast<int16_t>(handle.node_length_ - 1);
              TLeafNode * leaf =
                  reinterpret_cast<TLeafNode *>(handle.node_[index]);
              int16_t pos = handle.node_pos_[index];
              err = leaf->get_key(pos, key);
              if (UNLIKELY(ERROR_CODE_OK != err))
              {
                CMBTREE_LOG(ERROR, "get_key error, err = %d pos = %d",
                    err, pos);
                ret = ERROR_CODE_NOT_EXPECTED;
              }
              else if (handle.forward_direction_)
              {
                if (handle.forcast_compare_[index]
                    || key - handle.end_key_ < 0
                    || (handle.end_key_ - key == 0 && !handle.end_exclude_))
                {
                  leaf->get_value(pos, value);
                  if (UNLIKELY(ERROR_CODE_OK != err))
                  {
                    CMBTREE_LOG(ERROR, "get_value error, err = %d pos = %d",
                        err, pos);
                    ret = ERROR_CODE_NOT_EXPECTED;
                  }
                }
                else
                {
                  handle.finish_flag_ = true;
                  ret = ERROR_CODE_NOT_FOUND;
                }
              }
              else
              {
                if (handle.forcast_compare_[index]
                    || key - handle.end_key_ > 0
                    || (handle.end_key_ - key == 0 && !handle.end_exclude_))
                {
                  leaf->get_value(pos, value);
                  if (UNLIKELY(ERROR_CODE_OK != err))
                  {
                    CMBTREE_LOG(ERROR, "get_value error, err = %d pos = %d",
                        err, pos);
                    ret = ERROR_CODE_NOT_EXPECTED;
                  }
                }
                else
                {
                  handle.finish_flag_ = true;
                  ret = ERROR_CODE_NOT_FOUND;
                }
              }
            }
          }
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      int64_t BtreeBase<K, V, Alloc>::get_object_count()
      {
        int64_t ret = 0;
        if (UNLIKELY(!init_))
        {
          CMBTREE_LOG(ERROR, "Btree has not been initialized");
          ret = 0;
        }
        else
        {
          ret = object_counter_.value();
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      int64_t BtreeBase<K, V, Alloc>::get_alloc_memory()
      {
        return node_alloc_.get_alloced_size() + recycle_alloc_.get_alloced_size();
      }

      template<class K, class V, class Alloc>
      int64_t BtreeBase<K, V, Alloc>::get_reserved_memory()
      {
        return node_alloc_.get_reserved_size()
               + recycle_alloc_.get_reserved_size()
               + recycle_pool_.get_reserved_size();
      }

      template<class K, class V, class Alloc>
      void BtreeBase<K, V, Alloc>::clear()
      {
        if (UNLIKELY(!init_))
        {
          CMBTREE_LOG(ERROR, "Btree has not been initialized");
        }
        else
        {
          destroy_(root_);
          root_ = NULL;
          object_counter_.reset();
        }
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::init()
      {
        int32_t ret = ERROR_CODE_OK;
        if (init_)
        {
          CMBTREE_LOG(ERROR, "Btree has been initialized");
          ret = ERROR_CODE_FAIL;
        }
        else if (ERROR_CODE_OK != (ret = node_alloc_.init()))
        {
          CMBTREE_LOG(ERROR, "node_alloc_ init error, ret = %d", ret);
        }
        else if (ERROR_CODE_OK != (ret = recycle_alloc_.init()))
        {
          CMBTREE_LOG(ERROR, "recycle_alloc_ init error, ret = %d", ret);
          node_alloc_.destroy();
        }
        else if (ERROR_CODE_OK != (ret = object_counter_.init()))
        {
          CMBTREE_LOG(ERROR, "object_counter_ init error, ret = %d", ret);
          recycle_alloc_.destroy();
          node_alloc_.destroy();
        }
        else if (ERROR_CODE_OK != (ret = recycle_pool_.init()))
        {
          CMBTREE_LOG(ERROR, "recycle_pool_ init error, ret = %d", ret);
          object_counter_.destroy();
          recycle_alloc_.destroy();
          node_alloc_.destroy();
        }
        else
        {
          init_ = true;
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::reset()
      {
        int32_t ret = ERROR_CODE_OK;
        if (UNLIKELY(!init_))
        {
          CMBTREE_LOG(ERROR, "Btree has not been initialized");
          ret = ERROR_CODE_FAIL;
        }
        else
        {
          destroy();
          int32_t err = init();
          if (ERROR_CODE_OK != err)
          {
            CMBTREE_LOG(ERROR, "init error, err = %d", err);
            ret = err;
          }
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      void BtreeBase<K, V, Alloc>::destroy()
      {
        if (init_)
        {
          destroy_(root_);
          root_ = NULL;
          object_counter_.destroy();
          recycle_pool_.destroy();
          node_alloc_.destroy();
          recycle_alloc_.destroy();
          init_ = false;
        }
      }

      template<class K, class V, class Alloc>
      void BtreeBase<K, V, Alloc>::dump_mem_info()
      {
        int64_t recycle_node_num = recycle_pool_.get_recycle_node_num();
        int64_t btree_node_num = recycle_pool_.get_btree_node_num();
        CMBTREE_LOG(TRACE, "    [CMBTree Info] Recycle Pool recycle_node_num=%ld "
            "recycle_node_size=%ld btree_node_num=%ld btree_node_size=%ld",
            recycle_node_num, recycle_node_num * recycle_alloc_.get_block_size(),
            btree_node_num, btree_node_num * node_alloc_.get_block_size());
        CMBTREE_LOG(TRACE, "    [CMBTree Info] ==========Recycle Node Used Info Start==========");
        recycle_alloc_.dump_thread_mem_info();
        CMBTREE_LOG(TRACE, "    [CMBTree Info] ==========Recycle Node Used Info End  ==========");
        CMBTREE_LOG(TRACE, "    [CMBTree Info] ==========BTree Node Used Info Start==========");
        node_alloc_.dump_thread_mem_info();
        CMBTREE_LOG(TRACE, "    [CMBTree Info] ==========BTree Node Used Info End  ==========");
      }

#ifdef CMBTREE_DEBUG
      template<class K, class V, class Alloc>
      void BtreeBase<K, V, Alloc>::print()
      {
        if (NULL != root_) root_->print(0);
      }
#endif

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::get_get_handle_(TGetHandle & handle)
      {
        int32_t ret = ERROR_CODE_OK;
        ret = handle.set_recycle_node(
            recycle_pool_.inc_ref_and_get_cur_recycle_node());
        if (UNLIKELY(ERROR_CODE_OK != ret))
        {
          CMBTREE_LOG(ERROR, "acquire_recycle_node error");
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::get_put_handle_(TPutHandle & handle)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;
        ret = handle.set_recycle_node(
            recycle_pool_.inc_ref_and_get_cur_recycle_node());
        if (UNLIKELY(ERROR_CODE_OK != ret))
        {
          CMBTREE_LOG(ERROR, "acquire_recycle_node error");
        }
        else
        {
          err = handle.set_recycle_pool(&recycle_pool_);
          if (ERROR_CODE_OK != err)
          {
            CMBTREE_LOG(ERROR, "set_recycle_pool error, err = %d", err);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::put_pair_(const K & key,
          const V & value, const bool overwrite, TRecycleNode * recycle_node)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;

        if (UNLIKELY(NULL == root_))
        {
          ret = put_pair_into_empty_tree_(key, value);
          if (LIKELY(ERROR_CODE_OK == ret))
          {
            object_counter_.inc();
          }
        }
        else
        {
          TWriteParam param;
          err = get_children_(key, param);
          if (UNLIKELY(ERROR_CODE_NEED_RETRY == err))
          {
            ret = ERROR_CODE_NEED_RETRY;
          }
          else if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "get_children_ error, err=%d", err);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            if (UNLIKELY(param.found_ == 0 && !overwrite))
            {
              CMBTREE_LOG(ERROR, "key already existed");
              ret = ERROR_CODE_KEY_REPEAT;
            }
            else
            {
              ret = put_pair_into_leaf_node_(key, value, param, recycle_node);
            }
            if (UNLIKELY(ERROR_CODE_OK != ret))
            {
              // 失败了，恢复已经加锁的节点
              shared_unlock_param_(param);
            }
          }

          if (LIKELY(ERROR_CODE_OK == ret && 0 != param.found_))
          {
            object_counter_.inc();
          }
        }

        return ret;
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::put_pair_into_empty_tree_(const K & key,
          const V & value)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;

        TLeafNode * new_root = new (node_alloc_.alloc_block()) TLeafNode();
        if (UNLIKELY(NULL == new_root))
        {
          CMBTREE_LOG(ERROR, "alloc root node error");
          ret = ERROR_CODE_ALLOC_FAIL;
        }
        else
        {
          err = new_root->add_pair(0, key, value);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "add_pair return error, err = %d", err);
            new_root->~BtreeLeafNode<K, V>();
            node_alloc_.free_block(new_root);
            new_root = NULL;
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            if (NULL == CMBTREE_ATOMIC_CAS(&root_, NULL, new_root))
            {
              //CMBTREE_LOG(INFO, "assign a new root node %p", new_root);
            }
            else
            {
              CMBTREE_LOG(DEBUG, "CMBTREE_ATOMIC_CAS collision");
              new_root->~BtreeLeafNode<K, V>();
              node_alloc_.free_block(new_root);
              new_root = NULL;
              ret = ERROR_CODE_NEED_RETRY;
            }
          }
        }

        return ret;
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::set_ptr_into_inter_node_(int16_t level,
          const TBaseNode * node, TWriteParam & param)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;

        // level是-1表示更新root_指针
        if (UNLIKELY(level < -1 && level >= param.node_length_))
        {
          CMBTREE_LOG(ERROR, "level is error, level = %d", level);
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else if (LIKELY(level != -1))
        {
          TBaseNode * parent = param.node_[level];
          if (UNLIKELY(parent->is_leaf()))
          {
            CMBTREE_LOG(ERROR, "node should not be leaf, "
                "level = %d, param.node_length_ = %d", level, param.node_length_);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            TInterNode * inter = reinterpret_cast<TInterNode *>(parent);
            err = inter->set_ptr(param.node_pos_[level], node);
            if (UNLIKELY(ERROR_CODE_OK != err))
            {
              CMBTREE_LOG(ERROR, "set_ptr error, err = %d, level = %d "
                  "node_pos_ = %d", err, level, param.node_pos_[level]);
              ret = ERROR_CODE_NOT_EXPECTED;
            }
          }
        }
        else
        {
          if (param.node_[0] == CMBTREE_ATOMIC_CAS(&root_, param.node_[0], node))
          {
            //CMBTREE_LOG(INFO, "assign a new root node %p", node);
          }
          else
          {
            CMBTREE_LOG(DEBUG, "CMBTREE_ATOMIC_CAS collision");
            ret = ERROR_CODE_NEED_RETRY;
          }
        }

        return ret;
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::split_inter_node_and_add_tri_(
          TInterNode * inter, int16_t level, int16_t pos,
          const K & key, const TBaseNode * node1, const TBaseNode * node2,
          TWriteParam & param, TRecycleNode * recycle_node)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;

        TInterNode * new_inter1 = new (node_alloc_.alloc_block()) TInterNode();
        TInterNode * new_inter2 = new (node_alloc_.alloc_block()) TInterNode();
        if (UNLIKELY(NULL == new_inter1 || NULL == new_inter2))
        {
          CMBTREE_LOG(ERROR, "alloc failed");
          ret = ERROR_CODE_ALLOC_FAIL;
        }
        else
        {
          err = inter->copy_left_part_and_add_tri(
              new_inter1, pos, key, node1, node2);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "copy_left_part_and_set(_add)_pair error, "
                "err = %d", err);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            err = inter->copy_right_part_and_add_tri(
                new_inter2, pos, key, node1, node2);
            if (UNLIKELY(ERROR_CODE_OK != err))
            {
              CMBTREE_LOG(ERROR, "copy_right_part_and_set(_add)_pair "
                  "error, err = %d", err);
            }
            else
            {
              K new_key;
              err = inter->get_middle_key_after_adding(pos, key, new_key);
              if (UNLIKELY(ERROR_CODE_OK != err))
              {
                CMBTREE_LOG(ERROR, "InterNode get_key error, err = %d", err);
                ret = ERROR_CODE_NOT_EXPECTED;
              }
              else
              {
                ret = add_tri_into_inter_node_(static_cast<int16_t>(level - 1),
                    new_key, new_inter1, new_inter2, param, recycle_node);
                if (LIKELY(ERROR_CODE_OK == ret))
                {
                  err = recycle_node->add_node(inter);
                  if (UNLIKELY(ERROR_CODE_OK != err))
                  {
                    CMBTREE_LOG(ERROR, "recycle_node.add_node error, "
                        "err = %d", err);
                    ret = ERROR_CODE_NOT_EXPECTED;
                  }
                }
                else
                {
                  err = recycle_node->add_node(new_inter1);
                  if (UNLIKELY(ERROR_CODE_OK != err))
                  {
                    CMBTREE_LOG(ERROR, "recycle_node.add_node error, "
                        "err = %d", err);
                    ret = ERROR_CODE_NOT_EXPECTED;
                  }
                  err = recycle_node->add_node(new_inter2);
                  if (UNLIKELY(ERROR_CODE_OK != err))
                  {
                    CMBTREE_LOG(ERROR, "recycle_node.add_node error, "
                        "err = %d", err);
                    ret = ERROR_CODE_NOT_EXPECTED;
                  }
                }
              }
            }
          }
        }

        return ret;
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::add_tri_direct_into_inter_node_(
          TInterNode * inter, int16_t level, int16_t pos,
          const K & key, const TBaseNode * node1, const TBaseNode * node2,
          TWriteParam & param, TRecycleNode * recycle_node)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;

        TInterNode * new_inter = new (node_alloc_.alloc_block()) TInterNode();
        if (UNLIKELY(NULL == new_inter))
        {
          CMBTREE_LOG(ERROR, "alloc failed");
          ret = ERROR_CODE_ALLOC_FAIL;
        }
        else
        {
          err = inter->clone_and_add_tri(new_inter, pos, key, node1, node2);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "clone_and_put_key error, err = %d", err);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            ret = set_ptr_into_inter_node_(static_cast<int16_t>(level - 1),
                new_inter, param);
            if (LIKELY(ERROR_CODE_OK == ret))
            {
              err = recycle_node->add_node(inter);
              if (UNLIKELY(ERROR_CODE_OK != err))
              {
                CMBTREE_LOG(ERROR, "recycle_node.add_node error, "
                    "err = %d", err);
                ret = ERROR_CODE_NOT_EXPECTED;
              }
              else
              {
                for (int16_t i = 0; i < level; i++)
                {
                  err = param.node_[i]->shared_unlock();
                  if (ERROR_CODE_OK != err)
                  {
                    CMBTREE_LOG(ERROR, "shared_unlock error, err = %d", err);
                  }
                }
              }
            }
            else
            {
              recycle_node->add_node(new_inter);
            }
          }
        }

        return ret;
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::add_tri_into_inter_node_(int16_t level,
          const K & key, const TBaseNode * node1, const TBaseNode * node2,
          TWriteParam & param, TRecycleNode * recycle_node)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;

        if (UNLIKELY(level < -1 || level >= param.node_length_))
        {
          CMBTREE_LOG(ERROR, "level is error, level = %d", level);
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else if (UNLIKELY(-1 == level))
        {
          TInterNode * new_root = NULL;
          new_root = new (node_alloc_.alloc_block()) TInterNode();
          if (UNLIKELY(NULL == new_root))
          {
            CMBTREE_LOG(ERROR, "alloc root node error");
            ret = ERROR_CODE_ALLOC_FAIL;
          }

          if (LIKELY(ERROR_CODE_OK == ret))
          {
            err = new_root->put_key(0, key, node1, node2);
            if (UNLIKELY(ERROR_CODE_OK != err))
            {
              CMBTREE_LOG(ERROR, "put_key error, err = %d", err);
              new_root->~BtreeInterNode<K, V>();
              node_alloc_.free_block(new_root);
              new_root = NULL;
              ret = ERROR_CODE_NOT_EXPECTED;
            }
          }

          if (LIKELY(ERROR_CODE_OK == ret))
          {
            if (param.node_[0] == CMBTREE_ATOMIC_CAS(&root_, param.node_[0], new_root))
            {
              //CMBTREE_LOG(INFO, "assign a new root node %p", new_root);
            }
            else
            {
              CMBTREE_LOG(DEBUG, "CMBTREE_ATOMIC_CAS collision");
              new_root->~BtreeInterNode<K, V>();
              node_alloc_.free_block(new_root);
              new_root = NULL;
              ret = ERROR_CODE_NEED_RETRY;
            }
          }
        }
        else
        {
          TBaseNode * parent = param.node_[level];
          err = parent->shared2exclusive_lock();
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(DEBUG, "put op collision");
            ret = ERROR_CODE_NEED_RETRY;
          }
          else if (UNLIKELY(parent->is_leaf()))
          {
            CMBTREE_LOG(ERROR, "node is not inter node, level = %d", level);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            int16_t pos = param.node_pos_[level];
            TInterNode * interp = reinterpret_cast<TInterNode *>(parent);
            if (UNLIKELY(interp->get_size() >= CONST_NODE_OBJECT_COUNT))
            {
              ret = split_inter_node_and_add_tri_(interp, level, pos,
                  key, node1, node2, param, recycle_node);
            }
            else
            {
              ret = add_tri_direct_into_inter_node_(interp, level, pos,
                  key, node1, node2, param, recycle_node);
            }
            if (UNLIKELY(ERROR_CODE_OK != ret))
            {
              err = parent->exclusive2shared_lock();
              if (UNLIKELY(ERROR_CODE_OK != err))
              {
                CMBTREE_LOG(ERROR, "exclusive2shared_lock error");
              }
            }
            else
            {
              err = parent->exclusive_unlock();
              if (UNLIKELY(ERROR_CODE_OK != err))
              {
                CMBTREE_LOG(ERROR, "exclusive_unlock error");
              }
            }
          }
        }

        return ret;
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::split_leaf_node_and_put_pair_(
          TLeafNode * leaf, int16_t level, int16_t pos,
          const K & key, const V & value,
          TWriteParam & param, TRecycleNode * recycle_node)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;

        TLeafNode * new_leaf1 = new (node_alloc_.alloc_block()) TLeafNode();
        TLeafNode * new_leaf2 = new (node_alloc_.alloc_block()) TLeafNode();
        if (UNLIKELY(NULL == new_leaf1 || NULL == new_leaf2))
        {
          CMBTREE_LOG(ERROR, "alloc failed");
          ret = ERROR_CODE_ALLOC_FAIL;
        }
        else
        {
          if (0 == param.found_)
          {
            err = leaf->copy_left_part_and_set_pair(
                new_leaf1, pos, key, value);
          }
          else
          {
            err = leaf->copy_left_part_and_add_pair(
                new_leaf1, pos, key, value);
          }
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "copy_left_part_and_set(_add)_pair error, "
                "err = %d", err);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            if (0 == param.found_)
            {
              err = leaf->copy_right_part_and_set_pair(
                  new_leaf2, pos, key, value);
            }
            else
            {
              err = leaf->copy_right_part_and_add_pair(
                  new_leaf2, pos, key, value);
            }
            if (UNLIKELY(ERROR_CODE_OK != err))
            {
              CMBTREE_LOG(ERROR, "copy_right_part_and_set(_add)_pair "
                  "error, err = %d", err);
            }
            else
            {
              K new_key;
              err = new_leaf2->get_key(0, new_key);
              if (UNLIKELY(ERROR_CODE_OK != err))
              {
                CMBTREE_LOG(ERROR, "LeafNode get_key error, err = %d", err);
                ret = ERROR_CODE_NOT_EXPECTED;
              }
              else
              {
                ret = add_tri_into_inter_node_(static_cast<int16_t>(level - 1),
						new_key, new_leaf1, new_leaf2, param, recycle_node);
                if (LIKELY(ERROR_CODE_OK == ret))
                {
                  err = recycle_node->add_node(leaf);
                  if (UNLIKELY(ERROR_CODE_OK != err))
                  {
                    CMBTREE_LOG(ERROR, "recycle_node.add_node error, "
                        "err = %d", err);
                    ret = ERROR_CODE_NOT_EXPECTED;
                  }
                }
                else
                {
                  err = recycle_node->add_node(new_leaf1);
                  if (UNLIKELY(ERROR_CODE_OK != err))
                  {
                    CMBTREE_LOG(ERROR, "recycle_node.add_node error, "
                        "err = %d", err);
                    ret = ERROR_CODE_NOT_EXPECTED;
                  }
                  err = recycle_node->add_node(new_leaf2);
                  if (UNLIKELY(ERROR_CODE_OK != err))
                  {
                    CMBTREE_LOG(ERROR, "recycle_node.add_node error, "
                        "err = %d", err);
                    ret = ERROR_CODE_NOT_EXPECTED;
                  }
                }
              }
            }
          }
        }

        return ret;
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::put_pair_direct_into_leaf_node_(
          TLeafNode * leaf, int16_t level, int16_t pos,
          const K & key, const V & value,
          TWriteParam & param, TRecycleNode * recycle_node)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;

        TLeafNode * new_leaf = new (node_alloc_.alloc_block()) TLeafNode();
        if (UNLIKELY(NULL == new_leaf))
        {
          CMBTREE_LOG(ERROR, "alloc failed");
          ret = ERROR_CODE_ALLOC_FAIL;
        }
        else
        {
          if (0 == param.found_)
          {
            err = leaf->clone_and_set_pair(new_leaf, pos, key, value);
          }
          else
          {
            err = leaf->clone_and_add_pair(new_leaf, pos, key, value);
          }
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "clone_and_set(_add)_pair error, "
                "err = %d", err);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            ret = set_ptr_into_inter_node_(static_cast<int16_t>(level - 1),
					new_leaf, param);
            if (LIKELY(ERROR_CODE_OK == ret))
            {
              err = recycle_node->add_node(leaf);
              if (UNLIKELY(ERROR_CODE_OK != err))
              {
                CMBTREE_LOG(ERROR, "recycle_node.add_node error, "
                    "err = %d", err);
                ret = ERROR_CODE_NOT_EXPECTED;
              }
              else
              {
                for (int16_t i = 0; i < level; i++)
                {
                  err = param.node_[i]->shared_unlock();
                  if (ERROR_CODE_OK != err)
                  {
                    CMBTREE_LOG(ERROR, "shared_unlock error, err = %d", err);
                  }
                }
              }
            }
            else
            {
              err = recycle_node->add_node(new_leaf);
              if (UNLIKELY(ERROR_CODE_OK != err))
              {
                CMBTREE_LOG(ERROR, "recycle_node.add_node error, "
                    "err = %d", err);
                ret = ERROR_CODE_NOT_EXPECTED;
              }
            }
          }
        }

        return ret;
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::put_pair_into_leaf_node_(const K & key,
          const V & value, TWriteParam & param, TRecycleNode * recycle_node)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;

        CMBTREE_CHECK_TRUE(param.node_length_ > 0
            && param.node_length_ < CONST_MAX_DEPTH,
            "node_length_ error, node_length_ = %d", param.node_length_);
        int16_t param_index = static_cast<int16_t>(param.node_length_ - 1);
        TBaseNode * node = param.node_[param_index];
        int16_t pos = param.node_pos_[param_index];
        if (UNLIKELY(NULL == node))
        {
          CMBTREE_LOG(ERROR, "leaf node is NULL");
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else if (UNLIKELY(!node->is_leaf()))
        {
          CMBTREE_LOG(ERROR, "last node is not leaf");
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          TLeafNode * leaf = reinterpret_cast<TLeafNode *>(node);
          err = leaf->shared2exclusive_lock();
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(DEBUG, "put op collision");
            ret = ERROR_CODE_NEED_RETRY;
          }
          else
          {
            if (UNLIKELY(param.found_ != 0
                  && leaf->get_size() >= CONST_NODE_OBJECT_COUNT))
            {
              ret = split_leaf_node_and_put_pair_(leaf, param_index, pos,
                  key, value, param, recycle_node);
            }
            else
            {
              ret = put_pair_direct_into_leaf_node_(leaf, param_index, pos,
                  key, value, param, recycle_node);
            }
            if (UNLIKELY(ERROR_CODE_OK != ret))
            {
              err = leaf->exclusive2shared_lock();
              if (UNLIKELY(ERROR_CODE_OK != err))
              {
                CMBTREE_LOG(ERROR, "exclusive2shared_lock error");
              }
            }
            else
            {
              err = leaf->exclusive_unlock();
              if (UNLIKELY(ERROR_CODE_OK != err))
              {
                CMBTREE_LOG(ERROR, "exclusive_unlock error");
              }
            }
          }
        }

        return ret;
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::get_children_(const K & key,
          TWriteParam & param)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;

        TBaseNode * proot = root_;

        if (UNLIKELY(NULL == proot))
        {
          CMBTREE_LOG(ERROR, "root_ is NULL");
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          int32_t i = 0;
          int16_t pos = -1;
          int64_t found = -1;
          param.node_length_ = 0;
          do
          {
            TInterNode * last_node = NULL;
            int16_t last_pos = 0;
            bool locked = false;
            while (ERROR_CODE_OK == ret && !locked)
            {
              if (UNLIKELY(i == 0))
              {
                proot = root_;
              }
              else
              {
                last_node = reinterpret_cast<TInterNode *>(param.node_[i - 1]);
                last_pos = param.node_pos_[i - 1];
                err = last_node->get_ptr(last_pos, proot);
                if (UNLIKELY(ERROR_CODE_OK != err))
                {
                  CMBTREE_LOG(ERROR, "get_ptr error, err = %d, pos = %d",
                      err, last_pos);
                  ret = ERROR_CODE_NOT_EXPECTED;
                }
              }
              err = proot->try_shared_lock();
              if (UNLIKELY(err != ERROR_CODE_OK))
              {
                ret = ERROR_CODE_NEED_RETRY;
              }
              else
              {
                if (UNLIKELY(i == 0))
                {
                  if (proot == root_)
                  {
                    locked = true;
                  }
                }
                else
                {
                  TBaseNode * proot_now = NULL;
                  err = last_node->get_ptr(last_pos, proot_now);
                  if (UNLIKELY(ERROR_CODE_OK != err))
                  {
                    CMBTREE_LOG(ERROR, "get_ptr error, err = %d, pos = %d",
                        err, last_pos);
                    ret = ERROR_CODE_NOT_EXPECTED;
                  }
                  else
                  {
                    if (proot == proot_now)
                    {
                      locked = true;
                    }
                  }
                }
                if (!locked)
                {
                  proot->shared_unlock();
                }
              }
            }
            if (LIKELY(ERROR_CODE_OK == ret))
            {
              err = proot->find_pos(key, pos, found);
              if (UNLIKELY(ERROR_CODE_OK != err))
              {
                CMBTREE_LOG(ERROR, "find_pos return error, err = %d, "
                    "pos = %d, found = %ld", err, pos, found);
                ret = ERROR_CODE_NOT_EXPECTED;
                proot->shared_unlock();
              }
              else
              {
                param.node_[i] = proot;
                param.node_pos_[i] = pos;
                param.node_length_++;
                if (LIKELY(!proot->is_leaf()))
                {
                  // pos+1是所需要找寻的子树的根节点位置
                  param.node_pos_[i]++;
                }
              }
              i++;
            }
          }
          while (ERROR_CODE_OK == ret && !proot->is_leaf());

          if (UNLIKELY(ERROR_CODE_OK != ret))
          {
            // 失败了，恢复已经加锁的节点
            shared_unlock_param_(param);
          }
          else
          {
            param.found_ = found;
            if (LIKELY(found != 0))
            {
              param.node_pos_[param.node_length_ - 1]++;
            }
          }
        }

        return ret;
      }

      //template<class K, class V, class Alloc>
      //int32_t BtreeBase<K, V, Alloc>::find_path_(const K & key,
      //    TFindType find_type, TWriteParam & param)
      //{
      //  int32_t ret = ERROR_CODE_OK;
      //  int32_t err = ERROR_CODE_OK;

      //  TBaseNode * proot = root_;

      //  if (UNLIKELY(NULL == proot))
      //  {
      //    CMBTREE_LOG(ERROR, "root_ is NULL");
      //    ret = ERROR_CODE_NOT_EXPECTED;
      //  }
      //  else
      //  {
      //    int32_t i = 0;
      //    int16_t pos = -1;
      //    int64_t found = -1;
      //    param.node_length_ = 0;
      //    do
      //    {
      //      TInterNode * last_node = NULL;
      //      int16_t last_pos = 0;
      //      bool locked = false;
      //      while (ERROR_CODE_OK == ret && !locked)
      //      {
      //        if (UNLIKELY(i == 0))
      //        {
      //          proot = root_;
      //        }
      //        else
      //        {
      //          last_node = reinterpret_cast<TInterNode *>(param.node_[i - 1]);
      //          last_pos = param.node_pos_[i - 1];
      //          err = last_node->get_ptr(last_pos, proot);
      //          if (UNLIKELY(ERROR_CODE_OK != err))
      //          {
      //            CMBTREE_LOG(ERROR, "get_ptr error, err = %d, pos = %d",
      //                err, last_pos);
      //            ret = ERROR_CODE_NOT_EXPECTED;
      //          }
      //        }
      //        if (FIND_PATH_PUT == type)
      //        {
      //          err = proot->try_shared_lock();
      //          if (UNLIKELY(err != ERROR_CODE_OK))
      //          {
      //            ret = ERROR_CODE_NEED_RETRY;
      //          }
      //          else
      //          {
      //            if (UNLIKELY(i == 0))
      //            {
      //              if (proot == root_)
      //              {
      //                locked = true;
      //              }
      //            }
      //            else
      //            {
      //              TBaseNode * proot_now = NULL;
      //              err = last_node->get_ptr(last_pos, proot_now);
      //              if (UNLIKELY(ERROR_CODE_OK != err))
      //              {
      //                CMBTREE_LOG(ERROR, "get_ptr error, err = %d, pos = %d",
      //                    err, last_pos);
      //                ret = ERROR_CODE_NOT_EXPECTED;
      //              }
      //              else
      //              {
      //                if (proot == proot_now)
      //                {
      //                  locked = true;
      //                }
      //              }
      //            }
      //          }
      //          if (!locked)
      //          {
      //            proot->shared_unlock();
      //          }
      //        }
      //        else
      //        {
      //          break;
      //        }
      //      }
      //      if (LIKELY(ERROR_CODE_OK == ret))
      //      {
      //        err = proot->find_pos(key, pos, found);
      //        if (UNLIKELY(ERROR_CODE_OK != err))
      //        {
      //          CMBTREE_LOG(ERROR, "find_pos return error, err = %d, "
      //              "pos = %d, found = %ld", err, pos, found);
      //          ret = ERROR_CODE_NOT_EXPECTED;
      //          if (FIND_PATH_PUT == type)
      //          {
      //            proot->shared_unlock();
      //          }
      //        }
      //        else
      //        {
      //          param.node_[i] = proot;
      //          param.node_pos_[i] = pos;
      //          param.node_length_++;
      //          if (LIKELY(!proot->is_leaf()))
      //          {
      //            // pos+1是所需要找寻的子树的根节点位置
      //            param.node_pos_[i]++;
      //          }
      //        }
      //        i++;
      //      }
      //    }
      //    while (ERROR_CODE_OK == ret && !proot->is_leaf());

      //    if (UNLIKELY(ERROR_CODE_OK != ret))
      //    {
      //      if (FIND_PATH_PUT == type)
      //      {
      //        // 失败了，恢复已经加锁的节点
      //        shared_unlock_param_(param);
      //      }
      //    }
      //    else
      //    {
      //      param.found_ = found;
      //      //if (LIKELY(found != 0))
      //      //{
      //      //  param.node_pos_[param.node_length_ - 1]++;
      //      //}
      //    }
      //  }

      //  return ret;
      //}

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::shared_unlock_param_(TWriteParam & param)
      {
        for (int32_t i = 0; i < param.node_length_; i++)
        {
          param.node_[i]->shared_unlock();
        }
        return ERROR_CODE_OK;
      }

      template<class K, class V, class Alloc>
      void BtreeBase<K, V, Alloc>::destroy_(TBaseNode * node)
      {
        if (NULL != node)
        {
          if (node->is_leaf())
          {
            reinterpret_cast<TLeafNode *>(node)->~BtreeLeafNode<K, V>();
          }
          else
          {
            TInterNode * inter = reinterpret_cast<TInterNode *>(node);
            for (int16_t i = 0; i < node->get_size() + 1; i++)
            {
              TBaseNode * ptr = NULL;
              inter->get_ptr(i, ptr);
              destroy_(ptr);
            }
            inter->~BtreeInterNode<K, V>();
          }
          node_alloc_.free_block(node);
        }
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::get_start_path_(TScanHandle & handle)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;

        TBaseNode * proot = root_;

        if (UNLIKELY(NULL == proot))
        {
          handle.begin_flag_ = false;
          handle.finish_flag_ = true;
        }
        else
        {
          int32_t i = 0;
          int16_t pos = -1;
          int64_t found = -1;
          handle.node_length_ = 0;
          do
          {
            TInterNode * last_node = NULL;
            int16_t last_pos = 0;
            if (UNLIKELY(i == 0))
            {
              proot = root_;
            }
            else
            {
              last_node = reinterpret_cast<TInterNode *>(handle.node_[i - 1]);
              last_pos = handle.node_pos_[i - 1];
              err = last_node->get_ptr(last_pos, proot);
              if (UNLIKELY(ERROR_CODE_OK != err))
              {
                CMBTREE_LOG(ERROR, "get_ptr error, err = %d, pos = %d",
                    err, last_pos);
                ret = ERROR_CODE_NOT_EXPECTED;
              }
            }
            if (LIKELY(ERROR_CODE_OK == ret))
            {
              err = proot->find_pos(handle.start_key_, pos, found);
              if (UNLIKELY(ERROR_CODE_OK != err))
              {
                CMBTREE_LOG(ERROR, "find_pos return error, err = %d, "
                    "pos = %d, found = %ld", err, pos, found);
                ret = ERROR_CODE_NOT_EXPECTED;
                proot->shared_unlock();
              }
              else
              {
                handle.node_[i] = proot;
                handle.node_pos_[i] = pos;
                handle.node_length_++;
                if (LIKELY(!proot->is_leaf()))
                {
                  // pos+1是所需要找寻的子树的根节点位置
                  handle.node_pos_[i]++;
                }
              }
            }
            if (LIKELY(ERROR_CODE_OK == err))
            {
              if (handle.forward_direction_)
              {
                if (i == 0 || !handle.forcast_compare_[i - 1])
                {
                  if (LIKELY(handle.node_pos_[i] < proot->get_size()))
                  {
                    K last_key;
                    err = proot->get_key(static_cast<int16_t>(proot->get_size() - 1),
							last_key);
                    if (UNLIKELY(ERROR_CODE_OK != err))
                    {
                      CMBTREE_LOG(ERROR, "get_key error, err = %d, "
                          "proot->get_size() = %d", err, proot->get_size());
                      ret = ERROR_CODE_NOT_EXPECTED;
                    }
                    else
                    {
                      if (last_key - handle.end_key_ < 0)
                      {
                        handle.forcast_compare_[i] = true;
                      }
                      else
                      {
                        handle.forcast_compare_[i] = false;
                      }
                    }
                  }
                  else
                  {
                    handle.forcast_compare_[i] = false;
                  }
                }
                else
                {
                  handle.forcast_compare_[i] = true;
                }
              }
              else
              {
                if (i == 0 || !handle.forcast_compare_[i - 1])
                {
                  if (LIKELY(handle.node_pos_[i] > 0))
                  {
                    K first_key;
                    err = proot->get_key(0, first_key);
                    if (UNLIKELY(ERROR_CODE_OK != err))
                    {
                      CMBTREE_LOG(ERROR, "get_key error, err = %d, "
                          "proot->get_size() = %d", err, proot->get_size());
                      ret = ERROR_CODE_NOT_EXPECTED;
                    }
                    else
                    {
                      if (first_key - handle.end_key_ > 0)
                      {
                        handle.forcast_compare_[i] = true;
                      }
                      else
                      {
                        handle.forcast_compare_[i] = false;
                      }
                    }
                  }
                  else
                  {
                    handle.forcast_compare_[i] = false;
                  }
                }
                else
                {
                  handle.forcast_compare_[i] = true;
                }
              }
            }
            i++;
          }
          while (ERROR_CODE_OK == ret && !proot->is_leaf());

          handle.found_ = found;
          if (handle.forward_direction_)
          {
            if (LIKELY(found != 0))
            {
              int16_t index = static_cast<int16_t>(handle.node_length_ - 1);
              err = step_forward_(handle, index);
              if (UNLIKELY(ERROR_CODE_NOT_FOUND == err))
              {
                handle.begin_flag_ = false;
                handle.finish_flag_ = true;
              }
              else if (UNLIKELY(ERROR_CODE_OK != err))
              {
                CMBTREE_LOG(ERROR, "step_forward error, err = %d index = %d",
                    err, index);
                ret = ERROR_CODE_NOT_EXPECTED;
              }
            }
          }
          else
          {
            bool less_than_min_key = true;
            for (int16_t i = 0; i < handle.node_length_ - 1; i++)
            {
              if (handle.node_pos_[i] != 0)
              {
                less_than_min_key = false;
              }
            }
            if (handle.node_pos_[handle.node_length_ - 1] != -1)
            {
              less_than_min_key = false;
            }
            if (less_than_min_key)
            {
              handle.begin_flag_ = false;
              handle.finish_flag_ = true;
            }
          }
        }

        return ret;
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::step_forward_(TScanHandle & handle, int16_t level)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;
        if (level < 0)
        {
          ret = ERROR_CODE_NOT_FOUND;
        }
        else
        {
          TBaseNode * & node = handle.node_[level];
          int16_t & pos = handle.node_pos_[level];
          int32_t is_leaf = node->is_leaf();
          int16_t size = node->get_size();
          if (LIKELY(!is_leaf))
          {
            size++;
          }
          if (pos < size - 1)
          {
            pos++;
            if (!is_leaf && pos == size - 1)
            {
              handle.forcast_compare_[level] = false;
            }
          }
          else
          {
            err = step_forward_(handle, static_cast<int16_t>(level - 1));
            if (UNLIKELY(ERROR_CODE_NOT_FOUND == err))
            {
              ret = err;
            }
            else if (UNLIKELY(ERROR_CODE_OK != err))
            {
              CMBTREE_LOG(ERROR, "step_forward_ error, err = %d level = %d",
                  err, level);
              ret = ERROR_CODE_NOT_EXPECTED;
            }
            else
            {
              reinterpret_cast<TInterNode *>(handle.node_[level - 1])->get_ptr(
                  handle.node_pos_[level - 1], node);
              if (handle.forcast_compare_[level - 1])
              {
                handle.forcast_compare_[level] = true;
              }
              else
              {
                K last_key;
                err = node->get_key(static_cast<int16_t>(node->get_size() - 1),
						last_key);
                if (UNLIKELY(ERROR_CODE_OK != err))
                {
                  CMBTREE_LOG(ERROR, "get_key error, err = %d, "
                      "node->get_size() = %d", err, node->get_size());
                  ret = ERROR_CODE_NOT_EXPECTED;
                }
                else
                {
                  if (last_key - handle.end_key_ < 0)
                  {
                    handle.forcast_compare_[level] = true;
                  }
                  else
                  {
                    handle.forcast_compare_[level] = false;
                  }
                }
              }
              pos = 0;
            }
          }
        }
        return ret;
      }

      template<class K, class V, class Alloc>
      int32_t BtreeBase<K, V, Alloc>::step_backward_(TScanHandle & handle, int16_t level)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;
        if (level < 0)
        {
          ret = ERROR_CODE_NOT_FOUND;
        }
        else
        {
          TBaseNode * & node = handle.node_[level];
          int16_t & pos = handle.node_pos_[level];
          int32_t is_leaf = node->is_leaf();
          int16_t size = node->get_size();
          if (UNLIKELY(!is_leaf))
          {
            size++;
          }
          if (pos > 0)
          {
            pos--;
            if (!is_leaf && pos == 0)
            {
              handle.forcast_compare_[level] = false;
            }
          }
          else
          {
            err = step_backward_(handle, static_cast<int16_t>(level - 1));
            if (UNLIKELY(ERROR_CODE_NOT_FOUND == err))
            {
              ret = err;
            }
            else if (UNLIKELY(ERROR_CODE_OK != err))
            {
              CMBTREE_LOG(ERROR, "step_forward_ error, err = %d level = %d",
                  err, level);
              ret = ERROR_CODE_NOT_EXPECTED;
            }
            else
            {
              reinterpret_cast<TInterNode *>(handle.node_[level - 1])->get_ptr(
                  handle.node_pos_[level - 1], node);
              if (handle.forcast_compare_[level - 1])
              {
                handle.forcast_compare_[level] = true;
              }
              else
              {
                K first_key;
                err = node->get_key(0, first_key);
                if (UNLIKELY(ERROR_CODE_OK != err))
                {
                  CMBTREE_LOG(ERROR, "get_key error, err = %d, "
                      "node->get_size() = %d", err, node->get_size());
                  ret = ERROR_CODE_NOT_EXPECTED;
                }
                else
                {
                  if (first_key - handle.end_key_ > 0)
                  {
                    handle.forcast_compare_[level] = true;
                  }
                  else
                  {
                    handle.forcast_compare_[level] = false;
                  }
                }
              }
              if (LIKELY(node->is_leaf()))
              {
                pos = static_cast<int16_t>(node->get_size() - 1);
              }
              else
              {
                pos = node->get_size();
              }
            }
          }
        }
        return ret;
      }
    } // end namespace cmbtree
  } // end namespace common
} // end namespace oceanbase

#endif

