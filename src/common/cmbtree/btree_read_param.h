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
 *     - Structure used for range query of CMBtree
 */
#ifndef OCEANBASE_COMMON_CMBTREE_BTREE_READ_PARAM_H_
#define OCEANBASE_COMMON_CMBTREE_BTREE_READ_PARAM_H_

#include "btree_define.h"

namespace oceanbase
{
  namespace common
  {
    namespace cmbtree
    {
      /**
       * BTree搜索过程的辅助结构
       */
      template<class K, class V>
      class BtreeBaseNode;

      template<class K, class V>
      class BtreeReadParam
      {
      public:
        /**
         * 构造
         */
        BtreeReadParam();
        /**
         * 析构
         */
        virtual ~BtreeReadParam();

#ifdef CMBTREE_DEBUG
        /**
         * 打印出来.
         */
        void print();
#endif

      protected:
      public:
        int64_t found_;                                      /// 找到的key是否相等
        BtreeBaseNode<K, V> * node_[CONST_MAX_DEPTH]; /// 路径上的节点
        int16_t node_pos_[CONST_MAX_DEPTH];                  /// 路将上的节点内位置
        int16_t node_length_;                                /// 节点的个数
      };

      template<class K, class V>
      BtreeReadParam<K, V>::BtreeReadParam()
      {
        node_length_ = 0;
        found_ = -1;
      }

      template<class K, class V>
      BtreeReadParam<K, V>::~BtreeReadParam()
      {
      }

#ifdef CMBTREE_DEBUG
      template<class K, class V>
      void BtreeReadParam<K, V>::print()
      {
        fprintf(stderr, "node_length: %d, found: %ld\n", node_length_, found_);
        for(int i = 0; i < node_length_; i++)
        {
          fprintf(stderr, " addr: %p pos: %d\n", node_[i], node_pos_[i]);
        }
      }
#endif

    } // end namespace cmbtree
  } // end namespace common
} // end namespace oceanbase

#endif
