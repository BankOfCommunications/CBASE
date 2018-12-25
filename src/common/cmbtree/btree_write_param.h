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
 *     - Intermediate data structure used for modifying CMBtree
 */
#ifndef OCEANBASE_COMMON_CMBTREE_BTREE_WRITE_PARAM_H_
#define OCEANBASE_COMMON_CMBTREE_BTREE_WRITE_PARAM_H_

#include "btree_define.h"
#include "btree_read_param.h"

namespace oceanbase
{
  namespace common
  {
    namespace cmbtree
    {
      /**
       * BTree写过程的辅助结构
       */
      template<class K, class V>
      class BtreeWriteParam : public BtreeReadParam<K, V>
      {
      public:
        typedef BtreeBaseNode<K, V>           TBaseNode;
      public:
        /**
         * 构造
         */
        BtreeWriteParam();
        /**
         * 析构
         */
        ~BtreeWriteParam();

      private:
        TBaseNode * new_root_node_;                /// 当产生新root node时候用
        TBaseNode * next_node_[CONST_MAX_DEPTH];   /// 下一节点
      };

      template<class K, class V>
      BtreeWriteParam<K, V>::BtreeWriteParam()
      {
        new_root_node_ = NULL;
        memset(next_node_, 0, CONST_MAX_DEPTH * sizeof(TBaseNode *));
      }

      template<class K, class V>
      BtreeWriteParam<K, V>::~BtreeWriteParam()
      {
      }

    } // end namespace cmbtree
  } // end namespace common
} // end namespace oceanbase

#endif
