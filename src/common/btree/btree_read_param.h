#ifndef OCEANBASE_COMMON_BTREE_BTREE_READ_PARAM_H_
#define OCEANBASE_COMMON_BTREE_BTREE_READ_PARAM_H_

#include "btree_define.h"

namespace oceanbase
{
  namespace common
  {
    /**
     * BTree搜索过程的辅助结构
     */
    class BtreeNode;
    class BtreeReadParam
    {
      friend class BtreeBase;
    public:
      /**
       * 构造
       */
      BtreeReadParam();
      /**
       * 析构
       */
      virtual ~BtreeReadParam();

#ifdef OCEAN_BASE_BTREE_DEBUG
      /**
       * 打印出来.
       */
      void print();
#endif

    protected:
      // 找到的key是否相等
      int64_t found_;
      // 当前节点使用
      BtreeNode *node_[CONST_MAX_DEPTH];
      // 节点的个数
      int32_t node_length_;
      // 当前节点位置
      int16_t node_pos_[CONST_MAX_DEPTH];
    };
  } // end namespace common
} // end namespace oceanbase

#endif
