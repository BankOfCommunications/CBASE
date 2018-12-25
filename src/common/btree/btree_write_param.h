#ifndef OCEANBASE_COMMON_BTREE_BTREE_WRITE_PARAM_H_
#define OCEANBASE_COMMON_BTREE_BTREE_WRITE_PARAM_H_

#include "btree_define.h"

namespace oceanbase
{
  namespace common
  {
    /**
     * BTree写过程的辅助结构
     */
    class BtreeNode;
    class BtreeReadParam;
    class BtreeWriteParam : public BtreeReadParam
    {
      friend class BtreeBase;
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
      // 当产生新root node时候用
      BtreeNode *new_root_node_;
      // 下一节点使用
      BtreeNode *next_node_[CONST_MAX_DEPTH];
      // tree_id
      int32_t tree_id;
    };
  } // end namespace common
} // end namespace oceanbase

#endif
