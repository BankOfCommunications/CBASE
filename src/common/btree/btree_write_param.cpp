#include "btree_read_param.h"
#include "btree_write_param.h"

namespace oceanbase
{
  namespace common
  {
    /**
     * BTree写过程的辅助结构
     */

    BtreeWriteParam::BtreeWriteParam() : BtreeReadParam()
    {
      tree_id = 0;
      new_root_node_ = NULL;
      memset(next_node_, 0, CONST_MAX_DEPTH * sizeof(BtreeNode*));
    }

    /**
     * 析造
     */
    BtreeWriteParam::~BtreeWriteParam()
    {
    }

  } // end namespace common
} // end namespace oceanbase
