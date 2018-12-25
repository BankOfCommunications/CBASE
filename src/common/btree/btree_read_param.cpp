#include "btree_node.h"
#include "btree_read_param.h"

namespace oceanbase
{
  namespace common
  {
    /**
     * BTree搜索过程的辅助结构
     */

    BtreeReadParam::BtreeReadParam()
    {
      node_length_ = 0;
      found_ = -1;
    }

    /**
     * 析造
     */
    BtreeReadParam::~BtreeReadParam()
    {
    }

#ifdef OCEAN_BASE_BTREE_DEBUG
    /**
     * 打印出来.
     */
    void BtreeReadParam::print()
    {
      fprintf(stderr, "node_length: %d, found: %ld\n", node_length_, found_);
      for(int i = 0; i < node_length_; i++)
      {
        fprintf(stderr, " addr: %p pos: %d\n", node_[i], node_pos_[i]);
      }
    }
#endif

  } // end namespace common
} // end namespace oceanbase
