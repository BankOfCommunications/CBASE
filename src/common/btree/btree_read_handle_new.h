#ifndef OCEANBASE_COMMON_BTREE_BTREE_READ_HANDLE_H_
#define OCEANBASE_COMMON_BTREE_BTREE_READ_HANDLE_H_

#include "btree_read_param.h"
#include "btree_base_handle.h"

namespace oceanbase
{
  namespace common
  {
    /**
     * 用于读的时候用, 用于范围查找
     */
    class BtreeAlloc;
    class BtreeReadHandle : public BtreeBaseHandle
    {
      friend class BtreeBase;
    public:
      /**
       * 构造
       */
      BtreeReadHandle();
      /**
       * 析构
       */
      ~BtreeReadHandle();

      /**
       * 设置from key
       */
      void set_from_key_range(char *key, int32_t key_size, int32_t key_exclude);

      /**
       * 设置to key
       */
      void set_to_key_range(char *key, int32_t key_size, int32_t key_exclude);

      /**
       * 设置btree_id
       */
      void set_tree_id(int32_t id);

    private:
      // key的搜索路径
      BtreeReadParam param_;
      // to key的路径
      BtreeReadParam to_param_;
      // from_key
      char *from_key_;
      // to_key
      char *to_key_;
      // 是否排除from_key
      int32_t from_exclude_;
      // 是否排除to_key
      int32_t to_exclude_;
      // 查询方向, -1 向左, 1 向右
      int32_t direction_;
      // 是否结束
      int32_t is_eof_;
      // btree_id
      int32_t tree_id_;
      // buffer, 放key的内容
      char buffer[CONST_KEY_MAX_LENGTH*2];
    };
  } // end namespace common
} // end namespace oceanbase

#endif

