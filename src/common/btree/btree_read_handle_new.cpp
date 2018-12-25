#include "btree_alloc.h"
#include "btree_root_pointer.h"
#include "btree_read_handle_new.h"

namespace oceanbase
{
  namespace common
  {
    /**
     * 构造
     */
    BtreeReadHandle::BtreeReadHandle()
    {
      from_key_ = NULL;
      from_exclude_ = 0;
      to_key_ = NULL;
      to_exclude_ = 0;
      direction_ = 0;
      is_eof_ = 0;
      tree_id_ = 0;
    }

    /**
     * 析构
     */
    BtreeReadHandle::~BtreeReadHandle()
    {
    }

    /**
     * 设置from key
     */
    void BtreeReadHandle::set_from_key_range(char *key, int32_t key_size, int32_t key_exclude)
    {
      OCEAN_BTREE_CHECK_TRUE(key_size >= 0, "key_size: %d", key_size);
      if (key_size <= 0)
      {
        from_key_ = key;
      }
      else
      {
        OCEAN_BTREE_CHECK_TRUE(key_size <= CONST_KEY_MAX_LENGTH, "key_size: %d", key_size);
        if (key_size > CONST_KEY_MAX_LENGTH)
          key_size = CONST_KEY_MAX_LENGTH;
        from_key_ = &buffer[0];
        memcpy(from_key_, key, key_size);
      }
      from_exclude_ = key_exclude;
    }

    /**
     * 设置to key
     */
    void BtreeReadHandle::set_to_key_range(char *key, int32_t key_size, int32_t key_exclude)
    {
      OCEAN_BTREE_CHECK_TRUE(key_size >= 0, "key_size: %d", key_size);
      if (key_size <= 0)
      {
        to_key_ = key;
      }
      else
      {
        OCEAN_BTREE_CHECK_TRUE(key_size <= CONST_KEY_MAX_LENGTH, "key_size: %d", key_size);
        if (key_size > CONST_KEY_MAX_LENGTH)
          key_size = CONST_KEY_MAX_LENGTH;
        to_key_ = &buffer[CONST_KEY_MAX_LENGTH];
        memcpy(to_key_, key, key_size);
      }
      to_exclude_ = key_exclude;
    }

    /**
     * 设置btree_id
     */
    void BtreeReadHandle::set_tree_id(int32_t id)
    {
      tree_id_ = id;
    }

  } // end namespace common
} // end namespace oceanbase
