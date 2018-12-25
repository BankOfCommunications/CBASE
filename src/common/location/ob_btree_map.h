/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_btree_map.h,v 0.1 2010/09/26 14:01:30 zhidong Exp $
 *
 * Authors:
 *   chuanhui <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OB_COMMON_BTREE_MAP_H_
#define OB_COMMON_BTREE_MAP_H_

#include "tblog.h"
#include "common/ob_malloc.h"
#include "common/ob_define.h"
#include "common/btree/btree_base.h"
#include "common/btree/key_btree.h"

namespace oceanbase
{
  namespace common
  {
    // this class is btree and hash data engine's abstract class
    // adaptor the lrucache engine interface for obvarcache
    // all the interfaces must be same with hashengine
    // and the real implementions are in btree class
    template <class _key, class _value>
    class ObBtreeMap
    {
    public:
      ObBtreeMap()
      {
        tree_map_ = NULL;
      }

      virtual ~ObBtreeMap()
      {
        if (tree_map_)
        {
          delete tree_map_;
          tree_map_ = NULL;
        }
      }
    
    public:
      // create the btreemap with init node count
      // not thread-safe if write_lock is not true
      int create(int64_t num, const bool write_lock = false);
      // get key value if exist
      int get(const _key & key, _value & value) const;
      // insert or update the key or value
      int set(const _key & key, const _value & new_value, _value & old_value,
          const bool okey = true, const bool ovalue = true);
      // del the key
      int erase(const _key & key, _value & value);
      // get data node count
      int64_t size() const;
    
    private:
      common::KeyBtree<_key, _value> * tree_map_;
    };
    
    template <class _key, class _value>
    int ObBtreeMap<_key, _value>::create(int64_t num, const bool write_lock)
    {
      int ret = common::OB_SUCCESS;
      if (num <= 0)
      {
        TBSYS_LOG(ERROR, "check create num failed:num[%ld]", num);
        ret = common::OB_INPUT_PARAM_ERROR;
      }
      else if (tree_map_)
      {
        TBSYS_LOG(ERROR, "check tree map not null:map[%p]", tree_map_);
        ret = common::OB_INNER_STAT_ERROR; 
      }
      else
      {
        tree_map_ = new(std::nothrow) common::KeyBtree<_key, _value>(static_cast<int32_t>(num));
        if (NULL == tree_map_)
        {
          TBSYS_LOG(ERROR, "new KeyBtree failed:map[%p]", tree_map_);
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (true == write_lock)
        {
          // write lock for multi-thread safe
          tree_map_->set_write_lock_enable(1);
        }
        else
        {
          TBSYS_LOG(DEBUG, "not enable write lock:map[%p]", tree_map_);
        }
      }
      return ret;
    }

    template <class _key, class _value>
    int64_t ObBtreeMap<_key, _value>::size() const
    {
      int64_t ret = 0;
      if (tree_map_)
      {
        ret = tree_map_->get_object_count();
      }
      else
      {
        TBSYS_LOG(ERROR, "check tree map is null:map[%p]", tree_map_);
      }
      return ret;
    }

    template <class _key, class _value>
    int ObBtreeMap<_key, _value>::erase(const _key & key, _value & value)
    {
      int ret = common::OB_SUCCESS;
      if (tree_map_)
      {
        ret = tree_map_->remove(key, value);
        if (ret != common::OB_SUCCESS)
        {
          TBSYS_LOG(DEBUG, "erase key value failed:ret[%d]", ret);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "check tree map is null:map[%p]", tree_map_);
        ret = common::OB_INNER_STAT_ERROR;
      }
      return ret;
    }

    template <class _key, class _value>
    int ObBtreeMap<_key, _value>::get(const _key & key, _value & value) const
    {
      int ret = common::OB_SUCCESS;
      if (tree_map_)
      {
        ret = tree_map_->get(key, value);
      }
      else
      {
        TBSYS_LOG(ERROR, "check tree map is null:map[%p]", tree_map_);
        ret = common::OB_INNER_STAT_ERROR;
      }
      return ret;
    }
    
    template <class _key, class _value>
    int ObBtreeMap<_key, _value>::set(const _key & key, const _value & new_value,
        _value & old_value, const bool okey /* = true */, const bool ovalue /* = true */)
    {
      int ret = common::OB_SUCCESS;
      if (tree_map_)
      {
        ret = tree_map_->put(key, new_value, old_value, okey, ovalue);
        if (ret != common::OB_SUCCESS)
        {
          TBSYS_LOG(DEBUG, "set key value failed:ret[%d]", ret);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "check tree map is null:map[%p]", tree_map_);
        ret = common::OB_INNER_STAT_ERROR;
      }
      return ret;
    }
  }
}


#endif // OB_COMMON_BTREE_MAP_H_

