/**
 * (C) 2013-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_placement_hashmap.h
 *
 * Authors:
 *   Fusheng Han <yanran.hfs@alipay.com>
 *
 */

#include "ob_placement_hashmap.h"

namespace oceanbase
{
  namespace common
  {
    namespace hash
    {
      template <class K, uint64_t N = 1031>
      class ObPlacementHashSet
      {
        public:
          ObPlacementHashSet() { clear(); }
          /**
           * @retval HASH_EXIST       set failed
           * @retval HASH_INSERT_SUCC insert new key succ
           * @retval OB_ERROR         set error when buckets are full
           */
          int  set  (const K & key);
          /**
           * @retval HASH_EXIST     key exists
           * @retval HASH_NOT_EXIST key does not exist
           */
          int  exist(const K & key) const;
          void clear();
        protected:
          ObPlacementHashMap<K, uint8_t, N> hash_map_;
      };

      template <class K, uint64_t N>
      int ObPlacementHashSet<K, N>::set(const K & key)
      {
        return hash_map_.set(key, 0);
      }

      template <class K, uint64_t N>
      int ObPlacementHashSet<K, N>::exist(const K & key) const
      {
        uint8_t v;
        return hash_map_.get(key, v);
      }

      template <class K, uint64_t N>
      void ObPlacementHashSet<K, N>::clear()
      {
        hash_map_.clear();
      }

    } // namespace hash
  } // namespace common
} // namespace oceanbase
