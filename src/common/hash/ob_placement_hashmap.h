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

#include "ob_hashutils.h"
#include <common/ob_bit_set.h>

namespace oceanbase
{
  namespace common
  {
    namespace hash
    {
      template <class K, class V, uint64_t N = 1031>
      class ObPlacementHashMap
      {
        public:
          ObPlacementHashMap() { clear(); }
          /**
           * put a key value pair into HashMap
           * when flag = 0, do not overwrite existing <key,value> pair
           * when flag != 0 and overwrite_key = 0, overwrite existing value
           * when flag != 0 and overwrite_key != 0, overwrite existing <key,value> pair
           * @retval HASH_EXIST set error when flag = 0
           * @retval HASH_OVERWRITE_SUCC overwrite succ when flag != 0
           * @retval HASH_INSERT_SUCC insert new key,value pair succ
           * @retval OB_ERROR set error when buckets are full
           */
          int  set  (const K & key, const V & value, int flag = 0, int overwrite_key = 0);
          /**
           * @retval HASH_EXIST get the corresponding value of key
           * @retval HASH_NOT_EXIST key does not exist
           */
          int  get  (const K & key, V & value) const;
          /**
           * @retval value get the corresponding value of key
           * @retval NULL key does not exist
           */
          const V * get(const K & key) const;
          void clear();
        protected:
          int set_   (uint64_t pos, const K & key, const V & value, int flag, int overwrite_key);
          int search_(const K & key, uint64_t & pos) const;
          static const int SET_RETRY_RET_CODE = -100;
        protected:
          ObBitSet<N> flags_;
          K           keys_[N];
          V           values_[N];
          int64_t     hash_collision_count_;
      };

      template <class K, class V, uint64_t N>
      int ObPlacementHashMap<K, V, N>::set(const K & key, const V & value,
          int flag/* = 0*/, int overwrite_key/* = 0*/)
      {
        int ret = 0;
        if (N == 0)
        {
          ret = OB_ERROR;
        }
        else
        {
          uint64_t pos = murmurhash2(&key, sizeof(key), 0) % N;
          uint64_t i = 0;
          for (; i < N; i++, pos++)
          {
            if (pos == N)
            {
              pos = 0;
            }
            ret = set_(pos, key, value, flag, overwrite_key);
            if (ret != SET_RETRY_RET_CODE)
            {
              break;
            }
            hash_collision_count_++;
          }
          if (N == i)
          {
            TBSYS_LOG(ERROR, "hash buckets are full");
            ret = OB_ERROR;
          }
        }
        return ret;
      }

      template <class K, class V, uint64_t N>
      int ObPlacementHashMap<K, V, N>::get(const K & key, V & value) const
      {
        int ret = 0;
        uint64_t pos = 0;
        ret = search_(key, pos);
        if (HASH_EXIST == ret)
        {
          value = values_[pos];
        }
        return ret;
      }

      template <class K, class V, uint64_t N>
      const V * ObPlacementHashMap<K, V, N>::get(const K & key) const
      {
        const V * ret = NULL;
        uint64_t pos = 0;
        int err = search_(key, pos);
        if (HASH_EXIST == err)
        {
          ret = &values_[pos];
        }
        return ret;
      }

      template <class K, class V, uint64_t N>
      void ObPlacementHashMap<K, V, N>::clear()
      {
        flags_.clear();
      }

      template <class K, class V, uint64_t N>
      int ObPlacementHashMap<K, V, N>::set_(uint64_t pos, const K & key, const V & value,
          int flag, int overwrite_key)
      {
        int ret = 0;
        if (!flags_.has_member(static_cast<int32_t>(pos)))
        {
          keys_[pos]   = key;
          values_[pos] = value;
          flags_.add_member(static_cast<int32_t>(pos));
          ret = HASH_INSERT_SUCC;
        }
        else if (keys_[pos] == key)
        {
          if (flag == 0)
          {
            ret = HASH_EXIST;
          }
          else
          {
            if (overwrite_key == 0)
            {
              keys_[pos] = key;
            }
            values_[pos] = value;
            ret = HASH_OVERWRITE_SUCC;
          }
        }
        else
        {
          ret = SET_RETRY_RET_CODE;
        }
        return ret;
      }

      template <class K, class V, uint64_t N>
      int ObPlacementHashMap<K, V, N>::search_(const K & key, uint64_t & pos) const
      {
        int ret = 0;
        if (0 == N)
        {
          ret = HASH_NOT_EXIST;
        }
        else
        {
          pos = murmurhash2(&key, sizeof(key), 0) % N;
          uint64_t i = 0;
          for (; i < N; i++, pos++)
          {
            if (pos == N)
            {
              pos = 0;
            }
            if (!flags_.has_member(static_cast<int32_t>(pos)))
            {
              ret = HASH_NOT_EXIST;
              break;
            }
            else if (keys_[pos] == key)
            {
              ret = HASH_EXIST;
              break;
            }
          }
          if (N == i)
          {
            ret = HASH_NOT_EXIST;
          }
        }
        return ret;
      }

    } // namespace hash
  } // namespace common
} // namespace oceanbase
