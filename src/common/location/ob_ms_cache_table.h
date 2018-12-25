#ifndef OB_COMMON_BTREE_TABLE_H_
#define OB_COMMON_BTREE_TABLE_H_

#include "ob_btree_map.h"
#include "common/ob_rowkey.h"
#include "common/ob_range2.h"
#include "common/utility.h"
#include "common/ob_cache.h"
#include "common/ob_define.h"


namespace oceanbase
{
  namespace common
  {
    // Search Engine for ob_cache replacement of ObCHashTable
    // Waring must be a template class as the cache template param
    // because of the hash implemention has already been template class
    template <class _key, class _value>
    class ObCBtreeTable
    {
    public:
      ObCBtreeTable(): cache_lock_(tbsys::WRITE_PRIORITY)
      {
      }

      virtual ~ObCBtreeTable()
      {
      }

    // cache engine interface
    public:
      // create btree map
      int init(int32_t slot_num);
      // add a new item and return the old item for mem destory
      // the search btree's key is the item.key_
      int add(common::CacheItemHead & item, common::CacheItemHead *& old_item);
      // get the value through string key
      // the key is constructed with (uint64_t table_id + ObString row_key)
      // at first convert the key to obRange type for btree compare with nodes
      common::CacheItemHead * get(const common::ObNewRange & key);

      // remove a item after it's replaced
      void remove(common::CacheItemHead & item);

      // remove the value through string key
      common::CacheItemHead * remove(const common::ObNewRange & key);

      // get item num
      int64_t get_item_num(void) const;
    public:
      // search mode for compare
      enum SearchMode
      {
        OB_SEARCH_MODE_EQUAL = 0x00,
        OB_SEARCH_MODE_GREATER_EQUAL,
        OB_SEARCH_MODE_GREATER_THAN,
        OB_SEARCH_MODE_LESS_THAN,
        OB_SEARCH_MODE_LESS_EQUAL
      };

      // the KeyBtree key type for ob_cache
      // it only the wrapper of (CacheItemHead *) for operator - definition
      struct MapKey
      {
        common::CacheItemHead * key_;
        int compare_range_with_key(const uint64_t table_id, const common::ObRowkey & row_key,
            const common::ObNewRange & range) const
        {
          int cmp = 0;
          if (table_id != range.table_id_)
          {
            cmp = table_id < range.table_id_ ? -1 : 1;
          }
          else
          {
            // the tablet range: (range.start_key_, range.end_key_]
            int cmp_endkey = 0;
            if (range.end_key_.is_max_row())
            {
              cmp_endkey = -1;
            }
            else
            {
              cmp_endkey = row_key.compare(range.end_key_);
            }

            if (cmp_endkey < 0) // row_key < end_key
            {
              if (range.start_key_.is_min_row())
              {
                cmp = 0;
              }
              else
              {
                int tmp = row_key.compare(range.start_key_);
                if (tmp > 0)
                {
                  cmp = 0;
                }
                else
                {
                  cmp = -1;
                }
              }
            }
            else if (cmp_endkey > 0) // row_key > end_key
            {
              cmp = 1;
            }
            else                     // row_key == end_key
            {
              cmp = 0;
            }
          }
          return cmp;
        }

        bool operator < (const MapKey & key) const
        {
          return (*this - (key)) < 0;
        }

        // overload operator - for key comparison
        int operator - (const MapKey & key) const
        {
          // range1
          common::ObNewRange range1;
          common::ObObj start_rowkey_obj_array1[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
          common::ObObj end_rowkey_obj_array1[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
          range1.start_key_.assign(start_rowkey_obj_array1, common::OB_MAX_ROWKEY_COLUMN_NUMBER);
          range1.end_key_.assign(end_rowkey_obj_array1, common::OB_MAX_ROWKEY_COLUMN_NUMBER);
          // range2
          common::ObNewRange range2;
          common::ObObj start_rowkey_obj_array2[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
          common::ObObj end_rowkey_obj_array2[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
          range2.start_key_.assign(start_rowkey_obj_array2, common::OB_MAX_ROWKEY_COLUMN_NUMBER);
          range2.end_key_.assign(end_rowkey_obj_array2, common::OB_MAX_ROWKEY_COLUMN_NUMBER);
          int64_t pos = 0;
          int ret = range1.deserialize(key_->key_, key_->key_size_, pos);
          if (ret != common::OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "deserialize range1 failed:ret[%d]", ret);
          }
          else
          {
            pos = 0;
            ret = range2.deserialize(key.key_->key_, key.key_->key_size_, pos);
            if (ret != common::OB_SUCCESS)
            {
              TBSYS_LOG(ERROR, "deserialize range2 failed:ret[%d]", ret);
            }
          }

          if (common::OB_SUCCESS == ret)
          {
            if (range1.start_key_ == range1.end_key_)
            {
              ret = compare_range_with_key(range1.table_id_, range1.start_key_, range2);
              /*
               TBSYS_LOG(DEBUG, "table[%lu], key[%s], range[%s], cmp[%d]", range1.table_id_, to_cstring(range1.start_key_),
                  to_cstring(range2), ret);
              */
            }
            else if (range2.start_key_ == range2.end_key_)
            {
              ret = 0 - compare_range_with_key(range2.table_id_, range2.start_key_, range1);
              /*
              TBSYS_LOG(DEBUG, "table[%lu], key[%s], range[%s], cmp[%d]", range2.table_id_, to_cstring(range2.start_key_),
                  to_cstring(range1), ret);
              */
            }
            else
            {
              ret = range1.compare_with_endkey(range2);
            }
          }
          return ret;
        }
      };

      private:
        // construct MapKey
        int construct_mapkey(const common::ObNewRange & key, MapKey & Key, common::CacheItemHead *& local);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObCBtreeTable);
        /// lock for cache logic
        tbsys::CRWLock cache_lock_;
        //mutable tbsys::CThreadMutex cache_lock_;
        /// treemap implemention of common::ob_cache search engine
        ObBtreeMap<MapKey, common::CacheItemHead *> tree_map_;
        //ObStlMap<MapKey, common::CacheItemHead *> tree_map_;
    };

    template <class _key, class _value>
    int ObCBtreeTable<_key, _value>::init(int32_t slot_num)
    {
      tbsys::CWLockGuard lock(cache_lock_);
      return tree_map_.create(slot_num);
    }

    template <class _key, class _value>
    int ObCBtreeTable<_key, _value>::add(common::CacheItemHead & item, common::CacheItemHead *& old_item)
    {
      int ret = common::OB_SUCCESS;
      MapKey Key;
      Key.key_ = &item;
      old_item = NULL;
      tbsys::CWLockGuard lock(cache_lock_);
      ret = tree_map_.set(Key, &item, old_item);
      if (ret != common::OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "check btree set failed:ret[%d]", ret);
      }
      else if (old_item != NULL)
      {
        old_item->get_mother_block()->inc_ref();
      }
      return ret;
    }

    template <class _key, class _value>
    common::CacheItemHead * ObCBtreeTable<_key, _value>::get(const common::ObNewRange & key)
    {
      common::CacheItemHead * result = NULL;
      MapKey Key;
      common::CacheItemHead * local = NULL;
      int ret = construct_mapkey(key, Key, local);
      if (ret != common::OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "construct MapKey failed through key:ret[%d]", ret);
      }
      else
      {
        Key.key_ = local;
        tbsys::CRLockGuard lock(cache_lock_);
        ret = tree_map_.get(Key, result);
        if (result != NULL)
        {
          result->get_mother_block()->inc_ref();
        }
      }

      if (local != NULL)
      {
        ob_tc_free(local);
        local = NULL;
      }
      return result;
    }

    template <class _key, class _value>
    int64_t ObCBtreeTable<_key, _value>::get_item_num() const
    {
      tbsys::CRLockGuard lock(cache_lock_);
      return tree_map_.size();
    }

    template <class _key, class _value>
    void ObCBtreeTable<_key, _value>::remove(common::CacheItemHead & item)
    {
      MapKey Key;
      Key.key_ = &item;
      common::CacheItemHead * result = NULL;
      tbsys::CWLockGuard lock(cache_lock_);
      tree_map_.erase(Key, result);
      // do not dec ref of result because of cache recycle procedure will do
    }

    template <class _key, class _value>
    int ObCBtreeTable<_key, _value>::construct_mapkey(const common::ObNewRange & key, MapKey & Key,
      common::CacheItemHead *& local)
    {
      int ret = common::OB_SUCCESS;
      /*
      int64_t size = key.length();
      if ((NULL == key.ptr()) || (0 == size))
      {
        TBSYS_LOG(ERROR, "check key ptr failed:key[%p]", key.ptr());
        ret = common::OB_INPUT_PARAM_ERROR;
      }
      */
      //else
      //{
        int64_t size = key.get_serialize_size();
        local = (common::CacheItemHead *)common::ob_tc_malloc(sizeof(common::CacheItemHead) + size,
            oceanbase::common::ObModIds::OB_MS_LOCATION_CACHE);
        if (NULL == local)
        {
          TBSYS_LOG(ERROR, "ob_tc_malloc failed:size[%ld]", size);
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
        }
      //}

      // serialize range as key
      if (common::OB_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = key.serialize(local->key_, size, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "serialize the range failed:ret[%d]", ret);
        }
        else
        {
          //memcpy(local->key_, key.ptr(), size);
          local->key_size_ = static_cast<int32_t>(size);
          Key.key_ = local;
        }
      }
      return ret;
    }

    template <class _key, class _value>
    common::CacheItemHead * ObCBtreeTable<_key, _value>::remove(const common::ObNewRange & key)
    {
      common::CacheItemHead * result = NULL;
      MapKey Key;
      common::CacheItemHead * local = NULL;
      int ret = construct_mapkey(key, Key, local);
      if (ret != common::OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "construct MapKey failed through key:ret[%d]", ret);
      }
      else
      {
        tbsys::CWLockGuard lock(cache_lock_);
        tree_map_.erase(Key, result);
        if (result != NULL)
        {
          result->get_mother_block()->inc_ref();
        }
      }

      // free local data buffer
      if (local != NULL)
      {
        ob_tc_free(local);
        local = NULL;
      }
      return result;
    }
  }
}


#endif //OB_COMMON_BTREE_TABLE_H_

