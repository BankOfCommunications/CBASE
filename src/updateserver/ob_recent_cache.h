/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#ifndef __OB_UPDATESERVER_OB_RECENT_CACHE_H__
#define __OB_UPDATESERVER_OB_RECENT_CACHE_H__
#include "common/ob_define.h"
#include "common/ob_atomic_type.h"
#include "tbsys.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace updateserver
  {
    template<typename K, typename V>
    class ObRecentCache
    {
      public:
        struct AssocItem : public ObAtomicType<AssocItem>
        {
          AssocItem(): k(), v() {}
          ~AssocItem(){}
          K k;
          V v;
        };

      ObRecentCache(): n_items_(0), items_(NULL)
      {}

      ~ObRecentCache()
      {
        if (NULL != items_)
        {
          delete []items_;
          items_ = NULL;
        }
      }

      int init(int64_t n_items)
      {
        int err = OB_SUCCESS;
        if (NULL != items_)
        {
          err = OB_INIT_TWICE;
        }
        else if (0 > n_items)
        {
          err = OB_INVALID_ARGUMENT;
        }
        else if (n_items > 0 && NULL == (items_ = new(std::nothrow)AssocItem[n_items]))
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          n_items_ = n_items;
        }
        return err;
      }

      int get(const K k, V& v) const
      {
        int err = OB_SUCCESS;
        AssocItem item;
        if (NULL == items_)
        {
          err = OB_ENTRY_NOT_EXIST;
        }
        else if (OB_SUCCESS != (err = items_[k % n_items_].copy(item))
                 && OB_EAGAIN != err)
        {
          TBSYS_LOG(ERROR, "items_[%ld].copy()=>%d", (int64_t)k, err);
        }
        else if (OB_EAGAIN == err || k != item.k)
        {
          err = OB_ENTRY_NOT_EXIST;
        }
        else
        {
          v = item.v;
        }
        return err;
      }

      int add(const K k, const V& v)
      {
        int err = OB_SUCCESS;
        AssocItem item;
        item.k = k;
        item.v = v;
        if (NULL == items_ || items_[k % n_items_].k >= k)
        {} // do nothing
        else if(OB_SUCCESS != (err = items_[k % n_items_].set(item))
                && OB_EAGAIN != err)
        {
          TBSYS_LOG(ERROR, "items[%ld].set()=>%d", k, err);
        }
        else if (OB_EAGAIN == err)
        {
          err = OB_SUCCESS;
        }
        return err;
      }
      private:
        DISALLOW_COPY_AND_ASSIGN(ObRecentCache);
        int64_t n_items_;
        AssocItem* items_;
    };
  }; // end namespace updateserver
}; // end namespace oceanbase

#endif /* __OB_UPDATESERVER_OB_RECENT_CACHE_H__ */
