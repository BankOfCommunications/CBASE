/*
 * Copyright (C) 2012-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   Fusheng Han <yanran.hfs@taobao.com>
 *     - A counter which increments thread-local sub-counter
 *       and sum all sub-counters when getting counter value
 */
#ifndef OCEANBASE_COMMON_CMBTREE_BTREE_COUNTER_H_
#define OCEANBASE_COMMON_CMBTREE_BTREE_COUNTER_H_

#include "btree_define.h"
#include "btree_recycle_node.h"
#include "btree_recycle_pool.h"

namespace oceanbase
{
  namespace common
  {
    namespace cmbtree
    {
      template <class Alloc>
      class BtreeCounter
      {
        public:
          typedef BtreeThreadStore<int64_t, DefaultInitializer<int64_t> >
              TThreadInt64;
        public:
          inline          BtreeCounter           ();
          inline          ~BtreeCounter          ();
          inline void     inc                    ();
          inline void     inc                    (int64_t v);
          inline int64_t  value                  () const;
          inline void     reset                  ();
          inline int32_t  init                   ();
          inline void     destroy                ();

        protected:
          class ValueAdder
          {
            public:
              inline      ValueAdder             ();
              inline void operator()             (const void * ptr);
              inline int64_t sum                 ();
            protected:
              int64_t sum_;
          };

          class ValueReseter
          {
            public:
              inline void operator()             (void * ptr);
          };

        protected:
          TThreadInt64 counter_;
      };

      template <class Alloc>
      BtreeCounter<Alloc>::BtreeCounter()
        : counter_()
      {
      }

      template <class Alloc>
      BtreeCounter<Alloc>::~BtreeCounter()
      {
      }

      template <class Alloc>
      void BtreeCounter<Alloc>::inc()
      {
        inc(1);
      }

      template <class Alloc>
      void BtreeCounter<Alloc>::inc(int64_t v)
      {
        int64_t * tv = counter_.get();
        if (NULL == tv)
        {
          CMBTREE_LOG(ERROR, "BtreeCounter internal error, "
              "get from thread store returned NULL");
        }
        else
        {
          (*tv) += v;
        }
      }

      template <class Alloc>
      int64_t BtreeCounter<Alloc>::value() const
      {
        int64_t ret = 0;
        ValueAdder value_adder;
        int err = counter_.for_each_obj_ptr(value_adder);
        if (UNLIKELY(ERROR_CODE_OK != err))
        {
          CMBTREE_LOG(ERROR, "BtreeCounter internal error, "
              "for_each_obj_ptr returned error, err = %d", err);
        }
        else
        {
          ret = value_adder.sum();
        }
        return ret;
      }

      template <class Alloc>
      void BtreeCounter<Alloc>::reset()
      {
        ValueReseter value_reseter;
        int err = counter_.for_each_obj_ptr(value_reseter);
        if (UNLIKELY(ERROR_CODE_OK != err))
        {
          CMBTREE_LOG(ERROR, "BtreeCounter internal error, "
              "for_each_obj_ptr returned error, err = %d", err);
        }
      }

      template <class Alloc>
      int32_t BtreeCounter<Alloc>::init()
      {
        return counter_.init();
      }

      template <class Alloc>
      void BtreeCounter<Alloc>::destroy()
      {
        counter_.destroy();
      }

      template <class Alloc>
      BtreeCounter<Alloc>::ValueAdder::ValueAdder()
        : sum_(0)
      {
      }

      template <class Alloc>
      void BtreeCounter<Alloc>::ValueAdder::operator()(const void * ptr)
      {
        const int64_t * tv = reinterpret_cast<const int64_t *>(ptr);
        if (NULL == tv)
        {
          CMBTREE_LOG(ERROR, "ValueAdder internal error, "
              "sum operator encountered NULL");
        }
        else
        {
          sum_ += *tv;
        }
      }

      template <class Alloc>
      int64_t BtreeCounter<Alloc>::ValueAdder::sum()
      {
        return sum_;
      }

      template <class Alloc>
      void BtreeCounter<Alloc>::ValueReseter::operator()(void * ptr)
      {
        int64_t * tv = reinterpret_cast<int64_t *>(ptr);
        if (NULL == tv)
        {
          CMBTREE_LOG(ERROR, "ValueReseter internal error, "
              "reset operator encountered NULL");
        }
        else
        {
          *tv = 0;
        }
      }

    } // end namespace cmbtree
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_CMBTREE_BTREE_COUNTER_H_
