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
#ifndef __OB_COMMON_CMBTREE_QLOCK_H__
#define __OB_COMMON_CMBTREE_QLOCK_H__

#include "common/ob_define.h"
#include "tbsys.h"

#ifndef LIKELY
#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x)   (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x)   (x)
#define UNLIKELY(x) (x)
#endif
#endif

namespace oceanbase
{
  namespace common
  {
    namespace cmbtree
    {
#define CAS(x, old_v, new_v) __sync_bool_compare_and_swap(x, old_v, new_v)
      struct QLock
      {
        enum State
        {
          EXCLUSIVE_BIT = 1UL<<31,
          UID_MASK = ~EXCLUSIVE_BIT
        };
        QLock(): n_ref_(0), uid_(0) {}
        ~QLock() {}
        volatile uint32_t n_ref_;
        volatile uint32_t uid_;

        static inline bool is_timeout(const int64_t end_time)
        {
          return end_time > 0 && tbsys::CTimeUtil::getTime() > end_time;
        }

        static inline uint32_t add_shared_ref(uint32_t* ref)
        {
          return __sync_fetch_and_add(ref, 1);
        }

        static inline uint32_t del_shared_ref(uint32_t* ref)
        {
          return __sync_fetch_and_add(ref, -1);
        }

        int try_shared_lock(uint32_t uid)
        {
          int err = OB_SUCCESS;
          UNUSED(uid);
          if (UNLIKELY(uid_ & EXCLUSIVE_BIT))
          {
            err = OB_EAGAIN;
          }
          else
          {
            add_shared_ref((uint32_t*)&n_ref_);
            if (UNLIKELY(uid_ & EXCLUSIVE_BIT))
            {
              err = OB_EAGAIN;
              del_shared_ref((uint32_t*)&n_ref_);
            }
          }
          return err;
        }

        int try_shared_unlock(uint32_t uid)
        {
          int err = OB_SUCCESS;
          UNUSED(uid);
          del_shared_ref((uint32_t*)&n_ref_);
          return err;
        }

        int try_upgrade_lock(const uint32_t uid)
        {
          int err = OB_SUCCESS;
          uint32_t cur_uid = uid_;
          if (0 != (uid & EXCLUSIVE_BIT))
          {
            err = OB_INVALID_ARGUMENT;
          }
          else if (uid != cur_uid && (cur_uid != 0 || !CAS(&uid_, cur_uid, uid)))
          {
            err = OB_EAGAIN;
          }
          return err;
        }

        int try_upgrade_unlock(const uint32_t uid)
        {
          int err = OB_SUCCESS;
          if (uid != uid_)
          {
            err = OB_LOCK_NOT_MATCH;
          }
          else
          {
            uid_ = 0;
          }
          return err;
        }

        int try_exclusive_lock(const uint32_t uid)
        {
          int err = OB_SUCCESS;
          uint64_t cur_uid = uid_;
          if (0 != (uid & EXCLUSIVE_BIT))
          {
            err = OB_INVALID_ARGUMENT;
          }
          else if (0 != cur_uid && (cur_uid & UID_MASK) != uid)
          {
            err = OB_EAGAIN;
          }
          else if (!CAS(&uid_, cur_uid, uid|EXCLUSIVE_BIT))
          {
            err = OB_EAGAIN;
          }
          return err;
        }

        int try_wait_shared_lock_release(const uint32_t uid)
        {
          int err = OB_SUCCESS;
          if ((uid_ & UID_MASK) != uid)
          {
            err = OB_LOCK_NOT_MATCH;
          }
          else if (0 != n_ref_)
          {
            err = OB_EAGAIN;
          }
          return err;
        }

        int try_exclusive_unlock(const uint32_t uid)
        {
          int err = OB_SUCCESS;
          if (0 == (uid_ & EXCLUSIVE_BIT) || (uid_ & UID_MASK) != uid)
          {
            err = OB_LOCK_NOT_MATCH;
          }
          else
          {
            uid_ = 0;
          }
          return err;
        }

        int shared_lock(const uint32_t uid, const int64_t end_time = -1)
        {
          int err = OB_EAGAIN;
          while(!is_timeout(end_time) && OB_EAGAIN == (err = try_shared_lock(uid)))
            ;
          return err;
        }

        int shared_unlock(const uint32_t uid)
        {
          return try_shared_unlock(uid);
        }

        int exclusive_lock(const uint32_t uid, const int64_t end_time = -1)
        {
          int err = OB_EAGAIN;
          uint64_t cur_uid = 0;
          if (0 != (uid & EXCLUSIVE_BIT))
          {
            err = OB_INVALID_ARGUMENT;
          }
          while(!is_timeout(end_time) && OB_EAGAIN == err)
          {
            cur_uid = uid_;
            if (0 != cur_uid && (cur_uid & UID_MASK) != uid)
            {
              err = OB_EAGAIN;
            }
            else if (!CAS(&uid_, cur_uid, uid|EXCLUSIVE_BIT))
            {
              err = OB_EAGAIN;
            }
            else
            {
              err = OB_SUCCESS;
            }
          }
          if (OB_SUCCESS != err)
          {}
          else if (OB_SUCCESS != (err = wait_shared_lock_release(uid, end_time)))
          {
            uid_ = static_cast<uint32_t>(cur_uid);
          }
          return err;
        }

        int exclusive_unlock(const uint32_t uid)
        {
          return try_exclusive_unlock(uid);
        }

        int try_shared2exclusive_lock(const uint32_t uid)
        {
          int err = OB_EAGAIN;
          uint64_t cur_uid = 0;
          if (0 != (uid & EXCLUSIVE_BIT))
          {
            err = OB_INVALID_ARGUMENT;
          }
          else
          {
            cur_uid = uid_;
            if (0 != cur_uid && (cur_uid & UID_MASK) != uid)
            {
              err = OB_EAGAIN;
            }
            else if (!CAS(&uid_, cur_uid, uid|EXCLUSIVE_BIT))
            {
              err = OB_EAGAIN;
            }
            else
            {
              shared_unlock(uid); // 这一步不可能失败
              if (OB_SUCCESS != (err = try_wait_shared_lock_release(uid)))
              {
                add_shared_ref((uint32_t*)&n_ref_);
                uid_ = static_cast<uint32_t>(cur_uid);
              }
            }
          }
          return err;
        }

        int share2exclusive_lock(const uint32_t uid, const int64_t end_time = -1)
        {
          int err = OB_EAGAIN;
          uint64_t cur_uid = 0;
          if (0 != (uid & EXCLUSIVE_BIT))
          {
            err = OB_INVALID_ARGUMENT;
          }
          while(!is_timeout(end_time) && OB_EAGAIN == err)
          {
            cur_uid = uid_;
            if (0 != cur_uid && (cur_uid & UID_MASK) != uid)
            {
              err = OB_DEAD_LOCK;
              TBSYS_LOG(DEBUG, "dead_lock detected, uid=%u, n_ref=%u", uid_, n_ref_);
            }
            else if (!CAS(&uid_, cur_uid, uid|EXCLUSIVE_BIT))
            {
              err = OB_EAGAIN;
            }
            else
            {
              err = OB_SUCCESS;
            }
          }
          if (OB_SUCCESS != err)
          {}
          else if (OB_SUCCESS != (err = shared_unlock(uid))) // 这一步不可能失败
          {}
          else if (OB_SUCCESS != (err = wait_shared_lock_release(uid, end_time)))
          {
            add_shared_ref((uint32_t*)&n_ref_);
            uid_ = static_cast<uint32_t>(cur_uid);
          }
          return err;
        }

        int exclusive2shared_lock(const uint32_t uid)
        {
          int err = OB_EAGAIN;
          add_shared_ref((uint32_t*)&n_ref_);
          if (OB_SUCCESS != (err = exclusive_unlock(uid)))
          {
            del_shared_ref((uint32_t*)&n_ref_);
          }
          return err;
        }

        int wait_shared_lock_release(const uint32_t uid, const int64_t end_time = -1)
        {
          int err = OB_EAGAIN;
          while(!is_timeout(end_time) && OB_EAGAIN == (err = try_wait_shared_lock_release(uid)))
          {
            asm ("PAUSE");
          }
          return err;
        }
      };
    }; // end namespace cmbtree
  }; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_CMBTREE_QLOCK_H__ */
