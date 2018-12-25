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
#ifndef __OB_COMMON_QLOCK_H__
#define __OB_COMMON_QLOCK_H__
#include "ob_define.h"
namespace oceanbase
{
  namespace common
  {
    struct QLock
    {
      enum State
      {
        EXCLUSIVE_BIT = 1UL<<31,
        UID_MASK = ~EXCLUSIVE_BIT
      };
      QLock(): n_ref_(0), uid_(0) {}
      ~QLock() {}
      void reset() {n_ref_ = 0; uid_ = 0;};
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
        if (uid_ & EXCLUSIVE_BIT)
        {
          err = OB_EAGAIN;
        }
        else
        {
          add_shared_ref((uint32_t*)&n_ref_);
          if (uid_ & EXCLUSIVE_BIT)
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
        if (uid & ~UID_MASK)
        {
          err = OB_INVALID_ARGUMENT;
        }
        else if (!__sync_bool_compare_and_swap(&uid_, 0, uid))
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
          __sync_synchronize();
          uid_ = 0;
        }
        return err;
      }

      int try_exclusive_lock(const uint32_t uid)
      {
        int err = OB_SUCCESS;
        if (uid & ~UID_MASK)
        {
          err = OB_INVALID_ARGUMENT;
        }
        else if (0 != n_ref_
                || !__sync_bool_compare_and_swap(&uid_, 0, uid|EXCLUSIVE_BIT))
        {
          err = OB_EAGAIN;
        }
        else if (0 != n_ref_)
        {
          err = OB_EAGAIN;
          __sync_synchronize();
          uid_ = 0;
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
        else
        {
          __sync_synchronize();
        }
        return err;
      }

      int try_exclusive_unlock(const uint32_t uid)
      {
        int err = OB_SUCCESS;
        uint32_t cur_uid = uid_;
        if (0 == (cur_uid & ~UID_MASK) || (cur_uid & UID_MASK) != uid)
        {
          err = OB_LOCK_NOT_MATCH;
        }
        else
        {
          __sync_synchronize();
          uid_ = 0;
        }
        return err;
      }

      int shared_lock(const uint32_t uid, const int64_t end_time = -1)
      {
        int err = OB_EAGAIN;
        while(OB_EAGAIN == (err = try_shared_lock(uid)) && !is_timeout(end_time))
        {
          PAUSE();
        }
        return err;
      }

      int shared_lock(const uint32_t uid, const int64_t end_time, const bool volatile &wait_flag)
      {
        int err = OB_EAGAIN;
        while(OB_EAGAIN == (err = try_shared_lock(uid)) && wait_flag && !is_timeout(end_time))
        {
          PAUSE();
        }
        return err;
      }

      int shared_unlock(const uint32_t uid)
      {
        return try_shared_unlock(uid);
      }

      bool is_exclusive_locked_by(const uint32_t uid) const
      {
        return (uid_ & ~UID_MASK) && (uid == (uid_ & UID_MASK));
      }

      uint32_t get_uid() const
      {
        return (uint32_t)(uid_ & UID_MASK);
      }

      // 多个线程拿着同一个uid去加互斥锁也没关系
      // 同一个线程拿着同一个uid反复加锁，只有第一次可以成功
      int exclusive_lock(const uint32_t uid, const int64_t end_time = -1)
      {
        int err = OB_EAGAIN;
        if (uid & ~UID_MASK || uid == 0)
        {
          err = OB_INVALID_ARGUMENT;
        }
        else
        {
          while(1)
          {
            if (__sync_bool_compare_and_swap(&uid_, 0, uid|EXCLUSIVE_BIT))
            {
              err = OB_SUCCESS;
              break;
            }
            if (is_timeout(end_time))
            {
              break;
            }
            PAUSE();
          }
        }
        if (OB_SUCCESS != err)
        {}
        else if (OB_SUCCESS != (err = wait_shared_lock_release(uid, end_time)))
        {
          uid_ = 0;
        }
        return err;
      }

      int exclusive_lock(const uint32_t uid, const int64_t end_time, const bool volatile &wait_flag)
      {
        int err = OB_EAGAIN;
        if (uid & ~UID_MASK || uid == 0)
        {
          err = OB_INVALID_ARGUMENT;
        }
        else
        {
          while(1)
          {
            if (__sync_bool_compare_and_swap(&uid_, 0, uid|EXCLUSIVE_BIT))
            {
              err = OB_SUCCESS;
              break;
            }
            if (!wait_flag || is_timeout(end_time))
            {
              break;
            }
            PAUSE();
          }
        }
        if (OB_SUCCESS != err)
        {}
        else if (OB_SUCCESS != (err = wait_shared_lock_release(uid, end_time, wait_flag)))
        {
          uid_ = 0;
        }
        return err;
      }

      int exclusive_unlock(const uint32_t uid)
      {
        return try_exclusive_unlock(uid);
      }

      // 多个线程拿着同一个uid去加互斥锁也没关系
      // 同一个线程拿着同一个uid反复加锁，只有第一次可以成功
      int share2exclusive_lock(const uint32_t uid, const int64_t end_time = -1)
      {
        int err = OB_EAGAIN;
        uint32_t cur_uid = 0;
        if (uid & ~UID_MASK || uid == 0)
        {
          err = OB_INVALID_ARGUMENT;
        }
        else
        {
          while(1)
          {
            cur_uid = uid_;
            // cur_uid == 0 表示没人加写锁，也没人加意向锁
            // cur_uid == uid 表示自己加了意向锁
            if (cur_uid != 0 && cur_uid != uid)
            {
              err = OB_DEAD_LOCK;
              TBSYS_LOG(DEBUG, "dead_lock detected, uid=%u, n_ref=%u", uid_, n_ref_);
              break;
            }
            else if (__sync_bool_compare_and_swap(&uid_, cur_uid, uid|EXCLUSIVE_BIT))
            {
              err = OB_SUCCESS;
              break;
            }
            if (is_timeout(end_time))
            {
              break;
            }
            PAUSE();
          }
        }
        if (OB_SUCCESS != err)
        {}
        else if (OB_SUCCESS != (err = shared_unlock(uid))) // 这一步不可能失败
        {}
        else if (OB_SUCCESS != (err = wait_shared_lock_release(uid, end_time)))
        {
          add_shared_ref((uint32_t*)&n_ref_);
          uid_ = cur_uid;
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
        while(OB_EAGAIN == (err = try_wait_shared_lock_release(uid)) && !is_timeout(end_time))
        {
          PAUSE();
        }
        return err;
      }

      int wait_shared_lock_release(const uint32_t uid, const int64_t end_time, const bool volatile &wait_flag)
      {
        int err = OB_EAGAIN;
        while(OB_EAGAIN == (err = try_wait_shared_lock_release(uid)) && wait_flag && !is_timeout(end_time))
        {
          PAUSE();
        }
        return err;
      }
    };

    class QLockGuard
    {
      public:
        QLockGuard(QLock &qlock, const uint32_t uid) : locked_(false),
                                                       qlock_(qlock),
                                                       uid_(uid)
        {
          if (!qlock_.is_exclusive_locked_by(uid_))
          {
            if (OB_SUCCESS != qlock_.exclusive_lock(uid_))
            {
              TBSYS_LOG(ERROR, "unexpected exclusive_lock failed uid=%u", uid_);
            }
            else
            {
              locked_ = true;
            }
          }
        };
        ~QLockGuard()
        {
          if (locked_)
          {
            qlock_.exclusive_unlock(uid_);
          }
        };
      private:
        bool locked_;
        QLock qlock_;
        uint32_t uid_;
    } __attribute__((aligned(8)));
  }; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_QLOCK_H__ */
