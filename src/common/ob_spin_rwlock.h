////===================================================================
 //
 // ob_spin_rwlock.h / common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2011-4-21 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_COMMON_SPIN_RWLOCK_H_
#define  OCEANBASE_COMMON_SPIN_RWLOCK_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include "tbsys.h"
#include "ob_atomic.h"
#include "ob_define.h"
#include "qlock.h"
#include "tbsys.h"

namespace oceanbase
{
  namespace common
  {
    class SpinRWLock
    {
      public:
        SpinRWLock() : ref_cnt_(0), wait_write_(0)
        {
        };
        ~SpinRWLock()
        {
          if (0 != ref_cnt_ || 0 != wait_write_)
          {
            TBSYS_LOG(ERROR, "invalid ref_cnt=%ld or wait_write_=%ld", ref_cnt_, wait_write_);
          }
        };
      public:
        inline bool try_rdlock()
        {
          bool bret = false;
          int64_t tmp = 0;
          while (0 <= (tmp = ref_cnt_))
          {
            int64_t nv = tmp + 1;
            if (tmp == (int64_t)atomic_compare_exchange((uint64_t*)&ref_cnt_, nv, tmp))
            {
              bret = true;
              break;
            }
          }
          return bret;
        };
        inline int rdlock()
        {
          int ret = common::OB_SUCCESS;
          int64_t tmp = 0;
          while (true)
          {
            tmp = ref_cnt_;
            if (0 > tmp || 0 < wait_write_)
            {
              // 写优先
              asm("pause");
              continue;
            }
            else
            {
              int64_t nv = tmp + 1;
              if (tmp == (int64_t)atomic_compare_exchange((uint64_t*)&ref_cnt_, nv, tmp))
              {
                break;
              }
              asm("pause");
            }
          }
          return ret;
        };
        inline int wrlock()
        {
          int ret = common::OB_SUCCESS;
          int64_t tmp = 0;
          atomic_inc((uint64_t*)&wait_write_);
          while (true)
          {
            tmp = ref_cnt_;
            if (0 != tmp)
            {
              asm("pause");
              continue;
            }
            else
            {
              int64_t nv = -1;
              if (tmp == (int64_t)atomic_compare_exchange((uint64_t*)&ref_cnt_, nv, tmp))
              {
                break;
              }
              asm("pause");
            }
          }
          atomic_dec((uint64_t*)&wait_write_);
          return ret;
        };
        inline bool try_wrlock()
        {
          bool bret = false;
          int64_t tmp = ref_cnt_;
          if (0 == tmp)
          {
            int64_t nv = -1;
            if (tmp == (int64_t)atomic_compare_exchange((uint64_t*)&ref_cnt_, nv, tmp))
            {
              bret = true;
            }
          }
          return bret;
        };
        inline int unlock()
        {
          int ret = common::OB_SUCCESS;
          int64_t tmp = 0;
          while (true)
          {
            tmp = ref_cnt_;
            if (0 == tmp)
            {
              TBSYS_LOG(ERROR, "need not unlock ref_cnt=%ld wait_write=%ld", ref_cnt_, wait_write_);
              break;
            }
            else if (-1 == tmp)
            {
              int64_t nv = 0;
              if (tmp == (int64_t)atomic_compare_exchange((uint64_t*)&ref_cnt_, nv, tmp))
              {
                break;
              }
            }
            else if (0 < tmp)
            {
              int64_t nv = tmp - 1;
              if (tmp == (int64_t)atomic_compare_exchange((uint64_t*)&ref_cnt_, nv, tmp))
              {
                break;
              }
            }
            else
            {
              TBSYS_LOG(ERROR, "invalid ref_cnt=%ld", ref_cnt_);
            }
          }
          return ret;
        };
      private:
        volatile int64_t ref_cnt_;
        volatile int64_t wait_write_;
    };

    class SpinRLockGuard
    {
      public: 
        SpinRLockGuard(SpinRWLock& lock) 
          : lock_(lock) 
        { 
          lock_.rdlock();
        }
        ~SpinRLockGuard()
        {
          lock_.unlock();
        }
      private:
        SpinRWLock& lock_;
    };

    class SpinWLockGuard
    {
      public: 
        SpinWLockGuard(SpinRWLock& lock) 
          : lock_(lock) 
        { 
          lock_.wrlock();
        }
        ~SpinWLockGuard()
        {
          lock_.unlock();
        }
      private:
        SpinRWLock& lock_;
    };

    struct DRWLock
    {
      const static int64_t N_THREAD = 4096;
      volatile int64_t read_ref_[N_THREAD][CACHE_ALIGN_SIZE/sizeof(int64_t)];
      volatile int64_t thread_num_;
      volatile uint64_t write_uid_ CACHE_ALIGNED;
      pthread_key_t key_ CACHE_ALIGNED;
      DRWLock()
      {
        write_uid_ = 0;
        memset((void*)read_ref_, 0, sizeof(read_ref_));
        pthread_key_create(&key_, NULL);
      }
      ~DRWLock()
      {
        pthread_key_delete(key_);
      }
      void rdlock()
      {
        volatile int64_t* ref = (volatile int64_t*)pthread_getspecific(key_);
        if (NULL == ref)
        {
          pthread_setspecific(key_, (void*)(ref = read_ref_[__sync_fetch_and_add(&thread_num_, 1) % N_THREAD]));
        }
        while(true)
        {
          if (0 == write_uid_)
          {
            __sync_fetch_and_add(ref, 1);
            if (0 == write_uid_)
            {
              break;
            }
            __sync_fetch_and_sub(ref, 1);
          }
          PAUSE();
        }
      }
      void rdunlock()
      {
        int64_t* ref = (int64_t*)pthread_getspecific(key_);
        __sync_synchronize();
        __sync_fetch_and_sub(ref, 1);
      }
      void wrlock()
      {
        while(!__sync_bool_compare_and_swap(&write_uid_, 0, 1))
          ;
        for(int64_t i = 0; i < std::min((int64_t)thread_num_, N_THREAD); i++)
        {
          while(*read_ref_[i] > 0)
            ;
        }
        write_uid_ = 2;
        __sync_synchronize();
      }
      void wrunlock()
      {
        __sync_synchronize();
        write_uid_ = 0;
      }
      void unlock()
      {
        if (2 == write_uid_)
        {
          wrunlock();
        }
        else
        {
          rdunlock();
        }
      }
    } CACHE_ALIGNED;
  }
}

#endif //OCEANBASE_COMMON_SPIN_RWLOCK_H_

