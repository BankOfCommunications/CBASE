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
#ifndef __OB_UPDATESERVER_OB_INC_SEQ_H__
#define __OB_UPDATESERVER_OB_INC_SEQ_H__
#include "string.h"
#include "common/ob_define.h"
#include "common/ob_spin_rwlock.h"
#include "pthread.h"
#include "algorithm"
#include "tbsys.h"

namespace oceanbase
{
  namespace updateserver
  {
    class SeqLock
    {
      struct Item
      {
        uint64_t value_;
      } CACHE_ALIGNED;
      public:
        static const uint64_t N_THREAD = 128;
        static const uint64_t N_THREAD_MASK = N_THREAD - 1;
        Item done_seq_[N_THREAD];
        uint64_t do_seq_ CACHE_ALIGNED;
        SeqLock(): do_seq_(0)
        {
          memset(done_seq_, 0, sizeof(done_seq_));
          __sync_synchronize();
        }
        ~SeqLock() {}
        int64_t lock()
        {
          uint64_t seq = __sync_add_and_fetch(&do_seq_, 1);
          volatile uint64_t& last_done_seq = done_seq_[(seq-1) & N_THREAD_MASK].value_;
          while(last_done_seq < seq - 1)
          {
            PAUSE() ;
          }
          __sync_synchronize();
          return seq;
        }

        int64_t unlock(int64_t seq)
        {
          volatile uint64_t& this_done_seq = done_seq_[seq & N_THREAD_MASK].value_;
          __sync_synchronize();
          this_done_seq = seq;
          return seq;
        }
    } CACHE_ALIGNED;

    typedef common::DRWLock RWLock;
    // Note: 类的实现严格依赖于调用end与调用ref/deref互斥,并且调用end之后禁止再调用ref
    class RefCnt
    {
      const static int64_t N_THREAD = RWLock::N_THREAD;
      struct RefNode
      {
        int64_t n;
      } CACHE_ALIGNED;
      public:
        RefCnt() : end_flag_(false),
                   thread_num_(0)
        {
          int tmp_ret = pthread_key_create(&key_, NULL);
          if (0 != tmp_ret)
          {
            TBSYS_LOG(ERROR, "pthread_key_create fail ret=%d", tmp_ret);
          }
          pthread_spin_init(&deref_lock_, PTHREAD_PROCESS_PRIVATE);
          memset((void*)ref_nodes_, 0, sizeof(ref_nodes_));
        };
        ~RefCnt()
        {
          pthread_spin_destroy(&deref_lock_);
          pthread_key_delete(key_);
        };
      public:
        void born()
        {
          end_flag_ = false;
        };
        bool end()
        {
          bool bret = true;
          pthread_spin_lock(&deref_lock_);
          end_flag_ = true;
          __sync_synchronize();
          bret = (get_ref_cnt() == 0);
          pthread_spin_unlock(&deref_lock_);
          return bret;
        };
        void ref()
        {
          volatile RefNode* ref_node = (volatile RefNode*)pthread_getspecific(key_);
          if (NULL == ref_node)
          {
            ref_node = &ref_nodes_[__sync_fetch_and_add(&thread_num_, 1) % N_THREAD];
            int tmp_ret = pthread_setspecific(key_, (void*)ref_node);
            if (0 != tmp_ret)
            {
              TBSYS_LOG(ERROR, "pthread_setspecific fail ret=%d", tmp_ret);
            }
          }
          if (end_flag_)
          {
            TBSYS_LOG(ERROR, "unexpected error, end_flag must not be true");
          }
          __sync_fetch_and_add(&(ref_node->n), 1);
        };
        bool deref()
        {
          bool bret = false;
          volatile RefNode* ref_node = (volatile RefNode*)pthread_getspecific(key_);
          if (NULL == ref_node)
          {
            ref_node = &ref_nodes_[__sync_fetch_and_add(&thread_num_, 1) % N_THREAD];
            int tmp_ret = pthread_setspecific(key_, (void*)ref_node);
            if (0 != tmp_ret)
            {
              TBSYS_LOG(ERROR, "pthread_setspecific fail ret=%d", tmp_ret);
            }
            TBSYS_LOG(INFO, "deref when ref_node=NULL, maybe anothor thread call ref");
          }
          if (NULL != ref_node)
          {
            __sync_synchronize();
            if (end_flag_)
            {
              pthread_spin_lock(&deref_lock_);
              __sync_fetch_and_add(&(ref_node->n), -1);
              __sync_synchronize();
              bret = end_flag_ && (get_ref_cnt() == 0);
              pthread_spin_unlock(&deref_lock_);
            }
            else
            {
              __sync_fetch_and_add(&(ref_node->n), -1);
              __sync_synchronize();
            }
          }
          return bret;
        };
      protected:
        int64_t get_ref_cnt()
        {
          int64_t ref = 0;
          for(int64_t i = 0; i < std::min((int64_t)thread_num_, N_THREAD); i++)
          {
            ref += ref_nodes_[i].n;
          }
          return ref;
        }
      private:
        pthread_key_t key_                    CACHE_ALIGNED;
        volatile bool end_flag_               CACHE_ALIGNED;
        volatile int64_t thread_num_          CACHE_ALIGNED;
        pthread_spinlock_t deref_lock_;       CACHE_ALIGNED;
        volatile RefNode ref_nodes_[N_THREAD] CACHE_ALIGNED;
    };

    class LockedIncSeq
    {
      private:
        RWLock rwlock_;
        SeqLock seq_;
        volatile int64_t value_ CACHE_ALIGNED;
      public:
        LockedIncSeq(): seq_(), value_(0){}
        ~LockedIncSeq(){}
        int64_t get_seq(){ return seq_.do_seq_; }
        void update(int64_t& value)
        {
          rwlock_.wrlock();
          if(value_ < value)
          {
            value_ = value;
          }
          rwlock_.wrunlock();
        }

        int64_t next(int64_t& value)
        {
          int64_t seq = seq_.lock();
          rwlock_.rdlock();
          if (value > value_)
          {
            value_ = value;
          }
          else
          {
            value = ++value_;
          }
          rwlock_.rdunlock();
          return seq_.unlock(seq);
        }
    } CACHE_ALIGNED;
    namespace types
    {
      struct uint128_t
      {
        uint64_t lo;
        uint64_t hi;
      }
        __attribute__ (( __aligned__( 16 ) ));
    }

    inline bool cas128( volatile types::uint128_t * src, types::uint128_t cmp, types::uint128_t with )
    {
      bool result;
      __asm__ __volatile__
        (
          "\n\tlock cmpxchg16b %1"
          "\n\tsetz %0\n"
          : "=q" ( result ), "+m" ( *src ), "+d" ( cmp.hi ), "+a" ( cmp.lo )
          : "c" ( with.hi ), "b" ( with.lo )
          : "cc"
          );
      return result;
    }

    inline void load128 (__uint128_t& dest, types::uint128_t *src)
    {
      __asm__ __volatile__ ("\n\txor %%rax, %%rax;"
                            "\n\txor %%rbx, %%rbx;"
                            "\n\txor %%rcx, %%rcx;"
                            "\n\txor %%rdx, %%rdx;"
                            "\n\tlock cmpxchg16b %1;\n"
                            : "=&A"(dest)
                            : "m"(*src)
                            : "%rbx", "%rcx", "cc");
    }

#define CAS128(src, cmp, with) cas128((types::uint128_t*)(src), *((types::uint128_t*)&(cmp)), *((types::uint128_t*)&(with)))
#define LOAD128(dest, src) load128((__uint128_t&)(dest), (types::uint128_t*)(src))
    struct CasIncSeq
    {
      int64_t seq_;
      int64_t value_;
      CasIncSeq(): seq_(0), value_(0) {}
      CasIncSeq(CasIncSeq& inc_seq): seq_(inc_seq.seq_), value_(inc_seq.value_) {}
      ~CasIncSeq(){}
      int64_t get_seq(){ return seq_; }
      void update(int64_t& value)
      {
        CasIncSeq old;
        CasIncSeq tmp;
        while(true)
        {
          LOAD128(old, this);
          if(old.value_ >= value)
          {
            break;
          }
          else
          {
            tmp.seq_ = old.seq_;
            tmp.value_ = value;
            if (CAS128(this, old, tmp))
            {
              break;
            }
          }
        }
      }
      int64_t next(int64_t& value)
      {
        CasIncSeq tmp;
        CasIncSeq old;
        while(true)
        {
          LOAD128(old, this);
          tmp.seq_ = old.seq_ + 1;
          tmp.value_ = value > old.value_? value: old.value_ + 1;
          if (CAS128(this, old, tmp))
          {
            break;
          }
        }
        return tmp.seq_;
      }
    }__attribute__ (( __aligned__( 16 ) ));
    typedef CasIncSeq IncSeq;
  }; // end namespace updateserver
}; // end namespace oceanbase

#endif /* __OB_UPDATESERVER_OB_INC_SEQ_H__ */

