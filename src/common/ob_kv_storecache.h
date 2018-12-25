/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_kv_storecache.h is for what ...
 *
 *  Description
 *
 * 无锁的lrucache实现
 *
 * ITEM_SIZE 为平均每个缓存对象的大小
 * 在没有深拷贝的情况下可以设为sizeof(Key) + sizeof(Value)
 *
 * 缓存对象可批量淘汰 BLOCK_SIZE 可以取默认值为1M
 * 缓存对象不可批量淘汰 BLOCK_SIZE 取值为ITEM_SIZE的大小
 *
 * BLOCK_SIZE 取值小于128K的时候 为了避免大量小内存的分配
 * 需要使用 KVStoreCacheComponent::MultiObjFreeList 作为内存分配器
 * 否则可以使用默认的 KVStoreCacheComponent::SingleObjFreeList 作为内存分配器
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef  OCEANBASE_COMMON_KV_STORE_CACHE_H_
#define  OCEANBASE_COMMON_KV_STORE_CACHE_H_

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "hash/ob_hashmap.h"
#include "hash/ob_hashutils.h"
#include "ob_define.h"
#include "ob_mod_define.h"
#include "ob_malloc.h"
#include "ob_atomic.h"
#include "ob_thread_objpool.h"
#include "ob_trace_log.h"
#include "ob_rowkey.h"
namespace oceanbase
{
  namespace common
  {
    namespace KVStoreCacheComponent
    {
      struct NotNeedDeepCopyTag {};
      struct ObStringDeepCopyTag {};
      struct ObRowkeyDeepCopyTag {};
      template <class T>
      struct traits
      {
        typedef NotNeedDeepCopyTag Tag;
      };

      template <>
      struct traits<ObString>
      {
        typedef ObStringDeepCopyTag Tag;
      };

      template <>
      struct traits<ObRowkey>
      {
        typedef ObRowkeyDeepCopyTag Tag;
      };
      template <class T>
      const char *log_str(const T &obj)
      {
        UNUSED(obj);
        return "nil";
      }

      template <class T>
      T *copy(const T &other, char *buffer)
      {
        return do_copy(other, buffer, typename traits<T>::Tag());
      }

      template <class T>
      int32_t size(const T &data)
      {
        return do_size(data, typename traits<T>::Tag());
      }

      template <class T>
      void destroy(T *data)
      {
        do_destroy(data, typename traits<T>::Tag());
      }

      template <class T>
      T *do_copy(const T &other, char *buffer, NotNeedDeepCopyTag)
      {
        return new(buffer) T(other);
      }

      template <class T>
      int32_t do_size(const T &data, NotNeedDeepCopyTag)
      {
        return sizeof(data);
      }

      template <class T>
      void do_destroy(T *data, NotNeedDeepCopyTag)
      {
        data->~T();
      }

      inline ObString *do_copy(const ObString &other, char *buffer, ObStringDeepCopyTag)
      {
        ObString *ret = new(buffer) ObString(other.size(), other.length(),
                                              (NULL == other.ptr()) ? NULL : (buffer + sizeof(ObString)));
        if (NULL != ret
            && NULL != ret->ptr()
            && NULL != other.ptr())
        {
          memcpy(ret->ptr(), other.ptr(), other.length());
        }
        return ret;
      }

      inline int32_t do_size(const ObString &data, ObStringDeepCopyTag)
      {
        return static_cast<int32_t>(sizeof(ObString) + std::max(data.size(), data.length()));
      }

      inline void do_destroy(ObString *data, ObStringDeepCopyTag)
      {
        data->~ObString();
      }
      class BufferAllocator
      {
        public:
          explicit BufferAllocator(char *buffer) : buffer_(buffer), pos_(0) {};
          inline char *alloc(const int64_t size)
          {
            char *ret = NULL;
            if (NULL != buffer_)
            {
              ret = buffer_ + pos_;
              pos_ += size;
            }
            return ret;
          };
        private:
          char *buffer_;
          int64_t pos_;
      };

      inline ObRowkey *do_copy(const ObRowkey &other, char *buffer, ObRowkeyDeepCopyTag)
      {
        ObRowkey *ret = new(buffer) ObRowkey();
        if (NULL != ret)
        {
          BufferAllocator allocator(buffer + sizeof(ObRowkey));
          int tmp_ret = other.deep_copy(*ret, allocator);
          if (OB_SUCCESS != tmp_ret)
          {
            TBSYS_LOG(WARN, "deep_copy rowkey fail ret=%d", tmp_ret);
            ret = NULL;
          }
        }
        return ret;
      }

      inline int32_t do_size(const ObRowkey &data, ObRowkeyDeepCopyTag)
      {
        return (static_cast<int32_t>(sizeof(ObRowkey)) + static_cast<int32_t>(data.get_deep_copy_size()));
      }

      inline void do_destroy(ObRowkey *data, ObRowkeyDeepCopyTag)
      {
        data->~ObRowkey();
      }

      struct DefaultAllocator
      {
        void *alloc(const int32_t nbyte) {return ob_tc_malloc(nbyte, ObModIds::OB_KVSTORE_CACHE);};
        void free(void *ptr) {ob_tc_free(ptr, ObModIds::OB_KVSTORE_CACHE);};
      };

      ////////////////////////////////////////////////////////////////////////////////////////////////////

      class StoreHandle
      {
        enum
        {
          FREE = 0,
          STORED = 1,
          LOCKED = 2,
        };
        public:
          StoreHandle() : seq_num(0), kv_pos(0), stat(FREE), mem_block(NULL), mb_infos_pos(0)
          {
          };
          StoreHandle copy_without_stat() const
          {
            StoreHandle ret;
            ret.seq_num = seq_num;
            ret.kv_pos = kv_pos;
            ret.stat = (NULL == mem_block) ? FREE : STORED;
            ret.mem_block = mem_block;
            ret.mb_infos_pos = mb_infos_pos;
            return ret;
          };
        public:
          int64_t seq_num;
          int32_t kv_pos;
          int32_t stat;
          void *mem_block;
          int64_t mb_infos_pos;
      };

      template <class Key, class Value, int64_t BlockSize, bool UseStaticBuf = true,
        class Allocator = DefaultAllocator>
      class MemBlock
      {
        union AtomicInt64
        {
          volatile uint64_t atomic;
          struct
          {
            int32_t buffer;
            int32_t pairs;
          };
          struct
          {
            int32_t ref;
            int32_t seq;
          };
        };
        struct KVPair
        {
          uint32_t magic;
          int32_t  size;
          Key*     first;
          Value*   second;

          KVPair() : magic(KVPAIR_MAGIC_NUM), size(0), first(NULL), second(NULL)
          {

          }
        };
        public:
          static const int64_t MEM_BLOCK_SIZE = BlockSize + sizeof(KVPair);
          static const int64_t ALIGN_SIZE = sizeof(size_t);
          static const uint32_t KVPAIR_MAGIC_NUM = 0x4B564B56;  //"KVKV"
        public:
          MemBlock()
          : next_(NULL), get_cnt_(0), last_time_(0), info_pos_(-1),
            payload_size_(0), buffer_(NULL)
          {
            atomic_pos_.buffer = 0;
            atomic_pos_.pairs = 0;
            atomic_num_.ref = 0;
          };
          ~MemBlock()
          {
            reset();
          };
          template <class Adapter>
          void reset(Adapter *adapter)
          {
            int32_t offset = 0;
            if (NULL != adapter)
            {
              for (int32_t i = 0; i < atomic_pos_.pairs; i++)
              {
                if (NULL != reinterpret_cast<KVPair*>(&buffer_[offset])->first)
                {
                  adapter->release(reinterpret_cast<KVPair*>(&buffer_[offset])->first);
                }
                offset += reinterpret_cast<KVPair*>(&buffer_[offset])->size;
              }
            }
          };
          void reset()
          {
            int32_t offset = 0;
            for (int32_t i = 0; i < atomic_pos_.pairs; i++)
            {
              if (NULL != reinterpret_cast<KVPair*>(&buffer_[offset])->first)
              {
                destroy(reinterpret_cast<KVPair*>(&buffer_[offset])->first);
              }
              if (NULL != reinterpret_cast<KVPair*>(&buffer_[offset])->second)
              {
                destroy(reinterpret_cast<KVPair*>(&buffer_[offset])->second);
              }
              offset += reinterpret_cast<KVPair*>(&buffer_[offset])->size;
            }
            /**
             * we can ensure the offset of buffer_ member is 8 bytes
             * aligned, and the start address of buffer_ member is 8 bytes
             * aligned, so we can new instance at the beginning of buffer_,
             * needn't align the start address of buffer_.
             */
            atomic_pos_.buffer = 0;
            atomic_pos_.pairs = 0;
            atomic_num_.ref = 0;
            get_cnt_ = 0;
            last_time_ = 0;
            info_pos_ = -1;
            payload_size_ = 0;
          };
          static int64_t upper_align(int64_t input, int64_t align)
          {
            int64_t ret = input;
            ret = (input + align - 1) & ~(align - 1);
            return ret;
          };
          static int64_t get_align_size(const Key &key, const Value &value)
          {
            return upper_align(sizeof(KVPair) + size(key) + size(value), ALIGN_SIZE);
          };
          int64_t get_payload_size()
          {
            return payload_size_;
          }
          int64_t get_info_pos() const
          {
            return info_pos_;
          };
          void set_info_pos(const int64_t info_pos)
          {
            info_pos_ = info_pos;
          };
          void inc_get_cnt()
          {
            atomic_inc((uint64_t*)&get_cnt_);
          };
          void set_last_time(const int64_t last_time)
          {
            atomic_exchange((uint64_t*)&(last_time_), last_time);
          };
          int64_t get_cnt() const
          {
            return get_cnt_;
          };
          int64_t last_time() const
          {
            return last_time_;
          };
          MemBlock *get_next() const
          {
            return next_;
          };
          void set_next(MemBlock *next)
          {
            next_ = next;
          };
          int32_t get_ref_cnt() const
          {
            return atomic_num_.ref;
          };
          int32_t get_seq_num() const
          {
            return atomic_num_.seq;
          };
          int32_t inc_ref_cnt()
          {
            AtomicInt64 atomic_old = {0};
            AtomicInt64 atomic_new = {0};
            AtomicInt64 atomic_cmp = {0};
            while (true)
            {
              atomic_old.atomic = atomic_num_.atomic;
              atomic_new.atomic = atomic_old.atomic;
              atomic_cmp.atomic = atomic_old.atomic;

              atomic_new.ref += 1;

              if (atomic_old.atomic == atomic_compare_exchange(&(atomic_num_.atomic), atomic_new.atomic, atomic_cmp.atomic))
              {
                break;
              }
            }
            return atomic_new.ref;
          };
          bool check_seq_num_and_inc_ref_cnt(const int32_t seq_num)
          {
            bool bret = false;
            AtomicInt64 atomic_old = {0};
            AtomicInt64 atomic_new = {0};
            AtomicInt64 atomic_cmp = {0};
            while (true)
            {
              atomic_old.atomic = atomic_num_.atomic;
              atomic_new.atomic = atomic_old.atomic;
              atomic_cmp.atomic = atomic_old.atomic;

              if (seq_num != atomic_old.seq)
              {
                break;
              }
              if (0 == atomic_old.ref)
              {
                break;
              }
              else if (0 > atomic_old.ref)
              {
                TBSYS_LOG(ERROR, "unexpect ref_cnt=%d seq_num=%d this=%p info_pos=%ld",
                          atomic_old.ref, atomic_old.seq, this, info_pos_);
                break;
              }

              atomic_new.ref += 1;
              if (atomic_old.atomic == atomic_compare_exchange(&(atomic_num_.atomic), atomic_new.atomic, atomic_cmp.atomic))
              {
                bret = true;
                break;
              }
            }
            return bret;
          };
          bool check_and_inc_ref_cnt()
          {
            bool bret = false;
            AtomicInt64 atomic_old = {0};
            AtomicInt64 atomic_new = {0};
            AtomicInt64 atomic_cmp = {0};
            while (true)
            {
              atomic_old.atomic = atomic_num_.atomic;
              atomic_new.atomic = atomic_old.atomic;
              atomic_cmp.atomic = atomic_old.atomic;

              if (0 == atomic_old.ref)
              {
                break;
              }
              else if (0 > atomic_old.ref)
              {
                TBSYS_LOG(ERROR, "unexpect ref_cnt=%d seq_num=%d this=%p info_pos=%ld",
                          atomic_old.ref, atomic_old.seq, this, info_pos_);
                break;
              }

              atomic_new.ref += 1;
              if (atomic_old.atomic == atomic_compare_exchange(&(atomic_num_.atomic), atomic_new.atomic, atomic_cmp.atomic))
              {
                bret = true;
                break;
              }
            }
            return bret;
          };
          int32_t dec_ref_cnt_and_inc_seq_num()
          {
            AtomicInt64 atomic_old = {0};
            AtomicInt64 atomic_new = {0};
            AtomicInt64 atomic_cmp = {0};
            while (true)
            {
              atomic_old.atomic = atomic_num_.atomic;
              atomic_new.atomic = atomic_old.atomic;
              atomic_cmp.atomic = atomic_old.atomic;

              atomic_new.ref -= 1;
              if (0 == atomic_new.ref)
              {
                atomic_new.seq += 1;
              }
              else if (0 > atomic_new.ref)
              {
                TBSYS_LOG(ERROR, "unexpect ref_cnt=%d seq_num=%d this=%p info_pos=%ld",
                          atomic_new.ref, atomic_new.seq, this, info_pos_);
                // no break; keep the ref_cnt
              }

              if (atomic_old.atomic == atomic_compare_exchange(&(atomic_num_.atomic), atomic_new.atomic, atomic_cmp.atomic))
              {
                break;
              }
            }
            return atomic_new.ref;
          };
          int store(const Key &key, const Value &value, int32_t &pos)
          {
            /**
             * we could create key and value struct instance in memblock, if
             * we can ensure that the start address of each block data is 8
             * bytes aligned, and the compiler can ensure the struct size is
             * 8 bytes aligned on 64 bit system, then we create key and
             * value struct instance at the beginning of block data, so the
             * start address of key instance or value instance is 8 bytes
             * aligned. it can make the cpu run faster than unaligned
             * instance.
             */
            int ret = OB_SUCCESS;
            int32_t align_kv_size = static_cast<int32_t>(get_align_size(key, value));
            AtomicInt64 old_atomic_pos = {0};
            AtomicInt64 new_atomic_pos = {0};
            while (true)
            {
              old_atomic_pos.atomic = atomic_pos_.atomic;
              new_atomic_pos.atomic = old_atomic_pos.atomic;
              if (old_atomic_pos.buffer + align_kv_size > payload_size_)
              {
                ret = OB_BUF_NOT_ENOUGH;
                break;
              }
              else
              {
                new_atomic_pos.buffer += align_kv_size;
                new_atomic_pos.pairs += 1;
                if (old_atomic_pos.atomic == atomic_compare_exchange(
                  &(atomic_pos_.atomic), new_atomic_pos.atomic, old_atomic_pos.atomic))
                {
                  break;
                }
              }
            }
            if (OB_SUCCESS == ret)
            {
              KVPair kv_pair;
              kv_pair.size = align_kv_size;
              Key *new_key = copy(key, &(buffer_[old_atomic_pos.buffer + sizeof(KVPair)]));
              Value *new_value = copy(value, &(buffer_[old_atomic_pos.buffer + sizeof(KVPair) + size(key)]));
              if (NULL == new_key
                  || NULL == new_value)
              {
                TBSYS_LOG_US(WARN, "copy fail, new_key=%p new_value=%p buffer_pos=%d pairs_pos=%d "
                          "old_buffer_pos=%d old_pairs_pos=%d this=%p key_size=%d value_size=%d",
                          new_key, new_value, atomic_pos_.buffer, atomic_pos_.pairs,
                          old_atomic_pos.buffer, old_atomic_pos.pairs, this, size(key), size(value));
                if (NULL != new_key)
                {
                  destroy(new_key);
                }
                if (NULL != new_value)
                {
                  destroy(new_value);
                }

                /**
                 * NOTE: even through copy key or value failed, we could assign
                 * the size and magic field of KVPair instance correctly and
                 * copy KVPair instance into memblock, because we could traverse
                 * the memblock by KVPair instance, it stores the size of
                 * current kv data, (sizeof(KVPair) + size(key) + size(value))
                 */
                kv_pair.first = NULL;
                kv_pair.second = NULL;
                memcpy(&(buffer_[old_atomic_pos.buffer]), &kv_pair, sizeof(KVPair));

                ret = OB_ERROR;
              }
              else
              {
                kv_pair.first = new_key;
                kv_pair.second = new_value;
                memcpy(&(buffer_[old_atomic_pos.buffer]), &kv_pair, sizeof(KVPair));
                pos = old_atomic_pos.buffer;
              }
            }
            return ret;
          };
          int get_next(int32_t &pos, Key *&pkey, Value *&pvalue)
          {
            int ret = OB_SUCCESS;
            KVPair* kv_pair = NULL;
            if (pos >= atomic_pos_.buffer || pos < 0)
            {
              ret = OB_ENTRY_NOT_EXIST;
            }
            else
            {
              kv_pair = reinterpret_cast<KVPair*>(&buffer_[pos]);
              int32_t tmp_pos = pos + kv_pair->size;
              if (NULL != kv_pair && kv_pair->magic != KVPAIR_MAGIC_NUM)
              {
                TBSYS_LOG(ERROR, "invalid kv pair magic, expected=%u, real=%u",
                          KVPAIR_MAGIC_NUM, kv_pair->magic);
                ret = OB_ERROR;
              }
              else if (tmp_pos >= atomic_pos_.buffer || tmp_pos < 0)
              {
                ret = OB_ENTRY_NOT_EXIST;
              }
              else
              {
                if (OB_SUCCESS == (ret = get(tmp_pos, pkey, pvalue)))
                {
                  pos = tmp_pos;
                }
              }
            }
            return ret;
          };
          int get(const int32_t pos, Key *&pkey, Value *&pvalue)
          {
            int ret = OB_SUCCESS;
            KVPair* kv_pair = NULL;
            if (pos > atomic_pos_.buffer || pos < 0)
            {
              ret = OB_ENTRY_NOT_EXIST;
            }
            else
            {
              kv_pair = reinterpret_cast<KVPair*>(&buffer_[pos]);
              if (NULL != kv_pair && kv_pair->magic != KVPAIR_MAGIC_NUM)
              {
                TBSYS_LOG(ERROR, "invalid kv pair magic, expected=%u, real=%u",
                          KVPAIR_MAGIC_NUM, kv_pair->magic);
                ret = OB_ERROR;
              }
              else if (NULL != kv_pair && NULL != kv_pair->first && NULL != kv_pair->second)
              {
                pkey = kv_pair->first;
                pvalue = kv_pair->second;
              }
              else
              {
                ret = OB_ENTRY_NOT_EXIST;
              }
            }
            return ret;
          };
          int alloc(const int64_t align_size)
          {
            int ret = OB_SUCCESS;
            int64_t alloc_size = MEM_BLOCK_SIZE;
            if (align_size > MEM_BLOCK_SIZE)
            {
              alloc_size = align_size;
            }

            if (!UseStaticBuf)
            {
              /**
               * current fixed size buffer is not big enough, free current
               * fixed buffer. then allocate a new buffer.
               */
              if (NULL != buffer_ && align_size > MEM_BLOCK_SIZE)
              {
                allocator_.free(buffer_);
                buffer_ = NULL;
              }

              if (NULL == buffer_)
              {
                buffer_ = static_cast<char*>(allocator_.alloc(static_cast<int32_t>(alloc_size)));
                if (NULL == buffer_)
                {
                  TBSYS_LOG(ERROR, "allocate memory for memblock failed");
                  ret = OB_ERROR;
                }
              }
            }
            else
            {
              if (align_size > MEM_BLOCK_SIZE)
              {
                TBSYS_LOG(ERROR, "the static buffer size is not enough, alloc_size=%ld, "
                                 "static_buffer_size=%ld",
                          align_size, MEM_BLOCK_SIZE);
                buffer_ = NULL;
                ret = OB_ERROR;
              }
              else
              {
                buffer_ = static_buffer_;
              }
            }

            if (NULL != buffer_)
            {
              payload_size_ = alloc_size;
            }

            return ret;
          };
          int64_t free()
          {
            int64_t payload_size = payload_size_;
            if (!UseStaticBuf && NULL != buffer_)
            {
              /**
               * only free the buffer which size is larger than memory block
               * size, this memory will return to system directly, if the
               * payload size is equal to memory block size, the buffer will
               * be reused.
               */
              if (payload_size > MEM_BLOCK_SIZE)
              {
                /**
                 * there is only one pair in the huge memblock, before free the
                 * memblock, we could destroy key and value in the pair, and
                 * reset the internal status. otherwise after call free() then
                 * call reset(), it will core dump because buffer_ is null.
                 */
                reset();
                allocator_.free(buffer_);
                buffer_ = NULL;
              }
            }
            //return the freed size
            return payload_size;
          };
          void clear()
          {
            if (!UseStaticBuf && NULL != buffer_)
            {
              //free all the buffer allocated dynamicly
              allocator_.free(buffer_);
              buffer_ = NULL;
            }
          }
        private:
          AtomicInt64 atomic_pos_;
          AtomicInt64 atomic_num_;
          MemBlock *next_;
          int64_t get_cnt_;
          int64_t last_time_;
          int64_t info_pos_;
          int64_t payload_size_;

          /**
           * the real buffer pointer, default it points the static_buffer_
           * if uses static buffer, else it points the extra memory
           * buffer.
           */
          Allocator allocator_;
          char *buffer_;
          char static_buffer_[UseStaticBuf ? MEM_BLOCK_SIZE : 0];
      };

      ////////////////////////////////////////////////////////////////////////////////////////////////////

      enum FreeListType
      {
        SINGLE,
        MULTI
      };

      template <class T, class Allocator = DefaultAllocator>
      class MultiObjFreeListTemp
      {
        static const int64_t BlockSize = T::MEM_BLOCK_SIZE;
        static const uint64_t MB_SIZE = 2 * 1024 * 1024;
        static const int64_t ITEM_NUM = MB_SIZE / sizeof(T) + ((MB_SIZE >= sizeof(T)) ? 0 : 1);
        typedef hash::SimpleAllocer<T, ITEM_NUM, hash::SpinMutexDefendMode, Allocator> FreeList;
        public:
          static const int type = MULTI;
        public:
          MultiObjFreeListTemp() : max_alloc_size_(INT64_MAX), alloc_size_(0)
          {
          };
          ~MultiObjFreeListTemp()
          {
            clear();
          };
          void clear()
          {
            if (0 != alloc_size_)
            {
              TBSYS_LOG(ERROR, "alloc_cnt=%ld not equal zero", alloc_size_);
            }
            else
            {
              allocator_.clear();
            }
          };
          void set_max_alloc_size(const int64_t max_alloc_size)
          {
            max_alloc_size_ = max_alloc_size;
          };
          int64_t get_max_alloc_size() const
          {
            return max_alloc_size_;
          };
          int64_t get_alloc_size() const
          {
            return alloc_size_;
          };
          T *alloc(const int64_t align_size)
          {
            T *ret = NULL;
            if (align_size <= 0 || align_size > BlockSize)
            {
              TBSYS_LOG(WARN, "invalid param, align_size=%ld, block_size=%ld",
                        align_size, BlockSize);
            }
            else
            {
              if ((int64_t)atomic_add((uint64_t*)&alloc_size_, BlockSize) + BlockSize < max_alloc_size_)
              {
                ret = allocator_.alloc();
                if (NULL != ret && OB_SUCCESS != ret->alloc(BlockSize))
                {
                  free(ret);
                  ret = NULL;
                }
              }
              if (NULL == ret)
              {
                atomic_add((uint64_t*)&alloc_size_, (uint64_t)-BlockSize);
              }
            }
            return ret;
          };
          void free(T *ptr)
          {
            if (NULL != ptr)
            {
              int64_t free_size = ptr->free();
              allocator_.free(ptr);
              atomic_add((uint64_t*)&alloc_size_, (uint64_t)-free_size);
            }
          };
        private:
          FreeList allocator_;
          int64_t max_alloc_size_; //maximum memory size allowed to allocate
          int64_t alloc_size_;     //how much memory has allocated currently
      };

      template <class T>
      class MultiObjFreeList : public MultiObjFreeListTemp<T, DefaultAllocator>
      {
      };

      template <class T, class Allocator = ThreadAllocator<T, MutilObjAllocator<T, 2*1024*1024> > >
      class SingleObjFreeListTemp
      {
        static const int64_t BlockSize = T::MEM_BLOCK_SIZE;
        public:
          static const int type = SINGLE;
        public:
          SingleObjFreeListTemp() : head_(NULL), max_alloc_size_(INT64_MAX), alloc_size_(0)
          {
            pthread_spin_init(&spin_, PTHREAD_PROCESS_PRIVATE);
          };
          ~SingleObjFreeListTemp()
          {
            clear();
            pthread_spin_destroy(&spin_);
          };
          void clear()
          {
            if (0 != alloc_size_)
            {
              TBSYS_LOG(ERROR, "alloc_cnt=%ld not equal zero", alloc_size_);
            }
            else
            {
              T *iter = head_;
              while (NULL != iter)
              {
                T *tmp = iter;
                iter = iter->get_next();
                tmp->clear();
                tmp->~T();
                allocator_.free(tmp);
              }
              head_ = NULL;
            }
          };
          void set_max_alloc_size(const int64_t max_alloc_size)
          {
            max_alloc_size_ = max_alloc_size;
          };
          int64_t get_max_alloc_size() const
          {
            return max_alloc_size_;
          };
          int64_t get_alloc_size() const
          {
            return alloc_size_;
          };
        public:
          T *alloc(const int64_t align_size)
          {
            T *ret = NULL;
            int64_t alloc_size = 0;

            if (align_size <= 0)
            {
              TBSYS_LOG(WARN, "alloc size is less than 0, size = %ld", align_size);
            }
            else
            {
              alloc_size = BlockSize;
              if (align_size > BlockSize)
              {
                alloc_size = align_size;
              }
              if ((int64_t)atomic_add((uint64_t*)&alloc_size_, alloc_size) + alloc_size <= max_alloc_size_)
              {
                pthread_spin_lock(&spin_);
                if (NULL != head_)
                {
                  ret = head_;
                  head_ = head_->get_next();
                  ret->reset();
                }
                pthread_spin_unlock(&spin_);
                if (NULL == ret)
                {
                  void *tmp = allocator_.alloc();
                  ret = new(tmp) T();
                }
                if (NULL != ret && OB_SUCCESS != ret->alloc(alloc_size))
                {
                  free(ret);
                  ret = NULL;
                }
              }
              if (NULL == ret)
              {
                atomic_add((uint64_t*)&alloc_size_, (uint64_t)-alloc_size);
              }
            }
            //TBSYS_LOG_US(DEBUG, "alloc %p", ret);
            return ret;
          };
          void free(T *ptr)
          {
            if (NULL != ptr)
            {
              pthread_spin_lock(&spin_);

              //T *iter = head_;
              //while (NULL != iter)
              //{
              //  if (ptr == iter)
              //  {
              //    abort();
              //  }
              //  iter = iter->get_next();
              //}

              int64_t free_size = ptr->free();
              ptr->~T();
              ptr->set_next(head_);
              head_ = ptr;
              pthread_spin_unlock(&spin_);
              atomic_add((uint64_t*)&alloc_size_, (uint64_t)-free_size);
            }
            //TBSYS_LOG_US(DEBUG, "free %p", ptr);
          };
        private:
          Allocator allocator_;
          pthread_spinlock_t spin_;
          T *head_;
          int64_t max_alloc_size_; //maximum memory size allowed to allocate
          int64_t alloc_size_;     //how much memory has allocated currently
      };

      template <class T>
        class SingleObjFreeList : public SingleObjFreeListTemp<T, ThreadAllocator<T, MutilObjAllocator<T, 2*1024*1024> > >
      {
      };
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    template <class Key, class Value, int64_t BlockSize,
              template <class> class FreeList,
              class Adapter>
    class KVStoreCache
    {
      typedef KVStoreCacheComponent::StoreHandle StoreHandle;
      typedef KVStoreCacheComponent::MemBlock<Key, Value, BlockSize,
        FreeList<void*>::type == KVStoreCacheComponent::MULTI> MemBlock;
      static const int64_t MEM_BLOCK_SIZE = MemBlock::MEM_BLOCK_SIZE;
      static const int64_t MAX_WASH_OUT_SIZE = 10 * MEM_BLOCK_SIZE;
      static const int64_t MAX_MEMBLOCK_INFO_COUNT = 128 * 1024; //128K
      typedef FreeList<MemBlock> MemBlockFreeList;
      struct MemBlockInfo
      {
        int64_t get_cnt;
        int64_t last_time;
        MemBlock * volatile mem_block;
      };
      class CmpFunc
      {
        public:
          CmpFunc(MemBlockInfo *mb_infos) : mb_infos_(mb_infos)
          {
          };
          bool operator() (int64_t a, int64_t b) const
          {
            bool bret = false;
            if (0 <= a && 0 <= b
                && 0 != mb_infos_[a].last_time
                && (0 == mb_infos_[b].last_time
                    || mb_infos_[a].last_time <= mb_infos_[b].last_time))
            {
              bret = true;
            }
            return bret;
          };
        private:
          MemBlockInfo *mb_infos_;
      };
      public:
        KVStoreCache() : inited_(false), adapter_(NULL), free_list_(), avg_get_cnt_(0),
                         max_mb_num_(MAX_MEMBLOCK_INFO_COUNT),total_mb_num_(0), mb_infos_(NULL),
                         not_revert_cnt_(0), cache_miss_cnt_(0), cache_hit_cnt_(0),
                         cur_memblock_(NULL)
        {
        };
        ~KVStoreCache()
        {
          destroy();
        };
      public:
        int init(const int64_t total_size)
        {
          int ret = OB_SUCCESS;
          if (inited_)
          {
            ret = OB_INIT_TWICE;
          }
          else if (0 >= total_size)
          {
            ret = OB_INVALID_ARGUMENT;
          }
          else
          {
            total_mb_num_ = total_size / MemBlock::MEM_BLOCK_SIZE + ((total_size >= MemBlock::MEM_BLOCK_SIZE) ? 0 : 1);
            max_mb_num_ = std::max(max_mb_num_, total_mb_num_);
            //preallocate mem block info with max size
            mb_infos_ = new(std::nothrow) MemBlockInfo[max_mb_num_];
            if (NULL != mb_infos_)
            {
              memset(mb_infos_, 0, sizeof(MemBlockInfo) * total_mb_num_);
              free_list_.set_max_alloc_size(total_mb_num_ * MemBlock::MEM_BLOCK_SIZE);
              inited_ = true;
            }
            else
            {
              TBSYS_LOG(ERROR, "failed to allocate memory for memblock info");
              ret = OB_ALLOCATE_MEMORY_FAILED;
            }
          }
          return ret;
        };
        int enlarge_total_size(const int64_t total_size)
        {
          int ret = OB_SUCCESS;
          if (!inited_)
          {
            ret = OB_NOT_INIT;
          }
          else if (0 >= total_size)
          {
            ret = OB_INVALID_ARGUMENT;
          }
          else
          {
            int64_t mb_num = total_size / MemBlock::MEM_BLOCK_SIZE + ((total_size >= MemBlock::MEM_BLOCK_SIZE) ? 0 : 1);
            if (mb_num > total_mb_num_ && mb_num <= max_mb_num_)
            {
              TBSYS_LOG(INFO, "cache size enlarged, new_cache_size=%ld, old_cache_size=%ld",
                        total_size, total_mb_num_ * MemBlock::MEM_BLOCK_SIZE);
              memset(mb_infos_ + total_mb_num_, 0, sizeof(MemBlockInfo) * (mb_num - total_mb_num_));
              free_list_.set_max_alloc_size(mb_num * MemBlock::MEM_BLOCK_SIZE);
              total_mb_num_ = mb_num;
            }
            else if (mb_num < total_mb_num_)
            {
              TBSYS_LOG(WARN, "can't resize cache size with less total size, "
                  "new_memblock_info_num=%ld, old_memblock_info_num=%ld",
                  mb_num, total_mb_num_);
              ret = OB_ERROR;
            }
            else if (mb_num == total_mb_num_)
            {
              TBSYS_LOG(INFO, "cache size doesn't change, old_cache_size=%ld, cache_size=%ld",
                  total_mb_num_ * MemBlock::MEM_BLOCK_SIZE, total_size);
            }
            else
            {
              TBSYS_LOG(INFO, "cache size can not change, max_cache_size=%ld, cache_size=%ld",
                  max_mb_num_ * MemBlock::MEM_BLOCK_SIZE, total_size);
              ret = OB_MEM_OVERFLOW;
            }
          }
          return ret;
        }
        int destroy()
        {
          int ret = OB_SUCCESS;
          if (0 != not_revert_cnt_)
          {
            TBSYS_LOG(WARN, "not_revert_cnt=%ld someone has got key-value but not revert", not_revert_cnt_);
            ret = OB_ERROR;
          }
          else
          {
            if (NULL != mb_infos_)
            {
              for (int64_t i = 0; i < total_mb_num_; i++)
              {
                if (NULL != mb_infos_[i].mem_block)
                {
                  free_list_.free(mb_infos_[i].mem_block);
                  mb_infos_[i].mem_block = NULL;
                }
              }
              delete[] mb_infos_;
              mb_infos_ = NULL;
            }
            if (NULL != cur_memblock_)
            {
              free_list_.free(cur_memblock_);
              cur_memblock_ = NULL;
            }
            inited_ = false;
            free_list_.clear();
            free_list_.set_max_alloc_size(INT64_MAX);
            adapter_ = NULL;
            total_mb_num_ = 0;
          }
          return ret;
        };
        int clear()
        {
          int ret = OB_SUCCESS;
          if (0 != not_revert_cnt_)
          {
            TBSYS_LOG(WARN, "not_revert_cnt=%ld someone has got key-value but not revert", not_revert_cnt_);
            ret = OB_ERROR;
          }
          else
          {
            if (NULL != mb_infos_)
            {
              for (int64_t i = 0; i < total_mb_num_; i++)
              {
                if (NULL != mb_infos_[i].mem_block)
                {
                  free_list_.free(mb_infos_[i].mem_block);
                  mb_infos_[i].mem_block = NULL;
                }
              }
              memset(mb_infos_, 0, sizeof(MemBlockInfo) * total_mb_num_);
              if (NULL != cur_memblock_)
              {
                free_list_.free(cur_memblock_);
                cur_memblock_ = NULL;
              }
            }
          }
          return ret;
        };
        void set_adapter(Adapter *adapter)
        {
          adapter_ = adapter;
        };
      public:
        int store(const Key &key, const Value &value, StoreHandle &handle,
                  Key **ppkey = NULL, Value **ppvalue = NULL)
        {
          int ret = OB_SUCCESS;
          MemBlock *memblock = NULL;
          int32_t seq_num = 0;
          int64_t align_kv_size = MemBlock::get_align_size(key, value);
          if (!inited_)
          {
            ret = OB_NOT_INIT;
          }
          else if (StoreHandle::FREE != handle.stat
                  && StoreHandle::STORED != handle.stat)
          {
            ret = OB_INVALID_ARGUMENT;
          }
          else if (NULL == (memblock = get_cur_memblock_(seq_num, align_kv_size)))
          {
            ret = OB_BUF_NOT_ENOUGH;
          }
          else
          {
            while (true)
            {
              ret = memblock->store(key, value, handle.kv_pos);
              if (OB_SUCCESS == ret)
              {
                handle.seq_num = seq_num;
                handle.mem_block = memblock;
                handle.stat = StoreHandle::STORED;
                if (NULL != ppkey || NULL != ppvalue)
                {
                  Key *pkey = NULL;
                  Value *pvalue = NULL;
                  get(handle, pkey, pvalue);
                  if (NULL != ppkey)
                  {
                    *ppkey = pkey;
                  }
                  if (NULL != ppvalue)
                  {
                    *ppvalue= pvalue;
                  }
                }
              }
              deref_memblock_(memblock);
              if (OB_SUCCESS == ret)
              {
                if (align_kv_size > MemBlock::MEM_BLOCK_SIZE)
                {
                  /**
                   * thread owns the big memblock, after store the only one kvpair
                   * into it, we submit the thread local memblock immediately.
                   * function get_cur_memblock_() can ensure big memblock is
                   * thread local.
                   */
                  submit_memblock_(memblock);
                }
                break;
              }
              else if (OB_BUF_NOT_ENOUGH == ret)
              {
                submit_cur_memblock_(memblock);
                if (NULL == (memblock = get_cur_memblock_(seq_num, align_kv_size)))
                {
                  break;
                }
                else
                {
                  continue;
                }
              }
              else
              {
                break;
              }
            }
          }
          return ret;
        };
        int dup_handle(const StoreHandle &old_handle, StoreHandle &new_handle)
        {
          int ret = OB_SUCCESS;
          if (!inited_)
          {
            ret = OB_NOT_INIT;
          }
          else
          {
            if (StoreHandle::LOCKED == old_handle.stat)
            {
              if (NULL == old_handle.mem_block
                  || !((MemBlock*)old_handle.mem_block)->check_seq_num_and_inc_ref_cnt(static_cast<int32_t>(old_handle.seq_num)))
              {
                ret = OB_ERROR;
              }
              else
              {
                atomic_inc((uint64_t*)&not_revert_cnt_);
                if (StoreHandle::LOCKED == new_handle.stat
                    && OB_SUCCESS != (ret = revert(new_handle)))
                {
                  StoreHandle tmp_handle = old_handle;
                  revert(tmp_handle);
                }
                else
                {
                  new_handle = old_handle;
                }
              }
            }
          }
          return ret;
        };
        int get_next(StoreHandle &handle, Key *&pkey, Value *&pvalue)
        {
          int ret = OB_SUCCESS;
          if (!inited_)
          {
            ret = OB_NOT_INIT;
          }
          else if (NULL == mb_infos_)
          {
            ret = OB_ITER_END;
          }
          else
          {
            bool loop = true;
            int32_t stat = handle.stat;
            while (loop)
            {
              switch (stat)
              {
                case StoreHandle::FREE:
                  {
                    MemBlock *tmp_mem_block = NULL;
                    if (handle.mb_infos_pos < total_mb_num_)
                    {
                      tmp_mem_block = mb_infos_[handle.mb_infos_pos].mem_block;
                    }
                    else if (handle.mb_infos_pos == total_mb_num_
                            && NULL != cur_memblock_)
                    {
                      tmp_mem_block = cur_memblock_;
                      TBSYS_LOG(DEBUG, "start scan cur_memblock");
                    }
                    else
                    {
                      ret = OB_ITER_END;
                      loop = false;
                    }
                    if (NULL != tmp_mem_block
                        && tmp_mem_block->check_and_inc_ref_cnt())
                    {
                      revert(handle);
                      handle.seq_num = tmp_mem_block->get_seq_num();
                      handle.kv_pos = 0;
                      handle.stat = StoreHandle::LOCKED;
                      handle.mem_block = tmp_mem_block;
                      atomic_inc((uint64_t*)&not_revert_cnt_);
                      ret = ((MemBlock*)handle.mem_block)->get(handle.kv_pos, pkey, pvalue);
                      if (OB_SUCCESS != ret)
                      {
                        handle.mb_infos_pos += 1;
                      }
                      else
                      {
                        loop = false;
                      }
                    }
                    else
                    {
                      handle.mb_infos_pos += 1;
                    }
                  }
                  break;
                case StoreHandle::LOCKED:
                  if (NULL == handle.mem_block)
                  {
                    ret = OB_INVALID_ARGUMENT;
                    loop = false;
                  }
                  else if (OB_SUCCESS != (ret = ((MemBlock*)handle.mem_block)->get_next(handle.kv_pos, pkey, pvalue)))
                  {
                    stat = StoreHandle::FREE;
                    handle.mb_infos_pos += 1;
                  }
                  else
                  {
                    loop = false;
                  }
                  break;
                case StoreHandle::STORED:
                default:
                  TBSYS_LOG(WARN, "store_handle invalid stat=%d", stat);
                  ret = OB_INVALID_ARGUMENT;
                  loop = false;
                  break;
              }
            }
          }
          return ret;
        };
        void reset_iter(StoreHandle &handle)
        {
          revert(handle);
          handle.mb_infos_pos = 0;
        };
        int get(StoreHandle &handle, Key *&key, Value *&value)
        {
          int ret = OB_SUCCESS;
          if (!inited_)
          {
            ret = OB_NOT_INIT;
          }
          else if (NULL == handle.mem_block
                  || StoreHandle::STORED != handle.stat)
          {
            ret = OB_INVALID_ARGUMENT;
          }
          else if (!((MemBlock*)handle.mem_block)->check_seq_num_and_inc_ref_cnt(static_cast<int32_t>(handle.seq_num)))
          {
            atomic_inc((uint64_t*)&cache_miss_cnt_);
            ret = OB_ENTRY_NOT_EXIST;
          }
          else
          {
            ret = ((MemBlock*)handle.mem_block)->get(handle.kv_pos, key, value);
            if (OB_SUCCESS == ret)
            {
              update_mb_info_((MemBlock*)handle.mem_block);
              handle.stat = StoreHandle::LOCKED;
              atomic_inc((uint64_t*)&not_revert_cnt_);
              atomic_inc((uint64_t*)&cache_hit_cnt_);
            }
            else
            {
              deref_memblock_((MemBlock*)handle.mem_block);
              ret = OB_INVALID_ARGUMENT;
            }
          }
          return ret;
        };
        int get(StoreHandle &handle, Key &key, Value &value)
        {
          int ret = OB_SUCCESS;
          Key *pkey = NULL;
          Value *pvalue = NULL;
          ret = get(handle, pkey, pvalue);
          if (OB_SUCCESS == ret)
          {
            key = *key;
            value = *pvalue;
          }
          return ret;
        };
        int revert(StoreHandle &handle)
        {
          int ret = OB_SUCCESS;
          if (!inited_)
          {
            ret = OB_NOT_INIT;
          }
          else if (NULL == handle.mem_block
                  || StoreHandle::LOCKED != handle.stat)
          {
            ret = OB_INVALID_ARGUMENT;
          }
          else
          {
            deref_memblock_((MemBlock*)handle.mem_block);
            handle.mem_block = NULL;
            handle.stat = StoreHandle::STORED;
            atomic_dec((uint64_t*)&not_revert_cnt_);
          }
          return ret;
        };
        int64_t get_miss_cnt() const
        {
          return cache_miss_cnt_;
        };
        int64_t get_hit_cnt() const
        {
          return cache_hit_cnt_;
        };
        int64_t size() const
        {
          return (free_list_.get_alloc_size());
        };
      private:
        bool deref_memblock_(MemBlock *memblock)
        {
          bool bret = false;
          if (0 == memblock->dec_ref_cnt_and_inc_seq_num())
          {
            memblock->reset(adapter_);
            free_list_.free(memblock);
            bret = true;
          }
          return bret;
        };
        void update_mb_info_(MemBlock *memblock)
        {
          int64_t info_pos = memblock->get_info_pos();
          if (0 <= info_pos
              && total_mb_num_ > info_pos
              && memblock == mb_infos_[info_pos].mem_block)
          {
            // 有原子性问题 可能更新的访问计数已经不是这个memblock的了 这个误差可以接受
            atomic_inc((uint64_t*)&(mb_infos_[info_pos].get_cnt));
            if (mb_infos_[info_pos].get_cnt > avg_get_cnt_)
            {
              mb_infos_[info_pos].last_time = tbsys::CTimeUtil::getTime();
              mb_infos_[info_pos].get_cnt = avg_get_cnt_;
            }
          }
          else if (-1 == info_pos)
          {
            memblock->inc_get_cnt();
            memblock->set_last_time(tbsys::CTimeUtil::getTime());
          }
          else
          {
            TBSYS_LOG_US(INFO, "maybe block has already been washed out, pos=%ld memblock=%p info_memblock=%p",
                      info_pos, memblock, (0 <= info_pos && total_mb_num_ > info_pos) ? mb_infos_[info_pos].mem_block : NULL);
          }
        };
        int64_t wash_out_timeout_(const int64_t min_free_size)
        {
          int64_t need_wash_out_size = MAX_WASH_OUT_SIZE;
          if (min_free_size > MAX_WASH_OUT_SIZE)
          {
            need_wash_out_size = min_free_size;
          }
          static const int64_t HEAP_SIZE = need_wash_out_size / BlockSize * 2;
          int64_t heap[HEAP_SIZE];
          int64_t num = 0;
          const int64_t &max = heap[0];
          memset(heap, -1, sizeof(heap));
          CmpFunc cmp_func(mb_infos_);
          int64_t sort_timeu = tbsys::CTimeUtil::getTime();
          int64_t sum_get_cnt = 0;
          int64_t num_get_cnt = 0;
          int64_t wash_out_size = 0;
          int64_t memblock_payload_size = 0;
          for (int64_t i = 0; i < total_mb_num_; i++)
          {
            if (NULL != mb_infos_[i].mem_block
                && (cmp_func(i, max) || num < HEAP_SIZE))
            {
              if (num < HEAP_SIZE)
              {
                heap[num++] = i;
              }
              else
              {
                std::pop_heap(&heap[0], &heap[num], cmp_func);
                heap[num - 1] = i;
              }
              std::push_heap(&heap[0], &heap[num], cmp_func);
              sum_get_cnt += mb_infos_[i].get_cnt;
              num_get_cnt += 1;
            }
          }
          for (int64_t i = 0; i < num; i++)
          {
            std::pop_heap(&heap[0], &heap[num - i], cmp_func);
          }
          if (0 != num_get_cnt)
          {
            avg_get_cnt_ = sum_get_cnt / num_get_cnt;
          }
          sort_timeu = tbsys::CTimeUtil::getTime() - sort_timeu;

          int64_t free_timeu = tbsys::CTimeUtil::getTime();
          for (int64_t i = 0, j = 0; i < num && j < HEAP_SIZE / 2; i++)
          {
            int64_t pos = heap[i];
            MemBlock *old = mb_infos_[pos].mem_block;
            if (NULL != old
                && old == (MemBlock*)atomic_compare_exchange((uint64_t*)&(mb_infos_[pos].mem_block), (uint64_t)NULL, (uint64_t)old))
            {
              memblock_payload_size = old->get_payload_size();
              TBSYS_LOG_US(DEBUG, "try to free memblock=%p", old);
              if (deref_memblock_(old))
              {
                wash_out_size += memblock_payload_size;
                j++;
              }
            }
            if (wash_out_size >= need_wash_out_size)
            {
              break;
            }
          }
          free_timeu = tbsys::CTimeUtil::getTime() - free_timeu;

          TBSYS_LOG_US(DEBUG, "sort_timeu=%ld free_timeu=%ld", sort_timeu, free_timeu);
          return wash_out_size;
        };
        MemBlock *get_cur_memblock_(int32_t &seq_num, const int64_t align_kv_size)
        {
          MemBlock *ret = NULL;
          if (align_kv_size > free_list_.get_max_alloc_size())
          {
            TBSYS_LOG_US(WARN, "cann't allocate memblock from free list, kv size is bigger "
                               "than total cache memory size, align_kv_size=%ld, "
                               "max_alloc_size=%ld",
                         align_kv_size, free_list_.get_max_alloc_size());
          }
          else
          {
            //if cache is 95% full, wash out some memblocks
            if ((free_list_.get_alloc_size() * 100 / free_list_.get_max_alloc_size() > 95)
               && wash_out_lock_.try_wrlock())
            {
              wash_out_timeout_(MAX_WASH_OUT_SIZE);
              wash_out_lock_.unlock();
            }

            while (true)
            {
              if (align_kv_size > MemBlock::MEM_BLOCK_SIZE
                  || (align_kv_size <= MemBlock::MEM_BLOCK_SIZE
                      && (NULL == (ret = cur_memblock_) || !ret->check_and_inc_ref_cnt())))
              {
                MemBlock *new_memblock = free_list_.alloc(align_kv_size);
                while (NULL == new_memblock)
                {
                  int64_t timeu = tbsys::CTimeUtil::getTime();
                  int64_t alloc_cnt_before = free_list_.get_alloc_size();
                  int64_t wash_out_size = wash_out_timeout_(align_kv_size);
                  TBSYS_LOG_US(DEBUG, "wash out alloc_size_before=%ld alloc_size_after=%ld timeu=%ld",
                              alloc_cnt_before, free_list_.get_alloc_size(), tbsys::CTimeUtil::getTime() - timeu);
                  new_memblock = free_list_.alloc(align_kv_size);
                  if (0 == wash_out_size && NULL == new_memblock)
                  {
                    TBSYS_LOG_US(WARN, "can't wash out some memblock, alloc_size=%ld, "
                                       "align_kv_size=%ld, wash_out_size=%ld",
                                 free_list_.get_alloc_size(), align_kv_size, wash_out_size);
                    break;
                  }
                }
                if (NULL == new_memblock)
                {
                  break;
                }
                // 第一次加引用计数 因为初始化后引用就为1
                new_memblock->inc_ref_cnt();
                // 第二次加引用计数 表示get_cur_memblock_也是对这个block的引用
                // 避免其他线程在get_cur_memblock返回前就把这个block填满 并且淘汰释放掉
                new_memblock->inc_ref_cnt();
                if (align_kv_size > MemBlock::MEM_BLOCK_SIZE)
                {
                  /**
                   * if the required memblock is big memblock, the allocated
                   * memblock is olny used by the current thread, and the memblock
                   * only can store one kvpair, so it can't share with other
                   * thread. it needn't set the cur_memblock.
                   */
                  ret = new_memblock;
                  break;
                }
                else
                {
                  MemBlock *old_memblock = NULL;
                  if (NULL == (old_memblock = (MemBlock*)atomic_compare_exchange((uint64_t*)&cur_memblock_, (uint64_t)new_memblock, (uint64_t)NULL)))
                  {
                    ret = new_memblock;
                    break;
                  }
                  else
                  {
                    free_list_.free(new_memblock);
                    continue;
                  }
                }
              }
              else
              {
                break;
              }
            }
          }
          if (NULL != ret)
          {
            seq_num = ret->get_seq_num();
          }
          return ret;
        };
        void submit_memblock_(MemBlock *submit_memblock)
        {
          int64_t i = 0;
          for (; i < total_mb_num_; i++)
          {
            if (NULL == (MemBlock*)atomic_compare_exchange((uint64_t*)&(mb_infos_[i].mem_block), (uint64_t)submit_memblock, (uint64_t)NULL))
            {
              break;
            }
          }
          if (i < total_mb_num_)
          {
            // 有原子性问题 可能更新的访问计数已经不是这个memblock的了 这个误差可以接受
            mb_infos_[i].get_cnt = submit_memblock->get_cnt();
            mb_infos_[i].last_time = tbsys::CTimeUtil::getTime();
            submit_memblock->set_info_pos(i);
          }
          else
          {
            TBSYS_LOG_US(WARN, "cannot find mb_info for cur_memblock=%p, will wash_out "
                               "it directly, alloc_size_=%ld",
                         submit_memblock, free_list_.get_alloc_size());
            deref_memblock_(submit_memblock);
          }
        };
        void submit_cur_memblock_(MemBlock *submit_memblock)
        {
          if (submit_memblock == (MemBlock*)atomic_compare_exchange((uint64_t*)&cur_memblock_, (uint64_t)NULL, (uint64_t)submit_memblock))
          {
            submit_memblock_(submit_memblock);
          }
        };
      private:
        bool inited_;

        Adapter *adapter_;
        MemBlockFreeList free_list_;

        int64_t avg_get_cnt_;
        int64_t max_mb_num_;
        int64_t total_mb_num_;
        MemBlockInfo *mb_infos_;

        int64_t not_revert_cnt_;
        int64_t cache_miss_cnt_;
        int64_t cache_hit_cnt_;

        MemBlock * volatile cur_memblock_;
        SpinRWLock wash_out_lock_;
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    struct CacheHandle
    {
      KVStoreCacheComponent::StoreHandle store_handle;
    };

    template <class Key, class Value, int64_t ItemSize,
              int64_t BlockSize = 2 * 1024 * 1024,
              template <class> class FreeList = KVStoreCacheComponent::SingleObjFreeList,
              class DefendMode = hash::MultiWriteDefendMode>
    class KeyValueCache
    {
      static const int64_t DEFAULT_ITEM_SIZE = 1024;
      static const int64_t DEFAULT_TIMEOUT_US = 10 * 1000 * 1000;

      typedef KeyValueCache<Key, Value, ItemSize, BlockSize, FreeList, DefendMode> Cache;
      typedef KVStoreCache<Key, Value, BlockSize, FreeList, Cache> Store;
      typedef typename KVStoreCacheComponent::StoreHandle StoreHandle;

      typedef typename hash::HashMapTypes<Key, StoreHandle>::AllocType HashAllocType;
      static const int64_t HashAllocatorPageSize = 512 * 1024;
      typedef ThreadAllocator<HashAllocType, MutilObjAllocator<HashAllocType, HashAllocatorPageSize, ObModIds::OB_KVSTORE_CACHE> > HashAllocator;
      //typedef ThreadAllocator<HashAllocType, SingleObjAllocator<HashAllocType> > HashAllocator;
      typedef hash::ObHashMap<Key, StoreHandle,
                              DefendMode,
                              hash::hash_func<Key>,
                              hash::equal_to<Key>,
                              HashAllocator> HashMap;
      public:
        KeyValueCache() : inited_(false)
        {
        };
        ~KeyValueCache()
        {
          destroy();
        };
      public:
        int init(const int64_t total_size)
        {
          int ret = OB_SUCCESS;
          int hash_ret = 0;
          if (inited_)
          {
            ret = OB_INIT_TWICE;
          }
          else if (OB_SUCCESS != (ret = store_.init(total_size)))
          {
            TBSYS_LOG(WARN, "init store fail ret=%d size=%ld", ret, total_size);
          }
          else if (0 != (hash_ret = map_.create(total_size / ItemSize)))
          {
            store_.destroy();
            TBSYS_LOG(WARN, "create map fail ret=%d num=%ld", hash_ret, total_size / ItemSize);
            ret = OB_ERROR;
          }
          else
          {
            store_.set_adapter(this);
            inited_ = true;
          }
          return ret;
        };
        int enlarge_total_size(const int64_t total_size)
        {
          int ret = OB_SUCCESS;
          if (!inited_)
          {
            ret = OB_NOT_INIT;
          }
          else if (OB_SUCCESS != (ret = store_.enlarge_total_size(total_size)))
          {
            TBSYS_LOG(WARN, "enlarge total memory size of store cache fail, ret=%d, size=%ld",
                ret, total_size);
          }
          return ret;
        }
        int destroy()
        {
          int ret = OB_SUCCESS;
          if (inited_)
          {
            if (OB_SUCCESS == (ret = store_.destroy()))
            {
              map_.destroy();
              inited_ = false;
            }
          }
          return ret;
        };
        int clear()
        {
          int ret = OB_SUCCESS;
          if (inited_)
          {
            ret = store_.clear();
            if (OB_SUCCESS == ret)
            {
              int hash_ret = map_.clear();
              if (0 == hash_ret)
              {
                ret = OB_SUCCESS;
              }
              else
              {
                ret = OB_ERROR;
              }
            }
          }
          return ret;
        };
        int64_t size() const
        {
          return store_.size();
        };
        int64_t count() const
        {
          return map_.size();
        };
        int64_t get_hit_cnt() const
        {
          return store_.get_hit_cnt();
        };
        int64_t get_miss_cnt() const
        {
          return store_.get_miss_cnt();
        };
      private:
        int internal_put(const Key &key, const Value &value, StoreHandle& store_handle, bool overwrite = true)
        {
          int ret = OB_SUCCESS;
          Key *pkey = NULL;
          if (!inited_)
          {
            ret = OB_NOT_INIT;
          }
          else if (!overwrite && hash::HASH_EXIST == (ret = map_.get(key, store_handle)))
          {
            ret = OB_ENTRY_EXIST;
          }
          else if (OB_SUCCESS != (ret = store_.store(key, value, store_handle, &pkey))
                  || NULL == pkey)
          {
            TBSYS_LOG(WARN, "store key-value fail ret=%d", ret);
            if (OB_SUCCESS == ret)
            {
              store_.revert(store_handle);
              ret = OB_ERROR;
            }
          }
          else
          {
            int hash_ret = map_.set(*pkey, store_handle.copy_without_stat(), 1, 1);
            if (hash::HASH_OVERWRITE_SUCC == hash_ret || hash::HASH_INSERT_SUCC == hash_ret)
            {
              ret = OB_SUCCESS;
            }
            else
            {
              store_.revert(store_handle);
              ret = OB_ERROR;
            }
          }
          return ret;
        };
      public:
        /**
         * default the overwrite param is true, it means that if the key
         * to put isn't existent in hashmap, isert it, if it is extent
         * in hashmap, overwrite it. if the overwrite param is false, it
         * first checks whether the key is existent in hashmap and the
         * value in hashmap isn't fake, if true, return OB_ENTRY_EXIST,
         * else store the key and value and must ensure the fake value
         * is overwrited.
         *
         * WARNING: when set the overwite param false, you could ensure
         * you have call get() function with only_cache=false, the get()
         * function will add a fake node for this key, and put()
         * function with overwrite=false will overwrite the fake node to
         * the actual node.
         *   if you get(only_cache=true), you'd better
         * put(overwrite=true), for this case, it doesn't support deep
         * copy for key.
         *   if you get(only_cache=false), you'd better
         * put(overwrite=false), for this case, it supports deep copy
         * for key.
         *
         *   get(only_cache=true) and put(overwrite=false) means only
         * get cached value from kvcache, don't overwrite if key exists,
         * insert if key doesn't exist.
         *
         *  get(only_cache=false) and put(overwrite=true), please don't
         *  use like this, the result is ok, but it will waste memblock
         *  memory.
         *
         * @return int
         */
        int put(const Key &key, const Value &value, bool overwrite = true)
        {
          int ret = OB_SUCCESS;
          StoreHandle store_handle;

          ret = internal_put(key, value, store_handle, overwrite);
            if (OB_SUCCESS == ret)
            {
              store_.revert(store_handle);
          }

          return ret;
        };
        int put_and_fetch(const Key &key, const Value &input_value, Value &output_value,
                          CacheHandle &handle, bool overwrite = true, bool only_cache = true)
        {
          int ret = OB_SUCCESS;
          StoreHandle store_handle;

          ret = internal_put(key, input_value, store_handle, overwrite);
          if (OB_SUCCESS == ret)
          {
            ret = get(key, output_value, handle, only_cache);
            store_.revert(store_handle);
          }
          else if (OB_ENTRY_EXIST == ret && !overwrite)
          {
            ret = get(key, output_value, handle, only_cache);
          }
          else
          {
            TBSYS_LOG(WARN, "failed to put key-value into kvcache, ret=%d", ret);
          }
          return ret;
        };
        int get_next(Key &key, Value &value, CacheHandle &handle)
        {
          int ret = OB_SUCCESS;
          Key *pkey = NULL;
          Value *pvalue = NULL;
          if (!inited_)
          {
            ret = OB_NOT_INIT;
          }
          else if (OB_SUCCESS != (ret = store_.get_next(handle.store_handle, pkey, pvalue))
                  || NULL == pkey || NULL == pvalue)
          {
            if (OB_SUCCESS == ret)
            {
              store_.revert(handle.store_handle);
              ret = OB_ERROR;
            }
          }
          else
          {
            key = *pkey;
            value = *pvalue;
          }
          return ret;
        };
        void reset_iter(CacheHandle &handle)
        {
          store_.reset_iter(handle.store_handle);
        };
        int get(const Key &key, Value &value, bool only_cache = true)
        {
          CacheHandle handle;
          int ret = get(key, value, handle, only_cache);
          if (OB_SUCCESS == ret)
          {
            revert(handle);
          }
          return ret;
        };
        int get(const Key &key, Value &value, CacheHandle &handle, bool only_cache = true)
        {
          Value *pvalue = NULL;
          int ret = get(key, pvalue, handle, only_cache);
          if (OB_SUCCESS == ret)
          {
            value = *pvalue;
          }
          return ret;
        }
#ifdef __DEBUG_TEST__
        int get(const Key &key, Value *&pvalue, const Value &cv, CacheHandle &handle)
#else
        int get(const Key &key, Value *&pvalue, CacheHandle &handle, bool only_cache = true)
#endif
        {
          int ret = OB_SUCCESS;
          int hash_ret = 0;
          Key *pkey = NULL;
          int64_t timeout_us = only_cache ? 0 : DEFAULT_TIMEOUT_US;
          if (!inited_)
          {
            ret = OB_NOT_INIT;
          }
          else if (hash::HASH_EXIST != (hash_ret = map_.get(key, handle.store_handle, timeout_us)))
          {
            //TBSYS_TRACE_LOG("kv_store_cache miss key=[%s]", KVStoreCacheComponent::log_str(key));
            ret = OB_ENTRY_NOT_EXIST;
          }
          else
          {
            /**
             * if key is existent in hash map, but we can't get kvpair from
             * store, the memblock which the kvpair belongs to is washed
             * out, it is erasing the key index in hash map and it doesn't
             * complete. so we get from hash map in a loop until the key
             * index is deleted from hash map.
             */
            do
            {
              if (OB_SUCCESS != (ret = store_.get(handle.store_handle, pkey, pvalue))
                  || NULL == pkey || NULL == pvalue)
              {
                if (OB_SUCCESS == ret)
                {
                  store_.revert(handle.store_handle);
                  ret = OB_ERROR;
                }
                else if (OB_ENTRY_NOT_EXIST == ret)
                {
                  if (only_cache)
                  {
                    break;
                  }
                  else if (hash::HASH_EXIST != (hash_ret = map_.get(key, handle.store_handle, DEFAULT_TIMEOUT_US)))
                  {
                    ret = OB_ENTRY_NOT_EXIST;
                    break;
                  }
                }
                //TBSYS_TRACE_LOG("kv_store_cache miss and error key=[%s]", KVStoreCacheComponent::log_str(key));
              }
            } while (OB_ENTRY_NOT_EXIST == ret);
          }

          if (OB_SUCCESS == ret)
          {
            //TBSYS_TRACE_LOG("kv_store_cache hit key=[%s]", KVStoreCacheComponent::log_str(key));
            //value = *pvalue;
#ifdef __DEBUG_TEST__
            //assert(value == cv);
#endif
          }
          return ret;
        };
        int dup_handle(const CacheHandle &old_handle, CacheHandle &new_handle)
        {
          int ret = OB_SUCCESS;
          ret = store_.dup_handle(old_handle.store_handle, new_handle.store_handle);
          return ret;
        };
        int revert(CacheHandle &handle)
        {
          int ret = OB_SUCCESS;
          if (!inited_)
          {
            ret = OB_NOT_INIT;
          }
          else
          {
            store_.revert(handle.store_handle);
          }
          return ret;
        };
        int erase(const Key &key)
        {
          int ret = OB_SUCCESS;
          if (!inited_)
          {
            ret = OB_NOT_INIT;
          }
          else
          {
            int hash_ret = map_.erase(key);
            if (hash::HASH_EXIST == hash_ret)
            {
              ret = OB_SUCCESS;
            }
            else if (hash::HASH_NOT_EXIST == hash_ret)
            {
              ret = OB_ENTRY_NOT_EXIST;
            }
            else
            {
              ret = OB_ERROR;
            }
          }
          return ret;
        };
        void release(const Key *key)
        {
          erase(*key);
        };
      private:
        bool inited_;
        Store store_;
        HashMap map_;
    };
  }
}

#endif //OCEANBASE_COMMON_KV_STORE_CACHE_H_
