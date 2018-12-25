////===================================================================
 //
 // ob_lighty_hash.h / hash / common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2012-04-12 by Yubai (yubai.lk@taobao.com) 
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

#ifndef  OCEANBASE_UPDATESERVER_LIGHTY_HASH_H_
#define  OCEANBASE_UPDATESERVER_LIGHTY_HASH_H_
#include <stdint.h>
#include <algorithm>
#include "common/ob_define.h"
#include "common/ob_atomic.h"
#include "ob_bit_lock.h"

namespace oceanbase
{
  namespace updateserver
  {
    namespace lightyhash
    {
      template <typename T>
      struct Hash
      {
        uint64_t operator ()(const T &v)
        {
          return v.hash();
        };
      };

      template <typename T>
      struct Hash<T*>
      {
        uint64_t operator ()(T *v)
        {
          return v ? v->hash() : 0;
        };
      };

      template <typename T>
      struct Hash<const T*>
      {
        uint64_t operator ()(const T *v)
        {
          return v ? v->hash() : 0;
        };
      };

      template <typename T>
      struct Equal
      {
        bool operator ()(const T &a, const T &b)
        {
          return (a == b);
        };
      };

      template <typename T>
      struct Equal<T*>
      {
        bool operator ()(T *a, T *b)
        {
          return (a && b && (*a == *b));
        };
      };

      template <typename T>
      struct Equal<const T*>
      {
        bool operator ()(const T *a, const T *b)
        {
          return (a && b && (*a == *b));
        };
      };

      template <typename Key,
                typename Value,
                typename BucketAllocator,
                typename NodeAllocator>
      class LightyHashMap
      {
        struct Node
        {
          Key key;
          Value value;
          union
          {
            Node *next;
            int64_t flag;
          };
        };
        static const int64_t EMPTY_FLAG = 0xffffffffffffffff;
        static const int64_t INIT_UNIT_SIZE = (64L * 1024L / sizeof(Node)) * sizeof(Node);
        typedef Hash<Key> HashFunc;
        typedef Equal<Key> EqualFunc;
        public:
          LightyHashMap(BucketAllocator &bucket_allocator, NodeAllocator &node_allocator);
          ~LightyHashMap();
        private:
          DISALLOW_COPY_AND_ASSIGN(LightyHashMap);
        public:
          int create(const int64_t bucket_num);
          void destroy();
          int clear();
        public:
          inline int insert(const Key &key, const Value &value);
          inline int get(const Key &key, Value &value);
          inline int erase(const Key &key, Value *value = NULL);
          inline int64_t uninit_unit_num() const;
          inline int64_t bucket_using() const;
          inline int64_t size() const;
        private:
          void init_bucket_unit_(const uint64_t bucket_pos);
        private:
          BucketAllocator &bucket_allocator_;
          NodeAllocator &node_allocator_;
          int64_t bucket_num_;
          Node *buckets_;
          volatile int64_t uninit_unit_num_;
          uint8_t *volatile init_units_;
          BitLock bit_lock_;
          int64_t bucket_using_;
          int64_t size_;
          HashFunc hash_func_;
          EqualFunc equal_func_;
      };

      template <typename Key, typename Value, typename BucketAllocator, typename NodeAllocator>
      LightyHashMap<Key, Value, BucketAllocator, NodeAllocator>::LightyHashMap(
        BucketAllocator &bucket_allocator,
        NodeAllocator &node_allocator) : bucket_allocator_(bucket_allocator),
                                         node_allocator_(node_allocator),
                                         bucket_num_(0),
                                         buckets_(NULL),
                                         uninit_unit_num_(0),
                                         init_units_(NULL),
                                         bucket_using_(0),
                                         size_(0),
                                         hash_func_(),
                                         equal_func_()
      {
      }

      template <typename Key, typename Value, typename BucketAllocator, typename NodeAllocator>
      LightyHashMap<Key, Value, BucketAllocator, NodeAllocator>::~LightyHashMap()
      {
        destroy();
      }

      template <typename Key, typename Value, typename BucketAllocator, typename NodeAllocator>
      int LightyHashMap<Key, Value, BucketAllocator, NodeAllocator>::create(const int64_t bucket_num)
      {
        int ret = common::OB_SUCCESS;
        int64_t uninit_unit_num = (bucket_num * sizeof(Node) / INIT_UNIT_SIZE) \
                                  + ((0 == (bucket_num * sizeof(Node) % INIT_UNIT_SIZE)) ? 0 : 1);
        if (NULL != buckets_)
        {
          ret = common::OB_INIT_TWICE;
        }
        else if (0 >= bucket_num)
        {
          ret = common::OB_INVALID_ARGUMENT;
        }
        else if (NULL == (buckets_ = (Node*)bucket_allocator_.alloc(bucket_num * sizeof(Node))))
        {
          ret = common::OB_MEM_OVERFLOW;
        }
        else if (NULL == (init_units_ = (uint8_t*)bucket_allocator_.alloc(uninit_unit_num * sizeof(uint8_t))))
        {
          ret = common::OB_MEM_OVERFLOW;
        }
        else if (OB_SUCCESS != (ret = bit_lock_.init(bucket_num)))
        {
          // init bit lock fail
        }
        else
        {
          bucket_num_ = bucket_num;
          uninit_unit_num_ = uninit_unit_num;
          memset(init_units_, 0, uninit_unit_num_ * sizeof(uint8_t));
          bucket_using_ = 0;
          size_ = 0;
        }
        if (common::OB_SUCCESS != ret)
        {
          destroy();
        }
        return ret;
      }

      template <typename Key, typename Value, typename BucketAllocator, typename NodeAllocator>
      void LightyHashMap<Key, Value, BucketAllocator, NodeAllocator>::destroy()
      {
        if (NULL != buckets_)
        {
          if (NULL != init_units_)
          {
            for (int64_t i = 0; i < bucket_num_; i++)
            {
              int64_t unit_pos = i * sizeof(Node) / INIT_UNIT_SIZE;
              uint8_t ov = init_units_[unit_pos];
              if (0 == (ov & 0x80))
              {
                continue;
              }
              Node *iter = buckets_[i].next;
              while (EMPTY_FLAG != buckets_[i].flag
                    && NULL != iter)
              {
                Node *tmp = iter;
                iter = iter->next;
                node_allocator_.free(tmp);
              }
              buckets_[i].flag = EMPTY_FLAG;
            }
          }
          bucket_allocator_.free(buckets_);
          buckets_ = NULL;
        }
        if (NULL != init_units_)
        {
          bucket_allocator_.free(init_units_);
          init_units_ = NULL;
        }
        bit_lock_.destroy();
        bucket_num_ = 0;
        uninit_unit_num_ = 0;
        bucket_using_ = 0;
        size_ = 0;
      }

      template <typename Key, typename Value, typename BucketAllocator, typename NodeAllocator>
      int LightyHashMap<Key, Value, BucketAllocator, NodeAllocator>::clear()
      {
        int ret = common::OB_SUCCESS;
        if (NULL == buckets_
            || NULL == init_units_)
        {
          ret = common::OB_NOT_INIT;
        }
        else
        {
          for (int64_t i = 0; i < bucket_num_; i++)
          {
            int64_t unit_pos = i * sizeof(Node) / INIT_UNIT_SIZE;
            uint8_t ov = init_units_[unit_pos];
            if (0 == (ov & 0x80))
            {
              continue;
            }
            BitLockGuard guard(bit_lock_, i);
            Node *iter = buckets_[i].next;
            while (EMPTY_FLAG != buckets_[i].flag
                  && NULL != iter)
            {
              Node *tmp = iter;
              iter = iter->next;
              node_allocator_.free(tmp);
            }
            buckets_[i].flag = EMPTY_FLAG;
          }
          uninit_unit_num_ = (bucket_num_ * sizeof(Node) / INIT_UNIT_SIZE) \
                              + ((0 == (bucket_num_ * sizeof(Node) % INIT_UNIT_SIZE)) ? 0 : 1);
          memset(init_units_, 0, uninit_unit_num_ * sizeof(Node));
          bucket_using_ = 0;
          size_ = 0;
        }
        return ret;
      }

      template <typename Key, typename Value, typename BucketAllocator, typename NodeAllocator>
      int LightyHashMap<Key, Value, BucketAllocator, NodeAllocator>::insert(const Key &key, const Value &value)
      {
        int ret = common::OB_SUCCESS;
        if (NULL == buckets_
            || NULL == init_units_)
        {
          ret = common::OB_NOT_INIT;
        }
        else
        {
          uint64_t hash_value = hash_func_(key);
          uint64_t bucket_pos = hash_value % bucket_num_;
          init_bucket_unit_(bucket_pos);
          BitLockGuard guard(bit_lock_, bucket_pos);
          if (EMPTY_FLAG == buckets_[bucket_pos].flag)
          {
            buckets_[bucket_pos].key = key;
            buckets_[bucket_pos].value = value;
            buckets_[bucket_pos].next = NULL;
            common::atomic_inc((uint64_t*)&bucket_using_);
            common::atomic_inc((uint64_t*)&size_);
          }
          else
          {
            Node *iter = &(buckets_[bucket_pos]);
            while (true)
            {
              if (equal_func_(iter->key, key))
              {
                ret = common::OB_ENTRY_EXIST;
                break;
              }
              if (NULL != iter->next)
              {
                iter = iter->next;
              }
              else
              {
                break;
              }
            }
            if (common::OB_SUCCESS == ret)
            {
              Node *node = (Node*)node_allocator_.alloc(sizeof(Node));
              if(NULL == node)
              {
                ret = common::OB_MEM_OVERFLOW;
              }
              else
              {
                node->key = key;
                node->value = value;
                node->next = NULL;
                iter->next = node;
                common::atomic_inc((uint64_t*)&size_);
              }
            }
          }
        }
        return ret;
      }

      template <typename Key, typename Value, typename BucketAllocator, typename NodeAllocator>
      int LightyHashMap<Key, Value, BucketAllocator, NodeAllocator>::get(const Key &key, Value &value)
      {
        int ret = common::OB_SUCCESS;
        if (NULL == buckets_
            || NULL == init_units_)
        {
          ret = common::OB_NOT_INIT;
        }
        else
        {
          uint64_t hash_value = hash_func_(key);
          uint64_t bucket_pos = hash_value % bucket_num_;
          init_bucket_unit_(bucket_pos);
          BitLockGuard guard(bit_lock_, bucket_pos);
          ret = common::OB_ENTRY_NOT_EXIST;
          if (EMPTY_FLAG != buckets_[bucket_pos].flag)
          {
            Node *iter = &(buckets_[bucket_pos]);
            while (NULL != iter)
            {
              if (equal_func_(iter->key, key))
              {
                value = iter->value;
                ret = common::OB_SUCCESS;
                break;
              }
              iter = iter->next;
            }
          }
        }
        return ret;
      }

      template <typename Key, typename Value, typename BucketAllocator, typename NodeAllocator>
      int LightyHashMap<Key, Value, BucketAllocator, NodeAllocator>::erase(const Key &key, Value *value)
      {
        int ret = common::OB_SUCCESS;
        if (NULL == buckets_
            || NULL == init_units_)
        {
          ret = common::OB_NOT_INIT;
        }
        else
        {
          uint64_t hash_value = hash_func_(key);
          uint64_t bucket_pos = hash_value % bucket_num_;
          init_bucket_unit_(bucket_pos);
          BitLockGuard guard(bit_lock_, bucket_pos);
          ret = common::OB_ENTRY_NOT_EXIST;
          if (EMPTY_FLAG != buckets_[bucket_pos].flag)
          {
            Node *iter = &(buckets_[bucket_pos]);
            Node *prev = NULL;
            while (NULL != iter)
            {
              if (equal_func_(iter->key, key))
              {
                if (NULL != value)
                {
                  *value = iter->value;
                }
                if (NULL == prev)
                {
                  buckets_[bucket_pos].flag = EMPTY_FLAG;
                }
                else
                {
                  // do not free deleted node
                  prev->next = iter->next;
                }
                ret = common::OB_SUCCESS;
                break;
              }
              prev = iter;
              iter = iter->next;
            }
          }
        }
        return ret;
      }

      template <typename Key, typename Value, typename BucketAllocator, typename NodeAllocator>
      int64_t LightyHashMap<Key, Value, BucketAllocator, NodeAllocator>::uninit_unit_num() const
      {
        return uninit_unit_num_;
      }

      template <typename Key, typename Value, typename BucketAllocator, typename NodeAllocator>
      int64_t LightyHashMap<Key, Value, BucketAllocator, NodeAllocator>::bucket_using() const
      {
        return bucket_using_;
      }

      template <typename Key, typename Value, typename BucketAllocator, typename NodeAllocator>
      int64_t LightyHashMap<Key, Value, BucketAllocator, NodeAllocator>::size() const
      {
        return size_;
      }

      template <typename Key, typename Value, typename BucketAllocator, typename NodeAllocator>
      void LightyHashMap<Key, Value, BucketAllocator, NodeAllocator>::init_bucket_unit_(const uint64_t bucket_pos)
      {
        while (0 < uninit_unit_num_)
        {
          uint64_t unit_pos = bucket_pos * sizeof(Node) / INIT_UNIT_SIZE;
          uint8_t ov = init_units_[unit_pos];
          if (ov & 0x80)
          {
            break;
          }
          ov = 0;
          uint8_t nv = ov | 0x01;
          if (ov == ATOMIC_CAS(&(init_units_[unit_pos]), ov, nv))
          {
            int64_t ms_size = std::min((bucket_num_ * sizeof(Node) - unit_pos * INIT_UNIT_SIZE), (uint64_t)INIT_UNIT_SIZE);
            memset((char*)buckets_ + unit_pos * INIT_UNIT_SIZE, -1, ms_size);
            ATOMIC_SUB(&uninit_unit_num_, 1);
            init_units_[unit_pos] = 0x80;
            break;
          }
        }
      }
    }
  }
}

#endif // OCEANBASE_UPDATESERVER_LIGHTY_HASH_H_

