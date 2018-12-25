////===================================================================
 //
 // ob_hashtable.cpp / hash / common / Oceanbase
 //
 // Copyright (C) 2010, 2013 Taobao.com, Inc.
 //
 // Created on 2010-07-23 by Yubai (yubai.lk@taobao.com)
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

#ifndef  OCEANBASE_COMMON_HASH_HASHTABLE_H_
#define  OCEANBASE_COMMON_HASH_HASHTABLE_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include "ob_hashutils.h"
#include "ob_serialization.h"
#include "common/ob_atomic.h"
#include "common/ob_allocator.h"
namespace oceanbase
{
  namespace common
  {
    namespace hash
    {
      template <class _key_type, class _value_type, class _hashfunc, class _equal, class _getkey, class _allocer, class _defendmode,
                template <class> class _bucket_array, class _bucket_allocer>
        class ObHashTable;
      template <class _key_type, class _value_type, class _hashfunc, class _equal, class _getkey, class _allocer, class _defendmode,
                template <class> class _bucket_array, class _bucket_allocer>
        class ObHashTableIterator;
      template <class _key_type, class _value_type, class _hashfunc, class _equal, class _getkey, class _allocer, class _defendmode,
                template <class> class _bucket_array, class _bucket_allocer>
        class ObHashTableConstIterator;

      template <class _value_type>
      struct ObHashTableNode
      {
        _value_type data;
        volatile bool is_fake;
        ObHashTableNode *next;
      };

      template <class _value_type, class _lock_type, class _cond_type=NCond>
      struct ObHashTableBucket
      {
        mutable _lock_type lock;
        mutable _cond_type cond;
        ObHashTableNode<_value_type> *node;
      };

      template <class _key_type,
               class _value_type,
               class _hashfunc,
               class _equal,
               class _getkey,
               class _allocer,
               class _defendmode,
                template <class> class _bucket_array,
                class _bucket_allocer>
      class ObHashTableIterator
      {
        private:
          typedef ObHashTableNode<_value_type> hashnode;
          typedef ObHashTable<_key_type, _value_type, _hashfunc, _equal, _getkey, _allocer, _defendmode, _bucket_array, _bucket_allocer> hashtable;
          typedef ObHashTableIterator<_key_type, _value_type, _hashfunc, _equal, _getkey, _allocer, _defendmode, _bucket_array, _bucket_allocer> iterator;
          typedef ObHashTableConstIterator<_key_type, _value_type, _hashfunc, _equal, _getkey, _allocer, _defendmode, _bucket_array, _bucket_allocer> const_iterator;
          typedef _value_type &reference;
          typedef _value_type *pointer;
          friend class ObHashTableConstIterator<_key_type, _value_type, _hashfunc, _equal, _getkey, _allocer, _defendmode, _bucket_array, _bucket_allocer>;
        public:
          ObHashTableIterator() : ht_(NULL), bucket_pos_(0), node_(0)
          {
          };
          ObHashTableIterator(const iterator &other) : ht_(other.ht_), bucket_pos_(other.bucket_pos_), node_(other.node_)
          {
          };
          ObHashTableIterator(const hashtable *ht, int64_t bucket_pos, hashnode *node) : ht_(ht), bucket_pos_(bucket_pos), node_(node)
          {
          };
          reference operator * () const
          {
            return node_->data;
          };
          pointer operator -> () const
          {
            return &(node_->data);
          };
          bool operator == (const iterator &iter) const
          {
            return (node_ == iter.node_);
          };
          bool operator != (const iterator &iter) const
          {
            return (node_ != iter.node_);
          };
          iterator &operator ++ ()
          {
            if (NULL != node_
                && NULL != (node_ = node_->next))
            {
              // do nothing
            }
            else
            {
              for (int64_t i = bucket_pos_ + 1; i < ht_->bucket_num_; i++)
              {
                if (NULL != (node_ = ht_->buckets_[i].node))
                {
                  bucket_pos_ = i;
                  break;
                }
              }
              if (NULL == node_)
              {
                bucket_pos_ = ht_->bucket_num_;
              }
            }
            return *this;
          };
          iterator operator ++ (int)
          {
            iterator iter = *this;
            ++ *this;
            return iter;
          };
        private:
          const hashtable *ht_;
          int64_t bucket_pos_;
          hashnode *node_;
      };

      template <class _key_type,
               class _value_type,
               class _hashfunc,
               class _equal,
               class _getkey,
               class _allocer,
               class _defendmode,
                template <class> class _bucket_array,
                class _bucket_allocer>
      class ObHashTableConstIterator
      {
        private:
          typedef ObHashTableNode<_value_type> hashnode;
          typedef ObHashTable<_key_type, _value_type, _hashfunc, _equal, _getkey, _allocer, _defendmode, _bucket_array, _bucket_allocer> hashtable;
          typedef ObHashTableIterator<_key_type, _value_type, _hashfunc, _equal, _getkey, _allocer, _defendmode, _bucket_array, _bucket_allocer> iterator;
          typedef ObHashTableConstIterator<_key_type, _value_type, _hashfunc, _equal, _getkey, _allocer, _defendmode, _bucket_array, _bucket_allocer> const_iterator;
          typedef const _value_type &const_reference;
          typedef const _value_type *const_pointer;
          friend class ObHashTableIterator<_key_type, _value_type, _hashfunc, _equal, _getkey, _allocer, _defendmode, _bucket_array, _bucket_allocer>;
        public:
          ObHashTableConstIterator() : ht_(NULL), bucket_pos_(0), node_(0)
          {
          };
          ObHashTableConstIterator(const const_iterator &other) : ht_(other.ht_), bucket_pos_(other.bucket_pos_), node_(other.node_)
          {
          };
          ObHashTableConstIterator(const iterator &other) : ht_(other.ht_), bucket_pos_(other.bucket_pos_), node_(other.node_)
          {
          };
          ObHashTableConstIterator(const hashtable *ht, int64_t bucket_pos, hashnode *node) : ht_(ht), bucket_pos_(bucket_pos), node_(node)
          {
          };
          const_reference operator * () const
          {
            return node_->data;
          };
          const_pointer operator -> () const
          {
            return &(node_->data);
          };
          bool operator == (const const_iterator &iter) const
          {
            return (node_ == iter.node_);
          };
          bool operator != (const const_iterator &iter) const
          {
            return (node_ != iter.node_);
          };
          const_iterator &operator ++ ()
          {
            if (NULL != node_
                && NULL != (node_ = node_->next))
            {
              // do nothing
            }
            else
            {
              for (int64_t i = bucket_pos_ + 1; i < ht_->bucket_num_; i++)
              {
                if (NULL != (node_ = ht_->buckets_[i].node))
                {
                  bucket_pos_ = i;
                  break;
                }
              }
              if (NULL == node_)
              {
                bucket_pos_ = ht_->bucket_num_;
              }
            }
            return *this;
          };
          const_iterator operator ++ (int)
          {
            const_iterator iter = *this;
            ++ *this;
            return iter;
          };
        private:
          const hashtable *ht_;
          int64_t bucket_pos_;
          hashnode *node_;
      };

      template <class _value_type>
      struct HashTableTypes
      {
        typedef ObHashTableNode<_value_type> AllocType;
      };

      template <class _key_type,  // key类型
               class _value_type, // value类型
               class _hashfunc,   // hash函数
               class _equal,      // 判断两个key是否相同的函数
               class _getkey,     // 根据value获取key的函数
               class _allocer,    // 内存分配器
               class _defendmode, // 多线程保护模式
                template <class> class _bucket_array,
                class _bucket_allocer = oceanbase::common::ObMalloc >
      class ObHashTable
      {
        public:
          typedef ObHashTableIterator<_key_type, _value_type, _hashfunc, _equal, _getkey, _allocer, _defendmode, _bucket_array, _bucket_allocer> iterator;
          typedef ObHashTableConstIterator<_key_type, _value_type, _hashfunc, _equal, _getkey, _allocer, _defendmode, _bucket_array, _bucket_allocer> const_iterator;
          typedef ObHashTableNode<_value_type> hashnode;
        private:
          static const int64_t ARRAY_SIZE = 1024 * 16;  // 乘以hashbucket的大小约1M 必须是2的整数次方
          typedef typename _defendmode::readlocker readlocker;
          typedef typename _defendmode::writelocker writelocker;
          typedef typename _defendmode::lock_type lock_type;
          typedef typename _defendmode::cond_type cond_type;
          typedef typename _defendmode::cond_waiter cond_waiter;
          typedef typename _defendmode::cond_broadcaster cond_broadcaster;
          typedef ObHashTable<_key_type, _value_type, _hashfunc, _equal, _getkey, _allocer, _defendmode, _bucket_array, _bucket_allocer> hashtable;
          typedef ObHashTableBucket<_value_type, lock_type, cond_type> hashbucket;
          typedef pre_proc<_value_type> preproc;
          typedef typename _bucket_array<hashbucket>::array_type bucket_array;
          friend class ObHashTableIterator<_key_type, _value_type, _hashfunc, _equal, _getkey, _allocer, _defendmode, _bucket_array, _bucket_allocer>;
          friend class ObHashTableConstIterator<_key_type, _value_type, _hashfunc, _equal, _getkey, _allocer, _defendmode, _bucket_array, _bucket_allocer>;
        private:
          ObHashTable(const hashtable &);
          hashtable operator= (const hashtable &);
        public:
          //ObHashTable() : allocer_(NULL), buckets_(NULL), bucket_num_(0), size_(0)
          ObHashTable() : allocer_(NULL), bucket_allocer_(&default_bucket_allocer_), bucket_num_(0), size_(0)
          {
            construct(buckets_);
          };
          ~ObHashTable()
          {
            //if (NULL != buckets_ && NULL != allocer_)
            if (inited(buckets_) && NULL != allocer_)
            {
              destroy();
            }
          };
        public:
          inline bool created()
          {
            return inited(buckets_);
          }
          // 注意 向上取质数自己调用ob_hashutils.h中的cal_next_prime去算，这里不管
          int create(int64_t bucket_num, _allocer *allocer, _bucket_allocer* bucket_allocer)
          {
            int ret = 0;
            if (0 >= bucket_num || NULL == allocer)
            {
              HASH_WRITE_LOG(HASH_WARNING, "invalid param bucket_num=%ld allocer=%p", bucket_num, allocer);
              ret = -1;
            }
            else if (inited(buckets_))
            {
              HASH_WRITE_LOG(HASH_WARNING, "hashtable has already been created allocer=%p bucket_num=%ld",
                            allocer_, bucket_num_);
              ret = -1;
            }
            else if (0 != hash::create(buckets_, bucket_num, ARRAY_SIZE, sizeof(hashbucket), *bucket_allocer))
            {
              HASH_WRITE_LOG(HASH_WARNING, "create buckets fail");
              ret = -1;
            }
            else
            {
              //memset(buckets_, 0, sizeof(hashbucket) * bucket_num);
              bucket_num_ = bucket_num;
              allocer_ = allocer;
              allocer_->inc_ref();
              bucket_allocer_ = bucket_allocer;
            }
            return ret;
          };
          int destroy()
          {
            int ret = 0;
            //if (NULL == buckets_ || NULL == allocer_)
            if (!inited(buckets_) || NULL == allocer_)
            {
              HASH_WRITE_LOG(HASH_DEBUG, "hashtable is empty");
            }
            else
            {
              for (int64_t i = 0; i < bucket_num_; i++)
              {
                hashnode *cur_node = NULL;
                if (NULL != (cur_node = buckets_[i].node))
                {
                  while (NULL != cur_node)
                  {
                    hashnode *tmp_node = cur_node->next;
                    allocer_->free(cur_node);
                    cur_node = tmp_node;
                  }
                  buckets_[i].node = NULL;
                }
              }
              allocer_->dec_ref();
              allocer_->~_allocer();//add dolphin fix memory leak when hashnode holding object which is pointer@20160608
              allocer_ = NULL;
              //delete[] buckets_;
              //buckets_ = NULL;
              hash::destroy(buckets_, *bucket_allocer_);
              bucket_num_ = 0;
              size_ = 0;
            }
            return ret;
          };
          int clear()
          {
            int ret = 0;
            //if (NULL == buckets_ || NULL == allocer_)
            if (!inited(buckets_) || NULL == allocer_)
            {
              ret = -1;
            }
            else
            {
              for (int64_t i = 0; i < bucket_num_; i++)
              {
                writelocker locker(buckets_[i].lock);
                hashnode *cur_node = NULL;
                if (NULL != (cur_node = buckets_[i].node))
                {
                  while (NULL != cur_node)
                  {
                    hashnode *tmp_node = cur_node->next;
                    allocer_->free(cur_node);
                    cur_node = tmp_node;
                  }
                }
                buckets_[i].node = NULL;
              }
              size_ = 0;
            }
            return ret;
          };
          iterator begin()
          {
            hashnode *node = NULL;
            int64_t bucket_pos = 0;
            //if (NULL == buckets_ || NULL == allocer_)
            if (!inited(buckets_) || NULL == allocer_)
            {
              HASH_WRITE_LOG(HASH_WARNING, "hashtable is empty");
            }
            else
            {
              while (NULL == node && bucket_pos < bucket_num_)
              {
                node = buckets_[bucket_pos].node;
                if (NULL == node)
                {
                  ++bucket_pos;
                }
              }
            }
            return iterator(this, bucket_pos, node);
          };
          iterator end()
          {
            return iterator(this, bucket_num_, NULL);
          };
          const_iterator begin() const
          {
            hashnode *node = NULL;
            int64_t bucket_pos = 0;
            //if (NULL == buckets_ || NULL == allocer_)
            if (!inited(buckets_) || NULL == allocer_)
            {
              HASH_WRITE_LOG(HASH_WARNING, "hashtable is empty");
            }
            else
            {
              while (NULL == node && bucket_pos < bucket_num_)
              {
                node = buckets_[bucket_pos].node;
                if (NULL == node)
                {
                  ++bucket_pos;
                }
              }
            }
            return const_iterator(this, bucket_pos, node);
          };
          const_iterator end() const
          {
            return const_iterator(this, bucket_num_, NULL);
          };
        private:
          inline int internal_get(const hashbucket &bucket, const _key_type &key,
                           _value_type &value, bool &is_fake) const
          {
            int ret = HASH_NOT_EXIST;
            hashnode *node = bucket.node;
            is_fake = false;

            while (NULL != node)
            {
              if (equal_(getkey_(node->data), key))
              {
                value = node->data;
                is_fake = node->is_fake;
                ret = HASH_EXIST;
                break;
              }
              else
              {
                node = node->next;
              }
            }

            return ret;
          }
          inline int internal_get(const hashbucket &bucket, const _key_type &key,
                           const _value_type *&value, bool &is_fake) const
          {
            int ret = HASH_NOT_EXIST;
            hashnode *node = bucket.node;
            is_fake = false;

            while (NULL != node)
            {
              if (equal_(getkey_(node->data), key))
              {
                value = &(node->data);
                is_fake = node->is_fake;
                ret = HASH_EXIST;
                break;
              }
              else
              {
                node = node->next;
              }
            }

            return ret;
          }
          inline int internal_set(hashbucket &bucket, const _value_type &value, const bool is_fake)
          {
            int ret = HASH_INSERT_SUCC;
            hashnode *node = NULL;

            node = (hashnode*)(allocer_->alloc());
            if (NULL == node)
            {
              ret = -1;
            }
            else
            {
              node->data = value;
              node->is_fake = is_fake;
              node->next = bucket.node;
              bucket.node = node;
              atomic_inc((uint64_t*)&size_);
            }

            return ret;
          }
        public:
          // 以下四个函数提供多线程安全保证
          // 成功返回HASH_EXIST
          // 如果key不存在则返回HASH_NOT_EXIST
          // if fake node is existent, also return HASH_NOT_EXIST
          // if timeout_us is not zero, check fake node, if no fake node, add
          // fake node, else wait fake node become normal node
          int get(const _key_type &key, _value_type &value, const int64_t timeout_us = 0)
          {
            int ret = 0;
            //if (NULL == buckets_ || NULL == allocer_)
            if (!inited(buckets_) || NULL == allocer_)
            {
              HASH_WRITE_LOG(HASH_WARNING, "hashtable is empty");
              ret = -1;
            }
            else
            {
              uint64_t hash_value = hashfunc_(key);
              int64_t bucket_pos = hash_value % bucket_num_;
              hashbucket &bucket = buckets_[bucket_pos];
              bool is_fake = false;
              readlocker locker(bucket.lock);
              ret = internal_get(bucket, key, value, is_fake);

              if (timeout_us > 0)
              {
                if (HASH_EXIST == ret && is_fake)
                {
                  struct timespec ts;
                  int64_t abs_timeout_us = get_cur_microseconds_time() + timeout_us;
                  ts = microseconds_to_ts(abs_timeout_us);
                  do
                  {
                    if (get_cur_microseconds_time() > abs_timeout_us
                        || ETIMEDOUT == cond_waiter()(bucket.cond, bucket.lock, ts))
                    {
                      HASH_WRITE_LOG(HASH_WARNING, "wait fake node become normal node timeout");
                      ret = HASH_GET_TIMEOUT;
                      break;
                    }
                    ret = internal_get(bucket, key, value, is_fake);
                    if (HASH_NOT_EXIST == ret)
                    {
                      HASH_WRITE_LOG(HASH_WARNING, "after wake up, fake node is non-existent or deleted");
                      ret = OB_ERROR;
                      break;
                    }
                  }
                  while (HASH_EXIST == ret && is_fake);
                }
                if (HASH_NOT_EXIST == ret)
                {
                  //add a fake value
                  if (HASH_INSERT_SUCC != internal_set(bucket, value, true))
                  {
                    ret = -1;
                  }
                }
              }
              else
              {
                if (HASH_EXIST == ret && is_fake)
                {
                  ret = HASH_NOT_EXIST;
                }
              }
            }
            return ret;
          };
          int get(const _key_type &key, const _value_type *&value, const int64_t timeout_us = 0)
          {
            int ret = 0;
            //if (NULL == buckets_ || NULL == allocer_)
            if (!inited(buckets_) || NULL == allocer_)
            {
              HASH_WRITE_LOG(HASH_WARNING, "hashtable is empty");
              ret = -1;
            }
            else
            {
              uint64_t hash_value = hashfunc_(key);
              int64_t bucket_pos = hash_value % bucket_num_;
              hashbucket &bucket = buckets_[bucket_pos];
              bool is_fake = false;
              readlocker locker(bucket.lock);
              ret = internal_get(bucket, key, value, is_fake);

              if (timeout_us > 0)
              {
                if (HASH_EXIST == ret && is_fake)
                {
                  struct timespec ts;
                  int64_t abs_timeout_us = get_cur_microseconds_time() + timeout_us;
                  ts = microseconds_to_ts(abs_timeout_us);
                  do
                  {
                    if (get_cur_microseconds_time() > abs_timeout_us
                        || ETIMEDOUT == cond_waiter()(bucket.cond, bucket.lock, ts))
                    {
                      HASH_WRITE_LOG(HASH_WARNING, "wait fake node become normal node timeout");
                      ret = HASH_GET_TIMEOUT;
                      break;
                    }
                    ret = internal_get(bucket, key, value, is_fake);
                    if (HASH_NOT_EXIST == ret)
                    {
                      HASH_WRITE_LOG(HASH_WARNING, "after wake up, fake node is non-existent or deleted");
                      ret = OB_ERROR;
                      break;
                    }
                  }
                  while (HASH_EXIST == ret && is_fake);
                }
                if (HASH_NOT_EXIST == ret)
                {
                  //add a fake value
                  if (HASH_INSERT_SUCC != internal_set(bucket, _value_type(), true))
                  {
                    ret = -1;
                  }
                }
              }
              else
              {
                if (HASH_EXIST == ret && is_fake)
                {
                  ret = HASH_NOT_EXIST;
                }
              }
            }
            return ret;
          };
          // 返回  -1  表示set调用出错, (无法分配新结点等)
          // 其他均表示插入成功：插入成功分下面三个状态
          // 返回  HASH_OVERWRITE_SUCC  表示覆盖旧结点成功(在flag非0的时候返回）
          // 返回  HASH_INSERT_SEC 表示插入新结点成功
          // 返回  HASH_EXIST  表示hash表结点存在（在flag为0的时候返回)
          int set(const _key_type &key, const _value_type &value, int flag = 0,
                  int broadcast = 0, int overwrite_key = 0)
          {
            int ret = 0;
            //if (NULL == buckets_ || NULL == allocer_)
            if (!inited(buckets_) || NULL == allocer_)
            {
              HASH_WRITE_LOG(HASH_WARNING, "hashtable is empty");
              ret = -1;
            }
            else
            {
              uint64_t hash_value = hashfunc_(key);
              int64_t bucket_pos = hash_value % bucket_num_;
              hashbucket &bucket = buckets_[bucket_pos];
              writelocker locker(bucket.lock);
              hashnode *node = bucket.node;
              while (NULL != node)
              {
                if (equal_(getkey_(node->data), key))
                {
                  if (0 == flag)
                  {
                    ret = HASH_EXIST;
                  }
                  else
                  {
                    if (overwrite_key)
                    {
                      hash::copy(node->data, value, hash::NormalPairTag());
                    }
                    else
                    {
                      hash::copy(node->data, value);
                    }
                    node->is_fake = false;
                    if (broadcast)
                    {
                      cond_broadcaster()(bucket.cond);
                    }
                    ret = HASH_OVERWRITE_SUCC;
                  }
                  break;
                }
                else
                {
                  node = node->next;
                }
              }
              if (NULL == node)
              {
                ret = internal_set(bucket, value, false);
              }
            }
            return ret;
          };
          template <class _callback>
          // 注意 atomic用于原子性的更新一个value,在bucket粒度的锁中执行
          // 所以callback仿函数中严禁调用hashtable的任何方法
          // 返回  HASH_EXIST表示结点存在并执行了callback
          // 返回  HASH_NOEXIST表示结点不存在
          int atomic(const _key_type &key, _callback &callback)
          {
            return atomic(key, callback, preproc_);
          };
          // 返回  HASH_EXIST表示结点存在并删除成功
          // 返回  HASH_NOEXIST表示结点不存在不用删除
          int erase(const _key_type &key, _value_type *value = NULL)
          {
            int ret = 0;
            //if (NULL == buckets_ || NULL == allocer_)
            if (!inited(buckets_) || NULL == allocer_)
            {
              HASH_WRITE_LOG(HASH_WARNING, "hashtable is empty");
              ret = -1;
            }
            else
            {
              uint64_t hash_value = hashfunc_(key);
              int64_t bucket_pos = hash_value % bucket_num_;
              hashbucket &bucket = buckets_[bucket_pos];
              writelocker locker(bucket.lock);
              hashnode *node = bucket.node;
              hashnode *prev = NULL;
              ret = HASH_NOT_EXIST;
              while (NULL != node)
              {
                if (equal_(getkey_(node->data), key))
                {
                  if (NULL == prev)
                  {
                    bucket.node = node->next;
                  }
                  else
                  {
                    prev->next = node->next;
                  }
                  if (NULL != value)
                  {
                    *value = node->data;
                  }
                  allocer_->free(node);
                  atomic_dec((uint64_t*)&size_);
                  cond_broadcaster()(bucket.cond); 
                  ret = HASH_EXIST;
                  break;
                }
                else
                {
                  prev = node;
                  node = node->next;
                }
              }
            }
            return ret;
          };

          /**
           * thread safe scan, will add read lock to the bucket, the modification to the value is forbidden
           *
           * @param callback
           * @return 0 in case success
           *         -1 in case not initialized
           */
          template<class _callback>
          int foreach(_callback &callback)
          {
            int ret = 0;
            if (!inited(buckets_) || NULL == allocer_)
            {
              HASH_WRITE_LOG(HASH_WARNING, "hashtable is empty");
              ret = -1;
            }
            else
            {
              for (int64_t i = 0; i < bucket_num_; i++)
              {
                const hashbucket &bucket = buckets_[i];
                readlocker locker(bucket.lock);
                hashnode *node = bucket.node;
                while (NULL != node)
                {
                  callback(node->data);
                  node = node->next;
                }
              }
            }
            return ret;
          }
        public:
          int64_t size() const
          {
            return size_;
          };
        public:
          template <class _archive>
          int serialization(_archive &archive)
          {
            int ret = 0;
            //if (NULL == buckets_ || NULL == allocer_)
            if (!inited(buckets_) || NULL == allocer_)
            {
              HASH_WRITE_LOG(HASH_WARNING, "hashtable is empty");
              ret = -1;
            }
            else if (0 != hash::serialization(archive, bucket_num_))
            {
              HASH_WRITE_LOG(HASH_WARNING, "serialize hash bucket_num fail bucket_num=%ld", bucket_num_);
              ret = -1;
            }
            else if (0 != hash::serialization(archive, size_))
            {
              HASH_WRITE_LOG(HASH_WARNING, "serialize hash size fail size=%ld", size_);
              ret = -1;
            }
            else if (size_ > 0)
            {
              for (iterator iter = begin(); iter != end(); iter++)
              {
                if (0 != hash::serialization(archive, *iter))
                {
                  HASH_WRITE_LOG(HASH_WARNING, "serialize item fail value_pointer=%p", &(*iter));
                  ret = -1;
                  break;
                }
              }
            }
            return ret;
          };
          template <class _archive>
          int deserialization(_archive &archive, _allocer *allocer)
          {
            int ret = 0;
            int64_t bucket_num = 0;
            int64_t size = 0;
            if (NULL == allocer)
            {
              HASH_WRITE_LOG(HASH_WARNING, "invalid param allocer null pointer");
              ret = -1;
            }
            else if (0 != hash::deserialization(archive, bucket_num)
                || 0 >= bucket_num)
            {
              HASH_WRITE_LOG(HASH_WARNING, "deserialize bucket_num fail");
              ret = -1;
            }
            else if (0 != hash::deserialization(archive, size))
            {
              HASH_WRITE_LOG(HASH_WARNING, "deserialize size fail");
              ret = -1;
            }
            else
            {
              //if (NULL != buckets_ && NULL != allocer_)
              if (inited(buckets_) && NULL != allocer_)
              {
                destroy();
              }
              if (0 == (ret = create(bucket_num, allocer, bucket_allocer_)))
              {
                _value_type value;
                for (int64_t i = 0; i < size; i++)
                {
                  if (0 != hash::deserialization(archive, value))
                  {
                    HASH_WRITE_LOG(HASH_WARNING, "deserialize item fail num=%ld", i);
                    ret = -1;
                    break;
                  }
                  else if (HASH_INSERT_SUCC != set(getkey_(value), value))
                  {
                    HASH_WRITE_LOG(HASH_WARNING, "insert item fail num=%ld", i);
                    ret = -1;
                    break;
                  }
                  else
                  {
                    // do nothing
                  }
                }
              }
            }
            return ret;
          };
          template <class _callback, class _preproc>
          // 在传给callback之前执行预处理一般不对外使用
          int atomic(const _key_type &key, _callback &callback, _preproc &preproc)
          {
            int ret = 0;
            //if (NULL == buckets_ || NULL == allocer_)
            if (!inited(buckets_) || NULL == allocer_)
            {
              HASH_WRITE_LOG(HASH_WARNING, "hashtable is empty");
              ret = -1;
            }
            else
            {
              uint64_t hash_value = hashfunc_(key);
              int64_t bucket_pos = hash_value % bucket_num_;
              const hashbucket &bucket = buckets_[bucket_pos];
              writelocker locker(bucket.lock);
              hashnode *node = bucket.node;
              ret = HASH_NOT_EXIST;
              while (NULL != node)
              {
                if (equal_(getkey_(node->data), key))
                {
                  callback(preproc(node->data));
                  ret = HASH_EXIST;
                  break;
                }
                else
                {
                  node = node->next;
                }
              }
            }
            return ret;
          };
        private:
          _bucket_allocer default_bucket_allocer_;
          _allocer *allocer_;
          _bucket_allocer *bucket_allocer_;
          //hashbucket *buckets_;
          bucket_array buckets_;
          int64_t bucket_num_;
          int64_t size_;

          mutable preproc preproc_;
          mutable _hashfunc hashfunc_;
          mutable _equal equal_;
          mutable _getkey getkey_;
      };
    }
  }
}

#endif //OCEANBASE_COMMON_HASH_HASHTABLE_H_
