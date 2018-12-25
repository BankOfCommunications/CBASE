////===================================================================
 //
 // ob_hashset.cpp / hash / common / Oceanbase
 //
 // Copyright (C) 2010, 2013 Taobao.com, Inc.
 //
 // Created on 2010-12-27 by Yubai (yubai.lk@taobao.com)
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

#ifndef  OCEANBASE_COMMON_HASH_HASHSET_H_
#define  OCEANBASE_COMMON_HASH_HASHSET_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include "ob_hashutils.h"
#include "ob_hashtable.h"
#include "ob_serialization.h"

namespace oceanbase
{
  namespace common
  {
    namespace hash
    {
      template <class _key_type>
      struct HashSetTypes
      {
        typedef HashMapPair<_key_type, HashNullObj> pair_type;
        typedef typename HashTableTypes<pair_type>::AllocType AllocType;
      };

      template <class _key_type,
                class _defendmode = ReadWriteDefendMode,
                class _hashfunc = hash_func<_key_type>,
                class _equal = equal_to<_key_type>,
                //class _allocer = SimpleAllocer<typename HashSetTypes<_key_type>::AllocType, 256, NoPthreadDefendMode> >
                class _allocer = SimpleAllocer<typename HashSetTypes<_key_type>::AllocType>,
                template <class> class _bucket_array = NormalPointer>
      class ObHashSet
      {
        typedef typename HashSetTypes<_key_type>::pair_type pair_type;
        typedef ObHashSet<_key_type, _hashfunc, _equal, _allocer, _defendmode> hashset;
        typedef ObHashTable<_key_type, pair_type, _hashfunc, _equal, pair_first<pair_type>, _allocer, _defendmode, _bucket_array, oceanbase::common::ObMalloc> hashtable;
      public:
        typedef typename hashtable::iterator iterator;
        typedef typename hashtable::const_iterator const_iterator;
      private:
        ObHashSet(const hashset &);
        hashset operator= (const hashset &);
      public:
          ObHashSet() : bucket_allocer_(ObModIds::OB_HASH_BUCKET), ht_()
        {
        };
        ~ObHashSet()
        {
        };
      public:
        iterator begin()
        {
          return ht_.begin();
        };
        const_iterator begin() const
        {
          return ht_.begin();
        };
        iterator end()
        {
          return ht_.end();
        };
        const_iterator end() const
        {
          return ht_.end();
        };
        int64_t size() const
        {
          return ht_.size();
        };
        int create(int64_t bucket_num)
        {
          return ht_.create(cal_next_prime(bucket_num), &allocer_, &bucket_allocer_);
        };
        int create(int64_t bucket_num, _allocer *allocer)
        {
          return ht_.create(cal_next_prime(bucket_num), allocer, &bucket_allocer_);
        };
        int destroy()
        {
          return ht_.destroy();
        };
        int clear()
        {
          return ht_.clear();
        };
        // 返回  -1表示有错误发生
        // 返回  HASH_EXIST表示结点存在
        // 返回  HASH_NOT_EXIST表示结点不存在
        int exist(const _key_type &key) const
        {
          int ret = 0;
          pair_type pair;
          ret = const_cast<hashtable&>(ht_).get(key, pair);
          return ret;
        };
        // flag默认为0表示不覆盖已有元素 非0表示覆盖已有元素
        // 返回  -1  表示set调用出错, (无法分配新结点等)
        // 其他均表示插入成功：插入成功分下面三个状态
        // 返回  HASH_OVERWRITE  表示覆盖旧结点成功(在flag非0的时候返回）
        // 返回  HASH_INSERT_SEC 表示插入新结点成功
        // 返回  HASH_EXIST  表示hash表结点存在（在flag为0的时候返回)
        int set(const _key_type &key)
        {
          pair_type pair(key, HashNullObj());
          return ht_.set(key, pair, 1);
        };
        // 返回  -1表示有错误发生
        // 返回  HASH_EXIST表示结点存在并删除成功
        // 返回  HASH_NOT_EXIST表示结点不存在不用删除
        // 如果value指针不为空并且删除成功则将值存入value指向的空间
        int erase(const _key_type &key)
        {
          int ret = 0;
          ret = ht_.erase(key);
          return ret;
        };
        template <class _archive>
        int serialization(_archive &archive)
        {
          return ht_.serialization(archive);
        };
        template <class _archive>
        int deserialization(_archive &archive)
        {
          return ht_.deserialization(archive, &allocer_);
        };
      private:
        _allocer allocer_;
          oceanbase::common::ObMalloc bucket_allocer_;
        hashtable ht_;
      };
    }
  }
}

#endif //OCEANBASE_COMMON_HASH_HASHSET_H_
