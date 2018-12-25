////===================================================================
 //
 // ob_hash_engine.h / hash / common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2010-09-09 by Yubai (yubai.lk@taobao.com) 
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

#ifndef  OCEANBASE_UPDATESERVER_HASH_ENGINE_H_
#define  OCEANBASE_UPDATESERVER_HASH_ENGINE_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_define.h"
#include "common/hash/ob_hashmap.h"
#include "ob_table_engine.h"
#include "ob_trans_buffer.h"
#include "ob_memtank.h"

namespace oceanbase
{
  namespace updateserver
  {
    class HashEngine;
    class HashEngineIterator;
    class HashEngineTransHandle
    {
      friend class HashEngine;
      public:
        HashEngineTransHandle() : host_(NULL), trans_type_(INVALID_TRANSACTION)
        {
        };
      private:
        bool is_valid_(const HashEngine *host) const
        {
          bool bret = true;
          if (host_ != host)
          {
            TBSYS_LOG(WARN, "trans handle not match host=%p handle_host=%p", host, host_);
            bret = false;
          }
          return bret;
        };
        bool is_valid_(const HashEngine *host, const TETransType trans_type) const
        {
          bool bret = true;
          if (!is_valid_(host))
          {
            bret = false;
          }
          else if (trans_type_ != trans_type)
          {
            //TBSYS_LOG(WARN, "trans handle not match trans_type=%u handle_trans_type=%u", trans_type, trans_type_);
            bret = false;
          }
          return bret;
        };
        void reset()
        {
          host_ = NULL;
          trans_type_ = INVALID_TRANSACTION;
        };
      private:
        HashEngine *host_;
        TETransType trans_type_;
    };

    class HashEngine
    {
      friend class HashEngineIterator;
      friend class SortedIterator;
      friend class UnsortedIterator;

      static const int64_t DEFAULT_HASH_ITEM_NUM = 10000000; // 1千万条
      //static const int64_t DEFAULT_HASH_ITEM_NUM = 100000;
      static const int64_t DEFAULT_INDEX_ITEM_NUM = 1000000; // 1百万条
      //static const int64_t DEFAULT_INDEX_ITEM_NUM = 10000;

      // hashmap的key 保存了tableid和rowkey
      struct HashKey
      {
        TEKey key;
        HashKey()
        {
        };
        inline HashKey(const TEKey &key) : key(key)
        {
        };
        inline HashKey(const HashKey &other)
        {
          *this = other;
        }
        inline HashKey &operator =(const HashKey &other)
        {
          key = other.key;
          return *this;
        };
        inline int64_t hash() const
        {
          return key.hash();
        };
        const char *log_str() const
        {
          return key.log_str();
        };
        inline bool operator == (const HashKey &other) const
        {
          return (key == other.key);
        };
        inline bool operator != (const HashKey &other) const
        {
          return (key == other.key);
        };
        inline int operator - (const HashKey &other) const
        {
          return (key - other.key);
        };
      };

      struct HashValue;
      typedef common::hash::HashMapPair<HashKey, HashValue> HashKV;

      // hashmap的value 保存了行信息 和 rowkey前缀相同的下一个HashValue的指针(构成前缀链表)
      struct HashValue
      {
        TEValue value;
        HashKV *next;
        HashValue() : next(NULL)
        {
        };
        inline HashValue(const TEValue &value) : value(value)
        {
        };
        // 注意赋值操作和拷贝构造不改变next值
        inline HashValue(const HashValue &other)
        {
          *this = other;
        }
        inline HashValue &operator =(const HashValue &other)
        {
          value = other.value;
          return *this;
        };
        const char *log_str() const
        {
          return value.log_str();
        };
      };

      // 冻结表需要建立排序数组 IndexItem就是排序数组的元素
      struct IndexItem
      {
        HashKey *hash_key_ptr;
        HashValue *hash_value_ptr;
        inline bool operator < (const IndexItem &other) const
        {
          return ((*hash_key_ptr - *(other.hash_key_ptr)) < 0);
        };
        inline bool operator == (const IndexItem &other) const
        {
          return (*hash_key_ptr == *(other.hash_key_ptr));
        };
        inline bool operator <= (const IndexItem &other) const
        {
          return ((*hash_key_ptr - *(other.hash_key_ptr)) <= 0);
        };
      };

      // 用于存hashmap获取value指针的回调仿函数
      struct HashAtomicFunc
      {
        HashKV *pkv;
        HashAtomicFunc() : pkv(NULL)
        {
        };
        void operator ()(HashKV &kv)
        {
          pkv = &kv;
        };
      };

      // indexmap的key 保存了tableid和rwokey前缀(通过get_key_prefix函数转化)
      typedef HashKey IndexKey;

      // indexmap的value 保存了前缀链表的头指针
      struct IndexValue
      {
        HashKV *list_head; // 前缀链表头指针
        IndexValue() : list_head(NULL)
        {
        };
      };

      typedef common::hash::ObHashMap<HashKey, HashValue, common::hash::ReadWriteDefendMode,
                        common::hash::hash_func<HashKey>, common::hash::equal_to<HashKey>,
                        common::hash::SimpleAllocer<common::hash::HashMapTypes<HashKey, HashValue>::AllocType>,
                        common::hash::BigArray> hashmap_t;
      typedef common::hash::ObHashMap<HashKey, HashValue, common::hash::ReadWriteDefendMode,
                        common::hash::hash_func<HashKey>, common::hash::equal_to<HashKey>,
                        common::hash::SimpleAllocer<common::hash::HashMapTypes<HashKey, HashValue>::AllocType>,
                        common::hash::BigArray>::iterator hashmap_iterator_t;
      typedef common::hash::ObHashMap<HashKey, HashValue, common::hash::ReadWriteDefendMode,
                        common::hash::hash_func<HashKey>, common::hash::equal_to<HashKey>,
                        common::hash::SimpleAllocer<common::hash::HashMapTypes<HashKey, HashValue>::AllocType>,
                        common::hash::BigArray>::const_iterator hashmap_const_iterator_t;
      //typedef ObHashMap<HashKey, HashValue> hashmap_t;
      //typedef ObHashMap<HashKey, HashValue>::iterator hashmap_iterator_t;
      //typedef ObHashMap<HashKey, HashValue>::const_iterator hashmap_const_iterator_t;
      typedef common::hash::ObHashMap<IndexKey, IndexValue> indexmap_t;
      typedef common::hash::ObHashMap<IndexKey, IndexValue>::iterator indexmap_iterator_t;
      typedef common::hash::ObHashMap<IndexKey, IndexValue>::const_iterator indexmap_const_iterator_t;

      private:
        // 注意：为了效率考虑依赖hashmap的具体实现
        HashKV *get_hashkv_ptr_(const HashKey &key);

        bool update_map_(const TEKey &key, const TEValue &value);
        int query_map_(TEKey &key, TEValue &value);
        int sorted_scan_(const TEKey &start_key,
                        const int min_key,
                        const int start_exclude,
                        const TEKey &end_key,
                        const int max_key,
                        const int end_exclude,
                        HashEngineIterator &iter);
        int nosorted_scan_(const TEKey &start_key,
                          const int min_key,
                          const int start_exclude,
                          const TEKey &end_key,
                          const int max_key,
                          const int end_exclude,
                          HashEngineIterator &iter);
        bool search_index_(const IndexItem &start_key, const int start_exclude, const int min_key,
                          const IndexItem &end_key, const int end_exclude, const int max_key,
                          const IndexItem *&index_head, const IndexItem *&index_tail);
        TEKey &get_min_key_() const;
        TEKey &get_max_key_() const;

        static bool unsorted_result_filt_(const IndexValue &index_value,
                                          const TEKey &start_key, const int start_exclude,
                                          const TEKey &end_key, const int end_exclude,
                                          const HashKV *&list_head, const HashKV *&list_tail);
      public:
        HashEngine();
        ~HashEngine();
      public:
        int init(MemTank *allocer,
                int64_t hash_item_num = DEFAULT_HASH_ITEM_NUM,
                int64_t index_item_num = DEFAULT_INDEX_ITEM_NUM);
        int destroy();
      public:
        // 插入key-value对，如果已存在则覆盖
        // @param [in] key 待插入的key
        // @param [in] value 待插入的value
        int set(HashEngineTransHandle &handle,
                const TEKey &key,
                const TEValue &value);
        int set(const TEKey &key, const TEValue &value);
    
        // 获取指定key的value
        // @param [in] key 要查询的key
        // @param [out] value 查询返回的value
        int get(const HashEngineTransHandle &handle,
                TEKey &key,
                TEValue &value);
        int get(TEKey &key, TEValue &value);
        
        // 范围查询，返回一个iterator
        // @param [in] 查询范围的start key
        // @param [in] 是否由min key开始查询
        // @param [in] 查询范围是否包含start key本身, 0为包含, 非0为不包含; 在hash实现时必须为0
        // @param [in] 查询范围的end key
        // @param [in] 是否由max key开始查询
        // @param [in] 查询范围是否包含end key本身, 0为包含, 非0为不包含; 在hash实现时必须为0
        // @param [out] iter 查询结果的迭代器
        int scan(const HashEngineTransHandle &handle,
                const TEKey &start_key,
                const int min_key,
                const int start_exclude,
                const TEKey &end_key,
                const int max_key,
                const int end_exclude,
                HashEngineIterator &iter);
        
        // 开始一次事务
        // 在btree实现时使用copy on write技术; 在hash实现时什么都不做 因此不保证事务
        // @param [in] type 事务类型, 读事务 or 写事务
        // @param [out] 事务handle
        int start_transaction(const TETransType type,
                              HashEngineTransHandle &handle);
        
        // 结束一次事务 对于写事务表示事务的提交
        // @param [in] 事务handle
        // @param [in] rollback 是否回滚事务
        int end_transaction(HashEngineTransHandle &handle, bool rollback);

        // 开始一次事务性的更新
        // 一次mutation结束之前不能开启新的mutation
        int start_mutation(HashEngineTransHandle &handle);

        // 结束一次mutation
        // @param[in] rollback 是否回滚
        int end_mutation(HashEngineTransHandle &handle, bool rollback);
        
        // 建立索引
        // 在btree实现时什么都不做
        int create_index();

        int clear();

        void dump2text(const char *fname) const;

        int64_t size() const;
      private:
        bool inited_;
        bool created_index_;
        IndexItem *sorted_index_;
        int64_t sorted_index_size_;
        hashmap_t hashmap_;
        indexmap_t indexmap_;

        bool write_transaction_started_;
        bool mutation_started_;
        TransBuffer trans_buffer_;
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    class IteratorInterface
    {
      public:
        IteratorInterface() {};
        virtual ~IteratorInterface() {};
      public:
        virtual int next() = 0;
        virtual int get_key(TEKey &key) const = 0;
        virtual int get_value(TEValue &value) const = 0;
        virtual void restart() = 0;
    };

    class SortedIterator : public IteratorInterface
    {
      public:
        SortedIterator();
        ~SortedIterator();
      public:
        int next();
        int get_key(TEKey &key) const;
        int get_value(TEValue &value) const;
        void restart();
      public:
        void set(const HashEngine::IndexItem *start_index_item,
                const HashEngine::IndexItem *end_index_item);
      private:
        const HashEngine::IndexItem *start_index_item_;
        const HashEngine::IndexItem *cur_index_item_;
        const HashEngine::IndexItem *end_index_item_;
    };

    class UnsortedIterator : public IteratorInterface
    {
      public:
        UnsortedIterator();
        ~UnsortedIterator();
      public:
        int next();
        int get_key(TEKey &key) const;
        int get_value(TEValue &value) const;
        void restart();
      public:
        void set(const HashEngine::HashKV *start_hash_kv,
                const HashEngine::HashKV *end_hash_kv);
      private:
        const HashEngine::HashKV *start_hash_kv_;
        const HashEngine::HashKV *cur_hash_kv_;
        const HashEngine::HashKV *end_hash_kv_;
    };

    class HashEngineIterator
    {
      friend class HashEngine;
      public:
        HashEngineIterator();
        ~HashEngineIterator();
        HashEngineIterator(const HashEngineIterator &other);
        HashEngineIterator &operator = (const HashEngineIterator &other);
      public:
        // 迭代下一个元素
        int next();
        // 将迭代器指向的TEValue返回
        // @return  0  成功
        int get_key(TEKey &key) const;
        int get_value(TEValue &value) const;
        // 再从头开始迭代
        void restart();
        // 重置迭代器
        void reset();
      private:
        void unsorted_set_(const HashEngine::HashKV *start_hash_kv,
                          const HashEngine::HashKV *end_hash_kv);
        void sorted_set_(const HashEngine::IndexItem *start_index_item,
                        const HashEngine::IndexItem *end_index_item);
      private:
        SortedIterator sorted_iter_;
        UnsortedIterator unsorted_iter_;
        IteratorInterface *iter_;
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_HASH_ENGINE_H_

