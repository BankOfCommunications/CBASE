////===================================================================
 //
 // ob_query_engine.h / hash / common / Oceanbase
 //
 // Copyright (C) 2010, 2012 Taobao.com, Inc.
 //
 // Created on 2012-03-28 by Yubai (yubai.lk@taobao.com)
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

#ifndef  OCEANBASE_UPDATESERVER_QUERY_ENGINE_H_
#define  OCEANBASE_UPDATESERVER_QUERY_ENGINE_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_define.h"
#include "common/cmbtree/btree_base.h"
#include "common/hash/ob_hashmap.h"
#include "common/page_arena.h"
#include "common/ob_list.h"
#include "ob_table_engine.h"
#include "ob_memtank.h"
#include "ob_btree_engine_alloc.h"
#include "ob_lighty_hash.h"

namespace oceanbase
{
  namespace updateserver
  {
    class HashEngineAllocator
    {
      public:
        HashEngineAllocator(MemTank &mt) : mt_(mt) {};
        ~HashEngineAllocator() {};
      public:
        void *alloc(const int64_t size)
        {
          return mt_.hash_engine_alloc(size);
        };
        void free(void *ptr)
        {
          UNUSED(ptr);
        };
      private:
        MemTank &mt_;
    };

    class BtreeEngineAllocator
    {
      public:
        BtreeEngineAllocator(MemTank &mt) : mt_(mt) {};
        BtreeEngineAllocator(BtreeEngineAllocator &other) : mt_(other.mt_) {};
        ~BtreeEngineAllocator() {};
      public:
        void *alloc(const int64_t size)
        {
          return mt_.btree_engine_alloc(static_cast<int32_t>(size));
        };
        void free(void *ptr)
        {
          UNUSED(ptr);
        };
      private:
        MemTank &mt_;
    };

    class QueryEngineIterator;
    class QueryEngine
    {
      friend class QueryEngineIterator;
      typedef common::cmbtree::BtreeBase<TEKey, TEValue*, BtreeEngineAllocator> keybtree_t;
      typedef lightyhash::LightyHashMap<TEHashKey, TEValue*, HashEngineAllocator, HashEngineAllocator> keyhash_t;
      public:
        static int64_t HASH_SIZE;
      public:
        QueryEngine(MemTank &allocer);
        ~QueryEngine();
      public:
        int init(const int64_t hash_size = 0);
        int destroy();
      public:
        // 插入key-value对
        // @param [in] key 待插入的key
        // @param [in] value 待插入的value
        int set(const TEKey &key, TEValue *value);

        // 获取指定key的value
        // @param [in] key 要查询的key
        // @param [out] value 查询返回的value
        TEValue *get(const TEKey &key);

        // 范围查询，返回一个iterator
        // @param [in] 查询范围的start key
        // @param [in] 是否由min key开始查询
        // @param [in] 查询范围是否包含start key本身, 0为包含, 非0为不包含;
        // @param [in] 查询范围的end key
        // @param [in] 是否由max key开始查询
        // @param [in] 查询范围是否包含end key本身, 0为包含, 非0为不包含;
        // @param [out] iter 查询结果的迭代器
        int scan(const TEKey &start_key,
                const int min_key,
                const int start_exclude,
                const TEKey &end_key,
                const int max_key,
                const int end_exclude,
                const bool reverse,
                QueryEngineIterator &iter);

        int scan_table(const TEKey &start_key,
                const int min_key,
                const int start_exclude,
                const TEKey &end_key,
                const int max_key,
                const int end_exclude,
                const bool reverse,
                QueryEngineIterator &iter); /*add zhaoqiong [Truncate Table]:20160318*/

        int clear();

        void dump2text(const char *fname);

        int64_t btree_size();
        int64_t btree_alloc_memory();
        int64_t btree_reserved_memory();
        void    btree_dump_mem_info();
        int64_t hash_size() const;
        int64_t hash_bucket_using() const;
        int64_t hash_uninit_unit_num() const;
        int get_table_truncate_stat (uint64_t table_id, bool &is_truncated); /*add zhaoqiong [Truncate Table]:20160318*/
      private:
        bool inited_;
        BtreeEngineAllocator btree_alloc_;
        BtreeEngineAllocator table_btree_alloc_; /*add zhaoqiong [Truncate Table]:20160318*/
        HashEngineAllocator hash_alloc_;
        keybtree_t keybtree_;
        keybtree_t table_keybtree_; /*add zhaoqiong [Truncate Table]:20160318*/
        keyhash_t keyhash_;
    };

    class QueryEngineIterator
    {
      friend class QueryEngine;
      public:
        QueryEngineIterator();
        ~QueryEngineIterator();
      public:
        // 迭代下一个元素
        int next();
        // 将迭代器指向的TEValue返回
        // @return  0  成功
        const TEKey &get_key() const;
        TEValue *get_value();
        // 重置迭代器
        void reset();
      private:
        void set_(QueryEngine::keybtree_t *keybtree);
        QueryEngine::keybtree_t::TScanHandle &get_read_handle_();
      private:
        QueryEngine::keybtree_t *keybtree_;
        QueryEngine::keybtree_t::TScanHandle read_handle_;
        TEKey key_;
        TEValue *pvalue_;
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_QUERY_ENGINE_H_
