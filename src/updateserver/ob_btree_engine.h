////===================================================================
 //
 // ob_btree_engine.h / hash / common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2010-09-15 by Yubai (yubai.lk@taobao.com) 
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

#ifndef  OCEANBASE_UPDATESERVER_BTREE_ENGINE_H_
#define  OCEANBASE_UPDATESERVER_BTREE_ENGINE_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_define.h"
#include "common/btree/key_btree.h"
#include "common/page_arena.h"
#include "ob_table_engine.h"
#include "ob_memtank.h"
#include "ob_btree_engine_alloc.h"

namespace oceanbase
{
  namespace updateserver
  {
    class BtreeEngineIterator;
    class BtreeEngineTransHandle
    {
      friend class BtreeEngine;
      public:
        BtreeEngineTransHandle() : trans_type_(INVALID_TRANSACTION),
                                   read_handle_(),
                                   write_handle_(), cur_write_handle_ptr_(NULL)
        {
        };
      private:
        // do not impl
        BtreeEngineTransHandle(const BtreeEngineTransHandle &other);
        BtreeEngineTransHandle &operator =(const BtreeEngineTransHandle &other);
        void reset()
        {
          trans_type_ = INVALID_TRANSACTION;
          cur_write_handle_ptr_ = NULL;
          new(&read_handle_) common::BtreeReadHandle;
          new(&write_handle_) common::BtreeWriteHandle;
        };
      private:
        TETransType trans_type_;
        common::BtreeReadHandle read_handle_;
        common::BtreeWriteHandle write_handle_;
        common::BtreeWriteHandle *cur_write_handle_ptr_;
    };

    class BtreeEngine
    {
      friend class BtreeEngineIterator;
      typedef common::KeyBtree<TEKey, TEValue*> keybtree_t;
      typedef common::PageArena<char> allocer_t;
      public:
        BtreeEngine();
        ~BtreeEngine();
      public:
        int init(MemTank *allocer);
        int destroy();
      public:
        // 插入key-value对，如果已存在则覆盖
        // @param [in] key 待插入的key
        // @param [in] value 待插入的value
        int set(BtreeEngineTransHandle &handle,
                const TEKey &key,
                const TEValue &value);
        int set(const TEKey &key, const TEValue &value);
    
        // 获取指定key的value
        // @param [in] key 要查询的key
        // @param [out] value 查询返回的value
        int get(BtreeEngineTransHandle &handle,
                const TEKey &key,
                TEValue &value);
        int get(const TEKey &key, TEValue &value);
        
        // 范围查询，返回一个iterator
        // @param [in] 查询范围的start key
        // @param [in] 是否由min key开始查询
        // @param [in] 查询范围是否包含start key本身, 0为包含, 非0为不包含; 在hash实现时必须为0
        // @param [in] 查询范围的end key
        // @param [in] 是否由max key开始查询
        // @param [in] 查询范围是否包含end key本身, 0为包含, 非0为不包含; 在hash实现时必须为0
        // @param [out] iter 查询结果的迭代器
        int scan(BtreeEngineTransHandle &handle,
                const TEKey &start_key,
                const int min_key,
                const int start_exclude,
                const TEKey &end_key,
                const int max_key,
                const int end_exclude,
                const bool reverse,
                BtreeEngineIterator &iter);
        
        // 开始一次事务
        // 在btree实现时使用copy on write技术; 在hash实现时什么都不做 因此不保证事务
        // @param [in] type 事务类型, 读事务 or 写事务
        // @param [out] 事务handle
        int start_transaction(const TETransType type,
                              BtreeEngineTransHandle &handle);
        
        // 结束一次事务 对于写事务表示事务的提交
        // @param [in] 事务handle
        // @param [in] rollback 是否回滚事务
        int end_transaction(BtreeEngineTransHandle &handle, bool rollback);

        // 开始一次事务性的更新
        // 一次mutation结束之前不能开启新的mutation
        int start_mutation(BtreeEngineTransHandle &handle);

        // 结束一次mutation
        // @param[in] rollback 是否回滚
        int end_mutation(BtreeEngineTransHandle &handle, bool rollback);
        
        // 建立索引
        // 在btree实现时什么都不做
        int create_index();

        int clear();

        void dump2text(const char *fname);

        int64_t size();
      private:
        bool inited_;
        UpsBtreeEngineAlloc key_alloc_;
        UpsBtreeEngineAlloc node_alloc_;
        keybtree_t keybtree_;
        MemTank *allocer_;

        bool transaction_started_;
        bool mutation_started_;
    };

    class BtreeEngineIterator
    {
      friend class BtreeEngine;
      public:
        BtreeEngineIterator();
        ~BtreeEngineIterator();
      public:
        // 迭代下一个元素
        int next();
        // 将迭代器指向的TEValue返回
        // @return  0  成功
        int get_key(TEKey &key) const;
        int get_value(TEValue &value) const;
        // 重置迭代器
        void reset();
      private:
        void set_(BtreeEngine::keybtree_t *keybtree, common::BtreeReadHandle *read_handle_ptr);
      private:
        BtreeEngine::keybtree_t *keybtree_;
        common::BtreeReadHandle *read_handle_ptr_;
        TEKey key_;
        TEValue *pvalue_;
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_BTREE_ENGINE_H_

