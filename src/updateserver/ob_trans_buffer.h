////===================================================================
 //
 // ob_trans_buffer.h / hash / common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2010-09-16 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 // 根据memtable特性 线程不安全 只能单线程使用
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_UPDATESERVER_TRANS_BUFFER_H_
#define  OCEANBASE_UPDATESERVER_TRANS_BUFFER_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_define.h"
#include "common/hash/ob_hashmap.h"
#include "ob_table_engine.h"

namespace oceanbase
{
  namespace updateserver
  {
    class TransBuffer
    {
      static const int64_t DEFAULT_SIZE = 1024;
      typedef common::hash::ObHashMap<TEKey, TEValue> hashmap_t;
      public:
        typedef common::hash::ObHashMap<TEKey, TEValue>::const_iterator iterator;
      public:
        TransBuffer();
        ~TransBuffer();
      public:
        int push(const TEKey &key, const TEValue &value);
        int get(const TEKey &key, TEValue &value) const;
        // 注意: 开始一次mutation 上次的mutation自动被提交
        int start_mutation();
        int end_mutation(bool rollback);
        int clear();
      public:
        iterator begin() const;
        iterator end() const;
      private:
        int commit_();
      private:
        hashmap_t trans_map_;
        hashmap_t patch_map_;
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_TRANS_BUFFER_H_

