/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_mutator_helper.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_MUTATOR_HELPER_H
#define _OB_MUTATOR_HELPER_H

#include "common/ob_mutator.h"
#include "common/ob_easy_array.h"

namespace oceanbase
{
  namespace common
  {
    struct kv_pair
    {
      const char* key_;
      ObObj obj_;

      kv_pair();
      kv_pair(const char* key, ObString val);
      kv_pair(const char* key, int64_t val);
    };

    class KV : public EasyArray<kv_pair>
    {
    public:
      KV(const char* key, ObString val);
      KV(const char* key, int64_t val);

      KV& operator()(const char* key, ObString val);
      KV& operator()(const char* key, int64_t val);
    };

    int kv_to_rowkey(const KV& kv, ObRowkey& rowkey, ObObj* rowkey_obj, int32_t obj_buf_count);

    class ObMutator;
    class ObMutatorHelper
    {
    public:
      ObMutatorHelper();

      int init(ObMutator* mutator);
      int insert(const char* table_name, const ObRowkey& rowkey, const KV& kv);
      int update(const char* table_name, const ObRowkey& rowkey, const KV& kv);
      int del_row(const char* table_name, const ObRowkey& rowkey);

      int insert(const char* table_name, const KV& rowkey_kv, const KV& kv);
      int update(const char* table_name, const KV& rowkey_kv, const KV& kv);
      int del_row(const char* table_name, const KV& rowkey_kv);
    private:
      bool check_inner_stat() const;
      int add_column(const char* table_name, const ObRowkey& rowkey, const char* column_name, const ObObj& value, ObMutator* mutator);

    private:
      ObMutator* mutator_;
    };
  }
}

#endif /* _OB_MUTATOR_HELPER_H */


