/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_nb_accessor.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_NB_ACCESSOR_H
#define _OB_NB_ACCESSOR_H

#include "common/ob_malloc.h"
#include "common/ob_scanner.h"
#include "common/ob_scan_param.h"
#include "common/ob_get_param.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_object.h"
#include "common/ob_mod_define.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_rowkey.h"
#include "common/ob_simple_condition.h"
#include "common/ob_easy_array.h"
#include "common/ob_mutator_helper.h"
#include "common/roottable/ob_scan_helper.h"
#include "nb_scan_cond.h"
#include "nb_query_res.h"
#include "nb_table_row.h"



namespace oceanbase
{
namespace common
{
  namespace nb_accessor
  {
    class ObNbAccessor
    {
    public:
      ObNbAccessor();
      virtual ~ObNbAccessor();

      int init(ObScanHelper* client_proxy);
      int insert(const char* table_name, const ObRowkey& rowkey, const KV& kv);
      int update(const char* table_name, const ObRowkey& rowkey, const KV& kv);
      int delete_row(const char* table_name, const ObRowkey& rowkey);

      //按照条件删除给定表单中的列，目前不能保证一致性
      int delete_row(const char* table_name, const SC& rowkey_columns, const ScanConds& scan_conds);

      int scan(QueryRes*& res, const char* table_name, const ObNewRange& range, const SC& select_columns, const ScanConds& scan_conds);
      int scan(QueryRes*& res, const char* table_name, const ObNewRange& range, const SC& select_columns);
      int get(QueryRes*& res, const char* table_name, const ObRowkey& rowkey, const SC& select_columns);
      void release_query_res(QueryRes* res);

      //add hongchen [UNLIMIT_TABLE] 20161031:b
      int  get_next_scanner(QueryRes*& res);
      //add hongchen [UNLIMIT_TABLE] 20161031:e

      int update(const char* table_name, const KV& rowkey_kv, const KV& kv);
      int insert(const char* table_name, const KV& rowkey_kv, const KV& kv);
      int delete_row(const char* table_name, const KV& rowkey_kv);
      int get(QueryRes*& res, const char* table_name, const KV& rowkey_kv, const SC& select_columns);

      inline void set_is_read_consistency(bool is_read_consistency)
      {
        is_read_consistency_ = is_read_consistency;
      }

  public:
      struct BatchHandle
      {
        ObMutator* mutator_;
        ObMutatorHelper mutator_helper_;
        bool modified;

        BatchHandle() : mutator_(NULL), modified(false)
        {
        }
      };

      int batch_begin(BatchHandle& handle);
      int batch_update(BatchHandle& handle, const char* table_name, const ObRowkey& rowkey, const KV& kv);
      int batch_insert(BatchHandle& handle, const char* table_name, const ObRowkey& rowkey, const KV& kv);
      int batch_end(BatchHandle& handle);
      
    private:
      bool check_inner_stat();

      ObScanHelper* client_proxy_;
      bool is_read_consistency_;
    };
  }
}
}

#endif /* _OB_NB_ACCESSOR_H */


