/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_alter_sys_cnf_stmt.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */

#ifndef OCEANBASE_SQL_OB_ALTER_SYS_CNF_STMT_H_
#define OCEANBASE_SQL_OB_ALTER_SYS_CNF_STMT_H_
#include "ob_basic_stmt.h"
#include "common/ob_string.h"
#include "common/ob_object.h"
#include "common/hash/ob_hashmap.h"
#include "parse_node.h"

namespace oceanbase
{
  namespace sql
  {
    enum ObConfigType
    {
      MEMORY,
      SPFILE,
      BOTH,
      NONETYPE
    };
    struct ObSysCnfItemKey
    {
      ObSysCnfItemKey()
      {
        server_type_ = common::OB_INVALID;
        cluster_id_ = common::OB_INVALID_ID;
        server_port_ = common::OB_INVALID_ID;
      }
      bool operator==(const ObSysCnfItemKey& other) const;
      int64_t hash() const;
      // int64_t        cluster_role; // == 0
      // int64_t        server_role; // == 0
      uint64_t          cluster_id_;
      common::ObString  param_name_;
      common::ObRole    server_type_;
      common::ObString  server_ip_;
      uint64_t          server_port_;
    };
    struct ObSysCnfItem
    {
      ObSysCnfItem()
      {
        param_value_.set_type(common::ObMinType);
        config_type_ = NONETYPE;
        server_type_ = common::OB_INVALID;
        cluster_id_ = common::OB_INVALID_ID;
        server_port_ = common::OB_INVALID_ID;
      }
      int64_t to_string(char* buf, const int64_t buf_len) const;

      common::ObString  param_name_;
      common::ObObj     param_value_;
      common::ObString  comment_;
      ObConfigType      config_type_;
      common::ObRole    server_type_;
      uint64_t          cluster_id_;
      common::ObString  server_ip_;
      uint64_t          server_port_;
    };
    
    class ObAlterSysCnfStmt : public ObBasicStmt
    {
    public:
      ObAlterSysCnfStmt();
      virtual ~ObAlterSysCnfStmt();

      int init();
      int add_sys_cnf_item(ResultPlan& result_plan, const ObSysCnfItem& sys_cnf_item);
      common::hash::ObHashMap<ObSysCnfItemKey, ObSysCnfItem>::iterator sys_cnf_begin();
      common::hash::ObHashMap<ObSysCnfItemKey, ObSysCnfItem>::iterator sys_cnf_end();
      void print(FILE* fp, int32_t level, int32_t index = 0);
    private:
      // disallow copy
      ObAlterSysCnfStmt(const ObAlterSysCnfStmt &other);
      ObAlterSysCnfStmt& operator=(const ObAlterSysCnfStmt &other);
    private:
      common::hash::ObHashMap<ObSysCnfItemKey, ObSysCnfItem> sys_cnf_map_;
    };
    inline common::hash::ObHashMap<ObSysCnfItemKey, ObSysCnfItem>::iterator 
        ObAlterSysCnfStmt::sys_cnf_begin()
    {
      return sys_cnf_map_.begin();
    }
    inline common::hash::ObHashMap<ObSysCnfItemKey, ObSysCnfItem>::iterator 
      ObAlterSysCnfStmt::sys_cnf_end()
    {
      return sys_cnf_map_.end();
    }
  }
}

#endif //OCEANBASE_SQL_OB_ALTER_SYS_CNF_STMT_H_


