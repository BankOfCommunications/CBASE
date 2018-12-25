/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_table_id_name.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_TABLE_ID_NAME_H
#define _OB_TABLE_ID_NAME_H

#include "common/ob_string.h"
#include "common/nb_accessor/ob_nb_accessor.h"
#include "common/ob_array.h"//add zhaoqiong [Schema Manager] 20150327

namespace oceanbase
{
  namespace common
  {
    struct ObTableIdName
    {
      ObString table_name_;
      ObString dbname_;//add dolphin [database manager]@20150613
      uint64_t table_id_;

      ObTableIdName() : table_id_(0) { }


    };

    /* 获得系统所有表单名字和对应的表单id */
    /// @note not thread-safe
    class ObTableIdNameIterator
    {
      public:
        ObTableIdNameIterator();
        virtual ~ObTableIdNameIterator();
        //mod zhaoqiong [Schema Manager] 20150420:b
        //int init(ObScanHelper* client_proxy, bool only_core_tables);
        int init(ObScanHelper* client_proxy, bool only_core_tables, int64_t base_version = 0, int64_t end_version = 0);
        //mod:e
        //add zhaoqiong [Schema Manager] 20150327:b
        /**
         * @brief get drop table and create table info from __all_ddl_operation
         */
        int get_table_list(ObArray<int64_t> & create_table_id, ObArray<int64_t> & drop_table_id, bool &refresh_schema);
        /**
         * @brief get latest_schema_version from __all_ddl_operation
         */
        int get_latest_schema_version(int64_t& latest_schema_version);
        //add:e
        virtual int next();

        /* 获得内部table_info的指针 */
        virtual int get(ObTableIdName** table_info);

        /* 释放内存 */
        void destroy();

      private:
        bool check_inner_stat();
        int normal_get(ObTableIdName** table_info);
        int internal_get(ObTableIdName** table_info);
      protected:
        bool need_scan_;
        bool only_core_tables_;
        int32_t table_idx_;
        nb_accessor::ObNbAccessor nb_accessor_;
        ObScanHelper* client_proxy_;
        nb_accessor::QueryRes* res_;
        ObTableIdName table_id_name_;
        //add zhaoqiong [Schema Manager] 20150327:b
        //for query __all_ddl_operation
        int64_t base_version_;
        int64_t end_version_;
        nb_accessor::QueryRes* ddl_res_;//save ddl_operation result
        //add:e
    };
  }
}

#endif /* _OB_TABLE_ID_NAME_H */


