/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_schema_checker.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_SCHEMA_CHECKER_H_
#define OCEANBASE_SQL_OB_SCHEMA_CHECKER_H_
#include "common/ob_schema.h"
#include "common/ob_string.h"
#include "sql/ob_stmt.h"

namespace oceanbase
{
  namespace sql
  {
    class ObSchemaChecker
    {
      public:
        ObSchemaChecker();
        // explicit ObSchemaChecker(const common::ObSchemaManagerV2& schema_mgr);
        virtual ~ObSchemaChecker();

        //add dragon [varchar limit] 2016-8-10 09:41:56
        /**
         * @brief varchar_len_check 对cid列进行varchar长度检测
         * @param cinfo 表id以及列id
         * @param length 变量的长度
         * @return 符合schema中长度要求，返回成功；否则，返回失败
         */
        int varchar_len_check(ObColumnInfo &cinfo, int64_t length);
        //add e

        void set_schema(const common::ObSchemaManagerV2& schema_mgr);
        bool column_exists(
            const common::ObString& table_name,
            const common::ObString& column_name) const;
        bool is_join_column(
            uint64_t table_id,
            uint64_t column_id) const;
        uint64_t get_column_id(
            const common::ObString& table_name, 
            const common::ObString& column_name) const;
        uint64_t get_table_id(const common::ObString& table_name) const;
        uint64_t get_local_table_id(const common::ObString& table_name) const;
        const common::ObTableSchema* get_table_schema(const char* table_name) const;
        const common::ObTableSchema* get_table_schema(const common::ObString& table_name) const;
        const common::ObTableSchema* get_table_schema(const uint64_t table_id) const;
        const common::ObColumnSchemaV2* get_column_schema(
            const common::ObString& table_name, 
            const common::ObString& column_name) const;
        const common::ObColumnSchemaV2* get_column_schema(
            const uint64_t table_id, 
            const uint64_t column_id) const;
        const common::ObColumnSchemaV2* get_table_columns(
            const uint64_t table_id,
            int32_t& size) const;
        bool is_rowkey_column(
            const common::ObString& table_name,
            const common::ObString& column_name) const;
        //add wenghaixing [secondary index] 20141105
        int is_index_full(uint64_t table_id,bool& is_full);
        //add e

      private:
        // disallow copy
        ObSchemaChecker(const ObSchemaChecker &other);
        ObSchemaChecker& operator=(const ObSchemaChecker &other);
      private:
        const common::ObSchemaManagerV2 *schema_mgr_;
        const common::ObSchemaManagerV2 *show_schema_mgr_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_SCHEMA_CHECKER_H_ */

