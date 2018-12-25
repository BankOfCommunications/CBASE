/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_show_schema_manager.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_ENSURE_SHOW_SCHEMAS_H_
#define OCEANBASE_SQL_OB_ENSURE_SHOW_SCHEMAS_H_
#include "sql/ob_basic_stmt.h"
#include "common/ob_schema.h"

namespace oceanbase
{
  namespace sql
  {
    class ObShowSchemaManager
    {
      public:
        static const common::ObSchemaManagerV2* get_show_schema_manager();

      private:
        // disallow
        ObShowSchemaManager() {}
        ObShowSchemaManager(const ObShowSchemaManager &other);
        ObShowSchemaManager& operator=(const ObShowSchemaManager &other);

        static int add_show_schemas(common::ObSchemaManagerV2& schema_mgr);
        static int add_show_schema(common::ObSchemaManagerV2& schema_mgr, int32_t stmt_type);
        static int add_show_tables_schema(common::ObSchemaManagerV2& schema_mgr);
        static int add_show_databases_schema(common::ObSchemaManagerV2& schema_mgr);// add by zhangcd [multi_database.show_databases] 20150617
        /* add liumengzhan_show_index [20141208]
         * add virtual table __index_show's schema to schema_mgr
         */
        static int add_show_index_schema(common::ObSchemaManagerV2& schema_mgr);
        //add:e
        static int add_show_variables_schema(common::ObSchemaManagerV2& schema_mgr);
        static int add_show_columns_schema(common::ObSchemaManagerV2& schema_mgr);
        static int add_show_create_table_schema(common::ObSchemaManagerV2& schema_mgr);
        static int add_show_parameters_schema(common::ObSchemaManagerV2& schema_mgr);
        static int add_show_table_status_schema(common::ObSchemaManagerV2& schema_mgr);

      private:        
        static common::ObSchemaManagerV2 *show_schema_mgr_;
        static tbsys::CThreadMutex mutex_;
    };
  }
}
#endif /* OCEANBASE_SQL_OB_ENSURE_SHOW_SCHEMAS_H_ */

