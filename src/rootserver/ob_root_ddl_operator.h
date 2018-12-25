/*
 * Copyright (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   zhidong <xielun.szd@taobao.com>
 *     - some work details here
 */

#ifndef _OB_ROOT_DDL_OPERATOR_H_
#define _OB_ROOT_DDL_OPERATOR_H_

#include "tbsys.h"
#include "common/ob_define.h"
#include "common/ob_schema_service.h"
//add wenghaixing [secondary index col checksum]20141217
#include "common/ob_string.h"
//add e
namespace oceanbase
{
  namespace common
  {
    class ObString;
    class TableSchema;
  }
  namespace rootserver
  {
    class ObRootServer2;
    class ObRootDDLOperator
    {
    public:
      ObRootDDLOperator();
      virtual ~ObRootDDLOperator();
    public:
      void init(ObRootServer2 * server, common::ObSchemaService * service);
      // create a new table
      int create_table(const common::TableSchema & table_schema);
      // delete a exist table
      int drop_table(const common::ObString & table_name);
      // alter table schema
      int alter_table(common::AlterTableSchema & table_schema);
      int modify_table_id(common::TableSchema &table_schema, const int64_t new_table_id);
      // update max used table id
      int update_max_table_id(const uint64_t table_id);
      //add wenghaixing [secondary index col checksum] 20141208
      //将校验和不对的索引表的状态改为不可用(ERROR)
      //modify liuxiao [muti database] 20150702
      //int modify_index_stat(common::ObString index_table_name,uint64_t index_table_id,int stat);
      int modify_index_stat(common::ObString index_table_name,uint64_t index_table_id,common::ObString db_name,int stat);
      //modify e
      //add e
    private:
      bool check_inner_stat(void) const;
      // read max table id and modify table schema
      int allocate_table_id(common::TableSchema & talbe_schema);
      // allocate table id and column ids
      int modify_table_schema(const uint64_t table_id, common::TableSchema & table_schema);
      // update schema table insert new table schema content
      bool insert_schema_table(const common::TableSchema & table_schema);
      // update schema table delete old table schema content
      bool delete_schema_table(const common::ObString & table_name, uint64_t & table_id);
      // delete tablet from root table
      int delete_root_table(const uint64_t table_id);
      // create empty tablet
      int create_empty_tablet(const common::TableSchema & table_schema);
      // alter table schema
      int alter_table_schema(const common::TableSchema & schema, common::AlterTableSchema & table_schema);
      typedef common::AlterTableSchema::AlterColumnSchema AlterColumn;
      // check whether the column can alter
      int check_alter_column(const common::TableSchema & schema, AlterColumn & column);
      // allocate column id
      int set_column_info(const common::TableSchema & schema, const char * column_name,
          uint64_t & max_column_id, AlterColumn & column);
      //  add dolphin [SchemaValidation] construct check table schema for schema vadidation @20150515:b
      /**
       * @param [in/out] table_schema the finial table schema to be constructed
       * @param [in] alter_schema used to construct the final schema
       * @version 1.0
       */
      int construct_check_table_schema(common::TableSchema & table_schema, const common::AlterTableSchema & alter_schema);

      /**
       *@param [in/out] table_schema the finial table schema to be constructed
       *@param [in]  alterColumn the processed every column schema of alter_schema
       * @version 1.0
      */
      int construct_alter_schema_column(common::TableSchema & table_schema,const common::AlterTableSchema::AlterColumnSchema & alterColumn);
      //  add 20150515:e
    private:
      // not support parallel ddl operation
      tbsys::CThreadMutex mutex_lock_;
      // schema inner table operator tool
      common::ObSchemaService * schema_client_;
      // select cs and create tablet tool
      // root table operator tool
      ObRootServer2 * root_server_;
    };
    //
    inline bool ObRootDDLOperator::check_inner_stat(void) const
    {
      return ((schema_client_ != NULL) && (root_server_ != NULL));
    }
  }
}



#endif // _OB_ROOT_DDL_OPERATOR_H_
