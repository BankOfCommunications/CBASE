/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_extra_tables_schema.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *   Zhidong SUN  <xielun.szd@alipay.com>
 *
 */
#ifndef _OB_EXTRA_TABLES_SCHEMA_H
#define _OB_EXTRA_TABLES_SCHEMA_H 1

#include "ob_schema_service_impl.h"

namespace oceanbase
{
  namespace common
  {
    /////////////////////////////////////////////////////////
    // WARNING: the sys table schema can not modified      //
    /////////////////////////////////////////////////////////
    class ObExtraTablesSchema
    {
    public:
      static const int64_t TEMP_ROWKEY_LENGTH = 64;
      static const int64_t SERVER_TYPE_LENGTH = 16;
      static const int64_t SERVER_IP_LENGTH = 32;
      // core tables
      static int first_tablet_entry_schema(TableSchema& table_schema);
      static int all_all_column_schema(TableSchema& table_schema);
      static int all_join_info_schema(TableSchema& table_schema);
    public:
      // other sys tables
      static int all_sys_stat_schema(TableSchema &table_schema);
      static int all_sys_param_schema(TableSchema &table_schema);
      static int all_sys_config_schema(TableSchema &table_schema);
      static int all_sys_config_stat_schema(TableSchema &table_schema);
      static int all_user_schema(TableSchema &table_schema);
      static int all_table_privilege_schema(TableSchema &table_schema);
      static int all_trigger_event_schema(TableSchema& table_schema);
      static int all_cluster_schema(TableSchema& table_schema);
      static int all_server_schema(TableSchema& table_schema);
      static int all_client_schema(TableSchema& table_schema);
      //add zhaoqiong [Schema Manager] 20150327:b
      static int all_ddl_operation(TableSchema& table_schema);
      //add:e
      //add dolphin [database manager]@20150605
      static int all_database_schema(TableSchema& schema);
      static int all_database_privilege_schema(TableSchema& schema);
      //add:e
      // virtual sys tables
      static int all_server_stat_schema(TableSchema &table_schema);
      static int all_server_session_schema(TableSchema &table_schema);
      static int all_statement_schema(TableSchema &table_schema);
      //add wenghaixing [secondary index.cluster]20150629
      //重新启用这个接口，用来解决主备集群创建索引不同步的问题
      static int all_index_process_schema(TableSchema &table_schema);
      //add e
      //add liuxiao [secondary index col checksum] 20150316
      //设置系统表__all_cchecksum_info的表结构
      static int all_cchecksum_info(TableSchema & table_schema);

      static int all_truncate_op_info(TableSchema &table_schema); //add zhaoqiong [Truncate Table]:20160318
      //add e
	  //add lijianqiang [sequence] 20150324b:
      /*Exp:??????,??sequence???,???sequence???*/
      static int all_sequence_schema(TableSchema &table_schema);
      //add 20150324:e
    private:
      ObExtraTablesSchema();
    };
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_EXTRA_TABLES_SCHEMA_H */
