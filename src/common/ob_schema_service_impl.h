/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_schema_service_impl.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_SCHEMA_SERVICE_IMPL_H
#define _OB_SCHEMA_SERVICE_IMPL_H

#include "common/ob_table_id_name.h"
#include "common/ob_schema_service.h"
#include "common/hash/ob_hashmap.h"

class TestSchemaService_assemble_table_Test;
class TestSchemaTable_generate_new_table_name_Test;
class TestSchemaService_assemble_column_Test;
class TestSchemaService_assemble_join_info_Test;
class TestSchemaService_create_table_mutator_Test;
class TestSchemaService_get_table_name_Test;
#define OB_STR(str) \
  ObString(0, static_cast<int32_t>(strlen(str)), const_cast<char *>(str))

namespace oceanbase
{
  namespace common
  {
    static ObString first_tablet_entry_name = OB_STR(FIRST_TABLET_TABLE_NAME);
    static ObString column_table_name = OB_STR(OB_ALL_COLUMN_TABLE_NAME);
    static ObString joininfo_table_name = OB_STR(OB_ALL_JOININFO_TABLE_NAME);
    //add zhaoqiong [Schema Manager] 20150327:b
    static ObString ddl_operation_name = OB_STR(OB_ALL_DDL_OPERATION_NAME);
    //add:e
    static ObString privilege_table_name = OB_STR(OB_ALL_TABLE_PRIVILEGE_TABLE_NAME);
    static ObString table_name_str = OB_STR("table_id");
    static const char* const TMP_PREFIX = "tmp_";
    //add wenghaixing [secondary index] 20141029
    //Õâ¸ö½Ó¿ÚÔÝÊ±·ÏÆú
    static ObString index_process_name=OB_STR(OB_INDEX_PROCESS_TABLE_NAME);
    //add e
    class ObSchemaServiceImpl : public ObSchemaService
    {
      public:
        ObSchemaServiceImpl();
        virtual ~ObSchemaServiceImpl();

        int init(ObScanHelper* client_proxy, bool only_core_tables);
        //add zhaoqiong [Schema Manager] 20150327:b
        virtual int64_t get_schema_version() const;
        /**
         * @brief for slave rs, make rs and service have same timestamp
         *
         * @param timestamp, from rs timestamp
         * @return
         */
        virtual int set_schema_version(int64_t timestamp);
        //add:e

        virtual int get_table_schema(const ObString& table_name, TableSchema& table_schema,const ObString& dbname = ObString::make_string(""));
        virtual int create_table(const TableSchema& table_schema);
        virtual int drop_table(const ObString& table_name);
        virtual int alter_table(const AlterTableSchema& table_schema, const int64_t old_schema_version);
		//add fyd [NotNULL_check] [JHOBv0.1] 20140108:b
		virtual int alter_table(const AlterTableSchema& table_schema, TableSchema& old_table_schema);
		//add 20140108:e
        virtual int get_table_id(const ObString& table_name, uint64_t& table_id);
        virtual int get_table_name(uint64_t table_id, ObString& table_name);
        virtual int modify_table_id(TableSchema& table_schema, const int64_t new_table_id);
        virtual int get_max_used_table_id(uint64_t &max_used_tid);
        virtual int set_max_used_table_id(const uint64_t max_used_tid);
        virtual int prepare_privilege_for_table(const nb_accessor::TableRow* table_row,
            ObMutator *mutator, const int64_t table_id);
        //add zhaoqiong [Schema Manager] 20150327:b
        virtual int refresh_schema();
        /**
         * @brief fetch schema_mutator from system table
         * @param start_version: local schema timestamp
         * @param end_version: new schema timestamp
         * @param schema_mutator [out]
         */
        virtual int fetch_schema_mutator(const int64_t start_version, const int64_t end_version, ObSchemaMutator& schema_mutator);
        //need set schema version in call fun
        virtual int get_schema(bool only_core_tables, ObSchemaManagerV2& out_schema);

        /**
         * @brief append a ddl operation record to __all_ddl_operation
         */
        virtual int add_ddl_operation(ObMutator* mutator, const uint64_t& table_id, const DdlType ddl_type);
        //add:e
        //add wenghaixing [secondary index col checksum] 20141208
        //modify liuxiao [muti database] 20150702
        virtual int modify_index_stat(ObString index_table_name,uint64_t index_table_id,ObString db_name,int stat);
        //modify:e
        //add e
        //add liuxiao [secondary index col checksum] 20150316
        virtual int clean_checksum_info(const int64_t max_draution_of_version,const int64_t current_version);
        virtual int check_column_checksum(const int64_t orginal_table_id,const int64_t index_table_id, const int64_t cluster_id, const int64_t current_version, bool &is_right);
        virtual int prepare_checksum_info_row(const uint64_t table_id ,const ObRowkey& rowkey,const ObObj &column_check_sum ,ObMutator *mutator);
        virtual int get_checksum_info(const ObNewRange new_range,const int64_t cluster_id,const int64_t required_version,ObString& cchecksum);
        //add e
        //add liumz, [secondary index static_index_build] 20150629:b
        virtual int get_cluster_count(int64_t &cc);
        virtual int get_index_stat(const uint64_t table_id, const int64_t cluster_count, IndexStatus &stat);
        virtual int fetch_index_stat(const uint64_t table_id, const int64_t cluster_id, int64_t &stat);
        //add:e

        //add jinty [Paxos Cluster.Balance] 20160708:b
        virtual int get_all_server_status(char *buf, ObArray<oceanbase::common::ObString> &typeArray, ObArray<int32_t> &inner_port_Array, ObArray<ObServer> &servers_ip_with_port, ObArray<int32_t> &cluster_id_array,ObArray<int32_t> & svr_role_array);
        //add e


        friend class ::TestSchemaService_assemble_table_Test;
        friend class ::TestSchemaService_assemble_column_Test;
        friend class ::TestSchemaService_assemble_join_info_Test;
        friend class ::TestSchemaService_create_table_mutator_Test;
        friend class ::TestSchemaService_get_table_name_Test;

      private:
        bool check_inner_stat();
        // for read
        int fetch_table_schema(const ObString& table_name, TableSchema& table_schema,const ObString& dbname);
         //add wenghaixing [secondary index col checksum] 20141208
        //modify liuxiao [muti database] 20150702
        //int create_modify_status_mutator(ObString index_table_name, int status,ObMutator* mutator);
        int create_modify_status_mutator(ObString index_table_name,ObString db_name, int status,ObMutator* mutator);
        //modify e
        //add e
        int create_table_mutator(const TableSchema& table_schema, ObMutator* mutator);
        int alter_table_mutator(const AlterTableSchema& table_schema, ObMutator* mutator, const int64_t old_schema_version);
		//add fyd [NotNULL_check] [JHOBv0.1] 20140108:b
        int alter_table_mutator(const AlterTableSchema& table_schema, ObMutator* mutator, TableSchema& old_table_schema);
		//add 20140108:e
        int assemble_table(const nb_accessor::TableRow* table_row, TableSchema& table_schema);
        int assemble_column(const nb_accessor::TableRow* table_row, ColumnSchema& column);
        int assemble_join_info(const nb_accessor::TableRow* table_row, JoinInfo& join_info);
        int init_id_name_map(ObTableIdNameIterator& iterator);
        // for update
        int add_join_info(ObMutator* mutator, const TableSchema& table_schema);
        int add_column(ObMutator* mutator, const TableSchema& table_schema);
        int update_column_mutator(ObMutator* mutator, ObRowkey & rowkey, const ColumnSchema & column);
        int rename_column_mutator(ObMutator* mutator, ObRowkey & rowkey, ColumnSchema & column);//add liuj [Alter_Rename] [JHOBv0.1] 20150104
        int rename_table_mutator(ObMutator* mutator, const AlterTableSchema &alter_schema,const TableSchema & old_schema);//add liuj [Alter_Rename] [JHOBv0.1] 20150104
        int reset_column_id_mutator(ObMutator* mutator, const AlterTableSchema & schema, const uint64_t max_column_id);
        int reset_schema_version_mutator(ObMutator* mutator, const AlterTableSchema & schema, const int64_t old_schema_version);
        int init_id_name_map();
        //add wenghaixing [secondary index]20141029
        //ÔÝÊ±²»ÓÃµÄ½è¿Ú
        int assemble_index_process(const nb_accessor::TableRow* table_row,IndexHelper& ih,uint64_t& IndexList);//modify wenghaixing [secondary index]20141104
        //add e
      int generate_new_table_name(char* buf, const uint64_t lenght, const char* table_name, const uint64_t table_name_length);
		//add fyd [NotNULL_check] [JHOBv0.1] 20140108:b
		int renew_null_column_mutator(ObMutator* mutator, ObRowkey & rowkey, const ColumnSchema & column);
		int add_single_column(ObMutator* mutator,  ColumnSchema* column,ObRowkey & rowkey);
		//add 20140108:e
         //add wenghaixing [secondary index] 20141029
         //ÔÝÊ±²»ÓÃµÄ½Ó¿Ú
         int add_index_process(ObMutator* mutator, const TableSchema& table_schema);
         //add e

      private:
        static const int64_t TEMP_VALUE_BUFFER_LEN = 32;
        ObScanHelper* client_proxy_;
        nb_accessor::ObNbAccessor nb_accessor_;
        hash::ObHashMap<uint64_t, ObString> id_name_map_;
        ObStringBuf string_buf_;
        tbsys::CThreadMutex string_buf_write_mutex_;
        tbsys::CThreadMutex mutex_;
        //add wenghaixing [secondary index col checksum] 20141217
        tbsys::CThreadMutex cc_mutex_;
        //add e
        bool is_id_name_map_inited_;
        bool only_core_tables_;
        int64_t schema_timestamp_;//add zhaoqiong [Schema Manager] 20150327
    };
  }
}

#endif /* _OB_SCHEMA_SERVICE_IMPL_H */
