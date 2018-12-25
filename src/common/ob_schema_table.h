/*===============================================================
*   (C) 2007-2011 Taobao Inc.
*   
*   
*   Version: 0.1 2011-12-23
*   
*   Authors:
*     jianming.cjq<jianming.cjq@taobao.com>
*   
*
================================================================*/

#ifndef OCEANBASE_COMMON_OB_SCHEMA_TABLE_H_
#define OCEANBASE_COMMON_OB_SCHEMA_TABLE_H_

#include "common/ob_mutator.h"
#include "common/ob_schema.h"
#include "common/ob_server.h"
#include "common/ob_client_helper.h"

namespace oceanbase
{
  namespace common
  {
    //映射表单信息到ob表单
    typedef void (*table_field)(const ObTableSchema& table, ObObj& value);
    
    struct TableMap
    {
      const char* column_name_;
      table_field func_;
      TableMap(const char* column_name, table_field func)
        :column_name_(column_name), func_(func) { }
    };
    
    void get_table_name(const ObTableSchema& table, ObObj& value);
    void get_table_id(const ObTableSchema& table, ObObj& value);
    void get_table_type(const ObTableSchema& table, ObObj& value);
    void get_rowkey_len(const ObTableSchema& table, ObObj& value);
    void get_rowkey_is_fixed_len(const ObTableSchema& table, ObObj& value);
    void get_rowkey_split(const ObTableSchema& table, ObObj& value);
    void get_compressor(const ObTableSchema& table, ObObj& value);
    void get_max_column_id(const ObTableSchema& table, ObObj& value);

    extern const TableMap TABLE_OR_MAP[8];
    
    //映射column信息到ob表单
    typedef void (*column_field)(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value);

    struct ColumnMap
    {
      const char* column_name_;
      column_field func_;
      ColumnMap(const char* column_name, column_field func)
        :column_name_(column_name), func_(func) { }
    };

    void get_table_name(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value);
    void get_table_id(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value);
    void get_column_name(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value);
    void get_column_id(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value);
    void get_data_type(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value);
    void get_data_length(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value);
    void get_column_group_id(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value);

    extern const ColumnMap COLUMN_OR_MAP[7];

    typedef column_field joininfo;
    typedef ColumnMap JoininfoMap;

    void get_start_pos(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value);
    void get_end_pos(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value);
    void get_right_table_name(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value);
    void get_right_table_id(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value);
    void get_right_column_name(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value);
    void get_right_column_id(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value);

    extern const JoininfoMap JOININFO_OR_MAP[10];
    
    int clean_table(const ObString& table_name, const ObString& select_column, common::ObClientHelper* client_helper, ObMutator* mutator);
    int dump_tables(const ObSchemaManagerV2& schema_mgr, common::ObClientHelper* client_helper, ObMutator* mutator);
    int dump_columns(const ObSchemaManagerV2& schema_mgr, common::ObClientHelper* client_helper, ObMutator* mutator);
    int dump_joininfo(const ObSchemaManagerV2& schema_mgr, common::ObClientHelper* client_helper, ObMutator* mutator);
    
    // 把内存中的schema_manager结构写入内部表单
    int dump_schema_manager(const ObSchemaManagerV2& schema_mgr, common::ObClientHelper* client_helper, const ObServer& update_server);
  }
}

#endif /* OCEANBASE_COMMON_OB_SCHEMA_TABLE_H_ */

