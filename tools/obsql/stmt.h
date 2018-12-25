#ifndef OCEANBASE_OBSQL_STMT_H
#define OCEANBASE_OBSQL_STMT_H

#include <stdint.h>
#include <map>
#include "common/ob_server.h"
#include "common/ob_range.h"
#include "common/ob_schema.h"
#include "client_rpc.h"
#include "common_func.h"

namespace oceanbase
{
  namespace obsql
  {

    using namespace oceanbase::common;    
    using namespace std;

    class Stmt
    {
        public:
            enum
            {
                OB_ER_OP = 0, /* error operator */
                OB_EQ_OP = 1,
                OB_LT_OP,
                OB_GT_OP,
                OB_LE_OP,
                OB_GE_OP,                
            };
            
            struct tableNode
            {
                char     table_name_[OB_MAX_TABLE_NAME_LENGTH];
                char     table_alias_[OB_MAX_TABLE_NAME_LENGTH];
                uint64_t table_id_;
            };

            struct columnNode
            {
                char     column_name_[OB_MAX_COLUMN_NAME_LENGTH];
                char     column_alias_[OB_MAX_COLUMN_NAME_LENGTH];
                uint64_t column_id_;
                uint64_t table_id_;
                char     table_name_[OB_MAX_TABLE_NAME_LENGTH];
                int32_t  position_; /* It is an indicator of the position in select statment */
            };

            struct keyNode
            {
                int32_t        op_type_;
                vector<char *> sub_key_list_;
            };

            typedef struct keyNode KeyNode;

            /* 
                  * 1. We do not support brace in the condtion now 
                  * 2. We do not support none-Rowkey columns in expressions
                  */
            struct conditionNode
            {
                char            column_name_[OB_MAX_COLUMN_NAME_LENGTH];
                bool            is_equal_condition_;
                ObString        row_key_;
                ObString        end_row_key_;
                ObBorderFlag    border_flag_;
                vector<KeyNode> raw_keynode_list_;
            };

            typedef struct tableNode     TableNode;
            typedef struct columnNode    ColumnNode;
            typedef struct conditionNode ConditionNode;
        
        public:
            Stmt(char* buf, int32_t size, 
                  ObClientServerStub& rpc_stub, 
                  map<string, vector<RowKeySubType> >& rowkey_map);
            virtual ~Stmt();
            virtual int32_t query() ;
            virtual int32_t init(ObClientServerStub& rpc_stub);
            virtual int32_t parse() = 0;
            virtual int32_t resolve() = 0;
            virtual int32_t execute() = 0;
            virtual int32_t output() = 0;

            const ObString& get_raw_query_string () { return query_string_; }
            vector<TableNode>& get_table_list() { return table_list_; }
            vector<ColumnNode>& get_column_list() { return column_list_; }
            vector<ConditionNode>& get_condition_list() { return condition_list_; }
            ObSchemaManagerV2& get_schema_manager() { return schema_mgr_; }
            int32_t set_schema_manager(ObSchemaManagerV2 schema_mgr) 
            {
                schema_mgr_ = schema_mgr;
                return common::OB_SUCCESS;
            }
            ObClientServerStub& get_rpc_stub() { return rpc_stub_; }
            map<string, vector<RowKeySubType> >& get_rowkey_map() { return rowkey_map_; }
            int32_t get_decode_rowkey_string(const char *buf, const int32_t data_len, int64_t& pos,
                                             const char *table_name, char *& outbuf, const int64_t out_len);

        protected:
            virtual int32_t parse_from(char *from_str) = 0;
            virtual int32_t parse_where(char *where_str) = 0;
            int32_t resolve_tables();
            int32_t resolve_conditions();
            int32_t pase_rowkey_node(int32_t op, char *value, ConditionNode &cnd_node);

        private:
            int32_t add_table(char *tbl_str);
            int32_t add_table(TableNode& tbl_node);
            int32_t parse_and(char *and_str);
            int32_t parse_rowkey_node(int32_t op, char *value, ConditionNode &cnd_node);

        private:
            ObString              query_string_;
            vector<TableNode>     table_list_;
            vector<ColumnNode>    column_list_;
            /* if multiple columns and condition, change item Condition to vector<ConditionNode> */
            vector<ConditionNode> condition_list_;

            ObClientServerStub&   rpc_stub_;
            ObSchemaManagerV2     schema_mgr_;

            map<string, vector<RowKeySubType> >& rowkey_map_;
    };

  }
}



#endif //OCEANBASE_OBSQL_STMT_H

