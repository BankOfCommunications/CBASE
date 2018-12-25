#ifndef OCEANBASE_OBSQL_SELECTSTMT_H
#define OCEANBASE_OBSQL_SELECTSTMT_H

#include <stdint.h>
#include "stmt.h"
#include "common/ob_server.h"
#include "common/ob_range.h"
#include "common/ob_scan_param.h"

namespace oceanbase
{
  namespace obsql
  {

    using namespace oceanbase::common;  
    using namespace std;

    class SelectStmt: public Stmt
    {
        public:
            struct orderNode
            {
                char                column_name_[OB_MAX_COLUMN_NAME_LENGTH];
                uint64_t            column_id_;
                int32_t             rel_position_; /* relative position in select statement */
                uint64_t            table_id_;
                char                table_name_[OB_MAX_TABLE_NAME_LENGTH];
                ObScanParam::Order  order;
            };

            typedef struct orderNode     OrderNode;
        
        public:
            SelectStmt(char* buf, int32_t size, 
                         ObClientServerStub& rpc_stub, 
                         map<string, vector<RowKeySubType> >& rowkey_map) 
                : Stmt(buf, size, rpc_stub, rowkey_map){}
            virtual ~SelectStmt() {}
            int32_t query() ;
            int32_t init(ObClientServerStub& rpc_stub);
            int32_t parse();
            int32_t resolve();
            int32_t execute();
            int32_t output();

        protected:
            int32_t parse_select(char *select_str);
            int32_t parse_from(char *from_str);
            int32_t parse_where(char *where_str);
            int32_t parse_order(char *order_str);
            int32_t resolve_select();
            int32_t resolve_order();
            int32_t add_column(char *column_str);
            int32_t add_column(ColumnNode& column_node);
 
        private:
            char* get_original_tblname(const char *alias);
            int32_t expand_star(vector<ColumnNode>::iterator& it, uint64_t table_id);
            int32_t output_head();
            int32_t output_body();

        private:
            vector<OrderNode> order_list_;
            ObScanner         scanner_;
            int64_t           elapsed_time_;
    };

  }
}



#endif //OCEANBASE_OBSQL_SELECTSTMT_H


