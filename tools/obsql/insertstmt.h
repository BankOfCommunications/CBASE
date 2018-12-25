#ifndef OCEANBASE_OBSQL_INSERTSTMT_H
#define OCEANBASE_OBSQL_INSERTSTMT_H

#include <stdint.h>
#include "stmt.h"
#include "common/ob_object.h"

namespace oceanbase
{
  namespace obsql
  {

    using namespace common;    
    using namespace std;

    class InsertStmt: public Stmt
    {
        public:
            struct valueNode
            {
                char                *raw_value_;
                ObObj               object_;
            };

            typedef struct valueNode     ValueNode;
            
        public:
            InsertStmt(char* buf, int32_t size, 
                        ObClientServerStub& rpc_stub,
                        map<string, vector<RowKeySubType> >& rowkey_map) 
                : Stmt(buf, size, rpc_stub, rowkey_map){}
            virtual ~InsertStmt(); 
            int32_t query() ;
            int32_t init(ObClientServerStub& rpc_stub);
            int32_t parse();
            int32_t resolve();
            int32_t execute();
            int32_t output();

        protected:
            int32_t parse_into(char *into_str);
            int32_t parse_values(char *value_str);
            int32_t parse_from(char *from_str);
            int32_t parse_where(char *where_str);
 
        private:
          vector<ValueNode> value_list_;
          int64_t           elapsed_time_;
    };

  }
}

#endif //OCEANBASE_OBSQL_INSERTSTMT_H



