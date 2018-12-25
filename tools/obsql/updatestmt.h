#ifndef OCEANBASE_OBSQL_UPDATESTMT_H
#define OCEANBASE_OBSQL_UPDATESTMT_H

#include <stdint.h>
#include "stmt.h"
#include "common/ob_object.h"

namespace oceanbase
{
  namespace obsql
  {

    using namespace common;    
    using namespace std;

    class UpdateStmt: public Stmt
    {
        public:
            struct setNode
            {
                char                column_name_[OB_MAX_COLUMN_NAME_LENGTH];
                uint64_t            column_id_;
                char                *raw_value_;
                ObObj               object_;
            };

            typedef struct setNode     SetNode;
            
        public:
            UpdateStmt(char* buf, int32_t size, 
                          ObClientServerStub& rpc_stub,
                          map<string, vector<RowKeySubType> >& rowkey_map) 
                : Stmt(buf, size, rpc_stub, rowkey_map){}
            virtual ~UpdateStmt();
            int32_t query() ;
            int32_t init(ObClientServerStub& rpc_stub);
            int32_t parse();
            int32_t resolve();
            int32_t execute();
            int32_t output();

        protected:
            int32_t parse_set(char *set_str);
            int32_t parse_from(char *from_str);
            int32_t parse_where(char *where_str);
 
        private:
          vector<SetNode> set_list_;
          int64_t         elapsed_time_;
    };

  }
}

#endif //OCEANBASE_OBSQL_UPDATESTMT_H


