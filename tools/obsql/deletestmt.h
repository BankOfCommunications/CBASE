#ifndef OCEANBASE_OBSQL_DELETESTMT_H
#define OCEANBASE_OBSQL_DELETESTMT_H

#include <stdint.h>
#include "stmt.h"

namespace oceanbase
{
  namespace obsql
  {

    using namespace common;    
    using namespace std;

    class DeleteStmt: public Stmt
    {
        public:
            DeleteStmt(char* buf, int32_t size, 
                         ObClientServerStub& rpc_stub,
                         map<string, vector<RowKeySubType> >& rowkey_map) 
                : Stmt(buf, size, rpc_stub, rowkey_map){}
            virtual ~DeleteStmt() {}
            int32_t query() ;
            int32_t init(ObClientServerStub& rpc_stub);
            int32_t parse();
            int32_t resolve();
            int32_t execute();
            int32_t output();

        protected:
            int32_t parse_from(char *from_str);
            int32_t parse_where(char *where_str);
 
        private:
          int64_t elapsed_time_;

    };

  }
}

#endif //OCEANBASE_OBSQL_DELETESTMT_H

