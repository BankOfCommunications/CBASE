/*
 *   (C) 2010-2012 Taobao Inc.
 *   
 *   Version: 0.1 $date
 *           
 *   Authors:
 *               
 */

#ifndef OCEANBASE_OBSQL_SCHEMA_PRT_H_
#define OCEANBASE_OBSQL_SCHEMA_PRT_H_

#include "common/ob_schema.h"
#include "common/ob_server.h"
#include "common/ob_define.h"

#include "common/data_buffer.h"
#include "common/ob_client_manager.h"

namespace oceanbase 
{
  namespace obsql 
  {
    class ObSchemaPrinter
    {
      public:
        ObSchemaPrinter(const oceanbase::common::ObSchemaManagerV2 & schema_mgr)
          : schema_mgr_(schema_mgr)
        {
        }
        virtual ~ObSchemaPrinter() {}

      public:

        int set_schema(const oceanbase::common::ObSchemaManagerV2 & schema_mgr) 
        { 
          schema_mgr_ = schema_mgr; 
          return common::OB_SUCCESS;
        }
        const oceanbase::common::ObSchemaManagerV2 get_schema() const { return schema_mgr_; }

        int output();

      private:
        int column_output(const oceanbase::common::ObTableSchema* table);
        int join_output(const oceanbase::common::ObTableSchema* table);
        const char* get_type_string(const oceanbase::common::ObColumnSchemaV2 * column);

      private:
        oceanbase::common::ObSchemaManagerV2 schema_mgr_;
    };
  }
}

#endif // OCEANBASE_OBSQL_SCHEMA_PRT_H_

