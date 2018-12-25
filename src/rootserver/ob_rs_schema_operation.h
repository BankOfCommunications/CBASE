/**
 * (C) 2007-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef OCEANBASE_ROOTSERVER_RS_SCHEMA_OPERATION_
#define OCEANBASE_ROOTSERVER_RS_SCHEMA_OPERATION_
#include "common/ob_schema.h"

namespace oceanbase
{
  namespace rootserver
  {
    class ObRsSchemaOperation
    {
      public:
        ObRsSchemaOperation();
        ~ObRsSchemaOperation();
        common::ObSchemaManagerV2 *get_schema_manager();
        void reset_schema_manager();
        void destroy_data();
        int generate_schema(const common::ObSchemaManagerV2 *schema_mgr);
        int change_table_schema(const common::ObSchemaManagerV2 *schema_mgr,
            const char* table_name, const uint64_t new_table_id);
      private:
        common::ObSchemaManagerV2 *schema_manager_;
    };
  }
}

#endif

