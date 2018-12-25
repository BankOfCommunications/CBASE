/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_TEST_OB_SCHEMA_PROXY_H_
#define OCEANBASE_TEST_OB_SCHEMA_PROXY_H_

#include "common/ob_string.h"
#include "updateserver/ob_ups_utils.h"

namespace oceanbase
{
  namespace test
  {
    class ObSchemaProxy// : public updateserver::ObSchemaManagerWrapper
    {
      public:
        static ObSchemaProxy& get_instance()
        {
          static ObSchemaProxy instance;
          return instance;
        }

        int fetch_schema();

        int64_t get_column_num();

        const common::ObString& get_column_name(uint64_t id);

        uint64_t get_table_id();

        const common::ObString& get_table_name();

      private:
        ObSchemaProxy();
        virtual ~ObSchemaProxy() {}

      private:
        common::ObString column_name_[12];
        common::ObString table_name_;
    };
  } // end namespace test
} // end namespace oceanbase

#endif // OCEANBASE_TEST_OB_SCHEMA_PROXY_H_
