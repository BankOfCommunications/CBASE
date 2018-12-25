/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_schema_service_ups_provider.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SCHEMA_SERVICE_UPS_PROVIDER_H
#define _OB_SCHEMA_SERVICE_UPS_PROVIDER_H 1
#include "common/roottable/ob_ups_provider.h"
#include "ob_ups_manager.h"
namespace oceanbase
{
  namespace rootserver
  {
    /// thread-safe ups provider for schema_service
    class ObSchemaServiceUpsProvider: public common::ObUpsProvider
    {
      public:
        ObSchemaServiceUpsProvider(const ObUpsManager &ups_manager);
        virtual ~ObSchemaServiceUpsProvider();

        virtual int get_ups(common::ObServer &ups);
      private:
        // disallow copy
        ObSchemaServiceUpsProvider(const ObSchemaServiceUpsProvider &other);
        ObSchemaServiceUpsProvider& operator=(const ObSchemaServiceUpsProvider &other);
      private:
        // data members
        const ObUpsManager &ups_manager_;
    };
  } // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_SCHEMA_SERVICE_UPS_PROVIDER_H */

