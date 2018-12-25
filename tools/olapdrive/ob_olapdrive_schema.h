/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_olapdrive_schema.h for define olapdrive schema manager. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_OLAPDRIVE_SCHEMA_H
#define OCEANBASE_OLAPDRIVE_SCHEMA_H

#include "common/ob_schema.h"

namespace oceanbase 
{ 
  namespace olapdrive
  {
    class ObOlapdriveSchema
    {
    public:
      ObOlapdriveSchema();
      ~ObOlapdriveSchema();

      int init();

      const common::ObSchemaManagerV2& get_schema_manager() const;
      common::ObSchemaManagerV2& get_schema_manager();

      const common::ObTableSchema* get_key_meta_schema() const;
      const common::ObTableSchema* get_campaign_schema() const;
      const common::ObTableSchema* get_adgroup_schema() const;
      const common::ObTableSchema* get_lz_schema() const;

      const common::ObString get_key_meta_name() const;
      const common::ObString get_campaign_name() const;
      const common::ObString get_adgroup_name() const;
      const common::ObString get_lz_name() const;

      const common::ObString get_column_name(const common::ObColumnSchemaV2& column) const;

    private:
      int init_table_schema();

    public:
      static const int64_t TABLE_COUNT = 4;

    private:
      DISALLOW_COPY_AND_ASSIGN(ObOlapdriveSchema);

      bool inited_;
      common::ObSchemaManagerV2 schema_manager_;
    };
  } // end namespace olapdrive
} // end namespace oceanbase

#endif //OCEANBASE_OLAPDRIVE_SCHEMA_H
