/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_olapdrive_schema.cpp for define olapdrive schema manager. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "tblog.h"
#include "common/ob_define.h"
#include "ob_olapdrive_schema.h"

namespace oceanbase 
{ 
  namespace olapdrive 
  {
    using namespace common;

    ObOlapdriveSchema::ObOlapdriveSchema()
    : inited_(false)
    {

    }

    ObOlapdriveSchema::~ObOlapdriveSchema()
    {

    }

    int ObOlapdriveSchema::init()
    {
      int ret             = OB_SUCCESS;
      int64_t schema_size = 0;

      if (inited_)
      {
        TBSYS_LOG(WARN, "olapdrive schema has inited before");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret 
          && (schema_size = schema_manager_.get_table_count()) != TABLE_COUNT)
      {
        TBSYS_LOG(WARN, "expected 4 schema in schema manager, but get %ld schema",
                  schema_size);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        inited_ = true;
      }

      return ret;
    }

    const ObSchemaManagerV2& ObOlapdriveSchema::get_schema_manager() const
    {
      return schema_manager_;
    }

    ObSchemaManagerV2& ObOlapdriveSchema::get_schema_manager()
    {
      return schema_manager_;
    }

    const ObTableSchema* ObOlapdriveSchema::get_key_meta_schema() const
    {
      return (schema_manager_.get_table_schema("meta"));
    }

    const ObTableSchema* ObOlapdriveSchema::get_campaign_schema() const
    {
      return (schema_manager_.get_table_schema("campaign"));
    }

    const ObTableSchema* ObOlapdriveSchema::get_adgroup_schema() const
    {
      return (schema_manager_.get_table_schema("adgroup"));
    }

    const ObTableSchema* ObOlapdriveSchema::get_lz_schema() const
    {
      return (schema_manager_.get_table_schema("lzp4p"));
    }

    const ObString ObOlapdriveSchema::get_key_meta_name() const 
    {
      ObString table_name;
      const char* name_str = NULL;

      if (inited_)
      {
        name_str = get_key_meta_schema()->get_table_name();
        table_name.assign(const_cast<char*>(name_str), 
            static_cast<int32_t>(strlen(name_str)));
      }

      return table_name;
    }

    const ObString ObOlapdriveSchema::get_campaign_name() const 
    {
      ObString table_name;
      const char* name_str = NULL;

      if (inited_)
      {
        name_str = get_campaign_schema()->get_table_name();
        table_name.assign(const_cast<char*>(name_str), static_cast<int32_t>(strlen(name_str)));
      }

      return table_name;
    }

    const ObString ObOlapdriveSchema::get_adgroup_name() const 
    {
      ObString table_name;
      const char* name_str = NULL;

      if (inited_)
      {
        name_str = get_adgroup_schema()->get_table_name();
        table_name.assign(const_cast<char*>(name_str), static_cast<int32_t>(strlen(name_str)));
      }

      return table_name;
    }

    const ObString ObOlapdriveSchema::get_lz_name() const 
    {
      ObString table_name;
      const char* name_str = NULL;

      if (inited_)
      {
        name_str = get_lz_schema()->get_table_name();
        table_name.assign(const_cast<char*>(name_str), static_cast<int32_t>(strlen(name_str)));
      }

      return table_name;
    }

    const ObString ObOlapdriveSchema::get_column_name(const ObColumnSchemaV2& column) const
    {
      const char* name_str = NULL;
      ObString column_name;

      name_str = column.get_name();
      column_name.assign(const_cast<char*>(name_str), static_cast<int32_t>(strlen(name_str)));

      return column_name;
    }
  } // end namespace olapdrive
} // end namespace oceanbase
