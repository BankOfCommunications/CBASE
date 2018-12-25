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
 */

#include "ob_rs_schema_operation.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;

namespace oceanbase
{
  namespace rootserver
  {
    ObRsSchemaOperation::ObRsSchemaOperation()
    {
      schema_manager_ = NULL;
    }
    ObRsSchemaOperation::~ObRsSchemaOperation()
    {
      destroy_data();
    }
    void ObRsSchemaOperation::destroy_data()
    {
      if (NULL != schema_manager_)
      {
        delete schema_manager_;
        schema_manager_ = NULL;
      }
    }
    common::ObSchemaManagerV2* ObRsSchemaOperation::get_schema_manager()
    {
      return schema_manager_;
    }
    void ObRsSchemaOperation::reset_schema_manager()
    {
      schema_manager_ = NULL;
    }
    int ObRsSchemaOperation::generate_schema(const common::ObSchemaManagerV2 *schema_mgr)
    {
      int ret = OB_SUCCESS;
      if (NULL == schema_mgr)
      {
        TBSYS_LOG(WARN, "invalid argument. schema_mgr=%p", schema_mgr);
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == ret)
      {
        if (NULL != schema_manager_)
        {
          TBSYS_LOG(WARN, "schema_manager is already exist. schema_manager=%p", schema_manager_);
          ret = OB_ERROR;

        }
      }
      if (OB_SUCCESS == ret)
      {
        int64_t now = tbsys::CTimeUtil::getTime();
        if (NULL != schema_manager_)
        {
          delete schema_manager_;
          schema_manager_ = NULL;
        }
        if (NULL == schema_manager_)
        {
          schema_manager_ = new (std::nothrow)common::ObSchemaManagerV2(now);
          if (NULL == schema_manager_)
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            TBSYS_LOG(WARN, "fail to allocate memory for schemaManager.");
          }
          else
          {
            *schema_manager_ = *schema_mgr;
            TBSYS_LOG(INFO, "new schema version=%ld", now);
            schema_manager_->set_version(now);
          }
        }
      }
      return ret;
    }
    int ObRsSchemaOperation::change_table_schema(const common::ObSchemaManagerV2 *schema_mgr,
    const char* table_name, const uint64_t new_table_id)
    {
      int ret = OB_SUCCESS;
      if (NULL == schema_mgr || NULL == table_name || new_table_id <= 0)
      {
        TBSYS_LOG(WARN, "INVALID ARGUMENT. schema_mgr=%p, table_name=%p, table_id=%lu",
            schema_mgr, table_name, new_table_id);
        ret = OB_INVALID_ARGUMENT;
      }
      const ObTableSchema *old_table_schema = NULL;
      uint64_t old_table_id = 0;

      if (OB_SUCCESS == ret)
      {
        old_table_schema = schema_mgr->get_table_schema(table_name);
        old_table_id = 0;
        if (NULL != old_table_schema)
        {
          old_table_id = old_table_schema->get_table_id();
        }
        else
        {
          TBSYS_LOG(ERROR, "error happen. not table exist. table_name=%s", table_name);
          ret = OB_ERROR;
        }
      }
      if (OB_SUCCESS == ret)
      {
        if (new_table_id > schema_manager_->get_max_table_id())
        {
          TBSYS_LOG(TRACE, "new max table id is =%ld", new_table_id);
          schema_manager_->set_max_table_id(new_table_id);
        }
        ret = schema_manager_->change_table_id(old_table_id, new_table_id);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to change table id. ret=%d, old_table_id=%ld, new_table_id=%ld", ret, old_table_id, new_table_id);
        }
      }
      return ret;
    }
  }
}

