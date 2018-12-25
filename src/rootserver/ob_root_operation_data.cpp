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

#include "ob_root_operation_data.h"
#include "ob_chunk_server_manager.h"
namespace oceanbase
{
  namespace rootserver
  {
    ObRootOperationData::ObRootOperationData()
    {
      operation_type_ = common::INVALID_TYPE;
    }
    ObRootOperationData::~ObRootOperationData()
    {
    }
    void ObRootOperationData::init(const ObRootServerConfig *config)
    {
      root_table_.init(config);
    }
    common::ObSchemaManagerV2* ObRootOperationData::get_schema_manager()
    {
      return schema_helper_.get_schema_manager();
    }
    void ObRootOperationData::reset_data()
    {
      operation_type_ = common::INVALID_TYPE;
      delete_tables_.clear();
      root_table_.reset_root_table();
      schema_helper_.reset_schema_manager();
    }
    void ObRootOperationData::destroy_data()
    {
      operation_type_ = common::INVALID_TYPE;
      delete_tables_.clear();
      root_table_.destroy_data();
      schema_helper_.destroy_data();
    }
    OperationType ObRootOperationData::get_operation_type()
    {
      return operation_type_;
    }
    void ObRootOperationData::set_operation_type(const OperationType &type)
    {
      operation_type_  = type;
    }
    ObRootTable2* ObRootOperationData::get_root_table()
    {
      return root_table_.get_root_table();
    }
    ObTabletInfoManager* ObRootOperationData::get_tablet_info_manager()
    {
      return root_table_.get_tablet_info_manager();
    }
    int ObRootOperationData::generate_root_table()
    {
      int ret = OB_SUCCESS;
      common::ObSchemaManagerV2 *schema_mgr = get_schema_manager();
      if (NULL == schema_mgr)
      {
        TBSYS_LOG(WARN, "invalid argument. schema_mgr = null");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        root_table_.set_schema_manager(schema_mgr);
        ret = root_table_.generate_root_table();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to generate root table. ret=%d", ret);
        }
      }
      return ret;
    }
    int ObRootOperationData::generate_bypass_data(const common::ObSchemaManagerV2 *schema_mgr)
    {
      int ret = OB_SUCCESS;
      if (NULL == schema_mgr)
      {
        TBSYS_LOG(WARN, "invalid argument. schema_mgr=%p", schema_mgr);
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == ret)
      {
        ret = schema_helper_.generate_schema(schema_mgr);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to generate_schema, ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = generate_root_table();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to generate new roottable. ret=%d", ret);
        }
      }
      return ret;
    }
    common::ObArray<uint64_t>& ObRootOperationData::get_delete_table()
    {
      return delete_tables_;
    }
    int ObRootOperationData::init_schema_mgr(const common::ObSchemaManagerV2 *schema_mgr)
    {
      return schema_helper_.generate_schema(schema_mgr);
    }
    int ObRootOperationData::change_table_schema(const common::ObSchemaManagerV2 *schema_mgr,
        const char* table_name, const uint64_t new_table_id)
    {
      return schema_helper_.change_table_schema(schema_mgr, table_name, new_table_id);
    }
    int ObRootOperationData::report_tablets(const ObTabletReportInfoList& tablets,
        const int32_t server_index, const int64_t frozen_mem_version)
    {
      int ret = OB_SUCCESS;
      UNUSED(frozen_mem_version);
      ret = root_table_.report_tablets(tablets, server_index, frozen_mem_version);
      return ret;
    }
    int ObRootOperationData::set_delete_table(common::ObArray<uint64_t> &delete_tables)
    {
      int ret = OB_SUCCESS;
      if (delete_tables.count() <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "invalid argument. table count=%ld", delete_tables.count());
      }
      else
      {
        for (int64_t i = 0; i < delete_tables.count(); i++)
        {
          delete_tables_.push_back(delete_tables.at(i));
        }
      }
      return ret;
    }
    int ObRootOperationData::check_root_table(common::ObTabletReportInfoList &delete_list)
    {
      return root_table_.check_root_table(delete_list);
    }
  }
}

