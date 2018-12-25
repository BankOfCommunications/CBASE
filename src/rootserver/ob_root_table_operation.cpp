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

#include "ob_root_table_operation.h"

namespace oceanbase
{
  namespace rootserver
  {
    ObRootTableOperation::ObRootTableOperation()
    {
      config_ = NULL;
      new_root_table_ = NULL;
      tablet_manager_ = NULL;
      root_table_tmp_ = NULL;
      tablet_manager_tmp_ = NULL;
    }
    ObRootTableOperation::~ObRootTableOperation()
    {
      destroy_data();
    }
    void ObRootTableOperation::init(const ObRootServerConfig *config)
    {
      config_ = const_cast<ObRootServerConfig *>(config);
    }
    void ObRootTableOperation::set_schema_manager(const common::ObSchemaManagerV2 *schema_mgr)
    {
      schema_manager_ = schema_mgr;
    }
    ObRootTable2* ObRootTableOperation::get_root_table()
    {
      return new_root_table_;
    }
    ObTabletInfoManager *ObRootTableOperation::get_tablet_info_manager()
    {
      return tablet_manager_;
    }
    void ObRootTableOperation::reset_root_table()
    {
      new_root_table_ = NULL;
      tablet_manager_ = NULL;
    }
    void ObRootTableOperation::destroy_data()
    {
      if (NULL != new_root_table_)
      {
        OB_DELETE(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, new_root_table_);
        new_root_table_ = NULL;
      }
      if (NULL != tablet_manager_)
      {
        OB_DELETE(ObTabletInfoManager, ObModIds::OB_RS_TABLET_MANAGER, tablet_manager_);
        tablet_manager_ = NULL;
      }
      if (NULL != root_table_tmp_)
      {
        OB_DELETE(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, root_table_tmp_);
        root_table_tmp_ = NULL;
      }
      if (NULL != tablet_manager_tmp_)
      {
        OB_DELETE(ObTabletInfoManager, ObModIds::OB_RS_TABLET_MANAGER, tablet_manager_tmp_);
        tablet_manager_tmp_ = NULL;
      }
    }
    int ObRootTableOperation::generate_root_table()
    {
      int ret = OB_SUCCESS;
      if (NULL != new_root_table_)
      {
        OB_DELETE(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, new_root_table_);
        new_root_table_ = NULL;
      }
      if (NULL != tablet_manager_)
      {
        OB_DELETE(ObTabletInfoManager, ObModIds::OB_RS_TABLET_MANAGER, tablet_manager_);
        tablet_manager_ = NULL;
      }
      tablet_manager_ = OB_NEW(ObTabletInfoManager, ObModIds::OB_RS_TABLET_MANAGER);
      if (tablet_manager_ == NULL)
      {
        TBSYS_LOG(ERROR, "new ObTabletInfoManager error");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      if (OB_SUCCESS == ret)
      {
        new_root_table_ = OB_NEW(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, tablet_manager_);
        if (NULL == new_root_table_)
        {
          TBSYS_LOG(ERROR, "new ObRootTable2 error");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          //new_root_table_->set_replica_num(config_->tablet_replicas_num);
        }
      }

      if (OB_SUCCESS == ret)
      {
        tablet_manager_tmp_ = OB_NEW(ObTabletInfoManager, ObModIds::OB_RS_TABLET_MANAGER);
        if (tablet_manager_tmp_ == NULL)
        {
          TBSYS_LOG(ERROR, "new ObTabletInfoManager error");
          ret = OB_ERROR;
        }
      }
      if (OB_SUCCESS == ret)
      {
        root_table_tmp_ = OB_NEW(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, tablet_manager_tmp_);
        if (root_table_tmp_ == NULL)
        {
          TBSYS_LOG(ERROR, "new ObRootTable2 error");
          ret = OB_ERROR;
        }
        else
        {
          TBSYS_LOG(INFO, "operation helper generate roottable, addr=%p", root_table_tmp_);
        }
      }
      return ret;
    }
    int ObRootTableOperation::report_tablets(const ObTabletReportInfoList& tablets,
        const int32_t server_index, const int64_t frozen_mem_version)
    {
      int ret = OB_SUCCESS;
      UNUSED(frozen_mem_version);
      TBSYS_LOG(INFO, "cs (cs_index=%d) report new tablet.", server_index);
      if (NULL == schema_manager_)
      {
        TBSYS_LOG(WARN, "inner stat error. check it. schema_manager=%p", schema_manager_);
        ret = OB_ERROR;
      }
      else
      {
        int64_t index = tablets.tablet_list_.get_array_index();
        ObTabletReportInfo* p_table_info = NULL;
        for(int64_t i = 0; i < index; ++i)
        {
          p_table_info = tablets.tablet_list_.at(i);
          if (p_table_info != NULL)
          {
            if (!p_table_info->tablet_info_.range_.is_left_open_right_closed())
            {
              TBSYS_LOG(WARN, "cs reported illegal tablet, server=%d crc=%lu range=%s",
                  server_index, p_table_info->tablet_info_.crc_sum_,
                  to_cstring(p_table_info->tablet_info_.range_));
              continue;
            }
            //todo add schema check
            if (NULL == schema_manager_->get_table_schema(p_table_info->tablet_info_.range_.table_id_))
            {
              TBSYS_LOG(DEBUG, "report table is %lu, refuse to add", p_table_info->tablet_info_.range_.table_id_);
              continue;
            }
            TBSYS_LOG(DEBUG, "add a tablet, server=%d crc=%lu version=%ld seq=%ld range=%s",
                server_index, p_table_info->tablet_info_.crc_sum_,
                p_table_info->tablet_location_.tablet_version_,
                p_table_info->tablet_location_.tablet_seq_,
                to_cstring(p_table_info->tablet_info_.range_));
            ret = new_root_table_->add(p_table_info->tablet_info_, server_index,
                p_table_info->tablet_location_.tablet_version_, p_table_info->tablet_location_.tablet_seq_);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "fail to add tablet. range=%s", to_cstring(p_table_info->tablet_info_.range_));
              break;
            }
          }
        }
      }
      return ret;
    }
    int ObRootTableOperation::check_root_table(common::ObTabletReportInfoList &delete_list)
    {
      int ret = OB_SUCCESS;
      if (NULL == new_root_table_ || NULL == root_table_tmp_)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "inner error. new_root_table_ = null,  root_table_tmp = null");
      }
      delete_list.reset();
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG, "root table begin=%p, end=%p, size=%ld",
            new_root_table_->end(), new_root_table_->begin(), new_root_table_->end() - new_root_table_->begin());
        root_table_tmp_->clear();
        new_root_table_->sort();
        TBSYS_LOG(DEBUG, "root table begin=%p, end=%p, size=%ld",
            new_root_table_->end(), new_root_table_->begin(), new_root_table_->end() - new_root_table_->begin());
        ret = new_root_table_->shrink_to(root_table_tmp_, delete_list);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to shrink. ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        root_table_tmp_->sort();
        int64_t timeout = config_->safe_wait_init_time;
        if (!root_table_tmp_->check_lost_range(schema_manager_))
        {
          TBSYS_LOG(WARN, "check root table failed we will wait for another %ld seconds", timeout);
          ret = OB_ERROR;
        }
      }
      if (OB_SUCCESS == ret)
      {
        if (NULL == schema_manager_)
        {
          TBSYS_LOG(WARN, "inner stat error. schema_manager=%p", schema_manager_);
          ret = OB_ERROR;
        }
      }
      if (OB_SUCCESS == ret)
      {
        //all bypass table is report
        for (const ObTableSchema* it = schema_manager_->table_begin(); it != schema_manager_->table_end(); ++it)
        {
          if (it->get_table_id() != OB_INVALID_ID)
          {
            if(!root_table_tmp_->table_is_exist(it->get_table_id()))
            {
              TBSYS_LOG(DEBUG, "table_id = %lu has not been reported integrity", it->get_table_id() );
              ret = OB_ERROR;
              break;
            }
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        if (!root_table_tmp_->check_tablet_copy_count(static_cast<int32_t>(config_->tablet_replicas_num)))
        {
          TBSYS_LOG(WARN, "tablet copy count are not satify, never mind");
        }
      }
      if (OB_SUCCESS == ret)
      {
        OB_DELETE(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, new_root_table_);
        new_root_table_ = NULL;
        OB_DELETE(ObTabletInfoManager, ObModIds::OB_RS_TABLET_MANAGER, tablet_manager_);
        tablet_manager_ = NULL;
        new_root_table_ = root_table_tmp_;
        tablet_manager_ = tablet_manager_tmp_;
        root_table_tmp_ = NULL;
        tablet_manager_tmp_ = NULL;
        TBSYS_LOG(INFO, "check root table success. new root table addr=%p", new_root_table_);
      }
      return ret;
    }
  }
}
