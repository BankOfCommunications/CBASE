/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_get.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_tablet_get.h"
#include "common/ob_new_scanner_helper.h"

using namespace oceanbase;
using namespace sql;
using namespace common;

void ObTabletGet::reset()
{
  tablet_manager_ = NULL;
  op_sstable_get_.reset();
  get_param_.reset();
  sql_get_param_ = NULL;
  ups_mget_row_desc_.reset();

  op_ups_multi_get_.reset();
  op_tablet_get_fuse_.reset();
  op_tablet_join_.reset();
  op_project_.reset();
  op_rename_.reset();
}

void ObTabletGet::reuse()
{
  tablet_manager_ = NULL;
  op_sstable_get_.reuse();
  get_param_.reset();
  sql_get_param_ = NULL;
  ups_mget_row_desc_.reset();

  op_ups_multi_get_.reuse();
  op_tablet_get_fuse_.reuse();
  op_tablet_join_.reuse();
  op_project_.reuse();
  op_rename_.reuse();
}

int ObTabletGet::create_plan(const ObSchemaManagerV2 &schema_mgr)
{
  int ret = OB_SUCCESS;
  ObTabletJoin::TableJoinInfo table_join_info;
  ObArray<uint64_t> basic_columns;
  uint64_t table_id = sql_get_param_->get_table_id();
  uint64_t renamed_table_id = sql_get_param_->get_renamed_table_id();
  ObProject *op_project = NULL;
  int64_t data_version = 0;
  bool is_need_incremental_data = true;
  ObCellInfo cell_info;
// del by maosy [Delete_Update_Function_isolation_RC] 20161218
  //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//  if (sql_get_param_->get_data_mark_param().is_valid())
//  {
//      TBSYS_LOG(DEBUG,"mul_del::debug,CS get real data mark param=[%s]",
//                to_cstring(sql_get_param_->get_data_mark_param()));
//  }
  //add duyr 20160531:e
//del e
  if (OB_SUCCESS != (ret = get_basic_column_and_join_info(
                               sql_get_param_->get_project(),
                               schema_mgr,
                               table_id,
                               renamed_table_id,
                               basic_columns,
                               table_join_info
                         // add by maosy [Delete_Update_Function_isolation_RC] 20161218
                               //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//                               ,&(sql_get_param_->get_data_mark_param())
                               //add duyr 20160531:e
                         //del e
                               )))
  {
    TBSYS_LOG(WARN, "fail to get basic column and join info:ret[%d]", ret);
  }
  else
  {
    TBSYS_LOG(DEBUG, "cs select basic column ids [%ld], project[%s]", basic_columns.count(),
      to_cstring(sql_get_param_->get_project()));
  }

  if (OB_SUCCESS == ret)
  {
    get_param_.reset(true);
    get_param_.set_is_result_cached(sql_get_param_->get_is_result_cached());
    get_param_.set_is_read_consistency(sql_get_param_->get_is_read_consistency());
    // add by maosy [Delete_Update_Function_isolation_RC] 20161218
    //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//    get_param_.set_data_mark_param(sql_get_param_->get_data_mark_param());
    //add duyr 20160531:e
    //del e
    for (int64_t i = 0; OB_SUCCESS == ret && i < sql_get_param_->get_row_size(); i ++)
    {
      cell_info.row_key_ = *(sql_get_param_->operator[](i));
      cell_info.table_id_ = table_id;
      for (int64_t col_id_idx = 0; OB_SUCCESS == ret && col_id_idx < basic_columns.count(); col_id_idx ++)
      {
        cell_info.column_id_ = basic_columns.at(col_id_idx);
        if (OB_SUCCESS != (ret = get_param_.add_cell(cell_info)))
        {
          TBSYS_LOG(WARN, "fail to add cell to get param:ret[%d]", ret);
        }
      }
    }
    TBSYS_LOG(DEBUG, "cs get param row size[%ld] sql_get_param row size[%ld]", get_param_.get_row_size(),
      sql_get_param_->get_row_size());
  }

  if (OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = op_sstable_get_.open_tablet_manager(tablet_manager_, &get_param_)))
    {
      TBSYS_LOG(WARN, "fail to open scan context:ret[%d]", ret);
    }
    else
    {
      plan_level_ = SSTABLE_DATA;
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (sql_get_param_->get_is_only_static_data())
    {
      is_need_incremental_data = false;
      last_rowkey_op_ = &op_sstable_get_;
    }
    else
    {
      op_sstable_get_.get_tablet_data_version(data_version);
      if (sql_get_param_->get_data_version() != OB_NEWEST_DATA_VERSION)
      {
        if (sql_get_param_->get_data_version() == OB_INVALID_VERSION)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "invalid version");
        }
        else if (sql_get_param_->get_data_version() < data_version)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "The request version is not exist: request version[%ld], sstable version[%ld]", sql_get_param_->get_data_version(), data_version);
        }
        else if (sql_get_param_->get_data_version() == data_version)
        {
          last_rowkey_op_ = &op_sstable_get_;
          is_need_incremental_data = false;
        }
      }
    }
    FILL_TRACE_LOG("gen sstable getter done, need increment data[%d]", is_need_incremental_data);
  }

  if (OB_SUCCESS == ret)
  {
    if (is_need_incremental_data)
    {
      if (OB_SUCCESS != (ret = need_incremental_data(
                             basic_columns,
                             table_join_info,
                             data_version,
                             sql_get_param_->get_data_version())))
      {
        TBSYS_LOG(WARN, "fail to add ups operator:ret[%d]", ret);
      }
    }
    else
    {
      op_root_ = &op_sstable_get_;
    }
  }


  if (OB_SUCCESS == ret && sql_get_param_->has_project())
  {
    op_project = &op_project_;
    if (OB_SUCCESS == ret)
    {
      op_project->assign(&sql_get_param_->get_project());
      if (OB_SUCCESS != (ret = op_project->set_child(0, *op_root_)))
      {
        TBSYS_LOG(WARN, "fail to set project child. ret=%d", ret);
      }
      else
      {

        TBSYS_LOG(DEBUG, "cs get project desc[%s]", to_cstring(*op_project));
        op_root_ = op_project;
      }
    }
  }

  //release tablet
  if (OB_SUCCESS != ret)
  {
    int err = OB_SUCCESS;
    if (OB_SUCCESS != (err = op_sstable_get_.close()))
    {
      TBSYS_LOG(WARN, "fail to close op sstable get:err[%d]", err);
    }
  }

  return ret;
}

int ObTabletGet::need_incremental_data(
    ObArray<uint64_t> &basic_columns,
    ObTabletJoin::TableJoinInfo &table_join_info,
    int64_t start_data_version,
    int64_t end_data_version)

{
  int ret = OB_SUCCESS;
  ObUpsMultiGet *op_ups_multi_get = NULL;
  ObTabletGetFuse *op_tablet_get_fuse = NULL;
  ObTabletJoin *op_tablet_join = NULL;

  uint64_t table_id = sql_get_param_->get_table_id();
  uint64_t renamed_table_id = sql_get_param_->get_renamed_table_id();

  ObVersionRange version_range;
  UNUSED(basic_columns);

  if(OB_SUCCESS == ret)
  {
      // del by maosy [Delete_Update_Function_isolation_RC] 20161218
      // add by gaojt [Delete_Update_Function_isolation] for fix data not safe 20160923 b:
//      if(sql_get_param_->get_data_mark_param ().is_valid () && start_data_version >= 2)
//      {
//          TBSYS_LOG(DEBUG,"gaojt test start version is = %ld",start_data_version);
//          version_range.start_version_ = ObVersion(start_data_version -1 );
//              version_range.border_flag_.unset_inclusive_start ();
//      }
//      else
//      {
          // add gaojt 20160923 :e
          //del e
          version_range.start_version_ = ObVersion(start_data_version + 1);
              version_range.border_flag_.set_inclusive_start();
//      }// add by gaojt
    version_range.border_flag_.unset_min_value();


    if (end_data_version == OB_NEWEST_DATA_VERSION)
    {
      version_range.border_flag_.set_max_value();
    }
    else
    {
      version_range.end_version_ = ObVersion(end_data_version);
      version_range.border_flag_.unset_max_value();
      version_range.border_flag_.set_inclusive_end();
    }
    get_param_.set_version_range(version_range);
  }

  // init ups get
  if (OB_SUCCESS == ret)
  {
    op_ups_multi_get = &op_ups_multi_get_;
    ups_mget_row_desc_.reset();
    if(OB_SUCCESS != (ret = op_ups_multi_get->set_rpc_proxy(rpc_proxy_)))
    {
      TBSYS_LOG(WARN, "ups scan set ups rpc stub fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = op_ups_multi_get->set_ts_timeout_us(ts_timeout_us_)))
    {
      TBSYS_LOG(WARN, "set ups scan timeout fail:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = ObNewScannerHelper::get_row_desc(get_param_, false, ups_mget_row_desc_)))
    {
      TBSYS_LOG(WARN, "fail to get row desc:ret[%d] get_param=%s", ret, to_cstring(get_param_));
    }
    else
    {
      get_param_.set_is_read_consistency(sql_get_param_->get_is_read_consistency());
      op_ups_multi_get->set_row_desc(ups_mget_row_desc_);
      op_ups_multi_get->set_get_param(get_param_);
    }
  }

  if (OB_SUCCESS == ret)
  {
    op_tablet_get_fuse = &op_tablet_get_fuse_;
    if (OB_SUCCESS == ret)
    {
      last_rowkey_op_ = op_tablet_get_fuse;
    }
  }

  if (OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = op_tablet_get_fuse->set_sstable_get(&op_sstable_get_)))
    {
      TBSYS_LOG(WARN, "set sstable scan fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = op_tablet_get_fuse->set_incremental_get(op_ups_multi_get)))
    {
      TBSYS_LOG(WARN, "set incremental scan fail:ret[%d]", ret);
    }
    else
    {
      plan_level_ = UPS_DATA;
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(table_join_info.join_column_.count() > 0)
    {
      op_tablet_join = &op_tablet_join_;
      if (OB_SUCCESS == ret)
      {
        op_tablet_join->set_version_range(version_range);
        op_tablet_join->set_table_join_info(table_join_info);
        op_tablet_join->set_batch_count(join_batch_count_);
        op_tablet_join->set_is_read_consistency(is_read_consistency_);
        op_tablet_join->set_child(0, *op_tablet_get_fuse);
        op_tablet_join->set_ts_timeout_us(ts_timeout_us_);
        if (OB_SUCCESS != (ret = op_tablet_join->set_rpc_proxy(rpc_proxy_) ))
        {
          TBSYS_LOG(WARN, "fail to set rpc proxy:ret[%d]", ret);
        }
        else
        {
          op_root_ = op_tablet_join;
          plan_level_ = JOIN_DATA;
        }
      }
    }
    else
    {
      op_root_ = op_tablet_get_fuse;
    }
  }

  if(OB_SUCCESS == ret && renamed_table_id != table_id)
  {
    if (OB_SUCCESS == ret)
    {
      if(OB_SUCCESS != (ret = op_rename_.set_table(renamed_table_id, table_id)))
      {
        TBSYS_LOG(WARN, "op_rename set table fail:ret[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = op_rename_.set_child(0, *op_root_)))
      {
        TBSYS_LOG(WARN, "op_rename set child fail:ret[%d]", ret);
      }
      else
      {
        op_root_ = &op_rename_;
      }
    }
  }
  //add zhujun [transaction read uncommit]2016/3/25
  if(ret == OB_SUCCESS)
  {
      //ObTransID trans_id_ptr = dynamic_cast<ObTransID>(sql_scan_param_->get_trans_id());
      op_ups_multi_get_.set_trans_id(sql_get_param_->get_trans_id());
  }
  //add:e
  return ret;
}

bool ObTabletGet::check_inner_stat() const
{
  int ret = true;
  if (NULL == tablet_manager_ ||
    NULL == sql_get_param_)
  {
    ret = false;
    TBSYS_LOG(WARN, "tablet_manager_ [%p], sql_get_param_[%p]",
      tablet_manager_, sql_get_param_);
  }
  return ret;
}

int ObTabletGet::set_tablet_manager(chunkserver::ObTabletManager *tablet_manager)
{
  int ret = OB_SUCCESS;
  if (NULL == tablet_manager)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "tablet_manager is null");
  }
  else
  {
    tablet_manager_ = tablet_manager;
  }
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObTabletGet, PHY_TABLET_GET);
  }
}

int64_t ObTabletGet::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (NULL != op_root_)
  {
    op_root_->to_string(buf, buf_len);
  }
  return pos;
}
