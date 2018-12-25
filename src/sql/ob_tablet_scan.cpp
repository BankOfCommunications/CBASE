/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_scan.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_tablet_scan.h"
#include "common/serialization.h"
#include "common/utility.h"
#include "common/ob_cur_time.h"
#include "common/ob_profile_log.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::common::serialization;

ObTabletScan::ObTabletScan()
{
}

ObTabletScan::~ObTabletScan()
{
}

void ObTabletScan::reset(void)
{
  sql_scan_param_ = NULL;
  op_ups_scan_.reset();
  op_tablet_join_.reset();
  op_rename_.reset();
  op_filter_.reset();
  op_project_.reset();
  op_scalar_agg_.reset();
  op_group_columns_sort_.reset();
  op_group_.reset();
  op_limit_.reset();
  ObTabletRead::reset();
  sort_for_listagg_.reset();//add gaojt [ListAgg][JHOBv0.1]20150109
}

void ObTabletScan::reuse(void)
{
  sql_scan_param_ = NULL;
  op_ups_scan_.reuse();
  op_tablet_join_.reuse();
  op_rename_.reuse();
  op_filter_.reuse();
  op_project_.reuse();
  op_scalar_agg_.reuse();
  op_group_columns_sort_.reuse();
  op_group_.reuse();
  op_limit_.reuse();
  ObTabletRead::reuse();
  sort_for_listagg_.reuse();//add gaojt [ListAgg][JHOBv0.1]20150109
}

bool ObTabletScan::has_incremental_data() const
{
  bool ret = false;
  switch (plan_level_)
  {
    case SSTABLE_DATA:
      ret = false;
      break;
    case UPS_DATA:
      ret = !op_ups_scan_.is_result_empty();
      break;
    case JOIN_DATA:
      ret = true;
      break;
  }
  return ret;
}

int ObTabletScan::need_incremental_data(
    ObArray<uint64_t> &basic_columns,
    ObTabletJoin::TableJoinInfo &table_join_info,
        int64_t start_data_version,
        int64_t end_data_version,
        bool is_truncated /*add zhaoqiong [Truncate Table]:20160318:b*/)

{
  int ret = OB_SUCCESS;
  ObUpsScan *op_ups_scan = NULL;
  ObTabletScanFuse *op_tablet_scan_fuse = NULL;
  ObTabletJoin *op_tablet_join = NULL;
  ObTableRename *op_rename = NULL;
  uint64_t table_id = sql_scan_param_->get_table_id();
  uint64_t renamed_table_id = sql_scan_param_->get_renamed_table_id();
  ObVersionRange version_range;

  if(OB_SUCCESS == ret)
  {
    op_ups_scan = &op_ups_scan_;
    version_range.start_version_ = ObVersion(start_data_version + 1);
    version_range.border_flag_.unset_min_value();
    version_range.border_flag_.set_inclusive_start();

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
    op_ups_scan->set_version_range(version_range);
  }

  // init ups scan
  if (OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = op_ups_scan->set_ups_rpc_proxy(rpc_proxy_)))
    {
      TBSYS_LOG(WARN, "ups scan set ups rpc stub fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = op_ups_scan->set_ts_timeout_us(ts_timeout_us_)))
    {
      TBSYS_LOG(WARN, "set ups scan timeout fail:ret[%d]", ret);
    }
    else
    {
      op_ups_scan->set_is_read_consistency(sql_scan_param_->get_is_read_consistency());
    }
  }

  if(OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = set_ups_scan_range(*(sql_scan_param_->get_range()))))
    {
      TBSYS_LOG(WARN, "fail to set ups scan range, ret [%d]", ret);
    }
    for (int64_t i=0;OB_SUCCESS == ret && i<basic_columns.count();i++)
    {
      if(OB_SUCCESS != (ret = op_ups_scan->add_column(basic_columns.at(i))))
      {
        TBSYS_LOG(WARN, "op ups scan add column fail:ret[%d]", ret);
      }
    }
    // del by maosy [Delete_Update_Function_isolation_RC] 20161218
    //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//    if (OB_SUCCESS == ret)
//    {
//        op_ups_scan->set_data_mark_param(sql_scan_param_->get_data_mark_param());
//    }
    //add duyr 20160531:e
  // del by maosy
  }



  if (OB_SUCCESS == ret)
  {
    op_tablet_scan_fuse = &op_tablet_scan_fuse_;

    last_rowkey_op_ = op_tablet_scan_fuse;

    if(!is_truncated)
    {
      if (OB_SUCCESS != (ret = op_tablet_scan_fuse->set_sstable_scan(&op_sstable_scan_)))
      {
        TBSYS_LOG(WARN, "set sstable scan fail:ret[%d]", ret);
      }
      else
      {
         TBSYS_LOG(DEBUG, "set sstable scan succeed:ret[%d]", ret);
      }
    }
    else
    {
      op_sstable_scan_.close();
      op_tablet_scan_fuse->set_sstable_scan(NULL);
      TBSYS_LOG(DEBUG, "set sstable scan NULL");
    }
  }

  if (ret == OB_SUCCESS)
  {
    if (OB_SUCCESS != (ret = op_tablet_scan_fuse->set_incremental_scan(op_ups_scan)))
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
        op_tablet_join->set_child(0, *op_tablet_scan_fuse);
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
      op_root_ = op_tablet_scan_fuse;
    }
  }

  if(OB_SUCCESS == ret && renamed_table_id != table_id)
  {
    op_rename = &op_rename_;
    if (OB_SUCCESS == ret)
    {
      if(OB_SUCCESS != (ret = op_rename->set_table(renamed_table_id, table_id)))
      {
        TBSYS_LOG(WARN, "op_rename set table fail:ret[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = op_rename->set_child(0, *op_root_)))
      {
        TBSYS_LOG(WARN, "op_rename set child fail:ret[%d]", ret);
      }
      else
      {
        op_root_ = op_rename;
      }
    }
  }
  //add zhujun [transaction read uncommit]2016/5/23
  if(ret == OB_SUCCESS)
  {
      op_ups_scan_.set_trans_id(sql_scan_param_->get_trans_id());
  }
  //add:e
  return ret;
}
//add:e
bool ObTabletScan::check_inner_stat() const
{
  bool ret = false;

  ret = join_batch_count_ > 0
  && NULL != sql_scan_param_
  && ts_timeout_us_ > 0
  && NULL != rpc_proxy_;

  if (!ret)
  {
    TBSYS_LOG(WARN, "join_batch_count_[%ld], "
    "sql_scan_param_[%p], "
    "network_timeout_[%ld], "
    "rpc_proxy_[%p]",
    join_batch_count_,
    sql_scan_param_,
    ts_timeout_us_,
    rpc_proxy_);
  }

  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObTabletScan, PHY_TABLET_SCAN);
  }
}

int64_t ObTabletScan::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (NULL != op_root_)
  {
    op_root_->to_string(buf, buf_len);
  }
  return pos;
}

int ObTabletScan::build_sstable_scan_param(ObArray<uint64_t> &basic_columns,
    const ObSqlScanParam &sql_scan_param, sstable::ObSSTableScanParam &sstable_scan_param) const
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(DEBUG, "sql_scan_param=%s", to_cstring(sql_scan_param));

  //add zhaoqiong [fixbug:tablet version info lost when query from ms] 20170228:b
  if (sql_scan_param.get_version() != OB_INVALID_VERSION)
  {
      ObVersionRange version_range;
      version_range.end_version_ = ObVersion(sql_scan_param.get_version());
      version_range.border_flag_.unset_max_value();
      version_range.border_flag_.set_inclusive_end();
      sstable_scan_param.set_version_range(version_range);
  }
  //add:e

  sstable_scan_param.set_range(*sql_scan_param.get_range());
  sstable_scan_param.set_is_result_cached(sql_scan_param.get_is_result_cached());
  sstable_scan_param.set_not_exit_col_ret_nop(false);
  sstable_scan_param.set_scan_flag(sql_scan_param.get_scan_flag());

  for (int64_t i=0; OB_SUCCESS == ret && i<basic_columns.count(); i++)
  {
    if(OB_SUCCESS != (ret = sstable_scan_param.add_column(basic_columns.at(i))))
    {
      TBSYS_LOG(WARN, "scan param add column fail:ret[%d]", ret);
    }
  }

  return ret;
}

//add wenghaixing[secondary index]20141110
int ObTabletScan::build_sstable_scan_param_pub(ObArray<uint64_t> &basic_columns,
    const ObSqlScanParam &sql_scan_param, sstable::ObSSTableScanParam &sstable_scan_param) const
    {
        return build_sstable_scan_param(basic_columns,sql_scan_param,sstable_scan_param);
    }

//add e
int ObTabletScan::create_plan(const ObSchemaManagerV2 &schema_mgr)
{
  int ret = OB_SUCCESS;
  sstable::ObSSTableScanParam sstable_scan_param;
  ObTabletJoin::TableJoinInfo table_join_info;
  ObArray<uint64_t> basic_columns;
  uint64_t table_id = sql_scan_param_->get_table_id();
  uint64_t renamed_table_id = sql_scan_param_->get_renamed_table_id();
  ObProject *op_project = NULL;
  ObLimit *op_limit = NULL;
  ObFilter *op_filter = NULL;
  int64_t data_version;
  bool is_need_incremental_data = true;
  INIT_PROFILE_LOG_TIMER();
  // del by maosy [Delete_Update_Function_isolation_RC] 20161218
  //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//  if (sql_scan_param_->get_data_mark_param().is_valid())
//  {
//      TBSYS_LOG(DEBUG,"mul_del::debug,CS scan real data mark param=[%s]",
//                to_cstring(sql_scan_param_->get_data_mark_param()));
//  }
  //add duyr 20160531:e
//del by maosy
  if (OB_SUCCESS != (ret = get_basic_column_and_join_info(
                               sql_scan_param_->get_project(),
                               schema_mgr,
                               table_id,
                               renamed_table_id,
                               basic_columns,
                               table_join_info
                         // del by maosy [Delete_Update_Function_isolation_RC] 20161218
                               //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//                               ,&(sql_scan_param_->get_data_mark_param())
                               //add duyr 20160531:e
                 //del by maosy
                               )))
  {
    TBSYS_LOG(WARN, "fail to get basic column and join info:ret[%d]", ret);
  }

  if (OB_SUCCESS == ret)
  {

    if (OB_SUCCESS != (ret = build_sstable_scan_param(basic_columns, *sql_scan_param_, sstable_scan_param)))
    {
      TBSYS_LOG(WARN, "build_sstable_scan_param ret=%d", ret);
    }
    else if(OB_SUCCESS != (ret = op_sstable_scan_.open_scan_context(sstable_scan_param, scan_context_)))
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
    FILL_TRACE_LOG("read only static data[%s]", sql_scan_param_->get_is_only_static_data() ? "TRUE":"FALSE");
    if (sql_scan_param_->get_is_only_static_data())
    {
      is_need_incremental_data = false;
      last_rowkey_op_ = &op_sstable_scan_;
    }
    else
    {
      op_sstable_scan_.get_tablet_data_version(data_version);
      FILL_TRACE_LOG("request data version[%ld], cs serving data version[%ld]",
        sql_scan_param_->get_data_version(), data_version);
      PROFILE_LOG_TIME(DEBUG, "op_sstable_scan_ open context complete, data version[%ld] , range=%s",
          data_version, to_cstring(*sql_scan_param_->get_range()));
      if (sql_scan_param_->get_data_version() != OB_NEWEST_DATA_VERSION)
      {
        if (sql_scan_param_->get_data_version() == OB_INVALID_VERSION)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "invalid version");
        }
        else if (sql_scan_param_->get_data_version() < data_version)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "The request version is not exist: request version[%ld], sstable version[%ld]", sql_scan_param_->get_data_version(), data_version);
        }
        else if (sql_scan_param_->get_data_version() == data_version)
        {
          last_rowkey_op_ = &op_sstable_scan_;
          is_need_incremental_data = false;
        }
      }
    }
  }

  //ObVersionRange new_range; //add zhaoqiong [Truncate Table]:20160318
  bool is_truncated = false; //add zhaoqiong [Truncate Table]:20170519

  if (OB_SUCCESS == ret)
  {
    if (is_need_incremental_data)
    {
      //mod zhaoqiong [Truncate Table]:20160318
//      if (OB_SUCCESS != (ret = need_incremental_data(
//                             basic_columns,
//                             table_join_info,
//                             data_version,
//                             sql_scan_param_->get_data_version())))
      if (OB_SUCCESS != (ret = check_incremental_data_range(static_cast<int64_t>(table_id),
                                                            data_version, sql_scan_param_->get_data_version(), is_truncated /*mod zhaoqiong [Truncate Table]:20170519*/)))
      {
        TBSYS_LOG(WARN, "fail to check incremental data :ret[%d]", ret);
      }
      else if (OB_SUCCESS != (ret = need_incremental_data(
                             basic_columns,
                             table_join_info,
                             data_version,sql_scan_param_->get_data_version(), is_truncated /*mod zhaoqiong [Truncate Table]:20170519*/)))
      //mod:e
      {
        TBSYS_LOG(WARN, "fail to add ups operator:ret[%d]", ret);
      }
    }
    else
    {
      op_root_ = &op_sstable_scan_;
    }
  }

  // daily merge scan no need project operator,
  // there is no composite columns but plain column scan.
  // ObProject is container of query columns and set
  // ObSSTableScanParam put into ObSSTableScan.
  if (OB_SUCCESS == ret
      && basic_columns.count() != sql_scan_param_->get_project().get_output_columns().count()
      && sql_scan_param_->has_project())
  {
    op_project = &op_project_;
    if (OB_SUCCESS == ret)
    {
      op_project->assign(&sql_scan_param_->get_project());
      if (OB_SUCCESS != (ret = op_project->set_child(0, *op_root_)))
      {
        TBSYS_LOG(WARN, "fail to set project child. ret=%d", ret);
      }
      else
      {
        op_root_ = op_project;
      }
    }
  }

  // op_filter should be the father of op_project
  // select c1 + 1 as k, c2 where k > 100
  // c1 + 1 just created after op_project
  if (OB_SUCCESS == ret && sql_scan_param_->has_filter())
  {
    op_filter = &op_filter_;
    if (OB_SUCCESS == ret)
    {
      op_filter->assign(&sql_scan_param_->get_filter());
      if (OB_SUCCESS != (ret = op_filter->set_child(0, *op_root_)))
      {
        TBSYS_LOG(WARN, "fail to set filter child. ret=%d", ret);
      }
      else
      {
        op_root_ = op_filter;
      }
    }
  }

  if (OB_SUCCESS == ret
    && (sql_scan_param_->has_scalar_agg() || sql_scan_param_->has_group()))
  {
    if (sql_scan_param_->has_scalar_agg() && sql_scan_param_->has_group())
    {
      ret = OB_ERR_GEN_PLAN;
      TBSYS_LOG(WARN, "Group operator and scalar aggregate operator"
        " can not appear in TabletScan at the same time. ret=%d", ret);
    }
    else if (sql_scan_param_->has_scalar_agg())
    {
        op_scalar_agg_.assign(&sql_scan_param_->get_scalar_agg());
        //add gaojt [ListAgg][JHOBv0.1]20150109:b
        if(sql_scan_param_->has_listagg())
        {
            sort_for_listagg_.assign(&sql_scan_param_->get_listagg_columns_sort());
            if (OB_SUCCESS != (ret = sort_for_listagg_.set_child(0, *op_root_)))
            {
              TBSYS_LOG(WARN, "Fail to set child of scalar aggregate operator. ret=%d", ret);
            }
            else if (OB_SUCCESS != (ret = op_scalar_agg_.set_child(0, sort_for_listagg_)))
            {
              TBSYS_LOG(WARN, "Fail to set child of scalar aggregate operator. ret=%d", ret);
            }
            else
            {
              op_root_ = &op_scalar_agg_;
            }
        }
        else
        {
        /* add 20150104:e */
            if (OB_SUCCESS != (ret = op_scalar_agg_.set_child(0, *op_root_)))
            {
              TBSYS_LOG(WARN, "Fail to set child of scalar aggregate operator. ret=%d", ret);
            }
            else
            {
              op_root_ = &op_scalar_agg_;
            }
        }
      // add scalar aggregation
    }
    else if (sql_scan_param_->has_group())
    {
      // add group by
      if (!sql_scan_param_->has_group_columns_sort())
      {
        ret = OB_ERR_GEN_PLAN;
        TBSYS_LOG(WARN, "Physical plan error, group need a sort operator. ret=%d", ret);
      }
      else
      {
        op_group_columns_sort_.assign(&sql_scan_param_->get_group_columns_sort());
        op_group_.assign(&sql_scan_param_->get_group());
      }
      if (OB_UNLIKELY(OB_SUCCESS != ret))
      {
      }
      else if (!sql_scan_param_->is_indexed_group() && OB_SUCCESS != (ret = op_group_columns_sort_.set_child(0, *op_root_)))//mod liumz, [optimize group_order by index]20170419
      {
        TBSYS_LOG(WARN, "Fail to set child of sort operator. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = op_group_.set_child(0, sql_scan_param_->is_indexed_group()?*op_root_:op_group_columns_sort_)))//mod liumz, [optimize group_order by index]20170419
      {
        TBSYS_LOG(WARN, "Fail to set child of group operator. ret=%d", ret);
      }
      else
      {
        op_root_ = &op_group_;
      }
    }
  }
  if (OB_SUCCESS == ret && sql_scan_param_->has_limit())
  {
    op_limit = &op_limit_;
    if (OB_SUCCESS == ret)
    {
      op_limit->assign(&sql_scan_param_->get_limit());
      if (OB_SUCCESS != (ret = op_limit->set_child(0, *op_root_)))
      {
        TBSYS_LOG(WARN, "fail to set limit child. ret=%d", ret);
      }
      else
      {
        op_root_ = op_limit;
      }
    }
  }

  //release tablet
  if (OB_SUCCESS != ret)
  {
    int err = OB_SUCCESS;
    if (OB_SUCCESS != (err = op_sstable_scan_.close()))
    {
      TBSYS_LOG(WARN, "fail to close op sstable scan:ret[%d]", err);
    }
  }

  return ret;
}

int ObTabletScan::set_ups_scan_range(const ObNewRange &scan_range)
{
  ObNewRange range = scan_range;
  ObNewRange tablet_range;
  int rc = OB_SUCCESS;
  if (OB_SUCCESS != (rc = get_tablet_range(tablet_range)))
  {
    TBSYS_LOG(WARN, "get tablet range fail, rc %d", rc);
  }
  else
  {
    if (range.compare_with_startkey2(tablet_range) < 0)
    {
      range.start_key_ = tablet_range.start_key_;
      range.border_flag_.unset_inclusive_start();
      if (tablet_range.border_flag_.inclusive_start())
      {
        range.border_flag_.set_inclusive_start();
      }
      range.border_flag_.unset_min_value();
    }
    if (range.compare_with_endkey2(tablet_range) > 0)
    {
      range.end_key_ = tablet_range.end_key_;
      range.border_flag_.unset_inclusive_end();
      if (tablet_range.border_flag_.inclusive_end())
      {
        range.border_flag_.set_inclusive_end();
      }
      range.border_flag_.unset_max_value();
    }
    rc = op_ups_scan_.set_range(range);
  }
  return rc;
}

//mod zhaoqiong [Truncate Table]:20170519:b
//add zhaoqiong [Truncate Table]:20160318:b
//int ObTabletScan::check_incremental_data_range(int64_t table_id, int64_t start_data_version, int64_t end_data_version, ObVersionRange &range)
int ObTabletScan::check_incremental_data_range(int64_t table_id, int64_t start_data_version, int64_t end_data_version, bool &is_truncated)
//mod:e
{
  int ret = OB_SUCCESS;
  ObVersionRange version_range;

  if(OB_SUCCESS == ret)
  {
    version_range.start_version_ = ObVersion(start_data_version + 1);
    version_range.border_flag_.unset_min_value();
    version_range.border_flag_.set_inclusive_start();

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
    if (OB_SUCCESS != (ret = rpc_proxy_->check_incremental_data_range(table_id, version_range, is_truncated /*mod zhaoqiong [Truncate Table]:20170519*/)))
    {
      TBSYS_LOG(WARN, "failed to check incremental data range");
    }
  }
  return ret;
}
//add:e

