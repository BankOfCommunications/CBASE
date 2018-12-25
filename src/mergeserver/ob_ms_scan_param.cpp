/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_ms_scan_param.cpp for 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#include "ob_ms_scan_param.h"
#include <math.h>
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;


int oceanbase::mergeserver::ObMergerScanParam::set_param(ObScanParam & param)
{
  int err = OB_SUCCESS;
  int64_t limit_offset  = 0;
  int64_t limit_count  = 0;
  int64_t sharding_minimum = 0;
  double topk_precision  = 0;
  decoded_org_param_ = &param;
  ms_scan_param_.reset();
  if (OB_SUCCESS == err 
    && (OB_SUCCESS != (err = ms_scan_param_.set(decoded_org_param_->get_table_id(),ObString(), *decoded_org_param_->get_range()))))
  {
    TBSYS_LOG(WARN,"fail to set merge server scan param [err:%d]", err);
  }
  if (OB_SUCCESS == err)
  {
    /// scan direction
    ms_scan_param_.set_scan_direction(decoded_org_param_->get_scan_direction());
    /// limit info
    decoded_org_param_->get_limit_info(limit_offset, limit_count);
    decoded_org_param_->get_topk_precision(sharding_minimum, topk_precision);
    const double DOUBLE_EPSINON = 1e-14;
    if ((0 == topk_precision) || (fabsl(topk_precision - 1) < DOUBLE_EPSINON) || (topk_precision > 1))
    {
      sharding_minimum = 0;
      topk_precision = 0;
    }
    ms_scan_param_.set_limit_info(limit_offset,limit_count);
    ms_scan_param_.set_topk_precision(0,0);
  }

  /// select::basic columns
  int32_t return_info_idx = 0;
  const uint64_t *basic_columns = decoded_org_param_->get_column_id();
  const ObArrayHelpers<bool> & select_return_infos = decoded_org_param_->get_return_infos();
  for (int32_t i = 0; 
    (i < decoded_org_param_->get_column_id_size())&& (OB_SUCCESS == err);
    i++, return_info_idx ++)
  {
    bool is_return = false;
    is_return = *select_return_infos.at(return_info_idx);
    if (OB_SUCCESS != (err = ms_scan_param_.add_column(basic_columns[i],is_return)))
    {
      TBSYS_LOG(WARN,"fail to add basic clumn [err:%d]", err);
    }
  }

  /// select::composite columns
  for (int32_t i = 0; 
    (i < decoded_org_param_->get_composite_columns().get_array_index()) && (OB_SUCCESS == err);
    i++, return_info_idx ++)
  {
    bool is_return = false;
    is_return = *select_return_infos.at(return_info_idx);
    const ObObj *post_expr = NULL;
    post_expr = decoded_org_param_->get_composite_columns().at(i)->get_postfix_expr();
    if (OB_SUCCESS != (err = ms_scan_param_.add_column(post_expr, is_return)))
    {
      TBSYS_LOG(WARN,"fail to add aggregate column [err:%d]", err);
    }
  }
  /// there is no need to copy where conditions, where conditions will be processed on cs

  /// groupby::groupby columns
  return_info_idx = 0;
  const ObArrayHelpers<bool> & return_infos =  decoded_org_param_->get_group_by_param().get_return_infos();

  for (int32_t i = 0; 
    (i < decoded_org_param_->get_group_by_param().get_groupby_columns().get_array_index()) && (OB_SUCCESS == err);
    i++, return_info_idx ++)
  {
    bool is_return = false;
    is_return = *return_infos.at(return_info_idx);
    if (OB_SUCCESS != (err = ms_scan_param_.get_group_by_param().add_groupby_column(return_info_idx, is_return)))
    {
      TBSYS_LOG(WARN,"fail to add groupby column [err:%d]", err);
    }
  }

  /// groupby::return columns
  for (int32_t i = 0; 
    (i < decoded_org_param_->get_group_by_param().get_return_columns().get_array_index()) && (OB_SUCCESS == err);
    i++, return_info_idx ++)
  {
    bool is_return = false;
    is_return = *return_infos.at(return_info_idx);
    if (OB_SUCCESS != (err = ms_scan_param_.get_group_by_param().add_return_column(return_info_idx, is_return)))
    {
      TBSYS_LOG(WARN,"fail to add return column [err:%d]", err);
    }
  }


  /// groupby::aggregate columns
  for (int32_t i = 0; 
    (i < decoded_org_param_->get_group_by_param().get_aggregate_columns().get_array_index()) && (OB_SUCCESS == err);
    i++, return_info_idx ++)
  {
    bool is_return = false;
    ObAggregateFuncType func = decoded_org_param_->get_group_by_param().get_aggregate_columns().at(i)->get_func_type();
    is_return = *return_infos.at(return_info_idx);
    if (func == COUNT)
    {
      func = SUM;
    }
    if (OB_SUCCESS != (err = ms_scan_param_.get_group_by_param().add_aggregate_column(return_info_idx, func, is_return)))
    {
      TBSYS_LOG(WARN,"fail to add aggregate column [err:%d]", err);
    }
  }

  /// groupby::composite columns
  for (int32_t i = 0; 
    (i < decoded_org_param_->get_group_by_param().get_composite_columns().get_array_index()) && (OB_SUCCESS == err);
    i++, return_info_idx ++)
  {
    bool is_return = false;
    is_return = *return_infos.at(return_info_idx);
    const ObObj *post_expr = NULL;
    post_expr = decoded_org_param_->get_group_by_param().get_composite_columns().at(i)->get_postfix_expr();
    if (OB_SUCCESS != (err = ms_scan_param_.get_group_by_param().add_column(post_expr, is_return)))
    {
      TBSYS_LOG(WARN,"fail to add aggregate column [err:%d]", err);
    }
  }

  /// groupby::having conditions
  if (OB_SUCCESS == err)
  {
    ms_scan_param_.get_group_by_param().get_having_condition().safe_copy(decoded_org_param_->get_group_by_param().get_having_condition());
    decoded_org_param_->get_group_by_param().get_having_condition().reset();
  }

  /// orderby infos
  int64_t orderby_column_size = 0;
  const int64_t *orderby_column_idxs_ = NULL;
  const uint8_t *orderby_orders_ = NULL;
  decoded_org_param_->get_orderby_column(orderby_column_idxs_,orderby_orders_, orderby_column_size);
  for (int32_t i = 0; (i < orderby_column_size) && (OB_SUCCESS == err); i++)
  {
    ObScanParam::Order order = static_cast<ObScanParam::Order>(orderby_orders_[i]);
    if (OB_SUCCESS != (err = ms_scan_param_.add_orderby_column(orderby_column_idxs_[i],order)))
    {
      TBSYS_LOG(WARN,"fail to add orderby info to ms scan param [err:%d,idx:%d]", err, i);
    }
  }


  if (decoded_org_param_->get_group_by_param().get_aggregate_row_width() > 0)
  {
    decoded_org_param_->set_limit_info(0,0);
    decoded_org_param_->set_topk_precision(sharding_minimum,topk_precision);
  }
  else
  {
    /// no need to do topk precision, because there is no groupby
    decoded_org_param_->set_limit_info(0,limit_offset + limit_count);
    decoded_org_param_->set_topk_precision(0,0);
  }

  /// let cs return all columns
  if (OB_SUCCESS == err)
  {
    decoded_org_param_->set_all_column_return();
  }

  /// add orderby info in cs_scan_param
  if (OB_SUCCESS == err)
  {
    /// if there is group by in query, and there is no order by. 
    /// and if a tablet's result cann't be returned in a single response
    /// we need some strategy to get the rest result
    /// here we add an orderby info into cs's scan param, so we can change limit offset and limit count to get
    /// the reset result
    if ((decoded_org_param_->get_group_by_param().get_aggregate_row_width() > 0)
      && (decoded_org_param_->get_orderby_column_size() <= 0))
    {
      if (OB_SUCCESS != (err = decoded_org_param_->add_orderby_column(0)))
      {
        TBSYS_LOG(WARN,"fail to add orderby info to cs scan param [err:%d]", err);
      }
    }
  }

  return err;
}
