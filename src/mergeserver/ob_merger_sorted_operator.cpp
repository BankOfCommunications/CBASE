/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_merger_sorted_operator.cpp for
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#include "ob_merger_sorted_operator.h"
#include <algorithm>
#include "common/ob_scan_param.h"
#include "common/ob_scanner.h"
#include "common/ob_range.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace std;

void ObMergerSortedOperator::sharding_result_t::init(ObScanner & sharding_res, const ObNewRange & query_range,
  const ObScanParam &param, ObRowkey & last_process_rowkey, const int64_t fullfilled_item_num)
{
  sharding_res_ = &sharding_res;
  sharding_query_range_ = &query_range;
  param_ = &param;
  last_row_key_ = last_process_rowkey;
  fullfilled_item_num_ = fullfilled_item_num;
}

bool ObMergerSortedOperator::sharding_result_t::operator <(const sharding_result_t &other) const
{
  bool res = false;
  if (param_->get_scan_direction() == ScanFlag::BACKWARD)
  {
    res = (sharding_query_range_->compare_with_endkey2(*other.sharding_query_range_) > 0);
  }
  else
  {
    res = (sharding_query_range_->compare_with_startkey2(*other.sharding_query_range_) < 0);
  }
  return res;
}

ObMergerSortedOperator::ObMergerSortedOperator()
{
  reset();
}


ObMergerSortedOperator::~ObMergerSortedOperator()
{
}

void ObMergerSortedOperator::reset()
{
  sharding_result_count_ = 0;
  cur_sharding_result_idx_ = -1;
  scan_param_ = NULL;
  seamless_result_count_  = 0;
}

int ObMergerSortedOperator::set_param(const ObScanParam & scan_param)
{
  int err = OB_SUCCESS;
  reset();
  scan_param_ = &scan_param;
  scan_range_ = *scan_param_->get_range();
  return err;
}

void ObMergerSortedOperator::sort(bool &is_finish, ObScanner * last_sharding_res)
{
  int64_t result_size = 0;
  bool seamless = true;
  int64_t seamless_result_idx = 0;
  int64_t seamless_row_count = 0;
  std::sort(sharding_result_arr_, sharding_result_arr_ + sharding_result_count_);
  if ((ScanFlag::FORWARD == scan_param_->get_scan_direction())
    && (sharding_result_arr_[0].sharding_query_range_->start_key_  != scan_range_.start_key_)
    && (!sharding_result_arr_[0].sharding_query_range_->start_key_.is_min_row()))
  {
    seamless = false;
  }
  else if ((ScanFlag::BACKWARD == scan_param_->get_scan_direction())
    && (sharding_result_arr_[0].sharding_query_range_->end_key_  != scan_range_.end_key_)
    && (!sharding_result_arr_[0].sharding_query_range_->end_key_.is_max_row()))
  {
    seamless = false;
  }
  int64_t limit_offset = 0;
  int64_t limit_count = 0;
  scan_param_->get_limit_info(limit_offset, limit_count);
  if (seamless)
  {
    seamless_result_count_  = 1;
    seamless_row_count += sharding_result_arr_[0].fullfilled_item_num_;
    if (seamless_row_count > limit_offset)
    {
      result_size += sharding_result_arr_[0].sharding_res_->get_serialize_size();
    }
  }
  for (seamless_result_idx = 0;
    (seamless_result_idx < sharding_result_count_ - 1) && (seamless);
    seamless_result_idx++)
  {
    if ((ScanFlag::FORWARD == scan_param_->get_scan_direction())
      && (sharding_result_arr_[seamless_result_idx].last_row_key_
      != sharding_result_arr_[seamless_result_idx+1].sharding_query_range_->start_key_))
    {
      seamless = false;
    }
    else if ((ScanFlag::BACKWARD == scan_param_->get_scan_direction())
      && (sharding_result_arr_[seamless_result_idx].last_row_key_
      != sharding_result_arr_[seamless_result_idx+1].sharding_query_range_->end_key_))
    {
      seamless = false;
    }
    if (seamless)
    {
      seamless_result_count_ ++;
      seamless_row_count += sharding_result_arr_[seamless_result_idx + 1].fullfilled_item_num_;
      if (seamless_row_count > limit_offset)
      {
        result_size += sharding_result_arr_[seamless_result_idx + 1].sharding_res_->get_serialize_size();
      }
    }
  }

  if ((limit_count > 0) || (limit_offset > 0))
  {
    is_finish = (seamless_row_count >= limit_offset + limit_count);
  }
  else
  {
    bool is_fullfilled = true;
    bool last_scanner_is_fullfilled = true;
    int64_t item_num = 0;
    int ret = OB_SUCCESS;
    if ((NULL != last_sharding_res) && (OB_SUCCESS != (last_sharding_res->get_is_req_fullfilled(is_fullfilled,item_num))))
    {
      is_fullfilled = true;
    }
    // if go ObMergerSortedOperator, means no groupby and orderby
    if (scan_range_.is_whole_range())
    {
      /* the  (!is_fullfilled) condition assures that scan returns at least 2MB data to client.
       * only if CS got more than 2MB result that is_fullfilled will be set to false at first transmition
       */
      is_finish = (result_size >= OB_MAX_PACKET_LENGTH - FULL_SCANNER_RESERVED_BYTE_COUNT) ||
        ((sharding_result_arr_[seamless_result_count_-1].sharding_res_ == last_sharding_res) && !is_fullfilled);
    }
    else
    {
      if ((sharding_result_arr_[0].sharding_query_range_->start_key_ == scan_range_.start_key_)
          && (seamless_result_count_ > 0
            && sharding_result_arr_[seamless_result_count_ - 1].sharding_query_range_->end_key_ == scan_range_.end_key_))
      {
        if (OB_SUCCESS != (ret = sharding_result_arr_[seamless_result_count_ - 1].sharding_res_->get_is_req_fullfilled(
              last_scanner_is_fullfilled, item_num)))
        {
          TBSYS_LOG(ERROR, "fail to get fullfilled flag from sharding result.ret=%d", ret);
          is_finish = true; // default. no better choice
        }
        else if (true == last_scanner_is_fullfilled)
        {
          ObNewRange scanner_range;
          if (OB_SUCCESS != (ret =sharding_result_arr_[seamless_result_count_ - 1].sharding_res_->get_range(scanner_range)))
          {
            TBSYS_LOG(ERROR, "fail to get range");
            is_finish= true;
          }
          else if (scanner_range.end_key_ < scan_range_.end_key_)
          {
            is_finish = false;
          }
          else
          {
            is_finish = true;
          }
        }
        else
        {
          is_finish = false;
        }
      }
      else
      {
        is_finish = false;
      }
    }
  }
}

int ObMergerSortedOperator::add_sharding_result(ObScanner & sharding_res, const ObNewRange &query_range,
  bool &is_finish)
{
  int err = OB_SUCCESS;
  if ((OB_SUCCESS == err) && (NULL == scan_param_))
  {
    TBSYS_LOG(WARN,"operator was not initialized yet [scan_param_:%p]", scan_param_);
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && (sharding_result_count_ >= MAX_SHARDING_RESULT_COUNT))
  {
    TBSYS_LOG(WARN,"array is full [MAX_SHARDING_RESULT_COUNT:%ld,sharding_result_count_:%ld]",
      MAX_SHARDING_RESULT_COUNT, sharding_result_count_);
    err = OB_ARRAY_OUT_OF_RANGE;
  }
  /// @todo (wushi wushi.ly@taobao.com) check fullfill item number and size property
  bool is_fullfilled = false;
  int64_t fullfilled_item_num = 0;
  ObRowkey last_row_key;
  ObNewRange cs_tablet_range;
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = sharding_res.get_is_req_fullfilled(is_fullfilled,fullfilled_item_num))))
  {
    TBSYS_LOG(WARN,"fail to get fullfilled info from sharding result [err:%d]", err);
  }
  if (OB_SUCCESS == err)
  {
    if (OB_SUCCESS == (err = sharding_res.get_last_row_key(last_row_key)))
    {
    }
    else if (OB_ENTRY_NOT_EXIST == err)
    {
      err = OB_SUCCESS;
    }
    else
    {
      TBSYS_LOG(WARN,"fail to get last rowkey from sharding result [err:%d]", err);
    }
  }

  if (OB_SUCCESS == err)
  {
    if (is_fullfilled)
    {
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = sharding_res.get_range(cs_tablet_range))))
      {
        TBSYS_LOG(WARN,"fail to get tablet range from sharding result [err:%d]", err);
      }
      if (OB_SUCCESS == err)
      {
        if (ScanFlag::FORWARD == scan_param_->get_scan_direction())
        {
          last_row_key = cs_tablet_range.end_key_;
        }
        else
        {
          last_row_key = cs_tablet_range.start_key_;
        }
      }
    }
    if (OB_SUCCESS == err)
    {
      fullfilled_item_num = sharding_res.get_row_num();
      sharding_result_arr_[sharding_result_count_].init(sharding_res,query_range,*scan_param_, last_row_key, fullfilled_item_num);
      sharding_result_count_ ++;
    }
  }
  is_finish = false;
  if (OB_SUCCESS == err)
  {
    sort(is_finish, &sharding_res);
  }
  TBSYS_LOG(DEBUG, "add sharding result. is_finish=%d, err=%d", is_finish, err);
  return err;
}

int ObMergerSortedOperator::seal()
{
  int err = OB_SUCCESS;
  if ((OB_SUCCESS == err) && (NULL == scan_param_))
  {
    TBSYS_LOG(WARN,"operator was not initialized yet [scan_param_:%p]", scan_param_);
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && (seamless_result_count_ <= 0) && (sharding_result_count_ > 0))
  {
    bool is_finish = false;
    sort(is_finish);
  }
  return err;
}


int ObMergerSortedOperator::next_cell()
{
  volatile int err = OB_SUCCESS;
  if ((OB_SUCCESS == err) && (cur_sharding_result_idx_ < 0))
  {
    cur_sharding_result_idx_ = 0;
  }
  while (OB_SUCCESS == err)
  {
    if (cur_sharding_result_idx_ >= seamless_result_count_)
    {
      err = OB_ITER_END;
    }
    if (OB_SUCCESS == err)
    {
      if (OB_SUCCESS ==(err = sharding_result_arr_[cur_sharding_result_idx_].sharding_res_->next_cell()))
      {
        break;
      }
      else if (OB_ITER_END == err)
      {
        err = OB_SUCCESS;
        cur_sharding_result_idx_ ++;
      }
      else
      {
        TBSYS_LOG(WARN,"fail to get next cell from ObScanner [idx:%ld,err:%d]", cur_sharding_result_idx_, err);
      }
    }
  }
  return err;
}

int ObMergerSortedOperator::get_cell(ObInnerCellInfo ** cell, bool *is_row_changed)
{
  int err = OB_SUCCESS;
  if (NULL == cell)
  {
    TBSYS_LOG(WARN,"invalid argument [cell:%p]", cell);
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && (cur_sharding_result_idx_ < 0))
  {
    TBSYS_LOG(WARN,"call next_cell first [cur_sharding_result_idx_:%ld]", cur_sharding_result_idx_);
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && (cur_sharding_result_idx_>= seamless_result_count_))
  {
    err = OB_ITER_END;
  }
  if (OB_SUCCESS == err)
  {
    if (OB_SUCCESS == (err = sharding_result_arr_[cur_sharding_result_idx_].sharding_res_->get_cell(cell,is_row_changed)))
    {
    }
    else if (OB_ITER_END == err)
    {
    }
    else
    {
      TBSYS_LOG(WARN,"fail to get cell from ObScanner [idx:%ld,err:%d]", cur_sharding_result_idx_, err);
    }
  }
  return err;
}

int ObMergerSortedOperator::get_cell(ObInnerCellInfo ** cell)
{
  return get_cell(cell,NULL);
}
