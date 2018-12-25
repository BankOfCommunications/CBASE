/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_read_param_modifier.cpp is for what ...
 *
 * Version: $id: ob_read_param_modifier.cpp,v 0.1 10/25/2010 6:23p wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#include "ob_read_param_modifier.h"
#include "common/ob_define.h"
#include "common/ob_range2.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

int oceanbase::mergeserver::get_next_param(const oceanbase::common::ObGetParam & org_param,
  const oceanbase::common::ObScanner & result,
  int64_t & got_cell, bool & finish,
  oceanbase::common::ObGetParam *get_param)
{
  ObReadParam *read_param = get_param;
  finish = false;
  int64_t item_count = 0;
  bool cur_req_finish = false;
  int64_t cell_size = org_param.get_cell_size();
  int ret = const_cast<ObScanner&>(result).get_is_req_fullfilled(cur_req_finish, item_count);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "get is req fullfilled failed:ret[%d]", ret);
  }
  // check the result cell size must be equal with fullfilled item count for get
  else if (result.get_cell_num() != item_count)
  {
    TBSYS_LOG(WARN, "check result cell size not coincident with fullfill num:"
      "size[%ld], fullfill[%ld]", result.get_cell_num(), item_count);
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    TBSYS_LOG(INFO, "get next param:finish[%d], total[%ld], org[%ld], cur_count[%ld], cur_finish[%d]",
      finish, got_cell, cell_size, item_count, cur_req_finish);
    got_cell += item_count;
    if (got_cell < cell_size)
    {
      finish = false;
    }
    else if ((got_cell == cell_size) && (true == cur_req_finish))
    {
      finish = true;
    }
    else
    {
      TBSYS_LOG(ERROR, "check cell size or finish status failed:total[%ld], org[%ld], "
        "cur_count[%ld], cur_finish[%d]", got_cell, cell_size, item_count, cur_req_finish);
      ret = OB_ERROR;
    }
  }

  if ((OB_SUCCESS == ret) && (false == finish) && (NULL != get_param))
  {
    get_param->reset();
    *read_param = org_param;
    const ObCellInfo * cell_info = NULL;
    int64_t count = got_cell;
    while (count < cell_size)
    {
      cell_info = org_param[count];
      if (NULL == cell_info)
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "check get param cell failed:cell[%ld], item[%ld], index[%ld]",
          cell_size, item_count, count);
        break;
      }
      /// add the cell to new get param
      ret = get_param->add_cell(*cell_info);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "add cell to new get param failed:cell[%ld], item[%ld], "
          "index[%ld], ret[%d]", cell_size, item_count, count, ret);
        if (OB_SIZE_OVERFLOW == ret)
        {
          if (NULL != get_param)
          {
            ret = get_param->rollback();
          }
          break;
        }
      }
      else
      {
        ++count;
      }
    }

    if (0 == get_param->get_cell_size())
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "check add any cell failed:cell[%ld], item[%ld], index[%ld], add[%ld]",
        cell_size, item_count, count, get_param->get_cell_size());
    }
  }
  return ret;
}

bool oceanbase::mergeserver::is_finish_scan(const ScanFlag::Direction scan_direction,  const ObNewRange & org_range,
  const ObNewRange & result_range)
{
  bool ret = false;
  if (ScanFlag::FORWARD == scan_direction)
  {
    if (result_range.compare_with_endkey2(org_range) >= 0)
    {
      ret = true;
    }
  }
  else
  {
    if (result_range.compare_with_startkey2(org_range) <= 0)
    {
      ret = true;
    }
  }
  return ret;
}

bool oceanbase::mergeserver::is_finish_scan(const ObScanParam & param, const ObNewRange & result_range)
{
  bool ret = false;
  if (ScanFlag::FORWARD == param.get_scan_direction())
  {
    if (result_range.compare_with_endkey2(*param.get_range()) >= 0)
    {
      ret = true;
    }
  }
  else
  {
    if (result_range.compare_with_startkey2(*param.get_range()) <= 0)
    {
      ret = true;
    }
  }
  return ret;
}

namespace
{
  int allocate_range_buffer(ObNewRange &range, ObMemBuffer &buffer)
  {
    int err = OB_SUCCESS;
    int64_t start_key_len = range.start_key_.get_deep_copy_size();
    int64_t end_key_len = range.end_key_.get_deep_copy_size();
    char* ptr = static_cast<char*>(buffer.malloc(start_key_len + end_key_len));
    if (NULL != ptr)
    {
      ObRawBufAllocatorWrapper start_key_wrapper(ptr, start_key_len);
      ObRawBufAllocatorWrapper end_key_wrapper(ptr + start_key_len, end_key_len);
      if((OB_SUCCESS == err) && (OB_SUCCESS != (err = range.start_key_.deep_copy(range.start_key_, start_key_wrapper))))
      {
        TBSYS_LOG(WARN, "fail to allocate memory to copy start_key");
      }

      if((OB_SUCCESS == err) && (OB_SUCCESS != (err = range.end_key_.deep_copy(range.end_key_, end_key_wrapper))))
      {
        TBSYS_LOG(WARN, "fail to allocate memory to copy end_key");
      }
    }
    return err;
  }
}

int oceanbase::mergeserver::get_next_param(const ObScanParam &org_scan_param,
  const oceanbase::common::ObScanner &prev_scan_result,
  ObScanParam *scan_param, ObMemBuffer &range_buffer)
{
  int err = OB_SUCCESS;
  const ObReadParam &org_read_param = org_scan_param;
  ObReadParam *read_param = scan_param;
  ObNewRange tablet_range;
  const ObNewRange *org_scan_range = NULL;
  ObNewRange cur_range;
  if(NULL != scan_param)
  {
    scan_param->reset();
    if ( NULL != org_scan_param.get_location_info())
    {
      scan_param->set_location_info(*(org_scan_param.get_location_info()));
    }
    *read_param = org_read_param;
  }
  bool request_fullfilled = false;
  int64_t fullfilled_row_num = 0;
  err = prev_scan_result.get_is_req_fullfilled(request_fullfilled,fullfilled_row_num);
  if (OB_SUCCESS == err)
  {
    org_scan_range = org_scan_param.get_range();
    if (NULL == org_scan_range)
    {
      TBSYS_LOG(WARN, "%s", "unexpected error, can't get range from org scan param");
      err  = OB_INVALID_ARGUMENT;
    }
  }

  ObRowkey last_row_key;
  if (OB_SUCCESS == err)
  {
    if (request_fullfilled)
    {
      err = prev_scan_result.get_range(tablet_range);
      if (OB_SUCCESS == err)
      {
        if (true == is_finish_scan(org_scan_param, tablet_range))
        {
          err = OB_ITER_END;
        }
        else if (ScanFlag::FORWARD == org_scan_param.get_scan_direction())
        {
          last_row_key = tablet_range.end_key_;
        }
        else
        {
          last_row_key = tablet_range.start_key_;
        }
      }
    }
    else
    {
      err = prev_scan_result.get_last_row_key(last_row_key);
      if (OB_ENTRY_NOT_EXIST == err)
      {
        /// first time, remain unchanged
      }
      else if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "%s", "unexpected error, scanner should contain at least one cell");
      }
    }
  }

  if (OB_SUCCESS == err && NULL != scan_param)
  {
    cur_range =*org_scan_range;
    // forward
    if (ScanFlag::FORWARD == org_scan_param.get_scan_direction())
    {
      cur_range.start_key_ = last_row_key;
      cur_range.border_flag_.unset_inclusive_start();
    }
    else
    {
      cur_range.end_key_ = last_row_key;
      if (request_fullfilled)
      {
        // tablet range start key
        cur_range.border_flag_.set_inclusive_end();
      }
      else
      {
        // the smallest rowkey
        cur_range.border_flag_.unset_inclusive_end();
      }
    }
  }

  // first time remain unchanged
  if (OB_ENTRY_NOT_EXIST == err)
  {
    cur_range =*org_scan_range;
    err = OB_SUCCESS;
  }

  if (OB_SUCCESS == err && NULL != scan_param)
  {
    err = allocate_range_buffer(cur_range, range_buffer);
  }

  if (OB_SUCCESS == err && NULL != scan_param)
  {
    scan_param->set(org_scan_param.get_table_id(), org_scan_param.get_table_name(), cur_range);
    for (int32_t cell_idx = 0;
      cell_idx < org_scan_param.get_column_id_size() && OB_SUCCESS == err;
      cell_idx ++)
    {
      err = scan_param->add_column(org_scan_param.get_column_id()[cell_idx]);
    }
    if (OB_SUCCESS == err)
    {
      scan_param->set_scan_flag(org_scan_param.get_scan_flag());
    }
  }
  return err;
}

int get_next_range_for_trivail_scan(const ObNewRange &org_scan_range,
  const ObScanner &prev_scan_result,
  const ScanFlag::Direction scan_direction,
  ObNewRange &cur_range)
{
  int err = OB_SUCCESS;
  ObNewRange tablet_range;
  bool request_fullfilled = false;
  int64_t fullfilled_row_num = 0;
  err = prev_scan_result.get_is_req_fullfilled(request_fullfilled,fullfilled_row_num);

  ObRowkey last_row_key;
  if (OB_SUCCESS == err)
  {
    if (request_fullfilled)
    {
      err = prev_scan_result.get_range(tablet_range);
      if (OB_SUCCESS == err)
      {
        if (true == is_finish_scan(scan_direction, org_scan_range, tablet_range))
        {
          err = OB_ITER_END;
        }
        else if (ScanFlag::FORWARD == scan_direction)
        {
          last_row_key = tablet_range.end_key_;
        }
        else
        {
          last_row_key = tablet_range.start_key_;
        }
      }
    }
    else
    {
      if (OB_SUCCESS != (err = prev_scan_result.get_last_row_key(last_row_key)))
      {
        TBSYS_LOG(WARN,"fail to get last rowkey from prev result [err:%d]", err);
      }
    }
  }

  if (OB_SUCCESS == err)
  {
    cur_range = org_scan_range;
    // forward
    if (ScanFlag::FORWARD == scan_direction)
    {
      cur_range.start_key_ = last_row_key;
      cur_range.border_flag_.unset_inclusive_start();
    }
    else
    {
      cur_range.end_key_ = last_row_key;
      if (request_fullfilled)
      {
        // tablet range start key
        cur_range.border_flag_.set_inclusive_end();
      }
      else
      {
        // the smallest rowkey
        cur_range.border_flag_.unset_inclusive_end();
      }
    }
  }
  return err;
}

int oceanbase::mergeserver::get_next_range(const oceanbase::common::ObScanParam &org_scan_param,
  const ObScanner &prev_scan_result,
  const int64_t prev_limit_offset,
  ObNewRange &cur_range,
  int64_t & cur_limit_offset,
  ObStringBuf &buf)
{
  int err = OB_SUCCESS;
  cur_limit_offset = 0;
  cur_range.reset();
  if ((OB_SUCCESS == err)
    && (org_scan_param.get_group_by_param().get_aggregate_row_width() > 0 )
    && (org_scan_param.get_orderby_column_size() <= 0))
  {
    TBSYS_LOG(WARN,"argument error [org_scan_param.get_group_by_param().get_aggregate_row_width():%ld,"
      "org_scan_param.get_orderby_column_size():%ld]", org_scan_param.get_group_by_param().get_aggregate_row_width(),
      org_scan_param.get_orderby_column_size());
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err)
    && (org_scan_param.get_scan_direction() == ScanFlag::BACKWARD)
    && ((org_scan_param.get_orderby_column_size() > 0) || (org_scan_param.get_group_by_param().get_aggregate_row_width() > 0)))
  {
    TBSYS_LOG(WARN,"argument error, backward scan are limited only for backward page browsing, "
      "it doesn't support groupby or orderby [org_scan_param.get_group_by_param().get_aggregate_row_width():%ld,"
      "org_scan_param.get_orderby_column_size():%ld]", org_scan_param.get_group_by_param().get_aggregate_row_width(),
      org_scan_param.get_orderby_column_size());
    err = OB_INVALID_ARGUMENT;
  }
  ObNewRange tablet_range;
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = prev_scan_result.get_range(tablet_range))))
  {
    TBSYS_LOG(WARN,"fail to get tablet range from prev result [err:%d]", err);
  }
  if ((OB_SUCCESS == err)  && (0 < prev_limit_offset))
  {
    /// check tablet split
    if ((tablet_range.compare_with_startkey2(*org_scan_param.get_range()) > 0)
      || (tablet_range.compare_with_endkey2(*org_scan_param.get_range()) < 0))
    {
      TBSYS_LOG(WARN,"cs tablet splitted during the whole request, please try this request again");
      err = OB_NEED_RETRY;
    }
  }

  if ((OB_SUCCESS == err) && (org_scan_param.get_orderby_column_size() <= 0))
  {
    err = get_next_range_for_trivail_scan(*org_scan_param.get_range(),prev_scan_result,
      org_scan_param.get_scan_direction(),cur_range);
    if ((OB_SUCCESS != err) && (OB_ITER_END != err))
    {
      TBSYS_LOG(WARN,"fail to get next range [err:%d]", err);
    }
  }
  else if ((OB_SUCCESS == err) && (org_scan_param.get_orderby_column_size() > 0))
  {
    int64_t fullfilled_item_num = 0;
    bool is_fullfill = false;
    if (OB_SUCCESS != (err = prev_scan_result.get_is_req_fullfilled(is_fullfill,fullfilled_item_num)))
    {
      TBSYS_LOG(WARN,"fail to get fullfill info from prev result [err:%d]", err);
    }
    if ((OB_SUCCESS == err) && (fullfilled_item_num != prev_scan_result.get_row_num()))
    {
      /// TBSYS_LOG(ERROR,"unexpected error [prev_result::fullfilled_item_num:%ld, prev_result::get_row_num():%ld]",
      ///   fullfilled_item_num, prev_scan_result.get_row_num());
      /// err = OB_ERR_UNEXPECTED;
      fullfilled_item_num = prev_scan_result.get_row_num();
    }
    if ((OB_SUCCESS == err) && is_fullfill)
    {
      if (is_finish_scan(org_scan_param.get_scan_direction(),*org_scan_param.get_range(),tablet_range))
      {
        err = OB_ITER_END;
      }
      else
      {
        cur_range = *org_scan_param.get_range();
        cur_range.start_key_ = tablet_range.end_key_;
        cur_range.border_flag_.unset_inclusive_start();
      }
    }
    if ((OB_SUCCESS == err) && !is_fullfill)
    {
      cur_range = *org_scan_param.get_range();
      cur_limit_offset = prev_limit_offset + fullfilled_item_num;
    }
  }
  if (OB_SUCCESS == err)
  {
    ObRowkey str;
    str = cur_range.start_key_;
    if ((OB_SUCCESS == err) && (str.length() > 0))
    {
      if (OB_SUCCESS != (err = buf.write_string(str,&(cur_range.start_key_))))
      {
        TBSYS_LOG(WARN,"fail to deep copy cur_range.start_key_ [err:%d]", err);
      }
    }
    str = cur_range.end_key_;
    if ((OB_SUCCESS == err) && (str.length() > 0))
    {
      if (OB_SUCCESS != (err = buf.write_string(str,&(cur_range.end_key_))))
      {
        TBSYS_LOG(WARN,"fail to deep copy cur_range.end_key_ [err:%d]", err);
      }
    }
  }
  return err;
}



////// for sql scan //////////////
int get_next_range_for_trivail_scan(const ObNewRange &org_scan_range,
  const ObNewScanner &prev_scan_result,
  const ScanFlag::Direction scan_direction,
  ObNewRange &cur_range)
{
  int err = OB_SUCCESS;
  ObNewRange tablet_range;
  bool request_fullfilled = false;
  int64_t fullfilled_row_num = 0;
  err = prev_scan_result.get_is_req_fullfilled(request_fullfilled,fullfilled_row_num);

  ObRowkey last_row_key;
  if (OB_SUCCESS == err)
  {
    if (request_fullfilled)
    {
      err = prev_scan_result.get_range(tablet_range);
      if (OB_SUCCESS == err)
      {
        if (true == is_finish_scan(scan_direction, org_scan_range, tablet_range))
        {
          err = OB_ITER_END;
        }
        else
        {
          last_row_key = tablet_range.end_key_;
        }
      }
    }
    else
    {
      if (OB_SUCCESS != (err = prev_scan_result.get_last_row_key(last_row_key)))
      {
        TBSYS_LOG(WARN,"fail to get last rowkey from prev result [err:%d]", err);
      }
    }
  }

  if (OB_SUCCESS == err)
  {
    cur_range = org_scan_range;
    // forward
    cur_range.start_key_ = last_row_key;
    cur_range.border_flag_.unset_inclusive_start();
  }
  return err;
}

int oceanbase::mergeserver::get_next_range(const oceanbase::sql::ObSqlScanParam &org_scan_param,
  const ObNewScanner &prev_scan_result,
  const int64_t prev_limit_offset,
  ObNewRange &cur_range,
  int64_t & cur_limit_offset,
  ObStringBuf &buf)
{
  int err = OB_SUCCESS;
  cur_limit_offset = 0;
  cur_range.reset();
  ObNewRange tablet_range;
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = prev_scan_result.get_range(tablet_range))))
  {
    TBSYS_LOG(WARN,"fail to get tablet range from prev result [err:%d]", err);
  }
  if ((OB_SUCCESS == err)  && (0 < prev_limit_offset))
  {
    /// check tablet split
    if ((tablet_range.compare_with_startkey2(*org_scan_param.get_range()) > 0)
      || (tablet_range.compare_with_endkey2(*org_scan_param.get_range()) < 0))
    {
      TBSYS_LOG(WARN,"cs tablet splitted during the whole request, please try this request again");
      err = OB_NEED_RETRY;
    }
  }

  if (OB_SUCCESS == err)
  {
    err = get_next_range_for_trivail_scan(*org_scan_param.get_range(),prev_scan_result,
      org_scan_param.get_scan_direction(),cur_range);
    if ((OB_SUCCESS != err) && (OB_ITER_END != err))
    {
      TBSYS_LOG(WARN,"fail to get next range [err:%d]", err);
    }
  }
  if (OB_SUCCESS == err)
  {
    ObRowkey str;
    str = cur_range.start_key_;
    if ((OB_SUCCESS == err) && (str.length() > 0))
    {
      if (OB_SUCCESS != (err = buf.write_string(str,&(cur_range.start_key_))))
      {
        TBSYS_LOG(WARN,"fail to deep copy cur_range.start_key_ [err:%d]", err);
      }
    }
    str = cur_range.end_key_;
    if ((OB_SUCCESS == err) && (str.length() > 0))
    {
      if (OB_SUCCESS != (err = buf.write_string(str,&(cur_range.end_key_))))
      {
        TBSYS_LOG(WARN,"fail to deep copy cur_range.end_key_ [err:%d]", err);
      }
    }
  }
  return err;
}
