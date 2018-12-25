
#include "ob_ups_scan.h"
#include "common/utility.h"
#include "common/ob_profile_log.h"

using namespace oceanbase;
using namespace sql;

void ObUpsScan::reset()
{
  cur_scan_param_.reset();
  row_desc_.reset();
}

void ObUpsScan::reuse()
{
  cur_scan_param_.reset();
  row_desc_.reset();
}

int ObUpsScan::open()
{
  int ret = OB_SUCCESS;
  row_counter_ = 0;
  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }
  else
  {
    cur_scan_param_.set_is_read_consistency(is_read_consistency_);

    if(OB_SUCCESS != (ret = fetch_next(true)))
    {
      TBSYS_LOG(WARN, "fetch row fail:ret[%d]", ret);
    }
    else
    {
      cur_ups_row_.set_row_desc(row_desc_);
    }
  }
  return ret;
}

int ObUpsScan::get_next_scan_param(const ObRowkey &last_rowkey, ObScanParam &scan_param)
{
  int ret = OB_SUCCESS;

  ObNewRange next_range = *(scan_param.get_range());

  next_range.start_key_ = last_rowkey;
  next_range.border_flag_.unset_inclusive_start();

  if(OB_SUCCESS != (ret = scan_param.set_range(next_range)))
  {
    TBSYS_LOG(WARN, "scan param set range fail:ret[%d]", ret);
  }

  return ret;
}

int ObUpsScan::add_column(const uint64_t &column_id)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS != (ret = cur_scan_param_.add_column(column_id)))
  {
    TBSYS_LOG(WARN, "add column id fail:ret[%d] column_id[%lu]", ret, column_id);
  }
  else if(OB_INVALID_ID == range_.table_id_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "set range first");
  }
  else if(OB_SUCCESS != (ret = row_desc_.add_column_desc(range_.table_id_, column_id)))
  {
    TBSYS_LOG(WARN, "add column desc fail:ret[%d]", ret);
  }
  return ret;
}

int ObUpsScan::close()
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(DEBUG, "ups scan row count=%ld", row_counter_);
  return ret;
}

int ObUpsScan::get_next_row(const common::ObRowkey *&rowkey, const ObRow *&row)
{
  int ret = OB_SUCCESS;
  bool is_fullfilled = false;
  int64_t fullfilled_item_num = 0;

  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }

  while(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = cur_new_scanner_.get_is_req_fullfilled(is_fullfilled, fullfilled_item_num)))
    {
      TBSYS_LOG(WARN, "get is req fullfilled fail:ret[%d]", ret);
    }
    else
    {
      ret = cur_new_scanner_.get_next_row(rowkey, cur_ups_row_);
      if(OB_ITER_END == ret )
      {
        TBSYS_LOG(DEBUG, "ups scanner is_fullfilled[%s]", is_fullfilled ? "TRUE" : "FALSE");
        if(is_fullfilled)
        {
          break;
        }
        else
        {
          if(OB_SUCCESS != (ret = fetch_next(false)))
          {
            TBSYS_LOG(WARN, "fetch row fail:ret[%d]", ret);
          }
        }
      }
      else if(OB_SUCCESS == ret)
      {
        ++row_counter_;
        break;
      }
      else
      {
        TBSYS_LOG(WARN, "get next row fail:ret[%d]", ret);
      }
    }
  }

  if(OB_SUCCESS == ret)
  {
    row = &cur_ups_row_;
    TBSYS_LOG(DEBUG, "ups scan row[%s]", to_cstring(cur_ups_row_));
  }
  return ret;
}

int ObUpsScan::fetch_next(bool first_scan)
{
  int ret = OB_SUCCESS;
  ObRowkey last_rowkey;
  INIT_PROFILE_LOG_TIMER();

  if(!first_scan)
  {
    if(OB_SUCCESS != (ret = cur_new_scanner_.get_last_row_key(last_rowkey)))
    {
      TBSYS_LOG(ERROR, "new scanner get rowkey fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = get_next_scan_param(last_rowkey, cur_scan_param_)))
    {
      TBSYS_LOG(WARN, "get scan param fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    int64_t remain_us = 0;
    if (is_timeout(&remain_us))
    {
      TBSYS_LOG(WARN, "process ups scan timeout, remain_us[%ld]", remain_us);
      ret = OB_PROCESS_TIMEOUT;
    }
    else
    {
      TBSYS_LOG(DEBUG, "remain ups scan time [%ld]us", remain_us);
      if(OB_SUCCESS != (ret = rpc_proxy_->sql_ups_scan(cur_scan_param_, cur_new_scanner_, remain_us)))
      {
        TBSYS_LOG(WARN, "scan ups fail:ret[%d]", ret);
      }
    }
  }
  PROFILE_LOG_TIME(DEBUG, "ObUpsScan::fetch_next first_scan[%d] , range=%s",
      first_scan, to_cstring(*cur_scan_param_.get_range()));

  return ret;
}

int ObUpsScan::set_range(const ObNewRange &range)
{
  int ret = OB_SUCCESS;
  ObString table_name; //设置一个空的table name
  range_ = range;
  if(OB_SUCCESS != (ret = cur_scan_param_.set(range_.table_id_, table_name, range_)))
  {
    TBSYS_LOG(WARN, "scan_param set range fail:ret[%d]", ret);
  }
  return ret;
}

void ObUpsScan::set_version_range(const ObVersionRange &version_range)
{
  cur_scan_param_.set_version_range(version_range);
}

//add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
void ObUpsScan::set_data_mark_param(const ObDataMarkParam &param)
{
  cur_scan_param_.set_data_mark_param(param);
}
//add duyr 20160531:e

int ObUpsScan::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(child_idx);
  UNUSED(child_operator);
  TBSYS_LOG(WARN, "not implement");
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObUpsScan, PHY_UPS_SCAN);
  }
}

int64_t ObUpsScan::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "UpsScan()\n");
  return pos;
}

ObUpsScan::ObUpsScan()
  :rpc_proxy_(NULL),
   ts_timeout_us_(0),
   row_counter_(0),
   is_read_consistency_(true)
{
}

ObUpsScan::~ObUpsScan()
{
}

bool ObUpsScan::check_inner_stat()
{
  return NULL != rpc_proxy_ && ts_timeout_us_ > 0;
}

int ObUpsScan::set_ups_rpc_proxy(ObSqlUpsRpcProxy *rpc_proxy)
{
  int ret = OB_SUCCESS;
  if(NULL == rpc_proxy)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "rpc_proxy is null");
  }
  else
  {
    rpc_proxy_ = rpc_proxy;
  }
  return ret;
}

int ObUpsScan::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  row_desc = &row_desc_;
  return ret;
}

bool ObUpsScan::is_result_empty() const
{
  int err = OB_SUCCESS;
  bool is_fullfilled = false;
  int64_t fullfilled_row_num = 0;
  if (OB_SUCCESS != (err = cur_new_scanner_.get_is_req_fullfilled(is_fullfilled, fullfilled_row_num) ))
  {
    TBSYS_LOG(WARN, "fail to get is fullfilled_item_num:err[%d]", err);
  }
  return (is_fullfilled && (fullfilled_row_num == 0));
}
