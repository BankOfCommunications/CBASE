/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_ms_sub_get_request.cpp for
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   xiaochu <xiaochu.yh@alipay.com>
 */

#include "ob_ms_sql_sub_get_request.h"
#include "common/utility.h"
#include <algorithm>
using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::mergeserver;

  ObMsSqlSubGetRequest::ObMsSqlSubGetRequest()
: rowkey_buf_(ObModIds::OB_MS_GET_EVENT)
{
  reset();
}

void ObMsSqlSubGetRequest::init(PageArena<int64_t, oceanbase::common::ModulePageAllocator> & mem_allocator)
{
  //ObVector的内存分配器使用大get param的page_arena_
  row_idx_in_org_param_.set_allocator(mem_allocator);
  //这个成员存放收到的rpc event的ObNewScanner的指针
  res_vec_.set_allocator(mem_allocator);
  rowkey_buf_.clear();
}


void ObMsSqlSubGetRequest::reset()
{
  last_rpc_event_ = 0;
  row_idx_in_org_param_.clear();
  res_vec_.clear();
  pget_param_ = NULL;
  received_row_count_ = 0;
  poped_row_count_ = 0;
  res_iterator_idx_  = 0;
  retry_times_ = 0;
  fail_svr_ip_ = 0;
  rowkey_buf_.reset();
}

void ObMsSqlSubGetRequest::set_param(ObSqlGetParam & get_param)
{
  reset();
  pget_param_ = &get_param;
}


int ObMsSqlSubGetRequest::add_row(const int64_t row_idx)
{
  int err = OB_SUCCESS;
  if (NULL == pget_param_)
  {
    TBSYS_LOG(WARN,"set ObSqlGetParam first [pget_param_:%p]", pget_param_);
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && ((row_idx < 0) || (row_idx >= pget_param_->get_row_size())))
  {
    TBSYS_LOG(WARN,"row_idx out of range [row_idx:%ld,pget_param_->get_row_size():%ld]",
        row_idx, pget_param_->get_row_size());
    err = OB_INVALID_ARGUMENT;
  }

  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = row_idx_in_org_param_.push_back(row_idx))))
  {
    TBSYS_LOG(WARN,"fail to append row index [err:%d]", err);
  }

  return err;
}


int ObMsSqlSubGetRequest::add_result(ObNewScanner &res)
{
  int err = OB_SUCCESS;
  if (NULL == pget_param_)
  {
    TBSYS_LOG(WARN,"set ObSqlGetParam first [pget_param_:%p]", pget_param_);
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = res_vec_.push_back(reinterpret_cast<int64_t>(&res)))))
  {
    TBSYS_LOG(WARN,"fail to appedn result [err:%d]", err);
  }
  if (OB_SUCCESS == err)
  {
    received_row_count_ += res.get_row_num();
    retry_times_ = 0;
    TBSYS_LOG(DEBUG, "got result. scanner row=%d, total received row=%ld", res_vec_.size(), received_row_count_);
  }
  return err;
}


/**
 * get_param 输出参数,子请求的get param
 */
int ObMsSqlSubGetRequest::get_next_param_(ObSqlGetParam & get_param)const
{
  int err = OB_SUCCESS;
  get_param.reset();
  ObSqlReadParam & read_param = get_param;
  // pget_param_指向全局
  read_param = *dynamic_cast<ObSqlReadParam*>(pget_param_);
  if (NULL == pget_param_)
  {
    TBSYS_LOG(WARN,"set ObSqlGetParam first [pget_param_:%p]", pget_param_);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    get_param.set_table_id(pget_param_->get_renamed_table_id(), pget_param_->get_table_id());
  }
  if ((OB_SUCCESS == err) && (received_row_count_ >= row_idx_in_org_param_.size()))
  {
    err = OB_ITER_END;
  }
  //取出该子请求所包含的行在大请求的get param中的下标
  for (int64_t row_idx = received_row_count_; (OB_SUCCESS == err) && (row_idx < row_idx_in_org_param_.size()); row_idx ++)
  {
    //这里是浅拷贝，只拷贝了ObRowKey中的ObObj的指针和个数
    if ((OB_SUCCESS != (err = get_param.add_rowkey(*pget_param_->operator [](row_idx_in_org_param_[static_cast<int32_t>(row_idx)]))))
        && (OB_SIZE_OVERFLOW != err))
    {
      TBSYS_LOG(WARN,"fail to add row to ObSqlGetParam [err:%d]", err);
    }
    else if (OB_SIZE_OVERFLOW == err)
    {
      err = OB_SUCCESS;
      break;
    }
  }
  //add zhujun[transaction read uncommit]2016/3/28
  if(OB_SUCCESS == err)
  {
      get_param.set_trans_id(pget_param_->get_trans_id());
  }
  //add:e

  return err;
}

int  ObMsSqlSubGetRequest::get_next_row(oceanbase::common::ObRow *&row, int64_t & org_row_idx)
{
  row = &cur_row_;
  return get_next_row(cur_row_, org_row_idx);
}


int  ObMsSqlSubGetRequest::get_next_row(oceanbase::common::ObRow &row, int64_t & org_row_idx)
{
  int err = OB_SUCCESS;
  if (NULL == pget_param_)
  {
    TBSYS_LOG(WARN,"set ObSqlGetParam first [pget_param_:%p]", pget_param_);
    err = OB_INVALID_ARGUMENT;
  }

  if ((OB_SUCCESS == err) && (res_iterator_idx_ >= res_vec_.size()))
  {
    err = OB_ITER_END;
  }

  if ((OB_SUCCESS == err) && (res_iterator_idx_ < res_vec_.size()))
  {
    // in case there are any empty scanner, use a do while loop for safe
    do
    {
      err = reinterpret_cast<ObNewScanner*>(res_vec_[static_cast<int32_t>(res_iterator_idx_)])->get_next_row(row);
      //TBSYS_LOG(INFO, "get next row.size=%d, err=%d, row=%s", res_vec_.size(), err, to_cstring(row));
      if (OB_ITER_END == err)
      {
        // TBSYS_LOG(INFO, "cur scanner used up. switch to next or end. res_iter_idx=%ld", res_iterator_idx_);
        res_iterator_idx_++;
      }
      else if (OB_SUCCESS == err)
      {
        // TBSYS_LOG(INFO, "row=%s", to_cstring(row));
        break;
      }
      else
      {
        TBSYS_LOG(WARN, "fail to get next row. %ld, %d", res_iterator_idx_, err);
        break;
      }
    } while(res_iterator_idx_ < res_vec_.size());

    if (OB_SUCCESS == err)
    {
      org_row_idx = row_idx_in_org_param_[static_cast<int32_t>(poped_row_count_)];
      poped_row_count_ ++;
      if (poped_row_count_ > row_idx_in_org_param_.size())
      {
        TBSYS_LOG(ERROR, "not all rows poped, but all indexes are poped [poped_row_count_:%ld,res_iterator_idx_:%ld]",
            poped_row_count_, res_iterator_idx_);
        err = OB_ERR_UNEXPECTED;
      }
      else
      {
        // don't verify rowkey, because:
        // in SELECT CS doesn't return rowkey in requested order, row.get_rowkey() doesn't work in this case
        //
#if 0
        {
          // verify result if INSERT/REPLACE/UPDATE
          const ObRowkey *verify_rowkey = NULL;
          if (OB_SUCCESS != (err = row.get_rowkey(verify_rowkey)))
          {
            TBSYS_LOG(WARN, "fail to get rowkey. err=%d", err);
          }
          else if (*((*pget_param_)[org_row_idx]) != *verify_rowkey)
          {
            TBSYS_LOG(ERROR, "result not correct param->row_key_:%s, result->row_key_:%s]",
                to_cstring(*(*pget_param_)[org_row_idx]), to_cstring(*verify_rowkey));
            err = OB_ERR_UNEXPECTED;
          }
        }
#endif
      }
    }
    else if (OB_ITER_END != err)
    {
      TBSYS_LOG(WARN,"get row from ObNewScanner [err:%d,res_iterator_idx_:%ld] fail", err, res_iterator_idx_);
    }
  }

  return err;
}

int ObSqlGetMerger::init(SubRequestVector *results, const int64_t res_count,
    const ObSqlGetParam & get_param)
{
  int err = OB_SUCCESS;
  if ((NULL == results) || (res_count <= 0))
  {
    TBSYS_LOG(WARN,"invalid argument [results:%p,res_count:%ld]", results, res_count);
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && (res_count > MAX_RES_SIZE))
  {
    TBSYS_LOG(WARN,"result size too large [res_count:%ld,MAX_RES_SIZE:%ld]", res_count, MAX_RES_SIZE);
    err = OB_ARRAY_OUT_OF_RANGE;
  }
  if (OB_SUCCESS == err)
  {
    res_size_ = res_count;
    res_vec_ = results;
    heap_size_ = 0;
    cur_row_idx_ = 0;
    get_param_ = &get_param;
    has_read_first_row_ = false;
  }
  if (OB_SUCCESS == err)
  {
    /* build init heap */
    for (int32_t res_idx = 0; (OB_SUCCESS == err) && (res_idx < res_size_); res_idx ++)
    {
      if (OB_SUCCESS == (err = res_vec_->operator[](res_idx)->get_next_row(info_heap_[heap_size_].cur_row_,info_heap_[heap_size_].org_row_idx_)))
      {
        info_heap_[heap_size_].res_idx_ = res_idx;
        heap_size_ ++;
      }
      else
      {
        TBSYS_LOG(WARN,"fail to get next row from result [err:%d,res_idx:%d]", err, res_idx);
      }
    }
    if (OB_SUCCESS == err)
    {
      make_heap(info_heap_,info_heap_ + heap_size_);
    }
  }
  return err;
}

int ObSqlGetMerger::get_next_row(common::ObRow &row)
{
  int err = OB_SUCCESS;
  if (NULL == res_vec_)
  {
    TBSYS_LOG(WARN,"ObSqlGetMerger not init yet");
    err = OB_INVALID_ARGUMENT;
  }

  if ((OB_SUCCESS == err) && (heap_size_ <= 0))
  {
    err = OB_ITER_END;
  }

  if (OB_SUCCESS == err)
  {
    // row = cur_row_;
    row = *info_heap_[0].cur_row_;
    /* valid check */
    if (cur_row_idx_ != info_heap_[0].org_row_idx_)
    {
      TBSYS_LOG(ERROR, "unexpected error [cur_row_idx_:%ld,got_row_idx:%ld,res_idx:%ld]",
          cur_row_idx_, info_heap_[0].org_row_idx_, info_heap_[0].res_idx_);
      err = OB_ERR_UNEXPECTED;
    }
    else
    {
      cur_row_idx_ ++;
    }
  }

  // try read current row, may got nothing if empty
  if (OB_SUCCESS == err)
  {
    int32_t res_idx = static_cast<int32_t>(info_heap_[0].res_idx_);
    ResInfo next_info;
    // update next_info using next row of this subrequest result
    err = res_vec_->operator[](res_idx)->get_next_row(next_info.cur_row_, next_info.org_row_idx_);
    if (OB_ITER_END == err)
    {
      pop_heap(info_heap_, info_heap_ + heap_size_);
      heap_size_ --;
      err = OB_SUCCESS;
    }
    else if (OB_SUCCESS == err)
    {
      // push updated next_info to heap again
      next_info.res_idx_ = res_idx;
      pop_heap(info_heap_, info_heap_ + heap_size_);
      info_heap_[heap_size_ - 1] = next_info;
      push_heap(info_heap_, info_heap_ + heap_size_);
    }
    else
    {
      TBSYS_LOG(WARN,"fail to get next row from result [res_idx:%d,err:%d]", res_idx, err);
    }
  }

  return err;
}
