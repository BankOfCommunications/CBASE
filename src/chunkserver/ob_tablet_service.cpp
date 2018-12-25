/** * (C) 2010-2012 Alibaba Group Holding Limited.  * * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_service.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_tablet_service.h"
#include "common/ob_profile_log.h"
#include "common/ob_schema_manager.h"

using namespace oceanbase;
using namespace chunkserver;
using namespace common;

ObTabletService::ObTabletService(ObChunkServer &chunk_server)
  :cur_rowkey_(NULL),
  cur_row_(NULL),
  tablet_read_(NULL),
  chunk_server_(chunk_server)
{
}

ObTabletService::~ObTabletService()
{
}

void ObTabletService::reset()
{
  tablet_get_.reset();
  tablet_scan_.reset();
  rowkey_allocator_.reuse();
  cur_rowkey_ = NULL;
  cur_row_ = NULL;
  tablet_read_ = NULL;
}

int ObTabletService::open(const sql::ObSqlReadParam &sql_read_param)
{
  int ret = OB_SUCCESS;
  common::ObMergerSchemaManager *merger_schema_mgr = NULL;
  const ObSchemaManagerV2 *schema_mgr = NULL;
  cur_row_ = NULL;
  const ObSqlScanParam *sql_scan_param = NULL;
  const ObSqlGetParam *sql_get_param = NULL;

  INIT_PROFILE_LOG_TIMER();

  sql_scan_param = dynamic_cast<const ObSqlScanParam *>(&sql_read_param);
  if (NULL == sql_scan_param)
  {
    sql_get_param = dynamic_cast<const ObSqlGetParam *>(&sql_read_param);
    if (NULL == sql_get_param)
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "invalid param type");
    }
    else
    {
      TBSYS_LOG(DEBUG, "cs use GET method");
    }
  }
  else
  {
    TBSYS_LOG(DEBUG, "cs use SCAN method");
  }

  if(OB_SUCCESS == ret)
  {
    if(NULL == (merger_schema_mgr = chunk_server_.get_schema_manager()))
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "chunk server get schema manager fail:ret[%d]", ret);
    }
    else if(NULL == (schema_mgr = merger_schema_mgr->get_user_schema(0)))
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "get user schema mgr fail:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ObTabletManager &tablet_mgr = chunk_server_.get_tablet_manager();
    ScanContext scan_context;

    if (NULL != sql_scan_param)
    {
      tablet_mgr.build_scan_context(scan_context);
      tablet_scan_.set_scan_context(scan_context);
      tablet_scan_.set_sql_scan_param(*sql_scan_param);
      tablet_read_ = &tablet_scan_;
    }
    else
    {
      if (OB_SUCCESS != (ret = tablet_get_.set_tablet_manager(&tablet_mgr)))
      {
        TBSYS_LOG(WARN, "fail to set tablet mgr:ret[%d]", ret);
      }
      else
      {
        tablet_get_.set_sql_get_param(*sql_get_param);
        tablet_read_ = &tablet_get_;
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    tablet_read_->set_ts_timeout_us(timeout_us_);
    if (OB_SUCCESS != (ret = tablet_read_->set_rpc_proxy(chunk_server_.get_rpc_proxy()) ))
    {
      TBSYS_LOG(WARN, "fail to set rpc proxy:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {

    tablet_read_->set_join_batch_count(chunk_server_.get_config().join_batch_count);
    tablet_read_->set_is_read_consistency(sql_read_param.get_is_read_consistency());

    PROFILE_LOG_TIME(DEBUG, "begin tablet_read_ create_plan.");
    if (OB_SUCCESS != (ret = tablet_read_->create_plan(*schema_mgr)))
    {
      TBSYS_LOG(WARN, "fail to create plan:ret[%d]", ret);
    }
    else
    {
      PROFILE_LOG_TIME(DEBUG, "tablet_read_ create_plan complete.");
      FILL_TRACE_LOG("create_plan done,ret=%d", ret);
      if (OB_UNLIKELY(TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_TRACE))
      {
        TBSYS_LOG(TRACE, "ExecutionPlan: \n%s", to_cstring(tablet_scan_));
      }
      if(OB_SUCCESS != (ret = tablet_read_->open()))
      {
        TBSYS_LOG(WARN, "open tablet scan fail:ret[%d]", ret);
        if (OB_DUPLICATE_COLUMN == ret)
        {
          TBSYS_LOG(USER_ERROR, "Duplicate entry for key \'PRIMARY\'");
          ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
        }
      }
      PROFILE_LOG_TIME(DEBUG, "tablet_read_ open complete ret[%d]", ret);
    }
  }

  if (schema_mgr != NULL)
  {
    int err = merger_schema_mgr->release_schema(schema_mgr);
    if(OB_SUCCESS != err)
    {
      ret = err;
      TBSYS_LOG(WARN, "release schema mgr fail:ret[%d]", ret);
    }
  }

  return ret;
}

int ObTabletService::fill_scan_data(ObNewScanner &new_scanner)
{
  int ret = OB_SUCCESS;
  int64_t fullfilled_row_num = 0;
  int64_t start_time = tbsys::CTimeUtil::getTime();
  int64_t timeout = timeout_us_ - start_time;


  INIT_PROFILE_LOG_TIMER();

  if (timeout <= 0)
  {
    ret = OB_RESPONSE_TIME_OUT;
    TBSYS_LOG(WARN, "cs already timeout. timeout[%ld], timeout_us_[%ld], start_time[%ld]", timeout, timeout_us_, start_time);
  }

  if(OB_SUCCESS == ret)
  {
    new_scanner.reuse();
  }

  while(OB_SUCCESS == ret)
  {
    if(NULL == cur_row_)
    {
      if(NULL != cur_rowkey_)
      {
        rowkey_allocator_.reuse();
        if(OB_SUCCESS != (ret = cur_rowkey_->deep_copy(last_rowkey_, rowkey_allocator_)))
        {
          TBSYS_LOG(WARN, "deep copy rowkey fail:ret[%d]", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = tablet_read_->get_next_row(cur_row_);
        if(OB_ITER_END == ret)
        {
          ret = OB_SUCCESS;
          if(OB_SUCCESS != (ret = new_scanner.set_is_req_fullfilled(true, fullfilled_row_num)))
          {
            TBSYS_LOG(WARN, "new scanner set is req fullfilled fail:ret[%d]", ret);
          }
          else if(OB_SUCCESS != (ret = new_scanner.set_last_row_key(last_rowkey_)))
          {
            TBSYS_LOG(WARN, "new scanner set last row key fail:ret[%d]", ret);
          }
          break;
        }
        else if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "get next row fail:ret[%d]", ret);
        }
      }

      if(OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = tablet_read_->get_last_rowkey(cur_rowkey_)))
        {
          TBSYS_LOG(WARN, "get last rowkey fail:ret[%d]", ret);
        }
      }
    }

    int64_t max_time = timeout - std::min(static_cast<int64_t>((static_cast<double>(timeout) * 0.3)),
                                          static_cast<int64_t>(500 * 1000) /* 500ms */);

    if (OB_SUCCESS == ret)
    {
      if ((start_time + max_time) < g_cur_time)
      {
        if (fullfilled_row_num > 0)
        {
          // at least get one row
          TBSYS_LOG(WARN, "get or scan too long time, start_time=%ld timeout=%ld timeu=%ld row_count=%ld",
              start_time, timeout, g_cur_time - start_time, fullfilled_row_num);
          if(OB_SUCCESS != (ret = new_scanner.set_is_req_fullfilled(false, fullfilled_row_num)))
          {
            TBSYS_LOG(WARN, "new scanner set is req fullfilled fail:ret[%d]", ret);
          }
          else if(OB_SUCCESS != (ret = new_scanner.set_last_row_key(last_rowkey_)))
          {
            TBSYS_LOG(WARN, "new scanner set last row key fail:ret[%d]", ret);
          }
          //else
          //{
          //  ret = OB_FORCE_TIME_OUT;
          //}
        }
        else
        {
          TBSYS_LOG(WARN, "can't get any row, start_time=%ld timeout=%ld timeu=%ld",
              start_time, timeout, g_cur_time - start_time);
          ret = OB_RESPONSE_TIME_OUT;
        }
        break;
      }
    }

    if(OB_SUCCESS == ret)
    {
      ret = new_scanner.add_row(*cur_row_);
      if(OB_SIZE_OVERFLOW == ret)
      {
        ret = OB_SUCCESS;
        if(OB_SUCCESS != (ret = new_scanner.set_is_req_fullfilled(false, fullfilled_row_num)))
        {
          TBSYS_LOG(WARN, "new scanner set is req fullfilled fail:ret[%d]", ret);
        }
        else if(OB_SUCCESS != (ret = new_scanner.set_last_row_key(last_rowkey_)))
        {
          TBSYS_LOG(WARN, "new scanner set last row key fail:ret[%d]", ret);
        }
        break;
      }
      else if(OB_SUCCESS == ret)
      {
        cur_row_ = NULL;
        fullfilled_row_num ++;

        if (fullfilled_row_num % 1000 == 0)
        {
          PROFILE_LOG_TIME(DEBUG, "fill next 1000 row, fullfilled_row_num=%ld", fullfilled_row_num);
        }
      }
      else
      {
        TBSYS_LOG(WARN, "get next row fail:ret[%d]", ret);
      }
    }
  }

  if ((OB_SUCCESS == ret || OB_FORCE_TIME_OUT == ret) && tablet_read_ == &tablet_scan_ )
  {
    ObNewRange range;
    //mod zhaoqiong [Truncate Table]:20160318:b
    //if (OB_SUCCESS == (ret = tablet_scan_.get_tablet_range(range)))
    if (OB_SUCCESS == (ret = tablet_scan_.get_scan_range(range)))
      //mod:e
    {
      new_scanner.set_range(range);
    }
  }

  return ret;
}

int ObTabletService::close()
{
  int ret = OB_SUCCESS;
  int err = OB_SUCCESS;
  if (OB_SUCCESS != (err = tablet_scan_.close()))
  {
    ret = err;
    TBSYS_LOG(WARN, "close tablet scan fail:err[%d]", err);
  }
  if (OB_SUCCESS != (err = tablet_get_.close()))
  {
    ret = err;
    TBSYS_LOG(WARN, "close tablet get fail:err[%d]", err);
  }
  return ret;
}

void ObTabletService::set_timeout_us(int64_t timeout_us)
{
  timeout_us_ = timeout_us;
}
