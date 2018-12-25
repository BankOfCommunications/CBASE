/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_scan_cell_stream.cpp for define scan rpc interface between
 * chunk server and update server. 
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "common/utility.h"
#include "ob_read_param_modifier.h"
#include "ob_scan_cell_stream.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;

    ObScanCellStream::ObScanCellStream(ObMergerRpcProxy * rpc_proxy,
      const ObServerType server_type, const int64_t time_out)
    : ObCellStream(rpc_proxy, server_type, time_out)
    {
      finish_ = false;
      reset_inner_stat();
    }
    
    ObScanCellStream::~ObScanCellStream()
    {
    }
    
    int ObScanCellStream::next_cell(void)
    {
      int ret = OB_SUCCESS;
      if (NULL == scan_param_)
      {
        TBSYS_LOG(DEBUG, "check scan param is null");
        ret = OB_ITER_END;
      }
      else if (!check_inner_stat())
      {
        TBSYS_LOG(ERROR, "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        // do while until get scan data or finished or error occured
        do
        {
          ret = get_next_cell();
          if (OB_ITER_END == ret)
          {
            TBSYS_LOG(DEBUG, "scan cell finish");
            break;
          }
          else if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "scan next cell failed:ret[%d]", ret);
            break;
          }
        } while (cur_result_.is_empty() && (OB_SUCCESS == ret));
      }
      return ret;
    }
    
    int ObScanCellStream::scan(const ObScanParam & param)
    {
      int ret = OB_SUCCESS;
      const ObNewRange * range = param.get_range();
      if (NULL == range)
      {
        TBSYS_LOG(WARN, "check scan param failed");
        ret = OB_INPUT_PARAM_ERROR;
      }
      else
      {
        reset_inner_stat();
        scan_param_ = &param;
        ret = scan_row_data();
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "scan server data failed:ret[%d]", ret);
        }
        else 
        {
          /**
           * ups return empty scanner, just return OB_ITER_END
           */
          if (cur_result_.is_empty() && !ObCellStream::first_rpc_)
          {
            // check already finish scan
            ret = check_finish_scan(cur_scan_param_);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "check finish scan failed:ret[%d]", ret);
            }
            else if (finish_)
            {
              ret = OB_ITER_END;
            }
          }
        }
      }
      return ret;
    }
    
    int ObScanCellStream::get_next_cell(void)
    {
      int ret = OB_SUCCESS;
      if (finish_)
      {
        ret = OB_ITER_END;
        TBSYS_LOG(DEBUG, "check next already finish");
      }
      else
      {
        last_cell_ = cur_cell_;
        ret = cur_result_.next_cell();
        // need rpc call for new data
        if (OB_ITER_END == ret)
        {
          // scan the new data only by one rpc call
          ret = scan_row_data();
          if (OB_SUCCESS == ret)
          {
            ret = cur_result_.next_cell();
            if ((ret != OB_SUCCESS) && (ret != OB_ITER_END))
            {
              TBSYS_LOG(WARN, "get next cell failed:ret[%d]", ret);
            }
            else if (OB_ITER_END == ret)
            {
              TBSYS_LOG(DEBUG, "finish the scan");
            }
          }
          else if (OB_ITER_END != ret)
          {
            TBSYS_LOG(WARN, "scan server data failed:ret[%d]", ret);
          }
        }
        else if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "next cell failed:ret[%d]", ret);
        }
      }
      return ret;
    }
    
    int ObScanCellStream::scan_row_data()
    {
      int ret = OB_SUCCESS;
      // step 1. modify the scan param for next scan rpc call
      if (!ObCellStream::first_rpc_)
      {
        // check already finish scan
        ret = check_finish_scan(cur_scan_param_);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "check finish scan failed:ret[%d]", ret);
        }
        else if (finish_)
        {
          ret = OB_ITER_END;
        }
      }

      // step 2. construct the next scan param
      if (OB_SUCCESS == ret)
      {
        ret = get_next_param(*scan_param_, cur_result_, &cur_scan_param_, range_buffer_);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "modify scan param failed:ret[%d]", ret);
        }
      }
    
      // step 3. scan data according the new scan param
      if (OB_SUCCESS == ret)
      {
        ret = ObCellStream::rpc_scan_row_data(cur_scan_param_);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "scan row data failed:ret[%d]", ret);
        }
      }

      return ret;
    }
    
    int ObScanCellStream::check_finish_scan(const ObScanParam & param)
    {
      int ret = OB_SUCCESS;
      bool is_fullfill = false;
      if (!finish_)
      {
        int64_t item_count = 0;
        ret = ObCellStream::cur_result_.get_is_req_fullfilled(is_fullfill, item_count);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "get scanner full filled status failed:ret[%d]", ret);
        }
        else if (is_fullfill)
        {
          ObNewRange result_range;
          ret = ObCellStream::cur_result_.get_range(result_range);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "get result range failed:ret[%d]", ret);
          }
          else
          {
            finish_ = is_finish_scan(param, result_range);
          }
        }
      }
      return ret;
    }

    int64_t ObScanCellStream::get_data_version() const
    {
      return cur_result_.get_data_version();
    }
  } // end namespace chunkserver
} // end namespace oceanbase
