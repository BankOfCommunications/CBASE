/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_read_param_modifier.cpp for modify scan param or get 
 * param. 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_read_param_modifier.h"
#include "common/ob_define.h"
#include "common/ob_range.h"
#include "common/utility.h"

namespace 
{
  using namespace oceanbase::common;
  using namespace oceanbase::chunkserver;

  int allocate_range_buffer(ObNewRange &range, ObMemBuf &buffer)
  {
    int err = OB_SUCCESS;
    int64_t start_key_size = range.start_key_.get_deep_copy_size() ;
    int64_t end_key_size = range.end_key_.get_deep_copy_size();
    int64_t buf_len = start_key_size +  end_key_size;
    if (0 < buf_len)
    {
      if (OB_SUCCESS != buffer.ensure_space(buf_len))
      {
        TBSYS_LOG(WARN, "fail to allocate memory for range key");
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        ObRawBufAllocatorWrapper start_wrapper(reinterpret_cast<char*>(buffer.get_buffer()), start_key_size);
        ObRawBufAllocatorWrapper end_wrapper(reinterpret_cast<char*>(buffer.get_buffer()) + start_key_size, end_key_size);
        range.start_key_.deep_copy(range.start_key_, start_wrapper);
        range.end_key_.deep_copy(range.end_key_, end_wrapper);
      }
    }
    return err;
  }

  int get_ups_read_param(ObReadParam & param,  const ObScanner & cs_result)
  {
    int err = OB_SUCCESS;
    ObVersionRange org_version_range;
    ObVersionRange cur_version_range;
    int64_t cs_version = 0;
    org_version_range = param.get_version_range();
    cur_version_range = org_version_range;

    cs_version = cs_result.get_data_version();
    if (OB_SUCCESS == err)
    {
      if (cur_version_range.border_flag_.is_max_value() 
          || org_version_range.end_version_ > cs_version + 1
          || (cur_version_range.border_flag_.inclusive_end() 
              && org_version_range.end_version_ == cs_version + 1))
      {
        cur_version_range.border_flag_.unset_min_value();
        cur_version_range.border_flag_.set_inclusive_start();
        cur_version_range.start_version_ = cs_version + 1;

        ObVersion cv = cs_version;
        FILL_TRACE_LOG("version range for ups:%s,org_version_range:%s,cs_version:%d-%hd-%hd",
                       range2str(cur_version_range),range2str(org_version_range),cv.major_,cv.minor_,cv.is_final_minor_);
      }
      else
      {
        TBSYS_LOG(DEBUG, "chunk server return all data needed");
        err = OB_ITER_END;
      }
    }
    if (OB_SUCCESS == err)
    {
      param.set_version_range(cur_version_range);
    }
    return err;
  }
}

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;

    bool is_finish_scan(const ObScanParam & param, const ObNewRange & result_range)
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
    
    int get_next_param(const ObReadParam & org_read_param, 
                       const ObCellArrayHelper &org_get_cells, 
                       const int64_t got_cell_num, ObGetParam *get_param)
    {
      int ret = OB_SUCCESS;
      int64_t surplus = org_get_cells.get_cell_size() - got_cell_num;
      int64_t cur_row_beg = got_cell_num;
      int64_t cur_row_end = got_cell_num;
      ObReadParam *read_param = get_param;
      if(NULL != get_param)
      {
        get_param->reset();
        *read_param = org_read_param;
      }
      if (surplus <= 0)
      {
        ret = OB_ITER_END;
      }
      if (OB_SUCCESS == ret)
      {
        while (cur_row_end < org_get_cells.get_cell_size() 
               && OB_SUCCESS == ret)
        {
          while (OB_SUCCESS == ret 
                 && cur_row_end < org_get_cells.get_cell_size()
                 && org_get_cells[cur_row_end].row_key_ == org_get_cells[cur_row_beg].row_key_
                 && org_get_cells[cur_row_end].table_id_ == org_get_cells[cur_row_beg].table_id_)
          {
            if(NULL != get_param)
            {
              ret = get_param->add_cell(org_get_cells[cur_row_end]);
            }
            if (OB_SIZE_OVERFLOW == ret)
            {
              break; 
            }
            if (OB_SUCCESS == ret)
            {
              cur_row_end ++;
            }
          } 
          if (OB_SIZE_OVERFLOW == ret)
          {
            if(NULL != get_param)
            {
              ret = get_param->rollback();
            }
            break;
          }
          if (OB_SUCCESS == ret)
          {
            cur_row_beg = cur_row_end;
          }
        }
      }
      return ret;
    }

    int get_next_param(
        const ObScanParam &org_scan_param, const ObScanner &prev_scan_result, 
        ObScanParam *scan_param, ObMemBuf &range_buffer)
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
          TBSYS_LOG(WARN, "unexpected error, can't get range from org scan param");
          err  = OB_INVALID_ARGUMENT;
        }
      }
      
      ObRowkey last_row_key;
      if (OB_SUCCESS == err)
      {
        if (request_fullfilled)
        {
          err = prev_scan_result.get_range(tablet_range);
          //TBSYS_LOG(INFO,"test::whx get_range [%s]",to_cstring(tablet_range));
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
         // TBSYS_LOG(INFO,"test::whx last_row_key [%s]",to_cstring(tablet_range));
          if (OB_ENTRY_NOT_EXIST == err)
          {
            /// first time, remain unchanged
          }
          else if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "unexpected error, scanner should contain at least one cell");
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
        if(OB_SUCCESS == err)
        {
          scan_param->set_scan_size(GET_SCAN_SIZE(org_scan_param.get_scan_size()));
          scan_param->set_scan_flag(org_scan_param.get_scan_flag());
        }
      }
      //TBSYS_LOG(INFO,"test::whx before set fake range [%s],range[%s]", to_cstring(*scan_param->get_fake_range()),to_cstring(*(scan_param->get_range())));
      //add wenghaixing [secondary index static_index_build.cs_scan]20150330
      scan_param->set_fake(org_scan_param.if_need_fake());
      ObNewRange fake_range;
      if(scan_param->if_need_fake())
      {
        fake_range = *org_scan_param.get_fake_range();
        scan_param->set_copy_args(false);
        {
          err = scan_param->set_fake_range(fake_range);
        }
        //modify e
        //TBSYS_LOG(INFO,"test::whx set fake range [%s]", to_cstring(*scan_param->get_fake_range()));
      }
      //TBSYS_LOG(INFO,"test::whx set fake range [%s],range[%s]", to_cstring(*scan_param->get_fake_range()),to_cstring(*(scan_param->get_range())));
      //add e
      return err;
    }

    int get_ups_param(
        oceanbase::common::ObScanParam & param, 
        const oceanbase::common::ObScanner & cs_result, 
        ObMemBuf &range_buffer)
    {
      int err = OB_SUCCESS;
      bool is_fullfill = false;
      int64_t fullfilled_row_num = 0;
      ObReadParam &read_param = param;
      ObNewRange cs_range;
      ObRowkey cs_max_rowkey;
      bool use_max_rowkey = false;
      err =  get_ups_read_param(read_param,cs_result);
      if (OB_SUCCESS == err)
      {
        err = cs_result.get_is_req_fullfilled(is_fullfill,fullfilled_row_num);
      }
      
      if (OB_SUCCESS == err )
      {
        if (is_fullfill)
        {
          err = cs_result.get_range(cs_range);
          if (OB_SUCCESS == err)
          {
            /// need change the new scan param range rowkey
            if (false == is_finish_scan(param, cs_range))
            {
              use_max_rowkey = true;
              if (ScanFlag::FORWARD == param.get_scan_direction()) 
              {
                cs_max_rowkey = cs_range.end_key_;
              }
              else
              {
                // the smallest rowkey
                cs_max_rowkey = cs_range.start_key_;
              }
            }
          }
        }
        else
        {
          err = cs_result.get_last_row_key(cs_max_rowkey);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "unexpected error, there should be at least one cell in the scanner");
          }
          use_max_rowkey = true;
        }
      }
    
      if (OB_SUCCESS == err && use_max_rowkey)
      {
        ObNewRange new_range = *param.get_range();
        if (ScanFlag::FORWARD == param.get_scan_direction())
        {
          new_range.end_key_ = cs_max_rowkey;
          new_range.border_flag_.set_inclusive_end();
        }
        else
        {
          // the smallest row key
          new_range.start_key_ = cs_max_rowkey;
          if (is_fullfill)
          {
            // not inclusive the tablet range start key
            new_range.border_flag_.unset_inclusive_start();
          }
          else
          {
            new_range.border_flag_.set_inclusive_start();
          }
        }
    
        err = allocate_range_buffer(new_range,range_buffer);
        if (OB_SUCCESS == err)
        {
          param.set(param.get_table_id(),param.get_table_name(),new_range);
        }
      }
      return err;
    }
    
    int get_ups_param(ObGetParam & param, const ObScanner & cs_result)
    {
      int err = OB_SUCCESS;
      ObReadParam &read_param = param;
      err =  get_ups_read_param(read_param,cs_result); 
      if (OB_SUCCESS == err)
      {
        bool is_fullfilled = false;
        int64_t fullfilled_cell_num  = 0;
        err = cs_result.get_is_req_fullfilled(is_fullfilled, fullfilled_cell_num);
        if (OB_SUCCESS == err && fullfilled_cell_num < param.get_cell_size())
        {
          err = param.rollback(param.get_cell_size() - fullfilled_cell_num);
          if ((err != OB_SUCCESS) || (param.get_cell_size() != fullfilled_cell_num))
          {
            TBSYS_LOG(WARN, "check param rollback failed:full_fill[%ld], param_size[%ld]",
                fullfilled_cell_num, param.get_cell_size());
          }
        }
      }
      return err;
    }

    //add zhaoqiong [Truncate Table]:20160318:b
    int get_ups_param( common::ObScanParam & param, ObMemBuf &range_buffer )
    {
      int err = OB_SUCCESS;
      ObNewRange new_range = *param.get_range();
      err = allocate_range_buffer(new_range,range_buffer);
      if (OB_SUCCESS == err)
      {
        param.set(param.get_table_id(),param.get_table_name(),new_range);
      }
      return err;
    }
    //mod:e
  } // end namespace chunkserver
} // end namespace oceanbase
