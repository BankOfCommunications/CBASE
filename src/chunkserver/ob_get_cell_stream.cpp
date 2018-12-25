/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_get_cell_stream.cpp for define get rpc interface between 
 * chunk server and update server. 
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_get_cell_stream.h"
#include "ob_read_param_modifier.h"
#include "common/utility.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;

    int ObGetCellStream::next_cell()
    {
      int ret = OB_SUCCESS;
      if (!check_inner_stat())
      {
        TBSYS_LOG(ERROR, "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        if (get_cells_.get_cell_size() == 0)
        {
          TBSYS_LOG(DEBUG, "check get param cells count is 0");
          ret = OB_ITER_END;
        }
        else
        {
          ret = get_next_cell();
        }
    
        if (OB_ITER_END == ret)
        {
          TBSYS_LOG(DEBUG, "get the next cell return finish");
        }
        else if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "check get next cell failed:ret[%d]", ret);
        }
      }
      return ret;
    }
    
    int ObGetCellStream::get(const ObReadParam &read_param, ObCellArrayHelper & get_cells)
    {
      int ret = OB_SUCCESS;
      reset_inner_stat();
      get_cells_ = get_cells;
      read_param_ = read_param;
      if (NULL != cache_)
      {
        ret = row_result_.init();
      }
      return ret;
    }
    
    void ObGetCellStream::set_cache(ObJoinCache &cache)
    {
      is_cached_ = true;
      cache_ = &cache;
    }
    
    int ObGetCellStream::get_next_cell(void)
    {
      int add_cache_err = OB_SUCCESS;
      int ret = cur_result_.next_cell();
      if (OB_ITER_END == ret)
      {
        // must add cell firstly before scanner reset
        if (is_cached_)
        {
          add_cache_err = add_cell_cache(true);
          if (add_cache_err != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "add cell to cache failed:err[%d]", add_cache_err);
          }
        }
        // new rpc call get cell data
        ret = get_new_cell_data();
        if (OB_ITER_END == ret)
        {
          TBSYS_LOG(DEBUG, "get cell data all finish");
        }
        else if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "get cell data failed:ret[%d]", ret);
        }
      }
      // add cell to cache
      if (OB_SUCCESS == ret)
      {
        if (is_cached_)
        {
          // all finishi if OB_ITER_END == ret
          add_cache_err = add_cell_cache(false);
          if (add_cache_err != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "add cell to cache failed:err[%d]", add_cache_err);
          }
        }
      }
      return ret;
    }
    
    int ObGetCellStream::add_cell_cache(const bool force)
    {
      int ret = OB_SUCCESS;
      if (!force)
      {
        ObCellInfo * cell = NULL;
        bool row_change = false;
        ret = ObCellStream::get_cell(&cell, &row_change);
        if ((OB_SUCCESS != ret) || (NULL == cell))
        {
          TBSYS_LOG(ERROR, "get cell failed:ret[%d]", ret);
          ret = OB_ERROR;
        }
        else
        {
          if (row_change)
          {
            ret = add_cache_clear_result();
            if (ret != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR, "add and clear result to cache failed:ret[%d]", ret);
            }
          }
          // add current cell to scanner temp result
          if (OB_SUCCESS == ret && cur_row_cache_result_valid_)
          {
            ret = row_result_.add_cell(*cell);
            if (ret != OB_SUCCESS)
            {
              cur_row_cache_result_valid_ = false;
              TBSYS_LOG(ERROR, "add cell to scanner failed:ret[%d]", ret);
            }
          }
        }
      }
      else
      {
        ret = add_cache_clear_result();
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "add cache and clear result failed:ret[%d]", ret);
        }
      }
      return ret;
    }
    
    // must clear the row scanner result
    int ObGetCellStream::add_cache_clear_result()
    {
      int ret = OB_SUCCESS;
      // check data
      if (!row_result_.is_empty() && cur_row_cache_result_valid_)
      {
        if (OB_SUCCESS == ret)
        {
          ret = add_row_cache(row_result_);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "add row cache failed:ret[%d]", ret);
          }
        }
      }
      // clear
      if (NULL != cache_)
      {
        row_result_.init();
        cur_row_cache_result_valid_ = true;
      }
      return ret;
    }
    
    // new rpc call for get the new data
    int ObGetCellStream::get_new_cell_data(void)
    {
      int ret = OB_SUCCESS;
      // step 1. check finish get
      if (!ObCellStream::first_rpc_)
      {
        bool finish = false;
        ret = check_finish_all_get(finish);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "check finish get failed:ret[%d]", ret);
        }
        else if (true == finish)
        {
          TBSYS_LOG(DEBUG, "check cell data is already finished");
          ret = OB_ITER_END;
        }
      }
    
      // step 2. construct get param
      if (OB_SUCCESS == ret)
      {
        ret = get_next_param(read_param_, get_cells_, item_index_, &param_);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "modify get param failed:ret[%d]", ret);
        }
      }
    
      // step 3. get cell data according to the new param
      if (OB_SUCCESS == ret)
      {
        ret = ObCellStream::rpc_get_cell_data(param_);
        if ((ret != OB_SUCCESS) && (OB_ITER_END != ret))
        {
          TBSYS_LOG(WARN, "check get cell data failed:ret[%d]", ret);
        }
    
        // finish iterator one rpc response data 
        if (OB_ITER_END == ret)
        {
          bool finish = false;
          // check whether all data are finished
          ret = check_finish_all_get(finish);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "check finish get failed:ret[%d]", ret);
          }
          else if (false == finish)
          {
            TBSYS_LOG(WARN, "unexpected error, request not fullfilled, but iterator end");
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            ret = OB_ITER_END;
          }
        }
      }
      return ret;
    }
    
    int ObGetCellStream::encape_get_param(const int64_t start, const int64_t end, 
                                          ObGetParam & param)
    {
      int ret = OB_SUCCESS;
      if (end < start)
      {
        TBSYS_LOG(ERROR, "check input param failed:start[%ld], end[%ld]", start, end);
        ret = OB_INPUT_PARAM_ERROR;
      }
      else
      {
        // param.reset();
        // add N item in get param
        for (int64_t i = start; i <= end; ++i)
        {
          // get param from array and add to the get param
          ret = param.add_cell((ObCellInfo &)(get_cells_)[i]);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "add cell to get param failed:pos[%ld], ret[%d]", i, ret);
            break;
          }
        }
      }
      return ret;
    }
    
    // check all get op finish
    int ObGetCellStream::check_finish_all_get(bool & finish)
    {
      int ret = OB_SUCCESS;
      int64_t item_count = 0;
      finish = false;
      if (is_finish_)
      {
        finish = is_finish_;
      }
      else
      {
        bool is_fullfill = false;
        ret = ObCellStream::cur_result_.get_is_req_fullfilled(is_fullfill, item_count);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "get scanner full filled status failed:ret[%d]", ret);
        }
        else if (item_count <= 0)
        {
          TBSYS_LOG(ERROR, "check item count failed:item_count[%ld], item_index[%ld]", 
                    item_count, item_index_);
          ret = OB_ITEM_COUNT_ERROR;
        }
        else
        {
          item_index_ += item_count;
          if (is_fullfill)
          {
            if (item_index_ < get_cells_.get_cell_size())
            {
              TBSYS_LOG(DEBUG, "continue");
            }
            else if (item_index_ == get_cells_.get_cell_size())
            {
              is_finish_ = true;
              finish = is_finish_;
            }
            else
            {
              ret = OB_ITEM_COUNT_ERROR;
              TBSYS_LOG(ERROR, "check index error:item_count[%ld], item_index[%ld], "
                               "get_count[%ld]",
                        item_count, item_index_, get_cells_.get_cell_size());
            }
          }
          else if (item_index_ == get_cells_.get_cell_size())
          {
            is_finish_ = true;
            finish = is_finish_;
            TBSYS_LOG(WARN, "check item index succ but fulifill failed:"
                      "item_count[%ld], item_index[%ld], get_count[%ld]",
                      item_count, item_index_, get_cells_.get_cell_size());
          }
        }
      }
      return ret;
    }
    
    int ObGetCellStream::add_row_cache(const ObRowCellVec &row)
    {
      int ret = OB_SUCCESS;
      uint64_t table_id = row.get_table_id();
      ObRowkey row_key  = row.get_row_key();
      ObString cache_key;
      if ((NULL == row_key.ptr()) || (0 == row_key.length()))
      {
        ret = OB_INPUT_PARAM_ERROR;
        TBSYS_LOG(ERROR, "check row key[%s] failed:table_id[%lu]", to_cstring(row_key), table_id);
      }
    
      if (OB_SUCCESS == ret)
      {
        int64_t get_param_end_version = read_param_.get_version_range().end_version_;
        if (!read_param_.get_version_range().border_flag_.inclusive_end())
        {
          get_param_end_version--;
        }
        ObJoinCacheKey cache_key(static_cast<int32_t>(get_param_end_version), table_id, row_key);
        ret = cache_->put_row(cache_key, row);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "put row to cache failed:ret[%d]", ret);
        }
      }
      return ret;
    }
    
    int ObGetCellStream::get_cache_row(const ObCellInfo & key, ObRowCellVec *& result)
    {
      int ret = OB_SUCCESS;
      uint64_t table_id = key.table_id_;
      ObRowkey row_key = key.row_key_;
      if (!is_cached_)
      {
        ret = OB_ENTRY_NOT_EXIST;
      }
      else if (!check_inner_stat())
      {
        TBSYS_LOG(ERROR, "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else if ((NULL == row_key.ptr()) || (0 == row_key.length()))
      {
        TBSYS_LOG(ERROR, "check row key failed:table_id[%lu]", table_id);
        ret = OB_INPUT_PARAM_ERROR;
      }
      else if (key.row_key_.length() > OB_MAX_ROW_KEY_LENGTH)
      {
        TBSYS_LOG(WARN, "rowkey length too big [length:%ld, OB_MAX_ROW_KEY_LENGTH:%ld]", 
                  key.row_key_.length(), OB_MAX_ROW_KEY_LENGTH);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        if (OB_SUCCESS == ret)
        {
          int64_t get_param_end_version = read_param_.get_version_range().end_version_;
          if (!read_param_.get_version_range().border_flag_.inclusive_end())
          {
            get_param_end_version--;
          }
          ObJoinCacheKey cache_key(static_cast<int32_t>(get_param_end_version), table_id, row_key); 
          ret = row_result_.init();
          if (OB_SUCCESS == ret)
          {
            // get the row cell vec according to the key from join cache
            ret = cache_->get_row(cache_key, row_result_);
            if (OB_SUCCESS != ret)
            {
              result = NULL;
              TBSYS_LOG(DEBUG, "find this row[%s] from cache failed:table_id[%lu], ret[%d]", 
                  to_cstring(row_key), table_id, ret); 
            }
            else
            {
              result = &row_result_;
              TBSYS_LOG(DEBUG, "find row[%s] from cache succ", to_cstring(row_key));
            }
          }
          else
          {
            TBSYS_LOG(WARN, "row data init failed:ret[%d]", ret);
          }
        }
      }
      return ret;
    }

    int64_t ObGetCellStream::get_data_version() const
    {
      return cur_result_.get_data_version();
    }
  } // end namespace chunkserver
} // end namespace oceanbase
