/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_groupby_operator.cpp for group by operator. 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_groupby_operator.h"
#include "ob_rowkey.h"

namespace oceanbase
{
  namespace common
  {
    using namespace oceanbase::common::hash;

    ObGroupByOperator::ObGroupByOperator()
    {
      param_ = NULL;
      inited_ = false;
      groupby_mem_size_ = -1;
      max_groupby_mem_size_ = -1;
    }
    
    ObGroupByOperator::~ObGroupByOperator()
    {
      param_ = NULL;
      inited_ = false;
      groupby_mem_size_ = -1;
      max_groupby_mem_size_ = -1;
    }
    
    void ObGroupByOperator::clear()
    {
      int64_t memory_size_water_mark = 
        std::max<int64_t>(groupby_mem_size_, max_groupby_mem_size_);
      if (memory_size_water_mark > 0 
          && get_memory_size_used() > memory_size_water_mark)
      {
        TBSYS_LOG(WARN, "clear groupby cellarray, memory_size_water_mark=%ld, "
            "cellarray_mem_size=%ld",
          memory_size_water_mark, get_memory_size_used());
        ObCellArray::clear();
      }
      else
      {
        ObCellArray::reset();
      }
      if (group_hash_map_.size() > 0)
      {
        group_hash_map_.clear();
      }
      groupby_mem_size_ = -1;
      max_groupby_mem_size_ = -1;
      empty_cell_.reset();
    }
    
    int ObGroupByOperator::init(const ObGroupByParam & param, const int64_t groupby_mem_size, 
        const int64_t max_groupby_mem_size)
    {
      int err = OB_SUCCESS;
      param_ = &param;
      groupby_mem_size_ = groupby_mem_size;
      max_groupby_mem_size_ = max_groupby_mem_size;
      if (!inited_)
      {
        err = group_hash_map_.create(HASH_SLOT_NUM);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to create hash table");
        }
        else
        {
          inited_ = true;
        }
      }
      return err;
    }

    int ObGroupByOperator::append_fake_composite_column(
        const ObRowkey& rowkey, 
        const uint64_t table_id)
    {
      int err = OB_SUCCESS;
      int64_t comp_column_size = 0;
      ObInnerCellInfo *cell_out = NULL;

      /**
       * for scan operation, maybe the groupby param includes 
       * composite column infos, when we caculate the composite 
       * columns, we also use the same resut cell array, so we add 
       * some fake cells to reverse some space for composite columns 
       * in result cell array after merge one row. after we do groupby
       * with this result cell array, we caculate the actual value of 
       * composite columns. 
       */
      if (NULL != param_)
      {
        comp_column_size = param_->get_composite_columns().get_array_index();
        if (comp_column_size > 0)
        {
          empty_cell_.table_id_ = table_id;
          empty_cell_.row_key_ = rowkey;
          for (int64_t i = 0; i < comp_column_size && OB_SUCCESS == err; ++i)
          {
            err = ObCellArray::append(empty_cell_, cell_out);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "fail to append fake composite column cell [err:%d]", err);
            }
          }
        }
      }

      return err;
    }
    
    int ObGroupByOperator::init_all_in_one_group_row(
        const ObRowkey& rowkey, const uint64_t table_id)
    {
      int err = OB_SUCCESS;
      if ((0 >= param_->get_aggregate_columns().get_array_index())
          ||(0 != param_->get_groupby_columns().get_array_index())
          ||(0 != this->get_cell_size()))
      {
        TBSYS_LOG(WARN,"aggregate function must act all records and result must be empty "
                  "[param_->get_aggregate_columns().size():%ld,"
                  "param_->get_groupby_columns().size():%ld,"
                  "this->get_cell_size():%ld]", param_->get_aggregate_columns().get_array_index(),
                  param_->get_groupby_columns().get_array_index(),
                  this->get_cell_size());
        err = OB_INVALID_ARGUMENT;
      }
      ObCellInfo empty_cell;
      ObInnerCellInfo *cell_out = NULL;
      empty_cell.value_.reset();
      empty_cell.row_key_ = rowkey;
      empty_cell.table_id_ = table_id;

      for (int64_t i = 0;  OB_SUCCESS == err && i < param_->get_return_columns().get_array_index(); i++)
      {
        err = this->append(empty_cell, cell_out);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to append return columns into aggregate cell array [err:%d]", err);
        }
      }

      for (int64_t i = 0; 
          (i < static_cast<int64_t>(param_->get_aggregate_columns().get_array_index())) && (OB_SUCCESS == err); 
          i++)
      {
        err = param_->get_aggregate_columns().at(i)->init_aggregate_obj(empty_cell.value_);
        if (OB_SUCCESS == err)
        {
          err = ObCellArray::append(empty_cell, cell_out);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to append cell to cell array [err:%d]", err);
          }
        }
        else
        {
          TBSYS_LOG(WARN,"fail to init aggregate object [err:%d, idx:%ld]", err, i);
        }
      }
      if (OB_SUCCESS == err)
      {
        ObGroupKey agg_key;
        int64_t agg_row_beg = 0;
        err = agg_key.init(*this,*param_, agg_row_beg, ObCellArray::get_cell_size() - 1, ObGroupKey::AGG_KEY);
        if (OB_SUCCESS == err)
        {
          int32_t hash_err = group_hash_map_.set(agg_key,agg_row_beg);
          if (HASH_INSERT_SUCC != hash_err)
          {
            err = OB_ERROR;
            TBSYS_LOG(WARN,"fail to set hash value of current group [err:%d]", hash_err);
          }
        }
        else
        {
          TBSYS_LOG(WARN,"fail to init aggregate group key [err:%d]", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        err = append_fake_composite_column(rowkey, table_id);
      }
      return err;
    }
    
    int ObGroupByOperator::add_row(const ObCellArray & org_cells, 
                                   const int64_t row_beg, const int64_t row_end)
    {
      int err = OB_SUCCESS;
      ObGroupKey org_key;
      ObGroupKey agg_key;
      ObInnerCellInfo * cell_out = NULL;
      if (NULL == param_)
      {
        TBSYS_LOG(WARN,"initialize first [param_:%p]", param_);
        err = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == err)
      {
        if (row_beg < 0
            || row_end < 0
            || row_beg > row_end
            || row_end >= org_cells.get_cell_size())
        {
          TBSYS_LOG(WARN,"param error [row_beg:%ld,row_end:%ld,org_cells.get_cell_size():%ld]", 
                    row_beg, row_end, org_cells.get_cell_size());
          err = OB_INVALID_ARGUMENT;
        }
      }
      if (OB_SUCCESS == err)
      {
        // need not aggregate
        if (param_->get_aggregate_row_width() == 0)
        {
          for (int64_t i = row_beg; i <= row_end && OB_SUCCESS == err; i++)
          {
            err = ObCellArray::append(org_cells[i], cell_out);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN,"fail to append cell to cell array [err:%d]", err);
            }
          }
        }
        else
        {
          err = org_key.init(org_cells,*param_,row_beg,row_end, ObGroupKey::ORG_KEY);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to initialize group key [err:%d]", err);
          }
          if (OB_SUCCESS == err)
          {
            int64_t agg_row_beg = -1;
            int hash_err = OB_SUCCESS;
            hash_err = group_hash_map_.get(org_key,agg_row_beg);
            if (HASH_EXIST == hash_err)
            {
              err = param_->aggregate(org_cells,row_beg, row_end, *this, agg_row_beg, 
                                      agg_row_beg + param_->get_aggregate_row_width() - 1);
              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(WARN,"fail to aggregate org row [err:%d]", err);
              }
            }
            else
            {
              agg_row_beg = ObCellArray::get_cell_size();
              err = param_->aggregate(org_cells,row_beg, row_end, *this, agg_row_beg, 
                                      agg_row_beg + param_->get_aggregate_row_width() - 1);
              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(WARN,"fail to aggregate org row [err:%d]", err);
              }
              if (OB_SUCCESS == err)
              {
                err = agg_key.init(*this,*param_, agg_row_beg, ObCellArray::get_cell_size() - 1, ObGroupKey::AGG_KEY);
                if (OB_SUCCESS != err)
                {
                  TBSYS_LOG(WARN,"fail to init aggregate group key [err:%d]", err);
                }
              }
              if (OB_SUCCESS == err)
              {
                hash_err = group_hash_map_.set(agg_key,agg_row_beg);
                if (HASH_INSERT_SUCC != hash_err)
                {
                  err = OB_ERROR;
                  TBSYS_LOG(WARN,"fail to set hash value of current group [err:%d]", hash_err);
                }
              }
              if (OB_SUCCESS == err)
              {
                err = append_fake_composite_column((*this)[agg_row_beg].row_key_, 
                                                   (*this)[agg_row_beg].table_id_);
              }
            }
          }
        }
      }
      return err;
    }

    int ObGroupByOperator::copy_row(
      const ObCellArray & agg_cells, const int64_t row_beg, const int64_t row_end)
    {
      int ret = OB_SUCCESS;
      ObGroupKey agg_key;
      ObInnerCellInfo * cell_out = NULL;

      if (NULL == param_)
      {
        TBSYS_LOG(WARN,"initialize first [param_:%p]", param_);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (row_beg < 0
          || row_end < 0
          || row_beg > row_end
          || row_end >= agg_cells.get_cell_size())
      {
        TBSYS_LOG(WARN,"param error [row_beg:%ld,row_end:%ld,agg_cells.get_cell_size():%ld]", 
                  row_beg, row_end, agg_cells.get_cell_size());
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        int64_t agg_row_beg = -1;
        int hash_err = OB_SUCCESS;
        agg_row_beg = ObCellArray::get_cell_size();

        for (int64_t i = row_beg; i <= row_end && OB_SUCCESS == ret; i++)
        {
          ret = ObCellArray::append(agg_cells[i], cell_out);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN,"fail to append cell to cell array [err:%d]", ret);
          }
        }

        if (OB_SUCCESS == ret && param_->get_aggregate_row_width() > 0)
        {
          ret = agg_key.init(*this, *param_, agg_row_beg, 
            ObCellArray::get_cell_size() - 1, ObGroupKey::AGG_KEY);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN,"fail to init aggregate group key [ret:%d]", ret);
          }
          else
          {
            hash_err = group_hash_map_.set(agg_key, agg_row_beg);
            if (HASH_INSERT_SUCC != hash_err)
            {
              ret = OB_ERROR;
              TBSYS_LOG(WARN,"fail to set hash value of current group [err:%d]", hash_err);
            }
          }
        }
      }

      return ret;
    }

    int ObGroupByOperator::copy_topk_row(
      const ObCellArray& org_cells, const int64_t row_width)
    {
      int ret = OB_SUCCESS;
      int64_t* heap = NULL;
      int64_t heap_size = 0;
      
      org_cells.get_topk_heap(heap, heap_size);
      if (NULL == heap || heap_size <= 0 || row_width <= 0)
      {
        TBSYS_LOG(WARN, "orgin cell array exclude topk heap info, "
                        "heap=%p, heap_size=%ld, row_width=%ld", 
          heap, heap_size, row_width);
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        for (int64_t i = 0; i < heap_size && OB_SUCCESS == ret; ++i)
        {
          ret = copy_row(org_cells, heap[i], heap[i] + row_width - 1);
        }
      }

      return ret;
    }

    int ObGroupByOperator::remove_group(const ObGroupKey& key)
    {
      return group_hash_map_.erase(key);
    }

    bool ObGroupByOperator::has_group(const ObGroupKey& key)
    {
      bool bret = false;
      int err = OB_SUCCESS;
      int64_t row_beg = 0;

      err = group_hash_map_.get(key, row_beg);
      if (HASH_EXIST == err)
      {
        bret = true;
      }

      return bret;
    }

  } // end namespace common
} // end namespace oceanbase
