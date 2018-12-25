/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_get_param_cell_iterator.cpp for iterate cell in get param.
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "tbsys.h"
#include "ob_get_param_cell_iterator.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;

    ObCellInfo ObGetParamCellIterator::fake_cell_;

    ObGetParamCellIterator::ObGetParamCellIterator()
    {
      get_param_ = NULL;
      cell_idx_ = -1;
      cell_size_ = 0;
    }
    ObGetParamCellIterator::~ObGetParamCellIterator()
    {
      get_param_ = NULL;
      cell_idx_ = -1;
      cell_size_ = 0;
    }
  
    int ObGetParamCellIterator::init(const ObGetParam & get_param, int64_t cell_idx)
    {
      int err = OB_SUCCESS;
      if (cell_idx < 0 || cell_idx > get_param.get_cell_size())
      {
        TBSYS_LOG(ERROR, "logic error, cell index out of range [cell_idx:%ld,"
          "get_param.get_cell_size:%ld]", cell_idx, get_param.get_cell_size());
        err = OB_SIZE_OVERFLOW;
      }
      if (OB_SUCCESS == err)
      {
        get_param_ = &get_param;
        cell_size_ = get_param_->get_cell_size();
        cell_idx_ = cell_idx;
      }
      else
      {
        get_param_ = NULL;
        cell_idx_ = -1;
        cell_size_ = 0;
      }
      return err;
    }
  
    ObGetParamCellIterator &ObGetParamCellIterator::operator ++()
    {
      cell_idx_ ++;
      return *this;
    }
  
    ObGetParamCellIterator ObGetParamCellIterator::operator ++(int)
    {
      ObGetParamCellIterator res;
      res = *this;
      cell_idx_ ++;
      return res;
    }
  
    bool ObGetParamCellIterator::operator==(const ObGetParamCellIterator& other)
    {
      return((get_param_ == other.get_param_) && (cell_idx_ == other.cell_idx_));
    }
  
    bool ObGetParamCellIterator::operator!=(const ObGetParamCellIterator& other)
    {
      return !(*this == other);
    }
  
    ObCellInfo *ObGetParamCellIterator::operator->()
    {
      ObCellInfo *result = NULL;
      if (cell_idx_ < 0 || cell_idx_ >= cell_size_)
      {
        TBSYS_LOG(ERROR, "logic error, try to access cell index out of range"
          "[get_param_:%p,cell_idex_:%ld,get_param_->get_cell_size:%ld]",
          get_param_, cell_idx_, get_param_ != NULL ? get_param_->get_cell_size() : 0);
        result = &fake_cell_;
      }
      else
      {
        result = get_param_->operator [](cell_idx_);
        if (NULL == result)
        {
          TBSYS_LOG(ERROR, "logic error, within range cell is null"
            "[get_param_:%p,cell_idex_:%ld,get_param_->get_cell_size:%ld]",
            get_param_, cell_idx_, get_param_ != NULL ? get_param_->get_cell_size() : 0);
          result = &fake_cell_;
        }
      }
      return result;
    }
  
    ObCellInfo &ObGetParamCellIterator::operator*()
    {
      return *(this->operator->());
    }
  } // end namespace chunkserver
} // end namespace oceanbase
