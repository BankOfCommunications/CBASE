/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_compact_cell_iterator.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_compact_cell_iterator.h"
#include "ob_object.h"

using namespace oceanbase;
using namespace common;

ObCompactCellIterator::ObCompactCellIterator()
  :column_id_(OB_INVALID_ID),
  row_start_(0),
  store_type_(SPARSE),
  step_(0),
  is_row_finished_(true),
  inited_(false)
{
}

int ObCompactCellIterator::init(const char *buf, enum ObCompactStoreType store_type /* = SPARSE */)
{
  int ret = OB_SUCCESS;
  if(NULL == buf)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "buf is null");
  }
  else if(OB_SUCCESS != (ret = buf_reader_.assign(buf)))
  {
    TBSYS_LOG(WARN, "buf read assign fail:ret[%d]", ret);
  }
  else
  {
    row_start_ = 0;
    store_type_ = store_type;
    step_ = 0;
    is_row_finished_ = true;
    inited_ = true;
  }
  return ret;
}

int ObCompactCellIterator::init(const ObString &buf, enum ObCompactStoreType store_type /* = SPARSE */)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS != (ret = buf_reader_.assign(buf.ptr(), buf.length())))
  {
    TBSYS_LOG(WARN, "buf read assign:ret[%d]", ret);
  }
  else
  {
    store_type_ = store_type;
    step_ = 0;
    inited_ = true;
    is_row_finished_ = true;
    row_start_ = 0;
  }
  return ret;
}

int ObCompactCellIterator::get_next_cell(ObObj& value,
    bool& is_row_finished)
{
  int ret = OB_SUCCESS;

  if (!inited_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "not init yet");
  }
  else if (is_row_finished_)
  {
    is_row_finished_ = false;
    row_start_ = buf_reader_.pos();
  }

  if (OB_SUCCESS == ret)
  {
    switch(store_type_)
    {
      case SPARSE:
        TBSYS_LOG(WARN, "not support");
        break;
      case DENSE:
        TBSYS_LOG(WARN, "not support");
        break;
      case DENSE_SPARSE:
      case DENSE_DENSE:
        if (0 == step_)
        {
          ret = parse(buf_reader_, value);
          if (OB_SUCCESS != ret && OB_ITER_END != ret)
          {
            TBSYS_LOG(WARN, "parse cell fail:ret[%d]", ret);
          }
          else if (ObExtendType == value.get_type()
              && ObActionFlag::OP_END_ROW == value.get_ext())
          {
            step_ = 1;
          }
        }
        else if (1 == step_)
        {
          if (DENSE_SPARSE == store_type_)
          {
            ret = parse(buf_reader_, value, &column_id_);
            if (OB_SUCCESS != ret && OB_ITER_END != ret)
            {
              TBSYS_LOG(WARN, "parse cell fail:ret[%d]", ret);
            }
            else if (ObExtendType == value.get_type()
                && ObActionFlag::OP_END_ROW == value_.get_ext())
            {
              step_ = 0;
            }
          }
          else
          {
            ret = parse(buf_reader_, value);
            if(OB_SUCCESS != ret && OB_ITER_END != ret)
            {
              TBSYS_LOG(WARN, "parse cell fail:ret[%d]", ret);
            }
            else if(ObExtendType == value_.get_type()
                && ObActionFlag::OP_END_ROW == value_.get_ext())
            {
              step_ = 0;
            }
          }
        }
        break;
      default:
        TBSYS_LOG(WARN, "ERROR");
        ret = OB_ERROR;
        break;
    }

    if(OB_SUCCESS == ret)
    {
      if(ObExtendType == value.get_type()
          && ObActionFlag::OP_END_ROW == value.get_ext())
      {
        is_row_finished_ = true;
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    is_row_finished = is_row_finished_;
  }

  return ret;
}

int ObCompactCellIterator::get_next_cell(ObObj& value,
    uint64_t& column_id, bool& is_row_finished)
{
  int ret = OB_SUCCESS;

  if (!inited_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "not init yet");
  }
  else if (is_row_finished_)
  {
    is_row_finished_ = false;
    row_start_ = buf_reader_.pos();
  }

  if (OB_SUCCESS == ret)
  {
    switch(store_type_)
    {
      case SPARSE:
        TBSYS_LOG(WARN, "not support");
        break;
      case DENSE:
        TBSYS_LOG(WARN, "not support");
        break;
      case DENSE_SPARSE:
      case DENSE_DENSE:
        if (0 == step_)
        {
          ret = parse(buf_reader_, value);
          if (OB_SUCCESS != ret && OB_ITER_END != ret)
          {
            TBSYS_LOG(WARN, "parse cell fail:ret[%d]", ret);
          }
          else if (ObExtendType == value.get_type()
              && ObActionFlag::OP_END_ROW == value.get_ext())
          {
            step_ = 1;
          }
        }
        else if (1 == step_)
        {
          if (DENSE_SPARSE == store_type_)
          {
            ret = parse(buf_reader_, value, &column_id);
            if (OB_SUCCESS != ret && OB_ITER_END != ret)
            {
              TBSYS_LOG(WARN, "parse cell fail:ret[%d]", ret);
            }
            else if (ObExtendType == value.get_type()
                && ObActionFlag::OP_END_ROW == value_.get_ext())
            {
              step_ = 0;
            }
          }
          else
          {
            ret = parse(buf_reader_, value);
            if(OB_SUCCESS != ret && OB_ITER_END != ret)
            {
              TBSYS_LOG(WARN, "parse cell fail:ret[%d]", ret);
            }
            else if(ObExtendType == value_.get_type()
                && ObActionFlag::OP_END_ROW == value_.get_ext())
            {
              step_ = 0;
            }
          }
        }
        break;
      default:
        TBSYS_LOG(WARN, "ERROR");
        ret = OB_ERROR;
        break;
    }

    if(OB_SUCCESS == ret)
    {
      if(ObExtendType == value.get_type()
          && ObActionFlag::OP_END_ROW == value.get_ext())
      {
        is_row_finished_ = true;
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    is_row_finished = is_row_finished_;
  }

  return ret;
}

int ObCompactCellIterator::next_cell()
{
  int ret = OB_SUCCESS;
  if(!inited_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "not init yet");
  }

  if(OB_SUCCESS == ret && is_row_finished_)
  {
    is_row_finished_ = false;
    row_start_ = buf_reader_.pos();
  }

  if(OB_SUCCESS == ret)
  {
    column_id_ = OB_INVALID_ID;
    switch(store_type_)
    {
    case SPARSE:
      ret = parse(buf_reader_, value_, &column_id_);
      if(OB_SUCCESS != ret && OB_ITER_END != ret)
      {
        TBSYS_LOG(WARN, "parse cell fail:ret[%d]", ret);
      }
      break;
    case DENSE:
      ret = parse(buf_reader_, value_);
      if(OB_SUCCESS != ret && OB_ITER_END != ret)
      {
        TBSYS_LOG(WARN, "parse cell fail:ret[%d]", ret);
      }
      break;
    case DENSE_SPARSE:
    case DENSE_DENSE:
      if(0 == step_)
      {
        ret = parse(buf_reader_, value_);
        if(OB_SUCCESS != ret && OB_ITER_END != ret)
        {
          TBSYS_LOG(WARN, "parse cell fail:ret[%d]", ret);
        }
        else if(ObExtendType == value_.get_type() && ObActionFlag::OP_END_ROW == value_.get_ext())
        {
          step_ = 1;
        }
      }
      else if(1 == step_)
      {
        if(DENSE_SPARSE == store_type_)
        {
          ret = parse(buf_reader_, value_, &column_id_);
          if(OB_SUCCESS != ret && OB_ITER_END != ret)
          {
            TBSYS_LOG(WARN, "parse cell fail:ret[%d]", ret);
          }
          else if(ObExtendType == value_.get_type() && ObActionFlag::OP_END_ROW == value_.get_ext())
          {
            step_ = 0;
          }
        }
        else
        {
          ret = parse(buf_reader_, value_);
          if(OB_SUCCESS != ret && OB_ITER_END != ret)
          {
            TBSYS_LOG(WARN, "parse cell fail:ret[%d]", ret);
          }
          else if(ObExtendType == value_.get_type() && ObActionFlag::OP_END_ROW == value_.get_ext())
          {
            step_ = 0;
          }
        }
      }
      break;
    default:
      TBSYS_LOG(WARN, "ERROR");
      ret = OB_ERROR;
      break;
    }

    if(OB_SUCCESS == ret)
    {
      if(ObExtendType == value_.get_type() && ObActionFlag::OP_END_ROW == value_.get_ext())
      {
        is_row_finished_ = true;
      }
    }
  }
  return ret;
}


int ObCompactCellIterator::get_cell(const ObObj *&value, bool *is_row_finished /* = NULL */, ObString *row /* = NULL */)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_INVALID_ID;
  if(OB_SUCCESS != (ret = get_cell(column_id, value, is_row_finished, row)))
  {
    TBSYS_LOG(WARN, "get cell fail:ret[%d]", ret);
  }
  else
  {
    if(OB_INVALID_ID != column_id)
    {
      ret = OB_NOT_SUPPORTED;
      TBSYS_LOG(WARN, "column_id should be OB_INVALID_ID. this function only is used in dense format!");
    }
  }
  return ret;
}

int ObCompactCellIterator::get_cell(uint64_t &column_id, ObObj &value, bool *is_row_finished, ObString *row)
{
  int ret = OB_SUCCESS;
  const ObObj *cell = NULL;
  if(!inited_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "not init yet");
  }
  else if(OB_SUCCESS != (ret = get_cell(column_id, cell, is_row_finished, row)))
  {
    TBSYS_LOG(WARN, "get cell fail:ret[%d]", ret);
  }
  else
  {
    value = *cell;
  }
  return ret;
}


int ObCompactCellIterator::get_cell(uint64_t &column_id, const ObObj *&value, bool *is_row_finished, ObString *row)
{
  int ret = OB_SUCCESS;

  if(!inited_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "not init yet");
  }

  if(OB_SUCCESS == ret)
  {
    column_id = column_id_;
    value = &value_;

    if(NULL != is_row_finished)
    {
      *is_row_finished = is_row_finished_;
    }

    if(is_row_finished_ && NULL != row)
    {
      row->assign_ptr(const_cast<char*>(buf_reader_.buf() + row_start_), (int32_t)(buf_reader_.pos() - row_start_));
    }
  }
  return ret;
}

int ObCompactCellIterator::parse(ObBufferReader &buf_reader, ObObj &value, uint64_t *column_id /* = NULL */) const
{
  int ret = OB_SUCCESS;

  const ObCellMeta *cell_meta = NULL;
  ret = buf_reader.get<ObCellMeta>(cell_meta);

  const int8_t *int8_value = NULL;
  const int16_t *int16_value = NULL;
  const int32_t *int32_value = NULL;
  const int64_t *int64_value = NULL;
  const float *float_value = NULL;
  const double *double_value = NULL;
  const bool *bool_value = NULL;
  //add peiouya [DATE_TIME] 20150906:b
  const ObDate *date_value = NULL;
  const ObTime *time_value = NULL;
  //add 20150906:e
  const ObDateTime *datetime_value = NULL;
  const ObPreciseDateTime *precise_datetime_value = NULL;
  const ObCreateTime *createtime_value = NULL;
  const ObModifyTime *modifytime_value = NULL;
  bool is_add = false;

#define case_clause_with_add(type_name, type, set_type) \
  ret = buf_reader.get<type_name>((const type_name*&)type##_value); \
  if(OB_SUCCESS == ret) \
  { \
    value.set_##set_type(*type##_value, is_add); \
  } \
  break


#define case_clause(type_name, type) \
  ret = buf_reader.get<type_name>((const type_name*&)type##_value); \
  if(OB_SUCCESS == ret) \
  { \
    value.set_##type(*type##_value); \
  } \
  break

  if(OB_SUCCESS == ret)
  {
    is_add = cell_meta->attr_ == ObCellMeta::AR_ADD ? true : false;

    switch(cell_meta->type_)
    {
    case ObCellMeta::TP_NULL:
      value.set_null();
      break;
    case ObCellMeta::TP_INT8:
      case_clause_with_add(int8_t, int8, int);
    case ObCellMeta::TP_INT16:
      case_clause_with_add(int16_t, int16, int);
    case ObCellMeta::TP_INT32:
    //mod lijianqiang [INT_32] 20151012:b
//        case_clause_with_add(int32_t, int32, int);
      case_clause_with_add(int32_t, int32, int32);
    //mod 20151012:e
    case ObCellMeta::TP_DECIMAL:
      if(OB_SUCCESS != (ret = parse_decimal(buf_reader, value)))
      {
        TBSYS_LOG(WARN, "parse number fail:ret[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = value.set_add(is_add)))
      {
        TBSYS_LOG(WARN, "set add fail:ret[%d]", ret);
      }
      break;
    case ObCellMeta::TP_INT64:
      case_clause_with_add(int64_t, int64, int);
    case ObCellMeta::TP_TIME:
      case_clause_with_add(ObDateTime, datetime, datetime);
    case ObCellMeta::TP_PRECISE_TIME:
      case_clause_with_add(ObPreciseDateTime, precise_datetime, precise_datetime);
    case ObCellMeta::TP_CREATE_TIME:
      case_clause(ObCreateTime, createtime);
    case ObCellMeta::TP_MODIFY_TIME:
      case_clause(ObModifyTime, modifytime);
    case ObCellMeta::TP_FLOAT:
      case_clause_with_add(float, float, float);
    case ObCellMeta::TP_DOUBLE:
      case_clause_with_add(double, double, double);
    case ObCellMeta::TP_BOOL:
      case_clause(bool, bool);

    case ObCellMeta::TP_VARCHAR:
      ret = parse_varchar(buf_reader, value);
      break;

    case ObCellMeta::TP_ESCAPE:
      switch(cell_meta->attr_)
      {
      case ObCellMeta::ES_END_ROW:
        value.set_ext(ObActionFlag::OP_END_ROW);
        break;
      case ObCellMeta::ES_DEL_ROW:
        value.set_ext(ObActionFlag::OP_DEL_ROW);
        break;
      //add zhaoqiong [Truncate Table]:20160318:b
      case ObCellMeta::ES_TRUN_TAB:
        value.set_ext(ObActionFlag::OP_DEL_ROW);
        break;
      //add:e
      case ObCellMeta::ES_NOP_ROW:
        value.set_ext(ObActionFlag::OP_NOP);
        break;
      case ObCellMeta::ES_NOT_EXIST_ROW:
        value.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
        break;
      case ObCellMeta::ES_VALID:
        value.set_ext(ObActionFlag::OP_VALID);
        break;
      case ObCellMeta::ES_NEW_ADD:
        value.set_ext(ObActionFlag::OP_NEW_ADD);
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        TBSYS_LOG(WARN, "unsupported escape %d", cell_meta->attr_);
      }
      break;
    case ObCellMeta::TP_EXTEND:
      switch(cell_meta->attr_)
      {
      case ObCellMeta::AR_MIN:
        value.set_min_value();
        break;
      case ObCellMeta::AR_MAX:
        value.set_max_value();
        break;
      case ObCellMeta::ES_DEL_ROW:
        value.set_ext(ObActionFlag::OP_DEL_ROW);
        break;
      //add zhaoqiong [Truncate Table]:20160318:b
      case ObCellMeta::ES_TRUN_TAB:
        value.set_ext(ObActionFlag::OP_DEL_ROW);
        break;
      //add:e
      default:
        ret = OB_NOT_SUPPORTED;
        TBSYS_LOG(WARN, "unsupported extend:cell_meta->attr_=%d",
            cell_meta->attr_);
      }
      break;
    //add peiouya [DATE_TIME] 20150906:b
    case ObCellMeta::TP_DATE:
      case_clause_with_add(ObDate, date, date);
    case ObCellMeta::TP_TIME2:
      case_clause_with_add(ObTime, time, time);
    //add 20150906:e
    default:
      ret = OB_NOT_SUPPORTED;
      TBSYS_LOG(WARN, "unsupported type %d", cell_meta->type_);
    }
  }

  if(NULL != column_id)
  {
    if(OB_SUCCESS == ret && ObCellMeta::TP_ESCAPE != cell_meta->type_)
    {
      const uint32_t *tmp_column_id = NULL;
      ret = buf_reader.get<uint32_t>(tmp_column_id);
      if(OB_SUCCESS == ret)
      {
        if(*tmp_column_id == OB_COMPACT_COLUMN_INVALID_ID)
        {
          *column_id = OB_INVALID_ID;
        }
        else
        {
          *column_id = *tmp_column_id;
        }
      }
    }
    else
    {
      *column_id = OB_INVALID_ID;
    }
  }

  return ret;
}

int ObCompactCellIterator::parse_varchar(ObBufferReader &buf_reader, ObObj &value) const
{
  int ret = OB_SUCCESS;
  const int32_t *varchar_len = NULL;
  ObString str_value;

  ret = buf_reader.get<int32_t>(varchar_len);
  if(OB_SUCCESS == ret)
  {
    str_value.assign_ptr(const_cast<char*>(buf_reader.cur_ptr()), *varchar_len);
    buf_reader.skip(*varchar_len);
    value.set_varchar(str_value);
  }
  return ret;
}

int ObCompactCellIterator::parse_decimal(ObBufferReader &buf_reader, ObObj &value) const
{
  //modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
  /*int ret = OB_SUCCESS;
  const uint8_t *vscale = NULL;
  const ObDecimalMeta *dec_meta = NULL;
  const uint32_t *word = NULL;
  uint32_t *words = NULL;

  if(OB_SUCCESS != (ret = buf_reader.get<uint8_t>(vscale)))
  {
    TBSYS_LOG(WARN, "read vscale fail:ret[%d]", ret);
  }
  else if(OB_SUCCESS != (ret = buf_reader.get<ObDecimalMeta>(dec_meta)))
  {
    TBSYS_LOG(WARN, "get decimal meta fail:ret[%d]", ret);
  }
  else
  {
    value.meta_.type_ = ObDecimalType;
    value.meta_.dec_vscale_ = *vscale & ObObj::META_VSCALE_MASK;
    value.meta_.dec_precision_ = dec_meta->dec_precision_;
    value.meta_.dec_scale_ = dec_meta->dec_scale_;
    value.meta_.dec_nwords_ = dec_meta->dec_nwords_;
  }

  uint8_t nwords = value.meta_.dec_nwords_;
  nwords ++;

  if (OB_SUCCESS == ret)
  {
    if (nwords <= 3)
    {
      words = reinterpret_cast<uint32_t*>(&(value.val_len_));
    }
    else
    {
      //@todo, use ob_pool.h to allocate memory
      ret = OB_NOT_IMPLEMENT;
    }
  }

  if(OB_SUCCESS == ret)
  {
    for(int8_t i=0;OB_SUCCESS == ret && i<nwords;i++)
    {
      if(OB_SUCCESS != (ret = buf_reader.get<uint32_t>(word)))
      {
        TBSYS_LOG(WARN, "get word fail:ret[%d]", ret);
      }
      else
      {
        words[i] = *word;
      }
    }
  }
*/
    //old code
    int ret = OB_SUCCESS;
    const uint8_t *vscale = NULL;
    const ObDecimalMeta *dec_meta = NULL;
    const int32_t *varchar_len = NULL;
    ObString str_value;

    if(OB_SUCCESS != (ret = buf_reader.get<uint8_t>(vscale)))
    {
        TBSYS_LOG(WARN, "read vscale fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = buf_reader.get<ObDecimalMeta>(dec_meta)))
    {
        TBSYS_LOG(WARN, "get decimal meta fail:ret[%d]", ret);
    }
    else
    {
        value.meta_.type_ = ObDecimalType;
    }
     if(OB_SUCCESS == ret)
       {
        if(OB_SUCCESS != (ret = buf_reader.get<int32_t>(varchar_len))){
        TBSYS_LOG(WARN, "get varchar_len error");
        }

        str_value.assign_ptr(const_cast<char*>(buf_reader.cur_ptr()), *varchar_len);
       // TBSYS_LOG(WARN, "parse_decimal=%.*s", str_value.length(),str_value.ptr());
        buf_reader.skip(*varchar_len);
        ret=value.set_decimal(str_value);
        value.meta_.dec_vscale_ = *vscale & ObObj::META_VSCALE_MASK;
        value.meta_.dec_precision_ = dec_meta->dec_precision_;
        value.meta_.dec_scale_ = dec_meta->dec_scale_;

       }

  return ret;
}

