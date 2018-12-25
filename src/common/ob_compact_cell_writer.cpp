/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_compact_cell_writer.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_compact_cell_writer.h"

using namespace oceanbase;
using namespace common;

ObCompactCellWriter::ObCompactCellWriter()
  :store_type_(SPARSE)
{
}

int ObCompactCellWriter::append_row(const ObRowkey& rowkey, const ObRow& row)
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS != (ret = append_rowkey(rowkey)))
  {
    TBSYS_LOG(WARN, "append rowkey error:ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = append_rowvalue(row)))
  {
    TBSYS_LOG(WARN, "append rowvalue error:ret=%d", ret);
  }
  else
  {
    //do nothing
  }

  return ret;
}

int ObCompactCellWriter::append_row(const ObRow& row)
{
  int ret = OB_SUCCESS;

  //row obj array
  int64_t obj_array_cnt = 0;
  const ObObj* obj_array_ptr = row.get_obj_array(obj_array_cnt);

  //row desc array
  const ObRowDesc* row_desc = row.get_row_desc();
  int64_t desc_array_cnt = 0;
  const ObRowDesc::Desc* desc_array_ptr = NULL;

  //rowkey
  int64_t rowkey_column_num = 0;

  if (0 == obj_array_cnt || obj_array_cnt > OB_ROW_MAX_COLUMNS_COUNT)
  {
    TBSYS_LOG(WARN, "invalid row column count: obj_array_cnt=[%ld]", obj_array_cnt);
    ret = OB_ERROR;
  }
  else if (NULL == obj_array_ptr)
  {
    TBSYS_LOG(WARN, "obj array ptr is NULL");
    ret = OB_ERROR;
  }
  else if (NULL == row_desc)
  {
    TBSYS_LOG(WARN, "row desc is NULL");
    ret = OB_ERROR;
  }
  else if (NULL == (desc_array_ptr = row_desc->get_cells_desc_array(desc_array_cnt)))
  {
    TBSYS_LOG(WARN, "desc array ptr is NULL");
    ret = OB_ERROR;
  }
  else if (0 == desc_array_cnt && desc_array_cnt > OB_ROW_MAX_COLUMNS_COUNT)
  {
    TBSYS_LOG(WARN, "invalid desc array cnt: desc_array_cnt=[%ld]", desc_array_cnt);
    ret = OB_ERROR;
  }
  else
  {
    rowkey_column_num = row_desc->get_rowkey_cell_count();
    if (0 == rowkey_column_num || rowkey_column_num > OB_MAX_ROWKEY_COLUMN_NUMBER)
    {
      TBSYS_LOG(WARN, "invalid rowkey column num: rowkey_column_num=[%ld]", rowkey_column_num);
      ret = OB_ERROR;
    }
  }

  //rowkey
  for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey_column_num; i ++)
  {
    if (OB_SUCCESS != (ret = append(obj_array_ptr[i])))
    {
      TBSYS_LOG(WARN, "append rowkey faile: ret=[%d], obj=[%s]", ret, to_cstring(obj_array_ptr[i]));
      break;
    }
  }

  if (OB_SUCCESS == ret && OB_SUCCESS != (ret = rowkey_finish()))
  {
    TBSYS_LOG(WARN, "add rowkey finish flag fail: ret=[%d]", ret);
  }

  //row value
  for(int64_t i = rowkey_column_num; OB_SUCCESS == ret && i < obj_array_cnt; i ++)
  {
    if (DENSE_SPARSE == store_type_)
    {
      if (OB_SUCCESS != (ret = append(desc_array_ptr[i].column_id_, obj_array_ptr[i])))
      {
        TBSYS_LOG(WARN, "append fail: i=[%ld], ret=[%d], column_id=[%lu], obj=[%s]",
            i, ret, desc_array_ptr[i].column_id_, to_cstring(obj_array_ptr[i]));
      }
    }
    else if (DENSE_DENSE == store_type_)
    {
      if (OB_SUCCESS != (ret = append(obj_array_ptr[i])))
      {
        TBSYS_LOG(WARN, "append fail: i=[%ld], ret=[%d], obj=[%s]", i, ret, to_cstring(obj_array_ptr[i]));
      }
    }
    else
    {
      TBSYS_LOG(WARN, "unsupport store type: i=[%ld], store_type_=[%d]", i, store_type_);
    }
  }

  if (OB_SUCCESS == ret && OB_SUCCESS != (ret = row_finish()))
  {
    TBSYS_LOG(WARN, "row finish error: ret=[%d]", ret);
  }

  return ret;
}

int ObCompactCellWriter::append_rowkey(const ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  for(int64_t i=0;OB_SUCCESS == ret && i<rowkey.get_obj_cnt();i++)
  {
    const ObObj &rowkey_obj = rowkey.get_obj_ptr()[i];
    if(OB_SUCCESS != (ret = append(rowkey_obj)))
    {
      TBSYS_LOG(WARN, "append rowkey fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = rowkey_finish()))
    {
      TBSYS_LOG(WARN, "add rowkey finish flag fail:ret[%d]", ret);
    }
  }
  return ret;
}

int ObCompactCellWriter::append_rowvalue(const ObRow& row)
{
  int ret = OB_SUCCESS;

  uint64_t table_id = 0;
  uint64_t column_id = 0;
  const ObObj* obj = NULL;
  int64_t column_num = row.get_column_num();

  for (int64_t i = 0; i < column_num; i ++)
  {
    if (OB_SUCCESS != (ret = row.raw_get_cell(i, obj, table_id, column_id)))
    {
      TBSYS_LOG(WARN, "raw get cell fail:ret=%d", ret);
      break;
    }
    else
    {
      if (DENSE_SPARSE == store_type_)
      {
        if (OB_SUCCESS != (ret = append(column_id, *obj)))
        {
          TBSYS_LOG(WARN, "append fail:ret=%d", ret);
          break;
        }
      }
      else if (DENSE_DENSE == store_type_)
      {
        if (OB_SUCCESS != (ret = append(*obj)))
        {
          TBSYS_LOG(WARN, "append fail:ret=%d", ret);
          break;
        }
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (0 == column_num)
    {
      //do nothing
    }
    else if (OB_SUCCESS != (ret = row_finish()))
    {
      TBSYS_LOG(WARN, "finish row error:ret=%d", ret);
    }
    else
    {
      //do nothing
    }
  }

  return ret;
}
int ObCompactCellWriter::init(char *buf, int64_t size, enum ObCompactStoreType store_type)
{
  int ret = OB_SUCCESS;
  if(NULL == buf)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "buf is null");
  }
  else if(OB_SUCCESS != (ret = buf_writer_.assign(buf, size)))
  {
    TBSYS_LOG(WARN, "buf write assign fail:ret[%d]", ret);
  }
  else
  {
    store_type_ = store_type;
  }
  return ret;
}

/*
int ObCompactCellWriter::write_decimal(const ObObj &decimal)
{
  int ret = OB_SUCCESS;
  ObDecimalMeta dec_meta;
  dec_meta.dec_precision_ = decimal.meta_.dec_precision_;
  dec_meta.dec_scale_ = decimal.meta_.dec_scale_;
  dec_meta.dec_nwords_ = decimal.meta_.dec_nwords_;

  const uint32_t *words = NULL;
  uint8_t nwords = decimal.meta_.dec_nwords_;
  nwords ++;

  if(OB_SUCCESS != (ret = buf_writer_.write<uint8_t>(decimal.meta_.dec_vscale_)))
  {
    TBSYS_LOG(WARN, "write dec vscale fail:ret[%d]", ret);
  }
  else if(OB_SUCCESS != (ret = buf_writer_.write<ObDecimalMeta>(dec_meta)))
  {
    TBSYS_LOG(WARN, "write dec_meta fail:ret[%d]", ret);
  }
  else
  {
    if(nwords <= 3)
    {
      words = reinterpret_cast<const uint32_t*>(&(decimal.val_len_));
    }
    else
    {
      words = decimal.value_.dec_words_;
    }

    for(uint16_t i=0;OB_SUCCESS == ret && i<=dec_meta.dec_nwords_;i++)
    {
      if(OB_SUCCESS != (ret = buf_writer_.write<uint32_t>(words[i])))
      {
        TBSYS_LOG(WARN, "write words fail:ret[%d]", ret);
      }
    }
  }

  return ret;
}
*/
//old code
//modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
int ObCompactCellWriter::write_decimal(const ObObj &decimal, ObObj *clone_value)
{
    int ret = OB_SUCCESS;

        ObDecimalMeta dec_meta;
        ObString str_deci;
        ObString str2;
        decimal.get_decimal(str_deci);
        //TBSYS_LOG(WARN, "write_decimal=%.*s", str_deci.length(),str_deci.ptr());
        dec_meta.dec_precision_ = decimal.meta_.dec_precision_;
        dec_meta.dec_scale_ = decimal.meta_.dec_scale_;
        dec_meta.dec_vscale_ = decimal.meta_.dec_vscale_;
        if (OB_SUCCESS
			!= (ret = buf_writer_.write < uint8_t > (decimal.meta_.dec_vscale_))) {
		TBSYS_LOG(WARN, "write dec vscale fail:ret[%d]", ret);
        } else if (OB_SUCCESS
			!= (ret = buf_writer_.write < ObDecimalMeta > (dec_meta))) {
		TBSYS_LOG(WARN, "write dec_meta fail:ret[%d]", ret);
        } else {

            if(OB_SUCCESS == ret)
            {
                ret = buf_writer_.write<int32_t>((int32_t)(str_deci.length()));
            }
            if(OB_SUCCESS == ret)
            {
                ret = buf_writer_.write_varchar(str_deci, &str2);
                // TBSYS_LOG(WARN, "write_decimal=%.*s", str2.length(),str2.ptr());

            }
            if(OB_SUCCESS == ret && NULL != clone_value)
            {
            if(OB_SUCCESS!=(ret=clone_value->set_decimal(str2))){
                TBSYS_LOG(ERROR,"write clone_value error!");
                }
            }
        }

	return ret;

}
//modify e


int ObCompactCellWriter::write_varchar(const ObObj &value, ObObj *clone_value)
{
  int ret = OB_SUCCESS;
  ObString varchar_value;
  ObString varchar_written;

  value.get_varchar(varchar_value);
  if(varchar_value.length() > INT32_MAX)
  {
    ret = OB_SIZE_OVERFLOW;
    TBSYS_LOG(WARN, "varchar is too long:[%d]", varchar_value.length());
  }
  if(OB_SUCCESS == ret)
  {
    ret = buf_writer_.write<int32_t>((int32_t)(varchar_value.length()));
  }
  if(OB_SUCCESS == ret)
  {
    ret = buf_writer_.write_varchar(varchar_value, &varchar_written);
  }
  if(OB_SUCCESS == ret && NULL != clone_value)
  {
    clone_value->set_varchar(varchar_written);
  }

  return ret;
}

int ObCompactCellWriter::append(uint64_t column_id, const ObObj &value, ObObj *clone_value)
{
  int ret = OB_SUCCESS;
  ObCellMeta cell_meta;
  cell_meta.attr_ = ObCellMeta::AR_NORMAL;
  int64_t int_value = 0;
  float float_value = 0.0f;
  double double_value = 0.0;
  bool bool_value = false;
  //add peiouya [DATE_TIME] 20150906:b
  ObDate date = 0;
  ObTime time = 0;
  //add 20150906:e
  //add lijianqiang [INT_32] 20150930:b
  int32_t int32_value = 0;
  //add 20150930:e
  ObDateTime datetime_value = 0;
  ObPreciseDateTime precise_datetime_value = 0;
  ObCreateTime createtime_value = 0;
  ObModifyTime modifytime_value = 0;

  if(NULL != clone_value)
  {
    *clone_value = value;
  }

  int64_t tmp_value = 0;

  //cell_meta.type_
  if(OB_SUCCESS == ret)
  {
    switch(value.get_type())
    {
    case ObNullType:
      cell_meta.type_ = ObCellMeta::TP_NULL;
      ret = buf_writer_.write<ObCellMeta>(cell_meta);
      break;
    case ObIntType:
      value.get_int(int_value);
      if (value.get_add_fast())
      {
        cell_meta.attr_ = ObCellMeta::AR_ADD;
      }
      switch(get_int_byte(int_value))
      {
      case 1:
        cell_meta.type_ = ObCellMeta::TP_INT8;
        ret = buf_writer_.write<ObCellMeta>(cell_meta);
        if (OB_SUCCESS == ret)
        {
          ret = buf_writer_.write<int8_t>((int8_t)int_value);
        }
        break;
      case 2:
        cell_meta.type_ = ObCellMeta::TP_INT16;
        ret = buf_writer_.write<ObCellMeta>(cell_meta);
        if (OB_SUCCESS == ret)
        {
          ret = buf_writer_.write<int16_t>((int16_t)int_value);
        }
        break;
      case 4:
      //del lijianqiang [INT_32] 20151012:b
//        cell_meta.type_ = ObCellMeta::TP_INT32;
//        ret = buf_writer_.write<ObCellMeta>(cell_meta);
//        if (OB_SUCCESS == ret)
//        {
//          ret = buf_writer_.write<int32_t>((int32_t)int_value);
//        }
//        break;
      //del 20151012:e
      case 8:
        cell_meta.type_ = ObCellMeta::TP_INT64;
        ret = buf_writer_.write<ObCellMeta>(cell_meta);
        if (OB_SUCCESS == ret)
        {
          ret = buf_writer_.write<int64_t>((int64_t)int_value);
        }
        break;
      }
      break;
    //add lijianqiang [INT_32] 20150930:b
    case ObInt32Type:
      value.get_int32(int32_value);
      if (value.get_add_fast())
      {
        cell_meta.attr_ = ObCellMeta::AR_ADD;
      }
      cell_meta.type_ = ObCellMeta::TP_INT32;
      ret = buf_writer_.write<ObCellMeta>(cell_meta);
      if (OB_SUCCESS == ret)
      {
        ret = buf_writer_.write<int32_t>(int32_value);
      }
      break;
    //add 20150930:e
    case ObDecimalType:
      cell_meta.type_ = ObCellMeta::TP_DECIMAL;
      ret = buf_writer_.write<ObCellMeta>(cell_meta);
      if (OB_SUCCESS == ret)
      {
       //modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
       // ret = write_decimal(value);//old code
       ret = write_decimal(value,clone_value);
       //modify e
      }
      break;
    case ObFloatType:
      cell_meta.type_ = ObCellMeta::TP_FLOAT;
      ret = buf_writer_.write<ObCellMeta>(cell_meta);
      if (OB_SUCCESS == ret)
      {
        value.get_float(float_value);
        ret = buf_writer_.write<float>(float_value);
      }
      break;
    case ObDoubleType:
      cell_meta.type_ = ObCellMeta::TP_DOUBLE;
      ret = buf_writer_.write<ObCellMeta>(cell_meta);
      if (OB_SUCCESS == ret)
      {
        value.get_double(double_value);
        ret = buf_writer_.write<double>(double_value);
      }
      break;
    case ObBoolType:
      cell_meta.type_ = ObCellMeta::TP_BOOL;
      ret = buf_writer_.write<ObCellMeta>(cell_meta);
      if (OB_SUCCESS == ret)
      {
        value.get_bool(bool_value);
        ret = buf_writer_.write<bool>(bool_value);
      }
      break;
    case ObDateTimeType:
      if (value.get_add_fast())
      {
        cell_meta.attr_ = ObCellMeta::AR_ADD;
      }
      cell_meta.type_ = ObCellMeta::TP_TIME;
      ret = buf_writer_.write<ObCellMeta>(cell_meta);
      if (OB_SUCCESS == ret)
      {
        value.get_datetime(datetime_value);
        ret = buf_writer_.write<ObDateTime>(datetime_value);
      }
      break;
    //add peiouya [DATE_TYPE] 20150831:b
    case ObDateType:
      if (value.get_add_fast())
      {
        cell_meta.attr_ = ObCellMeta::AR_ADD;
      }
      cell_meta.type_ = ObCellMeta::TP_DATE;
      ret = buf_writer_.write<ObCellMeta>(cell_meta);
      if (OB_SUCCESS == ret)
      {
        value.get_date(date);
        ret = buf_writer_.write<ObDate>(date);
      }
      break;
    case ObTimeType:
      if (value.get_add_fast())
      {
        cell_meta.attr_ = ObCellMeta::AR_ADD;
      }
      cell_meta.type_ = ObCellMeta::TP_TIME2;
      ret = buf_writer_.write<ObCellMeta>(cell_meta);
      if (OB_SUCCESS == ret)
      {
        value.get_time(time);
        ret = buf_writer_.write<ObTime>(time);
      }
      break;
    //add 20150831:e
    case ObPreciseDateTimeType:
      if (value.get_add_fast())
      {
        cell_meta.attr_ = ObCellMeta::AR_ADD;
      }
      cell_meta.type_ = ObCellMeta::TP_PRECISE_TIME;
      ret = buf_writer_.write<ObCellMeta>(cell_meta);
      if (OB_SUCCESS == ret)
      {
        value.get_precise_datetime(precise_datetime_value);
        ret = buf_writer_.write<ObPreciseDateTime>(precise_datetime_value);
      }
      break;
    case ObVarcharType:
      cell_meta.type_ = ObCellMeta::TP_VARCHAR;
      ret = buf_writer_.write<ObCellMeta>(cell_meta);
      if (OB_SUCCESS == ret)
      {
        ret = write_varchar(value, clone_value);
      }
      break;
    case ObCreateTimeType:
      cell_meta.type_ = ObCellMeta::TP_CREATE_TIME;
      ret = buf_writer_.write<ObCellMeta>(cell_meta);
      if (OB_SUCCESS == ret)
      {
        value.get_createtime(createtime_value);
        ret = buf_writer_.write<ObCreateTime>(createtime_value);
      }
      break;
    case ObModifyTimeType:
      cell_meta.type_ = ObCellMeta::TP_MODIFY_TIME;
      ret = buf_writer_.write<ObCellMeta>(cell_meta);
      if (OB_SUCCESS == ret)
      {
        value.get_modifytime(modifytime_value);
        ret = buf_writer_.write<ObModifyTime>(modifytime_value);
      }
      break;
    case ObExtendType:
      cell_meta.type_ = ObCellMeta::TP_EXTEND;
      if (OB_SUCCESS == value.get_ext(tmp_value))
      {
        if (ObObj::MIN_OBJECT_VALUE == tmp_value)
        {
          cell_meta.attr_ = ObCellMeta::AR_MIN;
        }
        else if (ObObj::MAX_OBJECT_VALUE == tmp_value)
        {
          cell_meta.attr_ = ObCellMeta::AR_MAX;
        }
        else if (ObActionFlag::OP_DEL_ROW == tmp_value)
        {
          cell_meta.attr_ = ObCellMeta::ES_DEL_ROW;
        }
        else
        {
          TBSYS_LOG(WARN, "extend type error:tmp_value=%ld", tmp_value);
          ret = OB_ERROR;
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = buf_writer_.write<ObCellMeta>(cell_meta);
      }
      break;
    default:
      TBSYS_LOG(WARN, "unsupported type:type[%d]", value.get_type());
      ret = OB_NOT_SUPPORTED;
      break;
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(column_id == OB_INVALID_ID)
    {
      if(SPARSE == store_type_)
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "cannot append column_id OB_INVALID_ID");
      }
    }
    else if(column_id > UINT32_MAX)
    {
      ret = OB_SIZE_OVERFLOW;
      TBSYS_LOG(WARN, "column_id is too long:%lu", column_id);
    }
    else
    {
      ret = buf_writer_.write<uint32_t>((uint32_t)column_id);
    }
  }
  return ret;
}

int ObCompactCellWriter::append_escape(int64_t escape)
{
  int ret = OB_SUCCESS;
  ObCellMeta cell_meta;

  if(escape < 0 || escape > 0x7)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "escape is invalid:[%ld]", escape);
  }
  else
  {
    cell_meta.type_ = ObCellMeta::TP_ESCAPE;
    cell_meta.attr_ = ((uint8_t)(escape) & 0x7);
    ret = buf_writer_.write<ObCellMeta>(cell_meta);
  }
  return ret;
}

