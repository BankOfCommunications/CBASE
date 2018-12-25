#include "ob_sequence.h"
#include "ob_values.h"
#include "ob_table_rpc_scan.h"
#include "ob_lock_v0.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObSequence::ObSequence()
{
  sequence_info_map_inited_ = false;
  use_lock_row_result_set_ = false;
  has_client_trans_ = false;
  has_cons_lock_row_desc_ = false;
  can_fill_ = true;
  has_checked_ = false;
  sequence_stmt_ = NULL;
  sql_context_ = NULL;
  out_result_set_ = NULL;
  //insert
  col_sequence_types_idx_ = 0;
  col_sequence_types_idx_idx_ = 0;
  //update
  project_for_update_ = NULL;
  out_put_columns_idx_ = 0;

  //add hongchen [SEQUENCE_BUG_FIX] 20170222:b
  init_global_prevval_state_and_lock_sequence_map();
  //add hongchen [] 20170222:e
}

ObSequence::~ObSequence()
{
}

void ObSequence::reset()
{
  //U can add what ever U want to reset here
  ObMultiChildrenPhyOperator::reset();
}

void ObSequence::reuse()
{
  //U can add what ever U want to reuse here
  ObMultiChildrenPhyOperator::reuse();
}

bool ObSequence::is_sequence_cond_id(uint64_t cond_id)
{
  UNUSED(cond_id);
  return OB_NOT_SUPPORTED;
}
int ObSequence::fill_the_sequence_info_to_cond_expr(ObSqlExpression *filter, uint64_t cond_expr_id)
{
  UNUSED(filter);
  UNUSED(cond_expr_id);
  return OB_NOT_SUPPORTED;
}
int ObSequence::add_sequence_info_to_map(ObString &sequence_name, SequenceInfo &s_info)
{
  int ret = OB_SUCCESS;
  if (-1 == (ret = sequence_info_map_.set(sequence_name,s_info)))
  {
    TBSYS_LOG(ERROR,"set (%.*s) to coiumn map failed!",sequence_name.length(),sequence_name.ptr());
    ret = OB_ERROR;
  }
  else if ((hash::HASH_EXIST == ret))//||( hash::HASH_INSERT_SUCC == ret))//
  {
    TBSYS_LOG(ERROR, "overwrite!");
    if (-1 == (ret = sequence_info_map_.set(sequence_name, s_info, 1)))//overWrite
    {
      TBSYS_LOG(ERROR,"Over write sequence info failed!");
    }
  }
  if ((ret == hash::HASH_INSERT_SUCC) || (ret == hash::HASH_OVERWRITE_SUCC))
  {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObSequence::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  //lock the row,get the result from "select * from __all_sequence where.. for update"
  if (use_lock_row_result_set_)
  {
    TBSYS_LOG(DEBUG,"lock get next row!");
    if (my_result_set_.is_with_rows() == true)
    {
      ObMySQLRow srow;
      if (OB_SUCCESS != (ret = my_result_set_.next_row(srow)))
      {
      }
      else
      {
        row = srow.get_inner_row();
//        TBSYS_LOG(ERROR,"the row is::[%s]",to_cstring(*(srow.get_inner_row())));
      }
    }
  }
  //quick path use
  else
  {
    ObValues *sequence_values = NULL;
    if (NULL == (sequence_values = dynamic_cast<ObValues*>(get_child(0))))
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "get the first child ::ObValues op of sequence failed!");
    }
    else
    {
      ret = sequence_values->get_next_row(row);
    }
  }
  return ret;
}
int ObSequence::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  UNUSED(row_desc);
  return OB_NOT_SUPPORTED;
}

int ObSequence::cons_sequence_row_desc(ObRowDesc& row_desc, const ObRowkeyInfo *&rowkey_info)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(sql_context_);
  OB_ASSERT(sql_context_->schema_manager_);
  const ObTableSchema *table_schema = NULL;
  if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(OB_ALL_SEQUENCE_TID)))
  {
    ret = OB_ERR_ILLEGAL_ID;
    TBSYS_LOG(ERROR, "fail to get table schema for table[%lu]", OB_ALL_SEQUENCE_TID);
  }
  else
  {
    rowkey_info = &table_schema->get_rowkey_info();
    int64_t rowkey_col_num = rowkey_info->get_size();
    row_desc.set_rowkey_cell_count(rowkey_col_num);
    /*1.cons primary key column(s)*/
    for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey_col_num; ++i) // for each primary key
    {
      const ObRowkeyColumn *rowkey_column = rowkey_info->get_column(i);
      OB_ASSERT(rowkey_column);
      if (OB_SUCCESS != (ret = row_desc.add_column_desc(OB_ALL_SEQUENCE_TID,
                                                        rowkey_column->column_id_)))
      {
        TBSYS_LOG(ERROR, "failed to add sequence row desc, err=%d", ret);
      }
    }// end for
    /* 2.cons other column(s)*/
    int64_t column_num = table_schema->get_max_column_id();
    for (int64_t j = OB_APP_MIN_COLUMN_ID; OB_SUCCESS == ret && j < column_num; j++)
    {
      uint64_t column_id = (uint64_t)(j);
      if(!rowkey_info->is_rowkey_column(column_id))
      {
        if (OB_SUCCESS != (ret = row_desc.add_column_desc(OB_ALL_SEQUENCE_TID,column_id)))
        {
          TBSYS_LOG(ERROR, "failed to add sequence row desc, err=%d", ret);
        }
      }
    }//end for
  }
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "the sequence row desc is::[%s]",to_cstring(row_desc));
  }
  return ret;
}

int ObSequence::init_sequence_info_map()
{
  int ret = OB_SUCCESS;
  if (!sequence_info_map_inited_)
  {
    if (OB_SUCCESS != (ret = sequence_info_map_.create(1000)))
    {
      TBSYS_LOG(ERROR, "create sequence_info_map_ fail:ret[%d]", ret);
    }
    else
    {
      sequence_info_map_inited_ = true;
    }
  }
  else
  {
    sequence_info_map_.clear();
  }
  return ret;
}

/**
 * @brief get prevval from the sequence_info_map_ by the key sequence_name
 * @param sequence_name [in]
 * @param prevval [out]
 * @param can_use_prevval [out]
 *
 */
int ObSequence::get_prevval_by_name(ObString *sequence_name, ObString &prevval, bool &can_use_prevval)
{
  int ret = OB_SUCCESS;
  SequenceInfo s_info;
  TBSYS_LOG(DEBUG, "sequence name :[%.*s]",sequence_name->length(),sequence_name->ptr());
  if (-1 == (ret = sequence_info_map_.get(*sequence_name, s_info)))
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "get sequence info failed!");
  }
  else if (hash::HASH_NOT_EXIST == ret)
  {
    TBSYS_LOG(ERROR, "get sequence [%.*s] failed! ret::[%d]",sequence_name->length(),sequence_name->ptr(),ret);
  }
  else
  {
    ret = OB_SUCCESS;
  }
  if (OB_SUCCESS == ret)
  {
    if (1 == s_info.can_use_prevval_)
    {
      can_use_prevval = true;
      if (OB_SUCCESS != (ret =  s_info.prevval_.get_decimal(prevval)))
      {
        TBSYS_LOG(ERROR,"get decimal value failed!");
      }
    }
  }
  return ret;
}

/**
 * @brief get nextval from the sequence_info_map_ by the key sequence_name
 * @param sequence_name [in]
 * @param nextval [out]
 * @param can_use_nextval [out]
 *
 */
int ObSequence::get_nextval_by_name(ObString *sequence_name, ObString &nextval, bool &can_use_nextval)
{
  int ret = OB_SUCCESS;
  SequenceInfo s_info;
  if (-1 == (ret = sequence_info_map_.get(*sequence_name, s_info)))
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "get sequence info failed!");
  }
  else if (hash::HASH_NOT_EXIST == ret)
  {
    TBSYS_LOG(ERROR, "get sequence [%.*s] failed! ret::[%d]",sequence_name->length(),sequence_name->ptr(),ret);
  }
  else
  {
    ret = OB_SUCCESS;
  }
  if (OB_SUCCESS == ret)
  {
    if (1 == s_info.valid_)
    {
      can_use_nextval = true;
      if (OB_SUCCESS != (ret =  s_info.current_value_.get_decimal(nextval)))
      {
        TBSYS_LOG(ERROR,"get decimal value failed!");
      }
    }
  }
  return ret;
}


/**
 *
 * @brief cast the sequenceinfo from varchar to decimal
 * @param s_value [in]
 * @param dec_current_value [out]
 * @param dec_min_value [out]
 * @param dec_max_value [out]
 * @param dec_inc_by [out]
 *
 */
int ObSequence::cast_sequence_info_to_decimal(SequenceInfo &s_value,
                                              ObDecimal &dec_current_value,
                                              ObDecimal &dec_min_value,
                                              ObDecimal &dec_max_value,
                                              ObDecimal &dec_inc_by)
{
  int ret = OB_SUCCESS;
  ObString min_value;
  ObString max_value;
  ObString current_value;
  ObString inc_by;
  //1.get the sequence info
  if (OB_SUCCESS != (ret = s_value.current_value_.get_decimal(current_value)))
  {
    TBSYS_LOG(ERROR, "get current value of sequence failed!");
  }
  else if (OB_SUCCESS != (ret = s_value.min_value_.get_decimal(min_value)))
  {
    TBSYS_LOG(ERROR, "get min value of sequence failed!");
  }
  else if (OB_SUCCESS != (ret = s_value.max_value_.get_decimal(max_value)))
  {
    TBSYS_LOG(ERROR, "get max value of sequence falied!");
  }
  else if (OB_SUCCESS != (ret = s_value.inc_by_.get_decimal(inc_by)))
  {
    TBSYS_LOG(ERROR, "get inc value of sequence falied!");
  }
  //2.cast to decimal
  if (OB_SUCCESS != (ret = dec_current_value.from(current_value.ptr(),current_value.length())))
  {
    TBSYS_LOG(ERROR, "cast current value to deciaml falied!");
  }
  else if (OB_SUCCESS != (ret = dec_min_value.from(min_value.ptr(),min_value.length())))
  {
    TBSYS_LOG(ERROR, "cast min value to deciaml falied!");
  }
  else if (OB_SUCCESS != (ret = dec_max_value.from(max_value.ptr(),max_value.length())))
  {
    TBSYS_LOG(ERROR, "cast max value to deciaml falied!");
  }
  else if(OB_SUCCESS != (ret = dec_inc_by.from(inc_by.ptr(),inc_by.length())))
  {
    TBSYS_LOG(ERROR, "cast inc by value to deciaml faliled!");
  }
  return ret;
}



/**
 *
 * @brief calculate nextval for current sequence come from SequenceInfo
 * @param s_value [in][out] in with the sequence info which come from sequence_info_map_ and out with the new nextval
 *
 */
int ObSequence::calc_nextval_for_sequence(SequenceInfo &s_value)
{
  int ret = OB_SUCCESS;
  int64_t cycle = OB_INVALID_INDEX;
  ObDecimal dec_min_value;
  ObDecimal dec_max_value;
  ObDecimal dec_current_value;
  ObDecimal dec_inc_by;
  ObDecimal dec_result;
  ObString result;
  //current sequence is new created,hasn't been used,the nextval is current val
  //the prevval is not valid and the prevval can't be used yet.
  if (0 == s_value.can_use_prevval_)
  {
    //do nothing
    TBSYS_LOG(DEBUG,"new created,the nextval can be used directly");
  }
  //current sequence has been used at least once,we need to build the nextval by calculating
  else
  {
    //1.do cast
    if (OB_SUCCESS != (ret = cast_sequence_info_to_decimal(s_value, dec_current_value, dec_min_value, dec_max_value, dec_inc_by)))
    {
      TBSYS_LOG(WARN,"cast sequence info from varchar to decimal failed!");
    }
    else
    {
      cycle = s_value.cycle_;
    }
    //2.caculate the next value
    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = dec_current_value.add(dec_inc_by,dec_result)))
      {
        TBSYS_LOG(ERROR, "caculate next value of sequence faliled!");
      }
      //3.judging the validity
      else
      {
        //legal
        if ((dec_result >= dec_min_value && dec_result <= dec_max_value)//in range
            || (dec_result > dec_current_value && dec_result < dec_min_value)//out of range,but in ascending order and less than min value
            || (dec_result < dec_current_value && dec_result > dec_max_value))//out of range,but in descding order and granter than max value
        {
          s_value.valid_ = 1;//set valid
        }
        else
        {
          if(1 == cycle)//illegal,out of range,but has cycle
          {
            if (dec_result > dec_current_value && dec_result > dec_max_value)
            {
              dec_result = dec_min_value;
            }
            else if(dec_result < dec_current_value && dec_result < dec_min_value)
            {
              dec_result = dec_max_value;
            }
            s_value.valid_ = 1;//has cycle ,set valid
          }
          if (0 == cycle)//illegal,out of range, has no cycle
          {
            if ((dec_result > dec_current_value && dec_result > dec_max_value)
                || (dec_result < dec_current_value && dec_result < dec_min_value))
            {
              s_value.valid_ = 0;//no cycle ,set valid false
              //bugfix for prevval use when the sequence is out of range but without cycle
              dec_result = dec_current_value;
            }
          }
        }
        //4.cast the result to string;
        ObString new_result;
        char buf[MAX_PRINTABLE_SIZE];
        memset(buf, 0, MAX_PRINTABLE_SIZE);
        int64_t result_length = 0;
        result_length = dec_result.to_string(buf, MAX_PRINTABLE_SIZE);
        result.assign_ptr(buf, static_cast<int32_t>(result_length));
        if (OB_SUCCESS != (ret = name_pool_.write_string(result, &new_result)))
        {
          TBSYS_LOG(ERROR, "write the new prevval to pool failed,ret::[%d]",ret);
        }
        //5. set to the SequenceInfo
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = s_value.current_value_.set_decimal(new_result)))
          {
            TBSYS_LOG(ERROR, "set next valid value failed!");
          }
        }
        TBSYS_LOG(DEBUG, "the  new value is ::%.*s",new_result.length(),new_result.ptr());
      }//end else
    }
  }//end else
   return ret;
}

/**
 *
 * @brief calculate prevval for current sequence come from SequenceInfo
 * @param s_value [in][out] in with the sequence info which come from sequence_info_map_ and out with the new prevval
 *
 */
int ObSequence::calc_prevval_for_sequence(SequenceInfo &s_value)
{
  int ret = OB_SUCCESS;
  ObString current_value;
  ObString new_result;
  if (OB_SUCCESS != (ret = s_value.current_value_.get_decimal(current_value)))
  {
    TBSYS_LOG(ERROR, "get current value of sequence failed!");
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = name_pool_.write_string(current_value, &new_result)))
    {
      TBSYS_LOG(ERROR, "write the new prevval to pool failed,ret::[%d]",ret);
    }
    else if (OB_SUCCESS != (ret = s_value.prevval_.set_decimal(new_result)))
    {
      TBSYS_LOG(ERROR, "set prevval value failed!");
    }
    TBSYS_LOG(DEBUG, "the new sequence prevval value is::%.*s",new_result.length(),new_result.ptr());
//    TBSYS_LOG(ERROR, "the new sequence prevval value is ::[%.*s]",new_result.length(),new_result.ptr());
  }
  return ret;
}

/**
 *
 * @brief use the sequence name build a next value,but we do overWrite to the new value into
 * the local map
 * @param sequence_name [in]
 * @param s_value [in][out]
 *
 */
int ObSequence::build_next_value_for_sequence(ObString &sequence_name, SequenceInfo &s_value)
{
  int ret = OB_SUCCESS;
  if (sequence_info_map_inited_)
  {
    if (-1 == (ret = sequence_info_map_.get(sequence_name, s_value)))
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "get [%.*s] sequence info error!",sequence_name.length(),sequence_name.ptr());
    }
    else if (hash::HASH_NOT_EXIST == ret)//not in hash map
    {
      TBSYS_LOG(ERROR, "the sequence [%.*s] not in map!",sequence_name.length(),sequence_name.ptr());
    }
    else if (hash::HASH_EXIST == ret)//succeed
    {
      if (OB_SUCCESS !=(ret = calc_nextval_for_sequence(s_value)))
      {
        TBSYS_LOG(WARN, "calc nextval for sequence [%.*s] failed!",sequence_name.length(),sequence_name.ptr());
      }
      if (OB_SUCCESS == ret)
      {
        if (-1 == (ret = sequence_info_map_.set(sequence_name, s_value, 1)))//overWrite
        {
          TBSYS_LOG(ERROR,"Over write sequence info failed!");
        }
        if (ret == hash::HASH_OVERWRITE_SUCC)
        {
          ret = OB_SUCCESS;
        }
      }
    }//end else if
    else
    {
      //won't be here!
    }
  }
  else
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "sequnece column map not inited!");
  }
  return ret;
}

/**
 *
 * @brief get current value and valid info from the row come from CS
 * @param sequence_row [in]
 * @param s_info [in][out]
 *
 */
int ObSequence::get_only_update_nextval_sequence_info(const ObRow *&sequence_row, SequenceInfo &s_info)
{
  int ret = OB_SUCCESS;
  if (NULL == sequence_row)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "the row is not inited!");
  }
  if (OB_SUCCESS == ret)
  {
    const ObObj *cell = NULL;
    ObString obj_current_value;
    if (OB_SUCCESS != (ret = sequence_row->raw_get_cell((CURRENT_VALUE_ID - OB_APP_MIN_COLUMN_ID), cell)))//int64_t(2)
    {
      TBSYS_LOG(ERROR, "get sequence row cell failed!");
    }
    else
    {
      if (OB_SUCCESS != (ret = cell->get_decimal(obj_current_value)))
      {
        TBSYS_LOG(ERROR , "get sequence obj_current_value  failed!");
      }
      else if (OB_SUCCESS != (ret = name_pool_.write_obj_varchar_to_decimal(*cell, &s_info.current_value_)))
      {
        TBSYS_LOG(ERROR, "store start with falied!");
      }
      //            TBSYS_LOG(ERROR, "@@@@@@@@the cell start with is::[%s]@@@@@@@",to_cstring(*cell));
    }
    //valid
    if (OB_SUCCESS != (ret = sequence_row->raw_get_cell((VALID_ID - OB_APP_MIN_COLUMN_ID), cell)))//int64_t(9)
    {
      TBSYS_LOG(ERROR, "get sequence row cell failed!");
    }
    else
    {
      int64_t valid = OB_INVALID_INDEX;
      if (OB_SUCCESS != (ret = cell->get_int(valid)))
      {
        TBSYS_LOG(ERROR , "get sequence valid failed!");
      }
      else
      {
        s_info.valid_ = valid;
      }
    }
  }
  return ret;
}

/**
 *
 * @brief get data type info from the row come from CS
 * @param sequence_row [in]
 * @param s_info [in][out]
 * @param only_update_prevval [in]
 *
 */
int ObSequence::get_sequence_data_type_info(const ObRow *&sequence_row, SequenceInfo &s_info, bool only_update_prevval)
{
  int ret = OB_SUCCESS;
  if (NULL == sequence_row)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "the row is not inited!");
  }
  if (OB_SUCCESS == ret)
  {
    const ObObj *cell = NULL;
    if (OB_SUCCESS != (ret = sequence_row->raw_get_cell((DATA_TYPE_ID - OB_APP_MIN_COLUMN_ID), cell)))//int64_t(1)
    {
      TBSYS_LOG(ERROR, "get sequence row cell failed!");
    }
    else
    {
      int64_t data_type = OB_INVALID_INDEX;
      if ((!only_update_prevval))//!only_update_prevval
      {
        if (OB_SUCCESS != (ret = cell->get_int(data_type)))
        {
          TBSYS_LOG(ERROR , "get sequence data_type failed!");
        }
        else
        {
          s_info.data_type_ = data_type;
          //            TBSYS_LOG(ERROR, "@@@@@@@@the cell data type is::[%s]@@@@@@@",to_cstring(*cell));
        }
      }
    }
  }
  return ret;
}

/**
 *
 * @brief get data type info from the row come from CS
 * @param sequence_row [in]
 * @param s_info [in][out]
 * @param only_update_prevval [in]
 * @param obj_current_value [out]
 *
 */
int ObSequence::get_sequence_current_value_info(const ObRow *&sequence_row, SequenceInfo &s_info, bool only_update_prevval, ObString &obj_current_value)
{
  int ret = OB_SUCCESS;
  if (NULL == sequence_row)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "the row is not inited!");
  }
  if (OB_SUCCESS == ret)
  {
    const ObObj *cell = NULL;
    if (OB_SUCCESS != (ret = sequence_row->raw_get_cell((CURRENT_VALUE_ID - OB_APP_MIN_COLUMN_ID), cell)))//int64_t(2)
    {
      TBSYS_LOG(ERROR, "get sequence row cell failed!");
    }
    else
    {
      if (OB_SUCCESS != (ret = cell->get_decimal(obj_current_value)))
      {
        TBSYS_LOG(ERROR , "get sequence obj_current_value  failed!");
      }
      if ((OB_SUCCESS == ret) && (!only_update_prevval))//!only_update_prevval
      {
        if (OB_SUCCESS != (ret = name_pool_.write_obj_varchar_to_decimal(*cell, &s_info.current_value_)))
        {
          TBSYS_LOG(ERROR, "store current value to name pool falied!");
        }
//            TBSYS_LOG(ERROR, "@@@@@@@@the cell start with is::[%s]@@@@@@@",to_cstring(*cell));
      }
    }
  }
  return ret;
}


/**
 *
 * @brief get inc by  info from the row come from CS
 * @param sequence_row [in]
 * @param s_info [in][out]
 * @param only_update_prevval [in]
 * @param obj_increment_by [out]
 *
 */
int ObSequence::get_sequence_inc_by_value_info(const ObRow *&sequence_row, SequenceInfo &s_info, bool only_update_prevval, ObString &obj_increment_by)
{
  int ret = OB_SUCCESS;
  if (NULL == sequence_row)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "the row is not inited!");
  }
  if (OB_SUCCESS == ret)
  {
    const ObObj *cell = NULL;
    if (OB_SUCCESS != (ret = sequence_row->raw_get_cell((INC_BY_ID - OB_APP_MIN_COLUMN_ID), cell)))//int64_t(3)
    {
      TBSYS_LOG(ERROR, "get sequence row cell failed!");
    }
    else
    {
      if (OB_SUCCESS != (ret = cell->get_decimal(obj_increment_by)))
      {
        TBSYS_LOG(ERROR , "get sequence increment_by failed!");
      }
      if ((OB_SUCCESS == ret) && (!only_update_prevval) && (!has_checked_))//!only_update_prevval
      {
        if (OB_SUCCESS != (ret = name_pool_.write_obj_varchar_to_decimal(*cell, &s_info.inc_by_)))
        {
          TBSYS_LOG(ERROR, "store increment by falied!");
        }
      }
//                   TBSYS_LOG(ERROR, "@@@@@@@@the cell increment is::[%s]@@@@@@@",to_cstring(*cell));
    }
  }
  return ret;
}

/**
 *
 * @brief get min value info from the row come from CS
 * @param sequence_row [in]
 * @param s_info [in][out]
 * @param only_update_prevval [in]
 *
 */
int ObSequence::get_sequence_min_value_info(const ObRow *&sequence_row, SequenceInfo &s_info, bool only_update_prevval)
{
  int ret = OB_SUCCESS;
  if (NULL == sequence_row)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "the row is not inited!");
  }
  if (OB_SUCCESS == ret)
  {
    const ObObj *cell = NULL;
    if (OB_SUCCESS != (ret = sequence_row->raw_get_cell((MIN_VALUE_ID - OB_APP_MIN_COLUMN_ID), cell)))//int64_t(4)
    {
      TBSYS_LOG(ERROR, "get sequence row cell failed!");
    }
    else
    {
      ObString obj_min_value;
      if (OB_SUCCESS != (ret = cell->get_decimal(obj_min_value)))
      {
        TBSYS_LOG(ERROR , "get sequence obj_min_value failed!");
      }
      if ((OB_SUCCESS == ret) && (!only_update_prevval) && (!has_checked_))//!only_update_prevval
      {
        if (OB_SUCCESS != (ret = name_pool_.write_obj_varchar_to_decimal(*cell, &s_info.min_value_)))
        {
          TBSYS_LOG(ERROR, "store min_value falied!");
        }
        TBSYS_LOG(DEBUG, "@@@@@@@@the cell min valued is::[%s]@@@@@@@",to_cstring(*cell));
      }
      //          TBSYS_LOG(ERROR, "@@@@@@@@the cell min valued is::[%s]@@@@@@@",to_cstring(*cell));
    }
  }
  return ret;
}

/**
 *
 * @brief get max value info from the row come from CS
 * @param sequence_row [in]
 * @param s_info [in][out]
 * @param only_update_prevval [in]
 *
 */
int ObSequence::get_sequence_max_value_info(const ObRow *&sequence_row, SequenceInfo &s_info, bool only_update_prevval)
{
  int ret = OB_SUCCESS;
  if (NULL == sequence_row)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "the row is not inited!");
  }
  if (OB_SUCCESS == ret)
  {
    const ObObj *cell = NULL;
    if (OB_SUCCESS != (ret = sequence_row->raw_get_cell((MAX_VALUE_ID - OB_APP_MIN_COLUMN_ID), cell)))//int64_t(5)
    {
      TBSYS_LOG(ERROR, "get sequence row cell failed!");
    }
    else
    {
      ObString obj_max_value;
      if (OB_SUCCESS != (ret = cell->get_decimal(obj_max_value)))
      {
        TBSYS_LOG(ERROR , "get sequence obj_max_value failed!");
      }
      if ((OB_SUCCESS == ret) && (!only_update_prevval) && (!has_checked_))//!only_update_prevval
      {
        if (OB_SUCCESS != (ret = name_pool_.write_obj_varchar_to_decimal(*cell, &s_info.max_value_)))
        {
          TBSYS_LOG(ERROR, "store max_value falied!");
        }
        TBSYS_LOG(DEBUG, "@@@@@@@@the cell max value is::[%s]@@@@@@@",to_cstring(*cell));
      }
      //          TBSYS_LOG(ERROR, "@@@@@@@@the cell max value is::[%s]@@@@@@@",to_cstring(*cell));
    }
  }
  return ret;
}

/**
 *
 * @brief get cycle value info from the row come from CS
 * @param sequence_row [in]
 * @param s_info [in][out]
 * @param only_update_prevval [in]
 *
 */
int ObSequence::get_sequence_cycle_value_info(const ObRow *&sequence_row, SequenceInfo &s_info, bool only_update_prevval)
{
  int ret = OB_SUCCESS;
  if (NULL == sequence_row)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "the row is not inited!");
  }
  if (OB_SUCCESS == ret)
  {
    const ObObj *cell = NULL;
    if (OB_SUCCESS != (ret = sequence_row->raw_get_cell((CYCLE_ID - OB_APP_MIN_COLUMN_ID), cell)))//int64_t(6)
    {
      TBSYS_LOG(ERROR, "get sequence row cell failed!");
    }
    else
    {
      int64_t cycle = OB_INVALID_INDEX;
      if ((!only_update_prevval))//!only_update_prevval
      {
        if (OB_SUCCESS != (ret = cell->get_int(cycle)))
        {
          TBSYS_LOG(ERROR , "get sequence cycle failed!");
        }
        else
        {
          s_info.cycle_ = cycle;
          //          TBSYS_LOG(ERROR, "@@@@@@@@the cell cycle  is::[%s]@@@@@@@",to_cstring(*cell));
        }
      }
    }
  }
  return ret;
}


/**
 *
 * @brief get valid value info from the row come from CS
 * @param sequence_row [in]
 * @param s_info [in][out]
 * @param only_update_prevval [in]
 *
 */
int ObSequence::get_sequence_valid_value_info(const ObRow *&sequence_row, SequenceInfo &s_info, bool only_update_prevval)
{
  int ret = OB_SUCCESS;
  if (NULL == sequence_row)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "the row is not inited!");
  }
  if (OB_SUCCESS == ret)
  {
    const ObObj *cell = NULL;
    if (OB_SUCCESS != (ret = sequence_row->raw_get_cell((VALID_ID - OB_APP_MIN_COLUMN_ID), cell)))//int64_t(9)
    {
      TBSYS_LOG(ERROR, "get sequence row cell failed!");
    }
    else
    {
      int64_t valid = OB_INVALID_INDEX;
      if ((!only_update_prevval))//!only_update_prevval
      {
        if (OB_SUCCESS != (ret = cell->get_int(valid)))
        {
          TBSYS_LOG(ERROR , "get sequence valid failed!");
        }
        else
        {
          s_info.valid_ = valid;
        }
      }
    }
  }
  return ret;
}

/**
 *
 * @brief get can use prev value info from the row come from CS,also if update prevval,get the prevval
 * @param sequence_row [in]
 * @param s_info [in][out]
 * @param update_prevval [in]
 * @param obj_current_value [out]
 *
 *
 */
int ObSequence::get_sequence_can_use_prev_value_info(const ObRow *&sequence_row, SequenceInfo &s_info, bool update_prevval, ObString &obj_current_value)
{
  int ret = OB_SUCCESS;
  if (NULL == sequence_row)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "the row is not inited!");
  }
  if (OB_SUCCESS == ret)
  {
    const ObObj *cell = NULL;
    if (OB_SUCCESS != (ret = sequence_row->raw_get_cell((CAN_USE_PREVVAL_ID - OB_APP_MIN_COLUMN_ID), cell)))//int64_t(11)
    {
      TBSYS_LOG(ERROR, "get sequence row cell failed!");
    }
    else
    {
      int64_t can_use_prevval = OB_INVALID_INDEX;
      if (OB_SUCCESS != (ret = cell->get_int(can_use_prevval)))
      {
        TBSYS_LOG(ERROR , "get sequence present value failed!");
      }
      else
      {
        //            col_info.can_use_prevval_ = can_use_prevval;
        if (update_prevval)
        {
          s_info.can_use_prevval_ = can_use_prevval;
          ObString prevval;
          if (OB_SUCCESS == ret)
          {
            if (OB_SUCCESS != (ret = name_pool_.write_string(obj_current_value, &prevval)))
            {
              TBSYS_LOG(ERROR, "write current value to name_pool failed!");
            }
            if (OB_SUCCESS == ret && OB_SUCCESS != (ret = s_info.prevval_.set_decimal(prevval)))
            {
              TBSYS_LOG(ERROR, "set prev value of sequecen faliled!");
            }
            //                TBSYS_LOG(ERROR,"^^^^^the [%.*s]prevval is[%.*s]^^^^^",sequence_name.length(),sequence_name.ptr(), prevval.length(),prevval.ptr());
          }
        }//end if
      }
    }
  }
  return ret;
}

/**
 *
 * @brief get can use prev value info from the row come from CS
 * @param sequence_row [in]
 * @param s_info [in][out]
 * @param only_update_prevval [in]
 *
 */
int ObSequence::get_sequence_use_quick_path_value_info(const ObRow *&sequence_row, SequenceInfo &s_info, bool only_update_prevval)
{
  int ret = OB_SUCCESS;
  if (NULL == sequence_row)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "the row is not inited!");
  }
  if (OB_SUCCESS == ret)
  {
    const ObObj *cell = NULL;
    if (OB_SUCCESS != (ret = sequence_row->raw_get_cell((USE_QUICK_PATH_ID - OB_APP_MIN_COLUMN_ID), cell)))//int64_t(12)
    {
      TBSYS_LOG(ERROR, "get sequence row cell failed!");
    }
    else
    {
      int64_t use_quick_path = OB_INVALID_INDEX;
      if ((!only_update_prevval))//!only_update_prevval
      {
        if (OB_SUCCESS != (ret = cell->get_int(use_quick_path)))
        {
          TBSYS_LOG(ERROR , "get sequence use quick path failed!");
        }
        else
        {
          s_info.use_quick_path_ = use_quick_path;
        }
      }
    }
  }
  return ret;
}

/**
 *
 * @brief this function is used for update the local map which the sequence info get from CS,
 * this function only be used in two cases:
 * case 1:the row come from CS for OB quick path and check the validity for the sequence U input from client
 * case 2:the row come from CS for lock(select * from __all_sequence where ...for update)
 * @param update_prevval [in]
 * @param update_nextval [in]
 *
 */
int ObSequence::update_sequence_info_map(bool update_prevval,bool update_nextval)
{
  int ret = OB_SUCCESS;
  //case1:only update nextval
  //case2:only update prevval
  //case3:update both
  //case4:neither update this case should't happen here
  bool only_update_nextval = !update_prevval && update_nextval;
  bool only_update_prevval = update_prevval && !update_nextval;
  //  bool update_both = update_prevval && update_nextval; //this case is default
  bool update_neither = !update_prevval && !update_nextval;
  if (update_neither)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR,"U must update at least one type value of sequence!");
  }

  const ObRow *tem_row = NULL;
  while(OB_SUCCESS == ret)//for each row
  {
    ret = ObSequence::get_next_row(tem_row);//be careful
    if (OB_ITER_END == ret)
    {
      ret = OB_SUCCESS;
      break;
    }
    else
    {
//      TBSYS_LOG(ERROR, "@@@@@@@@the row is::[%s]@@@@@@@",to_cstring(*tem_row));
      TBSYS_LOG(DEBUG, "@@@@@@@@the row is::[%s]@@@@@@@",to_cstring(*tem_row));
      const ObObj *cell = NULL;
      ObObjType cell_type;
      ObString sequence_name;
      ObString stored_sequence_name;
      SequenceInfo col_info;

      //name
      if (OB_SUCCESS != (ret = tem_row->raw_get_cell((NAME_ID - OB_APP_MIN_COLUMN_ID), cell)))// int64_t(0)
      {
        TBSYS_LOG(ERROR, "get sequence row cell failed!");
      }
      else
      {
        cell_type = cell->get_type();
        if (cell_type != ObVarcharType)
        {
          TBSYS_LOG(ERROR, "get sequence row name failed!");
        }
        else if (OB_SUCCESS != (ret = cell->get_varchar(sequence_name)))
        {
          TBSYS_LOG(ERROR , "get sequence name failed!");
        }
        else if (OB_SUCCESS != (ret = name_pool_.write_string(sequence_name, &stored_sequence_name)))
        {
          TBSYS_LOG(ERROR, "store sequence_name falied!");
        }
      }
      if (OB_SUCCESS == ret && has_checked_)//(only_update_nextval) || (only_update_prevval))//only_update_nextval and only_update_prevval
      {
        if (-1 == (ret = sequence_info_map_.get(stored_sequence_name,col_info)))
        {
          TBSYS_LOG(ERROR,"set (%.*s) to coiumn map failed!",sequence_name.length(),sequence_name.ptr());
          ret = OB_ERROR;
        }
        else if (hash::HASH_EXIST == ret)
        {
          ret = OB_SUCCESS;
          TBSYS_LOG(DEBUG, "reget!");
        }
        if (ret == hash::HASH_NOT_EXIST)
        {
          TBSYS_LOG(ERROR,"do not happen in this place!");
        }
      }
      //============================================
      //only_update_nextval,we just need the current_value and valid
      if (only_update_nextval && OB_SUCCESS == ret)//only_update_nextval
      {
        if (OB_SUCCESS != (ret = get_only_update_nextval_sequence_info(tem_row, col_info)))
        {
          TBSYS_LOG(WARN, "get only update nextval sequence info failed!");
        }
      }
      else//only_update_prevval and update_both
      {
        //TBSYS_LOG(ERROR, "@@@@@@@@the cell name is::[%s]@@@@@@@",to_cstring(*cell));
        //data type
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = get_sequence_data_type_info(tem_row, col_info, only_update_prevval)))
          {
            TBSYS_LOG(WARN, "get sequence data type info failed,ret::[%d]",ret);
          }
        }
        //start with
        ObString obj_current_value;
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = get_sequence_current_value_info(tem_row, col_info, only_update_prevval, obj_current_value)))
          {
            TBSYS_LOG(WARN, "get sequence current value info failed,ret::[%d]",ret);
          }
        }

        //increment by
        ObString obj_increment_by;
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = get_sequence_inc_by_value_info(tem_row, col_info, only_update_prevval, obj_increment_by)))
          {
            TBSYS_LOG(WARN, "get sequence current value info failed,ret::[%d]",ret);
          }
        }

        //min value
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = get_sequence_min_value_info(tem_row, col_info, only_update_prevval)))
          {
            TBSYS_LOG(WARN, "get sequence min value info failed,ret::[%d]",ret);
          }
        }

        //max value
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = get_sequence_max_value_info(tem_row, col_info, only_update_prevval)))
          {
            TBSYS_LOG(WARN, "get sequence max value info failed,ret::[%d]",ret);
          }
        }

        //cycle

        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = get_sequence_cycle_value_info(tem_row, col_info, only_update_prevval)))
          {
            TBSYS_LOG(WARN, "get sequence cycle value info failed,ret::[%d]",ret);
          }
        }
        //catch 7
        //order 8
        //valid 9
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = get_sequence_valid_value_info(tem_row, col_info,only_update_prevval)))
          {
            TBSYS_LOG(WARN, "get sequence valid value info failed,ret::[%d]",ret);
          }
        }
        //restart with 10
        //can use prevval value 11
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = get_sequence_can_use_prev_value_info(tem_row, col_info, update_prevval, obj_current_value)))
          {
            TBSYS_LOG(WARN, "get sequence can use prev  value info failed,ret::[%d]",ret);
          }
        }
        //use_quick_path 12
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = get_sequence_use_quick_path_value_info(tem_row, col_info, only_update_prevval)))
          {
            TBSYS_LOG(WARN, "get sequence use quick path value info failed,ret::[%d]",ret);
          }
        }
      }//end else
      //============================================
      //add to map for using later
      if (OB_SUCCESS == ret && sequence_info_map_inited_)
      {
        if (-1 == (ret = sequence_info_map_.set(stored_sequence_name,col_info)))
        {
          TBSYS_LOG(ERROR,"set (%.*s) to coiumn map failed!",stored_sequence_name.length(),stored_sequence_name.ptr());
          ret = OB_ERROR;
        }
        else if ((hash::HASH_EXIST == ret))//||( hash::HASH_INSERT_SUCC == ret))//
        {
//          TBSYS_LOG(ERROR, "overwrite!");
          TBSYS_LOG(DEBUG, "overwrite!");
          if (-1 == (ret = sequence_info_map_.set(stored_sequence_name, col_info, 1)))//overWrite
          {
            TBSYS_LOG(ERROR,"Over write sequence info failed!");
          }
        }
        if ((ret == hash::HASH_INSERT_SUCC) || (ret == hash::HASH_OVERWRITE_SUCC))
        {
          ret = OB_SUCCESS;
        }
      }//end if
    }//end else
  }//end while
//  const_cast<ObRow*>(tem_row)->clear();
  //check the sequences you have inputted from the terminal is exist or not!
  if (!has_checked_)
  {
    for (int64_t i = 0; OB_SUCCESS == ret && i < sequence_name_no_dup_.count(); i++)
    {
      SequenceInfo s_info;
      if (-1 == (ret = sequence_info_map_.get(sequence_name_no_dup_.at(i), s_info)))
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "get sequence info failed!");
      }
      else if (hash::HASH_NOT_EXIST == ret)
      {
        ret = OB_ERROR;
        can_fill_ = false;//use for select, update and delete in where clause of sequence
        TBSYS_LOG(USER_ERROR, "the sequence::[%.*s] you input is not exist,please check!",sequence_name_no_dup_.at(i).length(),sequence_name_no_dup_.at(i).ptr());
        break;
      }
      else
      {
        has_checked_ = true;
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

/**
 * @brief this function is use for OB sequence row-sequence, construct a new prevval for current sequence
 * and overwrite the new info to the local map,this function will be called after each row was filled
 * @param sequence_name [in]
 * @param update_prevval [in]
 *
 */
int ObSequence::update_sequence_info_map_for_prevval(ObString &sequence_name, bool update_prevval)
{
  int ret = OB_SUCCESS;
  SequenceInfo s_info;
  if (OB_SUCCESS == ret)
  {
    if (-1 == (ret = sequence_info_map_.get(sequence_name,s_info)))
    {
      TBSYS_LOG(ERROR,"get (%.*s) sequence_info_map failed!",sequence_name.length(),sequence_name.ptr());
      ret = OB_ERROR;
    }
    else if (ret == hash::HASH_NOT_EXIST)//
    {
      TBSYS_LOG(WARN,"not exist! should't happen here,ret::[%d]",ret);
    }
    else if (hash::HASH_EXIST == ret)
    {
      ret = OB_SUCCESS;
      if (update_prevval)
      {
        if (OB_SUCCESS !=(ret = calc_prevval_for_sequence(s_info)))
         {
           TBSYS_LOG(WARN, "calc nextval for sequence [%.*s] failed!",sequence_name.length(),sequence_name.ptr());
         }
      }
      //if U come here,the prevval can be used next time
      if (OB_SUCCESS == ret)
      {
         s_info.can_use_prevval_ = 1;
      }
      //overwrite to map
      if (OB_SUCCESS == ret)
      {
        if (-1 == (ret = sequence_info_map_.set(sequence_name, s_info, 1)))//overWrite
        {
          TBSYS_LOG(ERROR,"Over write sequence info failed!");
        }
        else if (hash::HASH_OVERWRITE_SUCC == ret)
        {
          ret = OB_SUCCESS;
        }
      }
    }//end else if (hash::HASH_EXIST == ret)
    else
    {
      TBSYS_LOG(ERROR,"won't be here!");
    }
  }
  return ret;
}

/**
 * @brief this function is used to get all the sequences info U input from the client
 * and write the info to the local sequence map ,also check the validity for all sequences
 */
int ObSequence::prepare_sequence_prevval()
{
  int ret = OB_SUCCESS;
  ObValues *sequence_values = NULL; //for sequence static data(info) usage
  if (NULL == (sequence_values = dynamic_cast<ObValues*>(get_child(0))))
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "get the first child ::ObValues op of sequence failed!");
  }
  // 1. get the sequecnes info for check
  else if (OB_SUCCESS == ret && OB_SUCCESS != (ret = sequence_values->open()))//get static data of sequence
  {
    TBSYS_LOG(ERROR, "failed get static data of sequences!");
  }
  // 2. init map
  else if (OB_SUCCESS == ret && OB_SUCCESS != (ret = init_sequence_info_map()))//init map
  {
    TBSYS_LOG(ERROR, "init sequence name column map failed!");
  }
  // 3.upate map
  else if (OB_SUCCESS != (ret = update_sequence_info_map(true, true)))
  {
    TBSYS_LOG(WARN, "set all sequence rows to map failed! [ret:%d]",ret);
  }
  return ret;
}


/**
 * @brief construct a new row for current sequence and store the row to the member of the sequence operator
 * @param sequence_name [in]
 * @param [out]
 */
int ObSequence::prepare_lock_row(ObString &sequence_name)
{
  int ret = OB_SUCCESS;
  lock_row_.reset(false, ObRow::DEFAULT_NULL);
  lock_row_desc_.set_rowkey_cell_count(1);
  if (!has_cons_lock_row_desc_ &&  OB_SUCCESS != (ret = lock_row_desc_.add_column_desc(OB_ALL_SEQUENCE_TID,
                                                    NAME_ID)))
  {
    TBSYS_LOG(ERROR, "failed to add row key row desc, err=%d", ret);
  }
//  TBSYS_LOG(ERROR,"cccccccccccsequence is::[%.*s]",sequence_name.length(),sequence_name.ptr());
  if (OB_SUCCESS == ret)
  {
    has_cons_lock_row_desc_ = true;
    lock_row_.set_row_desc(lock_row_desc_);
    ObObj row_key_cell;
    row_key_cell.set_varchar(sequence_name);
    if (OB_SUCCESS != (ret = lock_row_.set_cell(OB_ALL_SEQUENCE_TID,NAME_ID,row_key_cell)))
    {
      TBSYS_LOG(ERROR, "set sequence row key cell failed!");
    }
  }
  return ret;
}

/**
  * @brief start transaction,if U start transaction from the terminal manumotive,this function
  * will not start transaction again
  */
int ObSequence::start_sequence_trans()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  if (NULL == sql_context_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR,"sql context not inited!");
  }
  else
  {
    session = sql_context_->session_info_;
  }

  if (session->get_trans_id().is_valid())//already in transaction
  {
    has_client_trans_ = true;
    TBSYS_LOG(DEBUG, "already in transaction");
    ret = OB_SUCCESS;
  }
  else
  {
    if (OB_SUCCESS == ret)
    {
      ObResultSet result_set;
      ObString excute_sql = ObString::make_string(START_TRANS.c_str());
      TBSYS_LOG(DEBUG,"start trans!");
      if (OB_SUCCESS != (ret = result_set.init()))
      {
        TBSYS_LOG(ERROR, "init result set failed, ret = %d", ret);
      }
      else if (OB_SUCCESS != (ret = ObSql::direct_execute(excute_sql, result_set, *sql_context_)))
      {
        TBSYS_LOG(ERROR,"fail to do direct execute,ret = %d",ret);
      }
      //open
      if(OB_SUCCESS == ret && OB_SUCCESS != (ret = result_set.open()))
      {
        TBSYS_LOG(ERROR,"fail to open lock sql,ret = %d",ret);
      }
      //close
      else if (OB_SUCCESS != (ret = result_set.close()))
      {
        TBSYS_LOG(WARN,"close failed!");
      }
    }
  }

  return ret;
}
/**
 * @brief end transacion,if U start transaction from the terminal manumotive,this function
 * will not commit the transaction  automaic
 */
int ObSequence::end_sequence_trans()
{
  int ret = OB_SUCCESS;
  if (NULL == sql_context_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR,"sql context not inited!");
  }
  if (OB_SUCCESS == ret && has_client_trans_)
  {
    //do noting
    TBSYS_LOG(DEBUG,"U start transaction from the client");
  }
  else
  {
    TBSYS_LOG(DEBUG,"end transaction!");
    if (OB_SUCCESS == ret)
    {
      ObResultSet result_set;
      ObString excute_sql = ObString::make_string(END_TRANS.c_str());
      if (OB_SUCCESS != (ret = result_set.init()))
      {
        TBSYS_LOG(ERROR, "init result set failed, ret = %d", ret);
      }
      else if (OB_SUCCESS != (ret = ObSql::direct_execute(excute_sql, result_set, *sql_context_)))
      {
        TBSYS_LOG(ERROR,"fail to do direct execute,ret = %d",ret);
      }
      //open
      if(OB_SUCCESS == ret && OB_SUCCESS != (ret = result_set.open()))
      {
        TBSYS_LOG(ERROR,"fail to open lock sql,ret = %d",ret);
      }
      //close
      else if (OB_SUCCESS != (ret = result_set.close()))
      {
        TBSYS_LOG(WARN,"close failed!");
      }
    }
  }
  return ret;
}

/**
 *
 * @brief we use this function construct a dml SQL(update statement) update the sequence
 * whose name is "sequence_name" with new nextval(s_info.current_value_)
 * @param sequence_name [in]
 * @param s_info [in]
 * @return if OB_SUCCESS
 *
 */
int ObSequence::update_by_dml(ObString &sequence_name, SequenceInfo &s_info)
{
  //update __all_sequence set current_value = ,is_valid = ,can_use_prevval = where sequence_name = '';
  int ret = OB_SUCCESS;
  if (NULL == sql_context_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR,"sql context not inited!");
  }
  if (ret == OB_SUCCESS)
  {
    ObResultSet result_set;
    ObString current_value;
    TBSYS_LOG(DEBUG,"DML UPDATE");
    if (OB_SUCCESS != (ret = s_info.current_value_.get_decimal(current_value)))
    {
      TBSYS_LOG(ERROR,"get start with failed!");
    }
    else
    {
      //name
      std::string s_name;
      s_name.assign(sequence_name.ptr(),sequence_name.length());
      //start
      std::string new_value;
      new_value.assign(current_value.ptr(),current_value.length());
      //bugfix:if the value is a decimal and bigger than int_max,we should add "." to ensure the value not be changed in update stmt
      new_value.append(".");
      //valid
      std::string valid_value;
      char buf[MAX_PRINTABLE_SIZE];
      memset(buf, 0, MAX_PRINTABLE_SIZE);
      sprintf(buf,"%ld",s_info.valid_);
      valid_value.assign(buf);
      //can_use_prevval
      std::string can_use_prevval_value;
      s_info.can_use_prevval_ = 1;//if U come here,the flag must be true
      memset(buf, 0, MAX_PRINTABLE_SIZE);
      sprintf(buf,"%ld",s_info.can_use_prevval_);
      can_use_prevval_value.assign(buf);
      //full sql
      std::string full_sql;
      full_sql.append("update ");//update
      full_sql.append(OB_ALL_SEQUENCE_TABLE_NAME);//__all_sequence
      full_sql.append(" set ");//set
      full_sql.append(OB_SEQUENCE_CURRENT_VALUE);//current_value
      full_sql.append("=");//=
      full_sql.append(new_value);//new value
      full_sql.append(",");
      full_sql.append(OB_SEQUENCE_IS_VALID);//is_valid
      full_sql.append("=");//=
      full_sql.append(valid_value);//valid value
      full_sql.append(",");
      full_sql.append(OB_SEQUENCE_CAN_USE_PREVVAL);//can_use_prevval
      full_sql.append("=");//=
      full_sql.append(can_use_prevval_value);//can use prevval value
      full_sql.append(" where ");
      full_sql.append(OB_SEQUENCE_NAME);
      full_sql.append("=");//=
      full_sql.append(SINGLE_QUOTATION);//'
      full_sql.append(s_name);//sequence name
      full_sql.append(SINGLE_QUOTATION);//'
      full_sql.append(SEMICOLON);//;
      ObString excute_sql = ObString::make_string(full_sql.c_str());
      TBSYS_LOG(DEBUG,"the sql is::[%.*s]",excute_sql.length(),excute_sql.ptr());
      if (OB_SUCCESS != (ret = result_set.init()))
      {
        TBSYS_LOG(ERROR, "init result set failed, ret = %d", ret);
      }
      else if (OB_SUCCESS != (ret = ObSql::direct_execute(excute_sql, result_set, *sql_context_)))
      {
        TBSYS_LOG(ERROR,"fail to do direct execute,ret = %d",ret);
      }
      //do update
      if(OB_SUCCESS == ret && OB_SUCCESS != (ret = result_set.open()))
      {
        TBSYS_LOG(ERROR,"fail to open lock sql,ret = %d",ret);
      }
      else if (OB_SUCCESS == ret && OB_SUCCESS != (ret = result_set.close()))
      {
        TBSYS_LOG(WARN,"close failed!");
      }
    }
  }
  return ret;
}

int ObSequence::open()
{
  return OB_NOT_SUPPORTED;
}
int ObSequence::close()
{
//  sequence_info_map_.clear();
//  name_pool_.clear();
  return ObMultiChildrenPhyOperator::close();
}

int64_t ObSequence::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObBaseSequence\n");
  return pos;
}


/**
 *
 * @brief this function is use to replace  "sequence name" with the "sequence value" that
 * the name we pushed into the postExpr in the logical paln,this function is the core of sequence fill
 * in this function ,contain transaction,lock,upate,be careful.
 * @param post_expr_array [in][out] in with sequence_name(varchar),out with the sequence_value(decimal)
 * @param idx [in]
 * @param dml_type [in]
 * @param is_const [in]
 *
 */
int ObSequence::do_simple_fill(ObSEArray<ObObj,64> &post_expr_array, int64_t idx, int dml_type, bool is_const)
{
  int ret = OB_SUCCESS;
  ObString sequence_name;
  SequenceInfo s_value;
  ObObjType obj_type;
  ObObj *expr_array_obj = const_cast<ObObj *>(&post_expr_array[idx]);
  TBSYS_LOG(DEBUG, "&&&&&&&the obj is::[%s]&&&&&&&",to_cstring(*expr_array_obj));
  //1.get the const obj
  if (ObVarcharType != (obj_type = expr_array_obj->get_type()))
  {
    if (is_const)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "the type must be ObVarcharType!");
    }
    else
    {
      ret = OB_NOT_VARCHAR_OBJ;
    }
  }
  if (OB_SUCCESS == ret)
  {
    ObString value;
    bool can_use_nextval = false;
    bool can_use_prevval = false;
    if (OB_SUCCESS != (ret = expr_array_obj->get_varchar(sequence_name)))
    {
      TBSYS_LOG(ERROR, "get the expr array obj failed!");
    }

    //2.check if sequence_info_map_  has current sequence info;
    if(-1 == (ret = sequence_info_map_.get(sequence_name, s_value)))
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "get the sequence values error!");
    }
    else if (hash::HASH_NOT_EXIST == ret)//not find
    {
      ret = OB_NOT_VALID_SEQUENCE_VARCHAR;//complex expr, current obj is varchar,but not sequence varchar
    }
    else
    {
      ret = OB_SUCCESS;
    }
    if (OB_SUCCESS == ret)
    {
      bool is_next_type = true;
      if (OB_SEQUENCE_INSERT_LIMIT == dml_type)
      {
        is_next_type = get_cur_seuqnece_type_for_insert();
      }
      else if (OB_SEQUENCE_SELECT_LIMIT == dml_type)
      {
        is_next_type = get_cur_seuqnece_type_for_select(OB_SEQUENCE_SELECT_LIMIT, ret);
        sequence_idx_in_expr_.push_back(idx);
      }
      else if (OB_SEQUENCE_SELECT_WHERE_LIMIT == dml_type)
      {
        is_next_type = get_cur_seuqnece_type_for_select(OB_SEQUENCE_SELECT_WHERE_LIMIT, ret);
        sequence_pair_idx_for_where_++;
      }
      else if (OB_SEQUENCE_UPDATE_LIMIT == dml_type)
      {
        is_next_type = get_cur_sequence_type_for_update();
        sequence_idx_in_expr_.push_back(idx);
      }
      TBSYS_LOG(DEBUG, "the current sequence type is::[%s]",is_next_type?"next":"prev");
      //3.get the matching type start with value
      if (OB_SUCCESS == ret && is_next_type)//is next type,renew the start with and set can_use_prevval = ture
      {
        bool update_prevval = false;
        bool update_nextval = false;
        this->need_to_update_prevval(sequence_name, update_prevval);
        this->need_to_update_nextval(sequence_name, update_nextval);
        //a.检查当前sequence的next在当前行是否可直接用，如果不可以直接使用，需要到CS取最新的数据
        TBSYS_LOG(DEBUG,"current sequence is::[%.*s]",sequence_name.length(),sequence_name.ptr());
        if (!can_use_current_nextval(sequence_name))//本地当前seuquence不可用
        {
          //(1)开启事务
          if (!is_quick_path_sequence(sequence_name))//except quick path
          {
            if (OB_SUCCESS != (ret = start_sequence_trans()))
            {
              TBSYS_LOG(WARN,"start sequence trans failed!");
            }
          }
          //for quick path or the sequence has been locked within client transaction
          if (OB_SUCCESS == ret && ((has_client_trans_ && !can_lock_current_sequence(sequence_name)) || (is_quick_path_sequence(sequence_name))))
          {
            SequenceInfo s_info;
            TBSYS_LOG(DEBUG, "quick or client trans");
            if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = build_next_value_for_sequence(sequence_name, s_info))))
            {
              TBSYS_LOG(WARN, "build next value for [%.*s] failed!",sequence_name.length(),sequence_name.ptr());
            }
          }
          else//for normal sequence or client transaction
          {
            //(2)加锁当前sequence
            if ((OB_SUCCESS == ret) && ((has_client_trans_ && can_lock_current_sequence(sequence_name)) || (!has_client_trans_)))
            {
              if (OB_SUCCESS != (ret = prepare_lock_row(sequence_name)))
              {
                TBSYS_LOG(ERROR,"prepare the lock row failed!");
              }
              else if (OB_SUCCESS != (ret = check_lock_condition()))
              {
                TBSYS_LOG(WARN,"not inited!");
              }
              ObLockV0 lock_by_row(sql_context_);
              my_result_set_.reset();//reset for use
              set_use_lock_row_result_set(true);//for sequence data come from CS
//              TBSYS_LOG(ERROR,"the lock row is[%s]",to_cstring(lock_row_));
              if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = lock_by_row.lock_by_for_update(&lock_row_,  OB_ALL_SEQUENCE_TID, true, my_result_set_))))
              {
                TBSYS_LOG(ERROR,"lock sequence row failed!");
              }
              //(3)取当前sequence值并更新到本地
              TBSYS_LOG(DEBUG,"update next::[%s],update prev::[%s]",update_nextval == true? "true":"false",update_prevval==true? "true":"false");
              if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = this->update_sequence_info_map(update_prevval, update_nextval))))
              {
                TBSYS_LOG(WARN, "set current sequence row to map failed! [ret:%d]",ret);
              }
              set_use_lock_row_result_set(false);//for next use

              //(4)更新当前sequence
              //build the next value for the input sequence
              SequenceInfo s_info;
              if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = build_next_value_for_sequence(sequence_name, s_info))))
              {
                TBSYS_LOG(WARN, "build next value for [%.*s] failed!",sequence_name.length(),sequence_name.ptr());
              }
              if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = update_by_dml(sequence_name, s_info))))
              {
                TBSYS_LOG(ERROR, "update the sequence info failed!");
              }
              if (OB_SUCCESS == ret)
              {
                update_lock_sequence_map(sequence_name);
              }
            }
            //(5)解锁
            //(6)结束事务
            if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = end_sequence_trans())))//事务结束，自动解锁
            {
              TBSYS_LOG(WARN,"commit sequence trans failed!");
            }
          }//end else
        }//end if (!can_use...
        //b.获取本地的nextval值，在可用的情况下更新，不可用就报错
        if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = get_nextval_by_name(&sequence_name,value,can_use_nextval))))
        {
            TBSYS_LOG(ERROR, "get nextval failed!");
        }
        if (OB_SUCCESS == ret && !can_use_nextval)//the sequence can't be used any more
        {
          ret = OB_ERROR;
          TBSYS_LOG(USER_ERROR, "the sequence is out of range");
        }
        //c.使用完一个 next sequence 后，需要更新本地 map 中的prevval值，这里做记录，在一行结束后进行更新prevval值
        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(DEBUG,"has client_trans::[%s]",true == has_client_trans_?"yes":"no");
          if (OB_SUCCESS != (ret = add_single_row_sequence_name(sequence_name)))
          {
            TBSYS_LOG(ERROR, "add single row failed!");
          }
        }
      }
      else //if (!is_next_type) is preval type,
      {
        if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = get_prevval_by_name(&sequence_name, value, can_use_prevval))))
        {
            TBSYS_LOG(ERROR, "get the sequence prevval error!");
        }
        if (OB_SUCCESS == ret && (!can_use_prevval))//can't use
        {
          ret = OB_ERROR;
          //A PREVIOUS VALUE expression cannot be used before the NEXT VALUE
          TBSYS_LOG(USER_ERROR, "THE PREVVAL expression of  sequence [%.*s] can't be used before using NEXTVAL",sequence_name.length(),sequence_name.ptr());
        }
      }
      TBSYS_LOG(DEBUG,"set decimal::[%.*s]!",value.length(),value.ptr());
      if (OB_SUCCESS == ret && (can_use_nextval || can_use_prevval) && (OB_SUCCESS != (ret = expr_array_obj->set_decimal(value))))
      {
        TBSYS_LOG(ERROR, "replace the value for current column failed!");
      }
      if (OB_SUCCESS == ret)
      {
        if (is_next_type)
        {
          this->update_nextval_state_map(sequence_name);
        }
        else
        {
          this->update_prevval_state_map(sequence_name);
        }
      }
      //for update,we need to construct all squence_ids and store the sequence nextval
      if (OB_SUCCESS == ret && OB_SEQUENCE_UPDATE_LIMIT == dml_type && is_next_type && can_use_nextval)
      {
        //a.store value(nextval with decimal type,infact is a varchar
        if (OB_SUCCESS != (ret = add_nextval_to_project_op(value)))
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR,"failed to add the nextval to project op,ret=[%d]",ret);
        }
        //b.store the value_idx of current expr
        else if (OB_SUCCESS != (ret = add_idx_of_next_value_to_project_op(idx)))
        {
          TBSYS_LOG(ERROR, "failed to add idx of next value,ret=[%d]",ret);
        }
        //c.store the current expr_idx in out put columns
        else if (OB_SUCCESS != (ret = add_expr_idx_of_out_put_columns_to_project_op(out_put_columns_idx_)))
        {
          TBSYS_LOG(ERROR, "failed to add expr idx to project op,ret=[%d]",ret);
        }
        TBSYS_LOG(DEBUG,"the valueis [%.*s], the indx of valueis[%ld],the expr idxis[%ld]",value.length(),value.ptr(),idx,out_put_columns_idx_);
      }
    }//end if
  }//end if
  return ret;
}

/**
 * @brief a complex post expr is filled in this function by looping
 * @param post_expr_array [in][out] in with sequence name(varchar),out with the sequence value(decimal)
 * @param  dml_type [in]
 *
 */
int ObSequence::do_complex_fill(ObSEArray<ObObj,64> &post_expr_array, int dml_type)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCCESS == ret && i < post_expr_array.count(); i++)//for eache array item
  {
    int64_t expr_type = -1;
    if (ObIntType == post_expr_array[i].get_type())
    {
      if (OB_SUCCESS != (ret = post_expr_array[i].get_int(expr_type)))
      {
        TBSYS_LOG(WARN, "fail to get int value.err=%d", ret);
      }
      else if ((int64_t)ObPostfixExpression::CONST_OBJ == expr_type)//const obj
      {
        if (OB_SUCCESS != (ret = this->do_simple_fill(post_expr_array, i+=1, dml_type)))
        {
          if (OB_NOT_VARCHAR_OBJ == ret || OB_NOT_VALID_SEQUENCE_VARCHAR == ret)//not a sequence varchar obj
          {
            TBSYS_LOG(DEBUG, "not a varchar const_obj [ret::%d]",ret);
            ret = OB_SUCCESS;
            continue;
          }
          else
          {
            TBSYS_LOG(WARN, "do complex fill error![ret::%d]",ret);
          }
        }
      }
    }
  }//end for
  return ret;
}

/**
 *Use sequence name replace sequence value, because we will use new value replace sequence name when we handle next row.
 * @brief use the sequence name replace sequence value in expr.
 * @param [in]  ObSqlExpression                        &expr              : select caluse expression, contain value.
 * @param [in]  ObArray<std::pair<ObString,uint64_t> > &names_types_array : sequence name.
 * @param [out] ObSqlExpression                        &expr              : select caluse expression, contain name.
 */
void ObSequence::recover_the_sequence_expr(ObSqlExpression &expr, ObArray<std::pair<ObString,uint64_t> > &names_types_array)
{
//  TBSYS_LOG(DEBUG, "Pair array count:[%ld], sequence_idx_in_expr_.count:[%ld]",names_types_array.count(), sequence_idx_in_expr_.count());
  OB_ASSERT(names_types_array.count() == sequence_idx_in_expr_.count());
  int64_t idx = 0;
  ObString sequence_name;
  ObObj *expr_array_obj = NULL;
  ObPostfixExpression::ExprArray &post_expr_array = expr.get_expr_array();
  int64_t num = sequence_idx_in_expr_.count();
  for (int64_t i = 0; i < num; ++i)
  {
    sequence_name = names_types_array.at(i).first;
    idx = sequence_idx_in_expr_.at(i);
    expr_array_obj = const_cast<ObObj *>(&post_expr_array[idx]);
    expr_array_obj->set_varchar(sequence_name);
  }
  sequence_idx_in_expr_.clear();
}

void ObSequence::need_to_update_prevval(ObString &sequence_name, bool &update_prevval)
{
  int index = -1;
  if (-1 != (index = get_nextval_prevval_map_idx(sequence_name)))
  {
    if (0 == global_prevval_state_map[index])//没有使用过 prevval
    {
      update_prevval = true;
    }
    else
    {
      update_prevval = false;
    }
  }
  else
  {
    TBSYS_LOG(ERROR,"get index failed!");
  }
}
void ObSequence::need_to_update_nextval(ObString &sequence_name, bool &update_nextval)
{
  int index = -1;
  if (-1 != (index = get_nextval_prevval_map_idx(sequence_name)))
  {
    if (0 == single_row_nextval_state_map[index])//当前行没有使用过 nextval
    {
      update_nextval = true;
    }
    else
    {
      update_nextval = false;
    }
  }
  else
  {
    TBSYS_LOG(ERROR,"get index failed!");
  }
}
bool ObSequence::can_lock_current_sequence(ObString &sequence_name)
{
  bool can_lock = false;
  int index = -1;
  if (-1 != (index = get_nextval_prevval_map_idx(sequence_name)))
  {
    if (0 == lock_sequence_map[index])
    {
      can_lock = true;
    }
  }
  else
  {
    TBSYS_LOG(ERROR,"get index failed!");
  }
  return can_lock;
}

void ObSequence::update_nextval_state_map(ObString &sequence_name)
{
  int index = -1;
  if (-1 != (index = get_nextval_prevval_map_idx(sequence_name)))
  {
    single_row_nextval_state_map[index] = 1;
  }
  else
  {
    TBSYS_LOG(ERROR,"get index failed!");
  }
}
void ObSequence::update_prevval_state_map(ObString &sequence_name)
{
  int index = -1;
  if (-1 != (index = get_nextval_prevval_map_idx(sequence_name)))
  {
    global_prevval_state_map[index] = 1;
  }
  else
  {
    TBSYS_LOG(ERROR,"get index failed!");
  }
}

void ObSequence::update_lock_sequence_map(ObString &sequence_name)
{
  int index = -1;
  if (-1 != (index = get_nextval_prevval_map_idx(sequence_name)))
  {
    lock_sequence_map[index] = 1;
  }
  else
  {
    TBSYS_LOG(ERROR,"get index failed!");
  }
}


bool ObSequence::get_cur_seuqnece_type_for_insert()
{
  bool is_next = true;
  ObArray<uint64_t> &col_sequences = col_sequence_types_.at(col_sequence_types_idx_);
  if (1 == col_sequences.count())//该列只有一个sequence
  {
    is_next = ((common::NEXT_TYPE == col_sequences.at(0)) ? true : false);
    col_sequence_types_idx_++;
  }
  else if (col_sequences.count() > 1)//该列有大仧?sequence，返回当前列的第col_sequence_types_idx_idx_位置的sequence类型
  {
    is_next = ((common::NEXT_TYPE == col_sequences.at(col_sequence_types_idx_idx_)) ? true : false);
    col_sequence_types_idx_idx_++;
    if (col_sequences.count() == col_sequence_types_idx_idx_)//该列的sequence信息取完亍
    {
      col_sequence_types_idx_++;
      col_sequence_types_idx_idx_ = 0;
    }
  }
  else
  {
    TBSYS_LOG(ERROR,"won't be here!");//won't be here!
  }
  return is_next;
}

bool ObSequence::get_cur_seuqnece_type_for_select(int dml_type, int &ret)
{
  bool is_next = false;
  OB_ASSERT(sequence_names_idx_for_select_ < sequence_name_type_pairs_.count());
  common::ObArray<std::pair<common::ObString,uint64_t> >&names_types_array = sequence_name_type_pairs_.at(sequence_names_idx_for_select_);
  int64_t pair_idx = 0;
  if (OB_SEQUENCE_SELECT_WHERE_LIMIT == dml_type)
  {
    pair_idx = sequence_pair_idx_for_where_;
  }
  else if (OB_SEQUENCE_SELECT_LIMIT == dml_type)
  {
    pair_idx = sequence_idx_in_expr_.count();
  }
  else
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "dml type is wrong.");
  }
  if (OB_SUCCESS == ret)
  {
    if (common::NEXT_TYPE == names_types_array.at(pair_idx).second)
    {
      is_next = true;
    }
    else if (common::PREV_TYPE == names_types_array.at(pair_idx).second)
    {
      is_next = false;
    }
  }
  return is_next;
}

bool ObSequence::get_cur_sequence_type_for_update()
{
  bool is_next = true;
  common::ObArray<std::pair<common::ObString,uint64_t> >&names_types_array = sequence_name_type_pairs_.at(col_sequence_types_idx_);
  if (1 == names_types_array.count())
  {
    is_next = ((common::NEXT_TYPE == names_types_array.at(0).second) ? true : false);
    col_sequence_types_idx_++;
  }
  else if (names_types_array.count() > 1)//该列有大于1个的 sequence，返回当前列的第col_sequence_types_idx_idx_位置的sequence类型
  {
    is_next = ((common::NEXT_TYPE == names_types_array.at(col_sequence_types_idx_idx_).second) ? true : false);
    col_sequence_types_idx_idx_++;
    if (names_types_array.count() == col_sequence_types_idx_idx_)//该列的sequence信息取完亍
    {
      col_sequence_types_idx_++;
      col_sequence_types_idx_idx_ = 0;
    }
  }
  return is_next;
}

bool ObSequence::can_use_current_nextval(ObString &sequence_name)
{
  bool can_use = false;
  int index = -1;
  if (-1 != (index = get_nextval_prevval_map_idx(sequence_name)))
  {
    if (1 == single_row_nextval_state_map[index])
    {
      can_use = true;
    }
  }
  else
  {
    TBSYS_LOG(ERROR,"get index failed!");
  }
  return can_use;
}
void ObSequence::reset_nextval_state_map()
{
  //标记"0"sequence在当前行中没有使用过nextval
  //标记"1"sequence在当前行中使用过nextval
  for (int i = 0; i < OB_MAX_SEQUENCE_USE_IN_SINGLE_STATEMENT; i++)
  {
    single_row_nextval_state_map[i] = 0;
  }
}
int ObSequence::check_column_cast_validity(ObSqlExpression& expr, const int64_t &column_idx)
{
  int ret = OB_SUCCESS;
  if (out_row_desc_.get_column_num() <= 0)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR,"the row_desc is not inited");
  }
  if (OB_SUCCESS == ret)
  {
    char var_buf[OB_MAX_VARCHAR_LENGTH];
    ObRow val_row;
    val_row.set_row_desc(out_row_desc_);
    ObString varchar;
    ObObj casted_cell;
    varchar.assign_ptr(var_buf, OB_MAX_VARCHAR_LENGTH);
    casted_cell.set_varchar(varchar);
    const ObObj *single_value = NULL;
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;
    ObObj data_type;
    //add lbzhong [Update rowkey] 20160510:b
    int64_t tmp_column_idx = 0;
    //add:e
    if ((ret = expr.calc(val_row, single_value)) != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "Calculate value result failed, err=%d", ret);
    }
    else if (
             //add lbzhong [Update rowkey] 20160510:b
             column_idx != -1 &&
             //add:e
             OB_SUCCESS != (ret = out_row_desc_ext_.get_by_idx(column_idx, table_id, column_id, data_type)))
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "Failed to get column, err=%d", ret);
    }
    //add lbzhong [Update rowkey] 20160510:b
    else if (column_idx == -1 &&
             OB_SUCCESS != (ret = out_row_desc_ext_.get_by_id(expr.get_table_id(), expr.get_column_id(), tmp_column_idx, data_type)))
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "Failed to get column, err=%d", ret);
    }
    //add:e
    else
    {
      if (OB_SUCCESS != (ret = obj_cast(*single_value, data_type, casted_cell, single_value)))
      {
        TBSYS_LOG(WARN, "failed to cast obj, err=%d", ret);
      }
    }
  }
  return ret;
}
