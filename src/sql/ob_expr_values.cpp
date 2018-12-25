/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_expr_values.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_expr_values.h"
#include "ob_duplicate_indicator.h"
#include "common/utility.h"
#include "common/ob_obj_cast.h"
#include "common/hash/ob_hashmap.h"
//add dragon [varchar limit] 2016-8-12 12:09:42
#include "common/ob_schema.h"
#include "mergeserver/ob_merge_server_main.h"
#include "mergeserver/ob_merge_server.h"
//add e
using namespace oceanbase::sql;
using namespace oceanbase::common;
//add dragon [varchar limit] 2016-8-15 09:00:18
using namespace oceanbase::mergeserver;
//add e

ObExprValues::ObExprValues()
  :values_(OB_TC_MALLOC_BLOCK_SIZE, ModulePageAllocator(ObModIds::OB_SQL_ARRAY)),
   from_deserialize_(false),
   check_rowkey_duplicat_(false),
   check_rowkey_duplicat_rep_(false),//add hushuang[Secondary Index]for replace bug:20161103
   do_eval_when_serialize_(false),
   //add wenghaixing[decimal] for fix delete bug 2014/10/10
   is_del_update(false)
   //add e
   , row_num_(0), from_sub_query_(false) //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715
   , from_ud_(false) //add gaojt [Delete_Update_Function] [JHOBv0.1] 20151219
   //add dragon [varchar limit] 2016-8-15 12:50:22
   , manager_(NULL)
   //add e
{
}

ObExprValues::~ObExprValues()
{
}

//add dragon [varchar limit] 2016-8-12 10:53:57
int ObExprValues::init ()
{
  int ret = OB_SUCCESS;
  manager_ = NULL;
  //1.get mergeserver
  ObMergeServerMain *ins = ObMergeServerMain::get_instance ();

  if(NULL == ins)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "get ms instance failed");
  }
  else
  {
    //2.get merge schema manager
    manager_ = ins->get_merge_server ().get_schema_mgr();
    if(NULL == manager_)
    {
      ret = OB_ERR_NULL_POINTER;
      TBSYS_LOG(ERROR, "get manager failed");
    }
  }
  return ret;
}
//add e

//add wenghaixing[decimal] for fix delete bug 2014/10/10
void ObExprValues::set_del_upd(){
    is_del_update=true;
}
//add e

void ObExprValues::reset()
{
  row_desc_.reset();
  row_desc_ext_.reset();
  values_.clear();
  row_store_.clear();
  //row_.reset(false, ObRow::DEFAULT_NULL);
  from_deserialize_ = false;
  check_rowkey_duplicat_ = false;
  check_rowkey_duplicat_rep_ = false;//add hushuang[Secondary Index]for replace bug:20161103
  do_eval_when_serialize_ = false;
   //add wenghaixing[decimal] for fix delete bug 2014/10/10
   is_del_update=false;
   //add e
  //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715:b
  result_row_store_.clear();
  row_num_ = 0;
  from_sub_query_ = false;
  //add 20140715:e
  //add gaojt [Delete_Update_Function] [JHOBv0.1] 20151219:b
  from_ud_ = false;
  //add gaojt 20151219:e
}

void ObExprValues::reuse()
{
  row_desc_.reset();
  row_desc_ext_.reset();
  values_.clear();
  row_store_.clear();
  //row_.reset(false, ObRow::DEFAULT_NULL);
  from_deserialize_ = false;
  check_rowkey_duplicat_ = false;
  check_rowkey_duplicat_rep_ = false;//add hushuang[Secondary Index]for replace bug:20161103
  do_eval_when_serialize_ = false;
    //add wenghaixing[decimal] for fix delete bug 2014/10/10
   is_del_update=false;
    //add e
  //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715:b
  result_row_store_.clear();
  row_num_ = 0;
  from_sub_query_ = false;
  //add 20140715:e
  //add gaojt [Delete_Update_Function] [JHOBv0.1] 20151219:b
  from_ud_ = false;
  //add gaojt 20151219:e
}

int ObExprValues::set_row_desc(const common::ObRowDesc &row_desc, const common::ObRowDescExt &row_desc_ext)
{
  row_desc_ = row_desc;
  row_desc_ext_ = row_desc_ext;
  return OB_SUCCESS;
}

int ObExprValues::add_value(const ObSqlExpression &v)
{
  int ret = OB_SUCCESS;
  if ((ret = values_.push_back(v)) == OB_SUCCESS)
  {
    values_.at(values_.count() - 1).set_owner_op(this);
  }
  return ret;
}

int ObExprValues::open()
{
  int ret = OB_SUCCESS;
  if (from_deserialize_)
  {
    row_store_.reset_iterator();
    row_.set_row_desc(row_desc_);
    // pass
  }
  else if (0 >= row_desc_.get_column_num()
      || 0 >= row_desc_ext_.get_column_num())
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "row_desc not init");
  }
  //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715:b
  /*Exp:sub_query branch,different with normal values*/
  else if(from_sub_query_)
  {
    row_.set_row_desc(row_desc_);
  }
  //add 20140715:e
  //add gaojt [Delete_Update_Function] [JHOBv0.1] 20151219:b
  else if(from_ud_)
  {
    row_.set_row_desc(row_desc_);
  }
  //add gaojt 20151219:e
  else if (0 >= values_.count())
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "values not init");
  }
  else
  {
    row_.set_row_desc(row_desc_);
    row_store_.reuse();
    if (OB_SUCCESS != (ret = eval()))
    {
      TBSYS_LOG(WARN, "failed to eval exprs, err=%d", ret);
    }
  }
  return ret;
}

int ObExprValues::close()
{
  if (from_deserialize_)
  {
    row_store_.reset_iterator();
  }
  else
  {
    row_store_.reuse();
  }
  return OB_SUCCESS;
}
//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140911:b
/*Exp: reset the environment*/
int ObExprValues::reset_stuff_for_insert()
{
  int ret = OB_SUCCESS;
  result_row_store_.clear();
  row_num_ = 0;
  return ret;
}
//add 20140911:e
//add gaojt [Delete_Update_Function] [JHOBv0.1] 20151204:b
/*expr: reset environment for batch update and delete*/
int ObExprValues::reset_stuff_for_ud()
{
  int ret = OB_SUCCESS;
  result_row_store_.clear();
  row_num_ = 0;
  return ret;
}
//add gaojt 20151204:e
int ObExprValues::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = row_store_.get_next_row(row_)))
  {
    if (OB_ITER_END != ret)
    {
      TBSYS_LOG(WARN, "failed to get next row from row store, err=%d", ret);
    }
  }
  else
  {
    row = &row_;
  }
  return ret;
}

int ObExprValues::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  row_desc = &row_desc_;
  return OB_SUCCESS;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObExprValues, PHY_EXPR_VALUES);
  }
}

int64_t ObExprValues::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ExprValues(values_num=%ld, values=",
                  values_.count());
  pos += values_.to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, ", row_desc=");
  pos += row_desc_.to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, ")\n");
  return pos;
}

int ObExprValues::eval()
{
  int ret = OB_SUCCESS;
  OB_ASSERT(0 < values_.count());
  OB_ASSERT(0 < row_desc_.get_column_num());
  OB_ASSERT(0 == (values_.count() % row_desc_.get_column_num()));
  ModuleArena buf(OB_COMMON_MEM_BLOCK_SIZE, ModulePageAllocator(ObModIds::OB_SQL_TRANSFORMER));
  char* varchar_buff = NULL;
  if (NULL == (varchar_buff = buf.alloc(OB_MAX_VARCHAR_LENGTH)))
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(WARN, "No memory");
  }
  else
  {
    const ObRowStore::StoredRow *stored_row = NULL;
    ObDuplicateIndicator *indicator = NULL;
    ObDuplicateIndicator *indicator_rep = NULL;//add hushuang[Secondary Index]for replace bug:20161103
    int64_t col_num = row_desc_.get_column_num();
    int64_t row_num = values_.count() / col_num;
    // RowKey duplication checking doesn't need while 1 row only
    if (check_rowkey_duplicat_ && row_num > 1)
    {
      void *ind_buf = NULL;
      if (row_desc_.get_rowkey_cell_count() <= 0)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "RowKey is empty, ret=%d", ret);
      }
      else if ((ind_buf = buf.alloc(sizeof(ObDuplicateIndicator))) == NULL)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "Malloc ObDuplicateIndicator failed, ret=%d", ret);
      }
      else
      {
        indicator = new (ind_buf) ObDuplicateIndicator();
        if ((ret = indicator->init(row_num)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Init ObDuplicateIndicator failed, ret=%d", ret);
        }
      }
    }
    //add by hushuang [Secondary Index] for replace bug:20161103
    if (check_rowkey_duplicat_rep_ && row_num > 1)
    {
      void *ind_buf = NULL;
      if (row_desc_.get_rowkey_cell_count() <= 0)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "RowKey is empty, ret=%d", ret);
      }
      else if ((ind_buf = buf.alloc(sizeof(ObDuplicateIndicator))) == NULL)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "Malloc ObDuplicateIndicator_Rep failed, ret=%d", ret);
      }
      else
      {
        indicator_rep = new (ind_buf) ObDuplicateIndicator();
        if ((ret = indicator_rep->init(row_num)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Init ObDuplicateIndicator_Rep failed, ret=%d", ret);
        }
      }

      for (int64_t i = values_.count() - col_num ;check_rowkey_duplicat_rep_ && OB_SUCCESS == ret && i >= 0; i-=col_num)//for each row
      {
        ObRow val_row;
        val_row.set_row_desc(row_desc_);
        ObString varchar;
        ObObj casted_cell;
        for (int64_t j = 0; OB_SUCCESS == ret && j < col_num; ++j)
        {
          varchar.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
          casted_cell.set_varchar(varchar); // reuse the varchar buffer
          const ObObj *single_value = NULL;
          uint64_t table_id = OB_INVALID_ID;
          uint64_t column_id = OB_INVALID_ID;
          ObObj tmp_value;
          ObObj data_type;
          ObSqlExpression &val_expr = values_.at(i+j);
          if ((ret = val_expr.calc(val_row, single_value)) != OB_SUCCESS) // the expr should be a const expr here
          {
            TBSYS_LOG(WARN, "Calculate value result failed, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = row_desc_ext_.get_by_idx(j, table_id, column_id, data_type)))
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, "Failed to get column, err=%d", ret);
          }
          else
          {
            if(is_del_update)
            {
              if (OB_SUCCESS != obj_cast(*single_value, data_type, casted_cell, single_value))
              {
                TBSYS_LOG(DEBUG, "failed to cast obj, err=%d", ret);
              }
            }
            else
            {
              if (OB_SUCCESS != (ret = obj_cast(*single_value, data_type, casted_cell, single_value)))
              {
                TBSYS_LOG(WARN, "failed to cast obj, err=%d", ret);
              }
            }
          }
          if(OB_SUCCESS!=ret){
          }
          else if (OB_SUCCESS != (ret = ob_write_obj_v2(buf, *single_value, tmp_value)))
          {
            TBSYS_LOG(WARN, "str buf write obj fail:ret[%d]", ret);
          }
          else if ((ret = val_row.set_cell(table_id, column_id, tmp_value)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "Add value to ObRow failed");
          }
          else
          {
           //TBSYS_LOG(DEBUG, "i=%ld j=%ld cell=%s", i, j, to_cstring(tmp_value));
          }
        } // end for
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
          if (indicator_rep)
          {
            const ObRowkey *rowkey = NULL;
            bool is_dup = false;
            if ((ret = val_row.get_rowkey(rowkey)) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN, "Get RowKey failed, err=%d", ret);
            }
            else if ((ret = indicator_rep->have_seen(*rowkey, is_dup)) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN, "Check duplication failed, err=%d", ret);
            }
            else if(is_dup)
            {
              //TBSYS_LOG(ERROR, "is_dup");
              ret = OB_SUCCESS;
              continue;
            }
          }
        }
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
          if (OB_SUCCESS != (ret = row_store_.add_row(val_row, stored_row)))
          {
            TBSYS_LOG(WARN, "failed to add row into store, err=%d", ret);
          }
        }
      }//end for
      if (indicator_rep)
      {
        indicator_rep->~ObDuplicateIndicator();
      }
    }
    //add:e 20161103
    //add hushuang [Secondary Index] for replace bug:20161104
    else
   {//add:e
    for (int64_t i = 0; OB_SUCCESS == ret && i < values_.count(); i+=col_num) // for each row
    {
      ObRow val_row;
      val_row.set_row_desc(row_desc_);
      ObString varchar;
      ObObj casted_cell;
      for (int64_t j = 0; OB_SUCCESS == ret && j < col_num; ++j)
      {
        varchar.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
        casted_cell.set_varchar(varchar); // reuse the varchar buffer
        const ObObj *single_value = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        ObObj tmp_value;
        ObObj data_type;
        ObSqlExpression &val_expr = values_.at(i+j);
        if ((ret = val_expr.calc(val_row, single_value)) != OB_SUCCESS) // the expr should be a const expr here
        {
          TBSYS_LOG(WARN, "Calculate value result failed, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = row_desc_ext_.get_by_idx(j, table_id, column_id, data_type)))
        {
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(WARN, "Failed to get column, err=%d", ret);
        }
        /*
        else if (0 < row_desc_.get_rowkey_cell_count()
                 && j < row_desc_.get_rowkey_cell_count()
                 && single_value->is_null())
        {
          TBSYS_LOG(USER_ERROR, "primary key can not be null");
          ret = OB_ERR_INSERT_NULL_ROWKEY;
        }
        */
         //modify wenghaixing[decimal] for fix delete bug 2014/10/10
        else
        {   if(is_del_update)
            {
                if (OB_SUCCESS != obj_cast(*single_value, data_type, casted_cell, single_value))
                {
                    TBSYS_LOG(DEBUG, "failed to cast obj, err=%d", ret);
                }
            }
            else
            {
                if (OB_SUCCESS != (ret = obj_cast(*single_value, data_type, casted_cell, single_value)))
                {
                        TBSYS_LOG(WARN, "failed to cast obj, err=%d", ret);
                }
            }

        }
        //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
        //else if (OB_SUCCESS != (ret = ob_write_obj(buf, *single_value, tmp_value))) old code
                //old code
        /*
           else if (OB_SUCCESS != (ret = obj_cast(*single_value, data_type, casted_cell, single_value)))
        {
          TBSYS_LOG(WARN, "failed to cast obj, err=%d", ret);
        }
        */
        //modify e
        if(OB_SUCCESS!=ret){
        }
        else if (OB_SUCCESS != (ret = ob_write_obj_v2(buf, *single_value, tmp_value)))
            //modify:e
        {
          TBSYS_LOG(WARN, "str buf write obj fail:ret[%d]", ret);
        }
        else if ((ret = val_row.set_cell(table_id, column_id, tmp_value)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Add value to ObRow failed");
        }
        else
        {
          //TBSYS_LOG(DEBUG, "i=%ld j=%ld cell=%s", i, j, to_cstring(tmp_value));
        }
      } // end for
      if (OB_LIKELY(OB_SUCCESS == ret))
      {
        if (OB_SUCCESS != (ret = row_store_.add_row(val_row, stored_row)))
        {
          TBSYS_LOG(WARN, "failed to add row into store, err=%d", ret);
        }
        else if (indicator)
        {
          const ObRowkey *rowkey = NULL;
          bool is_dup = false;
          if ((ret = val_row.get_rowkey(rowkey)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "Get RowKey failed, err=%d", ret);
          }
          else if ((ret = indicator->have_seen(*rowkey, is_dup)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "Check duplication failed, err=%d", ret);
          }
          else if (is_dup)
          {
            ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
            TBSYS_LOG(USER_ERROR, "Duplicate entry \'%s\' for key \'PRIMARY\'", to_cstring(*rowkey));
          }
          TBSYS_LOG(DEBUG, "check rowkey isdup is %c rowkey=%s", is_dup?'Y':'N', to_cstring(*rowkey));
        }
      }
    }   // end for
  }
    if (indicator)
    {
      indicator->~ObDuplicateIndicator();
    }
  }
  return ret;
}

PHY_OPERATOR_ASSIGN(ObExprValues)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObExprValues);
  reset();
  row_desc_ = o_ptr->row_desc_;
  row_desc_ext_ = o_ptr->row_desc_ext_;

  values_.reserve(o_ptr->values_.count());
  for (int64_t i = 0; i < o_ptr->values_.count(); i++)
  {
    if ((ret = this->values_.push_back(o_ptr->values_.at(i))) == OB_SUCCESS)
    {
      this->values_.at(i).set_owner_op(this);
    }
    else
    {
      break;
    }
  }
  do_eval_when_serialize_ = o_ptr->do_eval_when_serialize_;
  check_rowkey_duplicat_ = o_ptr->check_rowkey_duplicat_;
  // Does not need to assign row_store_, because this function is used by MS only before opening
  return ret;
}

DEFINE_SERIALIZE(ObExprValues)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (do_eval_when_serialize_)
  {
    if (OB_SUCCESS != (ret = (const_cast<ObExprValues*>(this))->open()))
    {
      TBSYS_LOG(WARN, "failed to open expr_values, err=%d", ret);
    }
  }

  if (OB_LIKELY(OB_SUCCESS == ret))
  {
    if (OB_SUCCESS != (ret = row_desc_.serialize(buf, buf_len, tmp_pos)))
    {
      TBSYS_LOG(WARN, "serialize row_desc fail ret=%d buf=%p buf_len=%ld pos=%ld", ret, buf, buf_len, tmp_pos);
    }
    else if (OB_SUCCESS != (ret = row_store_.serialize(buf, buf_len, tmp_pos)))
    {
      TBSYS_LOG(WARN, "serialize row_store fail ret=%d buf=%p buf_len=%ld pos=%ld", ret, buf, buf_len, tmp_pos);
    }
    else
    {
      pos = tmp_pos;
    }
  }
  if (do_eval_when_serialize_)
  {
    (const_cast<ObExprValues*>(this))->close();
  }
  return ret;
}

DEFINE_DESERIALIZE(ObExprValues)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (OB_SUCCESS != (ret = row_desc_.deserialize(buf, data_len, tmp_pos)))
  {
    TBSYS_LOG(WARN, "serialize row_desc fail ret=%d buf=%p data_len=%ld pos=%ld", ret, buf, data_len, tmp_pos);
  }
  else if (OB_SUCCESS != (ret = row_store_.deserialize(buf, data_len, tmp_pos)))
  {
    TBSYS_LOG(WARN, "serialize row_store fail ret=%d buf=%p data_len=%ld pos=%ld", ret, buf, data_len, tmp_pos);
  }
  else
  {
    from_deserialize_ = true;
    pos = tmp_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObExprValues)
{
  return (row_desc_.get_serialize_size() + row_store_.get_serialize_size());
}
//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715:b

/*Exp:add sub_query's result to values*/
int ObExprValues::add_values(const common::ObRow &value)
{
  const ObRowStore::StoredRow *stored_row = NULL;
  return result_row_store_.add_row(value, stored_row);
}

//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20141024:b
/*Exp: check whether sub_query's result is duplicate,
* and move the result to row_store_*/
int ObExprValues::store_input_values()
{
  int ret = OB_SUCCESS;
  OB_ASSERT(0 < row_desc_.get_column_num());
  ModuleArena buf(OB_COMMON_MEM_BLOCK_SIZE, ModulePageAllocator(ObModIds::OB_SQL_TRANSFORMER));
  char* varchar_buff = NULL;
  if (NULL == (varchar_buff = buf.alloc(OB_MAX_VARCHAR_LENGTH)))
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(WARN, "No memory");
  }
  else
  {
    const ObRowStore::StoredRow *stored_row = NULL;
    ObDuplicateIndicator *indicator = NULL;
    if(from_sub_query_ && row_num_ >1 )
    {
        void *ind_buf = NULL;
        if (row_desc_.get_rowkey_cell_count() <= 0)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "RowKey is empty, ret=%d", ret);
        }
        else if (NULL == (ind_buf = buf.alloc(sizeof(ObDuplicateIndicator))))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "Malloc ObDuplicateIndicator failed, ret=%d", ret);
        }
        else
        {
          indicator = new (ind_buf) ObDuplicateIndicator();
          if ((ret = indicator->init(row_num_)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "Init ObDuplicateIndicator failed, ret=%d", ret);
          }
        }
    }
    int64_t col_num = row_desc_.get_column_num();
    ObRow temp_row;
    temp_row.set_row_desc(row_desc_);
    while(OB_SUCCESS == ret)
    {
      ret = result_row_store_.get_next_row(temp_row);
      if(OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
        break;
      }
      else if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to get next row from sub query");
      }
      else
      {
        ObRow val_row;
        val_row.set_row_desc(row_desc_);
        ObString varchar;
        ObObj casted_cell;
        for (int64_t j = 0; OB_SUCCESS == ret && j < col_num; ++j)
        {
          varchar.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
          casted_cell.set_varchar(varchar); // reuse the varchar buffer
          const ObObj *single_value = NULL;
          uint64_t table_id = OB_INVALID_ID;
          uint64_t column_id = OB_INVALID_ID;
          ObObj tmp_value;
          ObObj data_type;
          if (OB_SUCCESS != (ret = row_desc_ext_.get_by_idx(j, table_id, column_id, data_type)))
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, "Failed to get column, err=%d", ret);
          }
          else if(OB_SUCCESS != (ret = temp_row.get_cell(table_id, column_id, single_value)))
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, "Failed to get column value, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = obj_cast(*single_value, data_type, casted_cell, single_value)))
          {
            TBSYS_LOG(WARN, "failed to cast obj, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = ob_write_obj_v2(buf, *single_value, tmp_value)))
          {
            TBSYS_LOG(WARN, "str buf write obj fail:ret[%d]", ret);
          }
          else if (OB_SUCCESS != (ret = val_row.set_cell(table_id, column_id, tmp_value)))
          {
            TBSYS_LOG(WARN, "Add value to ObRow failed");
          }
        }
        if(ret == OB_SUCCESS)
        {
          if (OB_SUCCESS != (ret = row_store_.add_row(val_row, stored_row)))
          {
            TBSYS_LOG(WARN, "failed to add row into store, err=%d", ret);
          }
          else if (indicator)
          {
            const ObRowkey *rowkey = NULL;
            bool is_dup = false;
            if ((ret = val_row.get_rowkey(rowkey)) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN, "Get RowKey failed, err=%d", ret);
            }
            else if ((ret = indicator->have_seen(*rowkey, is_dup)) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN, "Check duplication failed, err=%d", ret);
            }
            else if (is_dup)
            {
              ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
              TBSYS_LOG(USER_ERROR, "Duplicate entry \'%s\' for key \'PRIMARY\'", to_cstring(*rowkey));
            }
            TBSYS_LOG(DEBUG, "check rowkey isdup is %c rowkey=%s", is_dup?'Y':'N', to_cstring(*rowkey));
          }
       }

        //add dragon [varchar limit] 2016-8-12 11:10:19
        //do varchar length check
        if(OB_SUCCESS == ret)
        {
          if(OB_SUCCESS != (ret = do_varchar_len_check(val_row)))
          {
            TBSYS_LOG(WARN, "varchar check not pass..");
            break;
          }
        }
        //add e
      }
    }
  }
  return ret;
}
//add 20141024:e

//add dragon [varchar limit] 2016-8-12 11:14:27
int ObExprValues::check_self()
{
  int ret = OB_SUCCESS;
  if(NULL == manager_ && OB_SUCCESS != init())
  {
    TBSYS_LOG(WARN, "check self failed, ret = %d", ret);
  }
  return ret;
}

int ObExprValues::alloc_schema_mgr(const ObSchemaManagerV2 *&schema_mgr)
{
  int ret = OB_SUCCESS;
  schema_mgr = NULL;
  int64_t sv = manager_->get_latest_version (); //获取最新的schema版本号
  if(NULL == (schema_mgr = manager_->get_user_schema (sv)))
  {
    ret = OB_ERR_NULL_POINTER;
    TBSYS_LOG(WARN, "get user schema failed, schema version is %ld", sv);
  }
  return ret;
}

int ObExprValues::release_schema (const ObSchemaManagerV2 *schema_mgr)
{
  int ret = OB_SUCCESS;
  if(NULL != schema_mgr)
  {
    ret = manager_->release_schema (schema_mgr);
  }
  return ret;
}

int ObExprValues::do_varchar_len_check(ObRow &row)
{
  int ret = OB_SUCCESS;
  int err = OB_SUCCESS; //用于判断schema_mgr是否释放成功
  if(OB_SUCCESS != (ret = check_self()))
  {
    TBSYS_LOG(WARN, "do self check failed, ret[%d]", ret);
  }
  else
  {
    const ObObj *cell = NULL;
    //申请schema_mgr_
    const ObSchemaManagerV2 *schema_mgr = NULL;
    if(OB_SUCCESS != (ret = alloc_schema_mgr(schema_mgr)))
    {
      TBSYS_LOG(WARN, "alloc ob schema managerV2 failed, ret[%d]", ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "alloc ob schema_mgr[%p] succ", schema_mgr);
    }
    for(int64_t cell_idx = 0; OB_SUCCESS == ret && cell_idx < row.get_column_num (); cell_idx++)
    {
      if(OB_SUCCESS != (ret = row.raw_get_cell (cell_idx, cell)))
      {
        TBSYS_LOG(WARN, "get cell[%ld] failed", cell_idx);
        break;
      }
      //如果获取到的cell的类型是varchar，则进行长度判断
      else if(ObVarcharType == cell->get_type ())
      {
        const ObRowDesc *row_desc = row.get_row_desc ();//获取行描述
        uint64_t tid = OB_INVALID_ID;
        uint64_t cid = OB_INVALID_ID;
        ret = row_desc->get_tid_cid(cell_idx, tid, cid); //获取表id和列id
        //获取column schema
        const ObColumnSchemaV2 *col_sche = schema_mgr->get_column_schema (tid, cid);
        if(NULL != col_sche)
        {
          int64_t length_sche = col_sche->get_size (); //字段属性的长度
          int64_t length_var = cell->get_data_length (); //变量的长度
          if(length_var > length_sche) //compare
          {
            TBSYS_LOG(WARN, "Varchar is too long, tid[%ld], cid[%ld], length in schema[%ld], "
                      "length in vachar[%ld]", tid, cid, length_sche, length_var);
            ret = OB_ERR_VARCHAR_TOO_LONG;
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "fetch column schema failed!");
          ret = OB_ERR_NULL_POINTER;
        }
      }
    }
    if(OB_SUCCESS != (err = release_schema (schema_mgr)))
    {
      TBSYS_LOG(WARN, "release schema failed! errno[%d]", err);
      if(OB_SUCCESS == ret)
        ret = err;
    }
  }
  return ret;
}
//add e
