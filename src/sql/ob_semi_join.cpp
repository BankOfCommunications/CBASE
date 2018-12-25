/**
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_semi_join.h
 *
 * Authors:
 *   LEI WANG <seilwang@163.com>
 *
 */
/*add by wanglei [semi join] 20151106*/
#include "ob_table_rpc_scan.h"
#include "ob_table_mem_scan.h"
#include "ob_postfix_expression.h"
#include <map>
#include <utility>
#include <string>
#include "common/ob_define.h"
#include "ob_semi_join.h"
#include "common/utility.h"
#include "common/ob_row_util.h"
#include "sql/ob_physical_plan.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace std;
ObSemiJoin::ObSemiJoin()
  :get_next_row_func_(NULL),
    last_left_row_(NULL),
    last_right_row_(NULL),
    right_cache_is_valid_(false),
    is_right_iter_end_(false),
    is_unequal_join_(false)
  ,hashmap_num_(0),
    TwoMpackect_limit(1.5*1024*1024),
    is_use_semi_join_(true),
    is_can_use_semi_join_(true),
    //mem_size_limit_(0),
    //sort_reader_(&in_mem_sort_),
    is_use_index_(false),
    first_index_table_id_(OB_INVALID_ID),
    second_index_table_id_(OB_INVALID_ID),
    table_filter_expr_(NULL),
    src_table_filter_expr_(NULL)
{
  arena_.set_mod_id(ObModIds::OB_MS_SUB_QUERY);
  last_left_row_has_printed_ = false;
  last_right_row_has_printed_ = false;
  left_cache_is_valid_ = false;
  is_left_iter_end_ = false;
  left_row_desc_ = NULL;
  id_ = 0;
  use_in_expr_ = false;
  use_btw_expr_ =false;
  right_table_filter_ =NULL;
  //add dragon [Bugfix 1224]
  is_alias_table_ = false;
  is_use_second_index_storing_ = false;
  is_use_second_index_without_storing_ = false;
  //add e
}
int ObSemiJoin::close()
{
  int ret = OB_SUCCESS;
  char *store_buf = last_join_left_row_store_.ptr();
  if (NULL != store_buf)
  {
    ob_free(store_buf);
    last_join_left_row_store_.assign_ptr(NULL, 0);
  }
  last_left_row_ = NULL;
  last_right_row_ = NULL;
  right_cache_is_valid_ = false;
  is_right_iter_end_ = false;
  is_unequal_join_ = false;//add liumz, [unequal_join]20150924
  //add dragon [Bugfix 1224] 2016-8-25 09:49:43
  is_alias_table_ = false;
  is_use_second_index_storing_ = false;
  is_use_second_index_without_storing_ = false;
  //add e
  //add dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
  last_left_row_has_printed_ = false;
  last_right_row_has_printed_ = false;
  left_cache_is_valid_ = false;
  is_left_iter_end_ = false;
  char *right_store_buf = last_join_right_row_store_.ptr();
  if (NULL != right_store_buf)
  {
    ob_free(right_store_buf);
    last_join_right_row_store_.assign_ptr(NULL, 0);
  }
  arena_.free();
  //add 20140610:e
  left_cache_for_store_.clear();
  right_cache_for_store_.clear();
  in_mem_sort_.reset();
  merge_sort_.reset();
  sort_reader_ = &in_mem_sort_;
  row_desc_.reset();
  sort_columns_.clear();
  ret = ObJoin::close();
  return ret;
}
ObSemiJoin::~ObSemiJoin()
{
  char *store_buf = last_join_left_row_store_.ptr();
  if (NULL != store_buf)
  {
    ob_free(store_buf);
    last_join_left_row_store_.assign_ptr(NULL, 0);
  }
  char *right_store_buf = last_join_right_row_store_.ptr();
  if (NULL != right_store_buf)
  {
    ob_free(right_store_buf);
    last_join_right_row_store_.assign_ptr(NULL, 0);
  }
  sub_result_.~ObArray();
  for(int i=0;i<MAX_SUB_QUERY_NUM;i++)
  {
    sub_query_map_[i].destroy();
  }
  arena_.free();
  in_mem_sort_.reset();
  merge_sort_.reset();
  sort_reader_ = &in_mem_sort_;
  sort_columns_.clear();
  left_cache_for_store_.clear();
  right_cache_for_store_.clear();
}
void ObSemiJoin::reset()
{
  get_next_row_func_ = NULL;
  last_left_row_ = NULL;
  last_right_row_ = NULL;
  char *store_buf = last_join_left_row_store_.ptr();
  if (NULL != store_buf)
  {
    ob_free(store_buf);
    last_join_left_row_store_.assign_ptr(NULL, 0);
  }
  char *right_store_buf = last_join_right_row_store_.ptr();
  if (NULL != right_store_buf)
  {
    ob_free(right_store_buf);
    last_join_right_row_store_.assign_ptr(NULL, 0);
  }
  last_left_row_has_printed_ = false;
  last_right_row_has_printed_ = false;
  left_cache_is_valid_ = false;
  left_cache_.clear();
  is_left_iter_end_ = false;
  right_cache_.clear();
  row_desc_.reset();
  right_cache_is_valid_ = false;
  is_right_iter_end_ = false;
  is_unequal_join_ = false;
  //add dragon [Bugfix 1224]
  is_alias_table_ = false;
  is_use_second_index_storing_ = false;
  is_use_second_index_without_storing_ = false;
  //add e
  left_cache_for_store_.clear();
  right_cache_for_store_.clear();
  equal_join_conds_.clear();
  other_join_conds_.clear();
  //sort_columns_.clear();
  left_op_ = NULL;
  right_op_ = NULL;
  for(int i =0;i<MAX_SUB_QUERY_NUM;i++)
    sub_query_map_[i].clear();
  hashmap_num_ = 0;
  in_mem_sort_.reset();
  merge_sort_.reset();
  sort_reader_ = &in_mem_sort_;
}
void ObSemiJoin::reuse()
{
  get_next_row_func_ = NULL;
  last_left_row_ = NULL;
  last_right_row_ = NULL;
  char *store_buf = last_join_left_row_store_.ptr();
  if (NULL != store_buf)
  {
    ob_free(store_buf);
    last_join_left_row_store_.assign_ptr(NULL, 0);
  }
  char *right_store_buf = last_join_right_row_store_.ptr();
  if (NULL != right_store_buf)
  {
    ob_free(right_store_buf);
    last_join_right_row_store_.assign_ptr(NULL, 0);
  }
  last_left_row_has_printed_ = false;
  last_right_row_has_printed_ = false;
  left_cache_is_valid_ = false;
  left_cache_.reuse();
  is_left_iter_end_ = false;
  right_cache_.reuse();
  left_cache_for_store_.reuse();
  right_cache_for_store_.reuse();
  row_desc_.reset();
  right_cache_is_valid_ = false;
  is_right_iter_end_ = false;
  is_unequal_join_ = false;
  //add dragon [Bugfix 1224]
  is_alias_table_ = false;
  is_use_second_index_storing_ = false;
  is_use_second_index_without_storing_ = false;
  //add e
  equal_join_conds_.clear ();
  other_join_conds_.clear();
  //sort_columns_.clear();
  left_op_ = NULL;
  right_op_ = NULL;
  for(int i =0;i<MAX_SUB_QUERY_NUM;i++)
    sub_query_map_[i].clear();
  hashmap_num_ = 0;
  in_mem_sort_.reset();
  merge_sort_.reset();
  sort_reader_ = &in_mem_sort_;
}
int ObSemiJoin::set_join_type(const ObJoin::JoinType join_type)
{
  int ret = OB_SUCCESS;
  ObJoin::set_join_type(join_type);
  switch(join_type)
  {
  case INNER_JOIN:
    get_next_row_func_ = &ObSemiJoin::inner_semi_get_next_row;
    break;
  case LEFT_OUTER_JOIN:
    get_next_row_func_ = &ObSemiJoin::left_semi_get_next_row;
    break;
  case RIGHT_OUTER_JOIN:
    get_next_row_func_ = &ObSemiJoin::right_semi_get_next_row;
    break;
  case LEFT_ANTI_SEMI_JOIN:
    get_next_row_func_ = &ObSemiJoin::left_anti_semi_get_next_row;
    break;
  case RIGHT_ANTI_SEMI_JOIN:
    get_next_row_func_ = &ObSemiJoin::right_anti_semi_get_next_row;
    break;
  default:
    ret = OB_ERR_UNEXPECTED;
    break;
  }
  return ret;
}
int ObSemiJoin::reset_join_type()
{
  int ret = OB_SUCCESS;
  if (is_unequal_join_)
  {
    ObJoin::JoinType join_type = ObJoin::get_join_type();
    switch(join_type)
    {
    case LEFT_SEMI_JOIN:
      get_next_row_func_ = &ObSemiJoin::left_semi_get_next_row;
      break;
    case RIGHT_SEMI_JOIN:
      get_next_row_func_ = &ObSemiJoin::right_semi_get_next_row;
      break;
    case LEFT_ANTI_SEMI_JOIN:
      get_next_row_func_ = &ObSemiJoin::left_anti_semi_get_next_row;
      break;
    case RIGHT_ANTI_SEMI_JOIN:
      get_next_row_func_ = &ObSemiJoin::right_anti_semi_get_next_row;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
    }
  }
  return ret;
}
void ObSemiJoin::set_is_can_use_semi_join(const bool &is_can_use_semi_join)
{
  is_can_use_semi_join_=is_can_use_semi_join;
}
int ObSemiJoin::get_data_for_distinct(const common::ObObj *cell,
                                      ObObjType type,
                                      char *buff,
                                      int buff_size,
                                      int& fliter_data_type)
{
  int ret = OB_SUCCESS;
  if(cell == NULL)
  {
    ret = OB_ERR_POINTER_IS_NULL;
  }
  else
  {
    memset(buff,'\0',buff_size);
    int64_t int_val = 0;
    int32_t int_val_32 = 0;
    float float_val = 0.0;
    double double_val = 0.0;
    bool is_add = false;
    ObString str_val;
    switch (type) {
    case ObNullType:
      break;
    case ObInt32Type:
      cell->get_int32(int_val_32,is_add);
      fliter_data_type = (int)sizeof(int32_t);
      sprintf(buff,"%d",int_val_32);
      break;
    case ObIntType:
      cell->get_int(int_val,is_add);
      fliter_data_type = (int)sizeof(int64_t);
      sprintf(buff,"%ld",int_val);
      break;
    case ObVarcharType:
      cell->get_varchar(str_val);
      fliter_data_type = (int)sizeof(ObString);
      snprintf(buff, buff_size, "%.*s", str_val.length(), str_val.ptr());
      break;
    case ObFloatType:
      cell->get_float(float_val,is_add);
      fliter_data_type = (int)sizeof(float);
      sprintf(buff,"%f",float_val);
      break;
    case ObDoubleType:
      cell->get_double(double_val,is_add);
      fliter_data_type = (int)sizeof(double);
      sprintf(buff,"%.12lf",double_val);
      break;
    case ObDateTimeType:
      cell->get_datetime(int_val,is_add);
      sprintf(buff,"%ld",int_val);
      break;
    case ObPreciseDateTimeType:
      cell->get_precise_datetime(int_val,is_add);
      sprintf(buff,"%ld",int_val);
      break;
    case ObSeqType:
      break;
    case ObCreateTimeType:
      cell->get_createtime(int_val);
      sprintf(buff,"%ld",int_val);
      break;
    case ObModifyTimeType:
      cell->get_modifytime(int_val);
      sprintf(buff,"%ld",int_val);
      break;
    case ObExtendType:
      cell->get_ext(int_val);
      if (common::ObObj::MIN_OBJECT_VALUE == int_val)
      {
      }
      else if (common::ObObj::MAX_OBJECT_VALUE == int_val)
      {
      }
      else
      {
      }
      break;
    case ObBoolType:
      break;
    case ObDecimalType:
    {
      ObString num;
      cell->get_decimal(num);
      ObDecimal od;
      char res[MAX_PRINTABLE_SIZE];
      memset(res,0,MAX_PRINTABLE_SIZE);
      if(OB_SUCCESS != od.from(num.ptr(),num.length())){
        TBSYS_LOG(WARN,"failed to convert decimal from buf");
      }
      else if (OB_SUCCESS!=od.modify_value(cell->get_precision(),cell->get_scale())){
        TBSYS_LOG(WARN,"failed to modify decimal");
      }
      else{
        od.to_string(res,MAX_PRINTABLE_SIZE);
      }
      snprintf(buff, buff_size, "%.*s", num.length(), num.ptr());
      break;
    }
    default:
      break;
    }
  }
  return ret;
}
int ObSemiJoin::get_tid_cid_for_cons_filter(ObArray<int64_t> &left_join_tid,
                                            ObArray<int64_t> &left_join_cid,
                                            ObArray<int64_t> &right_join_tid,
                                            ObArray<int64_t> &right_join_cid,
                                            const int flag)
{
  UNUSED(flag);
  int ret = OB_SUCCESS;
  if(0 >= equal_join_conds_.count())
  {
    ret = OB_ERR_HAS_NO_EQUI_COND;
  }
  else
  {
    for (int64_t i = 0; i < equal_join_conds_.count(); ++i)
    {
      const ObSqlExpression &expr = equal_join_conds_.at(i);
      ExprItem::SqlCellInfo c1;
      ExprItem::SqlCellInfo c2;
      if (expr.is_equijoin_cond(c1, c2))
      {
        left_join_tid.push_back(c1.tid);
        left_join_cid.push_back(c1.cid);
        right_join_tid.push_back(c2.tid);
        right_join_cid.push_back(c2.cid);
      }
    }
  }
  return ret;
}
int ObSemiJoin::cons_filter_for_left_table(ObSqlExpression *&table_filter_expr_,
                                           int &index_param_count,
                                           ObArray<int64_t> &left_join_tid,
                                           ObArray<int64_t> &left_join_cid,
                                           ObArray<int64_t> &right_join_tid,
                                           ObArray<int64_t> &right_join_cid)
{
  UNUSED(left_join_tid);
  UNUSED(left_join_cid);
  UNUSED(right_join_tid);
  UNUSED(right_join_cid);
  UNUSED(table_filter_expr_);
  UNUSED(index_param_count);
  //暂时关闭right join功能
  /*UNUSED(left_join_tid);
    UNUSED(left_join_cid);
    int limit_num = 4000;
    int ret=OB_SUCCESS;
    int filter_data_size = 0;
    int64_t total_size = 0;
    int curr_buket = 0;
    const int hash_map_bum=4;
    //map<string,bool> is_confilct;
    item_hash_array is_confilct;
    distinct_hash *tmp_hash=new distinct_hash[hash_map_bum];
    for(int i = 0;i <hash_map_bum;i++)
    {
        if(OB_SUCCESS != (ret = tmp_hash[i].create(hash::cal_next_prime(1024*1024*10))))
        {
            TBSYS_LOG(WARN, "hash create failed ,ret=[%d]", ret);
        }
        else
        {
            is_confilct.push_back(&tmp_hash[i]);
        }
    }
    if(OB_SUCCESS == ret)
    {
        ExprItem dem1,dem2,dem3,dem5,dem6,dem7;
        dem1.type_ = T_REF_COLUMN;
        //后缀表达式组建
        for (int64_t i = 0; i < 1; ++i)
        {
            const ObSqlExpression &expr = equal_join_conds_.at(i);
            ExprItem::SqlCellInfo c1;
            ExprItem::SqlCellInfo c2;
            if (expr.is_equijoin_cond(c1, c2))
            {
                dem1.value_.cell_.tid = c1.tid;
                dem1.value_.cell_.cid = c1.cid;
                table_filter_expr_->set_tid_cid(c1.tid,c1.cid);
                table_filter_expr_->add_expr_item(dem1);
            }
        }
        dem2.type_ = T_OP_ROW;
        dem2.data_type_ = ObMinType;
        dem2.value_.int_ = 1;
        table_filter_expr_->add_expr_item(dem2);
        dem3.type_ = T_OP_LEFT_PARAM_END;
        dem3.data_type_ = ObMinType;
        dem3.value_.int_ = 2;
        table_filter_expr_->add_expr_item(dem3);
        const ObRow *row_temp = NULL;
        while(OB_SUCCESS == (ret= right_op_->get_next_row(row_temp)))
        {
            if(total_size <= TwoMpackect_limit && index_param_count <= limit_num)
            {
                is_use_semi_join_ = true;
            }
            else
            {
                is_use_semi_join_ = false;
                break;
            }
            const ObRowStore::StoredRow *stored_row = NULL;
            right_cache_for_store_.add_row(*row_temp, stored_row);
            stored_row = NULL;
            if(OB_SUCCESS!=ret)
                TBSYS_LOG(WARN, "no more rows , sret=[%d]", ret);
            else
            {
                const common::ObObj *cell_temp=NULL;
                if(OB_SUCCESS!=(ret=row_temp->get_cell(right_join_tid.at(0),right_join_cid.at(0),cell_temp)))
                {
                    TBSYS_LOG(WARN, "get cell failure , ret=[%d]", ret);
                }
                else
                {
                    char buff[65536];
                    if(OB_SUCCESS!=(ret=get_data_for_distinct(cell_temp,
                                                              cell_temp->get_type(),
                                                              buff,
                                                              sizeof(buff),
                                                              filter_data_size)))
                    {
                        TBSYS_LOG(WARN, "get data faliure , ret=[%d]", ret);
                    }
                    else
                    {
                        ObString tmp_str;
                        tmp_str.assign_ptr(buff,sizeof(buff));
                        //string str_temp(buff);
                        bool tmp=true;
                        // if(is_confilct.find(str_temp)==is_confilct.end())

                        if(0 == index_param_count%100000)
                        {
                            curr_buket++;
                            if(curr_buket > hash_map_bum)
                            {
                                is_use_semi_join_=false;
                                return OB_ERR_CAN_NOT_USE_SEMI_JOIN;
                            }
                        }
                        //if(hash::HASH_NOT_EXIST == is_confilct.get(buff,tmp))
                        if(!is_in_hash_map(curr_buket,tmp_str,is_confilct))
                        {
                            is_confilct.at(curr_buket-1)->set(tmp_str,tmp);
                            ObConstRawExpr     col_val;
                            if (OB_SUCCESS != (ret = col_val.set_value_and_type(*cell_temp)))
                            {
                                TBSYS_LOG(ERROR, "failed to set column value, err=%d", ret);
                                break;
                            }
                            else
                            {
                                if (OB_SUCCESS != (ret = col_val.fill_sql_expression(
                                         *table_filter_expr_)))
                                {
                                    TBSYS_LOG(ERROR,"Add cell value failed");
                                    break;
                                }
                            }
                            dem5.type_ = T_OP_ROW;
                            dem5.data_type_ = ObMinType;
                            dem5.value_.int_ =1;
                            table_filter_expr_->add_expr_item(dem5);
                            index_param_count++;
                            row_temp = NULL;
                            total_size+=cell_temp->get_serialize_size();
                        }
                        else
                        {
                            row_temp = NULL;
                            continue;
                        }

                    }
                }
            }
        }
        for(int i = 0;i <hash_map_bum;i++)
        {
            is_confilct.at(i)->clear();
        }
        if(OB_ITER_END == ret)
        {
            ret =OB_SUCCESS;
        }
        if(OB_SUCCESS == ret)
        {
            //is_confilct.clear();
            if(0!=index_param_count)
            {
                dem6.type_ = T_OP_ROW;
                dem6.data_type_ = ObMinType;
                dem6.value_.int_ = index_param_count;
                table_filter_expr_->add_expr_item(dem6);
            }
            dem7.type_ = T_OP_IN;
            dem7.data_type_ = ObMinType;
            dem7.value_.int_ = 2;
            table_filter_expr_->add_expr_item(dem7);
            table_filter_expr_->add_expr_item_end();
        }
    }*/
  int ret = OB_SUCCESS;
  return ret;
}

bool  ObSemiJoin::is_use_second_index()
{
  return is_use_index_;
}

//add dragon [Bugfix 1224] 2016-8-25 09:20:57
int ObSemiJoin::set_alias_table (uint64_t tid, ObSelectStmt *&stmt, bool aliasT)
{
  int ret = OB_SUCCESS;
  aliasT = false;
  if(NULL == stmt && OB_INVALID_ID == tid)
  {
    TBSYS_LOG(ERROR, "Select Stmt or tid has not been init");
    ret = OB_ERR_UNEXPECTED;
  }
  if(OB_SUCCESS == ret)
  {
    TableItem *ti = NULL;
    if(NULL == (ti = stmt->get_table_item_by_id (tid)))
    {
      TBSYS_LOG(ERROR, "table[%ld] not belong to this stmt[%p]", tid, stmt);
      ret = OB_ERR_UNEXPECTED;
    }
    else if(ti->alias_name_.length () > 0)
    {
      aliasT = true;
    }
    else if(ti->alias_name_.length () <= 0)
    {
      aliasT = false;
    }
    else
    {
      TBSYS_LOG(WARN, "can't be here! aliasT[%s]", aliasT ? "true" : "false");
      ret = OB_ERROR;
    }
  }
  is_alias_table_ = aliasT;
  return ret;
}

bool ObSemiJoin::get_aliasT() const
{
  return is_alias_table_;
}
//add e

//add dragon [Bugfix 1224] 2016-8-30 11:57:15
bool ObSemiJoin::is_use_second_index_storing()
{
  return is_use_second_index_storing_;
}

bool ObSemiJoin::is_use_second_index_without_storing()
{
  return is_use_second_index_without_storing_;
}

int ObSemiJoin::set_is_use_second_index_storing(const bool &is_use_index_storing)
{
  is_use_second_index_storing_ = is_use_index_storing;
  return OB_SUCCESS;
}

int ObSemiJoin::set_is_use_second_index_without_storing(const bool &is_use_index_without_storing)
{
  is_use_second_index_without_storing_ = is_use_index_without_storing;
  return OB_SUCCESS;
}
//add 2016-8-30 11:57:19e

int  ObSemiJoin::set_is_use_second_index(const bool &is_use_index)
{
  is_use_index_ = is_use_index;
  return OB_SUCCESS;
}

int ObSemiJoin::set_index_table_id(uint64_t &first_index_table_id,uint64_t &second_index_table_id)
{
  first_index_table_id_ = first_index_table_id;
  second_index_table_id_ = second_index_table_id;
  return OB_SUCCESS;
}

bool ObSemiJoin::is_in_hash_map(int & curr_buket, ObString &str, item_hash_array &is_confilct)
{
  bool tmp_b=true;
  for(int i = 0;i < curr_buket;i++)
  {
    if(hash::HASH_EXIST == is_confilct.at(i)->get(str,tmp_b))
    {
      return true;
    }
  }
  return false;
}

//left semi join && semi join
int ObSemiJoin::cons_filter_for_right_table(ObSqlExpression *&table_filter_expr_,
                                            ObSqlExpression *&src_table_filter_expr_,
                                            int &index_param_count,
                                            ObArray<int64_t> &left_join_tid,
                                            ObArray<int64_t> &left_join_cid,
                                            ObArray<int64_t> &right_join_tid,
                                            ObArray<int64_t> &right_join_cid)
{
  UNUSED(right_join_tid);
  UNUSED(right_join_cid);
  int ret = OB_SUCCESS;
  uint64_t index_column_id = OB_INVALID_ID;
  int limit_num = 4000;
  int filter_data_size = 0;
  int64_t total_size = 0;
  //目前暂时使用库函数map，后面会给成ob自带的hashset。
  map<string,bool> is_confilct;
  //这段代码是使用ob的hash map做数据去重，目前存在问题:b
  //    int curr_buket = 0;
  //    const int hash_map_num=2;

  //    item_hash_array is_confilct;
  //    distinct_hash *tmp_hash = new distinct_hash[hash_map_num];
  //    for(int i = 0;i <hash_map_num;i++)
  //    {
  //       if(OB_SUCCESS != (ret = tmp_hash[i].create(hash::cal_next_prime(1024*1024))))
  //        {
  //            TBSYS_LOG(WARN, "hash create failed ,ret=[%d]", ret);
  //        }
  //        else
  //        {
  //            is_confilct.push_back(&tmp_hash[i]);
  //        }
  //    }
  //e
  if(OB_SUCCESS == ret)
  {
    ExprItem dem1,dem2,dem3,dem5,dem6,dem7;
    dem1.type_ = T_REF_COLUMN;
    //后缀表达式组建
    for (int64_t i = 0; i < 1; ++i)  //测试一下等值条件有两个的情况
    {
      const ObSqlExpression &expr = equal_join_conds_.at(i);
      ExprItem::SqlCellInfo c1;
      ExprItem::SqlCellInfo c2;
      if (expr.is_equijoin_cond(c1, c2))
      {
        uint64_t tmp_tid = OB_INVALID_ID;
        if(is_use_second_index())
          tmp_tid = second_index_table_id_;
        else
          tmp_tid = c2.tid;
        dem1.value_.cell_.tid = tmp_tid;
        dem1.value_.cell_.cid = c2.cid;
        index_column_id = c2.cid;
        table_filter_expr_->set_tid_cid(tmp_tid,c2.cid);
        table_filter_expr_->add_expr_item(dem1);
        if(is_use_second_index())
        {
          dem1.value_.cell_.tid = c2.tid;
          dem1.value_.cell_.cid = c2.cid;
          src_table_filter_expr_->set_tid_cid(c2.tid,c2.cid);
          src_table_filter_expr_->add_expr_item(dem1);
        }
      }
    }
    dem2.type_ = T_OP_ROW;
    dem2.data_type_ = ObMinType;
    dem2.value_.int_ = 1;
    table_filter_expr_->add_expr_item(dem2);
    if(is_use_second_index())
    {
      src_table_filter_expr_->add_expr_item(dem2);
    }
    dem3.type_ = T_OP_LEFT_PARAM_END;
    dem3.data_type_ = ObMinType;
    dem3.value_.int_ = 2;
    table_filter_expr_->add_expr_item(dem3);
    if(is_use_second_index())
    {
      src_table_filter_expr_->add_expr_item(dem3);
    }
    const ObRow *row_temp = NULL;
    while(OB_SUCCESS == (ret = left_op_->get_next_row(row_temp)))
    {
      const ObRowStore::StoredRow *stored_row = NULL;
      if(row_temp != NULL)
      {
        left_cache_for_store_.add_row(*row_temp, stored_row);
      }
      else
      {
        //异常未处理
      }
      stored_row = NULL;
      if(total_size <= TwoMpackect_limit && index_param_count <= limit_num)
      {
        is_use_semi_join_ = true;
      }
      else
      {
        is_use_semi_join_ = false;
        break;
      }
      if(OB_SUCCESS!=ret)
        TBSYS_LOG(WARN, "no more rows , sret=[%d]", ret);
      else
      {
        const common::ObObj *cell_temp=NULL;
        if(OB_SUCCESS!=(ret=row_temp->get_cell(left_join_tid.at(0),left_join_cid.at(0),cell_temp)))
        {
          TBSYS_LOG(WARN, "get cell failure , ret=[%d]", ret);
        }
        else
        {
          char buff[65536];
          if(OB_SUCCESS!=(ret=get_data_for_distinct(cell_temp,
                                                    cell_temp->get_type(),
                                                    buff,
                                                    sizeof(buff),
                                                    filter_data_size)))
          {
            TBSYS_LOG(WARN, "get data faliure , ret=[%d]", ret);
          }
          else
          {

            string str_temp(buff);
            bool tmp=true;
//            这段代码是使用ob的hash map做数据去重，目前存在问题:b
//            ObString tmp_str;
//            tmp_str.assign_ptr(buff,sizeof(buff));
//            bool tmp=true;
//            if(0 == index_param_count%1250)
//            {
//                curr_buket++;
//                if(curr_buket > hash_map_num)
//                {
//                    is_use_semi_join_=false;
//                    return OB_ERR_CAN_NOT_USE_SEMI_JOIN;
//                }
//            }
//            if(!is_in_hash_map(curr_buket,tmp_str,is_confilct))
//            e
            if(is_confilct.find(str_temp)==is_confilct.end())
            {
              //这段代码是使用ob的hash map做数据去重，目前存在问题:b
              //is_confilct.at(curr_buket-1)->set(tmp_str,tmp);
              //e
              is_confilct.insert(make_pair<string,bool>(str_temp,tmp));
              ObConstRawExpr     col_val;
              if (OB_SUCCESS != (ret = col_val.set_value_and_type(*cell_temp)))
              {
                TBSYS_LOG(ERROR, "failed to set column value, err=%d", ret);
                break;
              }
              else
              {
                if ((ret = col_val.fill_sql_expression(
                       *table_filter_expr_)) != OB_SUCCESS)
                {
                  TBSYS_LOG(ERROR,"Add cell value failed");
                  break;
                }
                if(is_use_second_index())
                {
                  if ((ret = col_val.fill_sql_expression(
                         *src_table_filter_expr_)) != OB_SUCCESS)
                  {
                    TBSYS_LOG(ERROR,"Add cell value failed");
                    break;
                  }
                }
              }

              dem5.type_ = T_OP_ROW;
              dem5.data_type_ = ObMinType;
              dem5.value_.int_ =1;
              table_filter_expr_->add_expr_item(dem5);
              if(is_use_second_index())
              {
                src_table_filter_expr_->add_expr_item(dem5);
              }
              index_param_count++;
              row_temp = NULL;
              //total_size+=cell_temp->get_serialize_size();
            }
            else
            {
              row_temp = NULL;
              continue;
            }
          }
        }
      }
    }
    is_confilct.clear ();
//    这段代码是使用ob的hash map做数据去重，目前存在问题:b
//    for(int i = 0;i <hash_map_num;i++)
//    {
//        is_confilct.at(i)->distory();
//    }
//    e
    if(OB_ITER_END == ret)
    {
      ret =OB_SUCCESS;
    }
    if(OB_SUCCESS == ret)
    {
      if(0!=index_param_count)
      {
        dem6.type_ = T_OP_ROW;
        dem6.data_type_ = ObMinType;
        dem6.value_.int_ = index_param_count;
        table_filter_expr_->add_expr_item(dem6);
        if(is_use_second_index())
        {
          src_table_filter_expr_->add_expr_item(dem6);
        }
      }
      dem7.type_ = T_OP_IN;
      dem7.data_type_ = ObMinType;
      dem7.value_.int_ = 2;
      table_filter_expr_->add_expr_item(dem7);
      table_filter_expr_->add_expr_item_end();
      if(is_use_second_index())
      {
        src_table_filter_expr_->add_expr_item(dem7);
        src_table_filter_expr_->add_expr_item_end();
      }
    }
  }
  return ret;
  //delete(tmp_hash);
  //add output column
  /*if(is_use_second_index())
    {
        ObTableRpcScan * main_query=NULL;
        ObSingleChildPhyOperator *sort_query=NULL;
        sort_query = dynamic_cast<ObSingleChildPhyOperator *>(get_child(1));
        main_query = dynamic_cast<ObTableRpcScan *>(sort_query->get_child(0));
        ObBinaryRefRawExpr col_expr(second_index_table_id_, index_column_id, T_REF_COLUMN);
        ObSqlRawExpr col_raw_expr(
                    common::OB_INVALID_ID,
                    second_index_table_id_,
                    index_column_id,
                    &col_expr);
        ObSqlExpression output_expr;
        if ((ret = col_raw_expr.fill_sql_expression(
                 output_expr)) != OB_SUCCESS)

        {
            //TRANS_LOG("Add table output columns faild");
        }
        else if((ret = main_query->add_output_column(output_expr)) != OB_SUCCESS)
        {
           // TRANS_LOG("Add table output columns faild");
        }
    }*/
  /*UNUSED(right_join_tid);
    UNUSED(right_join_cid);
    bool need_merge = false;
    int ret=OB_SUCCESS;
    int filter_data_size = 0;
    int64_t total_size = 0;
    int curr_buket = 0;
    const int hash_map_bum=2;
    //map<string,bool> is_confilct;
    item_hash_array is_confilct;
    distinct_hash *tmp_hash=new distinct_hash[hash_map_bum];
    for(int i = 0;i <hash_map_bum;i++)
    {
        tmp_hash[i].create(hash::cal_next_prime(1024*1024));
        is_confilct.push_back(&tmp_hash[i]);
    }
    ExprItem dem1,dem2,dem3,dem5,dem6,dem7;
    dem1.type_ = T_REF_COLUMN;
    //后缀表达式组建
    for (int64_t i = 0; i < 1; ++i)
    {
        const ObSqlExpression &expr = equal_join_conds_.at(i);
        ExprItem::SqlCellInfo c1;
        ExprItem::SqlCellInfo c2;
        if (expr.is_equijoin_cond(c1, c2))
        {
            dem1.value_.cell_.tid = c2.tid;
            dem1.value_.cell_.cid = c2.cid;
            table_filter_expr_->set_tid_cid(c2.tid,c2.cid);
            table_filter_expr_->add_expr_item(dem1);
        }
    }
    dem2.type_ = T_OP_ROW;
    dem2.data_type_ = ObMinType;
    dem2.value_.int_ = 1;
    table_filter_expr_->add_expr_item(dem2);
    dem3.type_ = T_OP_LEFT_PARAM_END;
    dem3.data_type_ = ObMinType;
    dem3.value_.int_ = 2;
    table_filter_expr_->add_expr_item(dem3);
    const ObRow *row_temp = NULL;
    while(OB_SUCCESS == left_op_->get_next_row(row_temp))
    {
        if(total_size <= TwoMpackect_limit)
        {
            is_use_semi_join_ = true;
        }
        else
        {
            is_use_semi_join_ = false;
            break;
        }
//        const ObRowStore::StoredRow *stored_row = NULL;
//        left_cache_for_store_.add_row(*row_temp, stored_row);
//        stored_row = NULL;

        store_row_in_merge_sort(row_temp,need_merge);
        if(OB_SUCCESS!=ret)
            TBSYS_LOG(WARN, "no more rows , sret=[%d]", ret);
        else
        {
            const common::ObObj *cell_temp=NULL;
            if(OB_SUCCESS!=(ret=row_temp->get_cell(left_join_tid.at(0),left_join_cid.at(0),cell_temp)))
            {
                TBSYS_LOG(WARN, "get cell failure , ret=[%d]", ret);
            }
            else
            {
                char buff[512];
                if(OB_SUCCESS!=(ret=get_data_for_distinct(cell_temp,
                                                          cell_temp->get_type(),
                                                          buff,
                                                          sizeof(buff),
                                                          filter_data_size)))
                {
                    TBSYS_LOG(WARN, "get data faliure , ret=[%d]", ret);
                }
                else
                {
                    ObString tmp_str;
                    tmp_str.assign_ptr(buff,sizeof(buff));
                    //string str_temp(buff);
                    bool tmp=true;
                    // if(is_confilct.find(str_temp)==is_confilct.end())

                    if(0 == index_param_count%100000)
                    {
                        curr_buket++;
                        if(curr_buket > hash_map_bum)
                        {
                            is_use_semi_join_=false;
                            return OB_ERR_CAN_NOT_USE_SEMI_JOIN;
                        }
                    }
                    //if(hash::HASH_NOT_EXIST == is_confilct.get(buff,tmp))
                    if(!is_in_hash_map(curr_buket,tmp_str,is_confilct))
                    {
                        is_confilct.at(curr_buket-1)->set(tmp_str,tmp);
                        ObConstRawExpr     col_val;
                        if (OB_SUCCESS != (ret = col_val.set_value_and_type(*cell_temp)))
                        {
                            TBSYS_LOG(ERROR, "failed to set column value, err=%d", ret);
                            break;
                        }
                        else
                        {
                            if ((ret = col_val.fill_sql_expression(
                                     *table_filter_expr_)) != OB_SUCCESS)
                            {
                                TBSYS_LOG(ERROR,"Add cell value failed");
                                break;
                            }
                        }
                        dem5.type_ = T_OP_ROW;
                        dem5.data_type_ = ObMinType;
                        dem5.value_.int_ =1;
                        table_filter_expr_->add_expr_item(dem5);
                        index_param_count++;
                        row_temp = NULL;
                        total_size+=cell_temp->get_serialize_size();
                    }
                    else
                    {
                        row_temp = NULL;
                        continue;
                    }
                }
            }
        }
    }
    for(int i = 0;i <hash_map_bum;i++)
    {
       is_confilct.at(i)->clear();
    }
    //is_confilct.clear();
    if(0!=index_param_count)
    {
        dem6.type_ = T_OP_ROW;
        dem6.data_type_ = ObMinType;
        dem6.value_.int_ = index_param_count;
        table_filter_expr_->add_expr_item(dem6);
    }
    dem7.type_ = T_OP_IN;
    dem7.data_type_ = ObMinType;
    dem7.value_.int_ = 2;
    table_filter_expr_->add_expr_item(dem7);
    table_filter_expr_->add_expr_item_end();
    return ret;*/
}
void ObSemiJoin::set_id(int id)
{
  id_ =id;
}
void ObSemiJoin::set_use_in(bool value)
{
  use_in_expr_ =value;
}
void ObSemiJoin::set_use_btw(bool value)
{
  use_btw_expr_ = value;
}
void ObSemiJoin::set_right_table_filter(ObFilter *r_filter)
{
  right_table_filter_ = r_filter;
}
int ObSemiJoin::add_right_table_filter(ObSqlExpression* ose)
{
  int ret = OB_SUCCESS;
//  ObSqlExpression* expr_clone = ObSqlExpression::alloc(); // @todo temporary work around
//  if (NULL == expr_clone)
//  {
//      ret = OB_ALLOCATE_MEMORY_FAILED;
//      TBSYS_LOG(WARN, "no memory");
//  }
//  else
//  {
//      *expr_clone = *ose;
//      if (OB_SUCCESS != (ret = right_table_filter_.add_filter(expr_clone)))
//      {
//          TBSYS_LOG(WARN, "fail to add filter to rpc scan operator. ret=%d", ret);
//      }
//  }
  //add wanglei [semi join ] fix now() 20160520：b
  ObTableRpcScan * main_query=NULL;
  ObSingleChildPhyOperator *sort_query=NULL;
  sort_query = dynamic_cast<ObSingleChildPhyOperator *>(get_child(1));
  if(sort_query == NULL)
  {}
  else
  {
    main_query = dynamic_cast<ObTableRpcScan *>(sort_query->get_child(0));
    if(main_query == NULL)
    {}
    else
    {
      ose->set_owner_op (main_query);
    }
  }
  //add wanglei [semi join ] fix now() 20160520：e
  if (OB_SUCCESS != (ret = right_table_filter_->add_filter(ose)))
  {
    TBSYS_LOG(WARN, "fail to add filter to rpc scan operator. ret=%d", ret);
  }
  return ret;
}
int ObSemiJoin::add_filter_to_rpc_scan(int &index_param_count,ObSqlExpression *&table_filter_expr,ObSqlExpression *&src_table_filter_expr,const int flag)
{
  UNUSED(src_table_filter_expr);
  int ret=OB_SUCCESS;
  ObTableRpcScan * main_query=NULL;
  ObSingleChildPhyOperator *sort_query=NULL;
  sort_query = dynamic_cast<ObSingleChildPhyOperator *>(get_child(flag));
  //add 20151229 16:43 :b
  if(sort_query != NULL)
  {
    //add :e
    if(is_use_semi_join_ && 0 != index_param_count)
    {
      if(sort_query->get_child(0) != NULL)
      {
        if(sort_query->get_child(0)->get_type() == PHY_TABLE_RPC_SCAN)
        {
          main_query = dynamic_cast<ObTableRpcScan *>(sort_query->get_child(0));
          if(is_use_second_index())
          {
            if((ret = main_query->add_main_filter(src_table_filter_expr)) != OB_SUCCESS)
            {
              //异常信息待定
            }
            else if((ret = main_query->add_filter(table_filter_expr)) != OB_SUCCESS)
            {
              //异常信息待定
            }
            else if((ret = main_query->add_index_filter_ll(table_filter_expr)) != OB_SUCCESS)
            {
              //异常信息待定
            }
          }
          else
            main_query->add_filter(table_filter_expr);
          //table_filter_expr->set_owner_op(main_query);
        }
        else
        {
          switch(sort_query->get_child(0)->get_type())
          {
          case PHY_TABLE_MEM_SCAN:
          {
            ObTableMemScan *memscan = NULL;
            memscan=dynamic_cast<ObTableMemScan *>(sort_query->get_child(0));
            memscan->add_filter(table_filter_expr);
            table_filter_expr->set_owner_op(memscan);
            while(1)
            {
              if(memscan->get_child(0)->get_child(0) != NULL)
              {
                if(PHY_TABLE_RPC_SCAN == memscan->get_child(0)->get_child(0)->get_type())
                {
                  main_query = dynamic_cast<ObTableRpcScan *>(memscan->get_child(0)->get_child(0));
                  break;
                }
                else
                {
                  memscan = dynamic_cast<ObTableMemScan *>(memscan->get_child(0)->get_child(0));
                }
              }
              else
              {
                //异常未处理
                ret = OB_ERR_POINTER_IS_NULL;
                //ret = OB_SUCCESS;//for UT
                TBSYS_LOG(WARN,"table rpc scan operator is null! ret=%d",OB_ERR_POINTER_IS_NULL);
              }
            }
          }
            break;
          case PHY_PROJECT:
            break;
          default:
            break;
          }
        }
      }
      else
      {
        //异常未处理
        ret = OB_ERR_POINTER_IS_NULL;
        //ret = OB_SUCCESS;//for UT
        TBSYS_LOG(WARN,"table rpc scan operator is null! ret=%d",OB_ERR_POINTER_IS_NULL);
      }
    }
    else
    {
      if(0 == index_param_count)
        ret = OB_ERR_INDEX_NUM_IS_ZERO;
    }
  }
  else
  {
    //异常未处理
    ret = OB_ERR_POINTER_IS_NULL;
    //ret = OB_SUCCESS;//for UT
    TBSYS_LOG(WARN,"sort operator is null! ret=%d",OB_ERR_POINTER_IS_NULL);

  }
  return ret;
}
int ObSemiJoin::add_sort_column(const uint64_t tid, const uint64_t cid, bool is_ascending_order)
{
  UNUSED(tid);
  UNUSED(cid);
  UNUSED(is_ascending_order);
  int ret = OB_SUCCESS;
  ObSortColumn sort_column;
  sort_column.table_id_ = tid;
  sort_column.column_id_ = cid;
  sort_column.is_ascending_ = is_ascending_order;
  if (OB_SUCCESS == ret  && OB_SUCCESS != (ret = sort_columns_.push_back(sort_column)))
  {
    TBSYS_LOG(WARN, "failed to push back to array, err=%d", ret);
  }
  return ret;
}
void ObSemiJoin::set_mem_size_limit(const int64_t limit)
{
  //TBSYS_LOG(INFO, "sort mem limit=%ld", limit);
  mem_size_limit_ = limit;
  UNUSED(limit);
}
int ObSemiJoin::set_run_filename(const common::ObString &filename)
{
  UNUSED(filename);
  //TBSYS_LOG(INFO, "sort run file=%.*s", filename.length(), filename.ptr());
  return merge_sort_.set_run_filename(filename);
  return 0;
}
int ObSemiJoin::store_row_in_merge_sort(const common::ObRow *&input_row, bool &need_merge)
{
  UNUSED(input_row);
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = in_mem_sort_.set_sort_columns(sort_columns_)))
  {
    TBSYS_LOG(WARN, "fail to set sort columns for in_mem_sort. ret=%d", ret);
  }
  else
  {
    merge_sort_.set_sort_columns(sort_columns_); // pointer assign, return void
    if (OB_SUCCESS != (ret = in_mem_sort_.add_row(*input_row)))
    {
      TBSYS_LOG(WARN, "failed to add row, err=%d", ret);
    }
    else if (need_dump())
    {
      if (OB_SUCCESS != (ret = merge_sort_.dump_run(in_mem_sort_)))
      {
        TBSYS_LOG(WARN, "failed to dump, err=%d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "need merge sort");
        in_mem_sort_.reset();
        need_merge = true;
        sort_reader_ = &merge_sort_;
      }
    }
    if (OB_ITER_END == ret)
    {
      ret = OB_SUCCESS;
    }
    if (OB_SUCCESS == ret)
    {
      if (need_merge && 0 < in_mem_sort_.get_row_count())
      {
        merge_sort_.set_final_run(in_mem_sort_);
        if (OB_SUCCESS != (ret = merge_sort_.build_merge_heap()))
        {
          TBSYS_LOG(WARN, "failed to build heap, err=%d", ret);
        }
      }
    }
  }
  return ret;
}
//add:e
int ObSemiJoin::get_next_row(const ObRow *&row)
{
  OB_ASSERT(get_next_row_func_);
  return (this->*(this->ObSemiJoin::get_next_row_func_))(row);
}
int ObSemiJoin::open()
{
  int ret = OB_SUCCESS;
  switch(join_type_)
  {
  case INNER_JOIN:
    ret=semi_join_open();
    break;
  case LEFT_OUTER_JOIN:
    ret=semi_join_open();
    break;
  case RIGHT_OUTER_JOIN:
    //暂时关闭right semi join接口，待修改
    //ret=semi_join_open_right();
    ret = OB_ERR_FUNC_DEV;
    break;
  case LEFT_ANTI_SEMI_JOIN:
    //暂时关闭left anti semi join接口，待修改
    ret = OB_ERR_FUNC_DEV;
    break;
  case RIGHT_ANTI_SEMI_JOIN:
    //暂时关闭right anti semi join接口，待修改
    ret = OB_ERR_FUNC_DEV;
    break;
  default:
    ret = OB_ERR_UNEXPECTED;
    break;
  }
  return ret;
}
int ObSemiJoin::semi_join_open_right()
{
  //变量声明与定义
  int ret=OB_SUCCESS;
  const ObRowDesc *left_row_desc = NULL;
  const ObRowDesc *right_row_desc = NULL;
  char *store_buf = NULL;
  char *right_store_buf = NULL;
  int index_param_count = 0;
  int64_t equal_join_conds_count = equal_join_conds_.count();
  ObArray<int64_t> left_join_tid,left_join_cid,right_join_tid,right_join_cid;
  ObSqlExpression *table_filter_expr_ = ObSqlExpression::alloc();
  ObSqlExpression *src_table_filter_expr_ = ObSqlExpression::alloc();
  ret = (0 == equal_join_conds_count)?OB_ERR_HAS_NO_EQUI_COND:OB_SUCCESS;
  if(0 >= equal_join_conds_count)
  {
    ret = OB_ERR_HAS_NO_EQUI_COND;
  }
  else if(get_tid_cid_for_cons_filter(left_join_tid, left_join_cid,right_join_tid,right_join_cid,1))
  {
    TBSYS_LOG(WARN, "get table id and table column failed, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = right_op_->open()))
  {
    TBSYS_LOG(WARN, "failed to open child ops, err=%d", ret);
  }
  else
  {
    if(0 >= equal_join_conds_count)
    {
      ret = OB_ERR_HAS_NO_EQUI_COND;
    }
    else
    {
      if (OB_SUCCESS != (ret = right_op_->get_row_desc(right_row_desc)))
      {
        TBSYS_LOG(WARN, "failed to get child row desc, err=%d", ret);
      }
      else
      {
        if(is_can_use_semi_join_)
        {
          if(OB_SUCCESS != (ret = cons_filter_for_left_table(table_filter_expr_,
                                                             index_param_count,
                                                             left_join_tid,
                                                             left_join_cid,
                                                             right_join_tid,
                                                             right_join_cid)))
          {
            TBSYS_LOG(WARN, "failed to construct filter for right table, err=%d", ret);
          }
          else
          {
            if(OB_SUCCESS != (ret = add_filter_to_rpc_scan(index_param_count,src_table_filter_expr_,table_filter_expr_,0)))
            {
              TBSYS_LOG(WARN, "failed to add filter to right table, err=%d", ret);
            }
          }
        }
        else
        {
          const ObRow *row_temp = NULL;
          while(OB_SUCCESS == right_op_->get_next_row(row_temp))
          {
            const ObRowStore::StoredRow *stored_row = NULL;
            right_cache_for_store_.add_row(*row_temp, stored_row);
            stored_row = NULL;
          }
        }
      }
    }
  }
  if(OB_SUCCESS != ret)
  {
    if(OB_ERR_INDEX_NUM_IS_ZERO == ret)
    {
      ret = OB_SUCCESS;
      ObSort *ob_sort = NULL;
      ob_sort = dynamic_cast<ObSort *>(get_child(0));
      if(OB_SUCCESS != (ret = ob_sort->open_without_sort()))
      {
        TBSYS_LOG(WARN, "failed to open right, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = ob_sort->get_row_desc(left_row_desc)))
      {
        TBSYS_LOG(WARN, "failed to get child row desc, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = cons_row_desc(*left_row_desc, *right_row_desc)))
      {
        TBSYS_LOG(WARN, "failed to cons row desc, err=%d", ret);
      }
    }
    else
    {
      TBSYS_LOG(WARN, "failed to pass filter , err=%d", ret);
    }
  }
  else
  {
    if(is_use_semi_join_)
    {
      if(OB_SUCCESS != (ret = left_op_->open()))
      {
        TBSYS_LOG(WARN, "failed to open right, err=%d", ret);
      }
    }
    else
    {
      //不允许使用semi join
      ret = OB_ERR_CAN_NOT_USE_SEMI_JOIN;
    }
    if(OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = left_op_->get_row_desc(left_row_desc)))
      {
        TBSYS_LOG(WARN, "failed to get child row desc, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = cons_row_desc(*left_row_desc, *right_row_desc)))
      {
        TBSYS_LOG(WARN, "failed to cons row desc, err=%d", ret);
      }
      if(ret!=OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "out of time, err=%d", ret);
      }
      else
      {
        if (NULL == (store_buf = static_cast<char*>(ob_malloc(MAX_SINGLE_ROW_SIZE, ObModIds::OB_SQL_SEMI_JOIN))))
        {
          TBSYS_LOG(ERROR, "no memory");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (NULL == (right_store_buf = static_cast<char*>(ob_malloc(MAX_SINGLE_ROW_SIZE, ObModIds::OB_SQL_SEMI_JOIN))))
        {
          TBSYS_LOG(ERROR, "no memory");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          OB_ASSERT(left_row_desc);
          OB_ASSERT(right_row_desc);
          curr_row_.set_row_desc(row_desc_);
          curr_cached_right_row_.set_row_desc(*right_row_desc);
          last_left_row_ = NULL;
          last_right_row_ = NULL;
          right_cache_is_valid_ = false;
          is_right_iter_end_ = false;
          if (equal_join_conds_.count() <= 0)
          {
            is_unequal_join_ = true;
          }
          else
          {
            is_unequal_join_ = false;
          }
          cached_right_row_.set_row_desc(*right_row_desc);
          curr_cached_left_row_.set_row_desc(*left_row_desc);
          last_left_row_has_printed_ = false;
          last_right_row_has_printed_ = false;
          left_cache_is_valid_ = false;
          is_left_iter_end_ = false;
          last_join_right_row_store_.assign_buffer(right_store_buf,MAX_SINGLE_ROW_SIZE);
          last_join_left_row_store_.assign_buffer(store_buf, MAX_SINGLE_ROW_SIZE);
          if(OB_SUCCESS != (ret = process_sub_query()))
          {
            TBSYS_LOG(ERROR, "mergejoin::process sub query error");
          }
        }
      }
    }
  }
  return ret;
}

int ObSemiJoin::semi_join_open()
{
  //变量声明与定义
  int ret=OB_SUCCESS;
  const ObRowDesc *right_row_desc = NULL;
  char *store_buf = NULL;
  char *right_store_buf = NULL;
  int64_t equal_join_conds_count = equal_join_conds_.count();
  ObArray<int64_t> left_join_tid,left_join_cid,right_join_tid,right_join_cid;
  if(ret == OB_SUCCESS)
  {
    ret = (0 == equal_join_conds_count)?OB_ERR_HAS_NO_EQUI_COND:OB_SUCCESS;
  }
  if(ret != OB_SUCCESS)
  {
    //异常未处理，但不影响功能正常使用
  }
  else if(0 >= equal_join_conds_count)
  {
    ret = OB_ERR_HAS_NO_EQUI_COND;
  }
  else if(get_tid_cid_for_cons_filter(left_join_tid, left_join_cid,right_join_tid,right_join_cid,0))
  {
    TBSYS_LOG(WARN, "get table id and table column failed, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = left_op_->open()))
  {
    TBSYS_LOG(WARN, "failed to open child ops, err=%d", ret);
  }
  else
  {
    if(0 >= equal_join_conds_count)
    {
      ret = OB_ERR_HAS_NO_EQUI_COND;
    }
    else
    {
      if (OB_SUCCESS != (ret = left_op_->get_row_desc(left_row_desc_)))
      {
        TBSYS_LOG(WARN, "failed to get child row desc, err=%d", ret);
      }
      else
      {
        //新流程，突破两M包的限制：b
        const ObRow *row_temp = NULL;
        //bool need_merge = false;
        const ObRowStore::StoredRow *stored_row = NULL;
        //int go = 0;
        while(OB_SUCCESS == (ret = left_op_->get_next_row(row_temp)))
        {
          //可能会出现数据量太大导致内存膨胀的情况
          stored_row = NULL;
          left_cache_for_store_.add_row(*row_temp, stored_row);
          //go++;
          stored_row = NULL;
        }
        if(ret == OB_ITER_END)
        {
          ret = OB_SUCCESS;
        }
        if(ret == OB_SUCCESS && is_can_use_semi_join_)
        {
          ObTableRpcScan * main_query=NULL;
          ObSingleChildPhyOperator *sort_query=NULL;
          sort_query = dynamic_cast<ObSingleChildPhyOperator *>(get_child(1));
          if(sort_query == NULL)
          {
            TBSYS_LOG(WARN,"get sort op failed!");
          }
          else
          {
            //目前只考虑sort操作下挂的是table rpc scan的情况，如果是memory scan
            //或者是multi bind等操作符就不会使用semi join
            main_query = dynamic_cast<ObTableRpcScan *>(sort_query->get_child(0));
            if(main_query == NULL)
            {
              is_can_use_semi_join_ = false;
              TBSYS_LOG(WARN,"[semi join]get ObTableRpcScan op failed!can not use semi join,this is just a warning! right table is memory table or sub query!");
            }
            else
            {
              main_query->set_is_right_table (true);
              //重置select_get_filter
              main_query->reset_select_get_filter ();
              //设置是否为右表
              main_query->get_rpc_scan().set_is_right_table (true);
              //设置是否使用semi join
              if(is_can_use_semi_join_)
                main_query->get_rpc_scan ().set_is_use_semi_join (true);
              //设置左表缓存数据
              main_query->get_rpc_scan ().set_semi_join_left_table_row (&left_cache_for_store_);
              //设置左表row describe
              main_query->get_rpc_scan ().set_semi_join_left_row_desc (left_row_desc_);
              //设置左表table id 以及等值连接列的column id
              //mod by dragon [Bugfix 1224] 2016-8-25 09:39:19
              /*---new:增加对表别名的处理,如果能够使用二级索引并且不是没有设置别名才可以真正使用索引查询---*/
              //mod by dragon [Bugfix 1224] 2016-8-30 11:54:03
              /* 为什么有这个区别？跟二级索引生成回表还是不回表的查询计划中的行描述有关 */
              if(!get_aliasT ())
              {
                if(is_use_second_index())
                  main_query->get_rpc_scan ().set_left_table_id_column_id (
                        left_join_tid.at (id_),
                        left_join_cid.at (id_),
                        right_join_cid.at (id_),
                        second_index_table_id_);
                else
                  main_query->get_rpc_scan ().set_left_table_id_column_id (
                        left_join_tid.at (id_),
                        left_join_cid.at (id_),
                        right_join_cid.at (id_),
                        right_join_tid.at (id_));
              }
              else
              {
                if(is_use_second_index_without_storing()) //回表
                {
                  main_query->get_rpc_scan ().set_left_table_id_column_id (
                        left_join_tid.at (id_),
                        left_join_cid.at (id_),
                        right_join_cid.at (id_),
                        second_index_table_id_);
                }
                else //不回表,或不用索引
                {
                  main_query->get_rpc_scan ().set_left_table_id_column_id (
                        left_join_tid.at (id_),
                        left_join_cid.at (id_),
                        right_join_cid.at (id_),
                        right_join_tid.at (id_));
                }
              }
              /*---old2---
              if(is_use_second_index() && !get_aliasT ())
                main_query->get_rpc_scan ().set_left_table_id_column_id (left_join_tid.at (id_),left_join_cid.at (id_),right_join_cid.at (id_),second_index_table_id_);
              else
                main_query->get_rpc_scan ().set_left_table_id_column_id (left_join_tid.at (id_),left_join_cid.at (id_),right_join_cid.at (id_),right_join_tid.at (id_));
              ---old2---*/
              //mod 2016-8-30 11:54:14e
              /*---old---
              if(is_use_second_index())
                main_query->get_rpc_scan ().set_left_table_id_column_id (left_join_tid.at (id_),left_join_cid.at (id_),right_join_cid.at (id_),second_index_table_id_);
              else
                main_query->get_rpc_scan ().set_left_table_id_column_id (left_join_tid.at (id_),left_join_cid.at (id_),right_join_cid.at (id_),right_join_tid.at (id_));
              ---old---*/
              //mod 2016-8-25e

              if(use_in_expr_)
              {
                main_query->get_rpc_scan ().set_use_in (true);
              }
              else if(use_btw_expr_)
              {
                main_query->get_rpc_scan ().set_use_btw (true);
              }
            }
          }
        }
      }
    }
  }
  if(ret == OB_SUCCESS)
  {
    //TBSYS_LOG(ERROR,"wanglei ::right_table_filter_:%s ",to_cstring(*right_table_filter_));
    if(right_table_filter_ != NULL && right_table_filter_->count () > 0 && is_can_use_semi_join_)
      ret = right_table_filter_->set_child(0, *right_op_);
    if(ret == OB_SUCCESS)
    {
      if(right_table_filter_ != NULL && right_table_filter_->count () > 0 && is_can_use_semi_join_)
        right_op_ = right_table_filter_;
      //TBSYS_LOG(ERROR, "right op is %s", to_cstring(*right_op_));
      if(OB_SUCCESS != (ret = right_op_->open()))
      {
        TBSYS_LOG(WARN, "failed to open right, err=%d", ret);
      }else  if (OB_SUCCESS != (ret = right_op_->get_row_desc(right_row_desc)))
      {
        TBSYS_LOG(WARN, "failed to get child row desc, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = cons_row_desc(*left_row_desc_, *right_row_desc)))
      {
        TBSYS_LOG(WARN, "failed to cons row desc, err=%d", ret);
      }
      if(ret!=OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "out of time, err=%d", ret);
      }
      else
      {
        if (NULL == (store_buf = static_cast<char*>(ob_malloc(MAX_SINGLE_ROW_SIZE, ObModIds::OB_SQL_SEMI_JOIN))))
        {
          TBSYS_LOG(ERROR, "no memory");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (NULL == (right_store_buf = static_cast<char*>(ob_malloc(MAX_SINGLE_ROW_SIZE, ObModIds::OB_SQL_SEMI_JOIN))))
        {
          TBSYS_LOG(ERROR, "no memory");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          OB_ASSERT(left_row_desc_);
          OB_ASSERT(right_row_desc);
          curr_row_.set_row_desc(row_desc_);
          curr_cached_right_row_.set_row_desc(*right_row_desc);
          last_left_row_ = NULL;
          last_right_row_ = NULL;
          right_cache_is_valid_ = false;
          is_right_iter_end_ = false;
          if (equal_join_conds_.count() <= 0)
          {
            is_unequal_join_ = true;
          }
          else
          {
            is_unequal_join_ = false;
          }
          cached_left_row_.set_row_desc(*left_row_desc_);
          curr_cached_left_row_.set_row_desc(*left_row_desc_);
          last_left_row_has_printed_ = false;
          last_right_row_has_printed_ = false;
          left_cache_is_valid_ = false;
          is_left_iter_end_ = false;
          last_join_right_row_store_.assign_buffer(right_store_buf,MAX_SINGLE_ROW_SIZE);
          last_join_left_row_store_.assign_buffer(store_buf, MAX_SINGLE_ROW_SIZE);
          if(OB_SUCCESS != (ret = process_sub_query()))
          {
            TBSYS_LOG(ERROR, "mergejoin::process sub query error");
          }
        }
      }
    }
  }
  if(ret == OB_SUCCESS && is_can_use_semi_join_)
  {
    left_cache_for_store_.reset_iterator ();
  }
  return ret;
}
//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140610:b
int ObSemiJoin::process_sub_query()
{
  int ret = OB_SUCCESS;
  const common::ObRowDesc *row_desc;
  const ObRow *row = NULL;
  int64_t cond_num = other_join_conds_.count();
  for(int i = 0;i< cond_num;i++)
  {
    ObSqlExpression &expr = other_join_conds_.at(i);
    int64_t sub_query_num = expr.get_sub_query_num();
    if(sub_query_num<=0)
    {
      continue;
    }
    else
    {
      for(int j = 0;j<sub_query_num;j++)
      {
        int32_t sub_query_index =expr.get_sub_query_idx(j);
        ObPhyOperator * sub_operator = NULL;
        if(NULL == (sub_operator = my_phy_plan_->get_phy_query(sub_query_index)))
        {
          ret = OB_INVALID_INDEX;
          TBSYS_LOG(ERROR,"get child of sub query operator faild");
        }
        else if(OB_SUCCESS != (ret = sub_operator->open()))
        {
          TBSYS_LOG(WARN, "fail to open sub select query:ret[%d]", ret);
        }
        else if (OB_SUCCESS != (ret = sub_operator->get_row_desc(row_desc)))
        {
          TBSYS_LOG(WARN, "fail to get row_desc:ret[%d]", ret);
        }
        else
        {
          bool direct_bind = false;
          sub_result_.clear();

          int64_t rowkey_count = 0;
          bool special_sub_query = false;
          expr.get_sub_query_column_num(j+1,rowkey_count,special_sub_query);

          uint64_t table_id = 0;
          uint64_t column_id = 0;

          //special sub_query (such as "where (select ...)")
          //sub_query as a independent expression
          //just retrun true/false
          //do not need process real result,just return bool value
          if(special_sub_query)
          {
            bool sub_query_result = false;
            if(OB_ITER_END == (ret = sub_operator->get_next_row(row)))
            {
              //resultset is null
              ret = OB_SUCCESS;
            }
            else if(OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "fail to get next row from rpc scan");
            }
            else
            {
              //there must be only one column
              row_desc->get_tid_cid(0,table_id,column_id);
              ObObj *temp;
              const_cast<ObRow *>(row)->get_cell(table_id,column_id,temp);
              sub_query_result = temp->is_true();
              if(OB_ITER_END != (ret = sub_operator->get_next_row(row)))
              {
                //sub_query resultset more than one row,can not convert to bool value
                //return error
                sub_operator->close();
                TBSYS_LOG(WARN, "Subquery returns more than 1 row");
                ret = OB_ERR_SUB_QUERY_RESULTSET_EXCESSIVE;
              }
              else
              {
                ret = OB_SUCCESS;
              }
            }
            //sub_query resultset sucessfully convert to bool value,
            //then use bool value replace sub_query mark
            if(OB_SUCCESS == ret)
            {
              //fill bool value to postfix expression
              expr.delete_sub_query_info(j+1);

              ObObj * result = (ObObj*)arena_.alloc(sizeof(ObObj));
              result->set_bool(sub_query_result);
              expr.add_expr_in_obj(*result);
              expr.complete_sub_query_info();
            }
          }
          else//sub_query as a part of IN or NOT_IN (such as "id in (select id from ...)")
          {
            //try push all sub_query reault to  sub_result_ (type:array)
            //if all sub_query reault size not bigger than  BIG_RESULTSET_THRESHOLD,use direct_bind stragety
            //or use hashmap and bloomfilter check
            while (OB_SUCCESS == ret && sub_result_.count()<=BIG_RESULTSET_THRESHOLD)
            {
              ret = sub_operator->get_next_row(row);
              if (OB_ITER_END == ret)
              {
                ret = OB_SUCCESS;
                direct_bind = true;
                break;
              }
              else if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "fail to get next row from rpc scan");
              }
              else
              {
                ObObj value[rowkey_count];
                for(int i =0;i<rowkey_count;i++)
                {
                  row_desc->get_tid_cid(i,table_id,column_id);
                  ObObj *temp;
                  const_cast<ObRow *>(row)->get_cell(table_id,column_id,temp);
                  value[i] = *temp;
                }

                ObRowkey columns;
                columns.assign(value,rowkey_count);
                ObRowkey columns2;
                if(OB_SUCCESS != (ret = columns.deep_copy(columns2,arena_)))
                {
                  TBSYS_LOG(WARN, "fail to deep copy column");
                  break;
                }
                sub_result_.push_back(columns2);
              }
            }
            //small resultset,use direct bind stragety
            //use specific value replace sub_query mark
            if(direct_bind)
            {
              //direct bind sub query result_set to main query's filter
              expr.delete_sub_query_info(j+1);
              //fill structure,indicate the columns num of one row
              ExprItem dem1;
              dem1.type_ = T_OP_ROW;
              dem1.data_type_ = ObMinType;
              dem1.value_.int_ = rowkey_count;
              //fill structure,indicate the total row num of sub query result
              ExprItem dem2;
              dem2.type_ = T_OP_ROW;
              dem2.data_type_ = ObMinType;
              dem2.value_.int_ = sub_result_.count();

              while (sub_result_.count()>0)
              {
                ObRowkey columns;
                sub_result_.pop_back(columns);
                ObObj * obj_ptr = const_cast <ObObj *>(columns.get_obj_ptr());
                for(int i = 0;i<rowkey_count;i++)
                {
                  expr.add_expr_in_obj(obj_ptr[i]);
                }
                expr.add_expr_item(dem1);
              }

              expr.add_expr_item(dem2);
              expr.complete_sub_query_info();
            }
            else if(hashmap_num_ >= MAX_SUB_QUERY_NUM)//restrict num of sub_query which use hashmap
            {
              TBSYS_LOG(WARN, "too many sub_query");
              ret = OB_ERR_SUB_QUERY_OVERFLOW;
            }
            else
            {
              //build hashmap and bloomfilter
              sub_query_map_[hashmap_num_].create(HASH_BUCKET_NUM);
              //add the row_store value to hashmap
              while(sub_result_.count()>0)
              {
                ObRowkey columns;
                sub_result_.pop_back(columns);
                sub_query_map_[hashmap_num_].set(columns,columns);
              }
              //add the left data to hashmap and bloomfilter
              while (OB_SUCCESS == ret)
              {
                ret = sub_operator->get_next_row(row);
                if (OB_ITER_END == ret)
                {
                  ret = OB_SUCCESS;
                  break;
                }
                else if (OB_SUCCESS != ret)
                {
                  TBSYS_LOG(WARN, "fail to get next row from sub query");
                }
                else
                {
                  TBSYS_LOG(DEBUG, "load data from sub query, row=%s", to_cstring(*row));
                  ObObj value[rowkey_count];
                  ObRow *curr_row = const_cast<ObRow *>(row);
                  for(int i =0;i<rowkey_count;i++)
                  {
                    row_desc->get_tid_cid(i,table_id,column_id);
                    ObObj *temp;
                    curr_row->get_cell(table_id,column_id,temp);
                    value[i] = *temp;
                  }
                  ObRowkey columns;
                  columns.assign(value,rowkey_count);
                  ObRowkey columns2;
                  if(OB_SUCCESS != (ret = columns.deep_copy(columns2,arena_)))
                  {
                    TBSYS_LOG(WARN, "fail to deep copy column");
                    break;
                  }
                  sub_query_map_[hashmap_num_].set(columns2,columns2);
                }
              }
              hashmap_num_ ++;
            }
          }
        }
        sub_operator->close();
      }
    }
    expr.update_sub_query_num();
  }
  return ret;
}
//add 20140610:e

int ObSemiJoin::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 >= row_desc_.get_column_num()))
  {
    TBSYS_LOG(ERROR, "not init");
    ret = OB_NOT_INIT;
  }
  else
  {
    row_desc = &row_desc_;
  }
  return ret;
}

int ObSemiJoin::compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const
{
  int ret = OB_SUCCESS;
  cmp = 0;
  const ObObj *res1 = NULL;
  const ObObj *res2 = NULL;
  ObExprObj obj1;
  ObExprObj obj2;
  for (int64_t i = 0; i < equal_join_conds_.count(); ++i)
  {
    const ObSqlExpression &expr = equal_join_conds_.at(i);
    ExprItem::SqlCellInfo c1;
    ExprItem::SqlCellInfo c2;
    if (expr.is_equijoin_cond(c1, c2))
    {
      if (OB_SUCCESS != (ret = r1.get_cell(c1.tid, c1.cid, res1)))
      {
        TBSYS_LOG(ERROR, "failed to get cell, err=%d tid=%lu cid=%lu", ret, c1.tid, c1.cid);
        break;
      }
      else if (OB_SUCCESS != (ret = r2.get_cell(c2.tid, c2.cid, res2)))
      {
        TBSYS_LOG(ERROR, "failed to get cell, err=%d tid=%lu cid=%lu", ret, c2.tid, c2.cid);
        break;
      }
      else
      {
        obj1.assign(*res1);
        obj2.assign(*res2);
        if (OB_SUCCESS != obj1.compare(obj2, cmp))
        {
          if (obj1.is_null())
          {
            // NULL vs obj2
            cmp = -10;
          }
          else
          {
            // obj1 cmp NULL
            cmp = 10;
          }

          //add dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140605:b
          break;
          //add 20140605:e
        }
        else if (0 != cmp)
        {
          break;
        }
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "invalid equijoin condition");
      ret = OB_ERR_UNEXPECTED;
      break;
    }
  }
  return ret;
}

int ObSemiJoin::left_row_compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const
{
  int ret = OB_SUCCESS;
  cmp = 0;
  const ObObj *res1 = NULL;
  const ObObj *res2 = NULL;
  ObExprObj obj1;
  ObExprObj obj2;
  for (int64_t i = 0; i < equal_join_conds_.count(); ++i)
  {
    const ObSqlExpression &expr = equal_join_conds_.at(i);
    ExprItem::SqlCellInfo c1;
    ExprItem::SqlCellInfo c2;
    if (expr.is_equijoin_cond(c1, c2))
    {
      if (OB_SUCCESS != (ret = r1.get_cell(c1.tid, c1.cid, res1)))
      {
        TBSYS_LOG(ERROR, "failed to get cell, err=%d tid=%lu cid=%lu", ret, c1.tid, c1.cid);
        break;
      }
      else if (OB_SUCCESS != (ret = r2.get_cell(c1.tid, c1.cid, res2)))
      {
        TBSYS_LOG(ERROR, "failed to get cell, err=%d tid=%lu cid=%lu", ret, c1.tid, c1.cid);
        break;
      }
      else
      {
        obj1.assign(*res1);
        obj2.assign(*res2);
        if (OB_SUCCESS != obj1.compare(obj2, cmp))
        {
          cmp = -10;            // @todo NULL
          //add dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140605:b
          break;
          //add 20140605:e
        }
        else if (0 != cmp)
        {
          break;
        }
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "invalid equijoin condition");
      ret = OB_ERR_UNEXPECTED;
      break;
    }
  }
  return ret;
}

//add dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140611:b
int ObSemiJoin::right_row_compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const
{
  int ret = OB_SUCCESS;
  cmp = 0;
  const ObObj *res1 = NULL;
  const ObObj *res2 = NULL;
  ObExprObj obj1;
  ObExprObj obj2;
  for (int64_t i = 0; i < equal_join_conds_.count(); ++i)
  {
    const ObSqlExpression &expr = equal_join_conds_.at(i);
    ExprItem::SqlCellInfo c1;
    ExprItem::SqlCellInfo c2;
    if (expr.is_equijoin_cond(c1, c2))
    {
      if (OB_SUCCESS != (ret = r1.get_cell(c2.tid, c2.cid, res1)))
      {
        TBSYS_LOG(ERROR, "failed to get cell, err=%d tid=%lu cid=%lu", ret, c1.tid, c1.cid);
        break;
      }
      else if (OB_SUCCESS != (ret = r2.get_cell(c2.tid, c2.cid, res2)))
      {
        TBSYS_LOG(ERROR, "failed to get cell, err=%d tid=%lu cid=%lu", ret, c1.tid, c1.cid);
        break;
      }
      else
      {
        obj1.assign(*res1);
        obj2.assign(*res2);
        if (OB_SUCCESS != obj1.compare(obj2, cmp))
        {
          cmp = -10;            // @todo NULL
          //add dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140605:b
          break;
          //add 20140605:e
        }
        else if (0 != cmp)
        {
          break;
        }
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "invalid equijoin condition");
      ret = OB_ERR_UNEXPECTED;
      break;
    }
  }
  return ret;
}
//add 20140611:e


int ObSemiJoin::curr_row_is_qualified(bool &is_qualified)
{
  int ret = OB_SUCCESS;
  is_qualified = true;
  const ObObj *res = NULL;
  int hash_mape_index = 0;//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140610
  //TBSYS_LOG(ERROR, "wl other_join_conds_ is=[%ld]",other_join_conds_.count());
  for (int64_t i = 0; i < other_join_conds_.count(); ++i)
  {
    ObSqlExpression &expr = other_join_conds_.at(i);
    //TBSYS_LOG(ERROR, "wl ObSqlExpression %ld ,is=[%s]",i,to_cstring(expr));
    //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140610:b
    bool use_hash_map = false;
    common::hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* p = NULL;
    if(hashmap_num_>0 && expr.get_sub_query_num()>0)
    {
      p =&(sub_query_map_[hash_mape_index]);
      use_hash_map = true;
      hash_mape_index = hash_mape_index + (int)expr.get_sub_query_num();
    }
    //add 20140610:e

    //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
    ////mod peiouya [IN_TYPEBUG_FIX] 20151225
    //////mod tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140610:b
    //////if (OB_SUCCESS != (ret = expr.calc(curr_row_, res)))
    ////if (OB_SUCCESS != (ret = expr.calc(curr_row_, res, p, use_hash_map)))
    //////mod 20140610:e
    //if (OB_SUCCESS != (ret = expr.calc(curr_row_, res, p, NULL, use_hash_map)))
    if (OB_SUCCESS != (ret = expr.calc(curr_row_, res, p,false, NULL, use_hash_map)))
      ////add 20151225:e
      //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
    {
      TBSYS_LOG(WARN, "failed to calc expr, err=%d", ret);
    }
    else if (!res->is_true())
    {
      is_qualified = false;
      break;
    }
  }
  return ret;
}

int ObSemiJoin::cons_row_desc(const ObRowDesc &rd1, const ObRowDesc &rd2)
{
  int ret = OB_SUCCESS;
  uint64_t tid = OB_INVALID_ID;
  uint64_t cid = OB_INVALID_ID;
  for (int64_t i = 0; i < rd1.get_column_num(); ++i)
  {
    if (OB_SUCCESS != (ret = rd1.get_tid_cid(i, tid, cid)))
    {
      TBSYS_LOG(ERROR, "unexpected branch");
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    else if (OB_SUCCESS != (ret = row_desc_.add_column_desc(tid, cid)))
    {
      TBSYS_LOG(WARN, "failed to add column desc, err=%d", ret);
      break;
    }
  }
  for (int64_t i = 0; OB_SUCCESS == ret && i < rd2.get_column_num(); ++i)
  {
    if (OB_SUCCESS != (ret = rd2.get_tid_cid(i, tid, cid)))
    {
      TBSYS_LOG(ERROR, "unexpected branch");
    }
    else if (OB_SUCCESS != (ret = row_desc_.add_column_desc(tid, cid)))
    {
      TBSYS_LOG(WARN, "failed to add column desc, err=%d", ret);
    }
  }
  return ret;
}

int ObSemiJoin::join_rows(const ObRow& r1, const ObRow& r2)
{
  int ret = OB_SUCCESS;
  const ObObj *cell = NULL;
  uint64_t tid = OB_INVALID_ID;
  uint64_t cid = OB_INVALID_ID;
  int64_t i = 0;
  for (; i < r1.get_column_num(); ++i)
  {
    if (OB_SUCCESS != (ret = r1.raw_get_cell(i, cell, tid, cid)))
    {
      TBSYS_LOG(ERROR, "unexpected branch, err=%d", ret);
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    else if (OB_SUCCESS != (ret = curr_row_.raw_set_cell(i, *cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell, err=%d i=%ld", ret, i);
      break;
    }
  } // end for
  for (int64_t j = 0; OB_SUCCESS == ret && j < r2.get_column_num(); ++j)
  {
    if (OB_SUCCESS != (ret = r2.raw_get_cell(j, cell, tid, cid)))
    {
      TBSYS_LOG(ERROR, "unexpected branch, err=%d", ret);
      ret = OB_ERR_UNEXPECTED;
    }
    else if (OB_SUCCESS != (ret = curr_row_.raw_set_cell(i+j, *cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell, err=%d j=%ld", ret, j);
    }
  } // end for
  return ret;
}
int ObSemiJoin::left_join_rows(const ObRow& r1)
{
  int ret = OB_SUCCESS;
  const ObObj *cell = NULL;
  uint64_t tid = OB_INVALID_ID;
  uint64_t cid = OB_INVALID_ID;
  int64_t i = 0;
  for (; i < r1.get_column_num(); ++i)
  {
    if (OB_SUCCESS != (ret = r1.raw_get_cell(i, cell, tid, cid)))
    {
      TBSYS_LOG(ERROR, "unexpected branch, err=%d", ret);
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    else if (OB_SUCCESS != (ret = curr_row_.raw_set_cell(i, *cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell, err=%d i=%ld", ret, i);
      break;
    }
  } // end for
  int64_t right_row_column_num = row_desc_.get_column_num() - r1.get_column_num();
  ObObj null_cell;
  null_cell.set_null();
  for (int64_t j = 0; OB_SUCCESS == ret && j < right_row_column_num; ++j)
  {
    if (OB_SUCCESS != (ret = curr_row_.raw_set_cell(i+j, null_cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell, err=%d j=%ld", ret, j);
    }
  } // end for
  return ret;
}
int ObSemiJoin::left_outer_ge_get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRow *left_row  = NULL;
  const ObRow *right_row = NULL;
  // fetch the next left row
  if (NULL != last_left_row_)
  {
    left_row = last_left_row_;
    last_left_row_ = NULL;
    //add dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
    last_left_row_has_printed_ = true;
    //add 20140610:e
  }
  else
  {
    ret = left_op_->get_next_row(left_row);
    //add dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
    last_left_row_has_printed_ = false;
    //add 20140610:e
  }

  while(OB_SUCCESS == ret)
  {
    if (right_cache_is_valid_)
    {
      OB_ASSERT(!right_cache_.is_empty());
      /*int cmp = 0;
      if (OB_SUCCESS != (ret = left_row_compare_equijoin_cond(*left_row, last_join_left_row_, cmp)))
      {
        TBSYS_LOG(WARN, "failed to compare, err=%d", ret);
        break;
      }
      if (0 == cmp)
      {*/
      // fetch the next right row from right_cache
      if (OB_SUCCESS != (ret = right_cache_.get_next_row(curr_cached_right_row_)))
      {
        if (OB_UNLIKELY(OB_ITER_END != ret))
        {
          TBSYS_LOG(WARN, "failed to get next row from right_cache, err=%d", ret);
        }
        else
        {
          right_cache_.reset_iterator(); // continue
          //del dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
          // fetch the next left row
          //            ret = left_op_->get_next_row(left_row);
          //del 20140610:e

          //add dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
          if (!last_left_row_has_printed_)
          {
            if (OB_SUCCESS != (ret = left_join_rows(*left_row)))
            {
              TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
            }
            else
            {
              // output
              row = &curr_row_;
              last_left_row_ = NULL;
              break;
            }
          }
          else
          {
            ret = left_op_->get_next_row(left_row);
            last_left_row_has_printed_ = false;
          }
          //add 20140610:e
        }
      }
      else
      {
        bool is_qualified = false;
        if (OB_SUCCESS != (ret = join_rows(*left_row, curr_cached_right_row_)))
        {
          TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
        {
          TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
        }
        else if (is_qualified)
        {
          // output
          row = &curr_row_;
          last_left_row_ = left_row;
          break;
        }
        else
        {
          // continue with the next cached right row
          OB_ASSERT(NULL == last_left_row_);
          OB_ASSERT(NULL != left_row);
        }
      }
      /*}
      else
      {
        // left_row > last_join_left_row_ on euqijoin conditions
        right_cache_is_valid_ = false;
        right_cache_.clear();
      }*/
    }
    else
    {
      // fetch the next right row
      if (OB_UNLIKELY(is_right_iter_end_))
      {
        // no more right rows, but there are left rows left
        OB_ASSERT(left_row);
        //del dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
        //        bool is_qualified = false;
        //del 20140610:e
        if (OB_SUCCESS != (ret = left_join_rows(*left_row)))
        {
          TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
        }
        //add dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
        else
        {
          // output
          row = &curr_row_;
          OB_ASSERT(NULL == last_left_row_);
          OB_ASSERT(NULL == last_right_row_);
          break;
        }
      }
      else if (NULL != last_right_row_)
      {
        right_row = last_right_row_;
        last_right_row_ = NULL;
      }
      else
      {
        ret = right_op_->get_next_row(right_row);
        if (OB_SUCCESS != ret)
        {
          if (OB_ITER_END == ret)
          {
            TBSYS_LOG(INFO, "end of right child op");
            is_right_iter_end_ = true;
            if (!right_cache_.is_empty())
            {
              // no more right rows and the right cache is not empty, we SHOULD look at the next left row
              right_cache_is_valid_ = true;
              OB_ASSERT(NULL == last_right_row_);
              OB_ASSERT(NULL == last_left_row_);
              if (!last_left_row_has_printed_)
              {
                if (OB_SUCCESS != (ret = left_join_rows(*left_row)))
                {
                  TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
                }
                else
                {
                  // output
                  row = &curr_row_;
                  break;
                }
              }
              else
              {
                ret = left_op_->get_next_row(left_row);
                last_left_row_has_printed_ = false;
              }
              //add 20140610:e
            }
            else
            {
              ret = OB_SUCCESS;
            }
            continue;
          }
          else
          {
            TBSYS_LOG(WARN, "failed to get next row from right child, err=%d", ret);
            break;
          }
        }
      }
      OB_ASSERT(left_row);
      OB_ASSERT(right_row);
      bool is_qualified = false;
      const ObRowStore::StoredRow *stored_row = NULL;
      if (OB_SUCCESS != (ret = right_cache_.add_row(*right_row, stored_row)))
      {
        TBSYS_LOG(WARN, "failed to store the row, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = join_rows(*left_row, *right_row)))
      {
        TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
      {
        TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
      }
      else if (is_qualified)
      {
        // output
        row = &curr_row_;
        last_left_row_ = left_row;
        OB_ASSERT(NULL == last_right_row_);
        break;
      }
      else
      {
        // continue with the next right row
        OB_ASSERT(NULL != left_row);
        OB_ASSERT(NULL == last_left_row_);
        OB_ASSERT(NULL == last_right_row_);
      }
    }
  } // end while
  return ret;
}

int ObSemiJoin::right_join_rows(const ObRow& r2)
{
  int ret = OB_SUCCESS;
  const ObObj *cell = NULL;
  uint64_t tid = OB_INVALID_ID;
  uint64_t cid = OB_INVALID_ID;
  int64_t left_row_column_num = row_desc_.get_column_num() - r2.get_column_num();
  ObObj null_cell;
  null_cell.set_null();
  for (int64_t i = 0; i < left_row_column_num; ++i)
  {
    if (OB_SUCCESS != (ret = curr_row_.raw_set_cell(i, null_cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell, err=%d i=%ld", ret, i);
      break;
    }
  } // end for
  for (int64_t j = 0; OB_SUCCESS == ret && j < r2.get_column_num(); ++j)
  {
    if (OB_SUCCESS != (ret = r2.raw_get_cell(j, cell, tid, cid)))
    {
      TBSYS_LOG(ERROR, "unexpected branch, err=%d", ret);
      ret = OB_ERR_UNEXPECTED;
    }
    else if (OB_SUCCESS != (ret = curr_row_.raw_set_cell(left_row_column_num+j, *cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell, err=%d j=%ld", ret, j);
    }
  } // end for
  return ret;
}
// LEFT_SEMI_JOIN
int ObSemiJoin::inner_semi_hash_get_next_row(const common::ObRow *&row)
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  return ret;
}
/*add by wanglei [semi join] 20151106*/
int ObSemiJoin::inner_semi_get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRow *left_row = NULL;
  const ObRow *right_row = NULL;
  // fetch the next left row
  if (NULL != last_left_row_)
  {
    left_row = last_left_row_;
    last_left_row_ = NULL;
  }
  else
  {
    // ret = sort_reader_->get_next_row(left_row);
    ret = left_cache_for_store_.get_next_row(cached_left_row_);
    left_row=&cached_left_row_;
  }

  while(OB_SUCCESS == ret)
  {
    if (right_cache_is_valid_)
    {
      OB_ASSERT(!right_cache_.is_empty());
      int cmp = 0;
      if (OB_SUCCESS != (ret = left_row_compare_equijoin_cond(*left_row, last_join_left_row_, cmp)))
      {
        TBSYS_LOG(WARN, "failed to compare, err=%d", ret);
        break;
      }
      if (0 == cmp)
      {
        // fetch the next right row from right_cache
        if (OB_SUCCESS != (ret = right_cache_.get_next_row(curr_cached_right_row_)))
        {
          if (OB_UNLIKELY(OB_ITER_END != ret))
          {
            TBSYS_LOG(WARN, "failed to get next row from right_cache, err=%d", ret);
          }
          else
          {
            right_cache_.reset_iterator(); // continue
            // fetch the next left row
            //ret = sort_reader_->get_next_row(left_row);
            ret = left_cache_for_store_.get_next_row(cached_left_row_);
            left_row=&cached_left_row_;
          }
        }
        else
        {
          bool is_qualified = false;
          if (OB_SUCCESS != (ret = join_rows(*left_row, curr_cached_right_row_)))
          {
            TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
          {
            TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
          }
          else if (is_qualified)
          {
            // output
            row = &curr_row_;
            last_left_row_ = left_row;
            break;
          }
          else
          {
            // continue with the next cached right row
            OB_ASSERT(NULL == last_left_row_);
            OB_ASSERT(NULL != left_row);
          }
        }
      }
      else
      {
        // left_row > last_join_left_row_ on euqijoin conditions
        right_cache_is_valid_ = false;
        right_cache_.clear();
      }
    }
    else
    {
      // fetch the next right row
      if (OB_UNLIKELY(is_right_iter_end_))
      {
        ret = OB_ITER_END;
        break;
      }
      else if (NULL != last_right_row_)
      {
        right_row = last_right_row_;
        last_right_row_ = NULL;
      }
      else
      {
        ret = right_op_->get_next_row(right_row);
        if (OB_SUCCESS != ret)
        {
          if (OB_ITER_END == ret)
          {
            TBSYS_LOG(DEBUG, "end of right child op");
            is_right_iter_end_ = true;
            if (!right_cache_.is_empty())
            {
              // no more right rows and the right cache is not empty, we SHOULD look at the next left row
              right_cache_is_valid_ = true;
              OB_ASSERT(NULL == last_right_row_);
              OB_ASSERT(NULL == last_left_row_);
              // ret = sort_reader_->get_next_row(left_row);
              ret = left_cache_for_store_.get_next_row(cached_left_row_);
              left_row=&cached_left_row_;
            }
            continue;
          }
          else
          {
            TBSYS_LOG(WARN, "failed to get next row from right child, err=%d", ret);
            break;
          }
        }
      }
      OB_ASSERT(left_row);
      OB_ASSERT(right_row);
      int cmp = 0;
      if (OB_SUCCESS != (ret = compare_equijoin_cond(*left_row, *right_row, cmp)))
      {
        TBSYS_LOG(WARN, "failed to compare, err=%d", ret);
        break;
      }
      if (0 == cmp)
      {
        if (right_cache_.is_empty())
        {
          // store the joined left row
          last_join_left_row_store_.assign_buffer(last_join_left_row_store_.ptr(),
                                                  MAX_SINGLE_ROW_SIZE);
          if (OB_SUCCESS != (ret = ObRowUtil::convert(*left_row,
                                                      last_join_left_row_store_,
                                                      last_join_left_row_)))
          {
            TBSYS_LOG(WARN, "failed to store left row, err=%d", ret);
            break;
          }
        }
        bool is_qualified = false;
        const ObRowStore::StoredRow *stored_row = NULL;
        if (OB_SUCCESS != (ret = right_cache_.add_row(*right_row, stored_row)))
        {
          TBSYS_LOG(WARN, "failed to store the row, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = join_rows(*left_row, *right_row)))
        {
          TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
        {
          TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
        }
        else if (is_qualified)
        {
          // output
          row = &curr_row_;
          last_left_row_ = left_row;
          OB_ASSERT(NULL == last_right_row_);
          break;
        }
        else
        {
          // continue with the next right row
          OB_ASSERT(NULL != left_row);
          OB_ASSERT(NULL == last_left_row_);
          OB_ASSERT(NULL == last_right_row_);
        }
      } // end 0 == cmp
      else if (cmp < 0)
      {
        // left_row < right_row on equijoin conditions
        if (!right_cache_.is_empty())
        {
          right_cache_is_valid_ = true;
        }
        last_right_row_ = right_row;
        OB_ASSERT(NULL == last_left_row_);
        //ret = sort_reader_->get_next_row(left_row);
        ret = left_cache_for_store_.get_next_row(cached_left_row_);
        left_row=&cached_left_row_;
      }
      else
      {
        // left_row > right_row on euqijoin conditions
        // continue with the next right row
        OB_ASSERT(NULL != left_row);
        OB_ASSERT(NULL == last_left_row_);
        OB_ASSERT(NULL == last_right_row_);
      }
    }
  } // end while
  return ret;
}
/*add by wanglei [semi join] 20151106*/
int ObSemiJoin::left_semi_get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRow *left_row  = NULL;
  const ObRow *right_row = NULL;
  if (NULL != last_left_row_)
  {
    left_row = last_left_row_;
    last_left_row_ = NULL;
    //add dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
    last_left_row_has_printed_ = true;
    //add 20140610:e
  }
  else
  {
    //ret = sort_reader_->get_next_row(left_row);
    ret = left_cache_for_store_.get_next_row(cached_left_row_);
    left_row=&cached_left_row_;
    //add dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
    last_left_row_has_printed_ = false;
    //add 20140610:e
  }

  while(OB_SUCCESS == ret)
  {
    if (right_cache_is_valid_)
    {
      OB_ASSERT(!right_cache_.is_empty());
      int cmp = 0;
      if (OB_SUCCESS != (ret = left_row_compare_equijoin_cond(*left_row, last_join_left_row_, cmp)))
      {
        TBSYS_LOG(WARN, "failed to compare, err=%d", ret);
        break;
      }
      if (0 == cmp)
      {
        // fetch the next right row from right_cache
        if (OB_SUCCESS != (ret = right_cache_.get_next_row(curr_cached_right_row_)))
        {
          if (OB_UNLIKELY(OB_ITER_END != ret))
          {
            TBSYS_LOG(WARN, "failed to get next row from right_cache, err=%d", ret);
          }
          else
          {
            right_cache_.reset_iterator(); // continue
            if (!last_left_row_has_printed_)
            {
              if (OB_SUCCESS != (ret = left_join_rows(*left_row)))
              {
                TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
              }
              else
              {
                // output
                row = &curr_row_;
                last_left_row_ = NULL;
                break;
              }
            }
            else
            {
              //ret = sort_reader_->get_next_row(left_row);
              ret = left_cache_for_store_.get_next_row(cached_left_row_);
              left_row=&cached_left_row_;
              last_left_row_has_printed_ = false;
            }
            //add 20140610:e
          }
        }
        else
        {
          bool is_qualified = false;
          if (OB_SUCCESS != (ret = join_rows(*left_row, curr_cached_right_row_)))
          {
            TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
          {
            TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
          }
          else if (is_qualified)
          {
            // output
            row = &curr_row_;
            last_left_row_ = left_row;
            break;
          }
          else
          {
            // continue with the next cached right row
            OB_ASSERT(NULL == last_left_row_);
            OB_ASSERT(NULL != left_row);
          }
        }
      }
      else
      {
        // left_row > last_join_left_row_ on euqijoin conditions
        right_cache_is_valid_ = false;
        right_cache_.clear();
      }
    }
    else
    {
      // fetch the next right row
      if (OB_UNLIKELY(is_right_iter_end_))
      {
        // no more right rows, but there are left rows left
        OB_ASSERT(left_row);

        if (OB_SUCCESS != (ret = left_join_rows(*left_row)))
        {
          TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
        }

        else
        {
          // output
          row = &curr_row_;
          OB_ASSERT(NULL == last_left_row_);
          OB_ASSERT(NULL == last_right_row_);
          break;
        }
      }
      else if (NULL != last_right_row_)
      {
        right_row = last_right_row_;
        last_right_row_ = NULL;
      }
      else
      {
        ret = right_op_->get_next_row(right_row);
        if (OB_SUCCESS != ret)
        {
          if (OB_ITER_END == ret)
          {
            TBSYS_LOG(INFO, "end of right child op");
            is_right_iter_end_ = true;
            if (!right_cache_.is_empty())
            {
              // no more right rows and the right cache is not empty, we SHOULD look at the next left row
              right_cache_is_valid_ = true;
              OB_ASSERT(NULL == last_right_row_);
              OB_ASSERT(NULL == last_left_row_);

              if (!last_left_row_has_printed_)
              {
                if (OB_SUCCESS != (ret = left_join_rows(*left_row)))
                {
                  TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
                }
                else
                {
                  // output
                  row = &curr_row_;
                  break;
                }
              }
              else
              {
                //ret = sort_reader_->get_next_row(left_row);
                ret = left_cache_for_store_.get_next_row(cached_left_row_);
                left_row=&cached_left_row_;
                last_left_row_has_printed_ = false;
              }
              //add 20140610:e
            }
            else
            {
              ret = OB_SUCCESS;
            }
            continue;
          }
          else
          {
            TBSYS_LOG(WARN, "failed to get next row from right child, err=%d", ret);
            break;
          }
        }
      }
      OB_ASSERT(left_row);
      OB_ASSERT(right_row);
      int cmp = 0;
      if (OB_SUCCESS != (ret = compare_equijoin_cond(*left_row, *right_row, cmp)))
      {
        TBSYS_LOG(WARN, "failed to compare, err=%d", ret);
        break;
      }
      if (0 == cmp)
      {
        if (right_cache_.is_empty())
        {
          // store the joined left row
          last_join_left_row_store_.assign_buffer(last_join_left_row_store_.ptr(),
                                                  MAX_SINGLE_ROW_SIZE);
          if (OB_SUCCESS != (ret = ObRowUtil::convert(*left_row,
                                                      last_join_left_row_store_,
                                                      last_join_left_row_)))
          {
            TBSYS_LOG(WARN, "failed to store left row, err=%d", ret);
            break;
          }
        }
        bool is_qualified = false;
        const ObRowStore::StoredRow *stored_row = NULL;
        if (OB_SUCCESS != (ret = right_cache_.add_row(*right_row, stored_row)))
        {
          TBSYS_LOG(WARN, "failed to store the row, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = join_rows(*left_row, *right_row)))
        {
          TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
        {
          TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
        }
        else if (is_qualified)
        {
          // output
          row = &curr_row_;
          last_left_row_ = left_row;
          OB_ASSERT(NULL == last_right_row_);
          break;
        }
        else
        {
          // continue with the next right row
          OB_ASSERT(NULL != left_row);
          OB_ASSERT(NULL == last_left_row_);
          OB_ASSERT(NULL == last_right_row_);
        }

      } // end 0 == cmp
      else if (cmp < 0)
      {
        // left_row < right_row on equijoin conditions
        if (!right_cache_.is_empty())
        {
          right_cache_is_valid_ = true;
          OB_ASSERT(NULL == last_left_row_);
          last_right_row_ = right_row;

          if (!last_left_row_has_printed_)
          {
            if (OB_SUCCESS != (ret = left_join_rows(*left_row)))
            {
              TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
            }
            else
            {
              // output
              row = &curr_row_;
              break;
            }
          }
          else
          {
            // ret = sort_reader_->get_next_row(left_row);
            ret = left_cache_for_store_.get_next_row(cached_left_row_);
            left_row=&cached_left_row_;
            last_left_row_has_printed_ = false;
          }
          //add 20140610:e
        }
        else
        {
          if (OB_SUCCESS != (ret = left_join_rows(*left_row)))
          {
            TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
          }
          else
          {
            // output
            row = &curr_row_;
            OB_ASSERT(NULL == last_left_row_);
            last_right_row_ = right_row;
            break;
          }
        }
      }
      else
      {
        // left_row > right_row on euqijoin conditions
        // continue with the next right row
        OB_ASSERT(NULL != left_row);
        OB_ASSERT(NULL == last_left_row_);
        OB_ASSERT(NULL == last_right_row_);
      }
    }
  } // end while
  return ret;
}
// RIGHT_SEMI_JOIN
int ObSemiJoin::right_semi_get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRow *right_row  = NULL;
  const ObRow *left_row = NULL;
  // fetch the next right row
  if (NULL != last_right_row_)
  {
    right_row = last_right_row_;
    last_right_row_ = NULL;
    last_right_row_has_printed_ = true;
  }
  else
  {
    ret = right_cache_for_store_.get_next_row(cached_right_row_);
    right_row=&cached_right_row_;
    last_right_row_has_printed_ = false;
  }
  while(OB_SUCCESS == ret)
  {
    if (left_cache_is_valid_)
    {
      OB_ASSERT(!left_cache_.is_empty());
      int cmp = 0;
      if (OB_SUCCESS != (ret = right_row_compare_equijoin_cond(*right_row, last_join_right_row_, cmp)))
      {
        TBSYS_LOG(WARN, "failed to compare, err=%d", ret);
        break;
      }
      if (0 == cmp)
      {
        // fetch the next left row from left_cache
        if (OB_SUCCESS != (ret = left_cache_.get_next_row(curr_cached_left_row_)))
        {
          if (OB_UNLIKELY(OB_ITER_END != ret))
          {
            TBSYS_LOG(WARN, "failed to get next row from left_cache, err=%d", ret);
          }
          else
          {
            left_cache_.reset_iterator(); // continue
            if (!last_right_row_has_printed_)
            {
              if (OB_SUCCESS != (ret = right_join_rows(*right_row)))
              {
                TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
              }
              else
              {
                // output
                row = &curr_row_;
                last_right_row_ = NULL;
                break;
              }
            }
            else
            {
              ret = right_cache_for_store_.get_next_row(cached_right_row_);
              right_row=&cached_right_row_;
              last_right_row_has_printed_ = false;
            }
          }
        }
        else
        {
          bool is_qualified = false;
          if (OB_SUCCESS != (ret = join_rows( curr_cached_left_row_,*right_row)))
          {
            TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
          {
            TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
          }
          else if (is_qualified)
          {
            // output
            row = &curr_row_;
            last_right_row_ = right_row;
            break;
          }
          else
          {
            // continue with the next cached left row
            OB_ASSERT(NULL == last_right_row_);
            OB_ASSERT(NULL != right_row);
          }
        }
      }
      else
      {
        // right_row > last_join_right_row_ on euqijoin conditions
        left_cache_is_valid_ = false;
        left_cache_.clear();
      }
    }
    else
    {
      // fetch the next left row
      if (OB_UNLIKELY(is_left_iter_end_))
      {
        // no more left rows, but there are right rows left
        OB_ASSERT(right_row);
        if (OB_SUCCESS != (ret = right_join_rows(*right_row)))
        {
          TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
        }
        else
        {
          // output
          row = &curr_row_;
          OB_ASSERT(NULL == last_right_row_);
          OB_ASSERT(NULL == last_left_row_);
          break;
        }
      }
      else if (NULL != last_left_row_)
      {
        left_row = last_left_row_;
        last_left_row_ = NULL;
      }
      else
      {
        ret = left_op_->get_next_row(left_row);
        if (OB_SUCCESS != ret)
        {
          if (OB_ITER_END == ret)
          {
            TBSYS_LOG(INFO, "end of left child op");
            is_left_iter_end_ = true;
            if (!left_cache_.is_empty())
            {
              // no more left rows and the left cache is not empty, we SHOULD look at the next right row
              left_cache_is_valid_ = true;
              OB_ASSERT(NULL == last_left_row_);
              OB_ASSERT(NULL == last_right_row_);
              if (!last_right_row_has_printed_)
              {
                if (OB_SUCCESS != (ret = right_join_rows(*right_row)))
                {
                  TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
                }
                else
                {
                  // output
                  row = &curr_row_;
                  break;
                }
              }
              else
              {
                ret = right_cache_for_store_.get_next_row(cached_right_row_);
                right_row=&cached_right_row_;
                last_right_row_has_printed_ = false;
              }
            }
            else
            {
              ret = OB_SUCCESS;
            }
            continue;
          }
          else
          {
            TBSYS_LOG(WARN, "failed to get next row from left child, err=%d", ret);
            break;
          }
        }
      }
      OB_ASSERT(right_row);
      OB_ASSERT(left_row);
      int cmp = 0;
      if (OB_SUCCESS != (ret = compare_equijoin_cond(*left_row, *right_row, cmp)))
      {
        TBSYS_LOG(WARN, "failed to compare, err=%d", ret);
        break;
      }
      if (0 == cmp)
      {
        if (left_cache_.is_empty())
        {
          // store the joined right row
          last_join_right_row_store_.assign_buffer(last_join_right_row_store_.ptr(),
                                                   MAX_SINGLE_ROW_SIZE);
          if (OB_SUCCESS != (ret = ObRowUtil::convert(*right_row,
                                                      last_join_right_row_store_,
                                                      last_join_right_row_)))
          {
            TBSYS_LOG(WARN, "failed to store right row, err=%d", ret);
            break;
          }
        }
        bool is_qualified = false;
        const ObRowStore::StoredRow *stored_row = NULL;
        if (OB_SUCCESS != (ret = left_cache_.add_row(*left_row, stored_row)))
        {
          TBSYS_LOG(WARN, "failed to store the row, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = join_rows(*left_row,*right_row)))
        {
          TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
        {
          TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
        }
        else if (is_qualified)
        {
          // output
          row = &curr_row_;
          last_right_row_ = right_row;
          OB_ASSERT(NULL == last_left_row_);
          break;
        }
        else
        {
          // continue with the next left row
          OB_ASSERT(NULL != right_row);
          OB_ASSERT(NULL == last_right_row_);
          OB_ASSERT(NULL == last_left_row_);
        }

      } // end 0 == cmp
      else if (cmp > 0)
      {
        // right_row < left_row on equijoin conditions
        if (!left_cache_.is_empty())
        {
          left_cache_is_valid_ = true;
          OB_ASSERT(NULL == last_right_row_);
          last_left_row_ = left_row;
          if (!last_right_row_has_printed_)
          {
            if (OB_SUCCESS != (ret = right_join_rows(*right_row)))
            {
              TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
            }
            else
            {
              // output
              row = &curr_row_;
              break;
            }
          }
          else
          {
            ret = right_cache_for_store_.get_next_row(cached_right_row_);
            right_row=&cached_right_row_;
            last_right_row_has_printed_ = false;
          }
        }
        else
        {
          if (OB_SUCCESS != (ret = right_join_rows(*right_row)))
          {
            TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
          }
          else
          {
            // output
            row = &curr_row_;
            OB_ASSERT(NULL == last_right_row_);
            last_left_row_ = left_row;
            break;
          }
        }
      }
      else
      {
        // right_row > left_row on euqijoin conditions
        // continue with the next left row
        OB_ASSERT(NULL != right_row);
        OB_ASSERT(NULL == last_right_row_);
        OB_ASSERT(NULL == last_left_row_);
      }
    }
  } // end while
  return ret;
}

// LEFT_ANTI_SEMI_JOIN
int ObSemiJoin::left_anti_semi_get_next_row(const ObRow *&row)
{
  row = NULL;
  return OB_SUCCESS;

}

// RIGHT_ANTI_SEMI_JOIN
int ObSemiJoin::right_anti_semi_get_next_row(const ObRow *&row)
{
  row = NULL;
  return OB_SUCCESS;

}
namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObSemiJoin, PHY_SEMI_JOIN);
  }
}
int64_t ObSemiJoin::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Semi ");
  pos += ObJoin::to_string(buf + pos, buf_len - pos);
  return pos;
}
PHY_OPERATOR_ASSIGN(ObSemiJoin)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObSemiJoin);
  reset();
  right_table_filter_->set_phy_plan (my_phy_plan_);
  if ((ret = set_join_type(o_ptr->join_type_)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Fail to set join type, ret=%d", ret);
  }
  else if ((ret = right_table_filter_->assign(o_ptr->right_table_filter_)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Assign empty_row_filter_ failed. ret=%d", ret);
  }
  else
  {
    ret = ObJoin::assign(other);
  }
  return ret;
}
