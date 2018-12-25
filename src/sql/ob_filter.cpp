/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_filter.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_filter.h"
#include "common/utility.h"
#include "sql/ob_physical_plan.h" //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20151016
using namespace oceanbase::sql;
using namespace oceanbase::common;

#define destroy_sql_expression_dlist(expr_list)\
  dlist_for_each_del(p, expr_list)\
  {\
    /*add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140606 */\
    dynamic_cast<ObSqlExpression*>(p)->destroy_filter();\
	/*add 20140606:e*/\
    ObSqlExpression::free(dynamic_cast<ObSqlExpression*>(p));\
  }

ObFilter::ObFilter()
//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20151016:b
  :need_handle_sub_query_(false),
  hashmap_num_(0)
//add tianz 20151016:e
{
  arena_.set_mod_id(ObModIds::OB_MS_SUB_QUERY);//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20151016
  //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
  for(int i=0;i<MAX_SUB_QUERY_NUM;i++)
  {
    is_subquery_result_contain_null[i] = false;
  }
  //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
}

ObFilter::~ObFilter()
{
  //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20151016:b
  sub_result_.~ObArray();
  for(int i=0;i<MAX_SUB_QUERY_NUM;i++)
  {
    sub_query_map_[i].destroy();
  }
  arena_.free();
  //add tianz 20151016:e
  destroy_sql_expression_dlist(filters_);
}

void ObFilter::reset()
{
  destroy_sql_expression_dlist(filters_);
  ObSingleChildPhyOperator::reset();
  //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20151016:b
  need_handle_sub_query_ = false;
  for(int i =0;i<MAX_SUB_QUERY_NUM;i++)
   sub_query_map_[i].clear();
  hashmap_num_ = 0;
  //add tianz 20151016:e
  //add peiouya [IN_TYPEBUG_FIX] 20151225
  for(int i=0;i<MAX_SUB_QUERY_NUM;i++)
  {
    sub_query_map_and_bloomfilter_column_type[i].clear ();
    is_subquery_result_contain_null[i] = false;  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
  }
  //add 20151225:e
}

void ObFilter::reuse()
{
  destroy_sql_expression_dlist(filters_);
  filters_.clear();
  ObSingleChildPhyOperator::reuse();
  //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20151016:b
  need_handle_sub_query_ = false;
  hashmap_num_ = 0;
  for(int i =0;i<MAX_SUB_QUERY_NUM;i++)
   sub_query_map_[i].clear();
  //add tianz 20151016:e
  //add peiouya [IN_TYPEBUG_FIX] 20151225
  for(int i=0;i<MAX_SUB_QUERY_NUM;i++)
  {
    sub_query_map_and_bloomfilter_column_type[i].clear ();
    is_subquery_result_contain_null[i] = false;  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
  }
  //add 20151225:e
}

int ObFilter::add_filter(ObSqlExpression* expr)
{
  int ret = OB_SUCCESS;
  expr->set_owner_op(this);
  if (!filters_.add_last(expr))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "failed to add column");
  }
  return ret;
}

//add dragon [type_err] 2016-12-6 b
int ObFilter::remove_last_filter()
{
  int ret = OB_SUCCESS;
  if(NULL == filters_.get_header()->get_next()
     || NULL == filters_.get_header()->get_prev())
  {
    TBSYS_LOG(ERROR, "wtf,you shouldn't see this");
    ret = OB_ERR_UNEXPECTED;
  }
  else
  {
    DLink *node = filters_.remove_last();
    ObSqlExpression *expr = dynamic_cast<ObSqlExpression*>(node);
    if(NULL == node)
    {
      TBSYS_LOG(WARN, "you cann't remove DList header!");
      ret = OB_ERR_UNEXPECTED;
    }
    else if(NULL == expr)
    {
      TBSYS_LOG(INFO, "this is not a ObSqlExpression type");
    }
    else
    {
      ObSqlExpression::free(expr);
    }
  }
  return ret;
}
//add e

int ObFilter::open()
{
  //del tianz [SubQuery_for_Instmt] [JHOBv0.1] 20151016:b
  //return ObSingleChildPhyOperator::open();
  //del tianz 20151016:e

  //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20151016:b
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ObSingleChildPhyOperator::open()))
  {
    if (!IS_SQL_ERR(ret))//add liumz, [ups -5049 log too much]20161217
    {
      TBSYS_LOG(WARN,"failed to open child op!ret=%d",ret);
    }
  }
  else if (need_handle_sub_query_
           && (OB_SUCCESS != (ret = process_sub_query())))
  {
    TBSYS_LOG(WARN,"failed to process sub query!ret=%d",ret);
  }
  return ret;
  //add tianz 20151016:e
}

//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20151016:b
int ObFilter::process_sub_query()
{
  int ret = OB_SUCCESS;
  const common::ObRowDesc *row_desc;
  const ObRow *row = NULL;
  const ObRow *temp_row = NULL;//add xionghui [fix equal subquery bug] 20150122

  dlist_for_each(ObSqlExpression, expr, filters_)
  {
    int64_t sub_query_num = expr->get_sub_query_num();
    if(sub_query_num<=0)
    {
      continue;
    }
    else
    {
      char* varchar_buf[MAX_SUB_QUERY_NUM] = {NULL};  //add peiouya [IN_TYPEBUG_FIX] 20151225
      for(int j = 0;j<sub_query_num;j++)
      {
        int32_t sub_query_index =expr->get_sub_query_idx(j);
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
          bool special_case = false;//add xionghui [fix equal subquery bug] 20150122
          expr->get_sub_query_column_num_new(j+1,rowkey_count,special_sub_query,special_case);

          uint64_t table_id = 0;
          uint64_t column_id = 0;

          //special sub_query (such as "where (select ...)")
          //sub_query as a independent expression
          //just retrun true/false
          //do not need process real result,just return bool value
          if(special_sub_query)
          {
              //add xionghui [fix equal subquery bug] 20150122 b:
              if(special_case)
              {
                  if(rowkey_count > 1)
                  {
                      ret=OB_ERR_COLUMN_SIZE;
                      TBSYS_LOG(USER_ERROR, "sub-query more than one cloumn ret=%d", ret);
                  }
                  else
                  {
                      ret = sub_operator->get_next_row(row);
                      if(ret==OB_SUCCESS)
                      {
                          ret=sub_operator->get_next_row(temp_row);
                          if(ret==OB_ITER_END)
                          {
                              ret=OB_SUCCESS;
                          }
                          else
                          {
                              sub_operator->close();
                              ret=OB_ERR_SUB_QUERY_RESULTSET_EXCESSIVE;
                              TBSYS_LOG(USER_ERROR, "sub-query more than one row ret=%d", ret);
                          }
                      }
                      if (ret!=OB_SUCCESS)
                      {
                        if(OB_ITER_END == ret)
                        {
                          //delete dinggh 20161024
                          //TBSYS_LOG(USER_ERROR, "Empty set");
                          //add dinggh [sub_query_with_null_bug_fix] 20161024 b:
                          ret = OB_SUCCESS;
                          expr->delete_sub_query_info(j+1);
                          ExprItem dem;
                          dem.type_ = T_NULL;
                          dem.data_type_ = ObNullType;
                          expr->add_expr_item(dem);
                          expr->complete_sub_query_info();
                          //add 20161024 e
                        }
                      }
                      else
                      {
                          row_desc->get_tid_cid(0,table_id,column_id);
                          ObObj* cell;
                          const_cast<ObRow *>(row)->get_cell(table_id,column_id,cell);
                          expr->delete_sub_query_info(j+1);
                          expr->add_expr_in_obj(*cell);
                          expr->complete_sub_query_info();
                      }
                  }
              }
              else
              {//add e:
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
                  expr->delete_sub_query_info(j+1);

                  ObObj * result = (ObObj*)arena_.alloc(sizeof(ObObj));
                  result->set_bool(sub_query_result);
                  expr->add_expr_in_obj(*result);
                  expr->complete_sub_query_info();
                }
              }//add xionghui [fix equal subquery bug] 20150122
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
                //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
                ////small resultset,use direct bind stragety
                ////use specific value replace sub_query mark
                //if(direct_bind)
                if(direct_bind && sub_result_.count()>0)
                //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
                {
                  //direct bind sub query result_set to main query's filter
                  expr->delete_sub_query_info(j+1);
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
                      expr->add_expr_in_obj(obj_ptr[i]);
                    }
                    expr->add_expr_item(dem1);
                  }

                  expr->add_expr_item(dem2);
                  expr->complete_sub_query_info();
                }
                else if(hashmap_num_ >= MAX_SUB_QUERY_NUM)//restrict num of sub_query which use hashmap
                {
                  TBSYS_LOG(WARN, "too many sub_query");
                  ret = OB_ERR_SUB_QUERY_OVERFLOW;
                }
                else
                {
                  //add peiouya [IN_TYPEBUG_FIX] 20151225:b
                  char *tmp_varchar_buff = NULL;
                  ObObj casted_cells[rowkey_count];
                  ObObj dst_value[rowkey_count];
                  alloc_small_mem_return_ptr(tmp_varchar_buff, rowkey_count, ret);
                  bind_memptr_for_casted_cells(casted_cells, rowkey_count, tmp_varchar_buff);
                  if (OB_SUCCESS != ret)
                  {
                    break;
                  }
                  varchar_buf[hashmap_num_]  = tmp_varchar_buff;
                  //add 20151225:e

                  //build hashmap and bloomfilter
                  sub_query_map_[hashmap_num_].create(HASH_BUCKET_NUM);
                  //add the row_store value to hashmap

                  //add peiouya [IN_TYPEBUG_FIX] 20151225
                  ret = sub_operator->get_output_columns_dsttype (sub_query_map_and_bloomfilter_column_type[hashmap_num_]);
                  if (OB_SUCCESS != ret)
                  {
                    break;
                  }
                  //add 20151225:e
                  is_subquery_result_contain_null[hashmap_num_] = false;  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
                  //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
                  //while(sub_result_.count()>0)
                  while((OB_SUCCESS == ret) && (sub_result_.count()>0))
                  //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
                  {
                    ObRowkey columns;
                    sub_result_.pop_back(columns);
                    //mod peiouya [IN_TYPEBUG_FIX] 20151225:b
                    //sub_query_map_[hashmap_num_].set(columns,columns);
                    cast_and_set_hashmap(&(sub_query_map_and_bloomfilter_column_type[hashmap_num_]),
                                                                                       columns,
                                                                                       rowkey_count,
                                                                                       casted_cells,
                                                                                       dst_value,
                                                                                       sub_query_map_[hashmap_num_],
                                                                                       arena_,
                                                                                       is_subquery_result_contain_null[hashmap_num_],  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
                                                                                       ret);
                    bind_memptr_for_casted_cells(casted_cells, rowkey_count, tmp_varchar_buff);
                    //mod 20151225:e
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
                      //mod peiouya [IN_TYPEBUG_FIX] 20151225:b
                      /*
                      ObRowkey columns2;
                      if(OB_SUCCESS != (ret = columns.deep_copy(columns2,arena_)))
                      {
                        TBSYS_LOG(WARN, "fail to deep copy column");
                        break;
                      }
                      sub_query_map_[hashmap_num_].set(columns2,columns2);
                      */
                     
                      cast_and_set_hashmap(&(sub_query_map_and_bloomfilter_column_type[hashmap_num_]),
                                                                                         columns,
                                                                                         rowkey_count,
                                                                                         casted_cells,
                                                                                         dst_value,
                                                                                         sub_query_map_[hashmap_num_],
                                                                                         arena_,
                                                                                         is_subquery_result_contain_null[hashmap_num_],  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
                                                                                         ret);
                      bind_memptr_for_casted_cells(casted_cells, rowkey_count, tmp_varchar_buff);
                      //mod  20151225:e
                    }
                  }
                  hashmap_num_ ++;
                }
            }
        }
        sub_operator->close();
      }
      //add peiouya [IN_TYPEBUG_FIX] 20151225
      for (int idx = 0; idx < MAX_SUB_QUERY_NUM; idx++)
      {
        if (NULL != varchar_buf[idx])
        {
          ob_free (varchar_buf[idx]);
        }
      }
      //add 20151225:e
    }
    expr->update_sub_query_num();
  }
  return ret;
}
//add 20151016:e


int ObFilter::close()
{
  destroy_sql_expression_dlist(filters_);
  return ObSingleChildPhyOperator::close();
}

int ObFilter::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == child_op_))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "child_op_ is NULL");
  }
  else
  {
    ret = child_op_->get_row_desc(row_desc);
  }
  return ret;
}

int ObFilter::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const common::ObRow *input_row = NULL;
  //TBSYS_LOG(DEBUG,"TEST FILTER:%s", to_cstring(*this));
  if (OB_UNLIKELY(NULL == child_op_))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "child_op_ is NULL");
  }
  else
  {
    const ObObj *result = NULL;
    bool did_output = true;
    while(OB_SUCCESS == ret
          && OB_SUCCESS == (ret = child_op_->get_next_row(input_row)))
    {
      //TBSYS_LOG(DEBUG,"TEST input_row:%s", to_cstring(*input_row));
      did_output = true;
      int hash_mape_index = 0;//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20151016
      ObSqlExpression* pp = NULL;
      uint64_t tabeid=0,columid=0;
      dlist_for_each(ObSqlExpression, p, filters_)
      {
        //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20151016:b
        pp = p;
        bool use_hash_map = false;
        bool is_hashmap_contain_null = false;  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
        common::hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* sub_hash = NULL;
        //add peiouya [IN_TYPEBUG_FIX] 20151225:b
        common::ObArray<ObObjType> * p_data_type_desc = NULL;
        if(hashmap_num_>0 && p->get_sub_query_num()>0)
        {
          sub_hash =&(sub_query_map_[hash_mape_index]);
          is_hashmap_contain_null = is_subquery_result_contain_null[hash_mape_index];  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
          use_hash_map = true;
          p_data_type_desc = &(sub_query_map_and_bloomfilter_column_type[hash_mape_index]);
          hash_mape_index = hash_mape_index + (int)p->get_sub_query_num();
        }
        //add 20151016:e

        //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
        ////mod tianz [SubQuery_for_Instmt] [JHOBv0.1] 20151016:b
        ////if (OB_SUCCESS != (ret = p->calc(*input_row, result)))
        ////if (OB_SUCCESS != (ret = p->calc(*input_row, result,sub_hash,use_hash_map)))
        ////mod tianz 20151016:e
        //if (OB_SUCCESS != (ret = p->calc(*input_row, result, sub_hash, p_data_type_desc, use_hash_map)))
        if (OB_SUCCESS != (ret = p->calc(*input_row, result, sub_hash,is_hashmap_contain_null, p_data_type_desc, use_hash_map)))
        {
        ////add 20151225:e
        //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
          TBSYS_LOG(WARN, "failed to calc expression, err=%d", ret);
          break;
        }
        else if (!result->is_true())
        {
          did_output = false;
          break;
        }
        input_row->get_row_desc()->get_tid_cid(0,tabeid,columid);

      } // end for

      if (did_output)
      {
        row = input_row;
        break;
      }

    } // end while
  }
  return ret;
}

//add liumz, [fix delete where bug] 20150128:b
void ObFilter::reset_iterator()
{
  if(child_op_ && PHY_MULTIPLE_GET_MERGE ==child_op_->get_type())
  {
    ObMultipleGetMerge *fuse_op = NULL;
    fuse_op = static_cast<ObMultipleGetMerge*>(child_op_);
    fuse_op->reset_iterator();
  }
}
//add :e

//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140408:b

/*Exp:use sub_query's number in main_query to find its index in physical_plan
* @param idx[in] sub_query's number in main_query
* @param index[out] sub_query index in physical_plan
*/
int ObFilter::get_sub_query_index(int32_t idx,int32_t &index)
{
  int ret = OB_ERROR;
  dlist_for_each(ObSqlExpression, p, filters_)
  {
    if(p->get_sub_query_num() == 0)
	 continue;
    for(int i=0;i<p->get_sub_query_num();i++)
    {
       idx--;
       if(0==idx)
       {
         index = p->get_sub_query_idx(i);
         ret = OB_SUCCESS;
         break;
       }	     
    }
    if(OB_SUCCESS == ret)
	break;
  }
  return ret;
}

/*Exp:get ObSqlExpression from readparam
* @param idx[in] idx, sub_query's number in main_query
* @param index[out] expr, ObSqlExpression which has corresponding sub_query
* @param index[out] query_index, sub_query's order in current expression
*/
int ObFilter::get_sub_query_expr(int32_t idx,ObSqlExpression *&expr,int &query_index)
{
  int ret = OB_ERROR;
  dlist_for_each(ObSqlExpression, p, filters_)
  {
     if(p->get_sub_query_num() == 0)
	 	continue;
     for(int i=0;i<p->get_sub_query_num();i++)
    {
       idx--;
       if(0==idx)
       {
         expr = p;
         query_index = i+1;
         ret = OB_SUCCESS;
         break;
       }	     
    }
    if(OB_SUCCESS == ret)
	break;	
  }
  return ret;
}

/*Exp:remove expression which has sub_query but not use bloomfilter 
* make stmt pass the first check 
*/
int ObFilter::remove_or_sub_query_expr()
{
   int ret = OB_SUCCESS;
   dlist_for_each(ObSqlExpression, p, filters_)
   {
     if(!p->has_bloom_filter() && ( p->get_sub_query_num()>0))
     	{
	 	 filters_.remove(p);
		 ObSqlExpression::free(dynamic_cast<ObSqlExpression*>(p));
     	}
   }
   return ret;
}

/*Exp:update sub_query num
* if direct bind sub_query result to main query,
* do not treat it as a sub_query any more
*/
void ObFilter::update_sub_query_num()
{
  dlist_for_each(ObSqlExpression, p, filters_)
   {
     if(0 == p->get_sub_query_num())
	 	continue;
     p->update_sub_query_num();
   }
}

/*Exp:copy filter expression to object filter
* used for ObMultiBind second check 
* @param object[in,out] object filter
*/
int ObFilter::copy_filter(ObFilter &object)
{
  int ret = OB_SUCCESS;
  dlist_for_each(ObSqlExpression, p, filters_)
  {
    if(p->get_sub_query_num()>0)
    {
      ObSqlExpression *filter = ObSqlExpression::alloc();
      filter->copy(p);
      object.add_filter(filter);
    }
  }
  return ret;
}

 //add 20140408:e
namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObFilter, PHY_FILTER);
  }
}

int64_t ObFilter::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Filter(filters=[");
  dlist_for_each_const(ObSqlExpression, p, filters_)
  {
    pos += p->to_string(buf+pos, buf_len-pos);
    databuff_printf(buf, buf_len, pos, ",");
  }
  databuff_printf(buf, buf_len, pos, "])\n");
  if (NULL != child_op_)
  {
    int64_t pos2 = child_op_->to_string(buf+pos, buf_len-pos);
    pos += pos2;
  }
  return pos;
}


DEFINE_SERIALIZE(ObFilter)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  //mod by bingo [Update rowkey bugfix] 20161218:b
  //add by maosy for size and expression is not equal 20161214
  obj.set_int(filters_.get_size());
  /*int count =0 ;
  dlist_for_each_const(ObSqlExpression, p, filters_)
  {
      count++;
  }
  obj.set_int (count);*/
  if (0 >= filters_.get_size())
  //if (0 >= count)
  //add by maosy e
  //mod by bingo:e
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "no column for output");
  }
  else if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(WARN, "fail to serialize filter expr count. ret=%d", ret);
  }
  else
  {
    dlist_for_each_const(ObSqlExpression, p, filters_)
    {
      if (OB_SUCCESS != (ret = p->serialize(buf, buf_len, pos)))
      {
        TBSYS_LOG(WARN, "filter expr serialize fail. ret=%d", ret);
        break;
      }
    } // end for
  }
  return ret;
}


DEFINE_GET_SERIALIZE_SIZE(ObFilter)
{
  int64_t size = 0;
  ObObj obj;
  //mod by bingo [Update rowkey bugfix] 20161218:b
  // add by maosy for size and expression is not equal 20161214
  /*int count =0 ;
  dlist_for_each_const(ObSqlExpression, p, filters_)
  {
      count++;
  }*/
  obj.set_int(filters_.get_size());
  //obj.set_int(count);
  //add by maosy e
  //mod by bingo:e
  size += obj.get_serialize_size();
  dlist_for_each_const(ObSqlExpression, p, filters_)
  {
    size += p->get_serialize_size();
  }
  return size;
}

DEFINE_DESERIALIZE(ObFilter)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  int64_t expr_count = 0, i = 0;
  destroy_sql_expression_dlist(filters_);
  if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(WARN, "fail to deserialize expr count. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = obj.get_int(expr_count)))
  {
    TBSYS_LOG(WARN, "fail to get expr_count. ret=%d", ret);
  }
  else
  {
    for (i = 0; i < expr_count; i++)
    {
      ObSqlExpression *expr = ObSqlExpression::alloc();
      if (NULL == expr)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "no memory");
        break;
      }
      else if (OB_SUCCESS != (ret = expr->deserialize(buf, data_len, pos)))
      {
        ObSqlExpression::free(expr);
        TBSYS_LOG(WARN, "fail to deserialize expression. ret=%d i=%ld count=%ld", ret, i, expr_count);
        break;
      }
      else if (OB_SUCCESS != (ret = add_filter(expr)))
      {
        TBSYS_LOG(WARN, "fail to add expression to filter.ret=%d, buf=%p, data_len=%ld, pos=%ld", ret, buf, data_len, pos);
        break;
      }
    }
  }
  return ret;
}

PHY_OPERATOR_ASSIGN(ObFilter)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObFilter);
  reset();
  dlist_for_each_const(ObSqlExpression, p, o_ptr->filters_)
  {
    ObSqlExpression *expr = ObSqlExpression::alloc();
    if (NULL == expr)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "no memory");
      break;
    }
    else
    {
      *expr = *p;
      expr->set_owner_op(this);
      if (!filters_.add_last(expr))
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "Add expression to ObFilter failed");
        break;
      }
    }
  }
  return ret;
}

ObPhyOperatorType ObFilter::get_type() const
{
  return PHY_FILTER;
}
