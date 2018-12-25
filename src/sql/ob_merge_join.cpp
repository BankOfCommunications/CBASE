/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_join.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_merge_join.h"
#include "common/utility.h"
#include "common/ob_row_util.h"
#include "sql/ob_physical_plan.h" //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140610
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObMergeJoin::ObMergeJoin()
  :get_next_row_func_(NULL),
   last_left_row_(NULL),
   last_right_row_(NULL),
   right_cache_is_valid_(false),
   is_right_iter_end_(false),
   is_unequal_join_(false)//add liumz, [unequal_join]20150924
   ,hashmap_num_(0)//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140610 
{
    arena_.set_mod_id(ObModIds::OB_MS_SUB_QUERY);//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140610
    //add dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
    last_left_row_has_printed_ = false;
    last_right_row_has_printed_ = false;
    left_cache_is_valid_ = false;
    is_left_iter_end_ = false;
    //add 20140610:e
    //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
    for(int i=0;i<MAX_SUB_QUERY_NUM;i++)
    {
      is_subquery_result_contain_null[i] = false;
    }
    //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
}

ObMergeJoin::~ObMergeJoin()
{
  char *store_buf = last_join_left_row_store_.ptr();
  if (NULL != store_buf)
  {
    ob_free(store_buf);
    last_join_left_row_store_.assign_ptr(NULL, 0);
  }

   //add dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140611:b
  char *right_store_buf = last_join_right_row_store_.ptr();
  if (NULL != right_store_buf)
  {
    ob_free(right_store_buf);
    last_join_right_row_store_.assign_ptr(NULL, 0);
  }
  //add 20140611:e
  //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140610:b
  sub_result_.~ObArray();
  for(int i=0;i<MAX_SUB_QUERY_NUM;i++)
  {
    sub_query_map_[i].destroy();
  }
  arena_.free();
  //add 20140610:e
}

void ObMergeJoin::reset()
{
  /*get_next_row_func_ = NULL;
  last_right_row_ = NULL;
  last_left_row_ = NULL;
  right_cache_is_valid_ = false;
  is_right_iter_end_ = false;*/

  get_next_row_func_ = NULL;
  last_left_row_ = NULL;
  last_right_row_ = NULL;
  //last_join_left_row_.reset(false, ObRow::DEFAULT_NULL);
  char *store_buf = last_join_left_row_store_.ptr();
  if (NULL != store_buf)
  {
    ob_free(store_buf);
    last_join_left_row_store_.assign_ptr(NULL, 0);
  }

  //add dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140611:b
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
 //add 20140611:e


  right_cache_.clear();
  //curr_cached_right_row_.reset(false, ObRow::DEFAULT_NULL);
  //curr_row_.reset(false, ObRow::DEFAULT_NULL);
  row_desc_.reset();
  right_cache_is_valid_ = false;
  is_right_iter_end_ = false;  
  is_unequal_join_ = false;//add liumz, [unequal_join]20150924


  equal_join_conds_.clear();
  other_join_conds_.clear();
  left_op_ = NULL;
  right_op_ = NULL;
  //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140610 :b
   for(int i =0;i<MAX_SUB_QUERY_NUM;i++)
  	sub_query_map_[i].clear();
   hashmap_num_ = 0;
   //add 20140610:e
   //add peiouya [IN_TYPEBUG_FIX] 20151225
   for(int i=0;i<MAX_SUB_QUERY_NUM;i++)
   {
     sub_query_map_and_bloomfilter_column_type[i].clear ();
     is_subquery_result_contain_null[i] = false;  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
   }
   //add 20151225:e
}

void ObMergeJoin::reuse()
{
  get_next_row_func_ = NULL;
  last_left_row_ = NULL;
  last_right_row_ = NULL;
  //last_join_left_row_.reset(false, ObRow::DEFAULT_NULL);
  char *store_buf = last_join_left_row_store_.ptr();
  if (NULL != store_buf)
  {
    ob_free(store_buf);
    last_join_left_row_store_.assign_ptr(NULL, 0);
  }

  //add dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140611:b
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
 //add 20140611:e

  right_cache_.reuse();
  //curr_cached_right_row_.reset(false, ObRow::DEFAULT_NULL);
  //curr_row_.reset(false, ObRow::DEFAULT_NULL);
  row_desc_.reset();
  right_cache_is_valid_ = false;
  is_right_iter_end_ = false;
  is_unequal_join_ = false;//add liumz, [unequal_join]20150924
  equal_join_conds_.clear();
  other_join_conds_.clear();
  left_op_ = NULL;
  right_op_ = NULL;
  //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140610 :b
   for(int i =0;i<MAX_SUB_QUERY_NUM;i++)
  	sub_query_map_[i].clear();
   hashmap_num_ = 0;
   //add 20140610:e
   //add peiouya [IN_TYPEBUG_FIX] 20151225
   for(int i=0;i<MAX_SUB_QUERY_NUM;i++)
   {
     sub_query_map_and_bloomfilter_column_type[i].clear ();
     is_subquery_result_contain_null[i] = false;  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
   }
   //add 20151225:e
}

int ObMergeJoin::set_join_type(const ObJoin::JoinType join_type)
{
  int ret = OB_SUCCESS;
  ObJoin::set_join_type(join_type);
  switch(join_type)
  {
    case INNER_JOIN:
      get_next_row_func_ = &ObMergeJoin::inner_get_next_row;
      break;
    case LEFT_OUTER_JOIN:
      get_next_row_func_ = &ObMergeJoin::left_outer_get_next_row;
      break;
    case RIGHT_OUTER_JOIN:
      get_next_row_func_ = &ObMergeJoin::right_outer_get_next_row;
      break;
    case FULL_OUTER_JOIN:
      get_next_row_func_ = &ObMergeJoin::full_outer_get_next_row;
      break;
    case LEFT_SEMI_JOIN:
      get_next_row_func_ = &ObMergeJoin::left_semi_get_next_row;
      break;
    case RIGHT_SEMI_JOIN:
      get_next_row_func_ = &ObMergeJoin::right_semi_get_next_row;
      break;
    case LEFT_ANTI_SEMI_JOIN:
      get_next_row_func_ = &ObMergeJoin::left_anti_semi_get_next_row;
      break;
    case RIGHT_ANTI_SEMI_JOIN:
      get_next_row_func_ = &ObMergeJoin::right_anti_semi_get_next_row;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

//add liumz, [unequal_join]20150924:b
int ObMergeJoin::reset_join_type()
{
  int ret = OB_SUCCESS;
  if (is_unequal_join_)
  {
    ObJoin::JoinType join_type = ObJoin::get_join_type();
    switch(join_type)
    {
      case INNER_JOIN:
        get_next_row_func_ = &ObMergeJoin::inner_get_next_row;
        break;
      case LEFT_OUTER_JOIN:
        get_next_row_func_ = &ObMergeJoin::left_outer_ge_get_next_row;
        break;
      case RIGHT_OUTER_JOIN:
        get_next_row_func_ = &ObMergeJoin::right_outer_get_next_row;
        break;
      case FULL_OUTER_JOIN:
        get_next_row_func_ = &ObMergeJoin::full_outer_get_next_row;
        break;
      case LEFT_SEMI_JOIN:
        get_next_row_func_ = &ObMergeJoin::left_semi_get_next_row;
        break;
      case RIGHT_SEMI_JOIN:
        get_next_row_func_ = &ObMergeJoin::right_semi_get_next_row;
        break;
      case LEFT_ANTI_SEMI_JOIN:
        get_next_row_func_ = &ObMergeJoin::left_anti_semi_get_next_row;
        break;
      case RIGHT_ANTI_SEMI_JOIN:
        get_next_row_func_ = &ObMergeJoin::right_anti_semi_get_next_row;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        break;
    }
  }
  return ret;
}
//add:e

int ObMergeJoin::get_next_row(const ObRow *&row)
{
  OB_ASSERT(get_next_row_func_);
  return (this->*(this->ObMergeJoin::get_next_row_func_))(row);
}

int ObMergeJoin::open()
{
  int ret = OB_SUCCESS;
  const ObRowDesc *left_row_desc = NULL;
  const ObRowDesc *right_row_desc = NULL;
  char *store_buf = NULL;
  //add dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
  char *right_store_buf = NULL;
  //add 20140610:e  

  /* del liumz, [unequal_join]20150924
  if (equal_join_conds_.count() <= 0)
  {
    TBSYS_LOG(WARN, "merge join can not work without equijoin conditions");
    ret = OB_NOT_SUPPORTED;
  }
  else*/ if (OB_SUCCESS != (ret = ObJoin::open()))
  {
    TBSYS_LOG(WARN, "failed to open child ops, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = left_op_->get_row_desc(left_row_desc)))
  {
    TBSYS_LOG(WARN, "failed to get child row desc, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = right_op_->get_row_desc(right_row_desc)))
  {
    TBSYS_LOG(WARN, "failed to get child row desc, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = cons_row_desc(*left_row_desc, *right_row_desc)))
  {
    TBSYS_LOG(WARN, "failed to cons row desc, err=%d", ret);
  }
  // allocate memory for last_join_left_row_store_
  else if (NULL == (store_buf = static_cast<char*>(ob_malloc(MAX_SINGLE_ROW_SIZE, ObModIds::OB_SQL_MERGE_JOIN))))
  {
    TBSYS_LOG(ERROR, "no memory");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  //add dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
  //allocate memory for last_join_right_row_store_
  else if (NULL == (right_store_buf = static_cast<char*>(ob_malloc(MAX_SINGLE_ROW_SIZE, ObModIds::OB_SQL_MERGE_JOIN))))
  {
    TBSYS_LOG(ERROR, "no memory");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  //add 20140610:e
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
    //add liumz, [unequal_join]20150924
    if (equal_join_conds_.count() <= 0)
    {
      is_unequal_join_ = true;
      //reset_join_type();
    }
    else
    {
      is_unequal_join_ = false;
    }
    //add:e

    //add dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
    curr_cached_left_row_.set_row_desc(*left_row_desc);
    last_left_row_has_printed_ = false;
    last_right_row_has_printed_ = false;
    left_cache_is_valid_ = false;
    is_left_iter_end_ = false;
    last_join_right_row_store_.assign_buffer(right_store_buf,MAX_SINGLE_ROW_SIZE);
    //add 20140610:e

    last_join_left_row_store_.assign_buffer(store_buf, MAX_SINGLE_ROW_SIZE);
    //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140610:b 
    if(OB_SUCCESS != (ret = process_sub_query()))
    {
      TBSYS_LOG(ERROR, "mergejoin::process sub query error");
    }
    //add 20140610:e
  }
  return ret;
}
//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140610:b 
int ObMergeJoin::process_sub_query()
{
  int ret = OB_SUCCESS;
  const common::ObRowDesc *row_desc;
  const ObRow *row = NULL;
  const ObRow *temp_row = NULL;//add xionghui [fix equal subqeury bug] 20150122
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
      char* varchar_buf[MAX_SUB_QUERY_NUM] = {NULL};  //add peiouya [IN_TYPEBUG_FIX] 20151225
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
          bool special_case = false;//add xionghui [fix equal subqeury bug] 20150122
          //mod by  xionghui [fix equal subqeury bug] 20150122 b:
          expr.get_sub_query_column_num_new(j+1,rowkey_count,special_sub_query,special_case);
          //mod e:
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
                       expr.delete_sub_query_info(j+1);
                       ExprItem dem;
                       dem.type_ = T_NULL;
                       dem.data_type_ = ObNullType;
                       expr.add_expr_item(dem);
                       expr.complete_sub_query_info();
                       //add 20161024 e
                     }
                   }
                   else
                   {
                       row_desc->get_tid_cid(0,table_id,column_id);
                       ObObj* cell;
                       const_cast<ObRow *>(row)->get_cell(table_id,column_id,cell);
                       expr.delete_sub_query_info(j+1);
                       expr.add_expr_in_obj(*cell);
                       expr.complete_sub_query_info();
                   }
               }
            }
            else
            {
             //add e:
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
            //small resultset,use direct bind stragety
            //use specific value replace sub_query mark
            //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
            //if(direct_bind)
            if(direct_bind && sub_result_.count()>0)
            //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
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
              while(sub_result_.count()>0)
              {
                ObRowkey columns;
                sub_result_.pop_back(columns);	
                //add peiouya [IN_TYPEBUG_FIX] 20151225
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
                  //mod 20151225:e
                }
              }      
              hashmap_num_ ++;
            }
          }  
        }
        sub_operator->close();
      }
      //add peiouya [IN_TYPEBUG_FIX] 20151225
      for(int idx = 0; idx < MAX_SUB_QUERY_NUM; idx++)
      {
        if (NULL != varchar_buf[idx])
        {
          ob_free (varchar_buf[idx]);
        }
      }
      //add 20151225:e
    }
    expr.update_sub_query_num();
  }
  return ret;
}
//add 20140610:e

int ObMergeJoin::close()
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

  //add 20140610:e

  row_desc_.reset();
  ret = ObJoin::close();
  return ret;
}

int ObMergeJoin::get_row_desc(const common::ObRowDesc *&row_desc) const
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

int ObMergeJoin::compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const
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

int ObMergeJoin::left_row_compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const
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
int ObMergeJoin::right_row_compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const
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


int ObMergeJoin::curr_row_is_qualified(bool &is_qualified)
{
  int ret = OB_SUCCESS;
  is_qualified = true;
  const ObObj *res = NULL;
  int hash_mape_index = 0;//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140610
  for (int64_t i = 0; i < other_join_conds_.count(); ++i)
  {
    ObSqlExpression &expr = other_join_conds_.at(i);
    //mod peiouya [IN_TYPEBUG_FIX] 20151225:b
    //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140610:b
    bool use_hash_map = false;
    bool is_hashmap_contain_null = false;  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
    common::hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* p = NULL;
    common::ObArray<ObObjType> * p_data_type_desc = NULL;
    if(hashmap_num_>0 && expr.get_sub_query_num()>0)
    {
      p =&(sub_query_map_[hash_mape_index]);
      is_hashmap_contain_null = is_subquery_result_contain_null[hash_mape_index];  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
      p_data_type_desc=  &(sub_query_map_and_bloomfilter_column_type[hash_mape_index]);
      use_hash_map = true;
      hash_mape_index = hash_mape_index + (int)expr.get_sub_query_num();
    }
    //add 20140610:e
    
    //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
    ////mod tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140610:b
    ////if (OB_SUCCESS != (ret = expr.calc(curr_row_, res)))
    ////if (OB_SUCCESS != (ret = expr.calc(curr_row_, res, p, use_hash_map)))
    ////mod 20140610:e
    //if (OB_SUCCESS != (ret = expr.calc(curr_row_, res, p, p_data_type_desc, use_hash_map)))
    if (OB_SUCCESS != (ret = expr.calc(curr_row_, res, p, is_hashmap_contain_null,p_data_type_desc, use_hash_map)))
    //mod 20151225:e
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

int ObMergeJoin::cons_row_desc(const ObRowDesc &rd1, const ObRowDesc &rd2)
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

int ObMergeJoin::join_rows(const ObRow& r1, const ObRow& r2)
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

// INNER_JOIN
int ObMergeJoin::inner_get_next_row(const common::ObRow *&row)
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
    ret = left_op_->get_next_row(left_row);
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
            ret = left_op_->get_next_row(left_row);
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
              ret = left_op_->get_next_row(left_row);
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
          last_join_left_row_store_.assign_buffer(last_join_left_row_store_.ptr(), MAX_SINGLE_ROW_SIZE);
          if (OB_SUCCESS != (ret = ObRowUtil::convert(*left_row, last_join_left_row_store_, last_join_left_row_)))
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
        ret = left_op_->get_next_row(left_row);
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

int ObMergeJoin::left_join_rows(const ObRow& r1)
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

int ObMergeJoin::left_outer_get_next_row(const common::ObRow *&row)
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
        //add 20140610:e
        //del dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
//        else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
//        {
//          TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
//        }
//        else if (is_qualified)
//        {
//          // output
//          row = &curr_row_;
//          OB_ASSERT(NULL == last_left_row_);
//          OB_ASSERT(NULL == last_right_row_);
//          break;
//        }
//        else
//        {
//          // continue with the next left row
//          OB_ASSERT(NULL == last_left_row_);
//          OB_ASSERT(NULL == last_right_row_);
//          ret = left_op_->get_next_row(left_row);
//          continue;
//        }
        //del 20140610:e
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
              //del dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
//              ret = left_op_->get_next_row(left_row);
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
          last_join_left_row_store_.assign_buffer(last_join_left_row_store_.ptr(), MAX_SINGLE_ROW_SIZE);
          if (OB_SUCCESS != (ret = ObRowUtil::convert(*left_row, last_join_left_row_store_, last_join_left_row_)))
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
          //del dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
//          ret = left_op_->get_next_row(left_row);
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
          //del dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
//          bool is_qualified = false;
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
              last_right_row_ = right_row;
              break;
          }
          //add 20140610:e
          //del dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
//          else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
//          {
//            TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
//          }
//          else if (is_qualified)
//          {
//            // output
//            row = &curr_row_;
//            OB_ASSERT(NULL == last_left_row_);
//            last_right_row_ = right_row;
//            break;
//          }
//          else
//          {
//            // continue with the next left row
//            OB_ASSERT(NULL == last_left_row_);
//            ret = left_op_->get_next_row(left_row);
//            last_right_row_ = right_row;
//          }
          //del  20140610:e
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

int ObMergeJoin::left_outer_ge_get_next_row(const common::ObRow *&row)
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
        //add 20140610:e
        //del dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
//        else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
//        {
//          TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
//        }
//        else if (is_qualified)
//        {
//          // output
//          row = &curr_row_;
//          OB_ASSERT(NULL == last_left_row_);
//          OB_ASSERT(NULL == last_right_row_);
//          break;
//        }
//        else
//        {
//          // continue with the next left row
//          OB_ASSERT(NULL == last_left_row_);
//          OB_ASSERT(NULL == last_right_row_);
//          ret = left_op_->get_next_row(left_row);
//          continue;
//        }
        //del 20140610:e
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
              //del dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
//              ret = left_op_->get_next_row(left_row);
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
      /*int cmp = 0;
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
          last_join_left_row_store_.assign_buffer(last_join_left_row_store_.ptr(), MAX_SINGLE_ROW_SIZE);
          if (OB_SUCCESS != (ret = ObRowUtil::convert(*left_row, last_join_left_row_store_, last_join_left_row_)))
          {
            TBSYS_LOG(WARN, "failed to store left row, err=%d", ret);
            break;
          }
        }*/
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
      /*
      } // end 0 == cmp
      else if (cmp < 0)
      {
        // left_row < right_row on equijoin conditions
        if (!right_cache_.is_empty())
        {
          right_cache_is_valid_ = true;
          OB_ASSERT(NULL == last_left_row_);
          last_right_row_ = right_row;
          //del dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
//          ret = left_op_->get_next_row(left_row);
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
          //del dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
//          bool is_qualified = false;
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
              last_right_row_ = right_row;
              break;
          }
          //add 20140610:e
          //del dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
//          else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
//          {
//            TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
//          }
//          else if (is_qualified)
//          {
//            // output
//            row = &curr_row_;
//            OB_ASSERT(NULL == last_left_row_);
//            last_right_row_ = right_row;
//            break;
//          }
//          else
//          {
//            // continue with the next left row
//            OB_ASSERT(NULL == last_left_row_);
//            ret = left_op_->get_next_row(left_row);
//            last_right_row_ = right_row;
//          }
          //del  20140610:e
        }
      }
      else
      {
        // left_row > right_row on euqijoin conditions
        // continue with the next right row
        OB_ASSERT(NULL != left_row);
        OB_ASSERT(NULL == last_left_row_);
        OB_ASSERT(NULL == last_right_row_);
      }*/
    }
  } // end while
  return ret;
}

int ObMergeJoin::right_join_rows(const ObRow& r2)
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


//del dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
//int ObMergeJoin::right_outer_get_next_row(const common::ObRow *&row)
//{
//  int ret = OB_SUCCESS;
//  const ObRow *left_row = NULL;
//  const ObRow *right_row = NULL;
//  // fetch the next left row
//  if (NULL != last_left_row_)
//  {
//    left_row = last_left_row_;
//    last_left_row_ = NULL;
//  }
//  else
//  {
//    ret = left_op_->get_next_row(left_row);
//  }

//  while(OB_SUCCESS == ret)
//  {
//    if (right_cache_is_valid_)
//    {
//      OB_ASSERT(!right_cache_.is_empty());
//      int cmp = 0;
//      if (OB_SUCCESS != (ret = left_row_compare_equijoin_cond(*left_row, last_join_left_row_, cmp)))
//      {
//        TBSYS_LOG(WARN, "failed to compare, err=%d", ret);
//        break;
//      }
//      if (0 == cmp)
//      {
//        // fetch the next right row from right_cache
//        if (OB_SUCCESS != (ret = right_cache_.get_next_row(curr_cached_right_row_)))
//        {
//          if (OB_UNLIKELY(OB_ITER_END != ret))
//          {
//            TBSYS_LOG(WARN, "failed to get next row from right_cache, err=%d", ret);
//          }
//          else
//          {
//            right_cache_.reset_iterator(); // continue
//            // fetch the next left row
//            ret = left_op_->get_next_row(left_row);
//          }
//        }
//        else
//        {
//          bool is_qualified = false;
//          if (OB_SUCCESS != (ret = join_rows(*left_row, curr_cached_right_row_)))
//          {
//            TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
//          }
//          else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
//          {
//            TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
//          }
//          else if (is_qualified)
//          {
//            // output
//            row = &curr_row_;
//            last_left_row_ = left_row;
//            break;
//          }
//          else
//          {
//            // continue with the next cached right row
//            OB_ASSERT(NULL == last_left_row_);
//            OB_ASSERT(NULL != left_row);
//          }
//        }
//      }
//      else
//      {
//        // left_row > last_join_left_row_ on euqijoin conditions
//        right_cache_is_valid_ = false;
//        right_cache_.clear();
//      }
//    }
//    else
//    {
//      // fetch the next right row
//      if (OB_UNLIKELY(is_right_iter_end_))
//      {
//        ret = OB_ITER_END;
//        break;
//      }
//      else if (NULL != last_right_row_)
//      {
//        right_row = last_right_row_;
//        last_right_row_ = NULL;
//      }
//      else
//      {
//        ret = right_op_->get_next_row(right_row);

//        if (OB_SUCCESS != ret)
//        {
//          if (OB_ITER_END == ret)
//          {
//            TBSYS_LOG(INFO, "end of right child op");
//            is_right_iter_end_ = true;
//            if (!right_cache_.is_empty())
//            {
//              // no more right rows and the right cache is not empty, we SHOULD look at the next left row
//              right_cache_is_valid_ = true;
//              OB_ASSERT(NULL == last_right_row_);
//              OB_ASSERT(NULL == last_left_row_);
//              ret = left_op_->get_next_row(left_row);
//            }
//            continue;
//          }
//          else
//          {
//            TBSYS_LOG(WARN, "failed to get next row from right child, err=%d", ret);
//            break;
//          }
//        }
//      }
//      OB_ASSERT(left_row);
//      OB_ASSERT(right_row);
//      int cmp = 0;
//      if (OB_SUCCESS != (ret = compare_equijoin_cond(*left_row, *right_row, cmp)))
//      {
//        TBSYS_LOG(WARN, "failed to compare, err=%d", ret);
//        break;
//      }
//      if (0 == cmp)
//      {
//        if (right_cache_.is_empty())
//        {
//          // store the joined left row
//          last_join_left_row_store_.assign_buffer(last_join_left_row_store_.ptr(), MAX_SINGLE_ROW_SIZE);
//          if (OB_SUCCESS != (ret = ObRowUtil::convert(*left_row, last_join_left_row_store_, last_join_left_row_)))
//          {
//            TBSYS_LOG(WARN, "failed to store left row, err=%d", ret);
//            break;
//          }
//        }
//        bool is_qualified = false;
//        const ObRowStore::StoredRow *stored_row = NULL;
//        if (OB_SUCCESS != (ret = right_cache_.add_row(*right_row, stored_row)))
//        {
//          TBSYS_LOG(WARN, "failed to store the row, err=%d", ret);
//        }
//        else if (OB_SUCCESS != (ret = join_rows(*left_row, *right_row)))
//        {
//          TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
//        }
//        else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
//        {
//          TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
//        }
//        else if (is_qualified)
//        {
//          // output
//          row = &curr_row_;
//          last_left_row_ = left_row;
//          OB_ASSERT(NULL == last_right_row_);
//          break;
//        }
//        else
//        {
//          // continue with the next right row
//          OB_ASSERT(NULL != left_row);
//          OB_ASSERT(NULL == last_left_row_);
//          OB_ASSERT(NULL == last_right_row_);
//        }
//      } // end 0 == cmp
//      else if (cmp < 0)
//      {
//        // left_row < right_row on equijoin conditions
//        if (!right_cache_.is_empty())
//        {
//          right_cache_is_valid_ = true;
//        }
//        last_right_row_ = right_row;
//        OB_ASSERT(NULL == last_left_row_);
//        ret = left_op_->get_next_row(left_row);
//      }
//      else
//      {
//        // left_row > right_row on euqijoin conditions
//        OB_ASSERT(NULL != left_row);
//        OB_ASSERT(NULL != right_row);
//        OB_ASSERT(NULL == last_left_row_);
//        OB_ASSERT(NULL == last_right_row_);
//        bool is_qualified = false;
//        if (OB_SUCCESS != (ret = right_join_rows(*right_row)))
//        {
//          TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
//        }
//        else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
//        {
//          TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
//        }
//        else if (is_qualified)
//        {
//          // output
//          row = &curr_row_;
//          last_left_row_ = left_row;
//          break;
//        }
//        else
//        {
//          // continue with the next right row
//        }
//      }
//    }
//  } // end while
//  if (OB_ITER_END == ret && !is_right_iter_end_)
//  {
//    OB_ASSERT(NULL == last_left_row_);
//    // left row finished but we still have right rows left
//    do
//    {
//      if (NULL != last_right_row_)
//      {
//        right_row = last_right_row_;
//        last_right_row_ = NULL;
//      }
//      else
//      {
//        ret = right_op_->get_next_row(right_row);
//        if (OB_SUCCESS != ret)
//        {
//          if (OB_ITER_END == ret)
//          {
//            TBSYS_LOG(INFO, "end of right child op");
//            break;
//          }
//          else
//          {
//            TBSYS_LOG(WARN, "failed to get next row from right child, err=%d", ret);
//            break;
//          }
//        }
//      }
//      OB_ASSERT(right_row);
//      OB_ASSERT(NULL == last_right_row_);
//      bool is_qualified = false;
//      if (OB_SUCCESS != (ret = right_join_rows(*right_row)))
//      {
//        TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
//      }
//      else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
//      {
//        TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
//      }
//      else if (is_qualified)
//      {
//        // output
//        row = &curr_row_;
//        break;
//      }
//      else
//      {
//        // continue with the next right row
//      }
//    }
//    while (OB_SUCCESS == ret);
//  }
//  return ret;
//}
//del 20140610:e

//add dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
int ObMergeJoin::right_outer_get_next_row(const common::ObRow *&row)
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
    ret = right_op_->get_next_row(right_row);
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
                ret = right_op_->get_next_row(right_row);
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
                  ret = right_op_->get_next_row(right_row);
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
          last_join_right_row_store_.assign_buffer(last_join_right_row_store_.ptr(), MAX_SINGLE_ROW_SIZE);
          if (OB_SUCCESS != (ret = ObRowUtil::convert(*right_row, last_join_right_row_store_, last_join_right_row_)))
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
              ret = right_op_->get_next_row(right_row);
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
//add 20140610:e



int ObMergeJoin::full_outer_get_next_row(const common::ObRow *&row)
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
    ret = left_op_->get_next_row(left_row);
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
            ret = left_op_->get_next_row(left_row);
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
        bool is_qualified = false;
        if (OB_SUCCESS != (ret = left_join_rows(*left_row)))
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
          OB_ASSERT(NULL == last_left_row_);
          OB_ASSERT(NULL == last_right_row_);
          break;
        }
        else
        {
          // continue with the next left row
          OB_ASSERT(NULL == last_left_row_);
          OB_ASSERT(NULL == last_right_row_);
          ret = left_op_->get_next_row(left_row);
          continue;
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
              ret = left_op_->get_next_row(left_row);
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
          last_join_left_row_store_.assign_buffer(last_join_left_row_store_.ptr(), MAX_SINGLE_ROW_SIZE);
          if (OB_SUCCESS != (ret = ObRowUtil::convert(*left_row, last_join_left_row_store_, last_join_left_row_)))
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
          ret = left_op_->get_next_row(left_row);
        }
        else
        {
          bool is_qualified = false;
          if (OB_SUCCESS != (ret = left_join_rows(*left_row)))
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
            OB_ASSERT(NULL == last_left_row_);
            last_right_row_ = right_row;
            break;
          }
          else
          {
            // continue with the next left row
            OB_ASSERT(NULL == last_left_row_);
            ret = left_op_->get_next_row(left_row);
            last_right_row_ = right_row;
          }
        }
      }
      else
      {
        // left_row > right_row on euqijoin conditions
        OB_ASSERT(NULL != left_row);
        OB_ASSERT(NULL != right_row);
        OB_ASSERT(NULL == last_left_row_);
        OB_ASSERT(NULL == last_right_row_);
        bool is_qualified = false;
        if (OB_SUCCESS != (ret = right_join_rows(*right_row)))
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
          // continue with the next right row
        }
      }
    }
  } // end while
  if (OB_ITER_END == ret && !is_right_iter_end_)
  {
    OB_ASSERT(NULL == last_left_row_);
    // left row finished but we still have right rows left
    do
    {
      if (NULL != last_right_row_)
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
            break;
          }
          else
          {
            TBSYS_LOG(WARN, "failed to get next row from right child, err=%d", ret);
            break;
          }
        }
      }
      OB_ASSERT(right_row);
      OB_ASSERT(NULL == last_right_row_);
      bool is_qualified = false;
      if (OB_SUCCESS != (ret = right_join_rows(*right_row)))
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
        break;
      }
      else
      {
        // continue with the next right row
      }
    }
    while (OB_SUCCESS == ret);
  }
  return ret;
}


// INNER_JOIN, LEFT_OUTER_JOIN, RIGHT_OUTER_JOIN, FULL_OUTER_JOIN
int ObMergeJoin::normal_get_next_row(const ObRow *&row)
{
  row = NULL;
/*
  // 省略边界条件
  const ObRow *left_row = NULL;
  const ObRow *right_row = NULL;
  if (last_left_row_有效)
  {
    left_row = last_left_row_;
    last_left_row_ = NULL;
  }
  else
  {
    left_op_->get_next_row(left_row);
  }

  while(两个子运算符都还有数据没有迭代完)
  {
    if (left_row在等值条件上等于last_join_left_row_
        && right_cache_is_valid_)
    {
      从right_cache_中取下一条数据到right_row;
      if (right_cache_全部取完了一遍)
      {
        重置right_cache_中的迭代器到第一行;
        left_op_->get_next_row(left_row);
      }
      else
      {
        join left_row和right_row两行数据，输出到curr_row_;
        row = &curr_row_;
        break;
      }
    }
    else
    {
      if (last_right_row_有效)
      {
        right_row_ = last_right_row_;
        last_right_row_ = NULL;
      }
      else
      {
        right_op_->get_next_row(right_row);
      }

      if (left_row和right_row在等值join条件上相等)
      {
        join两行数据，输出到curr_row_;
        if (left_row在等值条件上不等于last_join_left_row_)
        {
          清空right_cache_和last_join_left_row_;
          right_cache_is_valid_ = false; // 初始时为false
          拷贝left_row到last_join_left_row_;
        }
        right_row保存添加到right_cache_中;
        last_left_row_ = left_row;
        row = &curr_row_;
        break;
      }
      else if(left_row在等值join条件上 < right_row)
      {
        if (LEFT_OUTER_JOIN == join_type_ || FULL_OUTER_JOIN == join_type_)
        {
          用left_row产生curr_row_，不足的值用NULL补齐;
          last_right_row_ = right_row;
          row = &curr_row_;
          break;
        }
        else
        {
          // INNER_JOIN or RIGHT_OUTER_JOIN
          right_cache_is_valid_ = true;
          last_right_row_ = right_row;
          left_op_->get_next_row(left_row);
        }
      }
      else
      {
        // left_row在等值join条件上 > right_row
        if (RIGHT_OUTER_JOIN == join_type_ || FULL_OUTER_JOIN == join_type_)
        {
          用right_row产生curr_row_, 不足的值用NULL补齐;
          last_left_row_ = left_row;
          row = &curr_row_;
          break;
        }
        else
        {
          // INNER_JOIN or LEFT_OUTER_JOIN
          right_op_->get_next_row(right_row);
        }
      }
    }
  } // end while
  if (没有新行产生
      && (RIGHT_OUTER_JOIN == join_type_ || FULL_OUTER_JOIN == join_type_)
      && right_row有效)
  {
    用right_row产生curr_row_，不足的值用NULL补齐;
    row = &curr_row_;
  }
  if (没有新行产生
      && (LEFT_OUTER_JOIN == join_type_ || FULL_OUTER_JOIN == join_type_)
      && right_row有效)
  {
    用left_row产生curr_row_，不足的值用NULL补齐;
    row = &curr_row_;
  }
*/
  return OB_SUCCESS;
}

// LEFT_SEMI_JOIN
int ObMergeJoin::left_semi_get_next_row(const ObRow *&row)
{
  row = NULL;
/*
  // 省略边界条件
  const ObRow *left_row = NULL;
  const ObRow *right_row = NULL;
  left_op_->get_next_row(left_row);
  if (last_right_row_有效)
  {
    right_row = last_right_row_;
    last_right_row_ = NULL;
  }
  else
  {
    right_op_->get_next_row(right_row);
  }
  while(两个子运算符还有数据没有迭代完)
  {
    if (left_row和right_row在等值join条件上相等)
    {
      row = left_row;
      last_right_row_ = right_row;
      break;
    }
    else if(left_row在等值join条件上 < right_row)
    {
      left_op_->get_next_row(left_row);
    }
    else
    {
      // left_row在等值join条件上 > right_row
      right_op_->get_next_row(right_row);
    }
  }
*/
  return OB_SUCCESS;

}

// RIGHT_SEMI_JOIN
int ObMergeJoin::right_semi_get_next_row(const ObRow *&row)
{
  row = NULL;
/*
  // 省略边界条件
  const ObRow *left_row = NULL;
  const ObRow *right_row = NULL;
  right_op_->get_next_row(right_row);
  if (last_left_row_有效)
  {
    left_row = last_left_row_;
    last_left_row_ = NULL;
  }
  else
  {
    left_op_->get_next_row(left_row);
  }
  while(两个子运算符还有数据没有迭代完)
  {
    if (left_row和right_row在等值join条件上相等)
    {
      row = right_row;
      last_left_row_ = left_row;
      break;
    }
    else if(right_row在等值join条件上 < left_row)
    {
      right_op_->get_next_row(right_row);
    }
    else
    {
      // right_row在等值join条件上 > left_row
      left_op_->get_next_row(left_row);
    }
  }
*/
  return OB_SUCCESS;

}

// LEFT_ANTI_SEMI_JOIN
int ObMergeJoin::left_anti_semi_get_next_row(const ObRow *&row)
{
  row = NULL;
/*
  // 省略边界条件
  const ObRow *left_row = NULL;
  const ObRow *right_row = NULL;
  left_op_->get_next_row(left_row);
  if (last_right_row_有效)
  {
    right_row = last_right_row_;
    last_right_row_ = NULL;
  }
  else
  {
    right_op_->get_next_row(right_row);
  }
  while(两个子运算符还有数据没有迭代完，既left_row和right_row都有效)
  {
    if (left_row和right_row在等值join条件上相等)
    {
      left_op_->get_next_row(left_row);
    }
    else if(left_row在等值join条件上 < right_row)
    {
      row = left_row_;
      last_right_row_ = right_row;
      break;
    }
    else
    {
      // left_row在等值join条件上 > right_row
      right_op_->get_next_row(right_row);
    }
  }
  if (没有找到行要输出
      && left_row有效)
  {
    row = left_row;             // left_op_还有数据
  }
*/
  return OB_SUCCESS;

}

// RIGHT_ANTI_SEMI_JOIN
int ObMergeJoin::right_anti_semi_get_next_row(const ObRow *&row)
{
  row = NULL;
/*
  // 省略边界条件
  const ObRow *left_row = NULL;
  const ObRow *right_row = NULL;
  right_op_->get_next_row(right_row);
  if (last_left_row_有效)
  {
    left_row = last_left_row_;
    last_left_row_ = NULL;
  }
  else
  {
    left_op_->get_next_row(left_row);
  }
  while(两个子运算符还有数据没有迭代完，既left_row和right_row都有效)
  {
    if (left_row和right_row在等值join条件上相等)
    {
      right_op_->get_next_row(right_row);
    }
    else if(right_row在等值join条件上 < left_row)
    {
      row = right_row_;
      last_left_row_ = left_row;
      break;
    }
    else
    {
      // right_row在等值join条件上 > left_row
      left_op_->get_next_row(left_row);
    }
  }
  if (没有找到行要输出
      && right_row有效)
  {
    row = right_row;            // right_op_还有数据
  }
*/
  return OB_SUCCESS;

}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObMergeJoin, PHY_MERGE_JOIN);
  }
}

int64_t ObMergeJoin::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Merge ");
  pos += ObJoin::to_string(buf + pos, buf_len - pos);
  return pos;
}

PHY_OPERATOR_ASSIGN(ObMergeJoin)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObMergeJoin);
  reset();
  if ((ret = set_join_type(o_ptr->join_type_)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Fail to set join type, ret=%d", ret);
  }
  else
  {
    ret = ObJoin::assign(other);
  }
  return ret;
}
