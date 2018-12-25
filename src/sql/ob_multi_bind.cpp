/**
 * ob_multi_bind.cpp
 *
 * Authors:
 *   tianz
 * used to get sub_query's result and direct bind result to main query 
 * or build hashmap and bloomfilter, and check filter conditions
 */

#include "ob_multi_bind.h"
#include "ob_table_rpc_scan.h"
#include "ob_table_mem_scan.h"
#include "ob_postfix_expression.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObMultiBind::ObMultiBind()
  :hashmap_num_(0)
{
  arena_.set_mod_id(ObModIds::OB_MS_SUB_QUERY);
  //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
  for(int i=0;i<MAX_SUB_QUERY_NUM;i++)
  {
    is_subquery_result_contain_null[i] = false;
  }
  //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
}

ObMultiBind::~ObMultiBind()
{
  sub_result_.~ObArray();
  for(int i=0;i<MAX_SUB_QUERY_NUM;i++)
  {
    sub_query_map_[i].destroy();
  }
  sub_query_filter_.~ObFilter();
  arena_.free();

}

void ObMultiBind::reset()
{
  sub_result_.clear();
  sub_query_filter_.reset();
  for(int i =0;i<MAX_SUB_QUERY_NUM;i++)
  	sub_query_map_[i].destroy();
  hashmap_num_ = 0;
  ObMultiChildrenPhyOperator::reset();
  arena_.free();
  //add peiouya [IN_TYPEBUG_FIX] 20151225
  for(int i=0;i<MAX_SUB_QUERY_NUM;i++)
  {
    sub_query_map_and_bloomfilter_column_type[i].clear ();
    is_subquery_result_contain_null[i] = false;  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
  }
  //add 20151225:e
}

void ObMultiBind::reuse()
{
  sub_result_.clear();
  sub_query_filter_.reuse();
  for(int i =0;i<MAX_SUB_QUERY_NUM;i++)
  	sub_query_map_[i].destroy();
  hashmap_num_ = 0;
  ObMultiChildrenPhyOperator::reuse();  
  arena_.free();
  //add peiouya [IN_TYPEBUG_FIX] 20151225
  for(int i=0;i<MAX_SUB_QUERY_NUM;i++)
  {
    sub_query_map_and_bloomfilter_column_type[i].clear ();
    is_subquery_result_contain_null[i] = false;  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
  }
  //add 20151225:e
}

//if not all sub_query direct bind specific value, use hashmap do second check
int ObMultiBind::get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  ObSqlExpression *filter_expr;  
  const ObRow *temp_row = NULL;

  if (OB_UNLIKELY(ObMultiChildrenPhyOperator::get_child_num() <= 0))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "ObMultiBind has no child, ret=%d", ret);
  }

  while(OB_SUCCESS == ret)
  {
    ret = get_child(0)->get_next_row(temp_row);
    if (OB_ITER_END == ret)
    {
      break;
    }
    else if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get next row from sub query");
    }
    else if( 0 == hashmap_num_ )
    {
      row = temp_row;
      break;
    }
    else
    {
   	  bool check_pass = true;
	  const ObObj *result = NULL;
	  int64_t sub_query_num = 0;//current expression's sub query num
	  int sub_query_idx = 0;
	  //for every filter expression, do second check (hash check)
	  for (int32_t sub_idx = 0; ret == OB_SUCCESS && sub_idx < hashmap_num_; sub_idx = sub_idx+(int32_t)sub_query_num)
	  { 
	    common::hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* p =&(sub_query_map_[sub_idx]);
	    if(OB_SUCCESS != (ret = sub_query_filter_.get_sub_query_expr(sub_idx+1,filter_expr,sub_query_idx)))
	    {
	      TBSYS_LOG(WARN, "fail to get sub query expr");
	    }
	    else
	    {
	      sub_query_num = filter_expr->get_sub_query_num();
	    }
        //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
        ////add peiouya [IN_TYPEBUG_FIX] 20151225:b
        ////if (OB_SUCCESS != (ret = filter_expr->calc(*temp_row, result,p,true)))
        //if (OB_SUCCESS != (ret = filter_expr->calc(*temp_row, result, p, &(sub_query_map_and_bloomfilter_column_type[sub_idx]), true)))
        //mod 20151225:e
        if (OB_SUCCESS != (ret = filter_expr->calc(*temp_row, result, p,is_subquery_result_contain_null[sub_idx], &(sub_query_map_and_bloomfilter_column_type[sub_idx]), true)))
        //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
		{
	      TBSYS_LOG(WARN, "failed to calculate, err=%d", ret);
	      break;
	    }
	    else if (!result->is_true())
	    {
	      check_pass = false;
	      break;
	    }
	  } // end for
	
	  if (OB_LIKELY(OB_SUCCESS == ret))
	  {
	    if(check_pass)
	    {
	      row = temp_row;
	      break;
	    }	
	    else
	    {
	      continue;
	    }
	  }
	  else
	  {
	    TBSYS_LOG(WARN, "error ,error???");
	    break;
	  }	 
    }	  	
  }

  return ret;
}

//return main query's row_desc
int ObMultiBind::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(get_child(0) == NULL))
  {
  	ret = OB_NOT_INIT;
  	TBSYS_LOG(ERROR, "children_ops_[0] is NULL");
  }
  else
  {
  	ret = get_child(0)->get_row_desc(row_desc);
  }
  return ret;
}

//for explain stmt
int64_t ObMultiBind::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObMultiBind()\n");

  databuff_printf(buf, buf_len, pos, "====main query====\n");
  pos += get_child(0)->to_string(buf + pos, buf_len - pos);
 
  for (int32_t i = 1; i < ObMultiChildrenPhyOperator::get_child_num(); ++i)
  {
    databuff_printf(buf, buf_len, pos, "====sub query%d====\n", i);
    pos += get_child(i)->to_string(buf + pos, buf_len - pos);
    if (i != ObMultiChildrenPhyOperator::get_child_num() - 1)
    {
      databuff_printf(buf, buf_len, pos, ",");
    }
  }

  return pos;
}

int ObMultiBind::close()
{
	 sub_result_.clear();
	 sub_query_filter_.close();
	 for(int i=0;i<MAX_SUB_QUERY_NUM;i++)
	 {
	   sub_query_map_[i].destroy();
	 }
	 arena_.free();

  return ObMultiChildrenPhyOperator::close();
}

//process sub_query first, then main query open
int ObMultiBind::open()
{
  int ret = OB_SUCCESS;
  ObPhyOperator *main_operator = NULL;
  main_operator = get_child(0);
  if (NULL == main_operator)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "main operator(s) is/are NULL");
  }
  else
  {
    //mod zhaoqiong [TableMemScan_Subquery_BUGFIX] 20151118:b
    ObTableRpcScan *rpc_query = dynamic_cast<ObTableRpcScan *>(get_child(0));
    ObTableMemScan *mem_query = dynamic_cast<ObTableMemScan *>(get_child(0));
    if( rpc_query == NULL && mem_query == NULL)
    {
      TBSYS_LOG(WARN, "wrong get table_rpc_scan_op or table_mem_scan_op, can't dynamic cast");
      ret = OB_NOT_SUPPORTED;
    }
    else if (rpc_query != NULL)
    {
      if(OB_SUCCESS != (ret = build_hash_map()))
      {
        TBSYS_LOG(WARN, "failed to load sub query result, err=%d", ret);
      }
    }
    else
    {
      if(OB_SUCCESS != (ret = build_mem_hash_map()))
      {
        TBSYS_LOG(WARN, "failed to load sub query result, err=%d", ret);
      }
    }
    //mod:e

    if (ret == OB_SUCCESS)
    {
      if (OB_SUCCESS != (ret = main_operator->open()))
      {
      TBSYS_LOG(WARN, "failed to open main query, err=%d", ret);
      }
    }
  }
  return ret;  
}

//process sub_query
int ObMultiBind::build_hash_map()
{
  int ret = OB_SUCCESS;
  const ObRow *row = NULL;
  const ObRow *temp_row = NULL;//add xionghui [subquery_final] 20160216
  const common::ObRowDesc *row_desc;
  ObSqlExpression *table_filter_expr;
  ObSqlExpression *read_param_expr;
  ObTableRpcScan * main_query = dynamic_cast<ObTableRpcScan *>(get_child(0));
  int32_t read_method = main_query->get_read_method();
  hashmap_num_ = 0;
  int use_bloomfilter_num = 0;
  int sub_query_index_in_expr = 0;//use for direct bind,indicate sub_query order in one expression 
  char* varchar_buf[MAX_SUB_QUERY_NUM] = {NULL};  //add peiouya [IN_TYPEBUG_FIX] 20151225
  for (int32_t sub_idx = 1; ret == OB_SUCCESS && sub_idx < ObMultiChildrenPhyOperator::get_child_num(); sub_idx++)
  {
    if (OB_SUCCESS != (ret = main_query->get_sub_query_expr(sub_idx,table_filter_expr,sub_query_index_in_expr)))
    {
      TBSYS_LOG(WARN, "fail to get sub query expr");
    }
    else if (OB_SUCCESS != (ret = main_query->get_param_sub_query_expr(sub_idx,read_param_expr,sub_query_index_in_expr)))
    {
      TBSYS_LOG(WARN, "fail to get sub query expr");
    }
    else if (OB_SUCCESS != (ret = get_child(sub_idx)->open()))
    {
      TBSYS_LOG(WARN, "fail to open sub select query:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = get_child(sub_idx)->get_row_desc(row_desc)))
    {
      TBSYS_LOG(WARN, "fail to get row_desc:ret[%d]", ret);
    }
    else
    {
      bool direct_bind = false;
      sub_result_.clear();

      int64_t rowkey_count = 0;
      bool special_sub_query = false;
      //mod by xionghui [subquery_final] 20160216 b:
      //read_param_expr->get_sub_query_column_num(sub_query_index_in_expr,rowkey_count,special_sub_query);
      bool special_case = false;
      read_param_expr->get_sub_query_column_num_new(sub_query_index_in_expr,rowkey_count,special_sub_query,special_case);
      //mod e:
      uint64_t table_id = 0;
      uint64_t column_id = 0;
      //special sub_query (such as "where (select ...)")
      //sub_query as a independent expression
      //just retrun true/false
      //do not need process real result,just return bool value
      if (special_sub_query)
      {
        //add xionghui [subquery_final] 20160216 b:
        if (special_case)
        {
          if (rowkey_count > 1)
          {
            ret=OB_ERR_COLUMN_SIZE;
            TBSYS_LOG(USER_ERROR, "sub-query more than one cloumn ret=%d", ret);
          }
          else
          {
            ret = get_child(sub_idx)->get_next_row(row);
            if (ret==OB_SUCCESS)
            {
              ret=get_child(sub_idx)->get_next_row(temp_row);
              if (ret==OB_ITER_END)
              {
                ret=OB_SUCCESS;
              }
              else
              {
                get_child(sub_idx)->close();
                ret=OB_ERR_SUB_QUERY_RESULTSET_EXCESSIVE;
                TBSYS_LOG(USER_ERROR, "sub-query more than one row ret=%d", ret);
              }
            }
            if (ret!=OB_SUCCESS)
            {
              if (OB_ITER_END == ret)//子查询为空
              {
                //delete dinggh 20161024
                //TBSYS_LOG(USER_ERROR, "Empty set");
                //add dinggh [sub_query_with_null_bug_fix] 20161024 b:
                ret = OB_SUCCESS;

                //TBSYS_LOG(INFO, "aaaaaaaaaaaaaaaaa=%s", to_cstring(*table_filter_expr));

                table_filter_expr->delete_sub_query_info(sub_query_index_in_expr);
                read_param_expr->delete_sub_query_info(sub_query_index_in_expr);

                ExprItem dem;
                dem.type_ = T_NULL;
                dem.data_type_ = ObNullType;

                table_filter_expr->add_expr_item(dem);
                read_param_expr->add_expr_item(dem);

                table_filter_expr->complete_sub_query_info();
                read_param_expr->complete_sub_query_info();
                //add 20161024 e
              }
            }
            else
            {
              row_desc->get_tid_cid(0,table_id,column_id);
              ObObj* cell;
              const_cast<ObRow *>(row)->get_cell(table_id,column_id,cell);

              table_filter_expr->delete_sub_query_info(sub_query_index_in_expr);
              read_param_expr->delete_sub_query_info(sub_query_index_in_expr);

              //table_filter_expr->add_expr_in_obj(*cell);
              //read_param_expr->add_expr_in_obj(*cell);

              //add xionghui [ooooops bug] 20160315 b:
              ObConstRawExpr expr;
              expr.set_value_and_type(*cell);
              if (OB_SUCCESS != (ret = expr.fill_sql_expression(*table_filter_expr)))
              {
                TBSYS_LOG(ERROR, "failed to fill_sql_expression expr item, err=%d", ret);
              }
              if (OB_SUCCESS != (ret = expr.fill_sql_expression(*read_param_expr)))
              {
                TBSYS_LOG(ERROR, "failed to fill_sql_expression expr item, err=%d", ret);
              }
              //add 20160315 e

              table_filter_expr->complete_sub_query_info();
              read_param_expr->complete_sub_query_info();

            }
          }
        }
        else
        { //add e:
          bool sub_query_result = false;
          if (OB_ITER_END == (ret = get_child(sub_idx)->get_next_row(row)))
          {
            //resultset is null
            ret = OB_SUCCESS;
          }
          else if (OB_SUCCESS != ret)
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
            if (OB_ITER_END != (ret = get_child(sub_idx)->get_next_row(row)))
            {
              //sub_query resultset more than one row,can not convert to bool value
              //return error
              get_child(sub_idx)->close();
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
          if (OB_SUCCESS == ret)
          {
            //fill bool value to postfix expression
            table_filter_expr->delete_sub_query_info(sub_query_index_in_expr);
            read_param_expr->delete_sub_query_info(sub_query_index_in_expr);

            ObObj * result = (ObObj*)arena_.alloc(sizeof(ObObj));
            result->set_bool(sub_query_result);

            table_filter_expr->add_expr_in_obj(*result);
            read_param_expr->add_expr_in_obj(*result);

            table_filter_expr->complete_sub_query_info();
            read_param_expr->complete_sub_query_info();
          }
        } //add xionghui [subquery_final] 20160216
      }
      else //sub_query as a part of IN or NOT_IN (such as "id in (select id from ...)")
      {
        //try push all sub_query reault to  sub_result_ (type:array)
        //if all sub_query reault size not bigger than  BIG_RESULTSET_THRESHOLD,use direct_bind stragety
        //or use hashmap and bloomfilter check
        while (OB_SUCCESS == ret && sub_result_.count()<=BIG_RESULTSET_THRESHOLD)
        {
          ret = get_child(sub_idx)->get_next_row(row);
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
            for (int i =0;i<rowkey_count;i++)
            {
              row_desc->get_tid_cid(i,table_id,column_id);
              ObObj *temp;
              const_cast<ObRow *>(row)->get_cell(table_id,column_id,temp);
              value[i] = *temp;
            }
	
            ObRowkey columns;
            columns.assign(value,rowkey_count);
            ObRowkey columns2;
            if (OB_SUCCESS != (ret = columns.deep_copy(columns2,arena_)))
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
          table_filter_expr->delete_sub_query_info(sub_query_index_in_expr);
          read_param_expr->delete_sub_query_info(sub_query_index_in_expr);
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
            for (int i = 0;i<rowkey_count;i++)
            {
              table_filter_expr->add_expr_in_obj(obj_ptr[i]);
              read_param_expr->add_expr_in_obj(obj_ptr[i]);
            }
            table_filter_expr->add_expr_item(dem1);
            read_param_expr->add_expr_item(dem1);
          }
          table_filter_expr->add_expr_item(dem2);
          read_param_expr->add_expr_item(dem2);         
          table_filter_expr->complete_sub_query_info();
          read_param_expr->complete_sub_query_info();

        }
        else if (hashmap_num_ >= MAX_SUB_QUERY_NUM) //restrict num of sub_query which use hashmap
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

          //add peiouya [IN_TYPEBUG_FIX] 20151225
          ret = get_child(sub_idx)->get_output_columns_dsttype (sub_query_map_and_bloomfilter_column_type[hashmap_num_]);
          if (OB_SUCCESS != ret)
          {
            break;
          }
          //add 20151225:e

          common::ObBloomFilterV1 *bloom_filter = NULL;// expr in table_rpc_scan
          common::ObBloomFilterV1 *bloom_filter2 = NULL;// expr in readparam

          bool use_bloomfilter = false;
          //get method do not use bloomfilter, OR expr and NOT_IN expr do not use bloomfilter
          if((ObSqlReadStrategy::USE_GET != read_method) && read_param_expr->is_satisfy_bloomfilter() && use_bloomfilter_num <=MAX_USE_BLOOMFILTER_NUM)
          {
            //init bloomfilter
            use_bloomfilter = true;
            use_bloomfilter_num ++;
            read_param_expr->set_has_bloomfilter();
            read_param_expr->get_bloom_filter(bloom_filter);

            table_filter_expr->set_has_bloomfilter();
            table_filter_expr->get_bloom_filter(bloom_filter2);
	
            if (OB_SUCCESS != (ret = bloom_filter->init(BLOOMFILTER_ELEMENT_NUM)))
            {
              TBSYS_LOG(WARN, "Problem initialize bloom filter");
              break;
            }
            else if (OB_SUCCESS != (ret = bloom_filter2->init(BLOOMFILTER_ELEMENT_NUM)))
            {
              TBSYS_LOG(WARN, "Problem initialize bloom filter");
              break;
            }
            bloom_filter->add_rowkey_type_desc (sub_query_map_and_bloomfilter_column_type[hashmap_num_]);
            bloom_filter2->add_rowkey_type_desc (sub_query_map_and_bloomfilter_column_type[hashmap_num_]);
          }
          //add the data in sub_result to hashmap and bloomfilter
          is_subquery_result_contain_null[hashmap_num_] = false;  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
          //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
          //while(sub_result_.count()>0)
          while((OB_SUCCESS == ret) && (sub_result_.count()>0))
          //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
          {
            ObRowkey columns;
            sub_result_.pop_back(columns);
            //mod peiouya [IN_TYPEBUG_FIX] 20151225 :b
            if(use_bloomfilter)
            {
              cast_and_set_hashmap_and_bloomfilter(&(sub_query_map_and_bloomfilter_column_type[hashmap_num_]),
                                                                               columns,
                                                                               rowkey_count,
                                                                               casted_cells,
                                                                               dst_value,
                                                                               sub_query_map_[hashmap_num_],
                                                                               bloom_filter,
                                                                               bloom_filter2,
                                                                               arena_,
                                                                               is_subquery_result_contain_null[hashmap_num_],  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
                                                                               ret);
            }
            else
            {
              cast_and_set_hashmap(&(sub_query_map_and_bloomfilter_column_type[hashmap_num_]),
                                                                               columns,
                                                                               rowkey_count,
                                                                               casted_cells,
                                                                               dst_value,
                                                                               sub_query_map_[hashmap_num_],
                                                                               arena_,
                                                                               is_subquery_result_contain_null[hashmap_num_],  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
                                                                               ret);
            }
            bind_memptr_for_casted_cells(casted_cells, rowkey_count, tmp_varchar_buff);
            /*
              sub_query_map_[hashmap_num_].set(columns,columns);
              if(use_bloomfilter)
              {
                bloom_filter->insert(columns);
                bloom_filter2->insert(columns);
              }
           */
            //mod 20151225:e
          }
          //add the left data to hashmap and bloomfilter
          while (OB_SUCCESS == ret)
          {
            ret = get_child(sub_idx)->get_next_row(row);
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
              for (int i =0;i<rowkey_count;i++)
              {
                row_desc->get_tid_cid(i,table_id,column_id);
                ObObj *temp;
                curr_row->get_cell(table_id,column_id,temp);
                value[i] = *temp;
              }

              ObRowkey columns;
              columns.assign(value,rowkey_count);
              //mod peiouya [IN_TYPEBUG_FIX] 20151225 :b
              /*
                ObRowkey columns2;
                if(OB_SUCCESS != (ret = columns.deep_copy(columns2,arena_)))
                {
                  TBSYS_LOG(WARN, "fail to deep copy column");
                  break;
                }
                sub_query_map_[hashmap_num_].set(columns2,columns2);
                if(use_bloomfilter)
                {
                  bloom_filter->insert(columns2);
                  bloom_filter2->insert(columns2);
                }
             */
              if (use_bloomfilter)
              {
                cast_and_set_hashmap_and_bloomfilter(&(sub_query_map_and_bloomfilter_column_type[hashmap_num_]),
                                                                                 columns,
                                                                                 rowkey_count,
                                                                                 casted_cells,
                                                                                 dst_value,
                                                                                 sub_query_map_[hashmap_num_],
                                                                                 bloom_filter,
                                                                                 bloom_filter2,
                                                                                 arena_,
                                                                                 is_subquery_result_contain_null[hashmap_num_],  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
                                                                                 ret);
              }
              else
              {
                cast_and_set_hashmap(&(sub_query_map_and_bloomfilter_column_type[hashmap_num_]),
                                                                                 columns,
                                                                                 rowkey_count,
                                                                                 casted_cells,
                                                                                 dst_value,
                                                                                 sub_query_map_[hashmap_num_],
                                                                                 arena_,
                                                                                 is_subquery_result_contain_null[hashmap_num_],  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
                                                                                 ret);
              }
              bind_memptr_for_casted_cells(casted_cells, rowkey_count, tmp_varchar_buff);
              //mod 20151225:e
            }
          }//end process left data to hashmap and bloomfilter
      
          hashmap_num_ ++;
        }
      }//end IN or NOT IN process
    }
  }//end for

  if (OB_SUCCESS == ret)
  {
    //update sub_query_num
    main_query->update_sub_query_num();
    //remove sqlexpression
    main_query->copy_filter(sub_query_filter_);
    main_query->remove_or_sub_query_expr();
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
  return ret;
}

//add zhaoqiong [TableMemScan_Subquery_BUGFIX] 20151118:b
int ObMultiBind::build_mem_hash_map()
{
  int ret = OB_SUCCESS;
  const ObRow *row = NULL;
  const ObRow *temp_row = NULL;//add xionghui [subquery_final] 20160216
  const common::ObRowDesc *row_desc;
  ObSqlExpression *table_filter_expr;
  ObTableMemScan *main_query = dynamic_cast<ObTableMemScan *>(get_child(0));
  hashmap_num_ = 0;
  int sub_query_index_in_expr = 0;//use for direct bind,indicate sub_query order in one expression
  char* varchar_buf[MAX_SUB_QUERY_NUM] = {NULL};  //add peiouya [IN_TYPEBUG_FIX] 20151225
  for (int32_t sub_idx = 1; ret == OB_SUCCESS && sub_idx < ObMultiChildrenPhyOperator::get_child_num(); sub_idx++)
  {
    if(OB_SUCCESS != (ret = main_query->get_sub_query_expr(sub_idx,table_filter_expr,sub_query_index_in_expr)))
    {
      TBSYS_LOG(WARN, "fail to get sub query expr");
    }
    else if(OB_SUCCESS != (ret = get_child(sub_idx)->open()))
    {
      TBSYS_LOG(WARN, "fail to open sub select query:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = get_child(sub_idx)->get_row_desc(row_desc)))
    {
      TBSYS_LOG(WARN, "fail to get row_desc:ret[%d]", ret);
    }
    else
    {
      bool direct_bind = false;
      sub_result_.clear();
      int64_t rowkey_count = 0;

      uint64_t table_id = 0;
      uint64_t column_id = 0;

      //add by xionghui [subquery_final] 20160216 b:
      bool special_sub_query = false;
      bool special_case = false;
      table_filter_expr->get_sub_query_column_num_new(sub_query_index_in_expr,rowkey_count,special_sub_query,special_case);
      if(special_sub_query)
      {
        if(special_case)
        {
          if(rowkey_count > 1)
          {
              ret=OB_ERR_COLUMN_SIZE;
              TBSYS_LOG(USER_ERROR, "sub-query more than one cloumn ret=%d", ret);
          }
          else
          {
              ret = get_child(sub_idx)->get_next_row(row);
              if(ret==OB_SUCCESS)
              {
                  ret=get_child(sub_idx)->get_next_row(temp_row);
                  if(ret==OB_ITER_END)
                  {
                      ret=OB_SUCCESS;
                  }
                  else
                  {
                      get_child(sub_idx)->close();
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
                    table_filter_expr->delete_sub_query_info(sub_query_index_in_expr);
                    ExprItem dem;
                    dem.type_ = T_NULL;
                    dem.data_type_ = ObNullType;
                    table_filter_expr->add_expr_item(dem);
                    table_filter_expr->complete_sub_query_info();
                    //add 20161024 e
                  }
              }
              else
              {
                  row_desc->get_tid_cid(0,table_id,column_id);
                  ObObj* cell;
                  const_cast<ObRow *>(row)->get_cell(table_id,column_id,cell);
                  table_filter_expr->delete_sub_query_info(sub_query_index_in_expr);
                  //table_filter_expr->add_expr_in_obj(*cell);
                  //add xionghui [ooooops bug] 20160315 b:
                  ObConstRawExpr expr;
                  expr.set_value_and_type(*cell);
                  if (OB_SUCCESS != (ret = expr.fill_sql_expression(*table_filter_expr)))
                  {
                      TBSYS_LOG(ERROR, "failed to fill_sql_expression expr item, err=%d", ret);
                  }
                  //add 20160315 e
                  table_filter_expr->complete_sub_query_info();
              }
          }
        }
        else
        {
          bool sub_query_result = false;
          if(OB_ITER_END == (ret = get_child(sub_idx)->get_next_row(row)))
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
            if(OB_ITER_END != (ret = get_child(sub_idx)->get_next_row(row)))
            {
              //sub_query resultset more than one row,can not convert to bool value
              //return error
               get_child(sub_idx)->close();
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
             table_filter_expr->delete_sub_query_info(sub_query_index_in_expr);
             ObObj * result = (ObObj*)arena_.alloc(sizeof(ObObj));
             result->set_bool(sub_query_result);
             table_filter_expr->add_expr_in_obj(*result);
             table_filter_expr->complete_sub_query_info();
          }
        }
      }
      //sub_query as a part of IN or NOT_IN (such as "id in (select id from ...)")
      else
      //add by xionghui [subquery_final] 20160216 e:
      {
      //try push all sub_query reault to  sub_result_ (type:array)
      //if all sub_query reault size not bigger than  BIG_RESULTSET_THRESHOLD,use direct_bind stragety
      //or use hashmap and bloomfilter check
      while (OB_SUCCESS == ret && sub_result_.count()<=BIG_RESULTSET_THRESHOLD)
      {
        ret = get_child(sub_idx)->get_next_row(row);
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
      if(direct_bind  && sub_result_.count()>0)
      //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
      {
        //direct bind sub query result_set to main query's filter
        table_filter_expr->delete_sub_query_info(sub_query_index_in_expr);
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
          table_filter_expr->add_expr_in_obj(obj_ptr[i]);
          }
          table_filter_expr->add_expr_item(dem1);
        }
        table_filter_expr->add_expr_item(dem2);

        table_filter_expr->complete_sub_query_info();
      }
      else if(hashmap_num_ >= MAX_SUB_QUERY_NUM) //restrict num of sub_query which use hashmap
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

        //add peiouya [IN_TYPEBUG_FIX] 20151225
        ret = get_child(sub_idx)->get_output_columns_dsttype (sub_query_map_and_bloomfilter_column_type[hashmap_num_]);
        if (OB_SUCCESS != ret)
        {
          break;
        }
        //add 20151225:e

        //add the data in sub_result to hashmap and bloomfilter
        is_subquery_result_contain_null[hashmap_num_] = false;  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
        //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
        //while(sub_result_.count()>0)
        while((OB_SUCCESS == ret) && (sub_result_.count()>0))
        //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
        {
          ObRowkey columns;
          sub_result_.pop_back(columns);

          //sub_query_map_[hashmap_num_].set(columns,columns);
          //add peiouya [IN_TYPEBUG_FIX] 20151225
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
          //add 20151225:e
        }
        //add the left data to hashmap and bloomfilter
        while (OB_SUCCESS == ret)
        {
          ret = get_child(sub_idx)->get_next_row(row);
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
            //mod peiouya [IN_TYPEBUG_FIX] 20151225
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
        }//end process left data to hashmap and bloomfilter

        hashmap_num_ ++;
      }
    }//end IN or NOT IN process
  }
  }//end for

 if(OB_SUCCESS == ret)
 {
   //update sub_query_num
  main_query->update_sub_query_num();
  //remove sqlexpression
  main_query->copy_filter(sub_query_filter_);
  main_query->remove_or_sub_query_expr();
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

  return ret;
}
//add:e


//add peiouya [IN_Subquery_prepare_BUGFIX] 20141013:b
PHY_OPERATOR_ASSIGN(ObMultiBind)
{
  int ret = OB_SUCCESS;
  UNUSED(other);
  reset();
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObMultiBind, PHY_MULTI_BIND);
  }
}
 //add 20141013:e
