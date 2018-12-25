/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql_read_strategy.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_sql_read_strategy.h"

using namespace oceanbase;
using namespace common;
using namespace sql;

ObSqlReadStrategy::ObSqlReadStrategy()
  :simple_in_filter_list_(common::OB_COMMON_MEM_BLOCK_SIZE, ModulePageAllocator(ObModIds::OB_SQL_READ_STRATEGY)),
   simple_cond_filter_list_(common::OB_COMMON_MEM_BLOCK_SIZE, ModulePageAllocator(ObModIds::OB_SQL_READ_STRATEGY)),
   rowkey_info_(NULL)
{
   memset(start_key_mem_hold_, 0, sizeof(start_key_mem_hold_));
   memset(end_key_mem_hold_, 0, sizeof(end_key_mem_hold_));
  //mod fyd IN_EXPR  [PrefixKeyQuery_for_INstmt] 2014.4.8
  range_count_ = 0;//默认值为0 扶元地
  //add peiouya IN_EXPR  [PrefixKeyQuery_for_INstmt] 20140930:b
  range_count_cons_ = 0;//默认值为0 
  //add 20140930:e
  idx_key_ = 0 ;// 表示当前的rowkey 数组的下标
  mutiple_start_key_objs_ = NULL;
  mutiple_end_key_objs_ = NULL;
  mutiple_start_key_mem_hold_= NULL ;
  mutiple_end_key_mem_hold_= NULL;
  in_sub_query_idx_ = OB_INVALID_INDEX;//add dyr [Insert_Subquery_Function] [test] 20151029
}
//mod:e
ObSqlReadStrategy::~ObSqlReadStrategy()
{
  this->destroy();
}

//add wanglei [semi join update] 20160523:b
void ObSqlReadStrategy::reset_for_semi_join()
{
  simple_in_filter_list_.clear();
  simple_cond_filter_list_.clear();
  rowkey_info_ = NULL;
  memset(start_key_mem_hold_, 0, sizeof(start_key_mem_hold_));
  memset(end_key_mem_hold_, 0, sizeof(end_key_mem_hold_));
  /**
   * add by fyd  [PrefixKeyQuery_for_INstmt] 2014.4.8
   */
  release_rowkey_objs() ;
  idx_key_ = 0;
  range_count_ = 0;
  //add peiouya IN_EXPR  [PrefixKeyQuery_for_INstmt] 20140930:b
  range_count_cons_ = 0;//默认值为0
  //add 20140930:e
  //add:e
  in_sub_query_idx_ = OB_INVALID_INDEX;//add dyr [Insert_Subquery_Function] [test] 20151029
}
//add wanglei [semi join update] 20160523:b
void ObSqlReadStrategy::reset()
{
  simple_in_filter_list_.clear();
  simple_cond_filter_list_.clear();
  rowkey_info_ = NULL;
  memset(start_key_mem_hold_, 0, sizeof(start_key_mem_hold_));
  memset(end_key_mem_hold_, 0, sizeof(end_key_mem_hold_));
  /**
   * add by fyd  [PrefixKeyQuery_for_INstmt] 2014.4.8
   */
  release_rowkey_objs() ;
  idx_key_ = 0;
  range_count_ = 0;
  //add peiouya IN_EXPR  [PrefixKeyQuery_for_INstmt] 20140930:b
  range_count_cons_ = 0;//默认值为0 
  //add 20140930:e
  //add:e
  in_sub_query_idx_ = OB_INVALID_INDEX;//add dyr [Insert_Subquery_Function] [test] 20151029

}
/**
 * added  by fyd IN_EXPR  [PrefixKeyQuery_for_INstmt]  2014.4.8
 */
struct ObSqlReadStrategy::Comparer
{
	Comparer(const common::ObRowkeyInfo & rowkey_info):compare_rowkey_info_(rowkey_info)
	{
		 // compare_rowkey_info_ =  rowkey_info;
	}
	bool  operator()(const sql::ObSqlReadStrategy::RowKey_Objs objs1,const sql::ObSqlReadStrategy::RowKey_Objs objs2)
	{

		    int  ret = 0  ;
		    int i ;
		    for ( i = 0; i < compare_rowkey_info_.get_size();i++)
		    {
		    	if ( (ret = objs1.row_key_objs_[i].compare(objs2.row_key_objs_[i])) != 0)
		        {
		    		break;
		        }
		    	else  if ( objs1.row_key_objs_[i].is_max_value() || objs1.row_key_objs_[i].is_min_value() )
		    	{
		    		 break ;
		    	}
		    }
		    return ret < 0 ? true : false ;
	}
private:
	const common::ObRowkeyInfo &compare_rowkey_info_;
};
//add:e
/**
 * added by  fyd IN_EXPR  [PrefixKeyQuery_for_INstmt] 2014.4.9
 */
int ObSqlReadStrategy::compare_range(const sql::ObSqlReadStrategy::RowKey_Objs objs1,const sql::ObSqlReadStrategy::RowKey_Objs objs2) const
{
    int  ret = OB_SUCCESS  ;
    if ( NULL!=rowkey_info_)
    {
		for ( int  i = 0; i < rowkey_info_->get_size();i++)
		{
			if ( (ret = objs1.row_key_objs_[i].compare(objs2.row_key_objs_[i])) != 0)
			{
				break;
			}
			else  if ( objs1.row_key_objs_[i].is_max_value() || objs1.row_key_objs_[i].is_min_value() )
			{
				 break ;
			}
		}
    }
    else
    {
    	ret = OB_ERR_UNEXPECTED;
    	TBSYS_LOG(ERROR, "rowkey_info_shoud not be null, ret= %d", ret);
    }

    return ret  ;
}
/**
 * added  by fyd IN_EXPR  [PrefixKeyQuery_for_INstmt]  2014.4.10
 */
int ObSqlReadStrategy::eraseDuplicate(pRowKey_Objs start_rowkeys, pRowKey_Objs end_rowkeys, bool forward, int64_t position)
{
	int ret = OB_SUCCESS;
	if ( NULL == start_rowkeys || NULL == end_rowkeys || range_count_ <=0 )
	{
		ret = OB_ERR_UNEXPECTED ;
		TBSYS_LOG(ERROR, "param error   range_count_= %ld, ret =  %d",range_count_, ret);
	}
	else if ( position < range_count_ && position >=1 && !forward )
	{
		for ( int64_t i = position - 1 ; i < range_count_ - 1  ; i++ )
		{
			memcpy(start_rowkeys + i , start_rowkeys + (i + 1) ,  sizeof(RowKey_Objs));
			memcpy(end_rowkeys + i , end_rowkeys + (i + 1) ,  sizeof(RowKey_Objs));
		}
		range_count_ -- ;
		if ( range_count_ <= 0 )
		{
			ret = OB_ERR_UNEXPECTED ;
			TBSYS_LOG(ERROR, "IN duplicate ranges , range_count_ = %ld error  %d", range_count_,ret);
		}
	}
	else
	{
		ret = OB_ERR_UNEXPECTED ;
		TBSYS_LOG(WARN, "UNEXPECTED ,check your param !  ret =  %d", ret);
	}
	return ret;
}
//add:e
/** * added  by fyd  IN_EXPR  [PrefixKeyQuery_for_INstmt] 2014.4.8
 * desc: here,sort the the ranges,exclude the duplicated ranges,even merge the ranges,
 * todo: to be contineued.....
 * warning: before this function is called ,we must confirm that the ranges are intercrossed,expect duplicate.
 */
int ObSqlReadStrategy::sort_mutiple_range()
{
   int  ret = OB_SUCCESS;
   int64_t i ;
   if ( range_count_ > 1)
   {
  	 pRowKey_Objs start_rowkeys =  (pRowKey_Objs)mutiple_start_key_objs_ ;
   	 pRowKey_Objs end_rowkeys  =  (pRowKey_Objs)mutiple_end_key_objs_ ;
  	// UNUSED(tmp_rowkeys);
  	 std::sort(start_rowkeys , start_rowkeys + range_count_ ,  ObSqlReadStrategy::Comparer(*rowkey_info_));
  	// UNUSED(tmp_rowkeys);
  	 std::sort(end_rowkeys , end_rowkeys + range_count_ ,  ObSqlReadStrategy::Comparer(*rowkey_info_));
//  	 qsort( (void*)tmp_rowkeys, range_count_ ,sizeof(RowKey_Objs), compare_range);
  	 for ( i = range_count_ - 1; i >= 1 ;i-- )
  	 {
  		 if ( 0 == compare_range( *(start_rowkeys+i), *(start_rowkeys+i-1) )  && 0 == compare_range( *(end_rowkeys+i), *(end_rowkeys+i-1) ) )
  		 {
  			if ( (ret = eraseDuplicate( start_rowkeys,  end_rowkeys , false, i ) ) !=OB_SUCCESS )
  			{
  				TBSYS_LOG(ERROR, "fail to  erase duplicate ranges  %d", ret);
  				break;
  			}
  			else
  			{
  				TBSYS_LOG(DEBUG, "erase duplicate ranges success  %d", ret);
  			}
  		 }
  	 }
   }
   else if (range_count_ <= 0)
   {
		ret = OB_ERR_UNEXPECTED ;
		TBSYS_LOG(WARN, "check your param ! range_count_=%ld, ret =  %d",range_count_, ret);
   }
   return ret ;
}
//add:e
/**
 * 从调用关系看，该函数在find_rowkeys_from_equal_expr 被调用，因此可以认为目前该函数的作用仅仅是从从 等式形式的表达式中获取具体列(rowkey) 的range 范围
 * single_row_only ： true
 * fyd
 * 2014.3.24
 */
int ObSqlReadStrategy::find_single_column_range(bool real_val, int64_t idx, uint64_t column_id, bool &found)
{
  static const bool single_row_only = true;
  bool found_start = false;
  bool found_end = false;
  //mod by fyd IN_EXPR  [PrefixKeyQuery_for_INstmt]
 ////  int ret = find_closed_column_range(real_val, idx, column_id, found_start, found_end, single_row_only);
  int ret = find_closed_column_range_ex(real_val, idx, column_id, found_start, found_end, single_row_only);
 //mod:e
  found = (found_start && found_end);
  return ret;
}
/**
 * 构建所有的range
 * added  by fyd IN_EXPR  [PrefixKeyQuery_for_INstmt] 2014.3.27
 */
int ObSqlReadStrategy::find_scan_range(bool &found, bool single_row_only)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  uint64_t column_id = OB_INVALID_ID;
  bool found_start = false;
  bool found_end = false;
  //再次初始化，防止出错
  idx_key_ = 0;
  range_count_ = 0 ;
  if(NULL != rowkey_info_)
  {
	  for (idx = 0; idx < rowkey_info_->get_size(); idx++)
	  {
		if (OB_SUCCESS != (ret = rowkey_info_->get_column_id(idx, column_id)))
		{
		  TBSYS_LOG(WARN, "fail to get column id ret=%d, idx=%ld, column_id=%ld", ret, idx, column_id);
		  break;
		}
		else
		{
		  if (OB_SUCCESS != (ret = find_closed_column_range_ex(true, idx, column_id, found_start, found_end, single_row_only)))
		  {
			TBSYS_LOG(WARN, "fail to find closed column range for column %lu", column_id);
			break;
		  }
		}
		if (!found_start || !found_end)
		{
		  break; // no more search
		}
	  }

	 // 如果执行到目前的位置，程序出错了，但是程序会继续，因为上述程序的每次执行都是缩小程序的范围，
	 //因此这里，忽略所有的错误，继续执行，这不会对查询结果造成影响
	 //  需要在这里对range排序
	 //TODO

	  if (0 == idx && (!found_start) && (!found_end))
	  {
		found = false;
	  }
	  if ( OB_SUCCESS == ret )
	  {
		  if ( (ret = print_ranges()) !=OB_SUCCESS)
		  {
			  TBSYS_LOG(ERROR, "print ranges error! ret = %d",ret);
		  }
		  else if ( ( ret = sort_mutiple_range() )!= OB_SUCCESS)
		  {
			  TBSYS_LOG(ERROR, "sorting ranges error! ret = %d",ret);
		  }
		  else if ( (ret = print_ranges()) !=OB_SUCCESS)
		  {
			  TBSYS_LOG(ERROR, "print ranges error! ret = %d",ret);
		  }
	  }
	  else
	  {
		  if ( range_count_ > 0)
		  {
			  this ->range_count_ = 1;
			  TBSYS_LOG(WARN, "construct ranges  failed,the error can be ignored !,ret=%d",ret);
			  ret = OB_SUCCESS;
		  }
		  else
		  {
			  TBSYS_LOG(ERROR, "construct ranges  failed, ob_malloc may failed!,ret=%d",ret);
		  }

	  }
  }else
  {
	  ret = OB_ERR_UNEXPECTED ;
	  TBSYS_LOG(ERROR, "rowkey_info should not be null,ret=%d",ret);
  }
  return ret;
}
//add:e
/**
 * added  by fyd IN_EXPR  [PrefixKeyQuery_for_INstmt]  2014.4.8
 */
bool ObSqlReadStrategy::has_next_range()
{
	bool has_next = false;//ret = OB_SUCCESS;
	if ( idx_key_ >= 0 && idx_key_ < range_count_ )
	{
		has_next =true;
	}
	else //if (idx_key_ < range_count_ && idx_key_ >=0 )
	{
		has_next =false;
	}
	return has_next ;
}
//add:e
/**
 * added by fyd IN_EXPR  [PrefixKeyQuery_for_INstmt]  2014.4.9
 */
int ObSqlReadStrategy::get_next_scan_range(ObNewRange &range,bool &has_next)
{
	int ret = OB_SUCCESS;
	if ( range_count_ >= 1 )
	{
	    range.start_key_.assign(mutiple_start_key_objs_ + idx_key_*OB_MAX_ROWKEY_COLUMN_NUMBER, rowkey_info_->get_size());
	    range.end_key_.assign(mutiple_end_key_objs_ + idx_key_*OB_MAX_ROWKEY_COLUMN_NUMBER, rowkey_info_->get_size());
		if ( idx_key_ >= 0  && idx_key_ < range_count_-1)
		{
			idx_key_++;
			has_next =true;
		}
		else
		{
			idx_key_ = -1;
			has_next =false;
		}
	}
	else
	{
		TBSYS_LOG(WARN, "range count should not be negative or zero ,range count =%ld!", range_count_);
		idx_key_ = -1;
		has_next = false;
		ret = OB_ERR_UNEXPECTED;
	}
	return ret ;
}
//add:e
/**
 *描述: 从主键列开始，从相关条件子句表达式中寻找，相应的range,从第一主键列开始
 * this function
 */
int ObSqlReadStrategy::find_scan_range(ObNewRange &range, bool &found, bool single_row_only)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  uint64_t column_id = OB_INVALID_ID;
  bool found_start = false;
  bool found_end = false;
  OB_ASSERT(NULL != rowkey_info_);
  for (idx = 0; idx < rowkey_info_->get_size(); idx++)
  {
    start_key_objs_[idx].set_min_value();
    end_key_objs_[idx].set_max_value();
  }

  for (idx = 0; idx < rowkey_info_->get_size(); idx++)
  {
    if (OB_SUCCESS != (ret = rowkey_info_->get_column_id(idx, column_id)))
    {
      TBSYS_LOG(WARN, "fail to get column id ret=%d, idx=%ld, column_id=%ld", ret, idx, column_id);
      break;
    }
    else
    {
      if (OB_SUCCESS != (ret = find_closed_column_range(true, idx, column_id, found_start, found_end, single_row_only)))
      {
        TBSYS_LOG(WARN, "fail to find closed column range for column %lu", column_id);
        break;
      }
    }
    if (!found_start || !found_end)
    {
      break; // no more search
    }
  }
  range.start_key_.assign(start_key_objs_, rowkey_info_->get_size());
  range.end_key_.assign(end_key_objs_, rowkey_info_->get_size());

  if (0 == idx && (!found_start) && (!found_end))
  {
    found = false;
  }
  return ret;
}
/**
 * 从条件表达式句中，从rowkey中取出一列，计算出相应主键（单）列所在的range，该函数将废除
 * fyd
 * 2014.3.12
 */
int ObSqlReadStrategy::find_closed_column_range(bool real_val, int64_t idx, uint64_t column_id, bool &found_start, bool &found_end, bool single_row_only)
{
  int ret = OB_SUCCESS;
  int i = 0;
  uint64_t cond_cid = OB_INVALID_ID;
  int64_t cond_op = T_MIN_OP;
  ObObj cond_val;
  ObObj cond_start;
  ObObj cond_end;
  const ObRowkeyColumn *column = NULL;
  found_end = false;
  found_start = false;
  for (i = 0; i < simple_cond_filter_list_.count(); i++)
  {
    if (simple_cond_filter_list_.at(i).is_simple_condition(real_val, cond_cid, cond_op, cond_val))
    {
      if ((cond_cid == column_id) &&
          (NULL != (column = rowkey_info_->get_column(idx))))
      {
        ObObjType target_type = column->type_;
        ObObj expected_type;
        ObObj promoted_obj;
        const ObObj *p_promoted_obj = NULL;
        ObObjType source_type = cond_val.get_type();
        expected_type.set_type(target_type);
        ObString string;
        char *varchar_buff = NULL;
        //这是 目标类型为 varchar 类型，以及表达式类型不匹配，需要强制转换  fyd  2014.3.24
        if (target_type == ObVarcharType && source_type != ObVarcharType)//比较输入类型和定义类型 fyd 2014 .3.12
        {
          if (NULL == (varchar_buff = (char*)ob_malloc(OB_MAX_VARCHAR_LENGTH, ObModIds::OB_SQL_READ_STRATEGY)))
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", OB_MAX_VARCHAR_LENGTH, ret);
          }
          else
          {
            string.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
            promoted_obj.set_varchar(string);
            if (OB_SUCCESS != (ret = obj_cast(cond_val, expected_type, promoted_obj, p_promoted_obj)))
            {
                TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, source_type, target_type);
                ob_free(varchar_buff);
                varchar_buff = NULL;
                break;
            }
            else
            {
              switch (cond_op)
              {
                case T_OP_LT:// 小于
                case T_OP_LE: // 小于等于
                  if (end_key_objs_[idx].is_max_value())
                  {
                    end_key_objs_[idx] = *p_promoted_obj;
                    end_key_mem_hold_[idx] = varchar_buff;
                    found_end = true;
                  }
                  else if (*p_promoted_obj < end_key_objs_[idx])
                  {
                    end_key_objs_[idx] = *p_promoted_obj;
                    if (end_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(end_key_mem_hold_[idx]);
                      end_key_mem_hold_[idx] = varchar_buff;
                    }
                    else
                    {
                      end_key_mem_hold_[idx] = varchar_buff;
                    }
                  }
                  else
                  {
                    ob_free(varchar_buff);
                    varchar_buff = NULL;
                  }
                  break;
                case T_OP_GT:
                case T_OP_GE:
                  if (start_key_objs_[idx].is_min_value())
                  {
                    start_key_objs_[idx] = *p_promoted_obj;
                    found_start = true;
                    start_key_mem_hold_[idx] = varchar_buff;
                  }
                  else if (*p_promoted_obj > start_key_objs_[idx])
                  {
                    start_key_objs_[idx] = *p_promoted_obj;
                    if (start_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(start_key_mem_hold_[idx]);
                      start_key_mem_hold_[idx] = varchar_buff;
                    }
                    else
                    {
                      start_key_mem_hold_[idx] = varchar_buff;
                    }
                  }
                  else
                  {
                    ob_free(varchar_buff);
                    varchar_buff = NULL;
                  }
                  break;
                case T_OP_EQ:
                case T_OP_IS:
                  if (start_key_objs_[idx].is_min_value() && end_key_objs_[idx].is_max_value())
                  {
                    start_key_objs_[idx] = *p_promoted_obj;
                    end_key_objs_[idx] = *p_promoted_obj;
                    // when free, we compare this two address, if equals, then release once
                    start_key_mem_hold_[idx] = varchar_buff;
                    end_key_mem_hold_[idx] = varchar_buff;
                    found_start = true;
                    found_end = true;
                  }
                  else if (start_key_objs_[idx] == end_key_objs_[idx])
                  {
                    if (*p_promoted_obj != start_key_objs_[idx])
                    {
                      TBSYS_LOG(WARN, "two different equal condition on the sanme column, column_id=%lu", column_id);
                    }
                    ob_free(varchar_buff);
                    varchar_buff = NULL;
                  }
                  else
                  {
                    // actually, if the eq condition is not between the previous range, we also can set range using eq condition,in this case,
                    // the scan range is actually a single-get, filter will filter-out the record,
                    // so, here , we set range to a single-get scan uniformly,contact to lide.wd@taobao.com
                    //if (*p_promoted_obj >= start_key_objs_[idx] && *p_promoted_obj <= end_key_objs_[idx])
                    //{
                      start_key_objs_[idx] = *p_promoted_obj;
                      end_key_objs_[idx] = *p_promoted_obj;
                      if (start_key_mem_hold_[idx] != NULL)
                      {
                        ob_free(start_key_mem_hold_[idx]);
                      }
                      start_key_mem_hold_[idx] = varchar_buff;
                      if (end_key_mem_hold_[idx] != NULL)
                      {
                        ob_free(end_key_mem_hold_[idx]);
                      }
                      end_key_mem_hold_[idx] = varchar_buff;
                    //}
                  }
                  break;
                default:
                  ob_free(varchar_buff);
                  varchar_buff = NULL;
                  TBSYS_LOG(WARN, "unexpected cond op: %ld", cond_op);
                  ret = OB_ERR_UNEXPECTED;
                  break;
              }
            }
          }
        }
        else
        {
          if (OB_SUCCESS != (ret = obj_cast(cond_val, expected_type, promoted_obj, p_promoted_obj)))
          {
            TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, source_type, target_type);
            break;
          }
          else
          {
            switch (cond_op)
            {
              case T_OP_LT:
              case T_OP_LE:
                if (end_key_objs_[idx].is_max_value())
                {
                  end_key_objs_[idx] = *p_promoted_obj;
                  found_end = true;
                }
                else
                {
                  if (*p_promoted_obj < end_key_objs_[idx])
                  {
                    end_key_objs_[idx] = *p_promoted_obj;
                    if (end_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(end_key_mem_hold_[idx]);
                    }
                  }
                }
                break;
              case T_OP_GT:
              case T_OP_GE:
                if (start_key_objs_[idx].is_min_value())
                {
                  start_key_objs_[idx] = *p_promoted_obj;
                  found_start = true;
                }
                else
                {
                  if (*p_promoted_obj > start_key_objs_[idx])
                  {
                    start_key_objs_[idx] = *p_promoted_obj;
                    if (start_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(start_key_mem_hold_[idx]);
                    }
                  }
                }
                break;
              case T_OP_EQ:
              case T_OP_IS:
                if (start_key_objs_[idx].is_min_value() && end_key_objs_[idx].is_max_value())
                {
                  start_key_objs_[idx] = *p_promoted_obj;
                  end_key_objs_[idx] = *p_promoted_obj;
                  found_start = true;
                  found_end = true;
                }
                else if (start_key_objs_[idx] == end_key_objs_[idx])
                {
                  if (*p_promoted_obj != start_key_objs_[idx])
                  {
                    TBSYS_LOG(WARN, "two different equal condition on the same column, column_id=%lu"
                        " start_key_objs_[idx]=%s, *p_promoted_obj=%s",
                        column_id, to_cstring(start_key_objs_[idx]), to_cstring(*p_promoted_obj));
                  }
                }
                else
                {
                  // actually, if the eq condition is not between the previous range, we also can set range using eq condition,in this case,
                  // the scan range is actually a single-get, filter will filter-out the record,
                  // so, here , we set range to a single-get scan uniformly,contact to lide.wd@taobao.com
                  //if (*p_promoted_obj >= start_key_objs_[idx] && *p_promoted_obj <= end_key_objs_[idx])
                  //{
                    start_key_objs_[idx] = *p_promoted_obj;
                    end_key_objs_[idx] = *p_promoted_obj;
                    if (start_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(start_key_mem_hold_[idx]);
                    }
                    if (end_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(end_key_mem_hold_[idx]);
                    }
                  //}
                }
                break;
              default:
                TBSYS_LOG(WARN, "unexpected cond op: %ld", cond_op);
                ret = OB_ERR_UNEXPECTED;
                break;
            }
          }
        }
        if (single_row_only && (cond_op != T_OP_EQ && cond_op != T_OP_IS))
        {
          found_end = found_start = false;
        }
      }
    }
    else if ((!single_row_only) && simple_cond_filter_list_.at(i).is_simple_between(real_val, cond_cid, cond_op, cond_start, cond_end))
    {
      if (cond_cid == column_id)
      {
        OB_ASSERT(T_OP_BTW == cond_op);
        column = rowkey_info_->get_column(idx);
        ObObjType target_type;
        if (column == NULL)
        {
          TBSYS_LOG(WARN, "get column from rowkey_info failed, column = NULL");
        }
        else
        {
          target_type = column->type_;
          ObObj expected_type;
          expected_type.set_type(target_type);
          ObObj start_promoted_obj;
          ObString start_string;
          ObString end_string;
          char *varchar_buff = NULL;
          const ObObj *p_start_promoted_obj = NULL;
          ObObj end_promoted_obj;
          const ObObj *p_end_promoted_obj = NULL;
          ObObjType start_source_type = cond_start.get_type();
          ObObjType end_source_type = cond_end.get_type();
          if (target_type == ObVarcharType && start_source_type != ObVarcharType)
          {
            if (NULL == (varchar_buff = (char*)ob_malloc(OB_MAX_VARCHAR_LENGTH, ObModIds::OB_SQL_READ_STRATEGY)))
            {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", OB_MAX_VARCHAR_LENGTH, ret);
            }
            else
            {
              start_string.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
              start_promoted_obj.set_varchar(start_string);
              if (OB_SUCCESS != (ret = obj_cast(cond_start, expected_type, start_promoted_obj, p_start_promoted_obj)))
              {
                TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, start_source_type, target_type);
                ob_free(varchar_buff);
                varchar_buff = NULL;
                break;
              }
              else
              {
                if (start_key_objs_[idx].is_min_value())
                {
                  start_key_objs_[idx] = *p_start_promoted_obj;
                  found_start = true;
                  start_key_mem_hold_[idx] = varchar_buff;
                }
                else if (*p_start_promoted_obj > start_key_objs_[idx])
                {
                  start_key_objs_[idx] = *p_start_promoted_obj;
                  if (start_key_mem_hold_[idx] != NULL)
                  {
                    ob_free(start_key_mem_hold_[idx]);
                    start_key_mem_hold_[idx] = varchar_buff;
                  }
                  else
                  {
                    start_key_mem_hold_[idx] = varchar_buff;
                  }
                }
                else
                {
                  ob_free(varchar_buff);
                  varchar_buff = NULL;
                }
              }
            }
          }
          else
          {
            if (OB_SUCCESS != (ret = obj_cast(cond_start, expected_type, start_promoted_obj, p_start_promoted_obj)))
            {
              TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, start_source_type, target_type);
            }
            else
            {
              if (start_key_objs_[idx].is_min_value())
              {
                start_key_objs_[idx] = *p_start_promoted_obj;
                found_start = true;
              }
              else
              {
                if (*p_start_promoted_obj > start_key_objs_[idx])
                {
                  start_key_objs_[idx] = *p_start_promoted_obj;
                  if (start_key_mem_hold_[idx] != NULL)
                  {
                    ob_free(start_key_mem_hold_[idx]);
                    start_key_mem_hold_[idx] = NULL;
                  }
                }
              }
            }
          }
          varchar_buff = NULL;
          if (OB_SUCCESS == ret)
          {
            if (target_type == ObVarcharType && end_source_type != ObVarcharType)
            {
              if (NULL == (varchar_buff = (char*)ob_malloc(OB_MAX_VARCHAR_LENGTH, ObModIds::OB_SQL_READ_STRATEGY)))
              {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", OB_MAX_VARCHAR_LENGTH, ret);
              }
              else
              {
                end_key_mem_hold_[idx] = varchar_buff;
                end_string.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
                end_promoted_obj.set_varchar(end_string);
                if (OB_SUCCESS != (ret = obj_cast(cond_end, expected_type, end_promoted_obj, p_end_promoted_obj)))
                {
                  TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, end_source_type, target_type);
                  ob_free(varchar_buff);
                  varchar_buff = NULL;
                  break;
                }
                else
                {
                  if (end_key_objs_[idx].is_max_value())
                  {
                    end_key_objs_[idx] = *p_end_promoted_obj;
                    found_end = true;
                    end_key_mem_hold_[idx] = varchar_buff;
                  }
                  else if (*p_end_promoted_obj < end_key_objs_[idx])
                  {
                    end_key_objs_[idx] = *p_end_promoted_obj;
                    if (end_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(end_key_mem_hold_[idx]);
                      end_key_mem_hold_[idx] = varchar_buff;
                    }
                    else
                    {
                      end_key_mem_hold_[idx] = varchar_buff;
                    }
                  }
                  else
                  {
                    ob_free(varchar_buff);
                    varchar_buff = NULL;
                  }
                }
              }
            }
            else
            {
              if (OB_SUCCESS != (ret = obj_cast(cond_end, expected_type, end_promoted_obj, p_end_promoted_obj)))
              {
                TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, end_source_type, target_type);
              }
              else
              {
                if (end_key_objs_[idx].is_max_value())
                {
                  end_key_objs_[idx] = *p_end_promoted_obj;
                  found_end = true;
                }
                else
                {
                  if (*p_end_promoted_obj < end_key_objs_[idx])
                  {
                    end_key_objs_[idx] = *p_end_promoted_obj;
                    if (end_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(end_key_mem_hold_[idx]);
                      end_key_mem_hold_[idx] = NULL;
                    }
                  }
                }
              }
            }
          }
				}
      }
    }

    if (ret != OB_SUCCESS)
    {
      break;
    }
    //if (found_start && found_end)
    //{
      /* we can break earlier here */
      //break;
    //}
  }
  return ret;
}
/**
 * add　& mod   by fyd  IN_EXPR  [PrefixKeyQuery_for_INstmt] 2014.4.8
 */
int ObSqlReadStrategy::find_closed_column_range_simple_con(bool real_val, const ObRowkeyColumn *column, int64_t column_idx, int64_t idx_of_ranges, int64_t cond_op, ObObj cond_val, bool &found_start, bool &found_end, bool single_row_only)
{
    int ret = OB_SUCCESS;
    UNUSED(real_val);
    ObObjType target_type ;
    ObObj expected_type;
    ObObj promoted_obj;
    const ObObj *p_promoted_obj = NULL;
    ObObjType source_type = cond_val.get_type();
    ObString string;
    int64_t idx = idx_of_ranges*OB_MAX_ROWKEY_COLUMN_NUMBER + column_idx ;// 当前的位置
    char *varchar_buff = NULL;

    ////////////////
    bool need_store_space  = false ;
    if ( NULL == column )
    {
  	  ret = OB_ERR_UNEXPECTED;
  	  TBSYS_LOG(ERROR, "column should not be null,ret=%d",ret);
    }
    else if ( idx < 0 || idx >= range_count_ * OB_MAX_ROWKEY_COLUMN_NUMBER)
    {
  	  ret = OB_ERR_UNEXPECTED;
  	  TBSYS_LOG(ERROR, " obj index of ranges out of range ,column idx =%ld,range idx =%ld,ret=%d",column_idx, idx,ret);
    }
    else
    {
  	   target_type = column->type_;
  	   expected_type.set_type(target_type);
       ///
       //step 1
	  //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
        if ((target_type == ObVarcharType && source_type != ObVarcharType)||(target_type == ObDecimalType))
        //modify:e
	  {
		if (NULL == (varchar_buff = (char*)ob_malloc(OB_MAX_VARCHAR_LENGTH, ObModIds::OB_SQL_READ_STRATEGY)))
		{
		  ret = OB_ALLOCATE_MEMORY_FAILED;
		  TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", OB_MAX_VARCHAR_LENGTH, ret);
		}
		else
		{
		  need_store_space = true ;
		  string.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
		  promoted_obj.set_varchar(string);
		  //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
            if(target_type == ObDecimalType)
                ret = obj_cast_for_rowkey(cond_val, expected_type, promoted_obj, p_promoted_obj);
            else
                ret = obj_cast(cond_val, expected_type, promoted_obj, p_promoted_obj);
            //add:e
		  //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
          if (OB_SUCCESS != ret)
            //modify:e
		  {
			TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, source_type, target_type);
			ob_free(varchar_buff);
			varchar_buff = NULL;
			ret = OB_ERR_UNEXPECTED;
		  }
		}
	  }
	  else if (OB_SUCCESS != (ret = obj_cast(cond_val, expected_type, promoted_obj, p_promoted_obj)))
	  {
		  TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, source_type, target_type);
	  }
	  else
	  {
		   need_store_space = false ;
	  }

	  //step 2
	  if ( OB_SUCCESS == ret )
	  {
          switch (cond_op)
          {
            case T_OP_LT:// 小于
            case T_OP_LE: // 小于等于
              if (mutiple_end_key_objs_[idx].is_max_value())
              {
                mutiple_end_key_objs_[idx] = *p_promoted_obj;
                if ( need_store_space )
                   mutiple_end_key_mem_hold_[idx] = varchar_buff;
                found_end = true;
              }
              else if (*p_promoted_obj < mutiple_end_key_objs_[idx])
              {
            	  found_end = true;
            	  mutiple_end_key_objs_[idx] = *p_promoted_obj;
                if (mutiple_end_key_mem_hold_[idx] != NULL)
                {
                  ob_free(mutiple_end_key_mem_hold_[idx]);
                  mutiple_end_key_mem_hold_[idx] = NULL;
                  if (need_store_space )
                   mutiple_end_key_mem_hold_[idx] = varchar_buff;
                }
                else if (need_store_space)
                {
                  mutiple_end_key_mem_hold_[idx] = varchar_buff;
                }
              }
              else if (need_store_space)
              {
                ob_free(varchar_buff);
                varchar_buff = NULL;
              }
              break;
            case T_OP_GT:
            case T_OP_GE:
              if (mutiple_start_key_objs_[idx].is_min_value())
              {
            	mutiple_start_key_objs_[idx] = *p_promoted_obj;
                found_start = true;
                if ( need_store_space )
                  mutiple_start_key_mem_hold_[idx] = varchar_buff;
              }
              else if (*p_promoted_obj > mutiple_start_key_objs_[idx])
              {
            	 found_start = true;
            	 mutiple_start_key_objs_[idx] = *p_promoted_obj;
                if (mutiple_start_key_mem_hold_[idx] != NULL)
                {
                  ob_free(mutiple_start_key_mem_hold_[idx]);
                  mutiple_start_key_mem_hold_[idx] = NULL;
                  if ( need_store_space )
                    mutiple_start_key_mem_hold_[idx] = varchar_buff;
                }
                else if (need_store_space)
                {
                 mutiple_start_key_mem_hold_[idx] = varchar_buff;
                }
              }
              else if (need_store_space)
              {
                ob_free(varchar_buff);
                varchar_buff = NULL;
              }
              break;
            case T_OP_EQ:
            case T_OP_IS:
             // not set value
              if (mutiple_start_key_objs_[idx].is_min_value() && mutiple_end_key_objs_[idx].is_max_value())
              {
            	mutiple_start_key_objs_[idx] = *p_promoted_obj;
            	mutiple_end_key_objs_[idx] = *p_promoted_obj;
                // when free, we compare this two address, if equals, then release once
            	if (need_store_space)
            	{
					mutiple_start_key_mem_hold_[idx] = varchar_buff;
					mutiple_end_key_mem_hold_[idx] = varchar_buff;
            	}
                // found_start = true;
                // found_end = true;
              }
              else if (mutiple_start_key_objs_[idx] == mutiple_end_key_objs_[idx])
              {
            	 //new value  != old value  renew the value
                if (*p_promoted_obj != mutiple_start_key_objs_[idx])
                {
                  TBSYS_LOG(WARN, "two different equal condition on the same column, column_id=%lu", column->column_id_);

                  // release old space
                  if (mutiple_start_key_mem_hold_[idx] == mutiple_end_key_mem_hold_[idx] && NULL != mutiple_start_key_mem_hold_[idx])
                  {
                	  ob_free(mutiple_start_key_mem_hold_[idx]);
                	  mutiple_start_key_mem_hold_[idx] = NULL;
                	  mutiple_end_key_mem_hold_[idx] = NULL;
                  }
                  else
                  {
                	  if (NULL != mutiple_start_key_mem_hold_[idx])
                	  {
                	      ob_free(mutiple_start_key_mem_hold_[idx]);
                	      mutiple_start_key_mem_hold_[idx] = NULL;
                	  }
                	  if (NULL != mutiple_end_key_mem_hold_[idx])
                	  {
                	      ob_free(mutiple_end_key_mem_hold_[idx]);
                	      mutiple_end_key_mem_hold_[idx] = NULL;
                	   }
                  }
                  //specify new value
            	  mutiple_start_key_objs_[idx] = *p_promoted_obj;
            	  mutiple_end_key_objs_[idx] = *p_promoted_obj;
            	  // storage new space
                  if (need_store_space)
                  {
					  mutiple_start_key_mem_hold_[idx] = varchar_buff;
					  mutiple_end_key_mem_hold_[idx] = varchar_buff;
                  }

                }
                // new value  == old value, ignore the new value,release  the new malloced space
                else if (need_store_space)
                {
					ob_free(varchar_buff);
					varchar_buff = NULL;
                }
              }
              else
              {
            	  mutiple_start_key_objs_[idx] = *p_promoted_obj;
            	  mutiple_end_key_objs_[idx] = *p_promoted_obj;
                  if (mutiple_start_key_mem_hold_[idx] != NULL)
                  {
                    ob_free(mutiple_start_key_mem_hold_[idx]);
                    mutiple_start_key_mem_hold_[idx] = NULL;
                  }
                  if (mutiple_end_key_mem_hold_[idx] != NULL)
                  {
                    ob_free(mutiple_end_key_mem_hold_[idx]);
                    mutiple_end_key_mem_hold_[idx] = NULL;
                  }
                  if (need_store_space)
                  {
					  mutiple_start_key_mem_hold_[idx] = varchar_buff;
					  mutiple_end_key_mem_hold_[idx] = varchar_buff;
                  }
              }
			    found_start = true;
                found_end = true;
              break;
            default:
            	if (need_store_space )
            	{
                    ob_free(varchar_buff);
                    varchar_buff = NULL;
            	}
              TBSYS_LOG(WARN, "unexpected cond op: %ld", cond_op);
              ret = OB_ERR_UNEXPECTED;
          }
	  }
	  else
	  {
		  TBSYS_LOG(WARN, "error, ret=%d", ret);
	  }
    ////////////////
    }

    if (single_row_only && ( cond_op != T_OP_EQ && cond_op != T_OP_IS) )//
    {
      found_end = found_start = false;
    }
    varchar_buff = NULL;
    return  ret;
}
//add:e
/**
 * mod &　add　  by fyd IN_EXPR  [PrefixKeyQuery_for_INstmt]  2014.4.8
 */
int ObSqlReadStrategy::find_closed_column_range_simple_btw(bool real_val, const ObRowkeyColumn *column, int64_t column_idx, int64_t idx_of_ranges,/* int64_t cond_op, ObObj cond_val,*/ ObObj cond_start, ObObj cond_end, bool &found_start, bool &found_end, bool single_row_only)
{
   int ret = OB_SUCCESS;
   ObObj expected_type;
   ObObj start_promoted_obj;
   ObString start_string;
   ObString end_string;
   char *varchar_buff = NULL;
   const ObObj *p_start_promoted_obj = NULL;
   ObObj end_promoted_obj;
   const ObObj *p_end_promoted_obj = NULL;
   ObObjType start_source_type = cond_start.get_type();
   ObObjType end_source_type = cond_end.get_type();
   UNUSED(real_val);
   UNUSED(single_row_only);
   int64_t idx = idx_of_ranges*OB_MAX_ROWKEY_COLUMN_NUMBER + column_idx ;// 当前的位置
   bool need_store_space  = false ;
   ObObjType target_type ;
  // TBSYS_LOG(DEBUG, "column idx =%ld,range idx =%ld,obj start:%s,end:%s",column_idx, idx_of_ranges,to_cstring(mutiple_start_key_objs_[idx]), to_cstring(mutiple_end_key_objs_[idx]));
   ///
  if ( NULL == column )
  {
	  ret = OB_ERR_UNEXPECTED;
	  TBSYS_LOG(ERROR, "column should not be null,ret=%d",ret);
  }
  else if ( idx < 0 || idx >= range_count_ * OB_MAX_ROWKEY_COLUMN_NUMBER)
  {
	  ret = OB_ERR_UNEXPECTED;
	  TBSYS_LOG(ERROR, " obj index of ranges out of range ,column idx =%ld,range idx =%ld,ret=%d",column_idx, idx,ret);
  }
  else
  {
	  target_type = column->type_;
	  expected_type.set_type(target_type);
	   //step 1
	  //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
      if ((target_type == ObVarcharType && start_source_type != ObVarcharType)||(target_type == ObDecimalType))
      //modify:e
	  {
		if (NULL == (varchar_buff = (char*)ob_malloc(OB_MAX_VARCHAR_LENGTH, ObModIds::OB_SQL_READ_STRATEGY)))
		{
		  ret = OB_ALLOCATE_MEMORY_FAILED;
		  TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", OB_MAX_VARCHAR_LENGTH, ret);
		}
		else
		{
		  need_store_space = true ;
		  start_string.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
		  start_promoted_obj.set_varchar(start_string);
		  //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
              if(target_type == ObDecimalType)
                  ret = obj_cast_for_rowkey(cond_start, expected_type, start_promoted_obj, p_start_promoted_obj);
              else
                  ret = obj_cast(cond_start, expected_type, start_promoted_obj, p_start_promoted_obj);
              //add:e
		   //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
          if (OB_SUCCESS != ret )
              //modify:e
		  {
			TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, start_source_type, target_type);
			ob_free(varchar_buff);
			varchar_buff = NULL;
			ret = OB_ERR_UNEXPECTED;
		  }
		}
	  }
	  else if (OB_SUCCESS != (ret = obj_cast(cond_start, expected_type, start_promoted_obj, p_start_promoted_obj)))
	  {
		  TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, start_source_type, target_type);
	  }
	  else
	  {
		   need_store_space = false ;
		  // success do nothing
		  //nop
	  }
	  //step2
	  if ( OB_SUCCESS == ret )
	  {
		  if (mutiple_start_key_objs_[idx].is_min_value())
		  {
			mutiple_start_key_objs_[idx] = *p_start_promoted_obj;
			found_start = true;
			if ( need_store_space )
				mutiple_start_key_mem_hold_[idx] = varchar_buff;
		  }
		  else if (*p_start_promoted_obj > mutiple_start_key_objs_[idx])
		  {
			  found_start = true;
			 mutiple_start_key_objs_[idx] = *p_start_promoted_obj;
			if (mutiple_start_key_mem_hold_[idx] != NULL)
			{
			  ob_free(mutiple_start_key_mem_hold_[idx]);
			  mutiple_start_key_mem_hold_[idx] = NULL;
			  if ( need_store_space )
				  mutiple_start_key_mem_hold_[idx] = varchar_buff;
			}
			else if (need_store_space)
			{
			 mutiple_start_key_mem_hold_[idx] = varchar_buff;
			}
		  }
		  else if ( need_store_space )
		  {
			ob_free(varchar_buff);
			varchar_buff = NULL;
		  }
	  }
	  else
	  {
		  TBSYS_LOG(WARN, "error, ret=%d", ret);
	  }

	  // step 3
	  varchar_buff = NULL;
	  need_store_space = false ;
	  if ( OB_SUCCESS != ret )
	  {
		  TBSYS_LOG(WARN, "error, ret=%d", ret);
	  }
	 //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
      else if ((target_type == ObVarcharType && end_source_type != ObVarcharType)||(target_type == ObDecimalType))
            //modify:e
	  {
		if (NULL == (varchar_buff = (char*)ob_malloc(OB_MAX_VARCHAR_LENGTH, ObModIds::OB_SQL_READ_STRATEGY)))
		{
		  ret = OB_ALLOCATE_MEMORY_FAILED;
		  TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", OB_MAX_VARCHAR_LENGTH, ret);
		}
		else
		{
		  need_store_space = true ;
		  end_string.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
		  end_promoted_obj.set_varchar(end_string);
		  //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
                if(target_type == ObDecimalType)
                    ret = obj_cast_for_rowkey(cond_end, expected_type, end_promoted_obj, p_end_promoted_obj);
                else
                    ret = obj_cast(cond_end, expected_type, end_promoted_obj, p_end_promoted_obj);
                //add:e
          //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
		  if (OB_SUCCESS != ret)
            //modify:e
		  {
			TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, end_source_type, target_type);
			ob_free(varchar_buff);
			varchar_buff = NULL;
			ret = OB_ERR_UNEXPECTED;
		  }
		}
	  }
	  else if (OB_SUCCESS != (ret = obj_cast(cond_end, expected_type, end_promoted_obj, p_end_promoted_obj)))
	  {
		  TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, end_source_type, target_type);
	  }
	  else
	  {
		   need_store_space = false ;
		  // success do nothing
		  //nop
	  }
	  //step 4
	  if ( OB_SUCCESS == ret )
	  {
		  if (mutiple_end_key_objs_[idx].is_max_value())
		  {
			mutiple_end_key_objs_[idx] = *p_end_promoted_obj;
			found_end = true;
			if ( need_store_space )
				mutiple_end_key_mem_hold_[idx] = varchar_buff;
		  }
		  else if (*p_end_promoted_obj < mutiple_end_key_objs_[idx])
		  {
			found_end = true;
			mutiple_end_key_objs_[idx] = *p_end_promoted_obj;
			if (mutiple_end_key_mem_hold_[idx] != NULL)
			{
			  ob_free(mutiple_end_key_mem_hold_[idx]);
			  mutiple_end_key_mem_hold_[idx] = NULL;
			  if (need_store_space)
				  mutiple_end_key_mem_hold_[idx] = varchar_buff;
			}
			else if (need_store_space)
			{
			  mutiple_end_key_mem_hold_[idx] = varchar_buff;
			}
		  }
		  else if (need_store_space)
		  {
			ob_free(varchar_buff);
			varchar_buff = NULL;
		  }
	  }
	  else
	  {
		  TBSYS_LOG(WARN, "error, ret=%d", ret);
	  }
  }
   return  ret;
}
//add:e
/**
 *  added  by fyd  IN_EXPR  [PrefixKeyQuery_for_INstmt] 2014.4.8
 *  计算出该列的所有range
 *  Bug:暂时没有对多个range 进行优化，这个优化后期可以在sort_mitiple_range 中实现。
 *  fyd  2014.4.9
 *  single IN EXPR  only , if one more in expr ,we just ignore the others,fix me
 *  FixedBug : 在当前函数，如果idx（列在rowkeyinfo中的index）对应的列 和column_id 不一致，当前函数不会做检查  20140605
 */
int ObSqlReadStrategy::find_closed_column_range_ex(bool real_val, int64_t idx, uint64_t column_id, bool &found_start, bool &found_end, bool single_row_only)
{
  int ret = OB_SUCCESS;
  int i = 0;
  uint64_t cond_cid = OB_INVALID_ID;
  int64_t cond_op = T_MIN_OP;
  ObObj cond_val;
  ObObj cond_start;
  ObObj cond_end;
  const ObRowkeyColumn *column = NULL;
  found_end = false;
  found_start = false;

  if ( NULL != rowkey_info_)
  {
	  // 如果有IN， 这里我们先解析IN 中前缀条件，然后再解析 其他条件
	  //
	  if (simple_in_filter_list_.count() && !single_row_only )
	  {
		  ObArray<ObRowkey> rowkey_array;
		  common::PageArena<ObObj,common::ModulePageAllocator> rowkey_objs_allocator(PageArena<ObObj, ModulePageAllocator>::DEFAULT_PAGE_SIZE,ModulePageAllocator(ObModIds::OB_SQL_READ_STATEGY));
		  if ( NULL == (column = rowkey_info_->get_column(idx)))
		  {
			  ret = OB_ERR_UNEXPECTED;
			  TBSYS_LOG(WARN, "fail to get rowkey  from rowkey info ,ret=%d", ret);
		  } else if (  column->column_id_ != column_id )
		  {
			  ret = OB_ERR_UNEXPECTED;
			  TBSYS_LOG(WARN, "column_id do not match idx  ,ret=%d", ret);
		  }
		  else
		  {
			  for ( i = 0; i < simple_in_filter_list_.count() && ret == OB_SUCCESS; i++ )
			  {
				 if ( simple_in_filter_list_.at(i).is_in_expr_with_ex_rowkey(real_val, *rowkey_info_, rowkey_array, rowkey_objs_allocator))
				 {
						if ( 0 >= rowkey_array.count() )
						{
							TBSYS_LOG(WARN, "fail to get rowkey prefix info   from in expr  ,ret=%d", ret);
							continue;
						}
						else if ( range_count_ <= 0  )
						{
						  if ( (ret = malloc_rowkey_objs_space(rowkey_array.count())) != OB_SUCCESS )
						  {
							  TBSYS_LOG(ERROR, "fail to malloc space for rowkey ranges ret=%d", ret);
							  break;
						  }
						  else
						  {
							 //这里按照需要对部分进行初始化，第一个 range的start_key,end_key 最开始的时候已经完成了初始化
							  for ( int k = 0; k < rowkey_array.count() ; k++ )
							  {
								  for ( int j = 0; j < rowkey_info_->get_size(); j++ )
								  {
									  (mutiple_start_key_objs_ + k * OB_MAX_ROWKEY_COLUMN_NUMBER + j)->set_min_value();
									  (mutiple_end_key_objs_ + k * OB_MAX_ROWKEY_COLUMN_NUMBER + j)->set_max_value();

								  }
							  }
						  }
						}
						break;
				 }
			  }//end?for ( i = 0; i < simple_in_filter_list_.count() && ret == OB_SUCCESS; i++ )

			  ////说明该主键仍然属于指定的主键前缀的内容,在多IN 条件中
			  if ( OB_SUCCESS == ret && rowkey_array.count() > 0 && rowkey_array.at(0).get_obj_cnt() > idx )
			  {
				  for ( int rowkey_i = 0;rowkey_i < rowkey_array.count() && ret == OB_SUCCESS; rowkey_i++ )
				  {
						cond_val = *(rowkey_array.at(rowkey_i).ptr() + idx);
						TBSYS_LOG(DEBUG, "column idx=%ld, row idx = %d,write position = %ld,val=%s,prefx rk size = %ld,row num = %ld,current range idx = %d",
								idx,rowkey_i,rowkey_i*OB_MAX_ROWKEY_COLUMN_NUMBER+idx,to_cstring(cond_val),
								rowkey_array.at(0).get_obj_cnt() ,rowkey_array.count(), rowkey_i );
						// 这里只针对具体的一个主键列的具体一个值，idx表示在 rowkey info中 具体一列的 index
						ret = resolve_close_column_range_in(real_val, column, cond_val, idx, rowkey_i ,found_start, found_end, single_row_only);
//						TBSYS_LOG(INFO, "range idx = %d,ret=%d", rowkey_i, ret);
				 }
				// currently, we just handle the first in_expr,and ignore the others,so we just break after we analysis the first in expr successfully !
				if (ret != OB_SUCCESS)
				{
					TBSYS_LOG(WARN, "resolve  in expr for ranges error, ret=%d", ret);
				} else
				{
					TBSYS_LOG(DEBUG,
							"total in expr = %ld ,break at %d in expr = %s  ret=%d",
							simple_in_filter_list_.count(), i,
							to_cstring(simple_in_filter_list_.at(i)), ret);
				}

			  }

		  }//end?if (simple_in_filter_list_.count())

		  rowkey_objs_allocator.free();
	  }
	//////
	  if (range_count_  <= 0 )
	  {
		 // 重试10次,必须保证至少有一个range
		 int  repeat = 10;
		 while ( OB_SUCCESS != ( ret =  malloc_rowkey_objs_space(1)) && repeat-- )
		 {
			 TBSYS_LOG(WARN, "malloc  range space failed!,repeat = %d,ret =%d",repeat, ret);
		 }
		 if ( OB_SUCCESS == ret )
		 {
			  for ( i = 0; i < rowkey_info_->get_size(); i++)
			  {
				(mutiple_start_key_objs_+i)->set_min_value();
				(mutiple_end_key_objs_+i)->set_max_value();
			  }
			 TBSYS_LOG(DEBUG, "malloc  range space successed!");
		 }
		 else
		 {
			 // 如果开辟失败了，需要增加处理代码，后序处理都会失败
			 // do something....
		 }
	  }
	///////
	  if ( NULL == (column = rowkey_info_->get_column(idx)))
	  {
		  ret = OB_ERR_UNEXPECTED;
		  TBSYS_LOG(WARN, "rowkey column is null,the parameter should be checked! cid=%ld,ret=%d", column_id, ret );
	  }
	  else
	  {
		  for (i = 0; i < simple_cond_filter_list_.count() && ret == OB_SUCCESS; i++)
		  {
			if (simple_cond_filter_list_.at(i).is_simple_condition(real_val, cond_cid, cond_op, cond_val) && cond_cid == column_id )
			{
				  for ( int64_t range_index = 0; range_index < range_count_ && ret == OB_SUCCESS ;range_index ++)
				  {
					 if( (ret = find_closed_column_range_simple_con(real_val, column, idx, range_index, cond_op, cond_val, found_start, found_end, single_row_only)) != OB_SUCCESS)
					 {
						 TBSYS_LOG(WARN, "construct range = %ld,column = %ld  failed!", range_index, idx);
					 }
				  }
			}
			else if ((!single_row_only) && simple_cond_filter_list_.at(i).is_simple_between(real_val, cond_cid, cond_op, cond_start, cond_end)  && cond_cid == column_id)
			{
				  OB_ASSERT(T_OP_BTW == cond_op);
				  for ( int64_t range_index = 0; range_index <  range_count_ && ret == OB_SUCCESS ;range_index ++)
				  {
					 // if( (ret = find_closed_column_range_simple_btw(real_val, column, idx, range_index, cond_start, cond_end, found_start, found_end, single_row_only)) != OB_SUCCESS)
					 // {
						 // TBSYS_LOG(WARN, "construct range = %ld,column = %ld  failed!",range_index, idx);
					 // }
                    /*if (cond_start.compare(cond_end) > 0 )
					 {
						 if( (ret = find_closed_column_range_simple_btw(real_val, column, idx, range_index, cond_end, cond_start, found_end,found_start, single_row_only)) != OB_SUCCESS)
						 {
							 TBSYS_LOG(WARN, "construct range = %ld,column = %ld  failed!",range_index, idx);
						 }
					 }else*/
	//				 {
						 if( (ret = find_closed_column_range_simple_btw(real_val, column, idx, range_index, cond_start, cond_end, found_start, found_end, single_row_only)) != OB_SUCCESS)
						 {
							 TBSYS_LOG(WARN, "construct range = %ld,column = %ld  failed!",range_index, idx);
						 }
//					 }
				  }
			}
		  }// for (i = 0; i < simple_cond_filter_list_.count(); i++)
	  }

  }
  else
  {
	  ret = OB_ERR_UNEXPECTED;
	  TBSYS_LOG(WARN, "rowkey_info_ should not be null, ret = %d!",ret);
  }
  return ret;
}
//add:e

/**add fyd  IN_EXPR  [PrefixKeyQuery_for_INstmt] 2014.3.27
 *  说明：仅仅处理了具体某一列的 range， 该函数主要用来处理IN 子句多 range 问题
 *  idx_of_ranges  定位了具体rowkwey range 的具体某一列的位置
 *  如果当前列已经由了值，则忽略当前值
 */
int  ObSqlReadStrategy::resolve_close_column_range_in(bool real_val, const ObRowkeyColumn *column, ObObj cond_val, int64_t column_idx, int64_t idx_of_ranges, bool &found_start, bool &found_end, bool single_row_only)
{
	//return find_closed_column_range_simple_btw(real_val, column, column_idx, idx_of_ranges, cond_val, cond_val, found_start, found_end, single_row_only);
    UNUSED(real_val);
    UNUSED(single_row_only);
	int ret = OB_SUCCESS;
	ObObjType target_type ;
	ObObj expected_type;
	ObObj promoted_obj;
	const ObObj *p_promoted_obj = NULL;
	ObObjType source_type = cond_val.get_type();
	ObString string;
	char *varchar_buff = NULL;
	int64_t current_index = idx_of_ranges*OB_MAX_ROWKEY_COLUMN_NUMBER + column_idx ;// 当前的位置

	////////////////
	 bool need_store_space  = false ;
	  if ( NULL == column )
	  {
		  ret = OB_ERR_UNEXPECTED;
		  TBSYS_LOG(ERROR, "column should not be null,ret=%d",ret);
	  }
	  else if ( current_index < 0 || current_index >= range_count_ * OB_MAX_ROWKEY_COLUMN_NUMBER)
	  {
		  ret = OB_ERR_UNEXPECTED;
		  TBSYS_LOG(ERROR, " obj index of ranges out of range ,column idx =%ld,range idx =%ld,ret=%d",column_idx, current_index,ret);
	  }
	  else
	  {
		   target_type = column->type_;
		   expected_type.set_type(target_type);
		   ///
		   //step 1
		   //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
		  if ((target_type == ObVarcharType && source_type != ObVarcharType)||(target_type == ObDecimalType))
            //modify:e
		  {
			if (NULL == (varchar_buff = (char*)ob_malloc(OB_MAX_VARCHAR_LENGTH, ObModIds::OB_SQL_READ_STRATEGY)))
			{
			  ret = OB_ALLOCATE_MEMORY_FAILED;
			  TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", OB_MAX_VARCHAR_LENGTH, ret);
			}
			else
			{
			  need_store_space = true ;
			  string.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
			  promoted_obj.set_varchar(string);
          //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
            if(target_type == ObDecimalType)
                ret = obj_cast_for_rowkey(cond_val, expected_type, promoted_obj, p_promoted_obj);
            else
                ret = obj_cast(cond_val, expected_type, promoted_obj, p_promoted_obj);
            //add:e
            //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
			  if (OB_SUCCESS != ret )
            //modify:e
			  {
				TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, source_type, target_type);
				ob_free(varchar_buff);
				varchar_buff = NULL;
				ret = OB_ERR_UNEXPECTED;
			  }
			}
		  }
		  else if (OB_SUCCESS != (ret = obj_cast(cond_val, expected_type, promoted_obj, p_promoted_obj)))
		  {
			  TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, source_type, target_type);
		  }
		  else
		  {
			   need_store_space = false ;
		  }
		  //step 2
		  if ( OB_SUCCESS == ret )
		  {
			found_start = found_end = true;
			if (mutiple_start_key_objs_[ current_index ] == mutiple_end_key_objs_[ current_index ])
			{
				 TBSYS_LOG(WARN, "duplicate value for the same column, ret=%d", ret);
				 if ( need_store_space )
				 {
					 ob_free(varchar_buff);
					 varchar_buff = NULL;
				 }
			}
			else
			{
				if ((mutiple_end_key_objs_ + current_index)->is_max_value())
				{
				}
				else if (mutiple_end_key_mem_hold_ [current_index] != NULL )
				{
					  ob_free(mutiple_end_key_mem_hold_ [ current_index]);
					  mutiple_end_key_mem_hold_ = NULL;
				}
				*(mutiple_end_key_objs_ + current_index) = *p_promoted_obj;
				if ( need_store_space )
					mutiple_end_key_mem_hold_[current_index] = varchar_buff;


				if ((mutiple_start_key_objs_ + current_index)->is_min_value())
				{
				}
				else if ( mutiple_start_key_mem_hold_[ current_index] != NULL)
				{
					ob_free(mutiple_start_key_mem_hold_ [ current_index ] );
					mutiple_start_key_mem_hold_ [ current_index ] = NULL;
				}
				*(mutiple_start_key_objs_ + current_index) = *p_promoted_obj;
				if ( need_store_space )
					mutiple_start_key_mem_hold_[current_index] = varchar_buff;
			}
		  }
		  else
		  {
			  TBSYS_LOG(WARN, "error, ret=%d", ret);
		  }

		/////////////
	  }
	return  ret;
}
//add:e

int ObSqlReadStrategy::add_filter(const ObSqlExpression &expr)
{
  int ret = OB_SUCCESS;
  uint64_t cid = OB_INVALID_ID;
  int64_t op = T_INVALID;
  ObObj val1;
  ObObj val2;

  if (expr.is_simple_condition(false, cid, op, val1))
  {
    // TBSYS_LOG(DEBUG, "simple condition [%s]", to_cstring(expr));
    if (OB_SUCCESS != (ret = simple_cond_filter_list_.push_back(expr)))
    {
      TBSYS_LOG(WARN, "fail to add simple filter. ret=%d", ret);
    }
  }
  else if (expr.is_simple_between(false, cid, op, val1, val2))
  {
    // TBSYS_LOG(DEBUG, "simple between condition [%s]", to_cstring(expr));
    if (OB_SUCCESS != (ret = simple_cond_filter_list_.push_back(expr)))
    {
      TBSYS_LOG(WARN, "fail to add simple filter. ret=%d", ret);
    }
  }
  //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715:b
  /*Exp:store sub_query expression*/
  else if (expr.is_in_sub_query_expr(*rowkey_info_))
  {
    //mod dyr [Insert_Subquery_Function] [test] 20151029:b
//    if (OB_SUCCESS != (ret = in_sub_query_filter_list_.push_back(expr)))
//    {
//      TBSYS_LOG(WARN, "fail to add in sub query filter. ret=%d", ret);
//    }
    if (OB_SUCCESS != (ret = simple_in_filter_list_.push_back(expr)))
    {
      TBSYS_LOG(WARN, "fail to add in sub query filter. ret=%d", ret);
    }
    else
    {
      in_sub_query_idx_ = simple_in_filter_list_.count() - 1;
    }
    //mod dyr 20151029:e
  }
  //add 20140715:e
  else
  {
    ObArray<ObRowkey> rowkey_array;
    common::PageArena<ObObj,common::ModulePageAllocator> rowkey_objs_allocator(
        PageArena<ObObj, ModulePageAllocator>::DEFAULT_PAGE_SIZE,ModulePageAllocator(ObModIds::OB_SQL_READ_STATEGY));
    // modified  by fyd IN_EXPR  [PrefixKeyQuery_for_INstmt]  2014.3.27
    //del:b
//    if (true == expr.is_simple_in_expr(false, *rowkey_info_, rowkey_array, rowkey_objs_allocator))//  modified   2014.3.24
//    {
//      // TBSYS_LOG(DEBUG, "simple in expr [%s]", to_cstring(expr));
//      if (OB_SUCCESS != (ret = simple_in_filter_list_.push_back(expr)))
//      {
//        TBSYS_LOG(WARN, "fail to add simple filter. ret=%d", ret);
//      }
    //del:e
    if (true == expr.is_in_expr_with_ex_rowkey(false, *rowkey_info_, rowkey_array, rowkey_objs_allocator))//  modified   2014.3.24
    {
      // TBSYS_LOG(DEBUG, "simple in expr [%s]", to_cstring(expr));
      if (OB_SUCCESS != (ret = simple_in_filter_list_.push_back(expr)))
      {
        TBSYS_LOG(WARN, "fail to add simple filter. ret=%d", ret);
      }
      // 主动释放申请的内存
      rowkey_objs_allocator.free();
      //mod:e
    }
  }

  return ret;
}

// 这个函数的功能是从表达式获取一个主键值，但是在支持IN 后，会带来一个潜在问题，但是逻辑结果出现错误。
// 这里会默认到IN 以及其他所有表达式中去寻找主键，所有如果需要获取一个正确的主键值，需要手动对IN表达式进行清空,
// 例如，(c1,c2) in ((1,2),(3,4)) and c1=1 and c2=2 and c3=4，主键为c1,c2,c3,尽管得不到主键值（1,2,4） 但是逻辑结果不会出现错误。
// 这是潜在的bug
//  fyd [PrefixKeyQuery_for_INstmt] 2014.5.4 注释
// 已经修复
// fyd 2014.5.8
int ObSqlReadStrategy::find_rowkeys_from_equal_expr(bool real_val, ObIArray<ObRowkey> &rowkey_array, PageArena<ObObj,common::ModulePageAllocator> &objs_allocator)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  uint64_t column_id = OB_INVALID_ID;
  bool found = false;
  UNUSED(objs_allocator);
  OB_ASSERT(rowkey_info_->get_size() <= OB_MAX_ROWKEY_COLUMN_NUMBER);
  //modfify  fyd [PrefixKeyQuery_for_INstmt] 2014.5.4:b
//  for (idx = 0; idx < rowkey_info_->get_size(); idx++)
//  {
//    start_key_objs_[idx].set_min_value();
//    end_key_objs_[idx].set_max_value();
//  }
  //mod:e
  for (idx = 0; idx < rowkey_info_->get_size(); idx++)
  {
    if (OB_SUCCESS != (ret = rowkey_info_->get_column_id(idx, column_id)))
    {
      TBSYS_LOG(WARN, "fail to get column id. idx=%ld, ret=%d", idx, ret);
      break;
    }
    else if (OB_SUCCESS != (ret = find_single_column_range(real_val, idx, column_id, found)))// 从表达式中找出指定列的range，如果找到了返回found =true  fyd 2014.3.24
    {
      TBSYS_LOG(WARN, "fail to find closed range for column %lu", column_id);
      break;
    }
    else if (!found)
    {
      break; // no more search
    }
  }/* end for */
  if (OB_SUCCESS == ret && found && idx == rowkey_info_->get_size())
  //如果主键列都存在在条件子句中，就将start_key_objs_加入到 rowkey_array  fyd  2014.3.12
  {
    ObRowkey rowkey;
    //modfify  fyd [PrefixKeyQuery_for_INstmt] 2014.5.4:b
//    rowkey.assign(start_key_objs_, rowkey_info_->get_size());
    if ( range_count_ > 0 )
    {
        rowkey.assign(mutiple_start_key_objs_, rowkey_info_->get_size());
    }
    else
    {
    	ret = OB_ERR_UNEXPECTED;
    	TBSYS_LOG(WARN, "range_count_ should be positive, ret=%d", ret);
    }
    //mod:e
    if (OB_SUCCESS != (ret = rowkey_array.push_back(rowkey)))
    {
      TBSYS_LOG(WARN, "fail to push rowkey to list. rowkey=%s, ret=%d", to_cstring(rowkey), ret);
    }
  }
  return ret;
}

/**
 * GET 时，该函数是从IN 表达式构建 rowkey ，所以针对主键前缀，要具体考虑这个函数：要将全主键和前缀分开处理
 * 扶元地 2014.3.27
 * 补充：已经确定了GET,确定SCAN 或是GET，需要调用此函数  ；OB 已经知道了可以确定出rowkey，函数需要修正 是否包含全主键的判断方式。
 * 扶元地  2014.3.29
 */
int ObSqlReadStrategy::find_rowkeys_from_in_expr(bool real_val, ObIArray<ObRowkey> &rowkey_array, common::PageArena<ObObj,common::ModulePageAllocator> &objs_allocator)
{
  int ret = OB_SUCCESS;
  bool is_in_expr_with_rowkey = false;
  int i = 0;
  //  目前仅仅支持一个IN 表达式  扶元地 2014.3.29
  //如果是全主键，写到一个IN 中，否则也是全表扫描--fyd
  if (simple_in_filter_list_.count() > 1)
  {
    TBSYS_LOG(DEBUG, "simple in filter count[%ld]", simple_in_filter_list_.count());
    ret = OB_SUCCESS;
  }
  else
  {
    for (i = 0; i < simple_in_filter_list_.count(); i++)
    {
      // assume rowkey in sequence and all rowkey columns present
      if (false == (is_in_expr_with_rowkey = simple_in_filter_list_.at(i).is_simple_in_expr(real_val, *rowkey_info_, rowkey_array, objs_allocator)))
      {
        TBSYS_LOG(DEBUG, "fail to get rowkey(s) from in expression. ret=%d", ret);
        //added  fyd [PrefixKeyQuery_for_INstmt] 2014.5.4
         rowkey_array.clear();
        //add:e
      }
      else
      {
        TBSYS_LOG(DEBUG, "simple in expr rowkey_array count = %ld", rowkey_array.count());
      }
    }
  }
  // cast rowkey if needed
  if (OB_SUCCESS == ret && true == is_in_expr_with_rowkey)
  {
    char *in_rowkey_buf = NULL;
    int64_t total_used_buf_len = 0;
    int64_t used_buf_len = 0;
    int64_t rowkey_idx = 0;
    for (rowkey_idx = 0; rowkey_idx < rowkey_array.count(); rowkey_idx++)
    {
      bool need_buf = false;
      if (OB_SUCCESS != (ret = ob_cast_rowkey_need_buf(*rowkey_info_, rowkey_array.at(rowkey_idx), need_buf)))
      {
        TBSYS_LOG(WARN, "err=%d", ret);
      }
      else if (need_buf)
      {
        if (NULL == in_rowkey_buf)
        {
          in_rowkey_buf = (char*)objs_allocator.alloc(OB_MAX_ROW_LENGTH);
        }
        if (NULL == in_rowkey_buf)
        {
          TBSYS_LOG(ERROR, "no memory");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
      }
      if (OB_LIKELY(OB_SUCCESS == ret))
      {
        if (OB_MAX_ROW_LENGTH <= total_used_buf_len)
        {
          TBSYS_LOG(WARN, "rowkey has too much varchar. len=%ld", total_used_buf_len);
        }
        else if (OB_SUCCESS != (ret = ob_cast_rowkey(*rowkey_info_, rowkey_array.at(rowkey_idx),
                in_rowkey_buf, OB_MAX_ROW_LENGTH - total_used_buf_len, used_buf_len)))
        {
          TBSYS_LOG(WARN, "failed to cast rowkey, err=%d", ret);
        }
        else
        {
          total_used_buf_len = used_buf_len;
          in_rowkey_buf += used_buf_len;
        }
      }
    }
  }
  return ret;
}

int ObSqlReadStrategy::get_read_method(ObIArray<ObRowkey> &rowkey_array, PageArena<ObObj,common::ModulePageAllocator> &rowkey_objs_allocator, int32_t &read_method)
{
  // 从这里确定的查询方法，
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = find_rowkeys_from_in_expr(false, rowkey_array, rowkey_objs_allocator)))
    {
      TBSYS_LOG(WARN, "fail to find rowkeys in IN operator. ret=%d", ret);
    }
     // 在OB 的代码中，rowkey_array.count() > 0 如果 不是全主键的IN 表达式 在add_filter中已经被过滤掉，所以，如果返回的 count >0  就表示是全主键
    //  fyd  2014.3.31
     // todo
    else if (rowkey_array.count() > 0 /*&& ((ObRowkey)rowkey_array.at(0)).get_obj_cnt() ==  rowkey_info_->get_size() */) // modified by fyd  2014.3.31
    {
      read_method = USE_GET;
    }
    else if (OB_SUCCESS != (ret = find_rowkeys_from_equal_expr(false, rowkey_array, rowkey_objs_allocator)))
    {
      TBSYS_LOG(WARN, "fail to find rowkeys in equal where operator. ret=%d", ret);
    }
    else if (rowkey_array.count() == 1)
    {
      read_method = USE_GET;
    }
    else
    {
      read_method = USE_SCAN;
    }
  }
  return ret;
}


void ObSqlReadStrategy::destroy()
{
  for (int i = 0 ;i < OB_MAX_ROWKEY_COLUMN_NUMBER; ++i)
  {
	if (start_key_mem_hold_[i] != NULL && end_key_mem_hold_[i] != NULL && start_key_mem_hold_[i] == end_key_mem_hold_[i])
	{
	  // release only once on the same memory block
	  ob_free(start_key_mem_hold_[i]);
	  start_key_mem_hold_[i] = NULL;
	  end_key_mem_hold_[i] = NULL;
	}
	else
	{
	  if (start_key_mem_hold_[i] != NULL)
	  {
		ob_free(start_key_mem_hold_[i]);
		start_key_mem_hold_[i] = NULL;
	  }
	  if (end_key_mem_hold_[i] != NULL)
	  {
		ob_free(end_key_mem_hold_[i]);
		end_key_mem_hold_[i] = NULL;
	  }
	}
  }
   //add by fyd IN_EXPR  [PrefixKeyQuery_for_INstmt] 2014.4.8
  // 释放为多rowkkey新开辟的空间  扶元地 2014.3.31
   release_rowkey_objs();
   //add:e
   simple_cond_filter_list_.clear();//add lbzhong[Update rowkey] 20160106:b:e
   simple_in_filter_list_.clear();//add dolphin fix sql_expression leak

}

int ObSqlReadStrategy::assign(const ObSqlReadStrategy *other, ObPhyOperator *owner_op)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObSqlReadStrategy);
  reset();
  for (int64_t i = 0; ret == OB_SUCCESS && i < o_ptr->simple_in_filter_list_.count(); i++)
  {
    if ((ret = simple_in_filter_list_.push_back(o_ptr->simple_in_filter_list_.at(i))) == OB_SUCCESS)
    {
      if (owner_op)
      {
        simple_in_filter_list_.at(i).set_owner_op(owner_op);
      }
    }
    else
    {
      break;
    }
  }
  for (int64_t i = 0; ret == OB_SUCCESS && i < o_ptr->simple_cond_filter_list_.count(); i++)
  {
    if ((ret = simple_cond_filter_list_.push_back(o_ptr->simple_cond_filter_list_.at(i))) == OB_SUCCESS)
    {
      if (owner_op)
      {
        simple_cond_filter_list_.at(i).set_owner_op(owner_op);
      }
    }
    else
    {
      break;
    }
  }
  return ret;
}

int64_t ObSqlReadStrategy::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ReadStrategy(in_filter=");
  pos += simple_in_filter_list_.to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, ", cond_filter=");
  pos += simple_cond_filter_list_.to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, ")\n");
  return pos;
}
// added by fyd  IN_EX [PrefixKeyQuery_for_INstmt] 2014.3.25:b
int  ObSqlReadStrategy::release_rowkey_objs()
       {
       	int ret = OB_SUCCESS;
            //OB可能不需要自己主动去释放内存。
       	//step1
		  //mod peiouya IN_EXPR  [PrefixKeyQuery_for_INstmt] 20140930:b
		  //for ( int i = 0 ;i < OB_MAX_ROWKEY_COLUMN_NUMBER*range_count_; i++ )
		  for ( int i = 0 ;i < OB_MAX_ROWKEY_COLUMN_NUMBER*range_count_cons_; i++ )
		  //mod 20140930:e
		  {
			if (mutiple_start_key_mem_hold_[i] != NULL && mutiple_end_key_mem_hold_[i] != NULL && mutiple_start_key_mem_hold_[i] == mutiple_end_key_mem_hold_[i])
			{
			  // release only once on the same memory block
			  ob_free(mutiple_start_key_mem_hold_[i]);
			  mutiple_start_key_mem_hold_[i] = NULL;
			  mutiple_end_key_mem_hold_[i] = NULL;
			}
			else
			{
			  if (mutiple_start_key_mem_hold_[i] != NULL)
			  {
				ob_free(mutiple_start_key_mem_hold_[i]);
				mutiple_start_key_mem_hold_[i] = NULL;
			  }
			  if (mutiple_end_key_mem_hold_[i] != NULL)
			  {
				ob_free(mutiple_end_key_mem_hold_[i]);
				mutiple_end_key_mem_hold_[i] = NULL;
			  }
			}
		  }
		  // step 2
		TBSYS_LOG(DEBUG, "release the mem ,range_count_ = %ld,range_count_cons_ = %ld", range_count_, range_count_cons_);
		//mod peiouya IN_EXPR  [PrefixKeyQuery_for_INstmt] 20140930:b
       	//if ( range_count_ >= 1 )
		if ( range_count_cons_ >= 1 )
		//mod 20140930:e
       	{
       		TBSYS_LOG(DEBUG, "release the mem ,range = %ld start%d", range_count_, ret);

       		if (NULL != mutiple_start_key_objs_)
       		{
       			ob_free(mutiple_start_key_objs_ );
       			mutiple_start_key_objs_ = NULL;
       		}
       		if (NULL != mutiple_end_key_objs_)
       		{
       			ob_free(mutiple_end_key_objs_ );
       			mutiple_end_key_objs_ = NULL;
       		}
       		if( NULL != mutiple_start_key_mem_hold_)
       		{
       			ob_free(mutiple_start_key_mem_hold_ );
       			mutiple_start_key_mem_hold_ = NULL;
       		}
          		if( NULL != mutiple_end_key_mem_hold_)
           	{
          			ob_free(mutiple_end_key_mem_hold_ );
           		mutiple_end_key_mem_hold_ = NULL;
           	}
          	range_count_ = 0;
			//add peiouya IN_EXPR  [PrefixKeyQuery_for_INstmt] 20140930:b
			range_count_cons_ = 0;
			//add 20140930:e
          	idx_key_ = 0;
          	TBSYS_LOG(DEBUG, "release the mem ,range = %ld end%d", range_count_, ret);
       	}
       	return ret ;

       }
       int ObSqlReadStrategy::malloc_rowkey_objs_space( int64_t num)
       {
       	int ret = OB_SUCCESS;
       	if ( range_count_ > 0 )
       	{
       		ret = OB_ERR_UNEXPECTED ;
       		TBSYS_LOG(ERROR, "malloc space twice,error, ret=%d", ret);
       	}
       	else if ( num <=0 )
       	{
       		ret = OB_ERR_UNEXPECTED ;
       		TBSYS_LOG(ERROR, "Wrong parameter,should not be negative, ret=%d", ret);
       	}
       	else if ( (mutiple_start_key_objs_ = (common::ObObj *)ob_malloc(OB_MAX_ROWKEY_COLUMN_NUMBER*num*sizeof(common::ObObj), ObModIds::OB_SQL_READ_STRATEGY)) == NULL)
       	{
                      ret = OB_ALLOCATE_MEMORY_FAILED;
                      TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d",OB_MAX_ROWKEY_COLUMN_NUMBER*num*sizeof(common::ObObj), ret);
       	}
       	else if ( (mutiple_end_key_objs_ = (common::ObObj *)ob_malloc(OB_MAX_ROWKEY_COLUMN_NUMBER*num*sizeof(common::ObObj), ObModIds::OB_SQL_READ_STRATEGY)) == NULL)
       	{
   			ob_free(mutiple_start_key_objs_);
   			mutiple_start_key_objs_ = NULL;
               ret = OB_ALLOCATE_MEMORY_FAILED;
               TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", OB_MAX_ROWKEY_COLUMN_NUMBER*num*sizeof(common::ObObj), ret);
       	}
       	else if ( (mutiple_start_key_mem_hold_ = (char **)ob_malloc(sizeof(char*)*num*OB_MAX_ROWKEY_COLUMN_NUMBER, ObModIds::OB_SQL_READ_STRATEGY)) == NULL)
       	{
   			ob_free(mutiple_start_key_objs_);
   			mutiple_start_key_objs_ = NULL;
   			ob_free(mutiple_end_key_objs_);
   			mutiple_end_key_objs_ = NULL;
               ret = OB_ALLOCATE_MEMORY_FAILED;
               TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", sizeof(char*)*num*OB_MAX_ROWKEY_COLUMN_NUMBER, ret);
       	}
       	else if ( (mutiple_end_key_mem_hold_ = (char **)ob_malloc(sizeof(char*)*num*OB_MAX_ROWKEY_COLUMN_NUMBER, ObModIds::OB_SQL_READ_STRATEGY)) == NULL)
       	{
   			ob_free(mutiple_start_key_objs_);
   			mutiple_start_key_objs_ = NULL;
   			ob_free(mutiple_end_key_objs_);
   			mutiple_end_key_objs_ = NULL;
   			ob_free(mutiple_start_key_mem_hold_);
   			mutiple_start_key_mem_hold_ = NULL;
               ret = OB_ALLOCATE_MEMORY_FAILED;
               TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", sizeof(char*)*num*OB_MAX_ROWKEY_COLUMN_NUMBER, ret);
       	}
       	else
       	{
       		range_count_ = num;
			//add peiouya IN_EXPR  [PrefixKeyQuery_for_INstmt] 20140930:b
			range_count_cons_ = num;
			//add 20140930:e
       		idx_key_ = 0 ;
       		memset(mutiple_start_key_objs_, 0, OB_MAX_ROWKEY_COLUMN_NUMBER*num*sizeof(common::ObObj));
       		memset(mutiple_end_key_objs_, 0, OB_MAX_ROWKEY_COLUMN_NUMBER*num*sizeof(common::ObObj));
       		memset(mutiple_start_key_mem_hold_, 0, sizeof(char*)*num*OB_MAX_ROWKEY_COLUMN_NUMBER);
       		memset(mutiple_end_key_mem_hold_, 0, sizeof(char*)*num*OB_MAX_ROWKEY_COLUMN_NUMBER);
       		// do nothing
       	}
       	return ret ;

       }
       int  ObSqlReadStrategy::print_ranges() const
       {
          int ret = OB_SUCCESS;
          if ( NULL != rowkey_info_)
          {
			  int64_t rowkey_num = rowkey_info_->get_size();
			  for ( int i = 0;i < range_count_;i++)
			  {
			   for ( int j = 0; j < rowkey_num; j++)
			   {

				TBSYS_LOG(DEBUG, "ranges:line %d  column %d start is=  %s", i, j, to_cstring(*(mutiple_start_key_objs_+ i*OB_MAX_ROWKEY_COLUMN_NUMBER+j)));
				TBSYS_LOG(DEBUG, "ranges:line %d  column %d end is=  %s", i, j, to_cstring(*(mutiple_end_key_objs_+ i*OB_MAX_ROWKEY_COLUMN_NUMBER+j)));

			   }
			  }
          }
          else
          {
         		ret = OB_ERR_UNEXPECTED ;
         		TBSYS_LOG(WARN, "rowkey_info_ should not be null, ret=%d", ret);
          }
          return ret;
      }
    //add:e
 //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140911:b
/*Exp:reset*/
void ObSqlReadStrategy::reset_stuff()
{
  simple_in_filter_list_.clear();
  simple_cond_filter_list_.clear();
  memset(start_key_mem_hold_, 0, sizeof(start_key_mem_hold_));
  memset(end_key_mem_hold_, 0, sizeof(end_key_mem_hold_));
}
//add 20140911:e
//add wanglei [semi join update] 20160411:b
//mod dragon [type_err] 2016-12-6 b
int ObSqlReadStrategy::remove_last_inexpr()
{
  int ret = OB_SUCCESS;
  if(simple_in_filter_list_.count ()>0)
    ret = simple_in_filter_list_.remove (simple_in_filter_list_.count ()-1);
  return ret;
}
//void ObSqlReadStrategy::remove_last_inexpr()
//{
//    if(simple_in_filter_list_.count ()>0)
//        simple_in_filter_list_.remove (simple_in_filter_list_.count ()-1);
//}
//mod e
void ObSqlReadStrategy::remove_last_expr()
{
    if(simple_cond_filter_list_.count ()>0)
        simple_cond_filter_list_.pop_back ();
}
//add wanglei:e
