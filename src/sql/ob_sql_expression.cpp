/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_sql_expression.cpp,  05/28/2012 03:46:40 PM xiaochu Exp $
 *
 * Author:
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:
 *
 *
 */

#include "ob_sql_expression.h"
#include "common/utility.h"
#include "sql/ob_item_type_str.h"
#include "common/ob_cached_allocator.h"
#include "sql/ob_phy_operator_type.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObSqlExpression::ObSqlExpression()
  : column_id_(0), table_id_(0), is_aggr_func_(false), is_distinct_(false)
  , aggr_func_(T_INVALID)
{
  //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140404:b
  sub_query_num_ = 0;
  use_bloom_filter_ = false;//20140610
  direct_bind_query_num_ = 0;
  //add 20140404:e
  //add wanglei [to date optimization] 20160328
  is_optimization_ = false;
  //add by gaojt
  questionmark_num_in_update_assign_list_ = 0;
  is_update_ = false;
}

ObSqlExpression::~ObSqlExpression()
{
  //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140606:b
  if(use_bloom_filter_)
  {
    bloom_filter_.destroy();
  }
  post_expr_.free_post_in();

  //add 20140606:e
}

ObSqlExpression::ObSqlExpression(const ObSqlExpression &other)
  :DLink()
{
  *this = other;
}

ObSqlExpression& ObSqlExpression::operator=(const ObSqlExpression &other)
{
  if (&other != this)
  {
    post_expr_ = other.post_expr_;
    column_id_ = other.column_id_;
    table_id_ = other.table_id_;
    is_aggr_func_ = other.is_aggr_func_;
    is_distinct_ = other.is_distinct_;
    aggr_func_ = other.aggr_func_;
	listagg_delimeter_ = other.listagg_delimeter_;//add gaojt [ListAgg][JHOBv0.1]20150104
	//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140404:b
    direct_bind_query_num_ = other.direct_bind_query_num_;
    sub_query_num_ = other.sub_query_num_;
    use_bloom_filter_ = other.use_bloom_filter_;
    if(sub_query_num_ >0)
    {
      for(int i=0;i<sub_query_num_;i++)
      {
        sub_query_idx_[i] = other.sub_query_idx_[i];
      }
	if(use_bloom_filter_)
	{
	  bloom_filter_.deep_copy(other.bloom_filter_);
	}
    }
	//add 20140404:e
    // @note we do not copy the members of DLink on purpose

    questionmark_num_in_update_assign_list_ = other.questionmark_num_in_update_assign_list_;
    is_update_ = other.is_update_;
  }
  return *this;
}

int ObSqlExpression:: add_expr_obj(const ObObj &obj)
{
  return post_expr_.add_expr_obj(obj);
}
//add dolphin[coalesce return type]@20151126:b
void ObSqlExpression::set_expr_type(const ObObjType &type)
{
  post_expr_.set_expr_type(type);
}
//add:e
int ObSqlExpression::add_expr_item(const ExprItem &item)
{
  return post_expr_.add_expr_item(item);
}

int ObSqlExpression::add_expr_item_end()
{
  return post_expr_.add_expr_item_end();
}

//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140504:b

/*Exp: set mark for using bloomfilter,
* then pass bloomfilter address to postfix_expression
*/
void ObSqlExpression::set_has_bloomfilter()
{
  use_bloom_filter_ = true;
  post_expr_.set_has_bloomfilter(&bloom_filter_);
}

/*Exp:used before specific value replace the mark of sub_query, 
* pop other information of expression out to temp expression ,
* then remove the mark of sub_query
* SPECIAL POINT:before process this sub_query,previous sub_query may have already
* direct bind specific value, and not treate these sub_query as sub_query any more,
* should avoid the influence of this situation
* @param sub_query_index[in] ,sub_query index in current expression
*/
int ObSqlExpression::delete_sub_query_info(int sub_query_index)
{
  sub_query_index = sub_query_index - (int)direct_bind_query_num_;
  direct_bind_query_num_ ++;
  return post_expr_.delete_sub_query_info(sub_query_index);
}

/*Exp:update sub_query num
* if direct bind sub_query result to main query,
* do not treat it as a sub_query any more
* should update sub_query number
*/
void ObSqlExpression::update_sub_query_num()
{
  sub_query_num_ = sub_query_num_ - direct_bind_query_num_ ;
  direct_bind_query_num_ = 0;
}

/*Exp:copy filter expression to object filter
*used for ObMultiBind second check 
*@param res[in] res filter
*/
int ObSqlExpression::copy(ObSqlExpression* res)
{
  int ret = OB_SUCCESS;
  post_expr_ = res->post_expr_;
  column_id_ = res->column_id_;
  table_id_ = res->table_id_;
  is_aggr_func_ = res->is_aggr_func_;
  is_distinct_ = res->is_distinct_;
  aggr_func_ = res->aggr_func_;
  sub_query_num_ = res->sub_query_num_;
  return ret;
}

/*Exp:store sub_query physical_plan index to array
*@param idx[in] sub_query's number in current expression
*@param index[in] sub_query index in physical_plan
*/
int ObSqlExpression::set_sub_query_idx(int64_t idx,int32_t index)
{
  int ret = OB_SUCCESS;
  if(idx>MAX_SUB_QUERY_NUM)
  {
    TBSYS_LOG(WARN, "too many sub_query");
    ret = OB_ERR_SUB_QUERY_OVERFLOW;
  }
  else
  {
    sub_query_idx_[idx-1]= index;
  }
  return ret;
}

/*Exp: for free memory of bloomfilter used*/
void ObSqlExpression::destroy_filter()
{
  post_expr_.free_post_in();

  if(use_bloom_filter_)
  	bloom_filter_.destroy();
}

//add 20140504:e
static ObObj OBJ_ZERO;
static struct obj_zero_init
{
  obj_zero_init()
  {
    OBJ_ZERO.set_int(0);
  }
} obj_zero_init;

//mod peiouya peiouya [IN_TYPEBUG_FIX] 20151225:b
////mod tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140504:b
////int ObSqlExpression::calc(const common::ObRow &row, const common::ObObj *&result)
///*Exp: calc will call static function to do real work,
//*other params need transfer to static function as input parameters
//* @param hash_map[in]
//* @param second_check[in]
//*/
//int ObSqlExpression::calc(const common::ObRow &row, const common::ObObj *&result,hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* hash_map,bool second_check )
////mod 20140504:e
int ObSqlExpression::calc(const common::ObRow &row, const common::ObObj *&result,
           hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* hash_map,
           bool is_has_map_contain_null,  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
           common::ObArray<ObObjType> *hash_map_pair_type_desc,
           bool second_check)
//mod 20151225:e
{
  int err = OB_SUCCESS;
  //if (OB_UNLIKELY(is_aggr_func_ && T_FUN_COUNT == aggr_func_ && post_expr_.is_empty()))
  if (OB_UNLIKELY(is_aggr_func_ && (T_FUN_COUNT == aggr_func_  || T_FUN_ROW_NUMBER == aggr_func_) && post_expr_.is_empty()))
  {
    // COUNT(*)
    // point the result to an arbitray non-null cell
    result = &OBJ_ZERO;
  }
  else
  {
    //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
    ////mod peiouya peiouya [IN_TYPEBUG_FIX] 20151225:b
    //////mod tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140504:b
    //////err = post_expr_.calc(row, result);
    ////err = post_expr_.calc(row, result,hash_map,second_check);
    //////mod 20140504:e
    //    err = post_expr_.calc(row, result,hash_map, hash_map_pair_type_desc, second_check);
    err = post_expr_.calc(row, result,hash_map,is_has_map_contain_null, hash_map_pair_type_desc, second_check);
    ////mod 20151225:e
    //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
    //post_expr_.free_post_in();
  }
  return err;
}

int64_t ObSqlExpression::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_INVALID_ID == table_id_)
  {
    databuff_printf(buf, buf_len, pos, "expr<NULL,%lu>=", column_id_);
  }
  else
  {
    databuff_printf(buf, buf_len, pos, "expr<%lu,%lu>=", table_id_, column_id_);
  }
  if (is_aggr_func_)
  {
	databuff_printf(buf, buf_len, pos, "%s(%s", ob_aggr_func_str(aggr_func_), is_distinct_ ? "DISTINCT " : "");
  }
  if (post_expr_.is_empty())
  {
    databuff_printf(buf, buf_len, pos, "*");
  }
  else
  {
    databuff_printf(buf, buf_len, pos, "[");
    pos += post_expr_.to_string(buf+pos, buf_len-pos);
    databuff_printf(buf, buf_len, pos, "]");
  }
  if (is_aggr_func_)
  {
	databuff_printf(buf, buf_len, pos, ")");
  }
  return pos;
}
//add fanqiushi_index
int ObSqlExpression::change_tid(uint64_t &array_index)
{
    return post_expr_.change_tid(array_index);
}

int ObSqlExpression::get_cid(uint64_t &cid)
{
    return post_expr_.get_cid(cid);
}


//add:e
//add wanglei [second index fix] 20160425:b
 int ObSqlExpression::get_tid_indexs( ObArray<uint64_t> & index_list)
 {
        return post_expr_.get_tid_indexs(index_list);
 }
//add wanglei [second index fix] 20160425:e
DEFINE_SERIALIZE(ObSqlExpression)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialize_basic_param(buf, buf_len, pos)))
  {
	TBSYS_LOG(WARN, "fail to serialize basic param. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = post_expr_.serialize(buf, buf_len, pos)))
  {
	TBSYS_LOG(WARN, "fail to serialize postfix expression. ret=%d", ret);
  }
  else
  {
	//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140504:b
	ObObj use_bloom_filter;
	use_bloom_filter.set_bool(use_bloom_filter_);
	if(OB_SUCCESS != (ret = use_bloom_filter.serialize(buf, buf_len, pos)))
	{
	  TBSYS_LOG(WARN, "fail to serialize sub query num. ret=%d", ret);
	}
	else if(use_bloom_filter_)
	{
	  if(OB_SUCCESS != (ret = bloom_filter_.serialize(buf, buf_len, pos)))
	  {
		TBSYS_LOG(WARN, "fail to eserialize bloomfilter. ret=%d", ret);
	  }
	  TBSYS_LOG(DEBUG, "serialize bloonfilter success"); //mod peiouya 20151225
	}
	//add 20140504:e
	// success
	//TBSYS_LOG(INFO, "success serialize one ObSqlExpression. pos=%ld", pos);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObSqlExpression)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = deserialize_basic_param(buf, data_len, pos)))
  {
	TBSYS_LOG(WARN, "fail to serialize basic param. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = post_expr_.deserialize(buf, data_len, pos)))
  {
	TBSYS_LOG(WARN, "fail to serialize postfix expression. ret=%d", ret);
  }
  else
  {
  	//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140504:b
	ObObj use_bloom_filter;
	if(OB_SUCCESS != (ret = use_bloom_filter.deserialize(buf, data_len, pos)))
	{
	 TBSYS_LOG(WARN, "fail to deserialize sub query num. ret=%d", ret);
	}
	else if (OB_SUCCESS != (ret = use_bloom_filter.get_bool(use_bloom_filter_)))  
	{    
	 TBSYS_LOG(WARN, "fail to get expr_count. ret=%d", ret);  
	} 
	else if(use_bloom_filter_)
	{
	 if(OB_SUCCESS != (ret = bloom_filter_.deserialize(buf, data_len, pos)))
	 {
	  TBSYS_LOG(WARN, "fail to serialize bloomfilter. ret=%d", ret);
	 }
	 else
	 {
	  //pass bloomfilter address to postfix ....
	  set_has_bloomfilter();
	 }
	}
  	//add 20140504:e
  }
  return ret;
}

int ObSqlExpression::serialize_basic_param(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_SUCCESS == ret)
  {
    obj.set_int((int64_t)column_id_);
    if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    obj.set_int((int64_t)table_id_);
    if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    obj.set_bool(is_aggr_func_);
    if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    obj.set_bool(is_distinct_);
    if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    obj.set_int((int64_t)aggr_func_);
    if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
    }
  }
  //add gaojt [ListAgg][JHOBv0.1]20150104:b
  if (OB_SUCCESS == ret)
  {
    obj.set_varchar(listagg_delimeter_);
    if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
    }
  }
  /* add 20150104:e */
  return ret;
}

int ObSqlExpression::deserialize_basic_param(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  int64_t val = 0;
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
    }
    if (OB_SUCCESS != (ret = obj.get_int(val)))
    {
      TBSYS_LOG(WARN, "fail to get int value. ret=%d, column_id_=%lu", ret, column_id_);
    }
    else
    {
      column_id_ = (uint64_t)val;
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
    }
    if (OB_SUCCESS != (ret = obj.get_int(val)))
    {
      TBSYS_LOG(WARN, "fail to get int value. ret=%d, table_id_=%lu", ret, table_id_);
    }
    else
    {
      table_id_ = val;
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
    }
    if (OB_SUCCESS != (ret = obj.get_bool(is_aggr_func_)))
    {
      TBSYS_LOG(WARN, "fail to get int value. ret=%d, is_aggr_func_=%d", ret, is_aggr_func_);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
    }
    if (OB_SUCCESS != (ret = obj.get_bool(is_distinct_)))
    {
      TBSYS_LOG(WARN, "fail to get int value. ret=%d, is_distinct_=%d", ret, is_distinct_);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
    }
    if (OB_SUCCESS != (ret = obj.get_int(val)))
    {
      TBSYS_LOG(WARN, "fail to get int value. ret=%d, aggr_func_=%d", ret, aggr_func_);
    }
    else
    {
      aggr_func_ = (ObItemType)val;
    }
  }
  //add gaojt [ListAgg][JHOBv0.1]20150104:b
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
    }
    if (OB_SUCCESS != (ret = obj.get_varchar(listagg_delimeter_)))
    {
      TBSYS_LOG(WARN, "fail to get varchar value. ret=%d, listagg_delimeter_=%.*s", ret, listagg_delimeter_.length(),listagg_delimeter_.ptr());
    }
  }
  /* add 20150104:e */
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObSqlExpression)
{
  int64_t size = 0;
  size += get_basic_param_serialize_size();
  size += post_expr_.get_serialize_size();
  //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140504:b
  ObObj use_bloom_filter;
  use_bloom_filter.set_bool(use_bloom_filter_);
  size += use_bloom_filter.get_serialize_size();
  if(use_bloom_filter_)
  {
    size += bloom_filter_.get_serialize_size();
    TBSYS_LOG(WARN, "add bloonfilter size in get serialize size %ld",bloom_filter_.get_serialize_size());
  }
    //add 20140504:e
  return size;
}

int64_t ObSqlExpression::get_basic_param_serialize_size() const
{
  int64_t size = 0;
  ObObj obj;
	obj.set_int((int64_t)column_id_);
	size += obj.get_serialize_size();
  obj.set_int((int64_t)table_id_);
	size += obj.get_serialize_size();
	obj.set_bool(is_aggr_func_);
	size += obj.get_serialize_size();
	obj.set_bool(is_distinct_);
	size += obj.get_serialize_size();
	obj.set_int((int64_t)aggr_func_);
	size += obj.get_serialize_size(); //add gaojt [ListAgg][JHOBv0.1]20150104
    obj.set_varchar(listagg_delimeter_);//add gaojt [ListAgg][JHOBv0.1]20150104
    size += obj.get_serialize_size();
  return size;
}

int ObSqlExpressionUtil::make_column_expr(const uint64_t tid, const uint64_t cid, ObSqlExpression &expr)
{
  int ret = OB_SUCCESS;
  ExprItem item;

  item.type_ = T_REF_COLUMN;
  item.value_.cell_.tid = tid;
  item.value_.cell_.cid = cid;
  if (OB_SUCCESS != (ret = expr.add_expr_item(item)))
  {
    TBSYS_LOG(WARN, "fail to add expr item. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = expr.add_expr_item_end()))
  {
    TBSYS_LOG(WARN, "fail to add expr item. ret=%d", ret);
  }
  return ret;
}

static ObCachedAllocator<ObSqlExpression> SQL_EXPR_ALLOC;
static volatile uint64_t ALLOC_TIMES = 0;
static volatile uint64_t FREE_TIMES = 0;

ObSqlExpression* ObSqlExpression::alloc()
{
  ObSqlExpression *ret = SQL_EXPR_ALLOC.alloc();
  if (OB_UNLIKELY(NULL == ret))
  {
    TBSYS_LOG(ERROR, "failed to allocate expression object");
  }
  else
  {
    atomic_inc(&ALLOC_TIMES);
  }
  if (ALLOC_TIMES % 1000000 == 0)
  {
    TBSYS_LOG(INFO, "[EXPR] alloc %p, times=%ld cached=%d alloc_num=%d",
              ret, ALLOC_TIMES, SQL_EXPR_ALLOC.get_cached_count(), SQL_EXPR_ALLOC.get_allocated_count());
    ob_print_phy_operator_stat();
  }
  return ret;
}

void ObSqlExpression::free(ObSqlExpression* ptr)
{
  ptr->post_expr_.free_post_in();
  SQL_EXPR_ALLOC.free(ptr);
  atomic_inc(&FREE_TIMES);
  if (FREE_TIMES % 1000000 == 0)
  {
    TBSYS_LOG(INFO, "[EXPR] free %p, times=%ld cached=%d alloc_num=%d",
              ptr, FREE_TIMES, SQL_EXPR_ALLOC.get_cached_count(), SQL_EXPR_ALLOC.get_allocated_count());
  }
}
//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715:b

/*Exp:determin whether the expr is sub_query for insert,
* used for rpc_scan get filter
*/
bool ObSqlExpression::is_rows_filter()
{
  return post_expr_.is_rows_filter();  
}

/*Exp:used before specific value replace the mark of sub_query, 
* pop other information of expression out to temp expression ,
* then remove the mark of sub_query
*/
int ObSqlExpression::delete_sub_query_info()
{
  return post_expr_.delete_sub_query_info();
}
 //add 20140715:e
//add lbzhong[Update rowkey] 20151221:b
int ObSqlExpression::convert_to_equal_expression(const int64_t table_id, const int64_t column_id, const ObObj *&value)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS != (ret = post_expr_.convert_to_equal_expr(table_id, column_id, value)))
  {
    TBSYS_LOG(ERROR, "failed to convert_to_equal_expr, ret=%d", ret);
  }
  return ret;
}
//add:e
