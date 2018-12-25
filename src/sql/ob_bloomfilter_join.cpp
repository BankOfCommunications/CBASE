/**
 * ob_bloomfilter_bind.cpp
 *
 * Authors:
 *   steven.h.d
 * used to get hash_join's and nested_loop_join's result and direct bind result to main query
 * or build hashmap and bloomfilter, and check filter conditions
 */

#include "ob_bloomfilter_join.h"
#include "ob_table_rpc_scan.h"
#include "ob_table_mem_scan.h"
#include "ob_postfix_expression.h"
#include "common/utility.h"
#include "common/ob_row_util.h"
#include "sql/ob_physical_plan.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObBloomFilterJoin::ObBloomFilterJoin()
    :get_next_row_func_(NULL),
       last_left_row_(NULL),
       last_right_row_(NULL),
       right_cache_is_valid_(false),
       is_right_iter_end_(false)
       ,hashmap_num_(0)
{
  arena_.set_mod_id(ObModIds::OB_MS_SUB_QUERY);
  last_left_row_has_printed_ = false;
  last_right_row_has_printed_ = false;
  left_cache_is_valid_ = false;
  left_hash_id_cache_valid_ = false;
  is_left_iter_end_ = false;
  last_left_hash_id_ = -1;
  cur_hashmap_num_ = 0;
}

 ObBloomFilterJoin::~ObBloomFilterJoin()
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
  for(int i=0;i<MAX_SUB_QUERY_NUM;i++)
  {
    bloomfilter_map_[i].destroy();
  }
  sub_query_filter_.~ObFilter();
  arena_.free();
}

void ObBloomFilterJoin::reset()
{
  last_left_hash_id_ = -1;
  cur_hashmap_num_ = 0;
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
  left_cache_for_right_join_.clear();
  is_left_iter_end_ = false;
  right_cache_.clear();
  row_desc_.reset();
  right_cache_is_valid_ = false;
  is_right_iter_end_ = false;
  left_hash_id_cache_valid_ = false;
  equal_join_conds_.clear();
  other_join_conds_.clear();
  left_op_ = NULL;
  right_op_ = NULL;
  for(int i =0;i<MAX_SUB_QUERY_NUM;i++)
  {
    bloomfilter_map_[i].clear();
  }
  for(int i =0;i<HASH_BUCKET_NUM;i++)
  {
    hash_table_row_store[i].row_store.clear();
  }
  hashmap_num_ = 0;
  sub_query_filter_.reset();
  arena_.free();
}

void ObBloomFilterJoin::reuse()
{
  get_next_row_func_ = NULL;
  last_left_row_ = NULL;
  last_right_row_ = NULL;
  last_left_hash_id_ = -1;
  left_hash_id_cache_valid_ = false;
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
  left_cache_for_right_join_.reuse();
  is_left_iter_end_ = false;
  right_cache_.reuse();
  row_desc_.reset();
  right_cache_is_valid_ = false;
  is_right_iter_end_ = false;
  equal_join_conds_.clear();
  other_join_conds_.clear();
  left_op_ = NULL;
  right_op_ = NULL;
  sub_query_filter_.reuse();
  for(int i =0;i<MAX_SUB_QUERY_NUM;i++)
  {
    bloomfilter_map_[i].clear();
  }
  for(int i =0;i<HASH_BUCKET_NUM;i++)
  {
    hash_table_row_store[i].row_store.reuse();
  }
  hashmap_num_ = 0;
  cur_hashmap_num_ = 0;
  arena_.free();
}

/*
 *设置join类型
 * Bloomfilter join 仅处理inner join与left join
 */
int ObBloomFilterJoin::set_join_type(const ObJoin::JoinType join_type)
{
  int ret = OB_SUCCESS;
  ObJoin::set_join_type(join_type);
  switch(join_type)
  {
    case INNER_JOIN:
      //hash part2
      //get_next_row_func_ = &ObBloomFilterJoin::inner_hash_get_next_row;
      //hash part1
      get_next_row_func_ = &ObBloomFilterJoin::inner_get_next_row;
      use_bloomfilter_ = true;
      break;
    case LEFT_OUTER_JOIN:
      //hash part2
      //get_next_row_func_ = &ObBloomFilterJoin::left_hash_outer_get_next_row;
      //hash part1
      get_next_row_func_ = &ObBloomFilterJoin::left_outer_get_next_row;
      //TBSYS_LOG(ERROR,"left_outer_get_next_row");
      use_bloomfilter_ = true;
      break;
    case RIGHT_OUTER_JOIN:
      get_next_row_func_ = &ObBloomFilterJoin::right_outer_get_next_row;
      use_bloomfilter_ = false;
      break;
    case FULL_OUTER_JOIN:
      get_next_row_func_ = &ObBloomFilterJoin::full_outer_get_next_row;
      use_bloomfilter_ = false;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

/*
 *迭代get_next_row
 *根据join类型 选取相应的get_next_row的方法
 */

int ObBloomFilterJoin::get_next_row(const ObRow *&row)
{
  OB_ASSERT(get_next_row_func_);
  return (this->*(this->ObBloomFilterJoin::get_next_row_func_))(row);
}

/*
 *获取行描述
 *
 */

int ObBloomFilterJoin::get_row_desc(const common::ObRowDesc *&row_desc) const
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

int64_t ObBloomFilterJoin::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "BloomFilter ");
  pos += ObJoin::to_string(buf + pos, buf_len - pos);
  return pos;
}

int ObBloomFilterJoin::close()
{
  sub_query_filter_.close();
  for(int i=0;i<MAX_SUB_QUERY_NUM;i++)
  {
    bloomfilter_map_[i].destroy();
  }
  arena_.free();

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
  row_desc_.reset();
  ret = ObJoin::close();
  return ret;
}

/*
 * 1、打开前表运算符
 * 2、迭代前表每一行数据，生成Bloomfilter 并维护前表数据
 * 3、构建好的Bloomfilter 作为表达式加入后表filter
 * 4、初始化值
 */

int ObBloomFilterJoin::open()
{
  int ret = OB_SUCCESS;
  const ObRowDesc *left_row_desc = NULL;
  const ObRowDesc *right_row_desc = NULL;
  char *store_buf = NULL;
  char *right_store_buf = NULL;
  con_length_ = equal_join_conds_.count();
  if (con_length_ <= 0)
  {
    TBSYS_LOG(WARN, "bloom filter join can not work without equijoin conditions");
    ret = OB_NOT_SUPPORTED;
    return ret;
  }
  else if (OB_SUCCESS != (ret = left_op_->open()))
  {
    TBSYS_LOG(WARN, "failed to open child(ren) operator(s), err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = left_op_->get_row_desc(left_row_desc)))
  {
    TBSYS_LOG(WARN, "failed to get child row desc, err=%d", ret);
  }
  if(OB_SUCCESS == ret)
  {
    const ObRow *row = NULL;
    table_filter_expr_ = ObSqlExpression::alloc();
    hashmap_num_ = 0;
    bloomfilter_map_[hashmap_num_].create(HASH_BUCKET_NUM);
    common::ObBloomFilterV1 *bloom_filter = NULL;
    table_filter_expr_->set_has_bloomfilter();
    table_filter_expr_->get_bloom_filter(bloom_filter);
    if(OB_SUCCESS != (ret = bloom_filter->init(BLOOMFILTER_ELEMENT_NUM)))
    {
      TBSYS_LOG(WARN, "Problem initialize bloom filter");
    }
    ExprItem dem1,dem2,dem3,dem4,dem5;
    dem1.type_ = T_REF_COLUMN;
    //后缀表达式组建
    for (int64_t i = 0; i < con_length_; ++i)
    {
      const ObSqlExpression &expr = equal_join_conds_.at(i);
      ExprItem::SqlCellInfo c1;
      ExprItem::SqlCellInfo c2;
      if (expr.is_equijoin_cond(c1, c2))
      {
        dem1.value_.cell_.tid = c2.tid;
        dem1.value_.cell_.cid = c2.cid;
        //add dragon [invalid_argument] 2016-12-22
        if(use_bloomfilter_)
        {
          ObSingleChildPhyOperator *right_op =  dynamic_cast<ObSingleChildPhyOperator *>(get_child(1));
          ObTableRpcScan * right_rpc_scan;
          if(NULL == (right_rpc_scan = dynamic_cast<ObTableRpcScan *>(right_op->get_child(0)))){
            /* Never Mind, it's OK*/
            TBSYS_LOG(WARN, "Now, we only support TableRpcScan operator!");
          }
          else
          {
            bool use_index_back = false;
            uint64_t index_tid = OB_INVALID_ID;
            right_rpc_scan->get_is_index_without_storing(use_index_back, index_tid);
            TBSYS_LOG(DEBUG, "%d, %lu", use_index_back, index_tid);
            if(use_index_back)
            {
              if(OB_INVALID_ID == index_tid)
              {
                ret = OB_ERR_UNEXPECTED;
                TBSYS_LOG(WARN, "WTF, use index scan,but index tid is invalid");
              }
              else
                dem1.value_.cell_.tid = index_tid;
            }
            else
            {
              /*do nothing*/
            }
            TBSYS_LOG(DEBUG, "dem1 has been changed from %ld to %ld", c2.tid, dem1.value_.cell_.tid);
          }
        }

        //c1是左表, c2是右表
        TBSYS_LOG(DEBUG, "Dragon says: %ld/%ld c1 is left table while c2 is right table c1[%lu, %lu] c2[%lu, %lu]",
                  i, con_length_, c1.tid, c1.cid, c2.tid, c2.cid);
        //add dragon 2016-12-22 e
        table_filter_expr_->add_expr_item(dem1);
      }
    }
    dem2.type_ = T_OP_ROW;
    dem2.data_type_ = ObMinType;
    dem2.value_.int_ = con_length_;

    dem3.type_ = T_OP_LEFT_PARAM_END;
    dem3.data_type_ = ObMinType;
    dem3.value_.int_ = 2;

    dem4.type_ = T_REF_QUERY;
    dem4.data_type_ = ObMinType;
    dem4.value_.int_ = 1;

    dem5.type_ = T_OP_IN;
    dem5.data_type_ = ObMinType;
    dem5.value_.int_ = 2;
    table_filter_expr_->add_expr_item(dem2);
    table_filter_expr_->add_expr_item(dem3);
    table_filter_expr_->add_expr_item(dem4);
    table_filter_expr_->add_expr_item(dem5);
    table_filter_expr_->add_expr_item_end();
    ret = left_op_->get_next_row(row);
    //迭代前表数据，构建Bloomfilter
    while (OB_SUCCESS == ret)
    {
      //TBSYS_LOG(ERROR, "load data from left table, row=%s", to_cstring(*row));
      ObObj value[con_length_];
      ObRow *curr_row = const_cast<ObRow *>(row);
      //uint64_t bucket_num = 0;
      for (int64_t i = 0; i < con_length_; ++i)
      {
        const ObSqlExpression &expr = equal_join_conds_.at(i);
        ExprItem::SqlCellInfo c1;
        ExprItem::SqlCellInfo c2;
        if (expr.is_equijoin_cond(c1, c2))
        {
          ObObj *temp;
          if (OB_SUCCESS != (ret = curr_row->get_cell(c1.tid,c1.cid,temp)))
          {
            TBSYS_LOG(ERROR, "failed to get cell, err=%d tid=%lu cid=%lu", ret, c1.tid, c1.cid);
            break;
          }
          else
          {
            value[i] = *temp;
            //bucket_num = temp->murmurhash64A(bucket_num);
          }
        }
      }
      ObRowkey columns;
      columns.assign(value,con_length_);
      ObRowkey columns2;
      if(OB_SUCCESS != (ret = columns.deep_copy(columns2,arena_)))
      {
        TBSYS_LOG(WARN, "fail to deep copy column");
        break;
      }
      bloom_filter->insert(columns2);
      const ObRowStore::StoredRow *stored_row = NULL;
      //bucket_num = bucket_num%HASH_BUCKET_NUM;
      //TBSYS_LOG(ERROR,"DHC bucket_num=%d",(int)bucket_num);
      //sort--part1 维护前表数据
      left_cache_.add_row(*row, stored_row);
      stored_row = NULL;
      //hash--part2 维护前表数据
      //hash_table_row_store[(int)bucket_num].row_store.add_row(*row, stored_row);
      //hash_table_row_store[(int)bucket_num].hash_iterators.push_back(0);
      ret = left_op_->get_next_row(row);
    }
	//add wanglei [bloom filter fix] 20160524:b
	if(ret == OB_ITER_END)
		ret = OB_SUCCESS;
	//add wanglei [bloom filter fix] 20160524:e
    if(use_bloomfilter_)
    {
      //sort--part1 后表运算符
      ObSingleChildPhyOperator *sort_query =  dynamic_cast<ObSingleChildPhyOperator *>(get_child(1));
      ObTableRpcScan * main_query;
      if(NULL==(main_query= dynamic_cast<ObTableRpcScan *>(sort_query->get_child(0)))){
        ObTableMemScan * main_query_in;
        main_query_in= dynamic_cast<ObTableMemScan *>(sort_query->get_child(0));
        //main_query_in->set_expr(table_filter_expr_);
      }
      else
      {
        hashmap_num_ ++;
        main_query->add_filter(table_filter_expr_);
      }
      //hash--part2 后表运算符
      //ObTableRpcScan * main_query = dynamic_cast<ObTableRpcScan *>(get_child(1));
    }
    /*int64_t max_size=0; 测试hash是否分布均匀
    int64_t second_size=0;
    for(int64_t i=0;i<HASH_BUCKET_NUM;i++){
        max_size=max_size <hash_table_row_store[(int)i].hash_iterators.count()?hash_table_row_store[(int)i].hash_iterators.count():max_size;
        second_size=hash_table_row_store[(int)i].hash_iterators.count()<max_size?(hash_table_row_store[(int)i].hash_iterators.count()>second_size?hash_table_row_store[(int)i].hash_iterators.count():second_size):second_size;
    }
    TBSYS_LOG(ERROR,"max_size=%d second_size=%d",(int)max_size,(int)second_size);
    */
  }
  if(OB_SUCCESS == ret)
  {
      if (OB_SUCCESS != (ret = right_op_->open()))
      {
        TBSYS_LOG(WARN, "failed to open right_op_ operator(s), err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = right_op_->get_row_desc(right_row_desc)))
      {
        TBSYS_LOG(WARN, "failed to get right_op_ row desc, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = cons_row_desc(*left_row_desc, *right_row_desc)))
      {
        TBSYS_LOG(WARN, "failed to cons row desc, err=%d", ret);
      }
      else if (NULL == (store_buf = static_cast<char*>(ob_malloc(MAX_SINGLE_ROW_SIZE, ObModIds::OB_SQL_MERGE_JOIN))))
      {
        TBSYS_LOG(ERROR, "no memory");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (NULL == (right_store_buf = static_cast<char*>(ob_malloc(MAX_SINGLE_ROW_SIZE, ObModIds::OB_SQL_MERGE_JOIN))))
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
        curr_cached_left_row_.set_row_desc(*left_row_desc);
        last_left_row_has_printed_ = false;
        last_right_row_has_printed_ = false;
        left_cache_is_valid_ = false;
        is_left_iter_end_ = false;
        left_hash_id_cache_valid_ = false;
        last_left_hash_id_ = -1 ;
        cur_hashmap_num_ = 0;
        last_join_right_row_store_.assign_buffer(right_store_buf,MAX_SINGLE_ROW_SIZE);
        last_join_left_row_store_.assign_buffer(store_buf, MAX_SINGLE_ROW_SIZE);
      }
  }
  return ret;
}


//判断等值连接条件是否满足
int ObBloomFilterJoin::compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const
{
  //TBSYS_LOG(ERROR, "dhc ObBloomFilterJoin::compare_equijoin_cond()");
  int ret = OB_SUCCESS;
  cmp = 0;
  const ObObj *res1 = NULL;
  const ObObj *res2 = NULL;
  ObExprObj obj1;
  ObExprObj obj2;
  for (int64_t i = 0; i < con_length_; ++i)
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
            cmp = -10;
          }
          else
          {
            cmp = 10;
          }
          break;
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

int ObBloomFilterJoin::left_row_compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const
{
  //TBSYS_LOG(ERROR, "dhc ObBloomFilterJoin::left_row_compare_equijoin_cond()");
  int ret = OB_SUCCESS;
  cmp = 0;
  const ObObj *res1 = NULL;
  const ObObj *res2 = NULL;
  ObExprObj obj1;
  ObExprObj obj2;
  for (int64_t i = 0; i < con_length_; ++i)
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
          cmp = -10;
          break;
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

int ObBloomFilterJoin::right_row_compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const
{
  //TBSYS_LOG(ERROR, "dhc ObBloomFilterJoin::right_row_compare_equijoin_cond()");
  int ret = OB_SUCCESS;
  cmp = 0;
  const ObObj *res1 = NULL;
  const ObObj *res2 = NULL;
  ObExprObj obj1;
  ObExprObj obj2;
  for (int64_t i = 0; i < con_length_; ++i)
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
          cmp = -10;
          break;
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

int ObBloomFilterJoin::curr_row_is_qualified(bool &is_qualified)
{
  //TBSYS_LOG(ERROR, "dhc ObBloomFilterJoin::curr_row_is_qualified()");
  int ret = OB_SUCCESS;
  is_qualified = true;
  const ObObj *res = NULL;
  int hash_mape_index = 0;
  for (int64_t i = 0; i < other_join_conds_.count(); ++i)
  {
    ObSqlExpression &expr = other_join_conds_.at(i);
    bool use_hash_map = false;
    common::hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* p = NULL;
    if(hashmap_num_>0 && expr.get_sub_query_num()>0)
    {
      p =&(bloomfilter_map_[hash_mape_index]);
      use_hash_map = true;
      hash_mape_index = hash_mape_index + (int)expr.get_sub_query_num();
    }
    //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
    ////mod peiouya [IN_TYPEBUG_FIX] 20151225
    ////if (OB_SUCCESS != (ret = expr.calc(curr_row_, res, p,use_hash_map)))
    //if (OB_SUCCESS != (ret = expr.calc(curr_row_, res, p, NULL,use_hash_map)))
    if (OB_SUCCESS != (ret = expr.calc(curr_row_, res, p,false, NULL,use_hash_map)))
    ////mod20121225：e
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

int ObBloomFilterJoin::cons_row_desc(const ObRowDesc &rd1, const ObRowDesc &rd2)
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
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    else if (OB_SUCCESS != (ret = row_desc_.add_column_desc(tid, cid)))
    {
      TBSYS_LOG(WARN, "failed to add column desc, err=%d", ret);
      break;
    }
  }
  return ret;
}

int ObBloomFilterJoin::join_rows(const ObRow& r1, const ObRow& r2)
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
  }
  for (int64_t j = 0; OB_SUCCESS == ret && j < r2.get_column_num(); ++j)
  {
    if (OB_SUCCESS != (ret = r2.raw_get_cell(j, cell, tid, cid)))
    {
      TBSYS_LOG(ERROR, "unexpected branch, err=%d", ret);
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    else if (OB_SUCCESS != (ret = curr_row_.raw_set_cell(i+j, *cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell, err=%d j=%ld", ret, j);
      break;
    }
  }
  return ret;
}

int ObBloomFilterJoin::left_join_rows(const ObRow& r1)
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
  }
  int64_t right_row_column_num = row_desc_.get_column_num() - r1.get_column_num();
  ObObj null_cell;
  null_cell.set_null();
  for (int64_t j = 0; OB_SUCCESS == ret && j < right_row_column_num; ++j)
  {
    if (OB_SUCCESS != (ret = curr_row_.raw_set_cell(i+j, null_cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell, err=%d j=%ld", ret, j);
      break;
    }
  }
  return ret;
}

int ObBloomFilterJoin::right_join_rows(const ObRow& r2)
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
  }
  for (int64_t j = 0; OB_SUCCESS == ret && j < r2.get_column_num(); ++j)
  {
    if (OB_SUCCESS != (ret = r2.raw_get_cell(j, cell, tid, cid)))
    {
      TBSYS_LOG(ERROR, "unexpected branch, err=%d", ret);
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    else if (OB_SUCCESS != (ret = curr_row_.raw_set_cell(left_row_column_num+j, *cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell, err=%d j=%ld", ret, j);
      break;
    }
  }
  return ret;
}

// INNER_JOIN
int ObBloomFilterJoin::inner_get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  //const ObRow *left_row = NULL;
  const ObRow *right_row = NULL;
  //get_next_right_row(right_row);
  // fetch the next left row
  if (NULL != last_left_row_)
  {
    curr_cached_left_row_ = *last_left_row_;
    last_left_row_ = NULL;
  }
  else
  {
    ret = left_cache_.get_next_row(curr_cached_left_row_);
  }
  while(OB_SUCCESS == ret)
  {
    if (right_cache_is_valid_)
    {
      OB_ASSERT(!right_cache_.is_empty());
      int cmp = 0;
      if (OB_SUCCESS != (ret = left_row_compare_equijoin_cond(curr_cached_left_row_, last_join_left_row_, cmp)))
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
            ret = left_cache_.get_next_row(curr_cached_left_row_);
          }
        }
        else
        {
          bool is_qualified = false;
          if (OB_SUCCESS != (ret = join_rows(curr_cached_left_row_, curr_cached_right_row_)))
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
            last_left_row_ = &curr_cached_left_row_;
            break;
          }
          else
          {
            // continue with the next cached right row
            OB_ASSERT(NULL == last_left_row_);
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
      //  TBSYS_LOG(ERROR,"DHC etch the next right row number=[%d]",number++);
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
        ret = right_op_->get_next_row(right_row);;
        //TBSYS_LOG(ERROR,"DHC etch the next right row ret=[%d]",ret);
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
              ret = left_cache_.get_next_row(curr_cached_left_row_);
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
      OB_ASSERT(right_row);
      int cmp = 0;
      if (OB_SUCCESS != (ret = compare_equijoin_cond(curr_cached_left_row_, *right_row, cmp)))
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
          if (OB_SUCCESS != (ret = ObRowUtil::convert(curr_cached_left_row_, last_join_left_row_store_, last_join_left_row_)))
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
        else if (OB_SUCCESS != (ret = join_rows(curr_cached_left_row_, *right_row)))
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
          last_left_row_ = &curr_cached_left_row_;
          OB_ASSERT(NULL == last_right_row_);
          break;
        }
        else
        {
          // continue with the next right row
          //OB_ASSERT(NULL != left_row);
          OB_ASSERT(NULL == last_left_row_);
          OB_ASSERT(NULL == last_right_row_);
        }
      } // end 0 == cmp
      else if (cmp < 0)
      {
        if (!right_cache_.is_empty())
        {
          right_cache_is_valid_ = true;
        }
        last_right_row_ = right_row;
        OB_ASSERT(NULL == last_left_row_);
        ret = left_cache_.get_next_row(curr_cached_left_row_);
      }
      else
      {
        //OB_ASSERT(NULL != left_row);
        OB_ASSERT(NULL == last_left_row_);
        OB_ASSERT(NULL == last_right_row_);
      }
    }
  } // end while
  return ret;
}

int ObBloomFilterJoin::right_outer_get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRow *right_row  = NULL;
  // fetch the next right row
  if (NULL != last_right_row_)
  {
    right_row = last_right_row_;
    last_right_row_ = NULL;
    last_right_row_has_printed_ = true;
  }
  else
  {
    ret = right_op_->get_next_row(right_row);;
    last_right_row_has_printed_ = false;
  }
  while(OB_SUCCESS == ret)
  {
    if (left_cache_is_valid_)
    {
      OB_ASSERT(!left_cache_for_right_join_.is_empty());
      int cmp = 0;
      if (OB_SUCCESS != (ret = right_row_compare_equijoin_cond(*right_row, last_join_right_row_, cmp)))
      {
        TBSYS_LOG(WARN, "failed to compare, err=%d", ret);
        break;
      }
      if (0 == cmp)
      {
        // fetch the next left row from left_cache
        if (OB_SUCCESS != (ret = left_cache_for_right_join_.get_next_row(curr_cached_left_row_)))
        {
          if (OB_UNLIKELY(OB_ITER_END != ret))
          {
            TBSYS_LOG(WARN, "failed to get next row from left_cache, err=%d", ret);
          }
          else
          {
            left_cache_for_right_join_.reset_iterator(); // continue
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
                ret = right_op_->get_next_row(right_row);;
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
        left_cache_for_right_join_.clear();
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
        curr_cached_left_row_ = *last_left_row_;
        last_left_row_ = NULL;
      }
      else
      {
        ret = left_cache_.get_next_row(curr_cached_left_row_);
        if (OB_SUCCESS != ret)
        {
          if (OB_ITER_END == ret)
          {
            TBSYS_LOG(INFO, "end of left child op");
            is_left_iter_end_ = true;
            if (!left_cache_for_right_join_.is_empty())
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
                  ret = right_op_->get_next_row(right_row);;
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
      int cmp = 0;
      if (OB_SUCCESS != (ret = compare_equijoin_cond(curr_cached_left_row_, *right_row, cmp)))
      {
        TBSYS_LOG(WARN, "failed to compare, err=%d", ret);
        break;
      }
      if (0 == cmp)
      {
        if (left_cache_for_right_join_.is_empty())
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
        if (OB_SUCCESS != (ret = left_cache_for_right_join_.add_row(curr_cached_left_row_, stored_row)))
        {
          TBSYS_LOG(WARN, "failed to store the row, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = join_rows(curr_cached_left_row_,*right_row)))
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
        // right_row < curr_cached_left_row_ on equijoin conditions
        if (!left_cache_for_right_join_.is_empty())
        {
          left_cache_is_valid_ = true;
          OB_ASSERT(NULL == last_right_row_);
          last_left_row_ = &curr_cached_left_row_;
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
              ret = right_op_->get_next_row(right_row);;
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
              last_left_row_ = &curr_cached_left_row_;
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

int ObBloomFilterJoin::left_outer_get_next_row(const common::ObRow *&row)
{
  //TBSYS_LOG(ERROR, "dhc ObBloomFilterJoin::full_outer_get_next_row() [%d]",(int)left_cache_.is_empty());
  int ret = OB_SUCCESS;
  const ObRow *right_row = NULL;
  // fetch the next left row
  if (NULL != last_left_row_)
  {
    curr_cached_left_row_ = *last_left_row_;
    last_left_row_ = NULL;
    last_left_row_has_printed_ = true;
  }
  else
  {
    ret = left_cache_.get_next_row(curr_cached_left_row_);
    last_left_row_has_printed_ = false;
  }
  while(OB_SUCCESS == ret)
  {
    if (right_cache_is_valid_)
    {
      OB_ASSERT(!right_cache_.is_empty());
      int cmp = 0;
      if (OB_SUCCESS != (ret = left_row_compare_equijoin_cond(curr_cached_left_row_, last_join_left_row_, cmp)))
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
                if (OB_SUCCESS != (ret = left_join_rows(curr_cached_left_row_)))
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
                ret = left_cache_.get_next_row(curr_cached_left_row_);
                last_left_row_has_printed_ = false;
            }
            //add 20140610:e
          }
        }
        else
        {
          bool is_qualified = false;
          if (OB_SUCCESS != (ret = join_rows(curr_cached_left_row_, curr_cached_right_row_)))
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
            last_left_row_ = &curr_cached_left_row_;
            break;
          }
          else
          {
            // continue with the next cached right row
            OB_ASSERT(NULL == last_left_row_);
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
        //del dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
        //bool is_qualified = false;
        //del 20140610:e
        if (OB_SUCCESS != (ret = left_join_rows(curr_cached_left_row_)))
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
        ret = right_op_->get_next_row(right_row);;
        if (OB_SUCCESS != ret)
        {
          if (OB_ITER_END == ret)
          {
            //TBSYS_LOG(INFO, "end of right child op");
            is_right_iter_end_ = true;
            if (!right_cache_.is_empty())
            {
              // no more right rows and the right cache is not empty, we SHOULD look at the next left row
              right_cache_is_valid_ = true;
              OB_ASSERT(NULL == last_right_row_);
              OB_ASSERT(NULL == last_left_row_);
              if (!last_left_row_has_printed_)
              {
                  if (OB_SUCCESS != (ret = left_join_rows(curr_cached_left_row_)))
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
                  ret = left_cache_.get_next_row(curr_cached_left_row_);
                  last_left_row_has_printed_ = false;
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
            TBSYS_LOG(WARN, "failed to get next row from right child, err=%d", ret);
            break;
          }
        }
      }
      OB_ASSERT(right_row);
      int cmp = 0;
      if (OB_SUCCESS != (ret = compare_equijoin_cond(curr_cached_left_row_, *right_row, cmp)))
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
          if (OB_SUCCESS != (ret = ObRowUtil::convert(curr_cached_left_row_, last_join_left_row_store_, last_join_left_row_)))
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
        else if (OB_SUCCESS != (ret = join_rows(curr_cached_left_row_, *right_row)))
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
          last_left_row_ = &curr_cached_left_row_;
          OB_ASSERT(NULL == last_right_row_);
          break;
        }
        else
        {
          // continue with the next right row
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
          //ret = left_cache_.get_next_row(curr_cached_left_row_);
          //del 20140610:e
          //add dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
          if (!last_left_row_has_printed_)
          {
              if (OB_SUCCESS != (ret = left_join_rows(curr_cached_left_row_)))
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
              ret = left_cache_.get_next_row(curr_cached_left_row_);
              last_left_row_has_printed_ = false;
          }
        }
        else
        {
          //del dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
          //bool is_qualified = false;
          //del 20140610:e
          if (OB_SUCCESS != (ret = left_join_rows(curr_cached_left_row_)))
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
        OB_ASSERT(NULL == last_left_row_);
        OB_ASSERT(NULL == last_right_row_);
      }
    }
  } // end while
  return ret;
}

int ObBloomFilterJoin::full_outer_get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRow *left_row = NULL;
  const ObRow *right_row = NULL;
  // fetch the next left row
  if (NULL != last_left_row_)
  {
    curr_cached_left_row_ = *last_left_row_;
    last_left_row_ = NULL;
  }
  else
  {
    ret = left_cache_.get_next_row(curr_cached_left_row_);
  }

  while(OB_SUCCESS == ret)
  {
    if (right_cache_is_valid_)
    {
      OB_ASSERT(!right_cache_.is_empty());
      int cmp = 0;
      if (OB_SUCCESS != (ret = left_row_compare_equijoin_cond(curr_cached_left_row_, last_join_left_row_, cmp)))
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
            ret = left_cache_.get_next_row(curr_cached_left_row_);
          }
        }
        else
        {
          bool is_qualified = false;
          if (OB_SUCCESS != (ret = join_rows(curr_cached_left_row_, curr_cached_right_row_)))
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
            last_left_row_ = &curr_cached_left_row_;
            break;
          }
          else
          {
            // continue with the next cached right row
            OB_ASSERT(NULL == last_left_row_);
            //OB_ASSERT(NULL != left_row);
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
        if (OB_SUCCESS != (ret = left_join_rows(curr_cached_left_row_)))
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
          ret = left_cache_.get_next_row(curr_cached_left_row_);
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
        ret = right_op_->get_next_row(right_row);;
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
              ret = left_cache_.get_next_row(curr_cached_left_row_);
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
      OB_ASSERT(right_row);
      int cmp = 0;
      if (OB_SUCCESS != (ret = compare_equijoin_cond(curr_cached_left_row_, *right_row, cmp)))
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
          if (OB_SUCCESS != (ret = ObRowUtil::convert(curr_cached_left_row_, last_join_left_row_store_, last_join_left_row_)))
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
        else if (OB_SUCCESS != (ret = join_rows(curr_cached_left_row_, *right_row)))
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
          last_left_row_ = &curr_cached_left_row_;
          OB_ASSERT(NULL == last_right_row_);
          break;
        }
        else
        {
          // continue with the next right row
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
          ret = left_cache_.get_next_row(curr_cached_left_row_);
        }
        else
        {
          bool is_qualified = false;
          if (OB_SUCCESS != (ret = left_join_rows(curr_cached_left_row_)))
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
            ret = left_cache_.get_next_row(curr_cached_left_row_);
            last_right_row_ = right_row;
          }
        }
      }
      else
      {
        // left_row > right_row on euqijoin conditions
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
          last_left_row_ = &curr_cached_left_row_;
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
        ret = right_op_->get_next_row(right_row);;
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


int ObBloomFilterJoin::left_rows(const common::ObRow *&row,int &last_left_hash_id_)
{
  //TBSYS_LOG(ERROR,"DHC last_hash_i=%d",last_left_hash_id_);
  int ret = OB_SUCCESS;
  for(;last_left_hash_id_<HASH_BUCKET_NUM;)
  {
    if (OB_SUCCESS != (ret = hash_table_row_store[(int)last_left_hash_id_].row_store.get_next_row(curr_cached_left_row_)))
    {
      if (OB_UNLIKELY(OB_ITER_END != ret))
      {
        TBSYS_LOG(WARN, "failed to get next row from hash_table, err=%d", ret);
      }
      else
      {
        last_left_hash_id_++;
        hash_table_row_store[(int)last_left_hash_id_].id_no_ = 0;
        hash_table_row_store[(int)last_left_hash_id_].row_store.reset_iterator();
        continue;
      }
    }
    else {
      hash_table_row_store[(int)last_left_hash_id_].id_no_++;
      //TBSYS_LOG(ERROR,"left_rows i=%d %s %d",last_left_hash_id_,to_cstring(curr_cached_left_row_),curr_cached_left_row_.get_raw_row().get_reserved2());
      if(1==hash_table_row_store[(int)last_left_hash_id_].hash_iterators.at(hash_table_row_store[(int)last_left_hash_id_].id_no_-1))
      {
        continue;
      }
      row = &curr_cached_left_row_;
      break;
    }
  }
  return ret;
}


int ObBloomFilterJoin::compare_hash_equijoin(const ObRow *&r1, const ObRow& r2, int &cmp,bool left_hash_id_cache_valid_,int &last_hash_id_)
{
  //TBSYS_LOG(ERROR, "dhc ObBloomFilterJoin::compare_hash_equijoin()");
  int ret = OB_SUCCESS;
  uint64_t bucket_num = 0;
  const ObObj *res1 = NULL;
  const ObObj *res2 = NULL;
  ObExprObj obj1;
  ObExprObj obj2;
  if(!left_hash_id_cache_valid_)
  {
    for (int64_t i = 0; i < con_length_; ++i)
    {
      const ObSqlExpression &expr = equal_join_conds_.at(i);
      ExprItem::SqlCellInfo c1;
      ExprItem::SqlCellInfo c2;
      const ObObj *temp;
      if (expr.is_equijoin_cond(c1, c2))
      {
        if (OB_SUCCESS != (ret = r2.get_cell(c2.tid,c2.cid,temp)))
        {
          TBSYS_LOG(ERROR, "failed to get cell, err=%d tid=%lu cid=%lu", ret, c2.tid, c2.cid);
          break;
        }
        else
        {
          bucket_num = temp->murmurhash64A(bucket_num);
        }
      }
    }
    bucket_num = bucket_num%HASH_BUCKET_NUM;
    last_hash_id_ = (int)bucket_num;
    hash_table_row_store[(int)bucket_num].row_store.reset_iterator();
    hash_table_row_store[(int)bucket_num].id_no_=0;
  }
  else
  {
    bucket_num = last_hash_id_;
  }
  while(OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = hash_table_row_store[(int)bucket_num].row_store.get_next_row(curr_cached_left_row_)))
    {
      if (OB_UNLIKELY(OB_ITER_END != ret))
      {
        TBSYS_LOG(WARN, "failed to get next row from hash_table, err=%d", ret);
      }
      left_hash_id_cache_valid_ = false;
      break;
    }
    else
    {
      hash_table_row_store[(int)bucket_num].id_no_++;
      for (int64_t ii = 0; ii < con_length_; ++ii)
      {
        const ObSqlExpression &expr = equal_join_conds_.at(ii);
        ExprItem::SqlCellInfo c1;
        ExprItem::SqlCellInfo c2;
        if (expr.is_equijoin_cond(c1, c2))
        {
          if (OB_SUCCESS != (ret = curr_cached_left_row_.get_cell(c1.tid,c1.cid,res1)))
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
                cmp = -10;
              }
              else
              {
                cmp = 10;
              }
              break;
            }
            else if (0 != cmp)
            {
              break;
            }
          }
        }
      }
      if(cmp==0)
      {
        r1 = &curr_cached_left_row_;
        break;
      }
    }
  }
  return ret;
}


int ObBloomFilterJoin::inner_hash_get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRow *right_row = NULL;
  const ObRow *left_row = NULL;
  while(OB_SUCCESS == ret)
  {
    if (left_hash_id_cache_valid_)
    {
      int cmp = -10;
      if (OB_SUCCESS != (ret = compare_hash_equijoin(left_row, *last_right_row_, cmp,left_hash_id_cache_valid_,last_hash_id_)))
      {
        if(OB_ITER_END == ret)
        {
          cmp = -10;
        }
      }
      if (0 == cmp)
      {
        bool is_qualified = false;
        if (OB_SUCCESS != (ret = join_rows(*left_row, *last_right_row_)))
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
          hash_table_row_store[(int)last_hash_id_].hash_iterators.at(hash_table_row_store[(int)last_hash_id_].id_no_-1) = 1;
          //TBSYS_LOG(ERROR,"curr_hash_row %s",to_cstring(*left_row));
          break;
        }
      }
      else
      {
        left_hash_id_cache_valid_ = false;
        ret = OB_SUCCESS;
      }
    }
    else
    {
      // fetch the next right row
      ret = right_op_->get_next_row(right_row);
      //TBSYS_LOG(ERROR, "get_next_row %d",numi++);
      if (OB_SUCCESS != ret)
      {
        if (OB_ITER_END == ret)
        {
          TBSYS_LOG(DEBUG, "end of right child op");
          is_right_iter_end_ = true;
          break;
        }
        else
        {
          TBSYS_LOG(WARN, "failed to get next row from right child, err=%d", ret);
          break;
        }
      }
      //TBSYS_LOG(ERROR,"DHC etch the next right row ret=[%d] row=%s",ret,to_cstring(*right_row));
      OB_ASSERT(right_row);
      int cmp = -10;
      if (OB_SUCCESS != (ret = compare_hash_equijoin(left_row, *right_row, cmp,left_hash_id_cache_valid_,last_hash_id_)))
      {
        if(OB_ITER_END == ret)
        {
          cmp = -10;
        }
      }
      if (0 == cmp)
      {
        left_hash_id_cache_valid_ = true;
        bool is_qualified = false;
        if (OB_SUCCESS != (ret = join_rows(*left_row, *right_row)))
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
          hash_table_row_store[(int)last_hash_id_].hash_iterators.at(hash_table_row_store[(int)last_hash_id_].id_no_-1) = 1;
          //TBSYS_LOG(ERROR,"curr_hash_row %s",to_cstring(*left_row));
          break;
        }
      }
    }
  } // end while

  return ret;
}


int ObBloomFilterJoin::left_hash_outer_get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRow *left_row = NULL;
  const ObRow *inner_hash_row = NULL;
  if(last_left_hash_id_ == -1)
  {
    if (OB_SUCCESS != (ret = inner_hash_get_next_row(inner_hash_row)))
    {
      if (OB_UNLIKELY(OB_ITER_END != ret))
      {
        TBSYS_LOG(WARN, "failed to get next row from hash_table, err=%d", ret);
      }
      else{
        last_left_hash_id_ = 0;
        hash_table_row_store[(int)last_left_hash_id_].id_no_ = 0;
        hash_table_row_store[(int)last_left_hash_id_].row_store.reset_iterator();
      }
    }
    else{
      row = inner_hash_row;
    }
  }
  if(last_left_hash_id_ != -1){
    if (OB_SUCCESS != (ret = left_rows(left_row,last_left_hash_id_)))
    {
      if (OB_UNLIKELY(OB_ITER_END != ret))
      {
        TBSYS_LOG(WARN, "failed to get next row from hash_table, err=%d", ret);
      }
    }
    else
    {
      //TBSYS_LOG(ERROR,"DHC last_hash_i=%d",last_left_hash_id_);
      if (OB_SUCCESS != (ret = left_join_rows(*left_row)))
      {
        TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
      }
      else
      {
         //TBSYS_LOG(ERROR,"dhc right_row=%s  row=%s",to_cstring(*left_row),to_cstring(curr_row_));
         row = &curr_row_;
      }
    }
  }
  return ret;
}



namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObBloomFilterJoin, PHY_BLOOMFILTER_JOIN);
  }
}

PHY_OPERATOR_ASSIGN(ObBloomFilterJoin)
{
  int ret = OB_SUCCESS;
  UNUSED(other);
  reset();
  return ret;
}
