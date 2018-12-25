/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_merger_sorted_operator.cpp for
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#include "ob_ms_sql_sorted_operator.h"
#include <algorithm>
#include "sql/ob_sql_scan_param.h"
#include "common/ob_new_scanner.h"
#include "common/ob_range2.h"
#include "ob_ms_request_result.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::mergeserver;
using namespace std;

void oceanbase::mergeserver::ObMsSqlSortedOperator::sharding_result_t::init(ObMsRequestResult & sharding_res, const ObNewRange & query_range,
  const ObSqlScanParam &param, const int64_t fullfilled_item_num)
{
  sharding_res_ = &sharding_res;
  sharding_query_range_ = &query_range;
  param_ = &param;
  fullfilled_item_num_ = fullfilled_item_num;
}

bool oceanbase::mergeserver::ObMsSqlSortedOperator::sharding_result_t::operator <(const sharding_result_t &other) const
{
  bool res = false;
  res = (sharding_query_range_->compare_with_startkey2(*other.sharding_query_range_) < 0);
  return res;
}

oceanbase::mergeserver::ObMsSqlSortedOperator::ObMsSqlSortedOperator()
{
  reset();
}


oceanbase::mergeserver::ObMsSqlSortedOperator::~ObMsSqlSortedOperator()
{
  //added  by fyd  [PrefixKeyQuery_for_INstmt]2014.7.15
  used_rowkey_.clear();
  //add:e
}

void oceanbase::mergeserver::ObMsSqlSortedOperator::reset()
{
  cur_sharding_result_idx_ = 0;
  scan_param_ = NULL;
  seamless_result_count_  = 0;
  total_mem_size_used_ = 0;
  sharding_result_arr_.clear();
  //added  by fyd  [PrefixKeyQuery_for_INstmt]2014.7.15
  used_rowkey_.clear();
  //add:e
}

int oceanbase::mergeserver::ObMsSqlSortedOperator::set_param(const ObSqlScanParam & scan_param)
{
  int err = OB_SUCCESS;
  reset();
  scan_param_ = &scan_param;
  scan_range_ = *scan_param_->get_range();
  //added  by fyd  [PrefixKeyQuery_for_INstmt]2014.7.15
  first_scan_range_ = scan_range_;
  this->used_rowkey_.push_back(first_scan_range_);
  //add:e
  return err;
}
//added  by fyd  [PrefixKeyQuery_for_INstmt]2014.4.1
int oceanbase::mergeserver::ObMsSqlSortedOperator::set_param_only(const ObSqlScanParam & scan_param)
{
  int err = OB_SUCCESS;
  //TBSYS_LOG(DEBUG, "ObMsSqlSortedOperator::set_param_only");
  scan_param_ = &scan_param;
  scan_range_ = *scan_param_->get_range();
  return err;
}
//add:e
// note: keep in mind that "readed" scanners' rowkey buffer were freed already
void oceanbase::mergeserver::ObMsSqlSortedOperator::sort(bool &is_finish, ObMsRequestResult * last_sharding_res)
{
  /*
   * 1. sort unread scanners
   * 2. check if first sorted unread scanner's start key equals to last seamed end key
   * 3. if seamed, check if finish
   * 4. update sort params
   *
   * sharding_result_count_: total scanner received
   * cur_sharding_result_idx_: current readable scanner index
   * seamless_result_count_: seamed scanners
   *
   */
  int64_t seamless_result_idx = 0;
  // sort 'clean' scanners
  sharding_result_t *first = &sharding_result_arr_.at(0);
  sharding_result_t *start = &sharding_result_arr_.at(cur_sharding_result_idx_);
  int64_t count = sharding_result_arr_.count();
  std::sort(start, first+count);
  if (seamless_result_count_ <= 0)
  {
	  //  modified by fyd  [PrefixKeyQuery_for_INstmt] 2014.7.15:b
    // no seamless result
//    if ((0 == sharding_result_arr_[0].sharding_query_range_->compare_with_startkey2(scan_range_))
//        || (sharding_result_arr_[0].sharding_query_range_->start_key_.is_min_row()))
//    {
//      seamless_result_count_ = 1;
//    }
	  TBSYS_LOG(DEBUG,"start_scan_range=%s",to_cstring(first_scan_range_));
	  TBSYS_LOG(DEBUG,"current_scan_range=%s",to_cstring(scan_range_));
	  if ((0 == sharding_result_arr_[0].sharding_query_range_->compare_with_startkey2(first_scan_range_))
		  || (sharding_result_arr_[0].sharding_query_range_->start_key_.is_min_row()))
	  {
		seamless_result_count_ = 1;
	  }
  }
  if (seamless_result_count_ > 0)
  {
    // at lease one seamless result
    for (seamless_result_idx = seamless_result_count_;
         seamless_result_idx < sharding_result_arr_.count();
        seamless_result_idx++)
    {

      //del:b
//      if (sharding_result_arr_[seamless_result_idx - 1].last_row_key_
//          == sharding_result_arr_[seamless_result_idx].sharding_query_range_->start_key_)
     //del:e
      int32_t  cmp_ret = sharding_result_arr_[seamless_result_idx - 1].sharding_query_range_->end_key_.compare(sharding_result_arr_[seamless_result_idx].sharding_query_range_->start_key_);
      if ( cmp_ret == 0 )
      {
        seamless_result_count_ = seamless_result_idx + 1;
        TBSYS_LOG(DEBUG, "1.prev last_row_key=%s, cur star_key_=%s, seamless=%ld",
            to_cstring(sharding_result_arr_[seamless_result_idx - 1].sharding_query_range_->end_key_),
            to_cstring(sharding_result_arr_[seamless_result_idx].sharding_query_range_->start_key_),
            seamless_result_count_
            );
      }
      else if ( cmp_ret < 0 )
      {
    	      const common::ObRowkey &last_row_key = sharding_result_arr_[seamless_result_idx - 1].sharding_query_range_->end_key_;
    	      const common::ObRowkey &fist_row_key = sharding_result_arr_[seamless_result_idx].sharding_query_range_->start_key_;
//    	      const common::ObObj *obj1= (last_row_key_.get_obj_ptr()+last_row_key_.get_obj_cnt()-1);
//    	      const common::ObObj *obj2= (fist_row_key_.get_obj_ptr()+last_row_key_.get_obj_cnt()-1);
    	      TBSYS_LOG(DEBUG,"last row key count =%ld,first count=%ld", last_row_key.get_obj_cnt(),last_row_key.get_obj_cnt());
    	      TBSYS_LOG(DEBUG,"last row key obj =%s,first  obj2=%s", to_cstring(*(last_row_key.get_obj_ptr()+last_row_key.get_obj_cnt()-1)),to_cstring(*(fist_row_key.get_obj_ptr()+last_row_key.get_obj_cnt()-1)));
//    	      const common::ObNewRange* publish =used_rowkey_.at(0);
    	      TBSYS_LOG(DEBUG,"count =%d,last pulished start  rowkey =%s",(int32_t)this->used_rowkey_.size(), to_cstring(used_rowkey_.at(0).start_key_));
    	      // change range?
    	      if (used_rowkey_.size() >1 && 0 == used_rowkey_.at(1).start_key_.compare(fist_row_key) && 0 == used_rowkey_.at(0).end_key_.compare(last_row_key))
    	      {
    	    	  seamless_result_count_ = seamless_result_idx + 1;
    	          used_rowkey_.erase(used_rowkey_.begin());
    	          TBSYS_LOG(DEBUG, "2.prev last_row_key=%s, cur star_key_=%s, seamless=%ld,used_rowkey_ count=%d",
    	              to_cstring(sharding_result_arr_[seamless_result_idx - 1].sharding_query_range_->end_key_),
    	              to_cstring(sharding_result_arr_[seamless_result_idx].sharding_query_range_->start_key_),
    	              seamless_result_count_, (int32_t)used_rowkey_.size());
    	      }
    	      else if (1 == used_rowkey_.size() && 0 == used_rowkey_.at(0).start_key_.compare(fist_row_key) )
    	      {
    	    	  seamless_result_count_ = seamless_result_idx + 1;
    	          used_rowkey_.erase(used_rowkey_.begin());
    	          TBSYS_LOG(DEBUG, "3.prev last_row_key=%s, cur star_key_=%s, seamless=%ld,used_rowkey_ count=%d",
    	              to_cstring(sharding_result_arr_[seamless_result_idx - 1].sharding_query_range_->end_key_),
    	              to_cstring(sharding_result_arr_[seamless_result_idx].sharding_query_range_->start_key_),
    	              seamless_result_count_, (int32_t)used_rowkey_.size());
    	      }
    	      else
    	      {
    	    	  break;
    	      }
      }
      else
      {
          TBSYS_LOG(ERROR, "start_key must be greater than or equal to end_key, prev last_row_key=%s, cur star_key_=%s, seamless=%ld",
          to_cstring(sharding_result_arr_[seamless_result_idx - 1].sharding_query_range_->end_key_),
          to_cstring(sharding_result_arr_[seamless_result_idx].sharding_query_range_->start_key_),
          seamless_result_count_
          );
          break;
      }

//      else
//      {
//        break;
//      }
        //mod:e
    }
  }

  if (seamless_result_count_ > 0) // implicates that startkey matched already
  {
    OB_ASSERT(NULL != last_sharding_res);
    TBSYS_LOG(DEBUG, "last seamless=%s", to_cstring(sharding_result_arr_[seamless_result_count_-1].sharding_query_range_->end_key_));
    if (sharding_result_arr_[seamless_result_count_-1].sharding_query_range_->end_key_ >= scan_range_.end_key_ ||
        sharding_result_arr_[seamless_result_count_-1].sharding_query_range_->end_key_.is_max_row())
    {
      /* check last seemless result.
       * Finish the whole scan if last scanner fullfilled and its end_key_ equals to scan_range's end_key_
       */
      TBSYS_LOG(DEBUG, "biubiu. seamless_result_count_=%ld, start_key=%s, end_key=%s",
          seamless_result_count_,
          to_cstring(sharding_result_arr_[seamless_result_count_-1].sharding_query_range_->start_key_),
          to_cstring(sharding_result_arr_[seamless_result_count_-1].sharding_query_range_->end_key_));
      is_finish = true;
    }
    else
    {
      is_finish = false;
    }
  }
}

int ObMsSqlSortedOperator::add_sharding_result(ObMsRequestResult & sharding_res, const ObNewRange &query_range,
  bool &is_finish)
{
  int64_t fullfilled_item_num = 0;
  int err = OB_SUCCESS;

  if ((OB_SUCCESS == err) && (NULL == scan_param_))
  {
    TBSYS_LOG(WARN,"operator was not initialized yet [scan_param_:%p]", scan_param_);
    err = OB_INVALID_ARGUMENT;
  }
  fullfilled_item_num = sharding_res.get_row_num();
  total_mem_size_used_ += sharding_res.get_used_mem_size();
  sharding_result_t sr;
  sr.init(sharding_res,query_range, *scan_param_, fullfilled_item_num);
  if (OB_SUCCESS == err)
  {
    if (OB_SUCCESS != (err = sharding_result_arr_.push_back(sr)))
    {
      TBSYS_LOG(WARN, "failed to add sharding result, err=%d", err);
    }
  }

  is_finish = false;
  if (OB_SUCCESS == err)
  {
    sort(is_finish, &sharding_res);
  }
  TBSYS_LOG(DEBUG, "add sharding result. sharding_result_count_=%ld, is_finish=%d, err=%d", sharding_result_arr_.count(), is_finish, err);

  if (1)
  {
    static __thread int64_t total_row_num = 0;
    total_row_num += fullfilled_item_num;
    TBSYS_LOG(DEBUG, "query_range=%s,fullfilled_item_num=%ld,total=%ld, is_finish=%d",
        to_cstring(query_range), fullfilled_item_num,total_row_num,is_finish);
  }
  return err;
}


///////////////////////////////////////
//////////// Row inferface ////////////
///////////////////////////////////////

int oceanbase::mergeserver::ObMsSqlSortedOperator::get_next_row(oceanbase::common::ObRow &row)
{
  int err = OB_SUCCESS;
  while (OB_SUCCESS == err)
  {
    if (cur_sharding_result_idx_ >= seamless_result_count_)
    {
      err = OB_ITER_END;
    }
    else if (OB_SUCCESS == err)
    {
      if (OB_SUCCESS ==(err = sharding_result_arr_[cur_sharding_result_idx_].sharding_res_->get_next_row(row)))
      {
        break;
      }
      else if (OB_ITER_END == err)
      {
        total_mem_size_used_ -= sharding_result_arr_[cur_sharding_result_idx_].sharding_res_->get_used_mem_size();
        // since this sharding will never be used again, release it!
        sharding_result_arr_[cur_sharding_result_idx_].sharding_res_->clear();
        cur_sharding_result_idx_ ++;
        err = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN,"fail to get next cell from ObNewScanner [idx:%ld,err:%d]", cur_sharding_result_idx_, err);
      }
    }
  }
  return err;
}
