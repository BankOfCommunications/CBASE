/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * /home/jianming.cjq/ss_g/src/sql/ob_multiple_get_merge.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_multiple_get_merge.h"
#include "common/ob_row_fuse.h"
//add wenghaixing [secondary index upd] 20141204
#include "ob_inc_scan.h"
//add e
using namespace oceanbase;
using namespace sql;

void ObMultipleGetMerge::reset()
{
  ObMultipleMerge::reset();
}

void ObMultipleGetMerge::reuse()
{
  ObMultipleMerge::reuse();
}

int ObMultipleGetMerge::open()
{
  int ret = OB_SUCCESS;
  const ObRowDesc *row_desc = NULL;

  for (int32_t i = 0; OB_SUCCESS == ret && i < child_num_; i ++)
  {
    if (OB_SUCCESS != (ret = child_array_[i]->open()))
    {
      TBSYS_LOG(WARN, "fail to open child i[%d]:ret[%d]", i, ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = get_row_desc(row_desc)))
    {
      TBSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
    }
    else
    {
      cur_row_.set_row_desc(*row_desc);
    }
  }
// add by maosy [Delete_Update_Function_isolation_RC] 20161228
  //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//  if (OB_SUCCESS == ret && data_mark_param_.is_valid())
//  {
//      data_mark_final_row_.set_row_desc(data_mark_final_row_desc_);
//  }
//add e
  if (data_mark_param_.is_valid())
  {
      TBSYS_LOG(DEBUG,"mul_del::debug,final ObMultipleGetMerge param[%s],ret=%d",
                to_cstring(data_mark_param_),ret);
  }
  //add duyr 20160531:e

  return ret;
}

int ObMultipleGetMerge::close()
{
  int ret = OB_SUCCESS;

  for (int32_t i = 0; i < child_num_; i ++)
  {
    if (OB_SUCCESS != (ret = child_array_[i]->close()))
    {
      TBSYS_LOG(WARN, "fail to close child i[%d]:ret[%d]", i, ret);
    }
  }

  return ret;
}

// add by maosy [Delete_Update_Function_isolation_RC] 20161228
/*
int ObMultipleGetMerge::get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRow *tmp_row = NULL;
  bool is_row_empty = true;
  if (child_num_ <= 0)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "has no child");
  }
  while (OB_SUCCESS == ret)
  {
    is_row_empty = true;
    cur_row_.reset(false, is_ups_row_ ? ObRow::DEFAULT_NOP : ObRow::DEFAULT_NULL);
    for (int32_t i = 0; OB_SUCCESS == ret && i < child_num_; i++)
    {
      ret = child_array_[i]->get_next_row(tmp_row);
      if (OB_SUCCESS != ret)
      {
        if (OB_ITER_END == ret)
        {
          if ( 0 != i)
          {
            ret = OB_ERROR;
            TBSYS_LOG(WARN, "should not be iter end[%d]", i);
          }
          else
          {
            int err = OB_SUCCESS;
            for (int32_t k = 1; OB_SUCCESS == err && k < child_num_; k ++)
            {
              err = child_array_[k]->get_next_row(tmp_row);
              if (OB_ITER_END != err)
              {
                err = OB_ERR_UNEXPECTED;
                ret = err;
                TBSYS_LOG(WARN, "should be iter end[%d], i=%d", k, i);
              }
            }
          }
        }
        else
        {
          if (!IS_SQL_ERR(ret))
          {
            TBSYS_LOG(WARN, "fail to get next row:ret[%d]", ret);
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
          // mod by maosy [Delete_Update_Function_isolation_RC] 20161218
//        TBSYS_LOG(DEBUG, "multiple get merge child[%d] row[%s]", i, to_cstring(*tmp_row));
//        //mod duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//        if (OB_SUCCESS != (ret = common::ObRowFuse::fuse_row(*tmp_row, cur_row_, is_row_empty, is_ups_row_)))
//        ObDataMark tmp_data_mark;
//        if (data_mark_param_.is_valid()
//            && OB_SUCCESS != (ret = ObDataMarkHelper::get_data_mark(tmp_row,data_mark_param_,tmp_data_mark)))
 //       {
 //           TBSYS_LOG(WARN,"fail to get data mark!row=[%s],data_mark=[%s],ret=%d",
 //                     to_cstring(*tmp_row),to_cstring(tmp_data_mark),ret);
//        }
//        else if (0 < i
 //                && data_mark_param_.need_modify_time_
 //                && data_mark_param_.is_valid()
//                 && tmp_data_mark.modify_time_ != cur_data_mark_.modify_time_)
//        {
//            ret = OB_UD_PARALLAL_DATA_NOT_SAFE;
//            TBSYS_LOG(ERROR,"data is not safe!prev_data_mark=[%s],cur_data_mark[%s],"
//                            "prev_row=[%s],cur_row=[%s],ret=%d",
//                            to_cstring(cur_data_mark_),to_cstring(tmp_data_mark),
//                            to_cstring(cur_row_),to_cstring(*tmp_row),ret);
 //       }
//        else if (OB_SUCCESS != (ret = common::ObRowFuse::fuse_row(*tmp_row, cur_row_, is_row_empty, is_ups_row_)))
 //       //mod duyr 20160531:e
 //       {
 //         TBSYS_LOG(WARN, "fail to fuse row:ret[%d]", ret);
 //       }
 //       else if (0 == i)
 //       {
 //         if (OB_SUCCESS != (ret = copy_rowkey(*tmp_row, cur_row_, false)))
//          {
 //           TBSYS_LOG(WARN, "fail to copy rowkey:ret[%d]", ret);
 //         }
 //       }

        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
 //       if (data_mark_param_.is_valid())
 //       TBSYS_LOG(DEBUG, "mul_del::debug,multiple get merge child[%d] row[%s],prev_data_mark=[%s],cur_data_mark=[%s],ret=%d",
//                  i, to_cstring(*tmp_row),to_cstring(cur_data_mark_),to_cstring(tmp_data_mark),ret);


 //       if (OB_SUCCESS == ret && data_mark_param_.is_valid())
//        {
 //           cur_data_mark_ = tmp_data_mark;
 //       }
 //       //add duyr 20160531:e
//      }
                if( i > 0 && data_mark_param_.is_valid())
                {
                    ObDataMark tmp_data_mark;
                    if (OB_SUCCESS != (ret = ObDataMarkHelper::get_data_mark(tmp_row,data_mark_param_,tmp_data_mark)))
                    {
                        TBSYS_LOG(WARN,"fail to get data mark!row=[%s],data_mark=[%s],ret=%d",
                                  to_cstring(*tmp_row),to_cstring(tmp_data_mark),ret);
                    }
                    else if (tmp_data_mark.modify_time_!=INT64_MAX
                             &&tmp_data_mark.modify_time_!=OB_INVALID_DATA
                             && data_mark_param_.need_modify_time_
                             && stmt_start_time_< tmp_data_mark.modify_time_
                             )
                    {
                        ret = OB_UD_PARALLAL_DATA_NOT_SAFE;
                        TBSYS_LOG(ERROR,"data is not safe!prev_data_mark=[%ld],start_time =%ld,"
                                  "prev_row=[%s],cur_row=[%s],ret=%d",
                                  tmp_data_mark.modify_time_,get_start_time (),
                                  to_cstring(cur_row_),to_cstring(*tmp_row),ret);
                    }
                    else
                    {
                        if (OB_SUCCESS != (ret = common::ObDataMarkHelper::convert_data_mark_row_to_normal_row(data_mark_param_,
                                                                                                               tmp_row,
                                                                                                               &data_mark_final_row_)))
                        {
                            TBSYS_LOG(WARN,"fail to convert data mark row to normal row!orig_row=[%s],final_row=[%s],ret=%d",
                                      to_cstring(cur_row_),to_cstring(data_mark_final_row_),ret);
                        }
                    }
                    TBSYS_LOG(DEBUG, "data is !prev_data_mark=[%s],start_time =%ld,"
                              "cur_row=[%s],tmp_row=[%s],ret=%d",
                              to_cstring(tmp_data_mark),get_start_time (),
                              to_cstring(cur_row_),to_cstring(data_mark_final_row_),ret);
                }
                if(ret ==OB_SUCCESS)
                {
                    if(i ==0 || !data_mark_param_.is_valid ())
                    {
                        if (OB_SUCCESS != (ret = common::ObRowFuse::fuse_row(*tmp_row, cur_row_, is_row_empty, is_ups_row_)))
                        {
                            TBSYS_LOG(WARN, "fail to fuse row:ret[%d]", ret);
                        }
                        if (i==0 && OB_SUCCESS != (ret = copy_rowkey(*tmp_row, cur_row_, false)))
                        {
                            TBSYS_LOG(WARN, "fail to copy rowkey:ret[%d]", ret);
                        }
                        TBSYS_LOG(DEBUG,"current = %s",to_cstring(cur_row_));
                    }
                    else if(OB_SUCCESS != (ret = common::ObRowFuse::fuse_row(data_mark_final_row_, cur_row_, is_row_empty, is_ups_row_)))
                    {
                         TBSYS_LOG(WARN, "fail to fuse row:ret[%d]", ret);
                    }
                }
            }
      //mod by maosy
    }
    if (OB_SUCCESS == ret)
    {
      if (is_ups_row_ || !is_row_empty)
      {
        break;
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    row = &cur_row_;
    //TBSYS_LOG(DEBUG, "multiple get merged row[%s]", to_cstring(*row));
    // mod by maosy [Delete_Update_Function_isolation_RC] 20161218
    //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
  //  if (data_mark_param_.is_valid())
 //   {
  //      if (OB_SUCCESS != (ret = common::ObDataMarkHelper::convert_data_mark_row_to_normal_row(data_mark_param_,
  //                                                                                             &cur_row_,
  //                                                                                             &data_mark_final_row_)))
  //      {
 //           TBSYS_LOG(WARN,"fail to convert data mark row to normal row!orig_row=[%s],final_row=[%s],ret=%d",
 //                     to_cstring(cur_row_),to_cstring(data_mark_final_row_),ret);
  //      }
 //       else
 //       {
  //          row = &data_mark_final_row_;

//        }
 //       TBSYS_LOG(DEBUG,"mul_del::debug,multiple get merge final row=[%s]",to_cstring(*row));
//    }
    //add duyr 20160531:e
  //del by maosy
  }
  return ret;
}
*/
//del e 
int ObMultipleGetMerge::get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRow *tmp_row = NULL;
  bool is_row_empty = true;
  if (child_num_ <= 0)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "has no child");
  }
  while (OB_SUCCESS == ret)
  {
    is_row_empty = true;
    cur_row_.reset(false, is_ups_row_ ? ObRow::DEFAULT_NOP : ObRow::DEFAULT_NULL);
    for (int32_t i = 0; OB_SUCCESS == ret && i < child_num_; i++)
    {
      ret = child_array_[i]->get_next_row(tmp_row);
      if (OB_SUCCESS != ret)
      {
        if (OB_ITER_END == ret)
        {
          if ( 0 != i)
          {
            ret = OB_ERROR;
            TBSYS_LOG(WARN, "should not be iter end[%d]", i);
          }
          else
          {
            int err = OB_SUCCESS;
            for (int32_t k = 1; OB_SUCCESS == err && k < child_num_; k ++)
            {
              err = child_array_[k]->get_next_row(tmp_row);
              if (OB_ITER_END != err)
              {
                err = OB_ERR_UNEXPECTED;
                ret = err;
                TBSYS_LOG(WARN, "should be iter end[%d], i=%d", k, i);
              }
            }
          }
        }
        else
        {
          if (!IS_SQL_ERR(ret))
          {
            TBSYS_LOG(WARN, "fail to get next row:ret[%d]", ret);
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG, "multiple get merge child[%d] row[%s]", i, to_cstring(*tmp_row));
        if( i > 0 && data_mark_param_.is_valid())
        {
            ObDataMark tmp_data_mark;
            ObRow final_row ;
            const ObRowDesc row_desc ;
            final_row.set_row_desc (row_desc);
            if (OB_SUCCESS != (ret = ObDataMarkHelper::get_data_mark(tmp_row,data_mark_param_,tmp_data_mark)))
            {
                TBSYS_LOG(WARN,"fail to get data mark!row=[%s],data_mark=[%s],ret=%d",
                          to_cstring(*tmp_row),to_cstring(tmp_data_mark),ret);
            }
            else if (data_mark_param_.need_modify_time_&& tmp_data_mark.modify_time_ != INT64_MAX )
            {
                if(stmt_start_time_< tmp_data_mark.modify_time_)
                {
                    ret = OB_UD_PARALLAL_DATA_NOT_SAFE;
                    TBSYS_LOG(ERROR,"data is not safe!prev_data_mark=[%ld],start_time =%ld,"
                              "prev_row=[%s],cur_row=[%s],ret=%d",
                              tmp_data_mark.modify_time_,get_start_time (),
                              to_cstring(cur_row_),to_cstring(*tmp_row),ret);
                }
                else if(tmp_data_mark.modify_time_ == OB_INVALID_DATA )
                {
                    int64_t row_action_flag = 0;
                    if(OB_SUCCESS != (ret = tmp_row->get_action_flag (row_action_flag)))
                    {
                        TBSYS_LOG(WARN,"failed to get action flag cell ,ret = %d",ret);
                    }
                    else if ( row_action_flag ==ObActionFlag::OP_DEL_ROW)
                    {
                        ret = OB_UD_PARALLAL_DATA_NOT_SAFE;
                        TBSYS_LOG(ERROR,"data is not safe!tmp_row modify time = %ld,start_time =%ld,"
                                  "prev_row=[%s],cur_row=[%s],ret=%d",
                                  tmp_data_mark.modify_time_,get_start_time (),
                                  to_cstring(cur_row_),to_cstring(*tmp_row),ret);
                    }
                }
            }
            if(ret != OB_SUCCESS)
            {}
            else if (OB_SUCCESS != (ret = common::ObDataMarkHelper::convert_data_mark_row_to_normal_row(data_mark_param_,
                                                                                                        tmp_row,
                                                                                                        &final_row)))
            {
                TBSYS_LOG(WARN,"fail to convert data mark row to normal row!orig_row=[%s],final_row=[%s],ret=%d",
                          to_cstring(cur_row_),to_cstring(final_row),ret);
            }
            else
            {
                tmp_row = &final_row ;
            }
            TBSYS_LOG(DEBUG,"tmp row = %s,tmp_pointer = %p,final row =%s,final_row = %p,start time = %ld,tmp_row modif time = %ld",
                      to_cstring(*tmp_row),tmp_row,to_cstring(final_row),&final_row,stmt_start_time_,tmp_data_mark.modify_time_);
        }// end for datamakr
        if(OB_SUCCESS != ret )
        {}
        else if (OB_SUCCESS != (ret = common::ObRowFuse::fuse_row(*tmp_row, cur_row_, is_row_empty, is_ups_row_)))
        {
          TBSYS_LOG(WARN, "fail to fuse row:ret[%d]", ret);
        }
        else if (0 == i)
        {
          if (OB_SUCCESS != (ret = copy_rowkey(*tmp_row, cur_row_, false)))
          {
            TBSYS_LOG(WARN, "fail to copy rowkey:ret[%d]", ret);
          }
        }
      }

    }
    if (OB_SUCCESS == ret)
    {
      if (is_ups_row_ || !is_row_empty)
      {
        break;
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    row = &cur_row_;
  }
  return ret;
}
namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObMultipleGetMerge, PHY_MULTIPLE_GET_MERGE);
  }
}

int64_t ObMultipleGetMerge::to_string(char *buf, int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "MuitipleGetMerge(children_num=%d)\n",
                  child_num_);
  //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
  if (data_mark_param_.is_valid())
  {
      pos += data_mark_param_.to_string(buf + pos,buf_len - pos);
  }
  //add duyr 20160531:e
  for (int32_t i = 0; i < child_num_; ++i)
  {
    databuff_printf(buf, buf_len, pos, "Child%d:\n", i);
    if (NULL != child_array_[i])
    {
      pos += child_array_[i]->to_string(buf+pos, buf_len-pos);
    }
  }
  return pos;
}
//add wenghaixing [secondary index upd.2] 20141127
void ObMultipleGetMerge::reset_iterator()
{
  ObMemSSTableScan* sscan = NULL;
  if(get_child(0) != NULL&&get_child(0)->get_type() == PHY_MEM_SSTABLE_SCAN)
  {
    sscan = static_cast<ObMemSSTableScan*>(get_child(0));
    sscan->reset_iterator();
  }
  ObIncScan* inscan = NULL;
  if(NULL != get_child(1) && get_child(1)->get_type() == PHY_INC_SCAN)
  {
    inscan = static_cast<ObIncScan*>(get_child(1));
    inscan->reset_iterator();
  }
}
//add e
PHY_OPERATOR_ASSIGN(ObMultipleGetMerge)
{
  int ret = OB_SUCCESS;
  cur_row_.reset(false, ObRow::DEFAULT_NULL);
  ret = ObMultipleMerge::assign(other);
  return ret;
}

