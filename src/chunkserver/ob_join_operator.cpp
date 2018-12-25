/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_join_operator.cpp for implementation of join operator. 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_join_operator.h"
#include "common/utility.h"
#include "common/ob_action_flag.h"
#include "common/ob_tsi_factory.h"
#include "ob_read_param_modifier.h"
#include "ob_get_param_cell_iterator.h"
#include <poll.h>

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;

    void ObJoinOperator::initialize()
    {
      schema_mgr_ = NULL;
      ups_join_stream_ = NULL;
      result_array_ = NULL;
    }
    
    ObJoinOperator::ObJoinOperator()
    : join_row_width_vec_(DEFAULT_JOIN_ROW_WITD_VEC_SIZE),
      join_offset_vec_(DEFAULT_JOIN_OFFSET_VEC_SIZE)
    {
      initialize();
    }
    
    ObJoinOperator::~ObJoinOperator()  
    {
      join_param_array_.clear();
      join_offset_vec_.clear();
      join_row_width_vec_.clear();
    }
    
    void ObJoinOperator::clear()
    {
      join_param_array_.reset();
      join_offset_vec_.reset();
      join_row_width_vec_.reset();
    }

    int ObJoinOperator::start_join(
        ObCellArray& result_array, const ObReadParam& join_read_param, 
        ObCellStream& ups_join_stream, const ObSchemaManagerV2& schema_mgr)
    {
      int err = OB_SUCCESS;

      result_array_ = &result_array;
      cur_join_read_param_ = join_read_param;
      ups_join_stream_ = &ups_join_stream;
      schema_mgr_ = &schema_mgr;

      err = do_join(); 

      return err;
    }

    int ObJoinOperator::do_join()
    {
      int err = OB_SUCCESS;

      clear();
      if (result_array_->get_cell_size() > 0)
      {
        err = prepare_join_param_();
      }

      if (OB_SUCCESS == err && join_param_array_.get_cell_size() > 0)
      {
        err = join(); 
      }

      return err;
    }
        
    /**
     * when do join operation, we need change modifytime and 
     * createtime to precisetime. 
     */ 
    int ObJoinOperator::ob_change_join_value_type(ObObj &obj)
    {
      int err = OB_SUCCESS;
      int64_t value = -1;

      if (obj.get_type() == ObCreateTimeType)
      {
        err = obj.get_createtime(value);
        if (OB_SUCCESS == err)
        {
          obj.set_precise_datetime(value);
        }
      }
      else if (obj.get_type() == ObModifyTimeType)
      {
        err = obj.get_modifytime(value);
        if (OB_SUCCESS == err)
        {
          obj.set_precise_datetime(value);
        }
      }

      return err;
    }
  
    int ObJoinOperator::ob_get_join_value(ObObj &res_out, const ObObj &left_value_in, 
                                          const ObObj &right_value_in)
    {
      int err = OB_SUCCESS;

      if ((ObNullType == left_value_in.get_type()) 
          || ((ObCreateTimeType != right_value_in.get_type())
              && (ObModifyTimeType != right_value_in.get_type())
         ))
      {
        res_out = right_value_in;
        err = ob_change_join_value_type(res_out);
      }
      else if (ObCreateTimeType == right_value_in.get_type())
      {
        ObCreateTime right_time;
        ObPreciseDateTime left_time;
        ObPreciseDateTime res_time;

        err = right_value_in.get_createtime(right_time);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to get create time from right value [err:%d]", err);
        }

        if (OB_SUCCESS == err)
        {
          err = left_value_in.get_precise_datetime(left_time);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to get precise time from left value [err:%d]", err);
          }
        }

        if (OB_SUCCESS == err)
        {
          res_time = std::min<int64_t>(right_time, left_time);
          res_out.set_precise_datetime(res_time);
        }
      }
      else if (ObModifyTimeType == right_value_in.get_type())
      {
        ObModifyTime right_time;
        ObPreciseDateTime left_time;
        ObPreciseDateTime res_time;

        err = right_value_in.get_modifytime(right_time);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to get modify time from right value [err:%d]", err);
        }

        if (OB_SUCCESS == err)
        {
          err = left_value_in.get_precise_datetime(left_time);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to get precise time from left value [err:%d]", err);
          }
        }

        if (OB_SUCCESS == err)
        {
          res_time = std::max<int64_t>(right_time, left_time);
          res_out.set_precise_datetime(res_time);
        }
      }

      return err;
    }

    inline bool ObJoinOperator::is_del_op(const int64_t ext_val)
    {
      return ((ext_val == ObActionFlag::OP_DEL_ROW)
              || (ext_val == ObActionFlag::OP_ROW_DOES_NOT_EXIST)
              || (ext_val == ObActionFlag::OP_DEL_TABLE));
    }
    
    int ObJoinOperator::prepare_join_param_()
    {
      int err = OB_SUCCESS;
      const ObColumnSchemaV2::ObJoinInfo *join_info = NULL;
      ObInnerCellInfo *join_left_cell = NULL;
      ObRowCellVec *join_cache_row = NULL;
      uint64_t prev_cache_row_table_id = OB_INVALID_ID;
      ObRowkey prev_cache_row_key;
      bool prev_row_searched_cache = false;
      bool prev_row_hit_cache = false;
      bool is_row_changed = false;
      bool need_change_row_key = true;
      uint64_t prev_join_right_table_id = OB_INVALID_ID;
      result_array_->reset_iterator();
      int64_t cell_num = 0;
      ObInnerCellInfo   *last_join_cell = NULL;
      int64_t           last_join_row_width = 0;
      int64_t           join_row_width_processed_cell_num = 0;
      ObCellInfo        apply_join_right_cell;
      ObCellInfo        apply_cell_adjusted;

      while (OB_SUCCESS == err)
      {
        err = result_array_->next_cell();
        if (OB_SUCCESS == err)
        {
          err = result_array_->get_cell(&join_left_cell,&is_row_changed);
          if (OB_SUCCESS == err)
          {
            cell_num ++;
            if (is_del_op(join_left_cell->value_.get_ext()))
            {
              continue ;
            }
            if (is_row_changed)
            {
              need_change_row_key = true;
            }
          }
        }
        else if (OB_ITER_END == err)
        {
          err = OB_SUCCESS;
          break;
        }

        if (OB_SUCCESS == err)
        {
          if (OB_INVALID_ID == join_left_cell->column_id_)
          {
            /**
             * column id of composite column is OB_INVALID_ID, needn't do 
             * join operation, skip it 
             */
            join_info = NULL;
          }
          else
          {
            const ObColumnSchemaV2  *column_schema = schema_mgr_->get_column_schema(join_left_cell->table_id_,
              join_left_cell->column_id_);
            if (NULL != column_schema)
            {
              join_info = column_schema->get_join_info();
            }
            else
            {
              join_info = NULL;
            }
          }
        }

        if (OB_SUCCESS == err && NULL != join_info)
        {
          ObObj row_key_obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
          ObRowkey temp_rowkey;
          uint32_t offset_idx = 0;
          int64_t offset_in_rowkey_objs = 0;
          const ObObj *objs_ptr = NULL;

          apply_join_right_cell.table_id_ = join_info->join_table_;
          apply_join_right_cell.column_id_  = join_info->correlated_column_;

          if (OB_INVALID_ID == apply_join_right_cell.column_id_ 
            || OB_INVALID_ID == apply_join_right_cell.table_id_)
          {
            TBSYS_LOG(WARN,"unepxected error, fail to get ids from join info "
              "[left_table_id:%lu,left_join_columnid:%lu]", 
              join_left_cell->table_id_,  join_left_cell->column_id_);
            err = OB_ERR_UNEXPECTED;
          }
          else if (apply_join_right_cell.table_id_ != prev_join_right_table_id || need_change_row_key)
          {
            for (offset_idx = 0; offset_idx < join_info->left_column_count_; offset_idx++)
            {
              offset_in_rowkey_objs = join_info->left_column_offset_array_[offset_idx];
              objs_ptr = join_left_cell->row_key_.get_obj_ptr();
              if (NULL != objs_ptr && offset_in_rowkey_objs < join_left_cell->row_key_.get_obj_cnt())
              {
                row_key_obj_array[offset_idx] = objs_ptr[offset_in_rowkey_objs];
              }
              else
              {
                TBSYS_LOG(WARN, "schema error, offset_in_rowkey_objs=%ld, left rowkey obj cnt=%ld",
                    offset_in_rowkey_objs, join_left_cell->row_key_.get_obj_cnt());
                err = OB_ERROR;
                break;
              }
            }

            if (OB_SUCCESS == err)
            {
              temp_rowkey.assign(row_key_obj_array, join_info->left_column_count_);
              if((OB_SUCCESS != (err = rowkey_buffer_.write_string(temp_rowkey, &apply_join_right_cell.row_key_))))
              {
                TBSYS_LOG(WARN, "fail to write string. No sufficient memory may cause this err. [err=%d]", err);
              }
              need_change_row_key = false;
            }

          }

          if (OB_SUCCESS == err)
          {
            // search in cache first
            int get_cache_err = OB_SUCCESS; 
            if (prev_row_searched_cache
              && prev_cache_row_table_id == apply_join_right_cell.table_id_
              && prev_cache_row_key == apply_join_right_cell.row_key_
              )
            {
              if (prev_row_hit_cache)
              {
                get_cache_err = OB_SUCCESS;
                join_cache_row->reset_iterator();
              }
              else
              {
                get_cache_err = OB_ENTRY_NOT_EXIST;
              }
            }
            else
            {
              get_cache_err = ups_join_stream_->get_cache_row(apply_join_right_cell,join_cache_row);
              prev_row_searched_cache = true;
              prev_cache_row_table_id = apply_join_right_cell.table_id_;
              prev_cache_row_key = apply_join_right_cell.row_key_;
              prev_row_hit_cache = (OB_SUCCESS == get_cache_err);
              prev_join_right_table_id = apply_join_right_cell.table_id_;
            }

            if (OB_SUCCESS == get_cache_err)
            {
              ObCellInfo *cur_cell = NULL;
              while (OB_SUCCESS == err)
              {
                err = join_cache_row->next_cell();
                if (OB_ITER_END == err)
                {
                  err = OB_SUCCESS;
                  break;
                }

                if (OB_SUCCESS == err)
                {
                  err = join_cache_row->get_cell(&cur_cell);
                  if (OB_SUCCESS == err 
                    && (cur_cell->column_id_ == apply_join_right_cell.column_id_
                    || cur_cell->value_.get_ext() == ObActionFlag::OP_DEL_ROW
                    || cur_cell->value_.get_ext() == ObActionFlag::OP_DEL_TABLE
                    || cur_cell->value_.get_ext() == ObActionFlag::OP_NOP
                    )
                    )
                  {
                    // apply_cell_adjusted = *join_left_cell;
                    err = ob_get_join_value(apply_cell_adjusted.value_,join_left_cell->value_,cur_cell->value_);
                    if (OB_SUCCESS == err)
                    {
                      err = result_array_->apply(apply_cell_adjusted ,join_left_cell);
                    }
                    else
                    {
                      TBSYS_LOG(WARN,"fail to get join mutation [tableid:%lu,rowkey:%s,"
                        "column_id:%lu,type:%d,value:%s]",cur_cell->table_id_,
                        to_cstring(cur_cell->row_key_), 
                        cur_cell->column_id_, cur_cell->value_.get_type(),
                        to_cstring(cur_cell->value_));
                    }
                  }
                }
              }//end while
            }

            // get according to rpc
            if (OB_SUCCESS != get_cache_err && OB_SUCCESS == err)
            {
              /**
               * FIXME: every join get the whole row, better method : get the 
               * whole row just, when cache was setted in ObCellStream
               */
              apply_join_right_cell.column_id_ = 0;
              if (NULL != last_join_cell
                && apply_join_right_cell.table_id_ == last_join_cell->table_id_
                && apply_join_right_cell.row_key_ == last_join_cell->row_key_
                )
              {
                // the same row, we only get once
              }
              else
              {
                if (join_param_array_.get_cell_size() != 0)
                {
                  err = join_row_width_vec_.push_back(last_join_row_width);
                  join_row_width_processed_cell_num += last_join_row_width;
                }

                last_join_row_width = 0;
                if (OB_SUCCESS == err)
                {
                  //can't append cell with is_fast_path=true, because it don't reset 
                  //table name and column name, but this cell will be added into get
                  //param, get param check the table name and column name
                  err = join_param_array_.append(apply_join_right_cell, last_join_cell);
                }
              }

              if (OB_SUCCESS == err)
              {
                last_join_row_width ++;
                err = join_offset_vec_.push_back(cell_num - 1);
              }
            }
          }
        }
      }

      if (OB_SUCCESS == err && join_offset_vec_.size() > 0)
      {
        if (join_param_array_.get_cell_size() != join_row_width_vec_.size() + 1
          || join_offset_vec_.size() -  join_row_width_processed_cell_num != last_join_row_width)
        {
          TBSYS_LOG(ERROR, "unexpected implementation algorithm error");
          err = OB_ERR_UNEXPECTED;
        }

        if (OB_SUCCESS == err)
        {
          err = join_row_width_vec_.push_back(join_offset_vec_.size() 
            - join_row_width_processed_cell_num);
        }
      }

      if (OB_SUCCESS== err)
      {
        result_array_->reset_iterator();
      }

      return err;
    }
        
    template<typename IteratorT>
    int ObJoinOperator::join_apply(
        const ObCellInfo & cell, 
        IteratorT & dst_off_beg, 
        IteratorT & dst_off_end)
    {
      int err = OB_SUCCESS;
      ObInnerCellInfo *affected_cell = NULL;
      const ObColumnSchemaV2::ObJoinInfo *join_info = NULL;
      uint64_t right_cell_table_id = OB_INVALID_ID;
      uint64_t right_cell_column_id = OB_INVALID_ID;

      for (; dst_off_beg != dst_off_end && OB_SUCCESS == err; ++ dst_off_beg )
      {
        err = result_array_->get_cell(*dst_off_beg,affected_cell);
        if (OB_SUCCESS == err)
        {
          const ObColumnSchemaV2  *column_schema = schema_mgr_->get_column_schema(affected_cell->table_id_,
            affected_cell->column_id_);
          if (NULL != column_schema)
          {
            join_info = column_schema->get_join_info();
          }
          else
          {
            join_info = NULL;
          }
        }

        if (OB_SUCCESS == err && NULL != join_info)
        {
          right_cell_column_id = join_info->correlated_column_;
          right_cell_table_id = join_info->join_table_;
          if (cell.table_id_ != right_cell_table_id)
          {
            TBSYS_LOG(ERROR,"unexpected error, ups return unwanted result [expected_tableid:%lu,real_tableid:%lu]",
              right_cell_table_id, cell.table_id_);
            err = OB_ERR_UNEXPECTED;
          }
        }

        if (OB_SUCCESS == err && NULL != join_info)
        {
          if (right_cell_column_id == cell.column_id_)
          {
            join_apply_cell_adjusted_.column_id_ = affected_cell->column_id_;
            join_apply_cell_adjusted_.table_id_ = affected_cell->table_id_;
            join_apply_cell_adjusted_.row_key_ = affected_cell->row_key_;

            err = ob_get_join_value(join_apply_cell_adjusted_.value_,affected_cell->value_,cell.value_);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN,"fail to get join value [left_value.type:%d,right_value.type:%d,err:%d]", affected_cell->value_.get_type(),
                cell.value_.get_type(), err);
            }

            if (OB_SUCCESS == err && affected_cell->value_.get_ext() != ObActionFlag::OP_ROW_DOES_NOT_EXIST)/// left table row exists
            {
              err = result_array_->apply(join_apply_cell_adjusted_,affected_cell);
              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(WARN,"apply error [affect_cell->table_id:%lu,affect_cell->rowkey:%s,"
                  "affect_cell->column_id:%lu,affect_cell->type:%d,affect_cell->ext:%ld,"
                  "mutation->table_id:%lu,mutation->rowkey->%s,"
                  "mutation->column_id:%lu,mutation->type:%d,mutation->ext:%ld",
                  affected_cell->table_id_, to_cstring(affected_cell->row_key_), 
                  affected_cell->column_id_, affected_cell->value_.get_type(), affected_cell->value_.get_ext(), 
                  join_apply_cell_adjusted_.table_id_, to_cstring(join_apply_cell_adjusted_.row_key_),
                  join_apply_cell_adjusted_.column_id_,
                  join_apply_cell_adjusted_.value_.get_type(), join_apply_cell_adjusted_.value_.get_ext());
              }
            }
          }
          else
          {
            /**
             * when do join operation, we set each cell with NULL type if 
             * it's delete row operation. 
             */
            if (cell.value_.get_ext() == ObActionFlag::OP_DEL_ROW 
              || cell.value_.get_ext() == ObActionFlag::OP_DEL_TABLE
              )
            {
              affected_cell->value_.set_null();
            }
          }
        }
      }

      if (OB_SUCCESS == err && dst_off_beg != dst_off_end)
      {
        TBSYS_LOG(ERROR, "unexpected error");
        err = OB_ERR_UNEXPECTED;
      }

      return err;
    }
    
    int ObJoinOperator::join()
    {
      int err = OB_SUCCESS;
      ObCellInfo *cur_cell = NULL;
      bool is_row_changed = false;
      ObCellArrayHelper get_cells(join_param_array_);
      ObCellArray::iterator src_cell_it_beg = join_param_array_.begin();
      ObCellArray::iterator join_param_end = join_param_array_.end();
      ObVector<int64_t>::iterator dst_width_it = join_row_width_vec_.begin();
      int64_t processed_dst_cell_num = 0;
      int64_t src_cell_idx = 0;
      bool is_first_row = true;

      err = ups_join_stream_->get(cur_join_read_param_, get_cells);
      while (OB_SUCCESS == err 
        && join_param_array_.get_cell_size() > 0
        &&  src_cell_it_beg != join_param_end)
      {
        err = ups_join_stream_->next_cell();
        if (OB_ITER_END == err)
        {
          src_cell_it_beg ++;
          break;
        }

        if (OB_SUCCESS == err)
        {
          err = ups_join_stream_->get_cell(&cur_cell, &is_row_changed);
        }

        if (OB_SUCCESS == err && is_row_changed && !is_first_row)
        {
          processed_dst_cell_num += *dst_width_it;
          src_cell_it_beg ++;
          dst_width_it ++;
          src_cell_idx ++;

          if (!(join_param_array_.end() != src_cell_it_beg))
          {
            TBSYS_LOG(ERROR, "updateserver return more result than wanted [src_cell_idx:%ld,"
              "get_param_size:%ld], rowkey:%s", src_cell_idx,join_param_array_.get_cell_size(),
              to_cstring(cur_cell->row_key_));
            err = OB_ERR_UNEXPECTED;
            break;
          }

          if ((OB_SUCCESS == err) && 
            (src_cell_it_beg->row_key_ != cur_cell->row_key_||src_cell_it_beg->table_id_ != cur_cell->table_id_))
          {
            TBSYS_LOG(ERROR, "updateserver return result not wanted [src_cell_idx:%ld,"
              "get_param_size:%ld], src rowkey:%s, cur rowkey:%s", 
              src_cell_idx,join_param_array_.get_cell_size(),
              to_cstring(src_cell_it_beg->row_key_), to_cstring(cur_cell->row_key_));
            err = OB_ERR_UNEXPECTED;
            break;
          }
        }

        if (OB_SUCCESS == err)
        {
          ObVector<int64_t>::iterator dst_off_it_beg = join_offset_vec_.begin() + processed_dst_cell_num;
          ObVector<int64_t>::iterator dst_off_it_end = dst_off_it_beg + *dst_width_it;
          err = join_apply(*cur_cell,dst_off_it_beg, dst_off_it_end);
          is_first_row = false;
        }
      }

      if ((join_offset_vec_.size() > 0) 
        && ((OB_ITER_END != err) 
        || (src_cell_it_beg  != join_param_array_.end())))
      {
        if (OB_SUCCESS == err)
        {
          TBSYS_LOG(ERROR, "%s", "unxpected error, update server return result not coincident with request");
          err = OB_ERR_UNEXPECTED;
        }
      }
      else if (OB_ITER_END == err)
      {
        err = OB_SUCCESS;
      }

      return err;
    }
    
    int ObJoinOperator::next_cell()
    {
      return result_array_->next_cell();
    }

    int ObJoinOperator::get_cell(ObInnerCellInfo** cell)
    {
      return result_array_->get_cell(cell);
    }

    int ObJoinOperator::get_cell(ObInnerCellInfo** cell, bool* is_row_changed)
    {
      return result_array_->get_cell(cell, is_row_changed);
    }
  } // end namespace chunkserver
} // end namespace oceanbase
