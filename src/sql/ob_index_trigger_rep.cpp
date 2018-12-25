/*
 * =====================================================================================
 *
 *       Filename:  ob_index_trigger_rep.cpp
 *
 *    Description:  index tirgger used for replace 
 *
 *        Version:  1.0
 *        Created:  20150105
 *       Revision:  none
 *       Compiler:  gcc/g++
 *
 *         Author:  Yongfeng LI <minCrazy@gmail.com> 
 *   Organization:  BANKCOMM & DASE ECNU
 *
 * =====================================================================================
 */

#include "ob_index_trigger_rep.h"
#include "sql/ob_physical_plan.h"

namespace oceanbase
{
  using namespace common;
  namespace sql
  {
    REGISTER_PHY_OPERATOR(ObIndexTriggerRep, PHY_INDEX_TRIGGER_REP);

    ObIndexTriggerRep::ObIndexTriggerRep()
    {

    }

    ObIndexTriggerRep::~ObIndexTriggerRep()
    {

    }

    int ObIndexTriggerRep::open()
    {
      int ret = OB_SUCCESS;
      if (NULL == child_op_)
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "child_op_ is NULL");
      }
      else if (OB_SUCCESS != (ret = child_op_->open()))
      {
        if (!IS_SQL_ERR(ret))//add liumz, [ups -5049 log too much]20161217
        {
          TBSYS_LOG(WARN, "fail to open child_op, err=%d", ret);
        }
      }
      else if (OB_SUCCESS != (ret = replace_values_.open()))
      {
        TBSYS_LOG(WARN, "fail to open replace_values, err=%d", ret);
      }
      else
      {
        const ObRow* row = NULL;
        const ObRowStore::StoredRow *stored_row = NULL;
        //row.set_row_desc(main_row_desc_);
        while(OB_SUCCESS == (ret = child_op_->get_next_row(row)))
        {
          if(NULL != row)
          {
            ret = data_row_.add_row(*row, stored_row);
          }
        }
        if(OB_ITER_END ==  ret)
        {
          ret = OB_SUCCESS;
        }
        else
        {
          TBSYS_LOG(WARN,"failed to open index trigger!ret = %d", ret);
        }
      }
      if(OB_SUCCESS == ret)
      {
        const ObRowDesc* desc = NULL;
        if(OB_SUCCESS != (ret = child_op_->get_row_desc(desc)))
        {
          TBSYS_LOG(WARN, "open index trigger rep failed,ret = %d", ret);
        }
        else if(NULL != desc)
        {
          child_op_row_desc_ = *desc;
        }
      }
      /*else if (OB_SUCCESS != (ret = replace_values_index_.open()))
      {
        TBSYS_LOG(WARN, "fail to open replace_values, err=%d", ret);
      }*/

      return ret;
    }

    int ObIndexTriggerRep::close()
    {
      int ret = OB_SUCCESS;
      if (NULL == child_op_)
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "child_op_ is NULL");
      }
      else
      {
        int tmp_ret = OB_SUCCESS;
        /*if (OB_SUCCESS != (tmp_ret = replace_values_index_.close()))
        {
           TBSYS_LOG(WARN, "fail to close replace_values_index_, err=%d", tmp_ret);
        }
        else */
        if (OB_SUCCESS != (tmp_ret = replace_values_.close()))
        {
          TBSYS_LOG(WARN, "fail to close replace_values, err=%d", tmp_ret);
        }
        else if (OB_SUCCESS != (tmp_ret = child_op_->close()))
        {
          TBSYS_LOG(WARN, "fail to close child_op, err=%d", tmp_ret);
        }
        else
        {
          data_row_.clear();
          child_op_row_desc_.reset();
        }
      }

      return ret;
    }

    void ObIndexTriggerRep::reset()
    {
      index_num_ = 0;
      main_tid_ = OB_INVALID_ID;
      for (int64_t i = 0; i < OB_MAX_INDEX_NUMS; i++)
      {
        index_row_desc_del_[i].reset();
        index_row_desc_ins_[i].reset();
      }

      replace_values_subquery_ = OB_INVALID_ID;
      replace_values_.reset();
      replace_index_values_subquery_ = OB_INVALID_ID;
      //replace_values_index_.reset();
      main_row_desc_.reset();
      child_op_row_desc_.reset();
      data_row_.reuse();
      ObSingleChildPhyOperator::reset();
    }

    void ObIndexTriggerRep::reuse()
    {
      index_num_ = 0;
      main_tid_ = OB_INVALID_ID;
      for (int64_t i = 0; i < OB_MAX_INDEX_NUMS; i++)
      {
        index_row_desc_del_[i].reset();
        index_row_desc_ins_[i].reset();
      }
      main_row_desc_.reset();
      replace_values_subquery_ = OB_INVALID_ID;
      replace_values_.reuse();
      replace_index_values_subquery_ = OB_INVALID_ID;
     // replace_values_index_.reset();
      data_row_.reuse();
      child_op_row_desc_.reset();
      ObSingleChildPhyOperator::reuse();
    }

    // liyongfeng [secondary index replace] process index trigger for each index
    int ObIndexTriggerRep::handle_trigger(const ObSchemaManagerV2 *schema_mgr, updateserver::ObIUpsTableMgr* host,
                                          updateserver::RWSessionCtx &session_ctx, ObRowStore* store)
    {
      int ret = OB_SUCCESS;

      for (int64_t idx = 0; idx < index_num_ && OB_SUCCESS == ret; idx++)
      {
        if (OB_SUCCESS != (ret = handle_trigger_one_index(idx, schema_mgr, host, session_ctx, store)))
        {
          TBSYS_LOG(ERROR, "fail to handle trigger for one index, index id=%ld, err=%d", idx, ret);
        }
      }

      return ret;
    }

    // liyongfeng [secondary index replace] process trigger for one index 
    int ObIndexTriggerRep::handle_trigger_one_index(const int64_t &index_idx, const ObSchemaManagerV2 *schema_mgr,
                                                    updateserver::ObIUpsTableMgr* host, updateserver::RWSessionCtx &session_ctx
                                                    ,ObRowStore* store)
    {
      int ret = OB_SUCCESS;
      UNUSED(store);
      bool is_row_empty = false;
      const ObObj *obj = NULL;
      uint64_t index_tid = OB_INVALID_ID;
      uint64_t cid = OB_INVALID_ID;

      ObRow cur_row;
      const ObRow *rep_row = NULL;

      ObRowStore insert_row_store;
      ObRowStore delete_row_store;

      const ObRowStore::StoredRow *stored_row = NULL;
      ObRow row_in_store,insert_row_in_store;

      ObObj obj_dml,obj_null;
      if (NULL == schema_mgr)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      /*else if (NULL == child_op_)
      {
        ret = OB_NOT_INIT;
      }*/
      else if (OB_SUCCESS != (ret = index_row_desc_ins_[index_idx].get_tid_cid(0, index_tid, cid)))// get index tid
      {
        TBSYS_LOG(ERROR, "fail to get index tid, err=%d", ret);
      }
      else
      {
        //bool is_first = is_first_replace();// if table has some records or not, if no records, is_first=true;
        bool child_op_break = false;
        bool expr_break = false;
        const ObRowkey* child_op_key = NULL;
        const ObRowkey* expr_key = NULL;
        cur_row.set_row_desc(child_op_row_desc_);
        replace_values_.reset_iterator();//reset replace values iterator first
        data_row_.reset_iterator();
        do
        {
          ret = data_row_.get_next_row(cur_row);
          expr_break = false;
          if(OB_ITER_END == ret)
          {
            ret = OB_SUCCESS;
            child_op_break = true;
          }
          if(OB_LIKELY(OB_SUCCESS == ret) && !child_op_break)
          {
            //construct a delete row from static data row
            if (OB_UNLIKELY(OB_SUCCESS != (ret = cur_row.get_is_row_empty(is_row_empty))))// if cur_row is empty or not
            {
              TBSYS_LOG(WARN, "fail to get is row empty, err=%d", ret);
            }
            else if(!is_row_empty)
            {
              row_in_store.set_row_desc(index_row_desc_del_[index_idx]);
              for (int64_t col_idx = 0; col_idx < index_row_desc_del_[index_idx].get_column_num() && OB_SUCCESS == ret; col_idx++)
              {
                if (OB_SUCCESS != (ret = index_row_desc_del_[index_idx].get_tid_cid(col_idx, index_tid, cid)))
                {
                  TBSYS_LOG(ERROR, "fail to get tid_cid from index delete row desc, err=%d", ret);
                }
                else if (OB_ACTION_FLAG_COLUMN_ID == cid)// add action flag
                {
                  obj_dml.set_int(ObActionFlag::OP_DEL_ROW);
                  /*if (OB_SUCCESS != (ret = row_in_store.set_cell(index_tid, cid, obj_dml)))
                  {
                    TBSYS_LOG(ERROR, "fail to set cell for delete row, err=%d", ret);
                  }*/
                  obj = &obj_dml;
                }
                else if (OB_SUCCESS != (ret = cur_row.get_cell(main_tid_, cid, obj)))
                {
                  TBSYS_LOG(ERROR, "fail to get cell from cur row, err=%d, main tid=%lu, cid=%lu", ret, main_tid_, cid);
                }
                if(OB_SUCCESS == ret)
                {
                  if (NULL == obj)
                  {
                    TBSYS_LOG(ERROR, "ObObj pointer should not be NULL");
                    ret = OB_INVALID_ARGUMENT;
                  }
                  else if (OB_SUCCESS != (ret = row_in_store.set_cell(index_tid, cid, *obj)))
                  {
                    TBSYS_LOG(ERROR, "fail to set cell for delete row, err=%d", ret);
                  }
                }
               }//end for
              if (OB_SUCCESS == ret)
              {
                if (OB_SUCCESS != (ret = delete_row_store.add_row(row_in_store, stored_row)))
                {
                  TBSYS_LOG(ERROR, "fail to add row into delete row store, err=%d, row=%s", ret, to_cstring(row_in_store));
                }
                else
                {
                  //TBSYS_LOG(ERROR,"test::whx delete_row_store = %s, cur_row = %s",to_cstring(row_in_store),to_cstring(cur_row));
                  row_in_store.clear();
                }
              }
            }
          }
          do
          {
            bool rowkey_equal = false;
            if(OB_SUCCESS != (ret = replace_values_.get_next_row(rep_row)))
            {
              if(OB_ITER_END == ret)
              {
                ret = OB_SUCCESS;
                expr_break = true;
              }
              else
              {
                TBSYS_LOG(WARN, "get next expr values row failed! ret = %d", ret);
                break;
              }
            }
            if(OB_SUCCESS == ret && !expr_break)
            {
              /*if(NULL == cur_row)
              {
                rowkey_equal = false;
              }
              else */
              if(OB_SUCCESS != (ret = cur_row.get_rowkey(child_op_key)) || OB_SUCCESS != (ret = rep_row->get_rowkey(expr_key)))
              {
                TBSYS_LOG(WARN, "get row key failed,ret = %d", ret);
              }
              else if(0 == child_op_key->compare(*expr_key))
              {
                expr_break = true;
                rowkey_equal = true;
              }
              insert_row_in_store.set_row_desc(index_row_desc_ins_[index_idx]);
              for (int64_t col_idx = 0; col_idx < index_row_desc_ins_[index_idx].get_column_num() && OB_SUCCESS == ret; col_idx++)
              {
                if (OB_SUCCESS != (ret = index_row_desc_ins_[index_idx].get_tid_cid(col_idx, index_tid, cid)))
                {
                  TBSYS_LOG(ERROR, "fail to get tid_cid from index insert row desc, err=%d", ret);
                }
                else if(OB_INDEX_VIRTUAL_COLUMN_ID == cid)
                {
                  obj_null.set_null();
                  obj = &obj_null;
                }
                else
                {
                  if(OB_INVALID_INDEX == main_row_desc_.get_idx(main_tid_,cid))
                  {
                    if(!rowkey_equal)
                    {
                      obj_null.set_null();
                      obj = &obj_null;
                    }
                    else
                    {
                      if (OB_SUCCESS != (ret = cur_row.get_cell(main_tid_, cid, obj)))
                      {
                        TBSYS_LOG(ERROR, "fail to get cell from cur row, err=%d, main tid=%lu, cid=%lu", ret, main_tid_, cid);
                        break;
                      }
                    }
                  }
                  else
                  {
                    if (OB_SUCCESS != (ret = rep_row->get_cell(main_tid_, cid, obj)))
                    {
                      TBSYS_LOG(ERROR, "fail to get cell from cur row, err=%d, main tid=%lu, cid=%lu", ret, main_tid_, cid);
                      break;
                    }
                  }
                }
                if(OB_SUCCESS == ret)
                {
                  if (NULL == obj)
                  {
                    TBSYS_LOG(ERROR, "ObObj pointer should not be NULL");
                    ret = OB_INVALID_ARGUMENT;
                  }
                  else if (OB_SUCCESS != (ret = insert_row_in_store.set_cell(index_tid, cid, *obj)))
                  {
                    TBSYS_LOG(ERROR, "fail to set cell for insert row, err=%d", ret);
                  }
                }
              }
              if (OB_SUCCESS == ret)
              {
                if (OB_SUCCESS != (ret = insert_row_store.add_row(insert_row_in_store, stored_row)))
                {
                  TBSYS_LOG(ERROR, "fail to add into insert row store, err=%d, row=%s", ret, to_cstring(row_in_store));
                }
                else
                {
                  //TBSYS_LOG(ERROR,"test::whx insert_row_store = %s, cur_row = %s",to_cstring(insert_row_in_store),to_cstring(*rep_row));
                  insert_row_in_store.clear();
                }
              }
            }

            if(expr_break)
            {
              break;
            }

          }while(OB_SUCCESS == ret);

          if(child_op_break)
          {
            break;
          }

        }while(OB_SUCCESS == ret);

       /* if (OB_SUCCESS == ret && PHY_MULTIPLE_GET_MERGE == child_op_->get_type())
        {
          ObMultipleGetMerge* fuse_op = static_cast<ObMultipleGetMerge*>(child_op_);
          fuse_op->reset_iterator();//reset static data iterator
          replace_values_.reset_iterator();//reset replace values iterator
        }*/

        if (OB_SUCCESS == ret)
        {
          ObIndexCellIterAdaptor icia;
          icia.set_row_iter(&delete_row_store, index_row_desc_del_[index_idx].get_rowkey_cell_count(), schema_mgr, index_row_desc_del_[index_idx]);
          ret = host->apply(session_ctx, icia, OB_DML_DELETE);
        }
        delete_row_store.clear();
        if (OB_SUCCESS == ret)
        {
          ObIndexCellIterAdaptor icia;
          icia.set_row_iter(&insert_row_store, index_row_desc_ins_[index_idx].get_rowkey_cell_count(), schema_mgr, index_row_desc_ins_[index_idx]);
          ret = host->apply(session_ctx, icia, OB_DML_INSERT);
        }
      }

      return ret;
    }

    // before replace, if table has some records, (static data + inc data)
    bool ObIndexTriggerRep::is_first_replace()
    {
      bool is_first = false;
      int ret = OB_SUCCESS;
      const ObRow *row = NULL;
      ret = child_op_->get_next_row(row);
      if (OB_ITER_END == ret)
      {
        is_first = true;
      }
      ObMultipleGetMerge* fuse_op = static_cast<ObMultipleGetMerge*>(child_op_);
      fuse_op->reset_iterator();

      return is_first;
    }
    int ObIndexTriggerRep::add_index_del_row_desc(int64_t idx, common::ObRowDesc desc)
    {
      int ret = OB_SUCCESS;
      if (idx >= OB_MAX_INDEX_NUMS || idx < 0)
      {
        TBSYS_LOG(ERROR, "fail to add row desc_del, idx is invalid, idx=%ld", idx);
        ret = OB_ERROR;
      }
      else
      {
        index_row_desc_del_[idx] = desc;
      }

      return ret;
    }

    int ObIndexTriggerRep::add_index_ins_row_desc(int64_t idx, common::ObRowDesc desc)
    {
      int ret = OB_SUCCESS;
      if (idx >= OB_MAX_INDEX_NUMS || idx < 0)
      {
        TBSYS_LOG(ERROR, "fail to add row desc_ins, idx is invalid, idx=%ld", idx);
        ret = OB_ERROR;
      }
      else
      {
        index_row_desc_ins_[idx] = desc;
      }

      return ret;
    }

    void ObIndexTriggerRep::ObIndexTriggerRep::set_index_num(const int64_t &num)
    {
      index_num_ = num;
    }

    void ObIndexTriggerRep::set_main_tid(const uint64_t &tid)
    {
      main_tid_ = tid;
    }

    int ObIndexTriggerRep::get_next_row(const ObRow *&row)
    {
      return replace_values_.get_next_row(row);
    }

    void ObIndexTriggerRep::reset_iterator()
    {
      replace_values_.reset_iterator();
    }

    int ObIndexTriggerRep::get_row_desc(const ObRowDesc *&row_desc) const
    {
      return replace_values_.get_row_desc(row_desc);
    }

    int64_t ObIndexTriggerRep::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "ObIndexTriggerRep: index_num=[%ld],main_tid=[%lu],", index_num_, main_tid_);
      databuff_printf(buf, buf_len, pos, "index delete row desc[");
      int64_t i = 0;
      int64_t cur_pos = 0;
      for (i = 0; i < index_num_ - 1; i++)
      {
        cur_pos = index_row_desc_del_[i].to_string(buf + pos, buf_len - pos);
        pos += cur_pos;
        databuff_printf(buf, buf_len, pos, ",");
      }
      cur_pos = index_row_desc_del_[i].to_string(buf + pos, buf_len - pos);
      pos += cur_pos;

      databuff_printf(buf, buf_len, pos, "],index insert row desc[");
      for (i = 0; i < index_num_ - 1; i++)
      {
        cur_pos = index_row_desc_ins_[i].to_string(buf + pos, buf_len - pos);
        pos += cur_pos;
        databuff_printf(buf, buf_len, pos, ",");
      }
      cur_pos = index_row_desc_ins_[i].to_string(buf + pos, buf_len - pos);
      pos += cur_pos;

      databuff_printf(buf, buf_len, pos, "],replace_values_subquery=[%lu],values=", replace_values_subquery_);
      pos += replace_values_.to_string(buf + pos, buf_len - pos);

      if (NULL != child_op_)
      {
        cur_pos = child_op_->to_string(buf + pos, buf_len - pos);
        pos += cur_pos;
      }

      return pos;
    }

    PHY_OPERATOR_ASSIGN(ObIndexTriggerRep)
    {
      int ret = OB_SUCCESS;
      CAST_TO_INHERITANCE(ObIndexTriggerRep);
      reset();
      index_num_ = o_ptr->index_num_;
      main_tid_ = o_ptr->main_tid_;
      data_max_cid_ = o_ptr->data_max_cid_;
      for (int64_t idx = 0; idx < o_ptr->index_num_; idx++)
      {
        index_row_desc_del_[idx] = o_ptr->index_row_desc_del_[idx];
        index_row_desc_ins_[idx] = o_ptr->index_row_desc_ins_[idx];
      }

     replace_values_subquery_ = o_ptr->replace_values_subquery_;
     replace_index_values_subquery_ = o_ptr->replace_index_values_subquery_;
     main_row_desc_ = o_ptr->main_row_desc_;
     data_row_ = o_ptr->data_row_;
     child_op_row_desc_ = o_ptr->child_op_row_desc_;
     return ret;
    }

    DEFINE_SERIALIZE(ObIndexTriggerRep)
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, index_num_)))
      {
        TBSYS_LOG(WARN, "fail to encode index_num, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, static_cast<int64_t>(main_tid_))))
      {
        TBSYS_LOG(WARN, "fail to encode main_tid, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, static_cast<int64_t>(data_max_cid_))))
      {
        TBSYS_LOG(WARN, "fail to encode data_max_cid_, err=%d", ret);
      }
      else
      {
        for (int64_t idx = 0; idx < index_num_; idx++)
        {
          if (OB_SUCCESS != (ret = index_row_desc_del_[idx].serialize(buf, buf_len, pos)))
          {
            TBSYS_LOG(WARN, "fail to serialize index delete row desc, err=%d", ret);
            break;
          }
          else if (OB_SUCCESS != (ret = index_row_desc_ins_[idx].serialize(buf, buf_len, pos)))
          {
            TBSYS_LOG(WARN, "fail to serialize index insert row desc, err=%d", ret);
            break;
          }
        }
      }

      if(OB_SUCCESS == ret)
      {
        ret = main_row_desc_.serialize(buf, buf_len, pos);
      }

      ObExprValues *replace_values = NULL;
      if (OB_SUCCESS == ret)
      {
        if (NULL == (replace_values = dynamic_cast<ObExprValues*>(my_phy_plan_->get_phy_query_by_id(replace_values_subquery_))))
        {
          TBSYS_LOG(ERROR, "invalid expr_values, subquery_id=%lu", replace_values_subquery_);
        }
        else if (OB_SUCCESS != (ret = replace_values->serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize expr_values, err=%d", ret);
        }
      }

      ObExprValues *replace_index_values = NULL;
      if (OB_SUCCESS == ret)
      {
        if (NULL == (replace_index_values = dynamic_cast<ObExprValues*>(my_phy_plan_->get_phy_query_by_id(replace_index_values_subquery_))))
        {
          TBSYS_LOG(ERROR, "invalid expr_values, subquery_id=%lu", replace_index_values_subquery_);
        }
       /* else if (OB_SUCCESS != (ret = replace_index_values->serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize expr_values, err=%d", ret);
        }*/
      }
      
      return ret;
    }

    DEFINE_DESERIALIZE(ObIndexTriggerRep)
    {
      int ret = OB_SUCCESS;
      int64_t tid;
      int64_t cid;
      if (OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &index_num_)))//deserialize for index_num_
      {
        TBSYS_LOG(WARN, "fail to decode index_num, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &tid)))
      {
        TBSYS_LOG(WARN, "fail to decode main_tid, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &cid)))
      {
        TBSYS_LOG(WARN, "fail to decode main_tid, err=%d", ret);
      }
      else
      {
        main_tid_ = static_cast<uint64_t>(tid);
        data_max_cid_ = static_cast<uint64_t>(cid);
        for (int64_t idx = 0; idx < index_num_; idx++)
        {
          if (OB_SUCCESS != (ret = index_row_desc_del_[idx].deserialize(buf, data_len, pos)))
          {
            TBSYS_LOG(WARN, "fail to deserialize expression, err=%d", ret);
            break;
          }
          else if (OB_SUCCESS != (ret = index_row_desc_ins_[idx].deserialize(buf, data_len, pos)))
          {
            TBSYS_LOG(WARN, "fail to deserialize expression, err=%d", ret);
            break;
          }
        }
      }

      if(OB_SUCCESS == ret)
      {
        ret = main_row_desc_.deserialize(buf, data_len, pos);
      }

      //deserialize for replace values
      if (OB_SUCCESS == ret && OB_SUCCESS != (ret = replace_values_.deserialize(buf, data_len, pos)))
      {
        TBSYS_LOG(WARN, "fail to deserialize replace values, err=%d", ret);
      }

      /*if (OB_SUCCESS == ret && OB_SUCCESS != (ret = replace_values_index_.deserialize(buf, data_len, pos)))
      {
        TBSYS_LOG(WARN, "fail to deserialize replace values, err=%d", ret);
      }*/

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObIndexTriggerRep)
    {
      int64_t size = 0;
      size += serialization::encoded_length_i64(index_num_);
      size += serialization::encoded_length_i64(static_cast<int64_t>(main_tid_));
      size += serialization::encoded_length_i64(static_cast<int64_t>(data_max_cid_));
      for (int64_t idx = 0; idx < index_num_; idx++)
      {
        size += index_row_desc_del_[idx].get_serialize_size();
        size += index_row_desc_ins_[idx].get_serialize_size();
      }

      size += main_row_desc_.get_serialize_size();
      size += replace_values_.get_serialize_size();

      //size += replace_values_index_.get_serialize_size();
      return size;
    }
  }// namespace sql
} // namespace oceanbase
