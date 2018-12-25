/**
 * liumengzhan_delete_index
 * Version: $Id$
 *
 * ob_delete_index.h
 *
 * Authors:
 *   liumengzhan<liumengzhan@bankcomm.com>
 *
 */
#include "ob_delete_index.h"
#include "common/utility.h"
#include "sql/ob_physical_plan.h"
#include "common/ob_define.h"
namespace oceanbase{
  namespace sql
  {
    REGISTER_PHY_OPERATOR(ObDeleteIndex, PHY_DELETE_INDEX);
  }
}
namespace oceanbase
{
  using namespace common;
  namespace sql
  {
    ObDeleteIndex::ObDeleteIndex()
      :main_table_id_(OB_INVALID_ID), index_num_(0)
    {
    }

    ObDeleteIndex::~ObDeleteIndex()
    {
    }

    void ObDeleteIndex::reset()
    {
      main_table_id_=OB_INVALID_ID;
      index_num_=0;
      row_desc_.reset();
      //del liumz, [UT_bugfix] 20140420:b
      /* no need
      for (int64_t i=0;i<index_num_;i++)
      {
        index_row_desc_[i].reset();
      }*/
      //del:e
      ObSingleChildPhyOperator::reset();
    }

    void ObDeleteIndex::reuse()
    {
      main_table_id_=OB_INVALID_ID;
      index_num_=0;
      row_desc_.reset();
      //del liumz, [UT_bugfix] 20140420:b
      /* no need
      for (int64_t i=0;i<index_num_;i++)
      {
        index_row_desc_[i].reset();
      }*/
      //del:e
      ObSingleChildPhyOperator::reset();
    }

    void ObDeleteIndex::set_main_tid(uint64_t table_id)
    {
      main_table_id_ = table_id;
    }

    uint64_t ObDeleteIndex::get_main_tid() const
    {
      return main_table_id_;
    }

    int ObDeleteIndex::set_index_num(int64_t index_num)
    {
      int ret = OB_SUCCESS;
      if (index_num<=0||index_num>OB_MAX_INDEX_NUMS)
      {
        ret = OB_INVALID_INDEX;
      }
      else
      {
        index_num_ = index_num;
      }
      return ret;
    }

    /*int64_t ObDeleteIndex::get_index_num() const
    {
      return index_num_;
    }*/

    void ObDeleteIndex::set_row_desc(const ObRowDesc &row_desc)
    {
      row_desc_ = row_desc;
    }

    const ObRowDesc& ObDeleteIndex::get_row_desc() const
    {
      return row_desc_;
    }

    int ObDeleteIndex::add_index_tid(int64_t index, const uint64_t index_tid)
    {
      int ret=OB_SUCCESS;
      if(index>=index_num_||index<0)
      {
        ret=OB_INVALID_INDEX;
      }
      else
        index_table_id_[index]=index_tid;
      return ret;
    }

    int ObDeleteIndex::add_index_row_desc(int64_t index, const ObRowDesc &row_desc)
    {
      int ret=OB_SUCCESS;
      if(index>=index_num_||index<0)
      {
        ret=OB_INVALID_INDEX;
      }
      else
        index_row_desc_[index]=row_desc;
      return ret;
    }

    int ObDeleteIndex::set_input_values(uint64_t table_id, int64_t index_num)
    {
      int ret = OB_SUCCESS;
      set_main_tid(table_id);
      ret = set_index_num(index_num);
      return ret;
    }

    int ObDeleteIndex::handle_trigger(const ObSchemaManagerV2 *schema_mgr, updateserver::ObIUpsTableMgr* host,
                                      updateserver::RWSessionCtx &session_ctx, const ObDmlType dml_type
                                      , ObRowStore* store)
    {
      int ret = OB_SUCCESS;
      //        TBSYS_LOG(ERROR, "[liumengzhan] index num = %ld", index_num_);
      for (int64_t i=0; i<index_num_ && OB_LIKELY(OB_SUCCESS == ret); i++)
      {
        ObRowDesc &index_row_desc = index_row_desc_[i];
        if(OB_SUCCESS != (ret = handle_one_index_table(index_row_desc, schema_mgr, host, session_ctx, dml_type,store)))
        {
          TBSYS_LOG(WARN,"fail to handle_one_index_table for table[%ld]", index_table_id_[i]);
          break;
        }
      }
      //        const ObTableSchema *main_table_schema = NULL;
      //        const ObRowkeyInfo *main_rowkey_info = NULL;
      //        int64_t main_rowkey_size = 0;
      //        if (NULL == (main_table_schema = schema_mgr->get_table_schema(main_table_id_))&&OB_LIKELY(OB_SUCCESS==ret))
      //        {
      //            ret = OB_SCHEMA_ERROR;
      //            TBSYS_LOG(WARN,"fail to get main table schema, tid=[%ld], ret=%d", main_table_id_, ret);
      //        }
      //        else
      //        {
      //            main_rowkey_info = &(main_table_schema->get_rowkey_info());
      //            if (NULL == main_rowkey_info)
      //            {
      //                ret = OB_ERROR;
      //                TBSYS_LOG(WARN,"fail to get rowkey info, tid=[%ld], ret=%d", main_table_id_, ret);
      //            }
      //            else
      //            {
      //                main_rowkey_size = main_rowkey_info->get_size();
      //                if(OB_SUCCESS!=(ret=handle_one_index_table(main_table_id_,row_desc_,schema_mgr,main_rowkey_size,host,session_ctx,dml_type)))
      //                {
      //                   TBSYS_LOG(WARN,"fail to handle_one_index_table for table[%ld]", main_table_id_);
      //                }
      //            }
      //        }//end if
      return ret;
    }

    int ObDeleteIndex::handle_one_index_table(const ObRowDesc &row_desc, const ObSchemaManagerV2 *schema_mgr,
                                              updateserver::ObIUpsTableMgr* host, updateserver::RWSessionCtx &session_ctx,
                                              const ObDmlType dml_type,ObRowStore* store)
    {
      int ret = OB_SUCCESS;
      ObRowStore row_store;
      const ObRowStore::StoredRow *stored_row = NULL;
      ObRow next_row;
      const ObRowDesc *next_row_desc = NULL;
      ObRow row_in_store;    
      //add liumz, [UT_bugfix] 20150420:b
      if (NULL == schema_mgr || NULL == host || NULL == child_op_)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN,"invalid argument,  schema_mgr or host is NULL.");
      }
      else if (NULL == store)
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(WARN,"child_op_ is NULL, ret=%d", ret);
      }
      else if(OB_SUCCESS != (ret = child_op_->get_row_desc(next_row_desc)))
      {
        TBSYS_LOG(WARN, "get child op desc failed,ret [%d]", ret);
      }
      else if(NULL != next_row_desc)
      {
        next_row.set_row_desc(*next_row_desc);
      }
      row_in_store.set_row_desc(row_desc);
      //add:e
      while (OB_LIKELY(OB_SUCCESS == ret) && OB_SUCCESS == (ret = store->get_next_row(next_row)))
      {
        /*add liumz, [UT_bugfix] 20150420:b
        if (NULL == next_row)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN,"fail to get next row, ret=%d", ret);
          break;
        }
        add:e*/
        


        const ObObj *obj_tmp = NULL;
        int64_t index_col_count = row_desc.get_column_num();
        int64_t child_col_count = next_row.get_column_num();

        for (int64_t i=0; i<index_col_count && OB_LIKELY(OB_SUCCESS == ret);i++)
        {
          uint64_t index_tid = OB_INVALID_ID;
          uint64_t index_cid = OB_INVALID_ID;
          if (OB_SUCCESS != (ret = row_desc.get_tid_cid(i, index_tid, index_cid)))
          {
            TBSYS_LOG(WARN,"fail to get tid&cid, ret=%d", ret);
            break;
          }
          for(int64_t j=0; j<child_col_count && OB_LIKELY(OB_SUCCESS == ret); j++)
          {
            uint64_t child_tid = OB_INVALID_ID;
            uint64_t child_cid = OB_INVALID_ID;
            if (OB_SUCCESS != (ret = next_row_desc->get_tid_cid(j, child_tid, child_cid)))
            {
              TBSYS_LOG(WARN,"fail to get tid&cid, ret=%d", ret);
              break;
            }
            if (index_cid == child_cid)
            {
              next_row.raw_get_cell(j, obj_tmp);
              row_in_store.set_cell(index_tid, index_cid, *obj_tmp);
              break;
            }
          }//end for
        }//end for
        if(OB_SUCCESS != (ret = (row_store.add_row(row_in_store, stored_row))))
        {
          TBSYS_LOG(WARN, "fail to add row to row store, ret=%d, row=%s",ret,to_cstring(row_in_store));
        }
      }//end while
      if(OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
      }
      if(OB_SUCCESS == ret)
      {
        ObIndexCellIterAdaptor icia;
        icia.set_row_iter(&row_store, row_desc.get_rowkey_cell_count(), schema_mgr, row_desc);
        ret = host->apply(session_ctx, icia, dml_type);
      }
      //add liuxiao [secondary index bug fix] 20150908
      if(OB_SUCCESS == ret && NULL != store)
      {
        store->reset_iterator();
      }
      //add e;
      row_store.clear();
      //reset_iterator();
      return ret;
    }

    //add liumz, [fix delete where bug], 20150128:b
    void ObDeleteIndex::reset_iterator()
    {
      if(child_op_ && PHY_PROJECT == child_op_->get_type())
      {
        ObProject *project_op = NULL;
        project_op = dynamic_cast<ObProject*>(child_op_);
        project_op->reset_iterator();
      }
    }
    //add:e

    int ObDeleteIndex::open()
    {
      int ret = OB_SUCCESS;
      if (NULL == child_op_)
      {
        ret = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (ret = child_op_->open()))
      {
        if (!IS_SQL_ERR(ret))//add liumz, [ups -5049 log too much]20161217
        {
          TBSYS_LOG(WARN, "open child_op fail ret=%d %p", ret, child_op_);
        }
      }
      return ret;
    }

    //del liumz, [UT_bugfix] 20150420:b
    /*
    int ObDeleteIndex::close()
    {
      int ret = OB_SUCCESS;
      if (NULL == child_op_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = child_op_->close()))
        {
          TBSYS_LOG(WARN, "close child_op fail ret=%d", tmp_ret);
        }
      }
      return ret;
    }*/
    //del:e

    int ObDeleteIndex::close()
    {
      row_desc_.reset();
      return ObSingleChildPhyOperator::close();
    }

    //    int ObDeleteIndex::get_next_row(const common::ObRow *&row)
    //    {
    //      int ret = OB_SUCCESS;
    //      const ObRow *const_next_row = NULL;
    //      ObRow *next_row = NULL;
    //      if (NULL == child_op_)
    //      {
    //        ret = OB_NOT_INIT;
    //      }
    //      else if(OB_SUCCESS!=(ret=child_op_->get_next_row(const_next_row)))
    //      {
    //          if(ret!=OB_ITER_END)
    //               TBSYS_LOG(WARN, "child_op get_next_row fail ret=%d", ret);
    //      }
    //      else
    //      {
    //          next_row = const_cast<ObRow *>(const_next_row);
    //          next_row->set_row_desc(row_desc_);
    //          row = next_row;
    //      }
    //      return ret;
    //    }

    int ObDeleteIndex::get_next_row(const common::ObRow *&row)
    {
      int ret = OB_SUCCESS;
      const ObRow *next_row = NULL;
      const ObRowDesc *next_row_desc = NULL;
      if (NULL == child_op_)
      {
        ret = OB_NOT_INIT;
      }
      else if(OB_SUCCESS!=(ret=child_op_->get_next_row(next_row)))
      {
        if(ret!=OB_ITER_END && !IS_SQL_ERR(ret))//add liumz, [ups -5049 log too much]20161217
          TBSYS_LOG(WARN, "child_op get_next_row fail ret=%d", ret);
      }
      else
      {
        cur_row_.clear();
        cur_row_.set_row_desc(row_desc_);
        next_row_desc = next_row->get_row_desc();
        if (next_row_desc == NULL)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN,"fail to get row desc, ret=%d", ret);
        }
        const ObObj *obj_tmp = NULL;
        int64_t main_col_count = row_desc_.get_column_num();//main table's row_count
        int64_t child_col_count = next_row->get_column_num();//row_count of child_op

        for (int64_t i=0;i<main_col_count&&OB_LIKELY(OB_SUCCESS==ret);i++)
        {
          uint64_t main_tid = OB_INVALID_ID;
          uint64_t main_cid = OB_INVALID_ID;
          if (OB_SUCCESS != (ret = row_desc_.get_tid_cid(i, main_tid, main_cid)))
          {
            TBSYS_LOG(WARN, "fail to get tid&cid, ret=%d", ret);
            break;
          }
          for(int64_t j=0; j<child_col_count && OB_LIKELY(OB_SUCCESS == ret); j++)
          {
            uint64_t child_tid = OB_INVALID_ID;
            uint64_t child_cid = OB_INVALID_ID;
            if (OB_SUCCESS != (ret = next_row_desc->get_tid_cid(j, child_tid, child_cid)))
            {
              TBSYS_LOG(WARN, "fail to get tid&cid, ret=%d", ret);
              break;
            }
            if (main_cid == child_cid)
            {
              next_row->raw_get_cell(j, obj_tmp);
              cur_row_.set_cell(main_tid, main_cid, *obj_tmp);
              break;
            }
          }//end for
        }//end for
        if(OB_LIKELY(OB_SUCCESS == ret))
        {
          row = &cur_row_;
        }
      }
      return ret;
    }

    int ObDeleteIndex::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      int ret = OB_SUCCESS;
      /*if (NULL == child_op_ || NULL == child_op_->get_child(0))
      {
        ret = OB_NOT_INIT;
      }
      else if(OB_SUCCESS != (ret = child_op_->get_child(0)->get_row_desc(row_desc)))
      {
        TBSYS_LOG(WARN, "child_op get_row_desc fail ret=%d", ret);
      }*/
      row_desc = &row_desc_;
      return ret;
    }

    //replace liumz, [UT_bugfix] 20150420:b
    //int ObDeleteIndex::get_index_tid(int64_t index, uint64_t table_id) const
    int ObDeleteIndex::get_index_tid(int64_t index, uint64_t &table_id) const
    //replace:e
    {
      int ret = OB_SUCCESS;
      if(index >= index_num_ || index<0)
      {
        ret=OB_INVALID_INDEX;
      }
      else
        table_id = index_table_id_[index];
      return ret;
    }

    int ObDeleteIndex::get_index_row_desc(int64_t index, const ObRowDesc *&row_desc) const
    {
      int ret=OB_SUCCESS;
      if(index>=index_num_||index<0)
      {
        ret=OB_INVALID_INDEX;
      }
      else
        row_desc = &index_row_desc_[index];
      return ret;
    }

    int64_t ObDeleteIndex::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "ObDeleteIndex(main_table_id_=%lu, index_num_=%ld", main_table_id_,index_num_);
      //pos += insert_values_.to_string(buf+pos, buf_len-pos);
      databuff_printf(buf, buf_len, pos, ")\n");
      if (NULL != child_op_)
      {
        pos += child_op_->to_string(buf+pos, buf_len-pos);
      }
      return pos;
    }


    PHY_OPERATOR_ASSIGN(ObDeleteIndex)
    {
      int ret = OB_SUCCESS;
      CAST_TO_INHERITANCE(ObDeleteIndex);
      reset();
      main_table_id_=o_ptr->main_table_id_;
      index_num_=o_ptr->index_num_;
      set_row_desc(o_ptr->get_row_desc());
      uint64_t tid = OB_INVALID_ID;
      const ObRowDesc *index_row_desc = NULL;
      for (int64_t i=0;i<index_num_&&OB_LIKELY(OB_SUCCESS==ret);i++)
      {
        if (OB_SUCCESS != (ret=o_ptr->get_index_tid(i, tid)))
        {
          TBSYS_LOG(ERROR, "get index tid fail, ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret=add_index_tid(i, tid)))
        {
          TBSYS_LOG(ERROR, "add index tid fail, ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret=o_ptr->get_index_row_desc(i, index_row_desc)))
        {
          TBSYS_LOG(ERROR, "get index row desc fail, ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret=add_index_row_desc(i, *index_row_desc)))
        {
          TBSYS_LOG(ERROR, "add index row desc fail, ret=%d", ret);
        }
      }
      return ret;
    }

    //    int ObDeleteIndex::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    DEFINE_SERIALIZE(ObDeleteIndex)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret=serialization::encode_vi64(buf, buf_len, pos, static_cast<int64_t>(main_table_id_))))
      {
        TBSYS_LOG(ERROR, "serialize main_table_id fail ret=%d buf=%p buf_len=%ld pos=%ld", ret, buf, buf_len, pos);
      }
      else if (OB_SUCCESS != (ret=serialization::encode_vi64(buf, buf_len, pos, index_num_)))
      {
        TBSYS_LOG(ERROR, "serialize index_num fail ret=%d buf=%p buf_len=%ld pos=%ld", ret, buf, buf_len, pos);
      }
      else if (OB_SUCCESS != (ret = row_desc_.serialize(buf, buf_len, pos)))
      {
        TBSYS_LOG(ERROR, "serialize row_desc fail ret=%d buf=%p buf_len=%ld pos=%ld", ret, buf, buf_len, pos);
      }
      for (int64_t i=0;i<index_num_&&OB_LIKELY(OB_SUCCESS==ret);i++)
      {
        uint64_t index_tid = index_table_id_[i];
        const ObRowDesc &index_row_desc = index_row_desc_[i];
        if (OB_SUCCESS != (ret=serialization::encode_vi64(buf, buf_len, pos, static_cast<int64_t>(index_tid))))
        {
          TBSYS_LOG(ERROR, "serialize index_tid fail ret=%d buf=%p buf_len=%ld pos=%ld", ret, buf, buf_len, pos);
        }
        else if (OB_SUCCESS != (ret = index_row_desc.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(ERROR, "serialize index_row_desc fail ret=%d buf=%p buf_len=%ld pos=%ld", ret, buf, buf_len, pos);
        }
      }
      return ret;
    }

    //    int ObDeleteIndex::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
    DEFINE_DESERIALIZE(ObDeleteIndex)
    {
      int64_t tid = 0;
      ObRowDesc index_row_desc;
      int ret = OB_SUCCESS;
      row_desc_.reset();
      if (OB_SUCCESS != (ret=serialization::decode_vi64(buf, data_len, pos, &tid)))
      {
        TBSYS_LOG(ERROR, "deserialize main_table_id fail ret=%d buf=%p buf_len=%ld pos=%ld", ret, buf, data_len, pos);
      }
      else if (OB_SUCCESS != (ret=serialization::decode_vi64(buf, data_len, pos, &index_num_)))
      {
        TBSYS_LOG(ERROR, "deserialize index_num fail ret=%d buf=%p buf_len=%ld pos=%ld", ret, buf, data_len, pos);
      }
      else if (OB_SUCCESS != (ret = row_desc_.deserialize(buf, data_len, pos)))
      {
        TBSYS_LOG(ERROR, "deserialize row_desc fail ret=%d buf=%p buf_len=%ld pos=%ld", ret, buf, data_len, pos);
      }
      if (OB_LIKELY(OB_SUCCESS==ret))
      {
        set_main_tid(static_cast<uint64_t>(tid));
      }

      for (int64_t i=0;i<index_num_&&OB_LIKELY(OB_SUCCESS==ret);i++)
      {
        index_row_desc.reset();
        if (OB_SUCCESS != (ret=serialization::decode_vi64(buf, data_len, pos, &tid)))
        {
          TBSYS_LOG(ERROR, "deserialize index_tid fail ret=%d buf=%p buf_len=%ld pos=%ld", ret, buf, data_len, pos);
        }
        else if (OB_SUCCESS != (ret = index_row_desc.deserialize(buf, data_len, pos)))
        {
          TBSYS_LOG(ERROR, "deserialize index_row_desc fail ret=%d buf=%p buf_len=%ld pos=%ld", ret, buf, data_len, pos);
        }
        else if (OB_SUCCESS != (ret=add_index_tid(i, static_cast<uint64_t>(tid))))
        {
          TBSYS_LOG(ERROR, "add index tid fail, ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret=add_index_row_desc(i, index_row_desc)))
        {
          TBSYS_LOG(ERROR, "add index row desc fail, ret=%d", ret);
        }
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObDeleteIndex)
    {
      return row_desc_.get_serialize_size()*(1+index_num_) + serialization::encoded_length_vi64(static_cast<int64_t>(main_table_id_))*(2+index_num_);
    }
  } // end namespace sql
} // end namespace oceanbase
