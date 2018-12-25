/**
 *
 * Version: $Id$
 * fanqiushi_index
 * ob_index_trigger.cpp
 *
 * Authors:
 *   Li Kai <yubai.lk@alipay.com>
 *
 */
#include "ob_index_trigger.h"
#include "common/utility.h"
#include "sql/ob_physical_plan.h"
#include "common/ob_define.h"
namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObIndexTrigger, PHY_INDEX_TRIGGER);
  }
}
namespace oceanbase
{
  using namespace common;
  namespace sql
  {
    ObIndexTrigger::ObIndexTrigger()
    {
    }

    ObIndexTrigger::~ObIndexTrigger()
    {
    }

    void ObIndexTrigger::reset()
    {
        cur_row_desc_.reset();
        main_table_id_=OB_INVALID_ID;
        index_num_=0;
       // RowDescArray_.clear();
        cur_index_=0;
        ObSingleChildPhyOperator::reset();
    }

    void ObIndexTrigger::reuse()
    {

    }
    int ObIndexTrigger::set_i64_values(int64_t m_id,int64_t index_num,int64_t cur_index)
    {
        int ret=OB_SUCCESS;
        main_table_id_=m_id;
        index_num_=index_num;
        cur_index_=cur_index;
        return ret;
    }
    int ObIndexTrigger::handle_trigger(const ObRowDesc*row_dec,const ObSchemaManagerV2 *schema_mgr,
                                       const int64_t rk_size,updateserver::ObIUpsTableMgr* host,
                                       updateserver::RWSessionCtx &session_ctx,const ObDmlType dml_type
                                       , ObRowStore* store)
    {
        int ret=OB_SUCCESS;
        //UNUSED(row_dec);
        //UNUSED(schema_mgr);
        UNUSED(rk_size);
       // UNUSED(host);
        uint64_t index_tid_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
        uint64_t array_count=0;
        uint64_t tmp_index_tid=OB_INVALID_ID;
        const ObTableSchema *index_table_schema = NULL;
        int64_t index_rowkey_size=OB_INVALID_ID;
        //const ObTableSchema *table_schema = NULL;
        if (OB_SUCCESS!=(ret=schema_mgr->get_all_modifiable_index_tid(main_table_id_,index_tid_array,array_count)))
        {
          TBSYS_LOG(WARN,"fail to get index table array for table[%ld]", main_table_id_);
        }
        else if(array_count!=0)
        {
            for(uint64_t i=0;i<array_count;i++)   //对该表的每一张可用的索引表进行处理
            {
                tmp_index_tid=index_tid_array[i];
                if (NULL == (index_table_schema = schema_mgr->get_table_schema(tmp_index_tid)))
                {
                 // ret = OB_ERR_ILLEGAL_ID;
                  TBSYS_LOG(WARN,"fail to get table schema for table[%ld]", tmp_index_tid);
                  continue;
                }
                else
                {
                    const ObRowkeyInfo *rowkey_info = &index_table_schema->get_rowkey_info();
                     index_rowkey_size = rowkey_info->get_size();
                }
               // TBSYS_LOG(ERROR,"test::fanqs,,tmp_index_tid=%ld", tmp_index_tid);
                if(OB_SUCCESS!=(ret=handle_one_index_table(tmp_index_tid,row_dec,schema_mgr,index_rowkey_size,host,session_ctx,dml_type,store))) //将一张索引表的增量数据存到内存表里面
                {
                   TBSYS_LOG(WARN,"fail to handle_one_index_table for table[%ld]", tmp_index_tid);
                   break;
                }
            }
        }
        return ret;
    }
    int ObIndexTrigger::handle_one_index_table(uint64_t index_tid,const ObRowDesc*row_dec,
                                               const ObSchemaManagerV2 *schema_mgr,const int64_t rk_size,
                                               updateserver::ObIUpsTableMgr* host,updateserver::RWSessionCtx &session_ctx,
                                               const ObDmlType dml_type, ObRowStore* store)
    {
        int ret=OB_SUCCESS;
        //UNUSED(row_dec);
        //UNUSED(index_tid);
        //UNUSED(schema_mgr);
       // UNUSED(rk_size);
        //UNUSED(host);
        ObRowDesc index_row_desc;
        const ObRowDesc *data_desc = NULL;
        if(OB_SUCCESS!=(ret=cons_index_row_desc(index_tid,schema_mgr,index_row_desc)))   //生成这张索引表的行描述信息
        {
            TBSYS_LOG(ERROR,"fail to cons_index_row_desc for table[%ld]", index_tid);
        }
        else if(OB_SUCCESS != (ret = get_row_desc(data_desc)))
        {
          TBSYS_LOG(ERROR, "failed to get data desc,ret = %d", ret);
        }
        else if(NULL != store)
        {
           // TBSYS_LOG(ERROR,"test::fanqs,index row_desc=%s", to_cstring(index_row_desc));
            ObRowStore row_store;
            const ObRowStore::StoredRow *stored_row = NULL;
            ObRow row_tmp;
            ObRow row_in_store;
            row_in_store.set_row_desc(index_row_desc);
            row_tmp.set_row_desc(*data_desc);
            //row_in_store=*row_tmp;
            while(OB_SUCCESS==(ret = store->get_next_row(row_tmp)))  //取得用户sql语句中输入的要插入原表的所有行
            {
                uint64_t tid=OB_INVALID_ID;
                uint64_t cid=OB_INVALID_ID;
                const ObObj *obj_tmp=NULL;
                for(int64_t i=0;i<row_tmp.get_column_num();i++)   //根据原表的一行构造索引表的一行
                {
                    row_dec->get_tid_cid(i,tid,cid);
                    row_tmp.raw_get_cell(i,obj_tmp);
                    if(is_cid_in_index_table(index_tid,cid,index_row_desc))
                        row_in_store.set_cell(index_tid,cid,*obj_tmp);
                }
                if(OB_SUCCESS!=(ret=(row_store.add_row(row_in_store,stored_row)))) //将索引表的一行存到row_store里面
                {
                    TBSYS_LOG(ERROR, "failed to do row_store.add_row faile,ret=%d,row=%s",ret,to_cstring(row_in_store));
                }
            }
            if(ret==OB_ITER_END)
                ret=OB_SUCCESS;
            /*ObInsertDBSemFilter *tmp_ds_sem=NULL;
            tmp_ds_sem=static_cast<ObInsertDBSemFilter*>(child_op_);
            tmp_ds_sem->reset_iterator();*/

            if(ret==OB_SUCCESS)
            {
                ObIndexCellIterAdaptor icia;
                icia.set_row_iter(&row_store,rk_size,schema_mgr,index_row_desc);
                ret=host->apply(session_ctx,icia,dml_type);
               // TBSYS_LOG(ERROR, "test::fanqs  host->apply ret=%d",ret);
            }
            //add liuxiao [secondary index bug fix] 20150908
            if(OB_SUCCESS == ret && NULL != store)
            {
              store->reset_iterator();
            }
            //add e;
            row_store.clear();
        }
        else
        {
          TBSYS_LOG(ERROR, "iterator pointer cannot be null");
        }
        return ret;
    }
    bool ObIndexTrigger::is_cid_in_index_table(uint64_t tid,uint64_t cid,ObRowDesc row_dec)
    {
        bool ret=false;
        uint64_t t_id=OB_INVALID_ID;
        uint64_t c_id=OB_INVALID_ID;
        for(int64_t i=0;i<row_dec.get_column_num();i++)
        {
            row_dec.get_tid_cid(i,t_id,c_id);
            if(t_id==tid&&c_id==cid)
            {
                ret=true;
                break;
            }
        }
        return ret;
    }

    int ObIndexTrigger::cons_index_row_desc(uint64_t index_tid,const ObSchemaManagerV2 *schema_mgr,ObRowDesc &row_desc)
    {
        int ret=OB_SUCCESS;
        const ObTableSchema *index_table_schema = NULL;
        if (NULL == (index_table_schema = schema_mgr->get_table_schema(index_tid)))
        {
          ret = OB_ERR_ILLEGAL_ID;
          TBSYS_LOG(ERROR,"fail to get table schema for table[%ld]", index_tid);
        }
        else
        {
            const ObRowkeyInfo *rowkey_info = &index_table_schema->get_rowkey_info();
            int64_t rowkey_col_num = rowkey_info->get_size();
            int64_t max_column_id=index_table_schema->get_max_column_id();
            row_desc.set_rowkey_cell_count(rowkey_col_num);
            for (int64_t i = 0;i < rowkey_col_num; ++i)
            {
                const ObRowkeyColumn *rowkey_column = rowkey_info->get_column(i);
                if(OB_SUCCESS !=(ret = row_desc.add_column_desc(index_tid,rowkey_column->column_id_)))
                {
                  break;
                }
            }
            for (int64_t j = OB_APP_MIN_COLUMN_ID; j <= max_column_id;  j++)
            {
                const ObColumnSchemaV2* column_schema=schema_mgr->get_column_schema(index_tid,j);
                if(rowkey_info->is_rowkey_column(j)||NULL==column_schema)
                {}
                else if(OB_SUCCESS != (ret = row_desc.add_column_desc(index_tid,column_schema->get_id())))
                {
                  break;
                }
            }
            if(OB_SUCCESS == ret && schema_mgr->is_index_has_storing(index_tid))
            {
              if(OB_SUCCESS != (ret = row_desc.add_column_desc(index_tid,OB_INDEX_VIRTUAL_COLUMN_ID)))
              {
                TBSYS_LOG(WARN, "add index vitual column failed,ret = %d", ret);
              }
            }
        }
        return ret;
    }

    int ObIndexTrigger::open()
    {
      int ret = OB_SUCCESS;
      //TBSYS_LOG(ERROR, "test::fanqs, ObIndexTrigger::open,,main_table_id_=%ld,index_num_=%ld,cur_index_=%ld",main_table_id_, index_num_,cur_index_);
      //could_insert_ = false;
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

    int ObIndexTrigger::close()
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
    }

    int ObIndexTrigger::get_next_row(const common::ObRow *&row)
    {
      int ret = OB_SUCCESS;
      if (NULL == child_op_)
      {
        ret = OB_NOT_INIT;
      }
      else if(OB_SUCCESS!=(ret=child_op_->get_next_row(row)))
      {
          if(ret!=OB_ITER_END && !IS_SQL_ERR(ret))//add liumz, [ups -5049 log too much]20161217
               TBSYS_LOG(WARN, "child_op get_next_row fail ret=%d", ret);
      }
      return ret;
    }

    int ObIndexTrigger::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
        int ret = OB_SUCCESS;
        if (NULL == child_op_)
        {
          ret = OB_NOT_INIT;
        }
        else if(OB_SUCCESS!=(ret=child_op_->get_row_desc(row_desc)))
        {
          TBSYS_LOG(WARN, "child_op get_next_row fail ret=%d", ret);
        }
        return ret;
    }

    int64_t ObIndexTrigger::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "ObIndexTrigger(main_table_id_=%lu index_num_=%lu cur_index_=%lu", main_table_id_,index_num_,cur_index_);
      //pos += insert_values_.to_string(buf+pos, buf_len-pos);
      databuff_printf(buf, buf_len, pos, ")\n");
      if (NULL != child_op_)
      {
        pos += child_op_->to_string(buf+pos, buf_len-pos);
      }
      return pos;
    }


    PHY_OPERATOR_ASSIGN(ObIndexTrigger)
    {
      int ret = OB_SUCCESS;
      CAST_TO_INHERITANCE(ObIndexTrigger);
      reset();
      cur_index_ = o_ptr->cur_index_;
      main_table_id_=o_ptr->main_table_id_;
      index_num_=o_ptr->index_num_;
      return ret;
    }

    DEFINE_SERIALIZE(ObIndexTrigger)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = cur_row_desc_.serialize(buf, buf_len, pos)))
      {
        TBSYS_LOG(WARN, "serialize row_desc fail ret=%d buf=%p buf_len=%ld pos=%ld", ret, buf, buf_len, pos);
      }
      else if(OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, (int64_t)main_table_id_)))
      {
        TBSYS_LOG(ERROR, "decode_i64(main_table_id_)=>%d", ret);
      }
      else if(OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, (int64_t)index_num_)))
      {
        TBSYS_LOG(ERROR, "decode_i64(index_num_)=>%d", ret);
      }
      else if(OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, (int64_t)cur_index_)))
      {
        TBSYS_LOG(ERROR, "decode_i64(cur_index_)=>%d", ret);
      }
      return ret;
    }

    DEFINE_DESERIALIZE(ObIndexTrigger)
    {
        int ret = OB_SUCCESS;
        if (OB_SUCCESS != (ret = cur_row_desc_.deserialize(buf, data_len, pos)))
        {
          TBSYS_LOG(WARN, "serialize row_desc fail ret=%d buf=%p data_len=%ld pos=%ld", ret, buf, data_len, pos);
        }
        else if(OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &main_table_id_)))
        {
          TBSYS_LOG(ERROR, "decode_i64(main_table_id_)=>%d", ret);
        }
        else if(OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &index_num_)))
        {
          TBSYS_LOG(ERROR, "decode_i64(index_num_)=>%d", ret);
        }
        else if(OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &cur_index_)))
        {
          TBSYS_LOG(ERROR, "decode_i64(cur_index_)=>%d", ret);
        }
        return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObIndexTrigger)
    {
        return cur_row_desc_.get_serialize_size()+serialization::encoded_length_i64(main_table_id_)*3;
    }
  } // end namespace sql
} // end namespace oceanbase
