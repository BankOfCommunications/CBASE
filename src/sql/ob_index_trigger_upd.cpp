#include "ob_index_trigger_upd.h"
#include "common/ob_obj_cast.h"
//add lbzhong [Update rowkey] 20160113:b
#include "common/hash/ob_hashmap.h"
//add:e
using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObIndexTriggerUpd, PHY_INDEX_TRIGGER_UPD);
  }
}

ObIndexTriggerUpd::ObIndexTriggerUpd()
{
  //add wenghaixing [secondary index upd.bugfix]20150127
  has_other_cond_=false;
  //arena_.reuse();
  //add e
}

ObIndexTriggerUpd::~ObIndexTriggerUpd()
{
  arena_.free();
}

int ObIndexTriggerUpd::open()
{

  int ret = OB_SUCCESS;
  //add wenghaixing [secondary index upd.bugfix]20150127
  row_.set_row_desc(data_row_desc_);
  arena_.reuse();
  //add e
  if (OB_SUCCESS != (ret = ObSingleChildPhyOperator::open()))
  {
    if (!IS_SQL_ERR(ret))
    {
      TBSYS_LOG(WARN, "failed to open child_op, err=%d", ret);
    }
  }
    /*
    else if(OB_SUCCESS!=(ret=get_next_data_row()))
    {
        TBSYS_LOG(ERROR,"failed to get data table row!ret[%d]",ret);
    }
    else
    {
        TBSYS_LOG(ERROR,"test::whx,success open()");
    }
    */


  return ret;
}

//add wenghaixing [secondary index upd.4] 20141129
int ObIndexTriggerUpd::close()
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
      //add liuxiao [secondary index static_index_build.bug_fix.merge_error]20150604
      ret = tmp_ret;
      //add e
    }
    else
    {
      arena_.reuse();
    }
  }
  return ret;
}

int ObIndexTriggerUpd::handle_trigger(const ObSchemaManagerV2 *schema_mgr, updateserver::ObIUpsTableMgr *host,
                                      updateserver::RWSessionCtx &session_ctx,  ObRowStore *store)
{
  int ret = OB_SUCCESS;
  if(schema_mgr == NULL)
  {
    TBSYS_LOG(ERROR,"schema manager is NULL");
    ret = OB_SCHEMA_ERROR;
  }
  else
  {
    store->set_is_update_second_index(true); //add lbzhong [Update rowkey] 20151223:b:e
    for(int64_t i = 0;i<update_index_num_;i++)
    {
      if(OB_SUCCESS != (ret = handle_trigger_one_table(i,schema_mgr,host,session_ctx, store)) && ret != OB_DECIMAL_UNLEGAL_ERROR)
      {
        int err = OB_SUCCESS;
        const ObTableSchema* schema = NULL;
        uint64_t tid = OB_INVALID_ID;
        uint64_t cid = OB_INVALID_ID;
        if(OB_SUCCESS != (err = (index_row_desc_del_[i].get_tid_cid(0,tid,cid))))
        {
          TBSYS_LOG(WARN,"failed to get table id from index_row_desc_del_,err[%d]",err);
          break;
        }
        else
        {
          schema = schema_mgr->get_table_schema(tid);
        }
        if(NULL != schema || OB_INVALID_ID == tid)
        {
          TBSYS_LOG(ERROR,"handle one table error,err=%d",ret);
          ret = OB_ERROR;
          break;
        }
        else
        {
          TBSYS_LOG(WARN,"this index is not in schema,maybe was droped!tid[%ld]",tid);
          ret = OB_SUCCESS;
          continue;
        }
      }
    }
  }
  return ret;
}

int ObIndexTriggerUpd::handle_trigger_one_table(int64_t index_num,const ObSchemaManagerV2 *schema_mgr,
                                                updateserver::ObIUpsTableMgr *host, updateserver::RWSessionCtx &session_ctx,  ObRowStore* store)
{
  int ret = OB_SUCCESS;
  //TBSYS_LOG(ERROR,"test::whx get_next_data_row!");
  common::ObRow input_row;
  //common::ObRowDesc *desc=NULL;
  const ObRowStore::StoredRow *stored_row = NULL;
  ObRowStore store_del;
  ObRowStore store_upd;
  common::ObRow row_to_store;
  int64_t i = index_num;
  char* varchar_buff = NULL;
  input_row.set_row_desc(data_row_desc_);
  sql::ObProject *tmp_op=NULL;
  //add lbzhong [Update rowkey] 20160511:b
  store_del.set_is_update_second_index(true);
  store_upd.set_is_update_second_index(true);
  //add:e
  if (NULL == store)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "child_op_ is NULL");
  }
  /*else if(NULL == child_op_->get_child(0))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "second child_op_ is NULL");
  }
  //add wenghaixing [secondary index upd.bugfix]20150127
  else if(has_other_cond_ && NULL == child_op_->get_child(0)->get_child(0))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "second child_op_ is NULL");
  }*/
  else if(NULL == (varchar_buff = arena_.alloc(OB_MAX_VARCHAR_LENGTH)))
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(WARN, "No memory");
  }
  else if(NULL == (tmp_op = static_cast<sql::ObProject*>(child_op_)))
  {
    ret = OB_ERROR;
  }
  //add e
  else
  {
    uint64_t data_tid = OB_INVALID_ID;
    uint64_t tid = OB_INVALID_ID;
    uint64_t cid = OB_INVALID_ID;
    const ObObj *obj=NULL;
    ObObj tmp_value;
    ObObj obj_dml, obj_virtual;
    const ObRowDesc *row_desc=NULL;
    if(OB_SUCCESS != (ret = get_trigger_row_desc(row_desc)))
    {
      TBSYS_LOG(WARN,"get row desc failed,ret[%d]",ret);
    }
    if(OB_SUCCESS == ret)
    {
      //add lbzhong [Update rowkey] 20160103:b
      hash::ObHashMap<int64_t, int32_t> index_columms_map; //<column_id, cast_obj index>
      bool is_update_rowkey = false;
      int64_t rowkey_size = data_row_desc_.get_rowkey_cell_count();
      if(set_index_columns_.count() != cast_obj_.count())
      {
        index_columms_map.create(512);
        uint64_t column_id = OB_INVALID_ID;
        is_update_rowkey = true;
        for(int32_t expr_num = 0, cast_obj_idx = 0; expr_num < set_index_columns_.count() && OB_SUCCESS == ret; expr_num++)
        {
          column_id = set_index_columns_.at(expr_num).get_column_id();
          bool is_exist_update = exist_update_column(column_id);
          if((expr_num < rowkey_size && !is_exist_update) || (expr_num >= rowkey_size && is_exist_update))
          {
            index_columms_map.set(column_id, cast_obj_idx);
            cast_obj_idx++;
          }
        }
      }
      //add:e
      //data_tid=row_desc_for_tid->get_tid_cid(0,data_tid,cid);
      tmp_op->reset_row_num_idx();
      while(OB_SUCCESS == ret)
      {
        ret = store->get_next_row(input_row);
        if(OB_SUCCESS != ret)
        {
          break;
        }
        //fill sequence val
        if(OB_SUCCESS == ret && OB_SUCCESS != (ret = tmp_op->fill_sequence_info(set_index_columns_)))
        {
          TBSYS_LOG(WARN, "fill sql expr array failed, ret = %d",ret);
        }
        //first we construct delete row
        ObString varchar;
        ObObj casted_cell;
        ObObj data_type;
        for(int64_t j = 0;j<row_desc->get_column_num()&&OB_SUCCESS == ret;j++)
        {
          if(OB_SUCCESS != (ret = row_desc->get_tid_cid(j,data_tid,cid)))
          {
            TBSYS_LOG(ERROR,"failed in get tid_cid from row desc,ret[%d]",ret);
            break;
          }
          else if(OB_ACTION_FLAG_COLUMN_ID == cid)
          {
          }
          else if(OB_INDEX_VIRTUAL_COLUMN_ID == cid)
          {
          }
          else if(OB_SUCCESS != (ret = input_row.get_cell(data_tid,cid,obj)))
          {
            TBSYS_LOG(ERROR,"failed in get cell from input_row,ret[%d],tid[%ld],cid[%ld]",ret,data_tid,cid);
            break;
          }
          else
          {
            if(NULL == obj)
            {
              TBSYS_LOG(ERROR,"obj's pointer can not be NULL!");
              ret = OB_INVALID_ARGUMENT;
              break;
            }
            else if(OB_SUCCESS != (ret = row_.set_cell(data_tid,cid,*obj)))
            {
              TBSYS_LOG(ERROR,"set cell for update failed,ret=%d",ret);
              break;
            }
          }
        }
        row_to_store.set_row_desc(index_row_desc_del_[i]);
        for(int col_idx = 0;col_idx<index_row_desc_del_[i].get_column_num()&&OB_SUCCESS == ret;col_idx++)
        {
          if(OB_SUCCESS != (ret = index_row_desc_del_[i].get_tid_cid(col_idx,tid,cid)))
          {
            TBSYS_LOG(ERROR,"failed in get tid_cid from row desc,ret[%d]",ret);
            break;
          }
          else if(OB_ACTION_FLAG_COLUMN_ID == cid)
          {
            obj_dml.set_int(ObActionFlag::OP_DEL_ROW);
            if(OB_SUCCESS != (ret = row_to_store.set_cell(tid,cid,obj_dml)))
            {
              TBSYS_LOG(ERROR,"set cell for update failed,ret=%d",ret);
              break;
            }
          }
          else if(OB_SUCCESS != (ret = row_.get_cell(data_tid,cid,obj)))
          {
            TBSYS_LOG(ERROR,"failed in get cell from input_row,ret[%d],tid[%ld],cid[%ld]",ret,data_tid,cid);
            break;
          }
          else
          {
            if(NULL == obj)
            {
              TBSYS_LOG(ERROR,"obj's pointer can not be NULL!");
              ret = OB_INVALID_ARGUMENT;
              break;
            }
            else if(OB_SUCCESS != (ret = row_to_store.set_cell(tid,cid,*obj)))
            {
              TBSYS_LOG(ERROR,"set cell for update failed,ret=%d",ret);
              break;
            }
          }
        }
        if(OB_SUCCESS == ret)
        {
          if(OB_SUCCESS != (ret = (store_del.add_row(row_to_store,stored_row))))
          {
            TBSYS_LOG(ERROR, "failed to do row_store.add_row faile,ret=%d,row=%s",ret,to_cstring(row_to_store));
          }
          else
          {
            row_to_store.clear();
          }
        }
        //TBSYS_LOG(ERROR,"test::whx row_to_store_del=%s",to_cstring(store_del));
        //second we construct update row
        row_to_store.set_row_desc(index_row_desc_upd_[i]);
        for(int col_idx = 0;col_idx<index_row_desc_upd_[i].get_column_num()&&OB_SUCCESS == ret;col_idx++)
        {
          if(OB_SUCCESS != (ret = index_row_desc_upd_[i].get_tid_cid(col_idx,tid,cid)))
          {
            TBSYS_LOG(ERROR,"failed in get tid_cid from row desc,ret[%d]",ret);
            break;
          }
          else if(OB_ACTION_FLAG_COLUMN_ID == cid)
          {
          }
          else if(OB_INDEX_VIRTUAL_COLUMN_ID == cid)
          {
            obj_virtual.set_null();
            obj = &obj_virtual;
          }
          else if(OB_SUCCESS != (ret = row_.get_cell(data_tid,cid,obj)))
          {
            TBSYS_LOG(ERROR,"failed in get cell from input_row,ret[%d]",ret);
            break;
          }

          if(OB_SUCCESS == ret)
          {
            //add lbzhong [Update rowkey] 20160112:b
            if(!is_update_rowkey) //not update sequence for rowkey
            {
              for(int expr_num = 0;expr_num<set_index_columns_.count()&&OB_SUCCESS == ret;expr_num++)
              {
                varchar.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
                casted_cell.set_varchar(varchar);
                if(OB_SUCCESS != (ret = cast_obj_.at(expr_num, data_type)))
                {
                  TBSYS_LOG(WARN, "get cast obj failed!ret [%d]",ret);
                  break;
                }
                if(cid == set_index_columns_.at(expr_num).get_column_id())
                {
                  ObSqlExpression &expr = set_index_columns_.at(expr_num);
                  if (OB_SUCCESS != (ret = expr.calc(row_, obj)))
                  {
                    TBSYS_LOG(WARN, "failed to calculate, err=%d", ret);
                    break;
                  }
                  else if(OB_SUCCESS != (obj_cast(*obj, data_type, casted_cell, obj)))
                  {
                    break;
                  }
                  else if(OB_SUCCESS != (ret = ob_write_obj_v2(arena_, *obj, tmp_value)))
                  {
                    TBSYS_LOG(WARN, "str buf write obj fail:ret[%d]", ret);
                    break;
                  }
                  else
                  {
                    //TBSYS_LOG(ERROR, "test::whx obj p = %d,s = %d,type = %d", data_type.get_precision(), data_type.get_scale(),data_type.get_type());
                    obj = &tmp_value;
                  }
                }
              }
            }
            else
            {
              int32_t cast_obj_idx = -1;
              for(int expr_num = 0; expr_num < set_index_columns_.count() && OB_SUCCESS == ret; expr_num++)
              {
                bool is_exist_update = exist_update_column(cid);
                if((expr_num < rowkey_size && !is_exist_update) || (expr_num >= rowkey_size && is_exist_update))
                {
                  if(cid == set_index_columns_.at(expr_num).get_column_id())
                  {
                    varchar.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
                    casted_cell.set_varchar(varchar);
                    ObSqlExpression &expr = set_index_columns_.at(expr_num);
                    if(hash::HASH_NOT_EXIST == index_columms_map.get(cid, cast_obj_idx))
                    {
                      TBSYS_LOG(WARN, "failed to get cast_obj_idx from index_columms_map, column_id=%ld, ret [%d]", cid, ret);
                      break;
                    }
                    else if(OB_SUCCESS != (ret = cast_obj_.at(cast_obj_idx, data_type)))
                    {
                      TBSYS_LOG(WARN, "get cast obj failed, idx=%d, ret [%d]", cast_obj_idx, ret);
                      break;
                    }
                    if (OB_SUCCESS != (ret = expr.calc(row_, obj)))
                    {
                      TBSYS_LOG(WARN, "failed to calculate, err=%d", ret);
                      break;
                    }
                    else if(OB_SUCCESS != (ret = obj_cast(*obj, data_type, casted_cell, obj)))
                    {
                      TBSYS_LOG(WARN, "fail to cast obj[%s], data_type=%s, ret[%d]", to_cstring(*obj), to_cstring(data_type), ret);
                      break;
                    }
                    else if(OB_SUCCESS != (ret = ob_write_obj_v2(arena_, *obj, tmp_value)))
                    {
                      TBSYS_LOG(WARN, "str buf write obj fail:ret[%d]", ret);
                      break;
                    }
                    else
                    {
                      obj = &tmp_value;
                      break;
                    }
                  }
                }
              }
            }
            //add:e

            if(OB_SUCCESS == ret)
            {
              if(NULL == obj)
              {
                TBSYS_LOG(ERROR,"obj's pointer can not be NULL!");
                ret = OB_INVALID_ARGUMENT;
                break;
              }
              else if(OB_SUCCESS != (ret = row_to_store.set_cell(tid,cid,*obj)))
              {
                TBSYS_LOG(ERROR,"set cell for update failed,ret=%d",ret);
                break;
              }
            }
          }
        }
        if(OB_SUCCESS == ret)
        {
          if(OB_SUCCESS != (ret = (store_upd.add_row(row_to_store,stored_row))))
          {
            TBSYS_LOG(ERROR, "failed to do row_store.add_row faile,ret=%d,row=%s",ret,to_cstring(row_to_store));
          }
          else
          {
            row_to_store.clear();
          }
          //TBSYS_LOG(ERROR,"test::whx row_to_store_upd=%s",to_cstring(store_upd));
        }

      }
      //add lbzhong [Update rowkey] 20160113:b
      index_columms_map.destroy();
      //add:e
      if(ret == OB_ITER_END)
         ret = OB_SUCCESS;
      tmp_op->reset_row_num_idx();
    }
  }
  //modify liuxiao [secondary index static_index_build.bug_fix.merge_error]20150604
  //reset_iterator();
  if(NULL != child_op_)
  {
    reset_iterator();
  }
  //modify e
  if(ret == OB_SUCCESS)
  {
    ObIndexCellIterAdaptor icia;
    ObDmlType dml_type = OB_DML_DELETE;
    icia.set_row_iter(&store_del,index_row_desc_del_[i].get_rowkey_cell_count(),schema_mgr,index_row_desc_del_[i]);
    ret = host->apply(session_ctx,icia,dml_type);
    // TBSYS_LOG(ERROR, "test::fanqs  host->apply ret=%d",ret);

  }
  store_del.clear();
  if(ret == OB_SUCCESS)
  {
    ObIndexCellIterAdaptor icia;
    ObDmlType dml_type = OB_DML_INSERT;
    icia.set_row_iter(&store_upd,index_row_desc_upd_[i].get_rowkey_cell_count(),schema_mgr,index_row_desc_upd_[i]);
    ret = host->apply(session_ctx,icia,dml_type);

    // TBSYS_LOG(ERROR, "test::fanqs  host->apply ret=%d",ret);

  }
  //add liuxiao [secondary index bug fix] 20150908
  if(OB_SUCCESS == ret && NULL != store)
  {
    store->reset_iterator();
  }
  //add e;
  store_upd.clear();
  return ret;
}
//add e
void ObIndexTriggerUpd::reset()
{
  index_num_ = 0;
  update_index_num_ = 0;
  set_index_columns_.clear();
  cast_obj_.clear();
  data_row_desc_.reset();
  for(int64_t i = 0;i<OB_MAX_INDEX_NUMS;i++)
  {
    index_row_desc_del_[i].reset();
    index_row_desc_upd_[i].reset();
  }
  arena_.reuse();
  ObSingleChildPhyOperator::reset();
  //add lbzhong [Update rowkey] 20160112:b
  update_columns_.clear();
  //add:e
}

void ObIndexTriggerUpd::reuse()
{
  index_num_=0;
  update_index_num_ = 0;
  set_index_columns_.clear();
  cast_obj_.clear();
  data_row_desc_.reset();
  for(int64_t i = 0;i<OB_MAX_INDEX_NUMS;i++)
  {
    index_row_desc_del_[i].reset();
    index_row_desc_upd_[i].reset();
  }    //ObSingleChildPhyOperator::reuse();
  arena_.reuse();
  //add lbzhong [Update rowkey] 20160112:b
  update_columns_.clear();
  //add:e
}

/*int ObIndexTriggerUpd::add_output_column(uint64_t index_num, const ObSqlExpression &expr)
{
    int ret = OB_SUCCESS;
    if(OB_SUCCESS==(ret=index_columns_[index_num].push_back(expr)))
    {
        index_columns_[index_num].at(index_columns_[index_num].count() - 1).set_owner_op(this);
    }
    index_num_=index_num+1;

    return ret;
}
*/

int ObIndexTriggerUpd::add_set_column(const ObSqlExpression &expr)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS == (ret = set_index_columns_.push_back(expr)))
  {
    set_index_columns_.at(set_index_columns_.count() - 1).set_owner_op(this);
  }
  return ret;
}

int ObIndexTriggerUpd::add_cast_obj(const ObObj &obj)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS == (ret = cast_obj_.push_back(obj)))
  {
    //cast_obj_.at(cast_obj_.count() - 1).set_owner_op(this);
  }
  return ret;
}
//add wenghaixing [secondary index upd.bugfix]20150127
void ObIndexTriggerUpd::set_data_row_desc(common::ObRowDesc &desc)
{
  data_row_desc_ = desc;
}
void ObIndexTriggerUpd::set_cond_bool(bool val)
{
  has_other_cond_ = val;
}
void ObIndexTriggerUpd::get_cond_bool(bool &val)
{
  val = has_other_cond_;
}
void ObIndexTriggerUpd::reset_iterator()
{
  if(!has_other_cond_)
  {
    ObMultipleGetMerge* omg=static_cast<ObMultipleGetMerge*>(child_op_->get_child(0));
    omg->reset_iterator();
  }
  else
  {
    ObMultipleGetMerge* omg=static_cast<ObMultipleGetMerge*>(child_op_->get_child(0)->get_child(0));
    omg->reset_iterator();
  }
}

//add e

//add lijianqiang [sequence update] 20150916:b
void ObIndexTriggerUpd::clear_clumns()
{
  set_index_columns_.clear();
  TBSYS_LOG(DEBUG,"clear the index array!");
}
//add 20150926:e
int ObIndexTriggerUpd::add_row_desc_del(int64_t idx,common::ObRowDesc desc)
{
  int ret = OB_SUCCESS;
  if(idx >= OB_MAX_INDEX_NUMS||idx<0)
  {
    TBSYS_LOG(ERROR,"add row desc_del ,idx is invalid! idx=%ld",idx);
    ret = OB_ERROR;
  }
  else
    index_row_desc_del_[idx]=desc;
  return ret;
}
int ObIndexTriggerUpd::add_row_desc_upd(int64_t idx,common::ObRowDesc desc)
{
  int ret = OB_SUCCESS;
  if(idx >= OB_MAX_INDEX_NUMS||idx<0)
  {
    TBSYS_LOG(ERROR,"add row desc_del ,idx is invalid! idx=%ld",idx);
    ret=OB_ERROR;
  }
  else
    index_row_desc_upd_[idx]=desc;
  return ret;
}

int ObIndexTriggerUpd::get_row_desc_del(int64_t idx, common::ObRowDesc &desc)
{
  int ret = OB_SUCCESS;
  if(idx >= OB_MAX_INDEX_NUMS)
  {
    ret=OB_ERROR;
  }
  else
    desc=index_row_desc_del_[idx];
  return ret;
}

void ObIndexTriggerUpd::set_index_num(int64_t num)
{
  index_num_=num;
}

void ObIndexTriggerUpd::set_update_index_num(int64_t num)
{
  update_index_num_ = num;
}

int ObIndexTriggerUpd::get_row_desc_upd(int64_t idx, common::ObRowDesc &desc)
{
  int ret=OB_SUCCESS;
  if(idx>OB_MAX_INDEX_NUMS)
  {
    ret=OB_ERROR;
  }
  else
    desc=index_row_desc_upd_[idx];
  return ret;
}
int ObIndexTriggerUpd:: get_next_data_row(const ObRow *&input_row)
{
  int ret = OB_SUCCESS;
  if(!has_other_cond_)
  {
    ret = child_op_->get_child(0)->get_next_row(input_row);
  }
  else if(has_other_cond_)
  {
    ret = child_op_->get_child(0)->get_child(0)->get_next_row(input_row);
  }
  if(OB_SUCCESS != ret && OB_ITER_END != ret)
  {
    TBSYS_LOG(ERROR,"can not get next_row ,err = %d",ret);
  }
  return ret;
}

int ObIndexTriggerUpd::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  if (NULL == child_op_)
  {
    ret = OB_NOT_INIT;
  }
  else
  {
    ObProject* op=static_cast<ObProject*>(child_op_);
    if(OB_SUCCESS != (ret = op->get_next_row(row)))
    {
      if(ret != OB_ITER_END && !IS_SQL_ERR(ret))//add liumz, [ups -5049 log too much]20161217
         TBSYS_LOG(WARN, "child_op get_next_row fail ret=%d", ret);
          //else ret=OB_SUCCESS;
    }
  }
  return ret;
}

int ObIndexTriggerUpd::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
    //row_desc = &data_row_desc_;
  if(OB_UNLIKELY(NULL == child_op_))
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "child op pointer is NULL");
  }
  else
  {
    ret = child_op_->get_row_desc(row_desc);
  }
  return ret;
}

int ObIndexTriggerUpd::get_trigger_row_desc(const ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  row_desc = &data_row_desc_;
  return ret;
}

DEFINE_SERIALIZE(ObIndexTriggerUpd)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos,index_num_)))
  {
    TBSYS_LOG(WARN,"failed to encode index_num_,ret[%d]",ret);
  }
  else if(OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos,update_index_num_)))
  {
    TBSYS_LOG(WARN,"failed to encode update index_num_,ret[%d]",ret);
  }
  else if(OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos,(uint64_t)data_max_cid_)))
  {
    TBSYS_LOG(WARN,"failed to encode data_max_cid_,ret[%d]",ret);
  }
  //for fix other cond bug  wenghaixing 20150127
  else if(OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, (int64_t)has_other_cond_)))
  {
    TBSYS_LOG(WARN,"failed to encode has_other_cond_");
  }
  //add e
  else
  {
    for(int64_t i=0;i<update_index_num_;i++)
    {
      if (ret == OB_SUCCESS && (OB_SUCCESS != (ret = index_row_desc_del_[i].serialize(buf, buf_len, pos))))
      {
        TBSYS_LOG(WARN, "serialize fail. ret=%d", ret);
        break;
      }
      if (ret == OB_SUCCESS && (OB_SUCCESS != (ret = index_row_desc_upd_[i].serialize(buf, buf_len, pos))))
      {
        TBSYS_LOG(WARN, "serialize fail. ret=%d", ret);
        break;
      }
    }
    if (ret == OB_SUCCESS && (OB_SUCCESS != (ret = data_row_desc_.serialize(buf, buf_len, pos))))
    {
      TBSYS_LOG(WARN, "serialize fail. ret=%d", ret);
    }
  }
  if(OB_SUCCESS == ret)
  {
    int64_t len = 0;
    len = set_index_columns_.count();
    if(OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos,len)))
    {
      TBSYS_LOG(WARN,"failed to encode ,ret[%d]",ret);
    }
    else
    {
      for(int64_t j=0;j<len;j++)
      {
        const ObSqlExpression &expr = set_index_columns_.at(j);
        if (ret == OB_SUCCESS && (OB_SUCCESS != (ret = expr.serialize(buf, buf_len, pos))))
        {
          TBSYS_LOG(WARN, "serialize fail. ret=%d", ret);
          break;
        }
      }
    }
  }

  if(OB_SUCCESS == ret)
  {
    int64_t len = 0;
    len = cast_obj_.count();
    if(OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos,len)))
    {
      TBSYS_LOG(WARN,"failed to encode ,ret[%d]",ret);
    }
    else
    {
      for(int64_t j = 0; j < len; j++)
      {
        const ObObj &obj = cast_obj_.at(j);
        if(OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "serialize fail. ret=%d", ret);
          break;
        }
      }
    }
  }
  //add lbzhong [Update rowkey] 20160113:b
  if(OB_SUCCESS == ret)
  {
    int64_t update_columns_count = update_columns_.count();
    if(OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, update_columns_count)))
    {
      TBSYS_LOG(WARN,"failed to encode update_columns_.count(), ret[%d]", ret);
    }
    else
    {
      for(int64_t i = 0; i < update_columns_count; i++)
      {
        if(OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, update_columns_.at(i))))
        {
          TBSYS_LOG(WARN,"failed to encode update_columns_[%ld], ret[%d]", i, ret);
          break;
        }
      }
    }
  }
  //add:e
  return ret;
}

DEFINE_DESERIALIZE(ObIndexTriggerUpd)
{
  int ret=OB_SUCCESS;
  //add for fix other cond bug  wenghaixing 20150127
  int64_t cond_flag = 0;
  int64_t max_cid = OB_INVALID_ID;
  //add e
  if(OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &index_num_)))
  {
    TBSYS_LOG(WARN,"failed to decode index_num_,ret[%d]",ret);
  }
  else if(OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &update_index_num_)))
  {
    TBSYS_LOG(WARN,"failed to decode update index_num_,ret[%d]",ret);
  }
  else if(OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &max_cid)))
  {
    TBSYS_LOG(WARN,"failed to decode max_cid,ret[%d]",ret);
  }
  //add for fix other cond bug  wenghaixing 20150127
  else if(OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &cond_flag)))
  {
    TBSYS_LOG(WARN,"failed to decode cond_flag");
  }
  //add e
  else
  {
    for(int64_t i=0;i<update_index_num_;i++)
    {
      if (OB_SUCCESS != (ret = index_row_desc_del_[i].deserialize(buf, data_len, pos)))
      {
        TBSYS_LOG(WARN, "fail to deserialize expression. ret=%d", ret);
        break;
      }
      else if (OB_SUCCESS != (ret = index_row_desc_upd_[i].deserialize(buf, data_len, pos)))
      {
        TBSYS_LOG(WARN, "fail to deserialize expression. ret=%d", ret);
        break;
      }
    }
    if (OB_SUCCESS == ret&&OB_SUCCESS != (ret = data_row_desc_.deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to deserialize expression. ret=%d", ret);
    }
  }
  if(OB_SUCCESS == ret)
  {
    int64_t len=0;
    ObSqlExpression expr;
    if(OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &len)))
    {
      TBSYS_LOG(WARN,"failed to decode index_num_,ret[%d]",ret);
    }
    else
    {
      for(int64_t j=0;j<len;j++)
      {
        if (OB_SUCCESS != (ret = add_set_column(expr)))
        {
          TBSYS_LOG(DEBUG, "fail to add expr to project ret=%d. buf=%p, data_len=%ld, pos=%ld", ret, buf, data_len, pos);
          break;
        }
        if (OB_SUCCESS != (ret = set_index_columns_.at(j).deserialize(buf, data_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to deserialize expression. ret=%d", ret);
          break;
        }
      }
    }
  }
  if(OB_SUCCESS == ret)
  {
    int64_t len = 0;
    ObObj obj;
    if(OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &len)))
    {
      TBSYS_LOG(WARN,"failed to decode index_num_,ret[%d]",ret);
    }
    else
    {
      for(int64_t j = 0; j < len; j++)
      {
        if (OB_SUCCESS != (ret = add_cast_obj(obj)))
        {
          TBSYS_LOG(WARN, "failed to add cast obj to project ret = %d",ret);
          break;
        }
        if(OB_SUCCESS != (ret = cast_obj_.at(cast_obj_.count()-1).deserialize(buf, data_len, pos)))
        {
          TBSYS_LOG(WARN, "failed to deserialize expression. ret = %d",ret);
          break;
        }
      }
    }
  }
  //add for fix other cond bug  wenghaixing 20150127
  if(OB_SUCCESS == ret)
  {
    has_other_cond_ = cond_flag == 0?false:true;
    data_max_cid_ = (uint64_t)max_cid;
  }
  //add e
  //add lbzhong [Update rowkey] 20160113:b
  if(OB_SUCCESS == ret)
  {
    int64_t update_columns_count = 0;
    if(OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &update_columns_count)))
    {
      TBSYS_LOG(WARN,"failed to encode update_columns_.count(), ret[%d]",ret);
    }
    else
    {
      int64_t column_id = OB_INVALID_ID;
      update_columns_.clear();
      for(int64_t i = 0; i < update_columns_count; i++)
      {
        if(OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &column_id)))
        {
          TBSYS_LOG(WARN,"failed to decode update_columns_[%ld], ret[%d]", i, ret);
          break;
        }
        else if(OB_SUCCESS != (ret = update_columns_.push_back(column_id)))
        {
          TBSYS_LOG(WARN,"failed to push_back update_columns_[%ld], column_id=%ld, ret[%d]", i, column_id, ret);
          break;
        }
      }
    }
  }
  //add:e
  return ret;
}


DEFINE_GET_SERIALIZE_SIZE(ObIndexTriggerUpd)
{
 /* int64_t size = 0;
  ObObj obj;
  obj.set_int(columns_.count());
  size += obj.get_serialize_size();
  for (int64_t i = 0; i < columns_.count(); ++i)
  {
    const ObSqlExpression &expr = columns_.at(i);
    size += expr.get_serialize_size();
  }
  */

  int64_t size = 0;
  size+=serialization::encoded_length_i64(index_num_);
  size+=serialization::encoded_length_i64(update_index_num_);
  size+=serialization::encoded_length_i64(data_max_cid_);
  for(int64_t i=0;i<index_num_;i++)
  {
    size += index_row_desc_del_[i].get_serialize_size();
    size += index_row_desc_upd_[i].get_serialize_size();
  }
  size += data_row_desc_.get_serialize_size();
  size += static_cast<int64_t>(sizeof(int64_t));
  for(int64_t j=0;j<set_index_columns_.count();j++)
  {
    const ObSqlExpression &expr = set_index_columns_.at(j);
    size += expr.get_serialize_size();
  }
  size += static_cast<int64_t>(sizeof(int64_t));
  for(int64_t j = 0; j < cast_obj_.count(); j++)
  {
    const ObObj &obj = cast_obj_.at(j);
    size += obj.get_serialize_size();
  }
  //add for fix other cond bug  wenghaixing 20150127
  size += static_cast<int64_t>(sizeof(int64_t));
  //add e
  //add lbzhong [Update rowkey] 20160112:b
  size += static_cast<int64_t>(sizeof(int64_t)); // for update_columns_ count
  for(int64_t j = 0; j < update_columns_.count(); j++)
  {
    size += static_cast<int64_t>(sizeof(int64_t));
  }
  //add:e
  return size;
}


ObPhyOperatorType ObIndexTriggerUpd::get_type() const
{
  return PHY_INDEX_TRIGGER_UPD;
}

int64_t ObIndexTriggerUpd::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObIndexTriggerUpd(index_num=[%ld],columns:",update_index_num_);
  databuff_printf(buf, buf_len, pos,"Row Desc:");
  for(int64_t i=0;i<update_index_num_;i++)
  {
    int64_t pos2 = index_row_desc_del_[i].to_string(buf+pos, buf_len-pos);
    pos += pos2;
    if (i != update_index_num_ -1)
    {
      databuff_printf(buf, buf_len, pos, ",");
    }
    pos2 = index_row_desc_upd_[i].to_string(buf+pos, buf_len-pos);
    pos += pos2;
    if (i != update_index_num_ -1)
    {
      databuff_printf(buf, buf_len, pos, ",");
    }
  }
  int64_t pos2 = data_row_desc_.to_string(buf+pos, buf_len-pos);
  pos += pos2;
  databuff_printf(buf, buf_len, pos, "\n");
  databuff_printf(buf, buf_len, pos, "set_comluns EXPR:");
  for(int64_t j=0;j<set_index_columns_.count();j++)
  {
    int64_t pos2 = set_index_columns_.at(j).to_string(buf+pos, buf_len-pos);
    pos += pos2;
    if (j != set_index_columns_.count() -1)
    {
      databuff_printf(buf, buf_len, pos, ",");
    }
  }
  if (NULL != child_op_)
  {
    int64_t pos2 = child_op_->to_string(buf+pos, buf_len-pos);
    pos += pos2;
  }
  return pos;
}

PHY_OPERATOR_ASSIGN(ObIndexTriggerUpd)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObIndexTriggerUpd);
  reset();
  for(int64_t i=0;i<o_ptr->index_num_;i++)
  {
    index_row_desc_del_[i]=o_ptr->index_row_desc_del_[i];
    index_row_desc_upd_[i]=o_ptr->index_row_desc_upd_[i];
  }
  data_row_desc_=o_ptr->data_row_desc_;
  for(int64_t j=0;j<o_ptr->set_index_columns_.count();j++)
  {
    if(ret!=OB_SUCCESS)break;
    if ((ret = set_index_columns_.push_back(o_ptr->set_index_columns_.at(j))) == OB_SUCCESS)
    {
      set_index_columns_.at(j).set_owner_op(this);
    }
    else
    {
      break;
    }
  }
  index_num_ = o_ptr->index_num_;
  update_index_num_ = o_ptr->update_index_num_;
  return ret;
}
//add lbzhong [Update rowkey] 20160112:b
bool ObIndexTriggerUpd::exist_update_column(const uint64_t column_id) const
{
  for (int32_t i = 0; i < update_columns_.count(); i++)
  {
    if(column_id == update_columns_.at(i))
    {
      return true;
    }
  }
  return false;
}
void ObIndexTriggerUpd::set_update_columns(ObArray<uint64_t> &update_columns)
{
  update_columns_ = update_columns;
}

//add:e
