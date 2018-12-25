/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_modify.h
 *
 * Authors:
 *   Li Kai <yubai.lk@alipay.com>
 *
 */
#ifndef  OCEANBASE_UPDATESERVER_MEMTABLE_MODIFY_H_
#define  OCEANBASE_UPDATESERVER_MEMTABLE_MODIFY_H_

#include "sql/ob_ups_modify.h"
#include "common/ob_iterator.h"
#include "common/ob_iterator_adaptor.h"
#include "ob_sessionctx_factory.h"
#include "ob_ups_table_mgr.h"
#include "ob_ups_utils.h"
//add fanqiushi_index
#include "sql/ob_index_trigger.h"
//add:e
//add wenghaixing [secondary index upd.4]20141129
#include "sql/ob_index_trigger_upd.h"
//add e
// add by liyongfeng:20150105 [secondary index replace]
#include "sql/ob_index_trigger_rep.h"
// add:e
//add liumengzhan_delete_index
#include "sql/ob_delete_index.h"
//add:e
#include "sql/ob_phy_operator.h"// add by maosy [Delete_Update_Function_isolation_RC] 20161218

namespace oceanbase
{
  namespace updateserver
  {
    template <class T>
    class MemTableModifyTmpl : public T, public RowkeyInfoCache
    {
      public:
        MemTableModifyTmpl(RWSessionCtx &session, ObIUpsTableMgr &host);
        ~MemTableModifyTmpl();
      public:
        int open();
        int close();
        int get_next_row(const common::ObRow *&row);
        int get_row_desc(const common::ObRowDesc *&row_desc) const;
        int get_affected_rows(int64_t& row_count);
        int64_t to_string(char* buf, const int64_t buf_len) const;
        //add wenghaixing [secondary index check_index_num]20150807
        int64_t get_index_num();
        //add e
        //add wenghaixing [secondary index lock_conflict]20150807
        bool ob_operator_has_trigger();
        //add e
      private:
        RWSessionCtx &session_;
        ObIUpsTableMgr &host_;
    };

    template <class T>
    MemTableModifyTmpl<T>::MemTableModifyTmpl(RWSessionCtx &session, ObIUpsTableMgr &host): session_(session),
                                                                                 host_(host)
    {
    }

    template <class T>
    MemTableModifyTmpl<T>::~MemTableModifyTmpl()
    {
    }

    template <class T>
    int MemTableModifyTmpl<T>::open()
    {
      int ret = OB_SUCCESS;
      const ObRowDesc *row_desc = NULL;
      uint64_t table_id = OB_INVALID_ID;
      uint64_t column_id = OB_INVALID_ID;
      const ObRowkeyInfo *rki = NULL;
      if (NULL == T::child_op_)
      {
        ret = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (ret = T::child_op_->open()))
      {
        if (!IS_SQL_ERR(ret))
        {
          TBSYS_LOG(WARN, "child operator open fail ret=%d", ret);
        }
      }
      else if (OB_SUCCESS != (ret = T::child_op_->get_row_desc(row_desc))
              || NULL == row_desc)
      {
        if (OB_ITER_END != ret)
        {
          TBSYS_LOG(WARN, "get_row_desc from child_op=%p type=%d fail ret=%d", T::child_op_, T::child_op_->get_type(), ret);
          ret = (OB_SUCCESS != ret) ? OB_ERROR : ret;
        }
        else
        {
          ret = OB_SUCCESS;
        }
      }
      else if (OB_SUCCESS != (ret = row_desc->get_tid_cid(0, table_id, column_id))
              || OB_INVALID_ID == table_id)
      {
        TBSYS_LOG(WARN, "get_tid_cid from row_desc fail, child_op=%p type=%d %s ret=%d",
                  T::child_op_, T::child_op_->get_type(), to_cstring(*row_desc), ret);
        ret = (OB_SUCCESS != ret) ? OB_ERROR : ret;
      }
      else if (NULL == (rki = get_rowkey_info(host_, table_id)))
      {
        TBSYS_LOG(WARN, "get_rowkey_info fail table_id=%lu", table_id);
        ret = OB_SCHEMA_ERROR;
      }
      else
      {
        UpsSchemaMgrGuard sm_guard;
        const CommonSchemaManager *sm = NULL;
        if (NULL == (sm = host_.get_schema_mgr().get_schema_mgr(sm_guard)))
        {
          TBSYS_LOG(WARN, "get_schema_mgr fail");
          ret = OB_SCHEMA_ERROR;
        }
        else
        {   //modify wenghaixing [secondary index] 20150811
            int64_t index_num = get_index_num();
            if(!ob_operator_has_trigger())
            {
              if(OB_SUCCESS == ret)
              {
                  ObCellIterAdaptor cia;
                  //modify wenghaixing [secondary index static_index_build.consistency]20150424
                  //cia.set_row_iter(T::child_op_, rki->get_size(), sm);
                  cia.set_row_iter(T::child_op_, rki->get_size(), sm, index_num);
                  //modify e
                  ret = host_.apply(session_, cia, T::get_dml_type());
                  //TBSYS_LOG(ERROR, "child_op_'s index num[%ld],index_num[%ld]",T::child_op_->get_index_num(),index_num);
                  session_.inc_dml_count(T::get_dml_type());
              }
            }
            else
            {
                ObRowStore store, store_ex;
                const common::ObRow *row = NULL;
                const ObRowDesc *desc = NULL;
                const ObRowStore::StoredRow *stored_row = NULL;
                if(sql::PHY_INDEX_TRIGGER_UPD == T::child_op_->get_type())
                {
                  sql::ObProject *tmp_op=NULL;
                  tmp_op = static_cast<sql::ObProject*>(T::child_op_->get_child(0));
                  tmp_op -> set_row_store(&store_ex, &store);
                }
                else if(sql::PHY_DELETE_INDEX== T::child_op_->get_type())
                {
                  sql::ObProject *tmp_op=NULL;
                  tmp_op = static_cast<sql::ObProject*>(T::child_op_->get_child(0));
                  tmp_op -> set_row_store(NULL, &store_ex);
                }
                if(OB_SUCCESS != (ret = T::child_op_ -> get_row_desc(desc)))
                {
                  TBSYS_LOG(WARN, "get row desc failed, ret = %d", ret);
                }
                //add lbzhong [Update rowkey] 20151221:b
                if(sql::PHY_INDEX_TRIGGER_UPD != T::child_op_ -> get_type())
                {
                //add:e
                  while(OB_SUCCESS == ret && OB_SUCCESS == (ret = T::child_op_ -> get_next_row(row)))
                  {
                    if(NULL != row && sql::PHY_INDEX_TRIGGER_UPD != T::child_op_ -> get_type())
                    {
                      if(OB_SUCCESS != (store.add_row(*row, stored_row)))
                      {
                        TBSYS_LOG(WARN, "add row to row store failed,ret = %d", ret);
                        break;
                      }
                    }
                  }
                  if(OB_ITER_END == ret)
                  {
                    ret = OB_SUCCESS;
                  }
                } //add lbzhong [Update rowkey] 20160510:b:e


                //modify data table first
                if(NULL != desc && OB_SUCCESS == ret)
                {
                  //store.reset_iterator();
                  //add lbzhong[Update rowkey] 20151221:b
                  if(sql::PHY_INDEX_TRIGGER_UPD == T::child_op_ -> get_type())
                  {
                    ObCellIterAdaptor cia;
                    cia.set_row_iter(T::child_op_, rki->get_size(), sm, index_num);
                    ret = host_.apply(session_, cia, T::get_dml_type());
                    session_.inc_dml_count(T::get_dml_type());
                  }
                  else
                  {
                  //add:e
                    ObIndexCellIterAdaptor icia;
                    icia.set_row_iter(&store, rki->get_size(), sm, *desc,index_num);
                    ret = host_.apply(session_,icia,T::get_dml_type());
                  } //add lbzhong [Update rowkey] 20160510:b:e // end if
                }
                //then modify index table
                if(OB_SUCCESS == ret)
                {
                  store.reset_iterator();
                  //add liumengzhan_delete_index
                  if(sql::PHY_DELETE_INDEX == T::child_op_->get_type())
                  {
                    ObIUpsTableMgr* host = &host_;
                    sql::ObDeleteIndex *delete_index_op = NULL;
                    delete_index_op = dynamic_cast<sql::ObDeleteIndex*>(T::child_op_);
                    if(OB_SUCCESS != (ret = delete_index_op->handle_trigger(sm, host, session_, T::get_dml_type(),&store_ex)))
                    {
                      TBSYS_LOG(ERROR, "delete index table fail,ret = %d",ret);
                    }
                  }
                  //add:e
                  //modify by fanqiushi_index
                  if(T::child_op_->get_type()==sql::PHY_INDEX_TRIGGER)
                  {
                    ObIUpsTableMgr* host=&host_;
                    sql::ObIndexTrigger *tmp_index_tri=NULL;
                    tmp_index_tri=static_cast<sql::ObIndexTrigger*>(T::child_op_);
                    if(OB_SUCCESS!=(ret=tmp_index_tri->handle_trigger(row_desc,sm,rki->get_size(),host,session_, T::get_dml_type(),&store)))
                    {
                      TBSYS_LOG(ERROR, "modify index table fail,ret[%d]",ret);
                    }
                  }
                  //add wenghaixing [secondary index upd.4] 20141129
                  if(T::child_op_->get_type()==sql::PHY_INDEX_TRIGGER_UPD)
                  {
                    ObIUpsTableMgr* host=&host_;
                    sql::ObIndexTriggerUpd *tmp_index_tri=NULL;
                    tmp_index_tri=static_cast<sql::ObIndexTriggerUpd*>(T::child_op_);
                    if(OB_SUCCESS!=(ret=tmp_index_tri->handle_trigger(sm,host,session_,&store_ex)))
                    {
                      TBSYS_LOG(ERROR, "modify index table fail,ret[%d]",ret);
                    }
                  }
                  //add e
                  // add by liyongfeng:20150105 [secondary index replace]
                  if (sql::PHY_INDEX_TRIGGER_REP == T::child_op_->get_type())
                  {
                    ObIUpsTableMgr* host = &host_;
                    sql::ObIndexTriggerRep *index_replace_op = dynamic_cast<sql::ObIndexTriggerRep*>(T::child_op_);
                    if (OB_SUCCESS != (ret = index_replace_op->handle_trigger(sm, host, session_,&store)))
                    {
                      TBSYS_LOG(ERROR, "fail to replace table index, err=%d", ret);
                    }
                  }
                }
                //modify e
            }
        }
      }
      if (OB_SUCCESS != ret)
      {
        if (NULL != T::child_op_)
        {
          T::child_op_->close();
        }
      }
      return ret;
    }

    template <class T>
    int MemTableModifyTmpl<T>::close()
    {
      int ret = OB_SUCCESS;
      if (NULL == T::child_op_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = T::child_op_->close()))
        {
          TBSYS_LOG(WARN, "child operator close fail ret=%d", tmp_ret);
        }
      }
      return ret;
    }

    template <class T>
    int MemTableModifyTmpl<T>::get_affected_rows(int64_t& row_count)
    {
      int ret = OB_SUCCESS;
      row_count = session_.get_ups_result().get_affected_rows();
      return ret;
    }

    template <class T>
    int MemTableModifyTmpl<T>::get_next_row(const common::ObRow *&row)
    {
      UNUSED(row);
      return OB_ITER_END;
    }

    template <class T>
    int MemTableModifyTmpl<T>::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      UNUSED(row_desc);
      return OB_ITER_END;
    }

    template <class T>
    int64_t MemTableModifyTmpl<T>::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "MemTableModify(op_type=%d dml_type=%s session=%p[%d:%ld])\n",
                      T::get_type(),
                      str_dml_type(T::get_dml_type()),
                      &session_,
                      session_.get_session_descriptor(),
                      session_.get_session_start_time());
      if (NULL != T::child_op_)
      {
        pos += T::child_op_->to_string(buf+pos, buf_len-pos);
      }
      return pos;
    }

    //add wenghaixing [secondary index check_index_num]20150807
    template <class T>
    int64_t MemTableModifyTmpl<T>::get_index_num()
    {
      int64_t index_num = 0;
      if(sql::PHY_DELETE_INDEX == T::child_op_->get_type())
      {
        sql::ObDeleteIndex *delete_index_op = NULL;
        delete_index_op = dynamic_cast<sql::ObDeleteIndex*>(T::child_op_);
        index_num = delete_index_op->get_index_num();
      }
      else if(T::child_op_->get_type()==sql::PHY_INDEX_TRIGGER)
      {
        sql::ObIndexTrigger *tmp_index_tri=NULL;
        tmp_index_tri = dynamic_cast<sql::ObIndexTrigger*>(T::child_op_);
        index_num = tmp_index_tri->get_index_num();
      }
      else if(T::child_op_->get_type()==sql::PHY_INDEX_TRIGGER_UPD)
      {
        sql::ObIndexTriggerUpd *tmp_index_tri=NULL;
        tmp_index_tri=static_cast<sql::ObIndexTriggerUpd*>(T::child_op_);
        index_num = tmp_index_tri->get_index_num();
      }
      else if (sql::PHY_INDEX_TRIGGER_REP == T::child_op_->get_type())
      {
        sql::ObIndexTriggerRep *index_replace_op = dynamic_cast<sql::ObIndexTriggerRep*>(T::child_op_);
        index_num = index_replace_op->get_index_num();
      }
      else if(sql::PHY_PROJECT == T::child_op_->get_type())
      {
        sql::ObProject *update_op = dynamic_cast<sql::ObProject*>(T::child_op_);
        index_num = update_op->get_index_num();
      }
      return index_num;
    }


    template <class T>
    bool MemTableModifyTmpl<T>::ob_operator_has_trigger(){return T::child_op_->get_type() >= sql::PHY_INDEX_TRIGGER;}
    //add e

    typedef MemTableModifyTmpl<sql::ObUpsModify> MemTableModify;
    typedef MemTableModifyTmpl<sql::ObUpsModifyWithDmlType> MemTableModifyWithDmlType;
  } // end namespace updateserver
} // end namespace oceanbase

#endif /* OCEANBASE_UPDATESERVER_MEMTABLE_MODIFY_H_ */

