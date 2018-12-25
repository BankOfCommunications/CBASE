/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_ps_store_item.h is for what ...
 *
 * Version: ***: ob_ps_store_item.h  Mon Jul  1 17:44:36 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_PS_STORE_ITEM_H_
#define OB_PS_STORE_ITEM_H_

#include "common/dlist.h"
#include "common/ob_mod_define.h"
#include "common/ob_allocator.h"
#include "common/ob_global_factory.h"
#include "ob_basic_stmt.h"
#include "ob_result_set.h"

namespace oceanbase
{
  namespace sql
  {
    enum PsStoreItemStatus
    {
      PS_ITEM_INVALID,                  /* plan not build */
      PS_ITEM_EXPIRED,                  /* plan expired */
      PS_ITEM_VALID                     /* plan is ok */
    };

    class ObPsStoreItemValue
    {
      static const int64_t COMMON_PARAM_NUM = 12;
    public:
      ObPsStoreItemValue():sql_id_(0), schema_version_(0), 
        stmt_type_(ObBasicStmt::T_NONE), inner_stmt_type_(ObBasicStmt::T_NONE),
        compound_(false), has_cur_time_(false){}
      ~ObPsStoreItemValue(){}
      uint64_t sql_id_;          /* key of this value also is stmt_id return to client*/
      int64_t schema_version_;  /* schema version when prepared */
      //int64_t affected_rows_;
      common::ObString sql_;            /* ps sql used to rebuild plan when schema expired*/
      ObBasicStmt::StmtType stmt_type_;
      ObBasicStmt::StmtType inner_stmt_type_;
      bool compound_;
      ObPhysicalPlan plan_;
      common::ObSEArray<ObResultSet::Field, common::OB_PREALLOCATED_NUM> field_columns_;
      common::ObSEArray<ObResultSet::Field, COMMON_PARAM_NUM> param_columns_;
	  //add peiouya [NotNULL_check] [JHOBv0.1] 20140306:b
	  common::ObSEArray<bool, COMMON_PARAM_NUM> params_constraint_;
	  //add 20140306:e
      common::ObStringBuf  str_buf_;
      bool has_cur_time_;
      // common::ObArray<common::ObObj*> params_;
    };// ObPsStoreItemValue;

    class ObPsStoreItem:public common::DLink
    {
      public:
        ObPsStoreItem();
        ~ObPsStoreItem();
        int64_t inc_ps_count();
        int64_t dec_ps_count();

        int rlock()
        {
          return pthread_rwlock_rdlock(&rwlock_);
        }

        int wlock()
        {
          return pthread_rwlock_wrlock(&rwlock_);
        }

        int try_rlock()
        {
          return pthread_rwlock_tryrdlock(&rwlock_);
        }

        int try_wlock()
        {
          return pthread_rwlock_trywrlock(&rwlock_);
        }

        int unlock()
        {
          return pthread_rwlock_unlock(&rwlock_);
        }

        void reset()
        {
          // do nothing to rwlock_, as no lock on it
          status_ = PS_ITEM_INVALID;
          ps_count_ = 0;
          clear();
        }
        ObPsStoreItemValue*  get_item_value()
        {
          return &value_;
        }

        PsStoreItemStatus get_status() const
        {
          return status_;
        }

        void set_status(PsStoreItemStatus status)
        {
          status_ = status;
        }

        bool is_valid() const
        {
          return (PS_ITEM_VALID == status_);
        }

        int64_t get_ps_count() const
        {
          return ps_count_;
        }

        void set_sql_id(uint64_t id)
        {
          value_.sql_id_ = id;
        }

        uint64_t get_sql_id() const
        {
          return value_.sql_id_;
        }

        int32_t get_type() const
        {
          return 1;
        }

        int64_t get_schema_version() const
        {
          return value_.schema_version_;
        }
        void set_schema_version(int64_t version)
        {
          value_.schema_version_ = version;
        }
        //void set_value(ObPsStoreItemValue* value)
        //{
        //  value_ = *value;
        //}
        ObPhysicalPlan & get_physical_plan();
        void store_ps_sql(const common::ObString &stmt);
        common::ObString& get_ps_sql()
        {
          return value_.sql_;
        }
        common::ObStringBuf& get_string_buf()
        {
          return value_.str_buf_;
        }
        
        void clear()
        {
          value_.plan_.clear();
          value_.field_columns_.clear();
          value_.param_columns_.clear();
		  //add peiouya [NotNULL_check] [JHOBv0.1] 20140306:b
		  value_.params_constraint_.clear();
		  //add 20140306:e
        }

        static ObPsStoreItem* alloc();
        static void free(ObPsStoreItem *item);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObPsStoreItem);

      private:
        pthread_rwlock_t rwlock_;
        PsStoreItemStatus status_;
        volatile int64_t ps_count_; /* ref of prepare */
        ObPsStoreItemValue value_; /* mem manager by resource pool */
    };

    typedef common::ObGlobalFactory<ObPsStoreItem, 2, ObModIds::OB_SQL_PS_STORE_ITEM> ObPsStoreItemGFactory;
    typedef common::ObTCFactory<ObPsStoreItem, 2, ObModIds::OB_SQL_PS_STORE_ITEM> ObPsStoreItemTCFactory;
    inline ObPsStoreItem *ObPsStoreItem::alloc()
    {
      return ObPsStoreItemTCFactory::get_instance()->get(1);
    }

    inline void ObPsStoreItem::free(ObPsStoreItem *item)
    {
      ObPsStoreItemTCFactory::get_instance()->put(item);
    }
  }
}
#endif
