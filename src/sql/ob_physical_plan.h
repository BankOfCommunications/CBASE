/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_physical_plan.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef _OB_PHYSICAL_PLAN_H
#define _OB_PHYSICAL_PLAN_H
#include "ob_phy_operator.h"
#include "ob_phy_operator_factory.h"
#include "common/ob_vector.h"
#include "common/page_arena.h"
#include "common/ob_transaction.h"
#include "common/ob_se_array.h"
#include "common/dlist.h"

namespace oceanbase
{
  namespace sql
  {
    class ObTableRpcScan;
    class ObResultSet;
    class ObPhysicalPlan: public common::DLink
    {
      public:
        struct ObTableVersion
        {
          ObTableVersion()
          {
            table_id_ = common::OB_INVALID_ID;
            version_ = 0;
          }
          int64_t table_id_;
          int64_t version_;
        };
      public:
        ObPhysicalPlan();
        virtual ~ObPhysicalPlan();

        int add_phy_query(ObPhyOperator *phy_query, int32_t* idx = NULL, bool main_query = false);
        int set_pre_phy_query(ObPhyOperator *phy_query, int32_t* idx = NULL);
        int store_phy_operator(ObPhyOperator *op);
        int32_t get_query_size() const { return static_cast<int32_t>(phy_querys_.count()); }
        ObPhyOperator* get_phy_query(int32_t index) const;
        ObPhyOperator* get_phy_query_by_id(uint64_t id) const;
        ObPhyOperator* get_main_query() const;
        ObPhyOperator* get_pre_query() const;
        void set_main_query(ObPhyOperator *query);
        int remove_phy_query(ObPhyOperator *phy_query);
        int remove_phy_query(int32_t index);

        void clear();
        int64_t to_string(char* buf, const int64_t buf_len) const;

        common::ModuleArena* get_allocator();
        int set_allocator(common::ModuleArena *allocator);
        int set_operator_factory(ObPhyOperatorFactory* factory);
        int deserialize_header(const char* buf, const int64_t data_len, int64_t& pos);

        int64_t get_curr_frozen_version() const;
        void set_curr_frozen_version(int64_t fv);
        void set_in_ups_executor(bool flag);
        bool in_ups_executor() const;
        bool is_terminate(int &ret) const;
        bool is_cons_from_assign() const { return cons_from_assign_; }
        const common::ObTransID& get_trans_id() const;

        /**
         * set the timestamp when the execution of this plan should time out
         *
         * @param ts_timeout_us [in] the microseconds timeout
         */
        void set_timeout_timestamp(const int64_t ts_timeout_us);
        int64_t get_timeout_timestamp() const;
        /**
         * Whether it has been time-out.
         * If we have timed-out, the operators' open() or get_next_row() should
         * return OB_PROCESS_TIMEOUT and abort processing
         *
         * @param remain_us [out] if not time-out, return the remaining microseconds
         * @return true or false
         */
        bool is_timeout(int64_t *remain_us = NULL) const;

        void set_result_set(ObResultSet *rs);
        ObResultSet* get_result_set();

        void set_start_trans(bool did_start) {start_trans_ = did_start;}
        bool get_start_trans() const {return start_trans_;};
        common::ObTransReq& get_trans_req() {return start_trans_req_;}

        int add_base_table_version(int64_t table_id, int64_t version);
        int add_base_table_version(const ObTableVersion table_version);
        int get_base_table_version(int64_t table_id, int64_t& version);
        const ObTableVersion& get_base_table_version(int64_t index) const;
        int64_t get_base_table_version_count();
        // whether this plan is to update user tables only
        bool is_user_table_operation() const;

        int64_t get_operator_size() const { return operators_store_.count(); }
        ObPhyOperator* get_phy_operator(int64_t index) const;
        int assign(const ObPhysicalPlan& other);

        NEED_SERIALIZE_AND_DESERIALIZE;

        int32_t get_type(){return 0;};
        static ObPhysicalPlan *alloc();
        static void free(ObPhysicalPlan *plan);
        //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140713 :b
        void need_extend_time(){need_extend_time_ = true;};
        bool is_need_extend_time(){return need_extend_time_;};
        //add 20140713:e
        //add tianz [EXPORT_TOOL] 20141120:b
        inline void add_start_range_values(int idx,oceanbase::common::ObObj& value)
        {
          range_start_objs_[idx] = value;
        }
        inline void add_end_range_values(int idx,oceanbase::common::ObObj& value)
        {
          range_end_objs_[idx] = value;
        }
        inline common::ObObj* get_start_range(){ return range_start_objs_;}
        inline common::ObObj* get_end_range(){ return range_end_objs_;}
        inline void set_has_range(){has_range_ = true;}
        inline bool get_has_range()const {return has_range_;}
        inline void set_start_is_min(){start_is_min_ = true;}
        inline bool start_is_min()const {return start_is_min_;}
        inline void set_end_is_max(){end_is_max_ = true;}
        inline bool end_is_max()const {return end_is_max_;}
        //add 20141120:e
        //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715:b
        void set_insert_select_flag(bool is_insert_select_flag){is_insert_select_flag_ = is_insert_select_flag;}//add by gaojt
        int get_insert_select_flag(){return is_insert_select_flag_;}//add by gaojt
        //add gaojt 20140715:e

      private:
        static const int64_t COMMON_OP_NUM = 16;
        static const int64_t COMMON_SUB_QUERY_NUM = 6;
        static const int64_t COMMON_BASE_TABLE_NUM = 64;
        typedef oceanbase::common::ObSEArray<ObPhyOperator*, COMMON_OP_NUM> OperatorStore;
        typedef oceanbase::common::ObSEArray<ObPhyOperator*, COMMON_SUB_QUERY_NUM> SubQueries;
        typedef oceanbase::common::ObSEArray<ObTableVersion, COMMON_BASE_TABLE_NUM> BaseTableStore;

      private:
        DISALLOW_COPY_AND_ASSIGN(ObPhysicalPlan);
        int deserialize_tree(const char *buf, int64_t data_len, int64_t &pos, common::ModuleArena &allocator, OperatorStore &operators_store, ObPhyOperator *&root);
        int serialize_tree(char *buf, int64_t buf_len, int64_t &pos, const ObPhyOperator &root) const;
        int create_and_assign_tree(const ObPhyOperator *other, bool main_query, bool is_qeury, ObPhyOperator *&out_op);
      private:
        common::ObTransID trans_id_;
        int64_t curr_frozen_version_; // do not serialize
        int64_t ts_timeout_us_;       // execution timeout for this plan
        ObPhyOperator   *main_query_;
        uint64_t pre_query_id_; // the sub query must be called before main_query_
        SubQueries phy_querys_;
        OperatorStore operators_store_; //存储物理计划中所有的物理操作符
        BaseTableStore table_store_;
        common::ModuleArena *allocator_;
        ObPhyOperatorFactory* op_factory_;
        ObResultSet *my_result_set_; // The result set who owns this physical plan
        bool start_trans_;
        bool in_ups_executor_;
        bool cons_from_assign_;
        common::ObTransReq start_trans_req_;
        uint64_t next_phy_operator_id_;
        bool need_extend_time_;//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140713
        //add tianz [EXPORT_TOOL] 20141120:b
        bool has_range_ ;
        bool start_is_min_;
        bool end_is_max_;
        common::ObObj range_start_objs_[OB_MAX_ROWKEY_COLUMN_NUMBER];
        common::ObObj range_end_objs_[OB_MAX_ROWKEY_COLUMN_NUMBER];
        //add 20141120:e
        bool is_insert_select_flag_;//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715
    };

    inline int ObPhysicalPlan::set_operator_factory(ObPhyOperatorFactory* factory)
    {
      op_factory_ = factory;
      return common::OB_SUCCESS;
    }

    inline int ObPhysicalPlan::set_allocator(common::ModuleArena *allocator)
    {
      int ret = common::OB_SUCCESS;
      if (NULL == allocator)
      {
        ret = common::OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "allocator is null");
      }
      else
      {
        allocator_ = allocator;
      }
      return ret;
    }

    inline common::ModuleArena* ObPhysicalPlan::get_allocator()
    {
      return allocator_;
    }

    inline void ObPhysicalPlan::clear()
    {
      TBSYS_LOG(DEBUG, "clear physical plan, addr=%p", this);
      main_query_ = NULL;
      pre_query_id_ = common::OB_INVALID_ID;
      phy_querys_.clear();
      if (cons_from_assign_)
      {
        for(int32_t i = 0; i < operators_store_.count(); i++)
        {
          TBSYS_LOG(DEBUG, "free %d operator %p type=%s", i, operators_store_.at(i), ob_phy_operator_type_str(operators_store_.at(i)->get_type()));
          ObPhyOperator::free(operators_store_.at(i));
          operators_store_.at(i) = NULL;
        }
      }
      else
      {
        TBSYS_LOG(DEBUG, "not cons from assign clear physical plan, addr=%p", this);
        for(int32_t i = 0; i < operators_store_.count(); i++)
        {
            //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160629:b
            if (NULL == operators_store_.at(i))
            {
                TBSYS_LOG(INFO,"the %dth operator in physical plan is NULL",i);
            }
            else
            {
            //add gaojt 20160629:e
                ob_dec_phy_operator_stat(operators_store_.at(i)->get_type());
                // we can not delete, because it will release space which is not allocated
                // delete operators_store_.at(i);
                if (NULL != op_factory_)
                {
                    op_factory_->release_one(operators_store_.at(i));
                }
                else
                {
                    operators_store_.at(i)->~ObPhyOperator();
                }
                operators_store_.at(i) = NULL;
            }//add gaojt [Delete_Update_Function] [JHOBv0.1] 20160629:b
        }
      }
      operators_store_.clear();
      table_store_.clear();
      in_ups_executor_ = false;
      cons_from_assign_ = false;
      need_extend_time_ = false;//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140713
      //add tianz [EXPORT_TOOL] 20141120:b
	  if (has_range_)
      {
        for (int i = 0; i < OB_MAX_ROWKEY_COLUMN_NUMBER; i++)
        {
          range_start_objs_[i].set_min_value();
          range_end_objs_[i].set_max_value();
        }
        start_is_min_ = false;
        end_is_max_ = false;
      }
      has_range_ = false;
	  //add 20141120:e
    }

    inline const common::ObTransID& ObPhysicalPlan::get_trans_id() const
    {
      return trans_id_;
    }

    inline int64_t ObPhysicalPlan::get_curr_frozen_version() const
    {
      return this->curr_frozen_version_;
    }

    inline void ObPhysicalPlan::set_curr_frozen_version(int64_t fv)
    {
      curr_frozen_version_ = fv;
    }

    inline void ObPhysicalPlan::set_timeout_timestamp(const int64_t ts_timeout_us)
    {
      ts_timeout_us_ = ts_timeout_us;
    }

    inline int64_t ObPhysicalPlan::get_timeout_timestamp() const
    {
      return this->ts_timeout_us_;
    }

    inline bool ObPhysicalPlan::is_timeout(int64_t *remain_us /*= NULL*/) const
    {
      int64_t now = tbsys::CTimeUtil::getTime();
      if (NULL != remain_us)
      {
        if (OB_LIKELY(ts_timeout_us_ > 0))
        {
          *remain_us = ts_timeout_us_ - now;
        }
        else
        {
          *remain_us = INT64_MAX; // no timeout
        }
      }
      return (ts_timeout_us_ > 0 && now > ts_timeout_us_);
    }

    inline void ObPhysicalPlan::set_result_set(ObResultSet *rs)
    {
      my_result_set_ = rs;
    }

    inline ObResultSet* ObPhysicalPlan::get_result_set()
    {
      return my_result_set_;
    }

    inline void ObPhysicalPlan::set_in_ups_executor(bool flag)
    {
      in_ups_executor_ = flag;
    }

    inline bool ObPhysicalPlan::in_ups_executor() const
    {
      return in_ups_executor_;
    }

    typedef common::ObGlobalFactory<ObPhysicalPlan, 1, ObModIds::OB_SQL_PS_STORE_PHYSICALPLAN> ObPhyPlanGFactory;
    typedef common::ObTCFactory<ObPhysicalPlan, 1, ObModIds::OB_SQL_PS_STORE_PHYSICALPLAN> ObPhyPlanTCFactory;
    //extern uint64_t phycount;
    inline ObPhysicalPlan *ObPhysicalPlan::alloc()
    {
      //atomic_inc(&phycount);
      return  ObPhyPlanTCFactory::get_instance()->get(0);
    }

    inline void ObPhysicalPlan::free(ObPhysicalPlan *plan)
    {
      //atomic_dec(&phycount);
      ObPhyPlanTCFactory::get_instance()->put(plan);
    }

  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_PHYSICAL_PLAN_H */
