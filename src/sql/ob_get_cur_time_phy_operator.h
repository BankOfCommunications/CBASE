/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_get_cur_time_phy_operator.h
 *
 * Authors:
 *   yongle.xh <yongle.xh@alipay.com>
 *
 */
#ifndef _OB_GET_CUR_TIME_PHY_OPERATOR_H
#define _OB_GET_CUR_TIME_PHY_OPERATOR_H 1
#include "mergeserver/ob_ms_rpc_proxy.h"
#include "sql/ob_logical_plan.h"

namespace oceanbase
{
  namespace sql
  {
    /**
     * this class is use in ms to get cur time from ups
     */
    class ObGetCurTimePhyOperator: public ObNoChildrenPhyOperator
    {
      public:
        ObGetCurTimePhyOperator(): rpc_(NULL), ups_plan_(NULL){}
        virtual ~ObGetCurTimePhyOperator();

        inline void set_rpc_stub(mergeserver::ObMergerRpcProxy* rpc) {rpc_ = rpc;}
        inline void set_ups_plan(sql::ObPhysicalPlan *ups_plan) {ups_plan_ = ups_plan;}
        //mod liuzy [datetime func] 20150909:b
        /*Exp: 将cur_time_fun_type_修改为ObArray<ObCurTimeType>类型，与逻辑计划对应*/
//        inline void set_cur_time_fun_type(const ObCurTimeType& type) {cur_time_fun_type_ = type;}
        inline void set_cur_time_fun_type(const ObCurTimeType& type) {cur_time_fun_type_.push_back(type);}
        //mod 20150909:e
        virtual int open();
        inline virtual int close() { return OB_SUCCESS; }
        virtual void reset();
        virtual void reuse();
        virtual int get_next_row(const common::ObRow *&) { return common::OB_NOT_SUPPORTED; }
        virtual int get_row_desc(const common::ObRowDesc *&) const { return common::OB_NOT_SUPPORTED; }
        virtual int64_t to_string(char *buf, const int64_t buf_len) const;
        virtual ObPhyOperatorType get_type() const { return PHY_CUR_TIME; }
        DECLARE_PHY_OPERATOR_ASSIGN;
      private:
        // types and constants
      private:
        // disallow copy
        ObGetCurTimePhyOperator(const ObGetCurTimePhyOperator& other);
        ObGetCurTimePhyOperator& operator=(const ObGetCurTimePhyOperator& other);
        // function members
        //mod liuzy [datetime func] 20150902:b
        //int get_cur_time();
        int get_cur_time(ObCurTimeType type = CUR_TIME);
        //mod 20150902:e
        int get_cur_time_from_ups();
      private:
        // data members
        mergeserver::ObMergerRpcProxy *rpc_;
        sql::ObPhysicalPlan *ups_plan_;
        //mod liuzy [datetime func] 20150909:b
        /*Exp: 将cur_time_fun_type_修改为ObArray<ObCurTimeType>类型，与逻辑计划对应*/
//        ObCurTimeType cur_time_fun_type_;
        ObArray<ObCurTimeType> cur_time_fun_type_;
        //mod 20150909:e
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_GET_CUR_TIME_PHY_OPERATOR_H */
