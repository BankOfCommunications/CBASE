/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_phy_operator.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_PHY_OPERATOR_H
#define _OB_PHY_OPERATOR_H 1
#include "common/ob_row.h"
#include "ob_phy_operator_type.h"
#include "common/ob_define.h"
#include "common/ob_allocator.h"
#include "common/ob_tc_factory.h"
#include "common/ob_mod_define.h"

#define DECLARE_PHY_OPERATOR_ASSIGN \
  virtual int assign(const ObPhyOperator* other)

#define PHY_OPERATOR_ASSIGN(TypeName) \
  int TypeName::assign(const ObPhyOperator* other)

#define CAST_TO_INHERITANCE(TypeName) \
  const TypeName *o_ptr = static_cast<const TypeName*>(other);

//typedef oceanbase::common::ObArray<oceanbase::sql::ObSqlExpression, oceanbase::common::ModulePageAllocator, oceanbase::common::ObArrayExpressionCallBack<oceanbase::sql::ObSqlExpression> > ObArraySqlEx;
namespace oceanbase
{
  namespace sql
  {

    class ObPhysicalPlan;
    /// 物理运算符接口
    class ObPhyOperator: public common::DLink
    {
      public:
        ObPhyOperator();
        virtual ~ObPhyOperator() {}

        /// 添加子运算符，有些运算符（例如join）可能有多个子运算符。叶运算符无子运算符。
        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator) = 0;

        /// 获取子预算符，child_idx对应的子操作符不存在会返回NULL
        virtual ObPhyOperator *get_child(int32_t child_idx) const
        {
          UNUSED(child_idx);
          return NULL;
        }

        virtual int32_t get_child_num() const
        {
          return 0;
        }

        virtual enum ObPhyOperatorType get_type() const
        {
          return PHY_INVALID;
        }

        //add wenghaixing [secondary index secondary index static_index_build.consistency]20150424
        virtual int64_t get_index_num()
        {
          return 0;
        }
        //add e

        /// 打开物理运算符。申请资源，打开子运算符等。构造row description；给子运算符传递配置等。
        virtual int open() = 0;

        /// 关闭物理运算符。释放资源，关闭子运算符等。
        /// @note If the operator is closed successfully, it could be opened and used again.
        virtual int close() = 0;

        /// 将对象重置到构造时的状态，包括变量重置、内存释放等等。
        virtual void reset() = 0;

        /// 将对象重置到构造时的状态，但是可能不释放某些重新申请很耗时的资源，如内存等。
        virtual void reuse() = 0;

        /**
         * 获得下一行的引用
         * @note 在下次调用get_next或者close前，返回的row有效
         * @pre 调用open()
         * @param row [out]
         *
         * @return OB_SUCCESS或OB_ITER_END或错误码
         */
        virtual int get_next_row(const common::ObRow *&row) = 0;

        /**
         * get the row description
         * the row desc should have been valid after open() and before close()
         * @pre call open() first
         */
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const = 0;

        /**
         * 打印本物理运算符及其所有子运算符
         * @note 先打印本运算符，再打印所有子运算符
         * @param buf [in] 打印到该缓冲区
         * @param buf_len [in] 缓冲区大小
         *
         * @return 打印字符数
         */
        virtual int64_t to_string(char* buf, const int64_t buf_len) const = 0;

        /**
         * Set the physical plan object who owns this operator.
         *
         * @param the_plan
         */
        void set_phy_plan(ObPhysicalPlan *the_plan);
        ObPhysicalPlan *get_phy_plan();

        /// phy operator id which is unique in the physical plan
        void set_id(uint64_t id);
        uint64_t get_id() const;

        DECLARE_PHY_OPERATOR_ASSIGN;
        VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;

        static ObPhyOperator* alloc(ObPhyOperatorType type);
        static void free(ObPhyOperator *op);
      //add peiouya [IN_TYPEBUG_FIX] 20151225:b
      public:
         virtual int get_output_columns_dsttype(common::ObArray<common::ObObjType> &output_columns_dsttype) ;
         virtual int add_dsttype_for_output_columns(common::ObArray<common::ObObjType>& columns_dsttype);
      //add 20151225:e
      private:
        DISALLOW_COPY_AND_ASSIGN(ObPhyOperator);
      protected:
        int64_t magic_;
        ObPhysicalPlan *my_phy_plan_; //< the physical plan object who owns this operator. Use this->my_phy_plan_->my_result_set_->my_session_ to get the environment.
        uint64_t id_;
    };

    inline ObPhyOperator::ObPhyOperator()
      :magic_(0xABCD1986ABCD1986),
       my_phy_plan_(NULL),
       id_(common::OB_INVALID_ID)
    {
    }
    inline void ObPhyOperator::set_phy_plan(ObPhysicalPlan *the_plan)
    {
      my_phy_plan_ = the_plan;
    }
    inline ObPhysicalPlan* ObPhyOperator::get_phy_plan()
    {
      return my_phy_plan_;
    }
    typedef common::ObGlobalFactory<ObPhyOperator, PHY_END, common::ObModIds::OB_SQL_PS_STORE_OPERATORS> ObPhyOperatorGFactory;
    typedef common::ObTCFactory<ObPhyOperator, PHY_END, common::ObModIds::OB_SQL_PS_STORE_OPERATORS> ObPhyOperatorTCFactory;

    inline void ObPhyOperator::set_id(uint64_t id)
    {
      id_ = id;
    }

    inline uint64_t ObPhyOperator::get_id() const
    {
      return id_;
    }

    inline ObPhyOperator* ObPhyOperator::alloc(ObPhyOperatorType type)
    {
      ObPhyOperator *ret =  ObPhyOperatorTCFactory::get_instance()->get(type);
      if (OB_LIKELY(NULL != ret))
      {
        OB_PHY_OP_INC(type);
      }
      return ret;
    }

    inline void ObPhyOperator::free(ObPhyOperator *op)
    {
      if (OB_LIKELY(NULL != op))
      {
        OB_PHY_OP_DEC(op->get_type());
      }
      ObPhyOperatorTCFactory::get_instance()->put(op);
    }
  } // end namespace sql
} // end namespace oceanbase


#define REGISTER_PHY_OPERATOR(OP, OP_TYPE) \
  REGISTER_CREATOR(oceanbase::sql::ObPhyOperatorGFactory, ObPhyOperator, OP, OP_TYPE)

#endif /* _OB_PHY_OPERATOR_H */
