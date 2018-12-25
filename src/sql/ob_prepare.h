/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_prepare.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_PREPARE_H_
#define OCEANBASE_SQL_OB_PREPARE_H_
#include "sql/ob_single_child_phy_operator.h"
#include "ob_sql_session_info.h"

namespace oceanbase
{
  namespace sql
  {
    class ObPhysicalPlan;
    class ObPrepare: public ObSingleChildPhyOperator
    {
      public:
        ObPrepare();
        virtual ~ObPrepare();
        virtual void reset();
        virtual void reuse();
        void set_stmt_name(const common::ObString& stmt_name);
        const common::ObString& get_stmt_name() const;

        /// execute the prepare statement
        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const { return PHY_PREPARE; }
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        /// @note always return OB_ITER_END
        virtual int get_next_row(const common::ObRow *&row);

      private:
        // disallow copy
        ObPrepare(const ObPrepare &other);
        ObPrepare& operator=(const ObPrepare &other);
        // function members
        int store_phy_plan_to_session();
      private:
        // data members
        common::ObString stmt_name_;
    };

    inline void ObPrepare::reset()
    {
      ObSingleChildPhyOperator::reset();
    }

    inline void ObPrepare::reuse()
    {
      ObSingleChildPhyOperator::reuse();
    }

    inline void ObPrepare::set_stmt_name(const common::ObString& stmt_name)
    {
      stmt_name_ = stmt_name;
    }
    inline const common::ObString& ObPrepare::get_stmt_name() const
    {
      return stmt_name_;
    }
    inline int ObPrepare::get_next_row(const common::ObRow *&row)
    {
      row = NULL;
      return common::OB_ITER_END;
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_PREPARE_H_ */
