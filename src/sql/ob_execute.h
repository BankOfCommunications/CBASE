/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_execute.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_EXECUTE_H_
#define OCEANBASE_SQL_OB_EXECUTE_H_
#include "sql/ob_single_child_phy_operator.h"
#include "ob_sql_session_info.h"

namespace oceanbase
{
  namespace sql
  {
    class ObExecute: public ObSingleChildPhyOperator
    {
      public:
        ObExecute();
        virtual ~ObExecute();
        virtual void reset();
        virtual void reuse();
        void set_stmt_id(const uint64_t stmt_id);
        int add_param_name(const common::ObString& name);

        /// execute the prepare statement
        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const { return PHY_EXECUTE; }
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int get_next_row(const common::ObRow *&row);
      private:
        // disallow copy
        ObExecute(const ObExecute &other);
        ObExecute& operator=(const ObExecute &other);
        // function members
        int fill_execute_items();
        //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160508:b
        int get_fillvalues_operator(ObPhyOperator *main_query, ObPhyOperator *&fill_values);
        //add gaojt 20160508:e
      private:
        // data members
        uint64_t stmt_id_;
        common::ObArray<common::ObString> param_names_;
    };

    inline void ObExecute::set_stmt_id(const uint64_t stmt_id)
    {
      stmt_id_ = stmt_id;
    }
    inline int ObExecute::add_param_name(const common::ObString& name)
    {
      return param_names_.push_back(name);
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_EXECUTE_H_ */
