/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_alter_sys_cnf.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */

#ifndef OCEANBASE_SQL_OB_ALTER_SYS_CNF_H_
#define OCEANBASE_SQL_OB_ALTER_SYS_CNF_H_
#include "ob_no_children_phy_operator.h"
#include "ob_alter_sys_cnf_stmt.h"
#include "ob_sql_context.h"
#include "common/ob_array.h"

namespace oceanbase
{
  namespace sql
  {
    class ObAlterSysCnf : public ObNoChildrenPhyOperator
    {
    public:
      ObAlterSysCnf();
      virtual ~ObAlterSysCnf();
      virtual void reset();
      virtual void reuse();
      int add_sys_cnf_item(const ObSysCnfItem& cnf_item);
      void set_sql_context(ObSqlContext& context);

      /// execute the alter system config statement
      virtual int open();
      virtual int close();
      virtual ObPhyOperatorType get_type() const { return PHY_ALTER_SYS_CNF; }
      virtual int64_t to_string(char* buf, const int64_t buf_len) const;

      /// @note always return OB_ITER_END
      virtual int get_next_row(const common::ObRow *&row);
      /// @note always return OB_NOT_SUPPORTED
      virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
    private:
      // disallow copy
      ObAlterSysCnf(const ObAlterSysCnf &other);
      ObAlterSysCnf& operator=(const ObAlterSysCnf &other);

      int execute_transaction_stmt(const int& trans_type);
        
    private:
      ObSqlContext local_context_;
      common::ObArray<ObSysCnfItem> sys_cnf_items_;
    };
    inline int ObAlterSysCnf::get_next_row(const common::ObRow *&row)
    {
      row = NULL;
      return common::OB_ITER_END;
    }
    inline int ObAlterSysCnf::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      row_desc = NULL;
      return common::OB_NOT_SUPPORTED;
    }
    inline int ObAlterSysCnf::add_sys_cnf_item(const ObSysCnfItem& cnf_item)
    {
      return sys_cnf_items_.push_back(cnf_item);
    }
  }
}

#endif //OCEANBASE_SQL_OB_ALTER_SYS_CNF_H_



