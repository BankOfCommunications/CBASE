/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_deallocate.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_DEALLOCATE_H_
#define OCEANBASE_SQL_OB_DEALLOCATE_H_
#include "sql/ob_no_children_phy_operator.h"
#include "ob_sql_session_info.h"

namespace oceanbase
{
  namespace sql
  {
    class ObDeallocate: public ObNoChildrenPhyOperator
    {
      public:
        ObDeallocate();
        virtual ~ObDeallocate();
        virtual void reset();
        virtual void reuse();
        void set_stmt_id(const uint64_t stmt_id);
        /// execute the prepare statement
        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const { return PHY_DEALLOCATE; }
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        /// @note always return OB_ITER_END
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        /// @note always return OB_ITER_END
        virtual int get_next_row(const common::ObRow *&row);
        //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160524:b
        void set_stmt_name(const common::ObString& stmt_name){stmt_name_ = stmt_name;};
        //add gaojt 20160524:e
      private:
        // disallow copy
        ObDeallocate(const ObDeallocate &other);
        ObDeallocate& operator=(const ObDeallocate &other);
        // function members
        int delete_plan_from_session();
      private:
        // data members
        uint64_t stmt_id_;
        //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160524:b
        ObString stmt_name_;
        //add gaojt 20160524:e
    };

    inline void ObDeallocate::set_stmt_id(const uint64_t stmt_id)
    {
      stmt_id_ = stmt_id;
    }
    inline int ObDeallocate::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      row_desc = NULL;
      return common::OB_ITER_END;
    }
    inline int ObDeallocate::get_next_row(const common::ObRow *&row)
    {
      row = NULL;
      return common::OB_ITER_END;
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_DEALLOCATE_H_ */
