/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_explain.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_EXPLAIN_H_
#define OCEANBASE_SQL_OB_EXPLAIN_H_
#include "sql/ob_single_child_phy_operator.h"
#include "common/ob_mutator.h"
namespace oceanbase
{
  namespace sql
  {
    class ObExplain: public ObSingleChildPhyOperator
    {
      public:
        ObExplain();
        explicit ObExplain(bool verbose);
        virtual ~ObExplain();
        virtual void reset();
        virtual void reuse();
        // set row desc
        int set_row_desc(const common::ObRowDesc &row_desc);
        
        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const { return PHY_EXPLAIN; }
        virtual int get_next_row(const common::ObRow *&row);
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        /// @note always return OB_NOT_SUPPORTED
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
      private:
        // disallow copy
        ObExplain(const ObExplain &other);
        ObExplain& operator=(const ObExplain &other);
      private:
        // data members
        bool verbose_;
        bool been_read_;
        common::ObRowDesc row_desc_;
        common::ObRow row_;
        common::ObStringBuf str_buf_;
    };

    inline int ObExplain::set_row_desc(const common::ObRowDesc &row_desc)
    {
      row_desc_ = row_desc;
      return common::OB_SUCCESS;
    }

    inline int ObExplain::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      row_desc = NULL;
      return common::OB_NOT_SUPPORTED;
    }

  } // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_EXPLAIN_H_ */

