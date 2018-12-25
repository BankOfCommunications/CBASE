/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_add_project.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ADD_PROJECT_H
#define _OB_ADD_PROJECT_H 1
#include "ob_project.h"

namespace oceanbase
{
  namespace sql
  {
    // keep the old columns of the input row, and append new columns to the output row
    class ObAddProject: public ObProject
    {
      public:
        ObAddProject();
        virtual ~ObAddProject();

        virtual int open();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        virtual ObPhyOperatorType get_type() const {return PHY_ADD_PROJECT;};

        DECLARE_PHY_OPERATOR_ASSIGN;
      private:
        // types and constants
      private:
        // disallow copy
        ObAddProject(const ObAddProject &other);
        ObAddProject& operator=(const ObAddProject &other);
        // function members
        int cons_row_desc(const common::ObRowDesc &input_row_desc);
        int shallow_copy_input_row(const common::ObRow &input_row);
      private:
        // data members
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_ADD_PROJECT_H */
