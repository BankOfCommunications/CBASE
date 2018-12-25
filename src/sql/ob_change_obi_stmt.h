/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_change_obi_stmt.h is for what ...
 *
 * Version: ***: ob_change_obi_stmt.h  Tue Aug 6 15:34:14 2013 lide.wd Exp $
 *
 * Authors:
 *   Author lide.wd
 *   Email: lide.wd@alipay.com
 *     -some work detail if you want
 *
 */

#ifndef OB_CHANGE_OBI_STMT_H_
#define OB_CHANGE_OBI_STMT_H_

#include "sql/ob_basic_stmt.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "common/ob_obi_role.h"
namespace oceanbase
{
  namespace sql
  {
    class ObChangeObiStmt : public ObBasicStmt
    {
      public:
        explicit ObChangeObiStmt(common::ObStringBuf *name_pool);
        virtual ~ObChangeObiStmt();
        int set_target_server_addr(const char *target_server_addr);
        void get_target_server_addr(common::ObString &target_server_addr);
        void set_target_role(common::ObiRole::Role role);
        common::ObiRole get_target_role();
        void set_force(bool force);
        bool get_force();
        void set_change_flow(bool change_flow);
        bool get_change_flow();
        virtual void print(FILE*, int32_t level, int32_t index=0);
      private:
        ObChangeObiStmt(const ObChangeObiStmt &other);
        ObChangeObiStmt& operator=(const ObChangeObiStmt &other);
      private:
        common::ObStringBuf* name_pool_;
        common::ObString target_server_addr_;
        bool force_;
        bool change_flow_;
        common::ObiRole role_;

    };
  }
}

#endif
