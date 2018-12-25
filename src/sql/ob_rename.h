/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_rename.h
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#ifndef _OB_RENAME_H
#define _OB_RENAME_H 1
#include "ob_single_child_phy_operator.h"
#include "ob_sql_expression.h"
#include "common/ob_array.h"

namespace oceanbase
{
  namespace sql
  {
    class ObRename: public ObSingleChildPhyOperator
    {
      public:
        ObRename();
        virtual ~ObRename();
        virtual void reset();
        virtual void reuse();
        int set_table(const uint64_t table_id, const uint64_t base_table_id);
        //add zhaoqiong [TableMemScan_Subquery_BUGFIX] 20151118:b
        bool is_base_table_id_valid();
        //add:e
        int add_column_id(const uint64_t& column_id);
        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        virtual ObPhyOperatorType get_type() const;

        DECLARE_PHY_OPERATOR_ASSIGN;
        NEED_SERIALIZE_AND_DESERIALIZE;
      protected:
        virtual int cons_row_desc();
      private:
        // disallow copy
        ObRename(const ObRename &other);
        ObRename& operator=(const ObRename &other);
      protected:
        // data members
        // 表的id。对于需要新生成表id的情况，是新生成的表id
        // 生成输出列的RowDesc时使用
        uint64_t table_id_;
        // 表的id。对于需要新生成表id的情况，是原始的表id
        // 访问基本表时使用
        uint64_t base_table_id_;
        common::ObArray<uint64_t> column_ids_;
        ObRow *org_row_;
        const ObRowDesc *org_row_desc_;
        ObRowDesc row_desc_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_RENAME_H */
