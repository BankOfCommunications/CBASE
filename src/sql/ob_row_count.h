/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_count.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_ROW_COUNT_H_
#define OCEANBASE_SQL_OB_ROW_COUNT_H_
#include "ob_single_child_phy_operator.h"

namespace oceanbase
{
  namespace sql
  {
    class ObRowCount: public ObSingleChildPhyOperator
    {
      public:
        ObRowCount();
        virtual ~ObRowCount();
        virtual void reset();
        virtual void reuse();

        inline void set_tid_cid(const uint64_t tid, const uint64_t cid);
        inline void set_when_func_index(const int32_t index);
        inline const uint64_t get_column_id() const;
        inline const uint64_t get_table_id() const;
        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        virtual ObPhyOperatorType get_type() const;

        DECLARE_PHY_OPERATOR_ASSIGN;
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        int cons_row_desc();
        // disallow copy
        ObRowCount(const ObRowCount &other);
        ObRowCount& operator=(const ObRowCount &other);
      protected:
        // data members
        uint64_t            table_id_;
        uint64_t            column_id_;
        int32_t             when_func_index_;
        common::ObRowDesc   row_desc_;
        common::ObRow       row_;
        bool                already_read_;
    };

    inline void ObRowCount::set_tid_cid(const uint64_t tid, const uint64_t cid)
    {
      table_id_ = tid;
      column_id_ = cid;
    }

    inline void ObRowCount::set_when_func_index(const int32_t index)
    {
      when_func_index_ = index;
    }

    inline const uint64_t ObRowCount::get_column_id() const
    {
      return column_id_;
    }

    inline const uint64_t ObRowCount::get_table_id() const
    {
      return table_id_;
    }

    inline ObPhyOperatorType ObRowCount::get_type() const
    {
      return PHY_ROW_COUNT;
    }

  } // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_ROW_COUNT_H_ */
