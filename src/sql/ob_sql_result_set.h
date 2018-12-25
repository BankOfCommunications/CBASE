/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Filename: ob_sql_result_set.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@taobao.com>
 *
 */

#ifndef _OB_SQL_RESULT_SET_H_
#define _OB_SQL_RESULT_SET_H_

#include "common/ob_new_scanner.h"
#include "sql/ob_result_set.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace sql
  {
    /*
     * @class ResultSet
     * @brief Result set of ObMsSQLProxy execution result
     *
     * Every sql execution will generate a result which is an
     * instance of this class.
     *
     * For server end:
     *     rs.open();
     *     do {
     *       rs.serialize(...);
     *       get fullfilled := rs.fullfilled()
     *     } while (!fullfilled);
     *     rs.close();
     *
     * For Client end:
     *     ObSQLResultSet rs;
     *     do {
     *       rs.deserialize(...);
     *       do something what u like
     *       if not fullfilled
     *       then
     *         get_next_packet(...)
     *       else
     *         break
     *       end if
     *     } while (true)
     */
    class ObSQLResultSet
    {
        typedef ObArray<sql::ObResultSet::Field> FieldArray;
      public:
        ObSQLResultSet() : errno_(OB_SUCCESS), extra_row_(NULL) {}
        ~ObSQLResultSet() {}

        int init();
        int open();
        int close();
        int reset();
        bool is_select();

        int serialize(char *buf, const int64_t len, int64_t &pos);
        int deserialize(const char *buf, const int64_t len, int64_t &pos);

        common::ObNewScanner& get_new_scanner()
        {
          return scanner_;
        }
        const common::ObNewScanner& get_new_scanner() const
        {
          return scanner_;
        }
        /* Only for server end */
        const ObString& get_sql_str() const
        {
          return sqlstr_;
        }
        const FieldArray& get_fields() const
        {
          return fields_;
        }
        void get_fullfilled(bool &fullfilled)
        {
          int64_t no_use;
          scanner_.get_is_req_fullfilled(fullfilled, no_use);
        }
        const int get_errno() const { return errno_; }
        const ObString& get_sqlstr() const { return sqlstr_; }

        sql::ObResultSet& get_result_set() { return rs_; }
        void set_errno(int oberrno) { errno_ = oberrno; }
        void set_fullfilled(const bool fullfilled)
        {
          scanner_.set_is_req_fullfilled(fullfilled, 0);
        }
        void set_sqlstr(const ObString &sqlstr)
        {
          sqlstr_ = sqlstr;
        }
        void clear();
      private:
        sql::ObResultSet rs_;
        FieldArray fields_;
        common::ObNewScanner scanner_;
        ObString sqlstr_;
        ObRowDesc row_desc_;
        int errno_;
        /* which should serilize before rows in sql result set. */
        const ObRow *extra_row_;
    };

    inline void ObSQLResultSet::clear()
    {
      fields_.clear();
      scanner_.clear();
      row_desc_.reset();
      errno_ = OB_SUCCESS;
      set_fullfilled(true);
    }

    inline int ObSQLResultSet::init()
    {
      return rs_.init();
    }

    inline int ObSQLResultSet::open()
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = rs_.open()))
      {
        TBSYS_LOG(WARN, "fail to open result. ret = [%d]", ret);
      }
      return ret;
    }

    inline int ObSQLResultSet::close()
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = rs_.close()))
      {
        TBSYS_LOG(WARN, "fail to close result. ret = [%d]", ret);
      }
      return ret;
    }

    inline int ObSQLResultSet::reset()
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = rs_.reset()))
      {
        TBSYS_LOG(WARN, "fail to close result. ret = [%d]", ret);
      }
      return ret;
    }

    inline bool ObSQLResultSet::is_select()
    {
      return rs_.is_with_rows();
    }
  }
}

#endif /* _OB_SQL_RESULT_SET_H_ */
