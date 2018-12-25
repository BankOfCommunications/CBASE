/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $id
 *
 * Authors:
 *   fufeng <fufeng.syd@taobao.com>
 *     - some work details if you want
 */

#ifndef _OB_MYSQL_RESULT_SET_H_
#define _OB_MYSQL_RESULT_SET_H_

#include "sql/ob_result_set.h"
#include "ob_mysql_field.h"
#include "ob_mysql_row.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace oceanbase {
  namespace obmysql {
    class ObMySQLResultSet
      : public ObResultSet
    {
      public:
        /**
         * 构造函数
         *
         * @param [in] obrs SQL执行起返回的数据集
         */
        ObMySQLResultSet()
          : field_index_(0), param_index_(0) {};
        /**
         * 析构函数
         */
        ~ObMySQLResultSet() {};

        /**
         * 返回下一个字段的信息
         *
         * @param [out] obmf 下一个字段的信息
         *
         * @return 成功返回OB_SUCCESS。如果没有数据，则返回Ob_ITER_END
         */
        int next_field(ObMySQLField &obmf);

        /**
         * 返回下一个参数的信息
         *
         * @param [out] obmf 下一个参数的信息
         *
         * @return 成功返回OB_SUCCESS。如果没有数据，则返回Ob_ITER_END
         *
         */
        int next_param(ObMySQLField &obmf);

        /**
         * 获取字段数（列数）
         *
         * @return 该次查询的字段数
         */
        uint64_t get_field_cnt() const;

        /**
         * 获取参数个数
         *
         * @return 绑定变量的参数个数
         */
        uint64_t get_param_cnt() const;

        /**
         * 获取下一行的数据
         *
         * @param [out] obmr 下一行数据
         *
         * @return 成功返回OB_SUCCESS。如果没有数据，则返回Ob_ITER_END
         */
        int next_row(ObMySQLRow &obmr);
        int64_t to_string(char* buf, const int64_t buf_len) const;
      private:
        int get_next_row(const ObRow *&row);
      private:
        int64_t field_index_;     /**< 下一个需要读取的字段的序号 */
        int64_t param_index_;     /* < 下一个需要读取的参数的序号*/
    }; // end class ObMySQLResultSet

    inline int64_t ObMySQLResultSet::to_string(char* buf, const int64_t buf_len) const
    {
      return ObResultSet::to_string(buf, buf_len);
    }

  } // end of namespace obmysql
} // end of namespace oceabase

#endif /* _OB_MYSQL_RESULT_SET_H_ */
