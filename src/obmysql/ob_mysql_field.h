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

#ifndef _OB_MYSQL_FIELD_H_
#define _OB_MYSQL_FIELD_H_

#include <inttypes.h>
#include "sql/ob_sql.h"

namespace oceanbase
{
  namespace obmysql
  {
    class ObMySQLResultSet;     // pre-declearation

    class ObMySQLField : private sql::ObResultSet::Field
    {
        friend class ObMySQLResultSet;
      public:
        /**
         * 构造函数
         */
        ObMySQLField();
        void set_charset(uint16_t charset) { charsetnr_ = charset; }
        /**
         * 将该行数据序列化成MySQL认识的格式
         *
         * @param [in] buf 序列化后的一行数据
         * @param [in] len buf的长度
         * @param [in,out] pos 输入buf第一个可写位置，输出序列化后buf第一个可写位置
         *
         * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
         */
        int serialize(char *buf, const int64_t len, int64_t &pos) const
        {
          return serialize_pro41(buf, len, pos);
        }
      private:
        /**
         * 将该行数据序列化成MySQL认识的格式（协议版本4.1）
         *
         * @param [in] buf 序列化后的一行数据
         * @param [in] len buf的长度
         * @param [in,out] pos 输入buf第一个可写位置，输出序列化后buf第一个可写位置
         *
         * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
         */
        int serialize_pro41(char *buf, const int64_t len, int64_t &pos) const;
      private:
        const char *catalog_;	    /* Catalog for table */
        const char *db_;            /* database name for table*/
        uint32_t length_;            /*Width of column (create length) */
        uint16_t flags_;            /* Div flags */
        uint8_t decimals_;         /* Number of decimals in field */
        uint16_t charsetnr_;        /* Character set */
        // enum enum_field_types type_; /* Type of field. See mysql_com.h for types */
        // void *extension;

    }; // end class ObMySQLField

  } // namespace obmysql
} // namespace oceanbase


#endif /* _OB_MYSQL_FIELD_H_ */
