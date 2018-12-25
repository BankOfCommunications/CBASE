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

#ifndef _OB_MYSQL_ROW_H_
#define _OB_MYSQL_ROW_H_

#include "common/ob_row.h"
#include "common/ob_object.h"
#include "ob_mysql_util.h"

using namespace oceanbase::common;

namespace oceanbase {
  namespace obmysql {

    class ObMySQLResultSet;

    class ObMySQLRow
    {
      friend class ObMySQLResultSet;
      public:
        ObMySQLRow();

      public:
        /**
         * 将该行数据序列化成MySQL认识的格式，输出位置：buf + pos，执行后pos指向buf中第一个free的位置。
         *
         * @param [in] buf 序列化以后输出的序列的空间
         * @param [in] len buf的长度
         * @param [out] pos 当前buf第一个free的位置
         *
         * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
         */
        int serialize(char *buf, const int64_t len, int64_t &pos) const;

        void set_protocol_type(MYSQL_PROTOCOL_TYPE type) const
        {
          type_ = type;
        }

        const ObRow *&get_inner_row()
        {
          return row_;
        }

      private:
        /**
         * 序列化一个cell到buf + pos的位置。
         *
         * @param [in] obj 需要序列化的cell
         * @param [in] buf 输出的buf
         * @param [in] len buf的大小
         * @param [in,out] pos 写入buf的位置
         * @param [in] cell index for binary protocol
         *
         * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
         */
        int cell_str(const ObObj &obj, char *buf, const int64_t len, int64_t &pos, int64_t cell_index) const;
        /**
         * 序列化一个null类型的cell到buf + pos的位置。
         *
         * @param [in] obj 需要序列化的cell
         * @param [in] buf 输出的buf
         * @param [in] len buf的大小
         * @param [in,out] pos 写入buf的位置
         * @param [in] field index
         *
         * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
         */
        int null_cell_str(const ObObj &obj, char *buf, const int64_t len, int64_t &pos, int64_t cell_index) const;
        /**
         * 序列化一个整型的cell到buf + pos的位置。
         * (ObBoolType, ObIntType)
         *
         * @param [in] obj 需要序列化的cell
         * @param [in] buf 输出的buf
         * @param [in] len buf的大小
         * @param [in,out] pos 写入buf的位置
         *
         * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
         */
        int int_cell_str(const ObObj &obj, char *buf, const int64_t len, int64_t &pos) const;
        int bool_cell_str(const ObObj &obj, char *buf, const int64_t len, int64_t &pos) const;
        //add lijianqiang [INT_32] 20151008:b
        /**
         * 序列化一个整型int32的cell到buf + pos的位置。
         * (ObInt32Type)
         *
         * @param [in] obj 需要序列化的cell
         * @param [in] buf 输出的buf
         * @param [in] len buf的大小
         * @param [in,out] pos 写入buf的位置
         *
         * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
         */
        int int32_cell_str(const ObObj &obj, char *buf, const int64_t len, int64_t &pos) const;
        //add 20151008:e
        /**
         * 序列化一个定点数型的cell到buf + pos的位置。
         * (ObDecimalType)
         *
         * @param [in] obj 需要序列化的cell
         * @param [in] buf 输出的buf
         * @param [in] len buf的大小
         * @param [in,out] pos 写入buf的位置
         *
         * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
         */
        int decimal_cell_str(const ObObj &obj, char *buf, const int64_t len, int64_t &pos) const;
        /**
         * 序列化一个datetime型的cell到buf + pos的位置。
         * (ObDateTimeType, ObPreciseDateTimeType, ObCreateTimeType, ObModifyTimeType)
         *
         * @param [in] obj 需要序列化的cell
         * @param [in] buf 输出的buf
         * @param [in] len buf的大小
         * @param [in,out] pos 写入buf的位置
         *
         * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
         */
        int datetime_cell_str(const ObObj &obj, char *buf, const int64_t len, int64_t &pos) const;
        //add peiouya [DATE_TIME] 20150906:b
        /**
         * 序列化一个datetime型的cell到buf + pos的位置。
         * (ObDateTimeType, ObPreciseDateTimeType, ObCreateTimeType, ObModifyTimeType)
         *
         * @param [in] obj 需要序列化的cell
         * @param [in] buf 输出的buf
         * @param [in] len buf的大小
         * @param [in,out] pos 写入buf的位置
         *
         * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
         */
        int date_cell_str(const ObObj &obj, char *buf, const int64_t len, int64_t &pos) const;
                /**
         * 序列化一个datetime型的cell到buf + pos的位置。
         * (ObDateTimeType, ObPreciseDateTimeType, ObCreateTimeType, ObModifyTimeType)
         *
         * @param [in] obj 需要序列化的cell
         * @param [in] buf 输出的buf
         * @param [in] len buf的大小
         * @param [in,out] pos 写入buf的位置
         *
         * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
         */
        int time_cell_str(const ObObj &obj, char *buf, const int64_t len, int64_t &pos) const;
        //add 20150906:e
        /**
         * 序列化一个字符串类型的cell到buf + pos的位置。
         * (ObVarcharType)
         *
         * @param [in] obj 需要序列化的cell
         * @param [in] buf 输出的buf
         * @param [in] len buf的大小
         * @param [in,out] pos 写入buf的位置
         *
         * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
         */
        int varchar_cell_str(const ObObj &obj, char *buf, const int64_t len, int64_t &pos) const;
        /**
         * 序列化一个浮点数型的cell到buf + pos的位置。
         * (ObFloatType, ObDoubleType)
         *
         * @param [in] obj 需要序列化的cell
         * @param [in] buf 输出的buf
         * @param [in] len buf的大小
         * @param [in,out] pos 写入buf的位置
         *
         * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
         */
        int float_cell_str(const ObObj &obj, char *buf, const int64_t len, int64_t &pos) const;
//add by wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_14:b
        bool check_decimal(ObString os,uint32_t p,uint32_t s)const;
        //add e
        //add liuzy [datatime bug] 20151112:b
        //Exp: revise four-digit for year
        int fix_year_format(char *buf, const int64_t len, int64_t &pos, uint64_t &len_zero, const struct tm *tms) const;
        //add 20151112:e
      private:
        const common::ObRow *row_;
        mutable MYSQL_PROTOCOL_TYPE type_;
        mutable char *bitmap_;
        mutable int64_t bitmap_bytes_;

    }; // end class ObMySQLRow

  } // end of namespace obmysql
} // end of namespace oceanbase

#endif /* _OB_MYSQL_ROW_H_ */
