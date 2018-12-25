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

#ifndef _OB_MYSQL_UTIL_H_
#define _OB_MYSQL_UTIL_H_

#include <inttypes.h>
#include <stdint.h>
#include "common/ob_string.h"
#include "common/ob_object.h"
#include "ob_mysql_global.h"
#include <float.h>              // for FLT_DIG and DBL_DIG
using namespace oceanbase::common;

namespace oceanbase
{
  namespace obmysql
  {
    enum MYSQL_PROTOCOL_TYPE
    {
      TEXT = 1,
      BINARY,
    };

    struct ObMySQLTypeMap
    {
        /* oceanbase::common::ObObjType ob_type; */
        EMySQLFieldType mysql_type;
        uint64_t length;        /* other than varchar type */
    };

    class ObMySQLUtil
    {
      public:
        /**
         * update null bitmap for binary protocol
         * please review http://dev.mysql.com/doc/internals/en/prepared-statements.html#null-bitmap for detail
         *
         * @param bitmap[in/out]         input bitmap
         * @param field_index            index of field
         *
         */
        static void update_null_bitmap(char *&bitmap, int64_t field_index);

        /**
         * update param's value if it is null
         *
         * @param param[out]
         * @param bitmap[in]
         * @param field_index[in]
         *
         * @return bool   true if current column value is null, else return false
         */
        static bool update_from_bitmap(ObObj &param, const char *bitmap, int64_t field_index);

        /**
         * 将长度写入到buf里面，可能占1,3,4,9个字节。
         *
         * @param [in] buf 要写入长度的buf
         * @param [in] len buf的总字节数
         * @param [in] length 需要写入的长度的值
         * @param [in,out] pos buf已使用的字节数
         *
         * @return oceanbase error code
         */
        static int store_length(char *buf, int64_t len, uint64_t length, int64_t &pos);
        /**
         * 读取长度字段，配合 store_length() 使用
         *
         * @param [in,out] pos 读取长度的位置，读取后更新pos
         * @param [out] length 长度
         */
        static int get_length(char *&pos, uint64_t &length);
        /**
         * 存一个 NULL 标志。
         *
         * @param buf 写入的内存地址
         * @param len buf的总字节数
         * @param pos buf已写入的字节数
         */
        static inline int store_null(char *buf, int64_t len, int64_t &pos);
        /**
         * @see store_str_v()
         * 将一段以'\0'结尾的字符串str打包进入buf（length coded string格式）
         *
         * @param [in] buf 写入的内存地址
         * @param [in] len buf的总字节数
         * @param [in] str 需要打包的字符串
         * @param [in,out] pos buf已使用的字节数
         *
         * @return OB_SUCCESS or oceanbase error code.
         *
         * @see store_length()
         */
        static int store_str(char *buf, int64_t len, const char *str, int64_t &pos);
        /**
         * 将一段长度为length的字符串str打包进入buf（length coded string格式）
         *
         * @param [in] buf 写入的内存地址
         * @param [in] len buf的总字节数
         * @param [in] str 需要打包的字符串
         * @param [in] length str的长度
         * @param [in,out] pos buf已使用的字节数
         *
         * @return OB_SUCCESS or oceanbase error code.
         *
         * @see store_str()
         * @see store_length()
         */
        static int store_str_v(char *buf, int64_t len, const char *str,
                               const uint64_t length, int64_t &pos);
        /**
         * 将ObString结构的字符串str打包进入buf（length coded string格式）
         *
         * @param [in] buf 写入的内存地址
         * @param [in] len buf的总字节数
         * @param [in] str ObString结构的字符串
         * @param [in,out] pos buf已使用的字节数
         *
         * @return OB_SUCCESS or oceanbase error code.
         *
         * @see store_length()
         * @see store_str_v()
         */
        static int store_obstr(char *buf, int64_t len, ObString str, int64_t &pos);
        /**
         * @see store_str_vzt()
         * 将一段以'\0'结尾的字符串str打包进入buf（zero terminated string格式）
         *
         * @param [in] buf 写入的内存地址
         * @param [in] len buf的总字节数
         * @param [in] str 需要打包的字符串
         * @param [in,out] pos buf已使用的字节数
         *
         * @return OB_SUCCESS or oceanbase error code.
         */
        static int store_str_zt(char *buf, int64_t len, const char *str, int64_t &pos);
        /**
         * @see store_str_zt()
         * 将一段长度为length的字符串str打包进入buf（zero terminated string格式）
         *
         * @param [in] buf 写入的内存地址
          * @param [in] len buf的总字节数
         * @param [in] str 需要打包的字符串
         * @param [in] length str的长度（不包括'\0'）
         * @param [in,out] pos buf已使用的字节数
         *
         * @return OB_SUCCESS or oceanbase error code.
         */
        static int store_str_vzt(char *buf, int64_t len, const char *str,
                               const uint64_t length, int64_t &pos);
        /**
         * @see store_str_vzt()
         * 将ObString结构的字符串str打包进入buf（zero terminated string格式）
         *
         * @param [in] buf 写入的内存地址
         * @param [in] len buf的总字节数
         * @param [in] str ObString结构的字符串
         * @param [in,out] pos buf已使用的字节数
         *
         * @return OB_SUCCESS or oceanbase error code.
         */
        static int store_obstr_zt(char *buf, int64_t len, ObString str, int64_t &pos);
        /**
         * 将ObString结构的字符串str打包进入buf（none-zero terminated raw string格式）
         *
         * @param [in] buf 写入的内存地址
         * @param [in] len buf的总字节数
         * @param [in] str ObString结构的字符串
         * @param [in,out] pos buf已使用的字节数
         *
         * @return OB_SUCCESS or oceanbase error code.
         */
        static int store_obstr_nzt(char *buf, int64_t len, ObString str, int64_t &pos);
//@{ 序列化整型数据，将v中的数据存到buf+pos的位置，并更新pos
        static inline int store_int1(char *buf, int64_t len, int8_t v, int64_t &pos);
        static inline int store_int2(char *buf, int64_t len, int16_t v, int64_t &pos);
        static inline int store_int3(char *buf, int64_t len, int32_t v, int64_t &pos);
        static inline int store_int4(char *buf, int64_t len, int32_t v, int64_t &pos);
        static inline int store_int5(char *buf, int64_t len, int64_t v, int64_t &pos);
        static inline int store_int6(char *buf, int64_t len, int64_t v, int64_t &pos);
        static inline int store_int8(char *buf, int64_t len, int64_t v, int64_t &pos);
//@}

//@{ 反序列话有符号整型，将结果写入到v中，并更新pos
        static inline void get_int1(char *&pos, int8_t &v);
        static inline void get_int2(char *&pos, int16_t &v);
        static inline void get_int3(char *&pos, int32_t &v);
        static inline void get_int4(char *&pos, int32_t &v);
        static inline void get_int8(char *&pos, int64_t &v);
//@}

//@{ 反序列化无符号整型，将结果写入到v中，并更新pos
        static inline void get_uint1(char *&pos, uint8_t &v);
        static inline void get_uint2(char *&pos, uint16_t &v);
        static inline void get_uint3(char *&pos, uint32_t &v);
        static inline void get_uint4(char *&pos, uint32_t &v);
        static inline void get_uint5(char *&pos, uint64_t &v);
        static inline void get_uint6(char *&pos, uint64_t &v);
        static inline void get_uint8(char *&pos, uint64_t &v);
//@}
        /**
         * 从ObObjType找到对应的MySQL Type的Length，该长度将用于MySQL客户端分配内存。
         *
         * @param [in] ob_type Oceabase type
         * @param [out] length MySQL Type的Length
         *
         * @return oceabase error code.
         *         OB_SUCCESS if success, OB_OBJ_TYPE_ERROR if ob_type not right.
         *
         * @note varchar类型的长度恒为0, 因为varchar的长度会根据具体的列产生变化。
         */
        static inline int get_type_length(ObObjType ob_type, int64_t &length);
        /**
         * 获取MySQL列类型
         *
         * @param [in] ob_type Oceabase 列类型
         * @param [out] mysql_type MySQL 列类型
         *
         * @return oceabase error code.
         *         OB_SUCCESS if success, OB_OBJ_TYPE_ERROR if ob_type not right.
         */
        static inline int get_mysql_type(ObObjType ob_type, EMySQLFieldType &mysql_type,
                                         uint8_t &num_decimals, uint32_t &length);

        /**
         * 获取Ocenabase列类型
         *
         * @param [out] ob_type Oceabase 列类型
         * @param [in] mysql_type MySQL 列类型
         *
         * @return oceabase error code.
         *         OB_SUCCESS if success, OB_OBJ_TYPE_ERROR if ob_type not right.
         */
        static inline int get_ob_type(ObObjType &ob_type, EMySQLFieldType mysql_type);

      private:
        static inline int get_map(ObObjType ob_type, const ObMySQLTypeMap *&map);
      public:
        static const uint64_t NULL_;
      private:
        static const ObMySQLTypeMap type_maps_[ObMaxType];
    }; // class ObMySQLUtil

    int ObMySQLUtil::store_null(char *buf, int64_t len, int64_t &pos)
    {
      return store_int1(buf, len, static_cast<int8_t>(251), pos);
    }

    int ObMySQLUtil::store_int1(char *buf, int64_t len, int8_t v, int64_t &pos)
    {
      int ret = OB_SUCCESS;

      if (len >= pos + 1)
      {
        int1store(buf + pos, v);
        pos++;
      }
      else
      {
        ret = OB_SIZE_OVERFLOW;
      }

      return ret;
    }

    int ObMySQLUtil::store_int2(char *buf, int64_t len, int16_t v, int64_t &pos)
    {
      int ret = OB_SUCCESS;

      if (len >= pos + 2)
      {
        int2store(buf + pos, v);
        pos += 2;
      }
      else
      {
        ret = OB_SIZE_OVERFLOW;
      }

      return ret;
    }

    int ObMySQLUtil::store_int3(char *buf, int64_t len, int32_t v, int64_t &pos)
    {
      int ret = OB_SUCCESS;

      if (len >= pos + 3)
      {
        int3store(buf + pos, v);
        pos += 3;
      }
      else
      {
        ret = OB_SIZE_OVERFLOW;
      }

      return ret;
    }

    int ObMySQLUtil::store_int4(char *buf, int64_t len, int32_t v, int64_t &pos)
    {
      int ret = OB_SUCCESS;

      if (len >= pos + 4)
      {
        int4store(buf + pos, v);
        pos += 4;
      }
      else
      {
        ret = OB_SIZE_OVERFLOW;
      }

      return ret;
    }

    int ObMySQLUtil::store_int5(char *buf, int64_t len, int64_t v, int64_t &pos)
    {
      int ret = OB_SUCCESS;

      if (len >= pos + 5)
      {
        int5store(buf + pos, v);
        pos += 5;
      }
      else
      {
        ret = OB_SIZE_OVERFLOW;
      }

      return ret;
    }

    int ObMySQLUtil::store_int6(char *buf, int64_t len, int64_t v, int64_t &pos)
    {
      int ret = OB_SUCCESS;

      if (len >= pos + 6)
      {
        int6store(buf + pos, v);
        pos += 6;
      }
      else
      {
        ret = OB_SIZE_OVERFLOW;
      }

      return ret;
    }

    int ObMySQLUtil::store_int8(char *buf, int64_t len, int64_t v, int64_t &pos)
    {
      int ret = OB_SUCCESS;

      if (len >= pos + 8)
      {
        int8store(buf + pos, v);
        pos += 8;
      }
      else
      {
        ret = OB_SIZE_OVERFLOW;
      }

      return ret;
    }

    void ObMySQLUtil::get_int1(char *&pos, int8_t &v)
    {
      v = sint1korr(pos);
      pos++;
    }
    void ObMySQLUtil::get_int2(char *&pos, int16_t &v)
    {
      v = sint2korr(pos);
      pos += 2;
    }
    void ObMySQLUtil::get_int3(char *&pos, int32_t &v)
    {
      v = sint3korr(pos);
      pos += 3;
    }
    void ObMySQLUtil::get_int4(char *&pos, int32_t &v)
    {
      v = sint4korr(pos);
      pos += 4;
    }
    void ObMySQLUtil::get_int8(char *&pos, int64_t &v)
    {
      v = sint8korr(pos);
      pos += 8;
    }


    void ObMySQLUtil::get_uint1(char *&pos, uint8_t &v)
    {
      v = uint1korr(pos);
      pos ++;
    }
    void ObMySQLUtil::get_uint2(char *&pos, uint16_t &v)
    {
      v = uint2korr(pos);
      pos += 2;
    }
    void ObMySQLUtil::get_uint3(char *&pos, uint32_t &v)
    {
      v = uint3korr(pos);
      pos += 3;
    }
    void ObMySQLUtil::get_uint4(char *&pos, uint32_t &v)
    {
      v = uint4korr(pos);
      pos += 4;
    }
    void ObMySQLUtil::get_uint5(char *&pos, uint64_t &v)
    {
      v = uint5korr(pos);
      pos += 5;
    }
    void ObMySQLUtil::get_uint6(char *&pos, uint64_t &v)
    {
      v = uint6korr(pos);
      pos += 6;
    }
    void ObMySQLUtil::get_uint8(char *&pos, uint64_t &v)
    {
      v = uint8korr(pos);
      pos += 8;
    }

    int ObMySQLUtil::get_map(ObObjType ob_type, const ObMySQLTypeMap *&map)
    {
      int ret = OB_SUCCESS;
      if (ob_type <= ObMinType || ob_type >= ObMaxType)
      {
        ret = OB_OBJ_TYPE_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        map = type_maps_ + ob_type;
      }

      return ret;
    }

    int ObMySQLUtil::get_type_length(ObObjType ob_type, int64_t &length)
    {
      const ObMySQLTypeMap *map = NULL;
      int ret = OB_SUCCESS;

      if ((ret = get_map(ob_type, map)) == OB_SUCCESS)
      {
        length = map->length;
      }
      return ret;
    }

    int ObMySQLUtil::get_mysql_type(ObObjType ob_type, EMySQLFieldType &mysql_type,
                                    uint8_t &num_decimals, uint32_t &length)
    {
      const ObMySQLTypeMap *map = NULL;
      int ret = OB_SUCCESS;

      if ((ret = get_map(ob_type, map)) == OB_SUCCESS)
      {
        mysql_type = map->mysql_type;
        num_decimals = 0;
        length = 0;
        static const uint32_t sign_len = 1; // we have only signed integers
        // decimals (1) -- max shown decimal digits * 0x00 for integers and static strings * 0x1f for dynamic strings, double, float * 0x00 - 0x51 for decimals
        // @see sql/field.cc in mysql source
        switch(mysql_type)
        {
          case MYSQL_TYPE_LONGLONG:
            length = MAX_BIGINT_WIDTH;
            break;
          case MYSQL_TYPE_TINY:
            length = MAX_TINYINT_WIDTH + sign_len;
            break;
          case MYSQL_TYPE_VAR_STRING:
            break;
          //add peiouya [DATE_TIME] 20150906:b
          case MYSQL_TYPE_NULL:
            break;
          case MYSQL_TYPE_DATE:
            length = MAX_DATE_WIDTH;
            break;
          case MYSQL_TYPE_TIME:
            length = MAX_TIME_WIDTH;
            break;
          //add 20150906:e
          //add lijianqiang [INT_32] 20150930:b
          case MYSQL_TYPE_LONG:
            length = MAX_INT_WIDTH + sign_len;//the MAX_INT_WIDTH is 10,do not forget add sign_len
            break;
          //add 20150930:e
          case MYSQL_TYPE_TIMESTAMP:
          case MYSQL_TYPE_DATETIME:
            length = MAX_DATETIME_WIDTH;
            break;
          case MYSQL_TYPE_FLOAT:
            length =  MAX_FLOAT_STR_LENGTH;
            num_decimals = 0x1f;
            break;
          case MYSQL_TYPE_DOUBLE:
            length = MAX_DOUBLE_STR_LENGTH;
            num_decimals = 0x1f;
            break;
            //add wenghaixing DECIMAL OceanBase_BankCommV0.2 2014_6_5:b
          case MYSQL_TYPE_NEWDECIMAL:
            length = 80;
            num_decimals = 0x1f;
            break;
           //add :e
          default:
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, "unexpected mysql_type=%d", mysql_type);
            break;
        } // end switch
      }
      return ret;
    }

    int ObMySQLUtil::get_ob_type(ObObjType &ob_type, EMySQLFieldType mysql_type)
    {
      int ret = OB_SUCCESS;
      switch(mysql_type)
      {
        case MYSQL_TYPE_NULL:
          ob_type = ObNullType;
          break;
        case MYSQL_TYPE_LONG:
          //add lijianqiang [INT_32] 20150930:b
          ob_type = ObInt32Type;
          break;
          //add 20150930:e
        case MYSQL_TYPE_LONGLONG:
          ob_type = ObIntType;
          break;
        case MYSQL_TYPE_FLOAT:
          ob_type = ObFloatType;
          break;
        case MYSQL_TYPE_DOUBLE:
          ob_type = ObDoubleType;
          break;
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_DATETIME:
          ob_type = ObPreciseDateTimeType;
          break;
        //add peiouya [DATE_TIME] 20150906:b
        case MYSQL_TYPE_DATE:
          ob_type = ObDateType;
          break;
        case MYSQL_TYPE_TIME:
          ob_type = ObTimeType;
          break;
        //add 20150906:e
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_STRING:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_BLOB:
          ob_type = ObVarcharType;
          break;
        case MYSQL_TYPE_TINY:
          ob_type = ObBoolType;
          break;
        case MYSQL_TYPE_DECIMAL:
          ob_type = ObDecimalType;
          break;
        default:
          TBSYS_LOG(WARN, "unsupport MySQL type %d", mysql_type);
          ret = OB_OBJ_TYPE_ERROR;
      }
      return ret;
    }
  }      // namespace obmysql
}      // namespace oceanbase

#endif /* _OB_MYSQL_UTIL_H_ */
