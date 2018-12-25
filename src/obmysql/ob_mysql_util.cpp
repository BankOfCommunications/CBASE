#include "ob_mysql_util.h"

namespace oceanbase
{
  namespace obmysql
  {
    const uint64_t ObMySQLUtil::NULL_ = UINT64_MAX;
    const ObMySQLTypeMap ObMySQLUtil::type_maps_[ObMaxType] =
    {
                                   /* ObMinType */
      {MYSQL_TYPE_NULL, 0},        /* ObNullType */
      {MYSQL_TYPE_LONGLONG, 0},    /* ObIntType */
      {MYSQL_TYPE_FLOAT, 0},       /* ObFloatType */
      {MYSQL_TYPE_DOUBLE, 0},      /* ObDoubleType */
      {MYSQL_TYPE_DATETIME, 0},    /* ObDateTimeType */
      {MYSQL_TYPE_TIMESTAMP, 0},    /* ObPreciseDateTimeType */
      {MYSQL_TYPE_VAR_STRING, 0},     /* ObVarcharType */
      {MYSQL_TYPE_NOT_DEFINED, 0}, /* ObSeqType */
      {MYSQL_TYPE_TIMESTAMP, 0},    /* ObCreateTimeType */
      {MYSQL_TYPE_TIMESTAMP, 0},    /* ObModifyTimeType */
      {MYSQL_TYPE_NOT_DEFINED, 0}, /* ObExtendType */
      {MYSQL_TYPE_TINY, 0},        /* ObBoolType */
      {MYSQL_TYPE_NEWDECIMAL, 0},      /* ObDecimalType */
      //add peiouya [DATE_TIME] 20150906:b
      {MYSQL_TYPE_DATE, 0},      /* ObDateType */
      {MYSQL_TYPE_TIME, 0},      /* ObTimeType */
      //add 20150906:e
      //add lijianqiang [INT_32] 20150930:b
      {MYSQL_TYPE_NOT_DEFINED,0}, /*ObIntervalType*/
      {MYSQL_TYPE_LONG, 0}         /*ObInt32Type*/
      //add 20150930:e
                                   /* ObMaxType */
    };

    //TODO avoid coredump if field_index is too large
    //http://dev.mysql.com/doc/internals/en/prepared-statements.html#null-bitmap
    //offset is 2
    void ObMySQLUtil::update_null_bitmap(char *&bitmap, int64_t field_index)
    {
      int byte_pos = static_cast<int>((field_index + 2) / 8);
      int bit_pos  = static_cast<int>((field_index + 2) % 8);
      bitmap[byte_pos] |= static_cast<char>(1 << bit_pos);
    }

    //called by handle COM_STMT_EXECUTE offset is 0
    bool ObMySQLUtil::update_from_bitmap(ObObj &param, const char *bitmap, int64_t field_index)
    {
      bool ret = false;
      int byte_pos = static_cast<int>(field_index / 8);
      int bit_pos  = static_cast<int>(field_index % 8);
      char value = bitmap[byte_pos];
      if (value & (1 << bit_pos))
      {
        ret = true;
        param.set_null();
      }
      return ret;
    }

    int ObMySQLUtil::store_length(char *buf, int64_t len, uint64_t length, int64_t &pos)
    {
      int ret = OB_SUCCESS;
      if (len < 0 || pos < 0 || len <= pos)
      {
        ret = OB_SIZE_OVERFLOW;
        //TBSYS_LOG(WARN, "Store length fail, buffer over flow!"
        //          " len: [%ld], pos: [%ld], length: [%ld], ret: [%d]",
        //          len, pos, length, ret);
      }

      int64_t remain = len - pos;
      if (OB_SUCCESS == ret)
      {
        if (length < (uint64_t) 251 && remain >= 1)
        {
          ret = store_int1(buf, len, (uint8_t) length, pos);
        }
        /* 251 is reserved for NULL */
        else if (length < (uint64_t) 0X10000 && remain >= 3)
        {
          ret = store_int1(buf, len, static_cast<int8_t>(252), pos);
          if (OB_SUCCESS == ret)
          {
            ret = store_int2(buf, len, (uint16_t) length, pos);
            if (OB_SUCCESS != ret)
            {
              pos--;
            }
          }
        }
        else if (length < (uint64_t) 0X1000000 && remain >= 4)
        {
          ret = store_int1(buf, len, (uint8_t) 253, pos);
          if (OB_SUCCESS == ret)
          {
            ret = store_int3(buf, len, (uint32_t) length, pos);
            if (OB_SUCCESS != ret)
            {
              pos--;
            }
          }
        }
        else if (length < UINT64_MAX && remain >= 9)
        {
          ret = store_int1(buf, len, (uint8_t) 254, pos);
          if (OB_SUCCESS == ret)
          {
            ret = store_int8(buf, len, (uint64_t) length, pos);
            if (OB_SUCCESS != ret)
            {
              pos--;
            }
          }
        }
        else if (length == UINT64_MAX) /* NULL_ == UINT64_MAX */
        {
          ret = store_null(buf, len, pos);
        }
        else
        {
          ret = OB_SIZE_OVERFLOW;
          //TBSYS_LOG(WARN, "Store length fail, buffer over flow!"
          //          " len: [%ld], pos: [%ld], length: [%ld], ret: [%d]",
          //          len, pos, length, ret);
        }
      }

      return ret;
    }

    int ObMySQLUtil::get_length(char *&pos, uint64_t &length)
    {
      uint8_t sentinel = 0;
      uint16_t s2 = 0;
      uint32_t s4 = 0;
      int ret = OB_SUCCESS;

      get_uint1(pos, sentinel);
      if (sentinel < 251)
      {
        length = sentinel;
      }
      else if (sentinel == 251)
      {
        length = NULL_;
      }
      else if (sentinel == 252)
      {
        get_uint2(pos, s2);
        length = s2;
      }
      else if (sentinel == 253)
      {
        get_uint3(pos, s4);
        length = s4;
      }
      else if (sentinel == 254)
      {
        get_uint8(pos, length);
      }
      else
      {
        // 255??? won't get here.
        pos--;                  // roll back
        ret = OB_INVALID_DATA;
      }

      return ret;
    }

    int ObMySQLUtil::store_str(char *buf, int64_t len, const char *str, int64_t &pos)
    {
      uint64_t length = strlen(str);
      return store_str_v(buf, len, str, length, pos);
    }

    int ObMySQLUtil::store_str_v(char *buf, int64_t len, const char *str,
                    const uint64_t length, int64_t &pos)
    {
      int ret = OB_SUCCESS;
      int64_t pos_bk = pos;

      if (OB_SUCCESS != (ret = store_length(buf, len, length, pos)))
      {
        TBSYS_LOG(WARN, "Store length fail!"
                  " len: [%ld], pos: [%ld], length: [%ld], ret: [%d]",
                  len, pos, length, ret);
      }
      else if (len >= pos && length <= static_cast<uint64_t>(len - pos))
      {
        memcpy(buf + pos, str, length);
        pos += length;
      }
      else
      {
        pos = pos_bk;        // roll back
        ret = OB_SIZE_OVERFLOW;
        //TBSYS_LOG(WARN, "Store string fail, buffer over flow!"
        //          " len: [%ld], pos: [%ld], length: [%ld], ret: [%d]",
        //          len, pos, length, ret);
      }

      return ret;
    }

    int ObMySQLUtil::store_obstr(char *buf, int64_t len, ObString str, int64_t &pos)
    {
      return store_str_v(buf, len, str.ptr(), str.length(), pos);
    }

    int ObMySQLUtil::store_str_zt(char *buf, int64_t len, const char *str, int64_t &pos)
    {
      uint64_t length = strlen(str);
      return store_str_vzt(buf, len, str, length, pos);
    }

    int ObMySQLUtil::store_str_vzt(char *buf, int64_t len, const char *str,
                                   const uint64_t length, int64_t &pos)
    {
      int ret = OB_SUCCESS;
      if (len > 0 && pos > 0 && len > pos && static_cast<uint64_t>(len - pos) > length)
      {
        memcpy(buf + pos, str, length);
        pos += length;
        buf[pos++] = '\0';
      }
      else
      {
        ret = OB_SIZE_OVERFLOW;
        //TBSYS_LOG(WARN, "Store string fail, buffer over flow!"
        //          " len: [%ld], pos: [%ld], length: [%ld], ret: [%d]",
        //          len, pos, length, ret);
      }
      return ret;
    }

    int ObMySQLUtil::store_obstr_zt(char *buf, int64_t len, ObString str, int64_t &pos)
    {
      return store_str_vzt(buf, len, str.ptr(), str.length(), pos);
    }

    int ObMySQLUtil::store_obstr_nzt(char *buf, int64_t len, ObString str, int64_t &pos)
    {
      int ret = OB_SUCCESS;
      if (len > 0 && pos > 0 && len > pos && len - pos >= str.length())
      {
        memcpy(buf + pos, str.ptr(), str.length());
        pos += str.length();
      }
      else
      {
        ret = OB_SIZE_OVERFLOW;
        //TBSYS_LOG(WARN, "Store string fail, buffer over flow!"
        //          " len: [%ld], pos: [%ld], length: [%d], ret: [%d]",
        //          len, pos, str.length(), ret);
      }
      return ret;
    }


  } // namespace obmysql
} // namespace oceanbase
