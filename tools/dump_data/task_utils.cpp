#include "task_utils.h"
#include "common/ob_action_flag.h"

using namespace oceanbase::common;

namespace oceanbase {
  namespace tools {
    static char body_delima = '\001';
    static char line_delima = '\n';
    static const char kTabAscii = '\t';

    static void escape_varchar(char *p, int size)
    {
      for(int i = 0;i < size; i++)
      {
        if (p[i] == body_delima || p[i] == 0xd ||
            p[i] == 0xa || p[i] == kTabAscii)
        {
          p[i] = 0x20;                              /* replace [ctrl+A], [CR] by space */
        }
      }
    }

    int serialize_cell(ObCellInfo *cell, ObDataBuffer &buff)
    {
      int64_t cap = buff.get_remain();
      int64_t len = -1;
      int type = cell->value_.get_type();
      char *data = buff.get_data();
      int64_t pos = buff.get_position();

      switch(type) {
       case ObNullType:
         len = 0;
         break;
       case ObIntType:
         int64_t val;
         if (cell->value_.get_int(val) != OB_SUCCESS)
         {
           TBSYS_LOG(ERROR, "get_int error");
           break;
         }
         len = snprintf(data + pos, cap, "%ld", val);
         break;
       case ObVarcharType:
         {
           ObString str;
           if (cell->value_.get_varchar(str) != OB_SUCCESS ||
               cap < str.length())
           {
             TBSYS_LOG(ERROR, "get_varchar error");
             break;
           }

           memcpy(data + pos, str.ptr(), str.length());
           escape_varchar(data + pos, str.length());
           len = str.length();
         }
         break;
       case ObPreciseDateTimeType:
         {
           int64_t value;
           if (cell->value_.get_precise_datetime(value) != OB_SUCCESS)
           {
             TBSYS_LOG(ERROR, "get_precise_datetime error");
             break;
           }
           len = ObDateTime2MySQLDate(value, type, data + pos, int(cap));
         }
         break;
       case ObDateTimeType:
         {
           int64_t value;
           if (cell->value_.get_datetime(value) != OB_SUCCESS)
           {
             TBSYS_LOG(ERROR, "get_datetime error ");
             break;
           }
           len = ObDateTime2MySQLDate(value, type, data + pos, int(cap));
         }
         break;
       case ObModifyTimeType:
         {
           int64_t value;
           if (cell->value_.get_modifytime(value) != OB_SUCCESS)
           {
             TBSYS_LOG(ERROR, "get_modifytime error ");
             break;
           }
           len = ObDateTime2MySQLDate(value, type, data + pos, int(cap));
         }
         break;
       case ObCreateTimeType:
         {
           int64_t value;
           if (cell->value_.get_createtime(value) != OB_SUCCESS)
           {
             TBSYS_LOG(ERROR, "get_createtime error");
             break;
           }
           len = ObDateTime2MySQLDate(value, type, data + pos, int(cap));
         }
         break;
       case ObFloatType:
         {
           float value;
           if (cell->value_.get_float(value) != OB_SUCCESS)
           {
             TBSYS_LOG(ERROR, "get_float error");
             break;
           }
           len = snprintf(data + pos, cap, "%f", value);
         }
         break;
       case ObDoubleType:
         {
           double value;
           if (cell->value_.get_double(value) != OB_SUCCESS) {
             TBSYS_LOG(ERROR, "get_double error");
             break;
           }
           len = snprintf(data + pos, cap, "%f", value);
         }
         break;
       default:
         TBSYS_LOG(WARN, "Not Defined Type %d", cell->value_.get_type());
         break;
      }

      if (len >= 0 )
        buff.get_position() += len;

      return int(len);
    }
    int append_delima(ObDataBuffer &buff)
    {
      if (buff.get_remain() < 1)
        return OB_ERROR;

      buff.get_data()[buff.get_position()++] = body_delima;
      return OB_SUCCESS;
    }

    int append_end_rec(ObDataBuffer &buff)
    {
      if (buff.get_remain() < 1)
        return OB_ERROR;

      buff.get_data()[buff.get_position()++] = line_delima;
      return OB_SUCCESS;
    }

    int append_delima(ObDataBuffer &buff, const RecordDelima &delima)
    {
      if (buff.get_remain() < delima.length())
        return OB_ERROR;

      delima.append_delima(buff.get_data(), buff.get_position(), buff.get_capacity());
      delima.skip_delima(buff.get_position());
      return OB_SUCCESS;
    }

    int append_end_rec(ObDataBuffer &buff, const RecordDelima &rec_delima)
    {
      return append_delima(buff, rec_delima);
    }

    //return value--string length
    int ObDateTime2MySQLDate(int64_t ob_time, int time_type, char *outp, int size)
    {
      int ret = OB_SUCCESS;
      int len = 0;
      time_t time_sec;

      switch (time_type)
      {
       case ObModifyTimeType:
       case ObCreateTimeType:
       case ObPreciseDateTimeType:
         time_sec = ob_time / 1000L / 1000L;
         break;
       case ObDateTimeType:
         time_sec = ob_time;
         break;
       default:
         TBSYS_LOG(ERROR, "unrecognized time format, type=%d", time_type);
         ret = OB_ERROR;
      }

      if (ret == OB_SUCCESS)
      {
        struct tm time_tm;
        localtime_r(&time_sec, &time_tm);

        len = (int)strftime(outp, size, "%Y-%m-%d %H:%M:%S", &time_tm);
      }

      return len;
    }

    int append_int64(int64_t value, ObDataBuffer &buffer)
    {
      int ret = OB_SUCCESS;

      char *data = buffer.get_data() + buffer.get_position();
      int cap = (int)buffer.get_remain();
      if (cap <= 0)
      {
        ret = OB_ERROR;
      }
      else
      {
        int len = snprintf(data, cap, "%ld", value);
        if (len > 0)
        {
          buffer.get_position() += len;
        }
        else
        {
          TBSYS_LOG(ERROR, "append_int64 error");
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    int append_double(double value, ObDataBuffer &buffer)
    {
      int ret = OB_SUCCESS;

      char *data = buffer.get_data() + buffer.get_position();
      int cap = (int)buffer.get_remain();

      if (cap > 0)
      {
        int len = snprintf(data, cap, "%f", value);
        if (len > 0)
        {
          buffer.get_position() += len;
        }
        else
        {
          TBSYS_LOG(ERROR, "append_double error");
          ret = OB_ERROR;
        }
      }
      else
      {
        ret = OB_ERROR;
      }

      return ret;
    }

    int append_int8(int8_t value, ObDataBuffer &buffer)
    {
      int ret = OB_SUCCESS;
      char *data = buffer.get_data() + buffer.get_position();
      int cap = (int)buffer.get_remain();

      int len = snprintf(data, cap, "%d", value);
      if (len > 0)
      {
        buffer.get_position() += len;
      }
      else
      {
        ret = OB_ERROR;
      }

      return ret;
    }

    int append_str(const char *str, int64_t len, ObDataBuffer &buffer)
    {
      int ret = OB_SUCCESS;

      if (buffer.get_remain() >= len) {
        memcpy(buffer.get_data() + buffer.get_position(), str, len);
        buffer.get_position() += len;
      } else {
        ret = OB_ERROR;
      }

      return ret;
    }
  }
}
