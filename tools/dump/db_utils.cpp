/*
 * =====================================================================================
 *
 *       Filename:  db_utils.cpp
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  06/17/2011 02:34:45 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#include "db_utils.h"
#include "db_dumper_config.h"
#include "common/ob_action_flag.h"
#include "updateserver/ob_ups_utils.h"
#include <stdio.h>

using namespace oceanbase::common;

static char header_delima = '\002';
static char body_delima = '\001';
static const char kTabSym = '\t';

//add by pangtz:20141205 [支持常见timestamp格式导入]
/*
 * support more formats of timestamp
 * YYYY-mm-dd
 * YYYYmmdd
 * YYYY-mm-dd HH:MM:SS
 * YYYY-mm-dd-HH:MM:SS
 * YYYY-mm-dd-HH.MM.SS.uuuuuu
 * YYYY/mm/dd HH:MM:SS.uuuuuu
 */
//add by zhuxh:20150714 [支持常见timestamp格式导入]
//Totally replaced
int transform_date_to_time(const char *str, int len, ObPreciseDateTime &t)
{
  int ret = OB_ERROR;
  ObPreciseDateTime mk_time = 0;
  struct tm mytime={0,0,0,0,0,0,0,0,0,0,0};

  int i = 0;
  //const static int MAXLEN = 256;
  int numbers[256]={0},numlens[256]={0},ni=0,ci=0;
  char chars[256]={0};
  for(i=0,ci=0,ni=0;i<len;)
  {
    while( i<len && !isdigit(str[i]) )
    {
      chars[ci]=str[i];
      ci++;
      i++;
    }
    while( i<len && isdigit(str[i]) )
    {
      numbers[ni]=numbers[ni]*10+(str[i]-'0');
      numlens[ni]++;
      i++;
    }
    ni++;
  }
  if( numlens[0]==4 )
  {
    mytime.tm_year = numbers[0]-1900;
    mytime.tm_mon = numbers[1]-1;
    mytime.tm_mday = numbers[2];
    mytime.tm_hour = numbers[3];
    mytime.tm_min = numbers[4];
    mytime.tm_sec = numbers[5];
    ret = OB_SUCCESS;
  }
  else if( numlens[0]==8 )
  {
    mytime.tm_mday = numbers[0]%100;
    numbers[0]/=100;
    mytime.tm_mon = numbers[0]%100-1;
    numbers[0]/=100;
    mytime.tm_year = numbers[0]-1900;
    ret = OB_SUCCESS;
  }
  //add by dinggh:20170305 [Date_Time] b:
  //length of tm_hour is 2 or 1
  else if ( numlens[0] == 2 || numlens[0] == 1 )
  {
    mytime.tm_hour = numbers[0];
    mytime.tm_min = numbers[1];
    mytime.tm_sec = numbers[2];
//  mytime.tm_year = 70;
//  mytime.tm_mon = 0;
//  mytime.tm_mday = 1;
    ret = OB_SUCCESS;
  }
  //add by dinggh:20170305 [Date_Time] e:
  else
    ret = OB_ERROR;
  //add by dinggh:20170305 [Date_Time] b:
  if( numlens[0] == 2 || numlens[0] == 1)
  {
    t = static_cast<ObPreciseDateTime>(mytime.tm_hour*3600+mytime.tm_min*60+mytime.tm_sec)*1000000L;
    ret = OB_SUCCESS;
  }
  else if(OB_SUCCESS == ret && (mk_time = mktime(&mytime))>0)
  //add by dinggh:20170305 [Date_Time] e:
  //if( OB_SUCCESS == ret && (mk_time = mktime(&mytime))>0 )//delete by dinggh:20170305 [Date_Time]
  {
    t = ((ObPreciseDateTime)mk_time)*1000000+(ObPreciseDateTime)numbers[6];
    ret = OB_SUCCESS;
  }
  else
  {
    t = -1;
    ret = OB_ERROR;
  }
  return ret;
//The followings are programs written before 20150101
/*
  int err = OB_SUCCESS;

  struct tm time;
  time_t tmp_time = 0;
  char back = str[len];
  const_cast<char*>(str)[len] = '\0';
  if (NULL != str && *str != '\0')
  {
    if (*str == 't' || *str == 'T')
    {
      if (sscanf(str + 1,"%ld", &t) != 1)
      {
        TBSYS_LOG(WARN, "%s", str + 1);
        err = OB_ERROR;
      }
    }
    else
    {
       //获取uuuuuu
      int usec=0;
      char *str_for_usec=NULL;
#if 0
      for(int i=len-1;i>=0;i--)
      {
          if((str[i]=='.')&&(len-i-1)==6)
          {
              str_for_usec=const_cast<char*>(str)+len-6;
              if(sscanf(str_for_usec, "%6d", &usec) != 1)
              {
                  err = OB_ERROR;
              }
              break;
          }
      }
#endif
      for(int i=len-1;i>=3;i--)
      {
        if(str[i] == '.'){
            if((str[i-3]=='.')&&(str[i-6]!='.')){
                break;
            }else{
            //if((str[i-3]=='.')&&(str[i-6]=='.')){
              str_for_usec=const_cast<char*>(str)+i+1;
              int k = len - i -1;
              switch(k){
                  case 1:
                    if(sscanf(str_for_usec, "%1d", &usec) != 1){
                        err = OB_ERROR;
                    }
                    break;
                  case 2:
                    if(sscanf(str_for_usec, "%2d", &usec) != 1){
                        err = OB_ERROR;
                    }
                    break;
                  case 3:
                    if(sscanf(str_for_usec, "%3d", &usec) != 1){
                        err = OB_ERROR;
                    }
                    break;
                  case 4:
                    if(sscanf(str_for_usec, "%4d", &usec) != 1){
                        err = OB_ERROR;
                    }
                    break;
                  case 5:
                    if(sscanf(str_for_usec, "%5d", &usec) != 1){
                        err = OB_ERROR;
                    }
                    break;
                  case 6:
                    if(sscanf(str_for_usec, "%6d", &usec) != 1){
                        err = OB_ERROR;
                    }
                    break;
                  default:
                    err = OB_ERROR;
                    break;
              }
              break;
          }
        }
      }
      if (strchr(str, '-') != NULL)
      {
        if (strchr(str, ' ') != NULL)
        {
          if ((sscanf(str,"%4d-%2d-%2d %2d:%2d:%2d",&time.tm_year,
                  &time.tm_mon,&time.tm_mday,&time.tm_hour,
                  &time.tm_min,&time.tm_sec)) != 6)
          {
                     err = OB_ERROR;
          }
        }
        else if(strchr(str, ':')!=NULL)
        {
          if ((sscanf(str,"%4d-%2d-%2d-%2d:%2d:%2d",&time.tm_year,&time.tm_mon,
                      &time.tm_mday,&time.tm_hour,
                      &time.tm_min,&time.tm_sec)) != 6)
          {
            err = OB_ERROR;
          }

        }
        else if(strchr(str, '.')!=NULL)
        {
          if ((sscanf(str,"%4d-%2d-%2d-%2d.%2d.%2d",&time.tm_year,&time.tm_mon,
                      &time.tm_mday,&time.tm_hour,
                      &time.tm_min,&time.tm_sec)) != 6)
          {
            err = OB_ERROR;
          }

        }
        else
        {
            if ((sscanf(str,"%4d-%2d-%2d",&time.tm_year,&time.tm_mon,
                    &time.tm_mday)) != 3)
            {
              err = OB_ERROR;
            }
            time.tm_hour = 0;
            time.tm_min = 0;
            time.tm_sec = 0;
        }
      }
      else if(strchr(str, '/') != NULL)
      {
          if (strchr(str, ' ') != NULL)
          {
            if ((sscanf(str,"%4d/%2d/%2d %2d:%2d:%2d",&time.tm_year,
                    &time.tm_mon,&time.tm_mday,&time.tm_hour,
                    &time.tm_min,&time.tm_sec)) != 6)
            {
                       err = OB_ERROR;
            }
          }
      }
      else
      {
        if (strchr(str, ':') != NULL)
        {
          if ((sscanf(str,"%4d%2d%2d %2d:%2d:%2d",&time.tm_year,
                  &time.tm_mon,&time.tm_mday,&time.tm_hour,
                  &time.tm_min,&time.tm_sec)) != 6)
          {
            err = OB_ERROR;
          }
        }
        else if (strlen(str) > 8)
        {
          if ((sscanf(str,"%4d%2d%2d%2d%2d%2d",&time.tm_year,
                  &time.tm_mon,&time.tm_mday,&time.tm_hour,
                  &time.tm_min,&time.tm_sec)) != 6)
          {
            err = OB_ERROR;
          }
        }
        else
        {
          if ((sscanf(str,"%4d%2d%2d",&time.tm_year,&time.tm_mon,
                  &time.tm_mday)) != 3)
          {
            err = OB_ERROR;
          }
          time.tm_hour = 0;
          time.tm_min = 0;
          time.tm_sec = 0;
        }
      }
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(ERROR,"sscanf failed : [%s] ",str);
      }
      else
      {
        time.tm_year -= 1900;
        time.tm_mon -= 1;
        time.tm_isdst = -1;

        if ((tmp_time = mktime(&time)) != -1)
        {
            if(usec == 0)
            {
                t = tmp_time * 1000 * 1000;
            }
            else
            {
                t = tmp_time * 1000 * 1000 + usec;
            }
        }
      }
    }
  }
  const_cast<char*>(str)[len] = back;
  return err;
*/
}
//add e
//add e

//add by zhuxh:20150715
int how_many_days(int y, int m)
{
  return ((m>=8?m+1:m)&1)+(m==2?((y%4==0 && y%100!=0) || y%400==0?29:28):30);
}
int transform_date_to_time(ObObjType tp, const char *str, int len, char timestamp[])
{
  int ret = OB_SUCCESS;
  int i = 0;
  //const static int MAXLEN = 256;
  int numbers[256]={0};
  int numlens[256]={0};
  int ni=0;
  int ci=0;
  char chars[256]={0};
  for(i=0,ci=0,ni=0;i<len;)
  {
    while( i<len && !isdigit(str[i]) )
    {
      chars[ci]=str[i];
      ci++;
      i++;
    }
    while( i<len && isdigit(str[i]) )
    {
      numbers[ni]=numbers[ni]*10+(str[i]-'0');
      numlens[ni]++;
      i++;
    }
    ni++;
  }

  if( tp == ObTimeType )
  {
    for(i=0;i<3;i++)
    {
      numbers[i+3]=numbers[i];
      numbers[i]=0;
      numlens[i+3]=numlens[i];
      numlens[i]=0;
    }
  }
  //h<24 && m<60 && s<60 is right
  if( numbers[3]>=24 || numbers[4]>=60 || numbers[5]>=60 )
    ret = OB_ERROR;
  if( tp != ObTimeType )
  {
      if( numlens[0]==4 )
        ret = OB_SUCCESS;
      else if( numlens[2]==4 ) //08/30/2015 //add by zhuxh:20150826
      {
        //[0] [1] [2]   [0][1][2]
        //2015/08/30 <= 08/30/2015
        int year = numbers[2];
        numbers[2] = numbers[1];
        numbers[1] = numbers[0];
        numbers[0] = year;
        int yearlen = numlens[2];
        numlens[2] = numlens[1];
        numlens[1] = numlens[0];
        numlens[0] = yearlen;
        ret = OB_SUCCESS;
      }
      else if( numlens[0]==8 )
      {
        //20151104 11:11:11.123456
        numbers[6]=numbers[4];
        numbers[5]=numbers[3];
        numbers[4]=numbers[2];
        numbers[3]=numbers[1];
        numbers[2] = numbers[0]%100;
        numbers[0] /= 100;
        numbers[1] = numbers[0]%100;
        numbers[0] /= 100;
        ret = OB_SUCCESS;
      }
      else
        ret = OB_ERROR;
      //Y-M:[1,12]-D:[1,days(Y,M)]
      if( numbers[1]<1 || numbers[1]>12 || numbers[2]<1 || numbers[2]>how_many_days(numbers[0],numbers[1]) )
        ret = OB_ERROR;
  }
  //add qianzm [.xxxxxx] 20151105
  if (OB_SUCCESS == ret)
  {
    if (numlens[6] > 6)
    {
      for (int i = numlens[6];i > 6;i--)
      {
        numbers[6] = numbers[6]/10;
      }
    }
    else if (numlens[6] < 6)
    {
      for (int i = numlens[6];i < 6;i++)
      {
        numbers[6] = numbers[6]*10;
      }
    }
  }
  //add e
  if( OB_SUCCESS == ret )
  {
    switch((int)tp)
    {
    case ObDateTimeType:
    case ObPreciseDateTimeType:// mod qianzm
        sprintf(timestamp,
        "%04d-%02d-%02d %02d:%02d:%02d.%06d",
        numbers[0],numbers[1],numbers[2],numbers[3],numbers[4],numbers[5],numbers[6]);
        break;
    case ObDateType:
        sprintf(timestamp,
        "%04d-%02d-%02d",
        numbers[0],numbers[1],numbers[2]);
        break;
    case ObTimeType:
        sprintf(timestamp,
        "%02d:%02d:%02d",
        numbers[3],numbers[4],numbers[5]);
        break;
    }
  }
  return ret;
}

int is_decimal(const char *str, const int &len)//add by liyongfeng 20140818 for check whether string satisfies decimal format:b
{
    int ret = OB_SUCCESS;
    int i = 0;
    int points_num = 0;

    if((str != NULL) && (len != 0))
    {
        if((str[i] == '+') || (str[i] == '-')) i++;
        if(str[i] == '.'){ i++; points_num++; }
        if((str[i] != '\0') && (i != len))
        {
            if((str[i] < '0') || (str[i] > '9')) {
                TBSYS_LOG(WARN, "input invalid decimal format");
                ret = OB_ERROR;
            } else {
                while(i < len)
                {
                    if(str[i] >= '0' && str[i] <= '9') { i++; }
                    else if(str[i] == '.') {
                        if(points_num == 0) { points_num++; i++; }
                        else {
                            TBSYS_LOG(WARN, "decimal format is not correct, decimal points exceed 1");
                            ret = OB_ERROR;
                            break;
                        }
                    } else {
                        TBSYS_LOG(WARN, "invalid character(non numeric or point)=[%c]", str[i]);
                        ret = OB_ERROR;
                        break;
                    }
                }
            }
        } else {
            TBSYS_LOG(WARN, "invalid decimal format, only sign character or point(+, - or .)");
            ret = OB_ERROR;
        }
    } else {
        TBSYS_LOG(WARN, "input invalid decimal string, null or len < 0");
        ret = OB_ERROR;
    }

    return ret;
}//add:e

//add by liyongfeng
int transform_str_to_int(const char* str, const int &len, int64_t &value)
{
    int ret = OB_SUCCESS;
    int i = 0;
    bool positive = true;
    int64_t flag = 1;
    value = 0;

    if((str != NULL) &&(len != 0))
    {
        i = 0;
        if(str[i] == '-')
        {
            positive = false;
            flag = -1;
            i++;
        }
        else if (str[i] == '+')
        {
            i++;
        }
        if(str[i] != '\0' && i != len)
        {
            while(i < len)
            {
                if(str[i] >= '0' && str[i] <= '9')
                {
                    value = value * 10 + flag * static_cast<int64_t>(str[i] - '0');
                    if((positive && (value > INT64_MAX || value < 0)) || (!positive && (value < INT64_MIN || value > 0))) // overflow
                    {
                        TBSYS_LOG(WARN, "value overflow, legal range[%ld, %ld]", INT64_MAX, INT64_MIN);
                        value = 0;
                        ret = OB_ERROR;
                        break;
                    }

                    i++;
                } else if(str[i] == '.' && i == len - 1) {//used for data type DECIMAL(n,0) transformed into INT
                    TBSYS_LOG(DEBUG, "the last character is '.', numeric string: %.*s", len, str);
                    i++;
                }
                else
                {
                    value = 0;
                    ret = OB_ERROR;
                    TBSYS_LOG(WARN, "invalid character(non numeric)=[%c]", str[i]);
                    break;
                }
            }
        }
        else
        {
            TBSYS_LOG(WARN, "only sign character(+ or -)");
            ret = OB_ERROR;
        }
    }
    else
    {
        TBSYS_LOG(WARN, "input invalid string, null or len < 0");
        ret = OB_ERROR;
    }
    return ret;
}
//add:e
void escape_varchar(char *p, int size)
{
  for(int i = 0;i < size; i++) {
    if (p[i] == body_delima || p[i] == 0xd ||
        p[i] == 0xa || p[i] == kTabSym) {
      p[i] = 0x20;                              /* replace [ctrl+A], [CR] by space */
    }
  }
}

int append_obj(const ObObj &obj, ObDataBuffer &buff)
{
  int64_t cap = buff.get_remain();
  int64_t len = -1;
  int type = obj.get_type();
  char *data = buff.get_data();
  int64_t pos = buff.get_position();

  switch(type) {
   case ObNullType:
     len = 0;
     break;
   case ObIntType:
     int64_t val;
     if (obj.get_int(val) != OB_SUCCESS) {
       TBSYS_LOG(ERROR, "get_int error");
       break;
     }
     len = snprintf(data + pos, cap, "%ld", val);
     break;
   case ObVarcharType:
     {
       ObString str;
       if (obj.get_varchar(str) != OB_SUCCESS ||
           cap < str.length()) {
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
       if (obj.get_precise_datetime(value) != OB_SUCCESS) {
         TBSYS_LOG(ERROR, "get_precise_datetime error");
         break;
       }
       len = ObDateTime2MySQLDate(value, type, data + pos, static_cast<int32_t>(cap));
     }
     break;
   case ObDateTimeType:
     {
       int64_t value;
       if (obj.get_datetime(value) != OB_SUCCESS) {
         TBSYS_LOG(ERROR, "get_datetime error");
         break;
       }
       len = ObDateTime2MySQLDate(value, type, data + pos, static_cast<int32_t>(cap));
     }
     break;
   case ObModifyTimeType:
     {
       int64_t value;
       if (obj.get_modifytime(value) != OB_SUCCESS) {
         TBSYS_LOG(ERROR, "get_modifytime error ");
         break;
       }
       len = ObDateTime2MySQLDate(value, type, data + pos, static_cast<int32_t>(cap));
     }
     break;
   case ObCreateTimeType:
     {
       int64_t value;
       if (obj.get_createtime(value) != OB_SUCCESS) {
         TBSYS_LOG(ERROR, "get_createtime error");
         break;
       }
       len = ObDateTime2MySQLDate(value, type, data + pos, static_cast<int32_t>(cap));
     }
     break;
   case ObFloatType:
     {
       float value;
       if (obj.get_float(value) != OB_SUCCESS) {
         TBSYS_LOG(ERROR, "get_float error");
         break;
       }
       len = snprintf(data + pos, cap, "%f", value);
     }
     break;
   case ObDoubleType:
     {
       double value;
       if (obj.get_double(value) != OB_SUCCESS) {
         TBSYS_LOG(ERROR, "get_double error");
         break;
       }
       len = snprintf(data + pos, cap, "%f", value);
     }
     break;
   default:
     TBSYS_LOG(WARN, "Not Defined Type %d", obj.get_type());
     break;
  }

  if (len >= 0 ) {
    buff.get_position() += len;
  }

  return static_cast<int>(len);
}

int serialize_cell(ObCellInfo *cell, ObDataBuffer &buff)
{
  return append_obj(cell->value_, buff);
}

int append_delima(ObDataBuffer &buff)
{
  if (buff.get_remain() < 1)
    return OB_ERROR;

  buff.get_data()[buff.get_position()++] = body_delima;
  return OB_SUCCESS;
}

int append_header_delima(ObDataBuffer &buff)
{
  if (buff.get_remain() < 1)
    return OB_ERROR;

  buff.get_data()[buff.get_position()++] = header_delima;
  return OB_SUCCESS;
}

int append_end_rec(ObDataBuffer &buff)
{
  char *data = buff.get_data();
  int64_t pos = buff.get_position();
  int64_t cap = buff.get_remain();

  int len = snprintf(data + pos, cap, "\n");
  if (len >=0 )
    buff.get_position() += len;

  return len;
}

//return value--string length
int ObDateTime2MySQLDate(int64_t ob_time, int time_type, char *outp, int size)
{
  int ret = OB_SUCCESS;
  int len = 0;
  time_t time_sec;

  switch (time_type) {
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

  if (ret == OB_SUCCESS) {
    struct tm time_tm;
    localtime_r(&time_sec, &time_tm);

    len = static_cast<int32_t>(strftime(outp, size, "%Y-%m-%d %H:%M:%S", &time_tm));
  }

  return len;
}

const char *get_op_string(int action)
{
  const char *res = NULL;

  if (action == ObActionFlag::OP_UPDATE) {
    res = "UPDATE";
  } else if (action == ObActionFlag::OP_INSERT) {
    res = "INSERT";
  } else if (action == ObActionFlag::OP_DEL_ROW) {
    res = "DELETE";
  }

  return res;
}

int db_utils_init()
{
  //  header_delima = DUMP_CONFIG->get_header_delima();
  //  body_delima = DUMP_CONFIG->get_body_delima();
  return 0;
}

void encode_int32(char* buf, uint32_t value)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
  memcpy(buf, &value, sizeof(value));
#else
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
  buf[2] = (value >> 16) & 0xff;
  buf[3] = (value >> 24) & 0xff;
#endif
}

int32_t decode_int32(const char *buf)
{
  uint32_t value;
#if __BYTE_ORDER == __LITTLE_ENDIAN
  memcpy(&value, buf, sizeof(value));
#else
  value = ((static_cast<uint32_t>(ptr[0]))
           | (static_cast<uint32_t>(ptr[1]) << 8)
           | (static_cast<uint32_t>(ptr[2]) << 16)
           | (static_cast<uint32_t>(ptr[3]) << 24));
#endif

  return value;
}

#if 0 //delete by pangtz
int transform_date_to_time(const char *str, int len, ObPreciseDateTime &t)
{
  int err = OB_SUCCESS;
  struct tm time;
  time_t tmp_time = 0;
  char back = str[len];
  const_cast<char*>(str)[len] = '\0';
  //TBSYS_LOG(INFO, "input str = %s", str);
  if (NULL != str && *str != '\0')
  {
    if (*str == 't' || *str == 'T')
    {
      if (sscanf(str + 1,"%ld", &t) != 1)
      {
        TBSYS_LOG(WARN, "%s", str + 1);
        err = OB_ERROR;
      }
    }
    else
    {
      if (strchr(str, '-') != NULL)
      {
        if (strchr(str, ':') != NULL)
        {
          if ((sscanf(str,"%4d-%2d-%2d %2d:%2d:%2d",&time.tm_year,
                  &time.tm_mon,&time.tm_mday,&time.tm_hour,
                  &time.tm_min,&time.tm_sec)) != 6)
          {
            err = OB_ERROR;
          }
        }
        else
        {
          if ((sscanf(str,"%4d-%2d-%2d",&time.tm_year,&time.tm_mon,
                  &time.tm_mday)) != 3)
          {
            err = OB_ERROR;
          }
          time.tm_hour = 0;
          time.tm_min = 0;
          time.tm_sec = 0;
        }
      }
      else
      {
        if (strchr(str, ':') != NULL)
        {
          if ((sscanf(str,"%4d%2d%2d %2d:%2d:%2d",&time.tm_year,
                  &time.tm_mon,&time.tm_mday,&time.tm_hour,
                  &time.tm_min,&time.tm_sec)) != 6)
          {
            err = OB_ERROR;
          }
        }
        else if (strlen(str) > 8)
        {
          if ((sscanf(str,"%4d%2d%2d%2d%2d%2d",&time.tm_year,
                  &time.tm_mon,&time.tm_mday,&time.tm_hour,
                  &time.tm_min,&time.tm_sec)) != 6)
          {
            err = OB_ERROR;
          }
        }
        else
        {
          if ((sscanf(str,"%4d%2d%2d",&time.tm_year,&time.tm_mon,
                  &time.tm_mday)) != 3)
          {
            err = OB_ERROR;
          }
          time.tm_hour = 0;
          time.tm_min = 0;
          time.tm_sec = 0;
        }
      }
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(ERROR,"sscanf failed : [%s] ",str);
      }
      else
      {
        time.tm_year -= 1900;
        time.tm_mon -= 1;
        time.tm_isdst = -1;

        if ((tmp_time = mktime(&time)) != -1)
        {
          t = tmp_time * 1000 * 1000;
        }
      }
    }
  }
  const_cast<char*>(str)[len] = back;
  return err;
}
#endif

void dump_scanner(ObScanner &scanner)
{
  ObScannerIterator iter;
  bool is_row_changed = false;

  for (iter = scanner.begin(); iter != scanner.end(); iter++)
  {
    ObCellInfo *cell_info;
    iter.get_cell(&cell_info, &is_row_changed);
    if (is_row_changed)
    {
      TBSYS_LOG(INFO, "table_id:%lu, rowkey:\n", cell_info->table_id_);
      //      hex_dump(cell_info->row_key_.ptr(), cell_info->row_key_.length());
    }
    TBSYS_LOG(INFO, "%s", print_cellinfo(cell_info, "DUMP-SCANNER"));
  }
}


