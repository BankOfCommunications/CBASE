#include "ob_rowkey_helper.h"

namespace oceanbase
{
  namespace common
  {
    int ObRowkeyHelper::prepare_obj_array(const ObRowkeyInfo& info, ObObj* array, const int64_t size)
    {
      int ret = OB_SUCCESS;
      if (NULL == array || size != info.get_size())
      {
        TBSYS_LOG(WARN, "invalid argument arry = %p, size=%ld, info size=%ld", 
            array, size, info.get_size());
        ret = OB_ERROR;
      }
      else
      {
        ObRowkeyColumn column;
        for (int64_t index = 0; index < size && OB_SUCCESS == ret; ++index)
        {
          ret = info.get_column(index, column);
          if (OB_SUCCESS == ret)
          {
            array[index].set_type(static_cast<ObObjType>(column.type_));
            array[index].set_val_len(static_cast<int32_t>(column.length_));
          }
        }
      }

      return ret;
    }

    int ObRowkeyHelper::get_row_key(const ObObj* array, const int64_t size, ObString& key)
    {
      int ret = OB_SUCCESS;
      if (NULL == array)
      {
        TBSYS_LOG(WARN, "invalid argument arry = %p", array);
        ret = OB_ERROR;
      }
      else
      {
        char*    buffer = key.ptr();
        int64_t buf_len = key.size();
        int64_t     pos = 0;

        for (int32_t index = 0; OB_SUCCESS == ret && index < size; ++index)
        {
          const ObObj *obj = &array[index];
          ret = serialize_obj(obj, buffer, buf_len, pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "serialize_obj failed, buffer=%p, buf_len=%ld, pos=%ld",
                buffer, buf_len, pos);
          }
        }

        if ( OB_SUCCESS == ret)
        {
          key.assign_ptr(buffer, static_cast<int32_t>(pos));
        }

      }
      return ret;
    }

    int ObRowkeyHelper::get_obj_array(const ObString& key, ObObj* array, const int64_t size)
    {
      int ret = OB_SUCCESS;
      if (NULL == array)
      {
        TBSYS_LOG(WARN, "invalid argument array=%p", array);
        ret = OB_ERROR;
      }
      else
      {

        //int32_t     len = 0;
        char*    buffer = const_cast<char*>(key.ptr());
        int64_t buf_len = key.length();
        int64_t     pos = 0;

        for (int32_t index = 0; OB_SUCCESS == ret && index < size; ++index)
        {
          ObObj* obj = &array[index];
          int32_t len = obj->get_val_len();

          // last part could be variable length.
          if (0 == len && obj->get_type() == ObVarcharType && index == size - 1)
          {
            len = static_cast<int32_t>(buf_len - pos);
            obj->set_val_len(len);
          }

          if (pos + len > buf_len)
          {
            TBSYS_LOG(ERROR, "pos=%ld, current column=%d > buf_len=%ld",
                pos, len, buf_len);
            ret = OB_SIZE_OVERFLOW;
          }
          else if ( OB_SUCCESS != (ret = deserialize_obj(obj, buffer, buf_len, pos)) )
          {
            TBSYS_LOG(ERROR, "deserialize_obj failed, buffer=%p, buf_len=%ld, pos=%ld",
                buffer, buf_len, pos);
          }
        }
      }
      return ret;
    }



    int ObRowkeyHelper::serialize_obj(const ObObj* obj, char* buf, const int64_t buf_len, int64_t& pos)
    {
      int ret = OB_SUCCESS;
      ObObjType type = obj->get_type();
      switch (type)
      {
        case ObIntType:
          {
            int64_t value = 0;
            ret = obj->get_int(value);
            if (OB_SUCCESS == ret)
            {
              int32_t length = obj->get_val_len();
              switch (length)
              {
                case 1:
                  {
                    ret = serialization::encode_i8(buf, buf_len, pos, static_cast<int8_t>(value));
                    if (OB_SUCCESS != ret)
                    {
                      TBSYS_LOG(WARN, "serialization failed buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
                    }
                    break;
                  }
                case 2:
                  {
                    ret = serialization::encode_i16(buf, buf_len, pos, static_cast<int16_t>(value));
                    if (OB_SUCCESS != ret)
                    {
                      TBSYS_LOG(WARN, "serialization failed buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
                    }
                    break;
                  }
                case 4:
                  {
                    ret = serialization::encode_i32(buf, buf_len, pos, static_cast<int32_t>(value));
                    if (OB_SUCCESS != ret)
                    {
                      TBSYS_LOG(WARN, "serialization failed buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
                    }
                    break;
                  }
                case 8:
                  {
                    ret = serialization::encode_i64(buf, buf_len, pos, value);
                    if (OB_SUCCESS != ret)
                    {
                      TBSYS_LOG(WARN, "serialization failed buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
                    }
                    break;
                  }
                default:
                  TBSYS_LOG(WARN, "unsupport serialization length");
                  ret = OB_ERR_UNEXPECTED;
                  break;
              }
            }
            else
            {
              TBSYS_LOG(WARN, "Get int value failed");
            }
            break;
          }
        //add lijianqiang [INT_32] 20151008:b
        case ObInt32Type:
          {
            int32_t value = 0;
            ret = obj->get_int32(value);
            if (OB_SUCCESS == ret)
            {
              ret = serialization::encode_i32(buf, buf_len, pos, value);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "serialization failed buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
              }
            }
            else
            {
              TBSYS_LOG(WARN, "Get int32 value failed");
            }
            break;
          }
        //add 20151008:e
        case ObVarcharType:
          {
            ObString value;
            ret = obj->get_varchar(value);
            if (OB_SUCCESS == ret)
            {
              memcpy(buf+pos, value.ptr(), value.length());
              pos += value.length();
            }
            else
            {
              TBSYS_LOG(WARN, "Get varchar value failed");
            }
            break;
          }
        case ObDateTimeType:
          {
            ObDateTime value;
            ret = obj->get_datetime(value);
            if (OB_SUCCESS == ret)
            {
              ret = serialization::encode_i64(buf,buf_len,pos,value);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "serialization failed buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
              }
            }
            else
            {
              TBSYS_LOG(WARN, "Get datatime value failed");
            }
            break;
          }
        //add peiouya [DATE_TIME] 20150906:b
        case ObDateType:
          {
            ObDate value;
            ret = obj->get_date(value);
            if (OB_SUCCESS == ret)
            {
              ret = serialization::encode_i64(buf,buf_len,pos,value);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "serialization failed buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
              }
            }
            else
            {
              TBSYS_LOG(WARN, "Get date time value failed");
            }
            break;
          }
        case ObTimeType:
          {
            ObTime value;
            ret = obj->get_time(value);
            if (OB_SUCCESS == ret)
            {
              ret = serialization::encode_i64(buf,buf_len,pos,value);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "serialization failed buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
              }
            }
            else
            {
              TBSYS_LOG(WARN, "Get date time value failed");
            }
            break;
          }
        //add 20150906:e
        case ObPreciseDateTimeType:
          {
            ObPreciseDateTime value;
            ret = obj->get_precise_datetime(value);
            if (OB_SUCCESS == ret)
            {
              ret = serialization::encode_i64(buf,buf_len,pos,value);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "serialization failed buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
              }
            }
            else
            {
              TBSYS_LOG(WARN, "Get date time value failed");
            }
            break;
          }
        case ObModifyTimeType:
          {
            ObModifyTime value;
            ret = obj->get_modifytime(value);
            if (OB_SUCCESS == ret)
            {
              ret = serialization::encode_i64(buf,buf_len,pos,value);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "serialization failed buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
              }
            }
            else
            {
              TBSYS_LOG(WARN, "Get modify time value failed");
            }
            break;
          }
        case ObCreateTimeType:
          {
            ObCreateTime value;
            ret = obj->get_createtime(value);
            if (OB_SUCCESS == ret)
            {
              ret = serialization::encode_i64(buf,buf_len,pos,value);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "serialization failed buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
              }
            }
            else
            {
              TBSYS_LOG(WARN, "Get create time value failed");
            }
            break;
          }
        default:
          TBSYS_LOG(ERROR, "invalid obj_type=%d, rowkey does not support", type);
          ret = OB_ERR_UNEXPECTED;
          break;
      }
      return ret;
    }

    int ObRowkeyHelper::deserialize_obj(ObObj* obj, const char* buf, const int64_t buf_len, int64_t& pos)
    {
      int ret = OB_SUCCESS;
      int32_t length = obj->get_val_len();
      ObObjType type = obj->get_type();
      switch (type)
      {
        case ObIntType:
          {
            int64_t value = 0;
            switch (length)
            {
              case 1:
                {
                  int8_t invalue;
                  ret = serialization::decode_i8(buf, buf_len, pos, &invalue);
                  if (OB_SUCCESS == ret)
                  {
                    value = invalue;
                  }
                  else
                  {
                    TBSYS_LOG(WARN, "deserialization failed buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
                  }
                  break;
                }
              case 2:
                {
                  int16_t invalue;
                  ret = serialization::decode_i16(buf, buf_len, pos, &invalue);
                  if (OB_SUCCESS == ret)
                  {
                    value = invalue;
                  }
                  else
                  {
                    TBSYS_LOG(WARN, "deserialization failed buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
                  }
                  break;
                }
              case 4:
                {
                  int32_t invalue;
                  ret = serialization::decode_i32(buf, buf_len, pos, &invalue);
                  if (OB_SUCCESS == ret)
                  {
                    value = invalue;
                  }
                  else
                  {
                    TBSYS_LOG(WARN, "deserialization failed buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
                  }
                  break;
                }
              case 8:
                {
                  int64_t invalue;
                  ret = serialization::decode_i64(buf, buf_len, pos, &invalue);
                  if (OB_SUCCESS == ret)
                  {
                    value = invalue;
                  }
                  else
                  {
                    TBSYS_LOG(WARN, "deserialization failed buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
                  }
                  break;
                }
              default:
                TBSYS_LOG(WARN, "unsuported deserialize length");
                ret = OB_ERR_UNEXPECTED;
                break;
            }
            if (OB_SUCCESS == ret)
            {
              obj->set_int(value);          
            }
            break;
          }
        //add lijianqiang [INT_32] 20151008:b
        case ObInt32Type:
          {
            int32_t value = 0;
                  int32_t invalue;
                  ret = serialization::decode_i32(buf, buf_len, pos, &invalue);
                  if (OB_SUCCESS == ret)
                  {
                    value = invalue;
                  }
                  else
                  {
                    TBSYS_LOG(WARN, "deserialization failed buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
                  }
            if (OB_SUCCESS == ret)
            {
              obj->set_int32(value);
            }
            break;
          }
        //add 20151008:e
        case ObVarcharType:
          {
            ObString value;
            value.assign(const_cast<char*>(buf)+pos, length);
            pos += length;
            obj->set_varchar(value);
            break;
          }
        case ObDateTimeType:
          {
            ObDateTime value;
            ret = serialization::decode_i64(buf,buf_len,pos,&value);
            if (OB_SUCCESS == ret)
            {
              obj->set_datetime(value);
            }
            else
            {
              TBSYS_LOG(WARN, "deserialize failed buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
            }
            break;
          }
        //add peiouya [DATE_TIME] 20150906:b
        case ObDateType:
          {
            ObDate value;
            ret = serialization::decode_i64(buf,buf_len,pos,&value);
            if (OB_SUCCESS == ret)
            {
              obj->set_date(value);
            }
            else
            {
              TBSYS_LOG(WARN, "deserialize failed buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
            }
            break;
          }
        case ObTimeType:
          {
            ObTime value;
            ret = serialization::decode_i64(buf,buf_len,pos,&value);
            if (OB_SUCCESS == ret)
            {
              obj->set_time(value);
            }
            else
            {
              TBSYS_LOG(WARN, "deserialize failed buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
            }
            break;
          }
        //add 20150906:e
        case ObPreciseDateTimeType:
          {
            ObPreciseDateTime value;
            ret = serialization::decode_i64(buf,buf_len,pos,&value);
            if (OB_SUCCESS == ret)
            {
              obj->set_precise_datetime(value);
            }
            else
            {
              TBSYS_LOG(WARN, "deserialize failed buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
            }
            break;
          }
        case ObModifyTimeType:
          {
            ObModifyTime value;
            ret = serialization::decode_i64(buf,buf_len,pos, &value);
            if (OB_SUCCESS == ret)
            {
              obj->set_modifytime(value);
            }
            else
            {
              TBSYS_LOG(WARN, "deserialize failed buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
            }
            break;
          }
        case ObCreateTimeType:
          {
            ObCreateTime value;
            ret = serialization::decode_i64(buf,buf_len,pos,&value);
            if (OB_SUCCESS == ret)
            {
              obj->set_createtime(value);
            }
            else
            {
              TBSYS_LOG(WARN, "deserialize failed buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
            }
            break;
          }
        default:
          TBSYS_LOG(ERROR, "invalid obj_type=%d, rowkey does not support", type);
          ret = OB_ERR_UNEXPECTED;
          break;
      }
      return ret;
    }


    int ObRowkeyHelper::binary_rowkey_to_obj_array(const ObRowkeyInfo& info, 
        const ObString& binary_key, ObObj* array, const int64_t size)
    {
      int ret = OB_SUCCESS;
      if (NULL == binary_key.ptr() || 0 >= binary_key.length() 
          || NULL == array || size < info.get_size())
      {
        TBSYS_LOG(WARN, "invalid argument array = %p, size=%ld, info size=%ld", 
            array, size, info.get_size());
        ret = OB_ERROR;
      }
      else
      {
        char*    buffer = const_cast<char*>(binary_key.ptr());
        int64_t buf_len = binary_key.length();
        int64_t     pos = 0;
        ObRowkeyColumn column;

        for (int64_t index = 0; index < info.get_size() && OB_SUCCESS == ret; ++index)
        {
          ret = info.get_column(index, column);
          if (OB_SUCCESS == ret)
          {
            ObObj& obj = array[index];

            obj.set_type(static_cast<ObObjType>(column.type_));
            obj.set_val_len(static_cast<int32_t>(column.length_));

            // last part varchar object could be variable length 
            if (column.type_ == ObVarcharType && index == info.get_size() - 1)
            {
              obj.set_val_len(static_cast<int32_t>(buf_len - pos));
            }

            if (pos + obj.get_val_len() > buf_len)
            {
              TBSYS_LOG(ERROR, "pos=%ld, current column=%d > buf_len=%ld",
                  pos, obj.get_val_len(), buf_len);
              ret = OB_SIZE_OVERFLOW;
            }
            else if (obj.get_val_len() > column.length_)
            {
              TBSYS_LOG(ERROR, "pos=%ld, current column=%d > column length=%ld, rowkey_info:%s",
                  pos, obj.get_val_len(), column.length_, to_cstring(info));
              ret = OB_SIZE_OVERFLOW;
            }
            else if ( OB_SUCCESS != (ret = deserialize_obj(&obj, buffer, buf_len, pos)) )
            {
              TBSYS_LOG(ERROR, "deserialize_obj failed, buffer=%p, buf_len=%ld, pos=%ld",
                  buffer, buf_len, pos);
            }
          }
        }
      }

      return ret;
    }

    int ObRowkeyHelper::obj_array_to_binary_rowkey(const ObRowkeyInfo& info, 
        ObString& binary_key, const ObObj* array, const int64_t size)
    {
      int ret = OB_SUCCESS;
      if (NULL == array || size != info.get_size())
      {
        TBSYS_LOG(WARN, "invalid argument array = %p, size=%ld, info size=%ld", 
            array, size, info.get_size());
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        char*    buffer = binary_key.ptr();
        int64_t buf_len = binary_key.length();
        int64_t     pos = 0;
        ObRowkeyColumn column;
        for (int64_t index = 0; index < info.get_size() && OB_SUCCESS == ret; ++index)
        {
          ObObj & obj = const_cast<ObObj&>(array[index]);
          ret = info.get_column(index, column);
          if (OB_SUCCESS == ret)
          {
            if (column.length_ > 0 && column.type_ != ObVarcharType) 
            {
              obj.set_val_len(static_cast<int32_t>(column.length_));
            }

            ret = serialize_obj(&obj, buffer, buf_len, pos);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "serialize_obj failed, buffer=%p, buf_len=%ld, pos=%ld",
                  buffer, buf_len, pos);
            }
          }
        }

        if (OB_SUCCESS == ret)
        {
          binary_key.assign(buffer, static_cast<int32_t>(pos));
        }
      }

      return ret;
    }

  }
}
