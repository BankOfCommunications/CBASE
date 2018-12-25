/*
 *   (C) 2007-2010 Taobao Inc.
 *   
 *   This program is free software; you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License version 2 as
 *   published by the Free Software Foundation.
 *       
 *         
 *         
 *   Version: 0.1
 *           
 *   Authors:
 *      qushan <qushan@taobao.com>
 *        - some work details if you want
 *               
 */

#include "ob_range.h"
#include <tbsys.h>
#include <string.h>
#include "utility.h"
#include <ob_string_buf.h>

namespace oceanbase 
{ 
  namespace common 
  {


  //add zhaoqiong [Truncate Table]:20160318:b
  DEFINE_SERIALIZE(ObBorderFlag)
  {
    int ret = OB_SUCCESS;
    ret = serialization::encode_i8(buf, buf_len, pos, data_);
    return ret;
  }

  DEFINE_DESERIALIZE(ObBorderFlag)
  {

    return serialization::decode_i8(buf, data_len, pos, &data_);
  }

  DEFINE_GET_SERIALIZE_SIZE(ObBorderFlag)
  {
    int64_t total_size = 0;
    total_size += serialization::encoded_length_i8(data_);
    return total_size;
  }


  DEFINE_GET_SERIALIZE_SIZE(ObVersion)
  {
    int64_t total_size = 0;
    total_size += serialization::encoded_length_i64(version_);
    return total_size;
  }

  DEFINE_SERIALIZE(ObVersion)
  {
    return serialization::encode_vi64(buf, buf_len, pos, version_);
  }

  DEFINE_DESERIALIZE(ObVersion)
  {
    return serialization::decode_vi64(buf, data_len, pos, &version_);
  }

  DEFINE_GET_SERIALIZE_SIZE(ObVersionRange)
  {
    int64_t total_size = 0;
    total_size += border_flag_.get_serialize_size();
    total_size += start_version_.get_serialize_size();
    total_size += end_version_.get_serialize_size();
    return total_size;
  }

  DEFINE_SERIALIZE(ObVersionRange)
  {
    int ret = OB_SUCCESS;

    int64_t serialize_size = get_serialize_size();

    if((NULL == buf) || (serialize_size + pos > buf_len))
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "buffer size is not enough, ret=[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = border_flag_.serialize(buf, buf_len, pos)))
    {
      TBSYS_LOG(ERROR, "serialize border_flag failed, ret=[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = start_version_.serialize(buf, buf_len, pos)))
    {
      TBSYS_LOG(ERROR, "serialize start_version failed, ret=[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = end_version_.serialize(buf, buf_len, pos)))
    {
      TBSYS_LOG(ERROR, "serialize end_version failed, ret=[%d]", ret);
    }
    return ret;
  }

  DEFINE_DESERIALIZE(ObVersionRange)
  {
    int ret = OB_SUCCESS;


    if (OB_SUCCESS != (ret = border_flag_.deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(ERROR, "deserialize border_flag failed");
    }
    else if (OB_SUCCESS != (ret = start_version_.deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(ERROR, "deserialize start_version failed");
    }
    else if (OB_SUCCESS != (ret = end_version_.deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(ERROR, "deserialize end_version failed");
    }
    return ret;
  }
  //add:e
#if 0
    // --------------------------------------------------------
    // class ObRange implements
    // --------------------------------------------------------
    
    bool ObRange::check(void) const
    {
      bool ret = true;
      if ((start_key_ > end_key_) && (!border_flag_.is_max_value()) && (!border_flag_.is_min_value()))
      {
        TBSYS_LOG(WARN, "%s", "check start key gt than end key");
        ret = false;
      }
      else if (start_key_ == end_key_)
      {
        if (!border_flag_.is_min_value() && !border_flag_.is_max_value())
        {
          if (start_key_.length() == 0 || !border_flag_.inclusive_start() || !border_flag_.inclusive_end())
          {
            TBSYS_LOG(WARN, "check border flag or length failed:length[%d], flag[%u]",
                start_key_.length(), border_flag_.get_data());
            ret = false;
          }
        }
      }
      return ret;
    }

    bool ObRange::equal(const ObRange& r) const 
    {
      return (compare_with_startkey(r) == 0) && (compare_with_endkey(r) == 0);
    }

    bool ObRange::intersect(const ObRange& r) const
    {
      // suppose range.start_key_ <= range.end_key_
      if (table_id_ != r.table_id_) return false;
      if (empty() || r.empty()) return false;
      if (is_whole_range() || r.is_whole_range()) return true;

      ObString lkey, rkey;
      bool ret = false;
      int8_t include_lborder = 0;
      int8_t include_rborder = 0;
      bool min_value = false;
      int cmp = compare_with_endkey(r);
      if (cmp < 0)
      {
        lkey = end_key_;
        rkey = r.start_key_;
        include_lborder = (border_flag_.inclusive_end());
        include_rborder = (r.border_flag_.inclusive_start());
        min_value = (r.border_flag_.is_min_value());
      }
      else if (cmp > 0)
      {
        lkey = r.end_key_;
        rkey = start_key_;
        include_lborder = (r.border_flag_.inclusive_end());
        include_rborder = (border_flag_.inclusive_start());
        min_value = (border_flag_.is_min_value());
      }
      else
      {
        ret = true;
      }

      if (cmp != 0) 
      {
        if (min_value) ret = true;
        else if (lkey < rkey) ret = false;
        else if (lkey > rkey) ret = true;
        else ret = (include_lborder != 0 && include_rborder != 0);
      }

      return ret;
    }

    void ObRange::dump() const
    {
      // TODO, used in ObString is a c string end with '\0'
      // just for test.
      const int32_t MAX_LEN = OB_MAX_ROW_KEY_LENGTH;
      char sbuf[MAX_LEN];
      char ebuf[MAX_LEN];
      memcpy(sbuf, start_key_.ptr(), start_key_.length());
      sbuf[start_key_.length()] = 0;
      memcpy(ebuf, end_key_.ptr(), end_key_.length());
      ebuf[end_key_.length()] = 0;
      TBSYS_LOG(DEBUG, 
          "table:%ld, border flag:%d, start key:%s(%d), end key:%s(%d)\n",
          table_id_, border_flag_.get_data(), sbuf,  start_key_.length(), ebuf, end_key_.length());
    }

    void ObRange::hex_dump(const int32_t log_level) const
    {
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level), 
          "table:%ld, border flag:%d\n", table_id_, border_flag_.get_data());
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level), 
          "dump start key with hex format, length(%d)", start_key_.length());
      common::hex_dump(start_key_.ptr(), start_key_.length(), true, log_level);
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level), 
          "dump end   key with hex format, length(%d)", end_key_.length());
      common::hex_dump(end_key_.ptr(), end_key_.length(), true, log_level);
    }

    int ObRange::to_string(char* buffer, const int32_t length) const
    {
      int ret = OB_SUCCESS;
      if (NULL == buffer || length <= 0) 
      {
        ret = OB_INVALID_ARGUMENT;
      }

      int32_t key_length = 0;
      int32_t pos = 0;
      int32_t remain_length = 0;
      int32_t byte_len = 0;
      int32_t max_key_length = 0;

      // calc print key length;
      if (border_flag_.is_min_value() && border_flag_.is_max_value())
      {
        key_length += 6; // MIN, MAX
      }
      else if (border_flag_.is_min_value())
      {
        key_length += 3 + 2 * end_key_.length(); // MIN, XXX
      }
      else if (border_flag_.is_max_value())
      {
        key_length += 3 + 2 * start_key_.length(); // XXX, MAX
      }
      else
      {
        key_length += 2 * end_key_.length() + 2 * start_key_.length(); // XXX, MAX
      }

      key_length += 3; // (,)

      // print table:xxx
      if (OB_SUCCESS == ret)
      {
        pos = snprintf(buffer, length, "table:%ld, ", table_id_);
        if (pos >= length)
        {
          ret = OB_SIZE_OVERFLOW;
        }
        else
        {
          remain_length = length - pos;
        }
      }

      if (OB_SUCCESS == ret)
      {

        const char* lb = 0;
        if (border_flag_.inclusive_start()) lb = "[";
        else lb = "(";
        if (border_flag_.is_min_value()) lb = "(MIN";

        byte_len = snprintf(buffer + pos, remain_length, "%s", lb);
        pos += byte_len;
        remain_length = length - pos;

        // add start_key_
        if (!border_flag_.is_min_value())
        {
          // buffer not enough, store part of start key, start= "key"|".."|",]\0"
          max_key_length = (remain_length < key_length) ? ((remain_length - 3) / 2 - 2) / 2 : start_key_.length();
          byte_len = hex_to_str(start_key_.ptr(), max_key_length, buffer + pos, remain_length);
          pos += byte_len * 2;
          if (remain_length < key_length)
          {
            buffer[pos++] = '.';
            buffer[pos++] = '.';
          }
        }

        // add ,
        buffer[pos++] = ',';
        remain_length = length - pos;

        // add end_key_
        if (!border_flag_.is_max_value())
        {
          // buffer not enough, store part of end key, end key = "key"|".."|"]\0"
          max_key_length = (remain_length < end_key_.length() * 2 + 2) ? (remain_length - 4) / 2 : end_key_.length();
          int byte_len = hex_to_str(end_key_.ptr(), max_key_length, buffer + pos, remain_length);
          pos += byte_len * 2;
          if (remain_length < end_key_.length() * 2 + 2)
          {
            buffer[pos++] = '.';
            buffer[pos++] = '.';
          }
        }

        const char* rb = 0;
        if (border_flag_.inclusive_end()) rb = "]";
        else rb = ")";
        if (border_flag_.is_max_value()) rb = "MAX)";
        snprintf(buffer + pos, length - pos, "%s", rb);

      }

      return ret;
    }

    int ObRange::trim(const ObRange& r, ObStringBuf & string_buf)
    {
      int ret = OB_SUCCESS;
      if(r.table_id_ != table_id_)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "table id is not equal table_id_[%ld] r.table_id_[%ld]",
          table_id_, r.table_id_);
      }

      if(OB_SUCCESS == ret)
      {
        if( 0 == compare_with_startkey2(r) ) // left interval equal
        {
          if( 0 == compare_with_endkey2(r) ) // both interval equal
          {
            ret = OB_EMPTY_RANGE;
          }
          else
          {
            if( compare_with_endkey2(r) > 0 ) // right interval great
            {
              border_flag_.unset_min_value();
              if(r.border_flag_.inclusive_end())
              {
                border_flag_.unset_inclusive_start();
              }
              else
              {
                border_flag_.set_inclusive_start();
              }
              ret = string_buf.write_string(r.end_key_, &start_key_);
              if(OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "write start key fail:ret[%d]", ret);
              }
            }
            else
            {
              ret = OB_ERROR;
              TBSYS_LOG(DEBUG, "dump this range");
              dump();
              TBSYS_LOG(DEBUG, "dump r range");
              r.dump();
              TBSYS_LOG(WARN, "r should be included by this range");
            }
          }
        }
        else if( 0 == compare_with_endkey2(r) ) // right interval equal
        {
          if( compare_with_startkey2(r) < 0 ) // left interval less
          {
            border_flag_.unset_max_value();
            if(r.border_flag_.inclusive_start())
            {
              border_flag_.unset_inclusive_end();
            }
            else
            {
              border_flag_.set_inclusive_end();
            }
            ret = string_buf.write_string(r.start_key_, &end_key_);
            if(OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "write end key fail:ret[%d]", ret);
            }
          }
          else
          {
            ret = OB_ERROR;
            
            TBSYS_LOG(DEBUG, "dump this range");
            dump();
            TBSYS_LOG(DEBUG, "dump r range");
            r.dump();
            TBSYS_LOG(WARN, "r should be included by this range");
          }
        }
        else
        {
          ret = OB_ERROR;
          
          TBSYS_LOG(DEBUG, "dump this range");
          dump();
          TBSYS_LOG(DEBUG, "dump r range");
          r.dump();
          TBSYS_LOG(WARN, "r should be included by this range");
        }
      }

      return ret;
    }

    int64_t ObRange::hash() const
    {
      uint32_t hash_val = 0;
      int8_t flag = border_flag_.get_data();

      /**
       * if it's max value, the border flag maybe is right close, 
       * ignore it. 
       */
      if (border_flag_.is_max_value())
      {
        flag = ObBorderFlag::MAX_VALUE;
      }
      hash_val = murmurhash2(&table_id_, sizeof(uint64_t), 0);
      hash_val = murmurhash2(&flag, sizeof(int8_t), hash_val);
      if (!border_flag_.is_min_value() && NULL != start_key_.ptr() 
          && start_key_.length() > 0)
      {
        hash_val = murmurhash2(start_key_.ptr(), start_key_.length(), hash_val);
      }
      if (!border_flag_.is_max_value() && NULL != end_key_.ptr() 
          && end_key_.length() > 0)
      {
        hash_val = murmurhash2(end_key_.ptr(), end_key_.length(), hash_val);
      }

      return hash_val;
    };

    DEFINE_SERIALIZE(ObRange)
    {
      int ret = OB_ERROR;
      ret = serialization::encode_vi64(buf, buf_len, pos, static_cast<int64_t>(table_id_));

      if (ret == OB_SUCCESS)
        ret = serialization::encode_i8(buf, buf_len, pos, border_flag_.get_data());

      if (ret == OB_SUCCESS)
        ret = start_key_.serialize(buf, buf_len, pos);

      if (ret == OB_SUCCESS)
        ret = end_key_.serialize(buf, buf_len, pos);

      return ret;
    }

    DEFINE_DESERIALIZE(ObRange)
    {
      int ret = OB_ERROR;
      ret = serialization::decode_vi64(buf, data_len, pos, (int64_t*)&table_id_);

      if (OB_SUCCESS == ret)
      {
        int8_t flag = 0;
        ret = serialization::decode_i8(buf, data_len, pos, &flag);
        if (OB_SUCCESS == ret)
        {
          border_flag_.set_data(flag);
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = start_key_.deserialize(buf, data_len, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to deserialize start key, buf=%p, data_len=%ld, pos=%ld, ret=%d",
              buf, data_len, pos, ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = end_key_.deserialize(buf, data_len, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to deserialize end key, buf=%p, data_len=%ld, pos=%ld, ret=%d",
              buf, data_len, pos, ret);
        }
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObRange)
    {
      int64_t total_size = 0;

      total_size += serialization::encoded_length_vi64(table_id_);
      total_size += serialization::encoded_length_i8(border_flag_.get_data());

      total_size += start_key_.get_serialize_size();
      total_size += end_key_.get_serialize_size();

      return total_size;
    }
#endif

  } // end namespace common
} // end namespace oceanbase

