/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 *         ob_range2.cpp is for what ...
 *
 *  Version: $Id: ob_range2.cpp 03/27/2012 10:45:11 AM qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#include "utility.h"
#include "ob_range2.h"

namespace oceanbase
{
  namespace common
  {

        bool ObNewRange::intersect(const ObNewRange& r) const
        {
          if (table_id_ != r.table_id_) return false;
          if (empty() || r.empty()) return false;
          if (is_whole_range() || r.is_whole_range()) return true;

          ObRowkey lkey, rkey;
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
            min_value = (r.start_key_.is_min_row());
          }
          else if (cmp > 0)
          {
            lkey = r.end_key_;
            rkey = start_key_;
            include_lborder = (r.border_flag_.inclusive_end());
            include_rborder = (border_flag_.inclusive_start());
            min_value = (start_key_.is_min_row());
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

        int ObNewRange::trim(const ObNewRange& r, ObStringBuf & string_buf)
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
                  TBSYS_LOG(WARN, "r should be included by this range");
                }
              }
            }
            else if( 0 == compare_with_endkey2(r) ) // right interval equal
            {
              if( compare_with_startkey2(r) < 0 ) // left interval less
              {
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

                TBSYS_LOG(WARN, "r should be included by this range");
              }
            }
            else
            {
              ret = OB_ERROR;

              TBSYS_LOG(WARN, "r should be included by this range");
            }
          }

          return ret;
        }

        bool ObNewRange::check(void) const
        {
          bool ret = true;
          if ((start_key_ > end_key_) && (!end_key_.is_max_row()) && (!start_key_.is_min_row()))
          {
            TBSYS_LOG(WARN, "check start key gt than end key, start_key=%s, end_key=%s",
                to_cstring(start_key_), to_cstring(end_key_));
            ret = false;
          }
          else if (start_key_ == end_key_)
          {
            if (!start_key_.is_min_row() && !end_key_.is_max_row())
            {
              if (start_key_.length() == 0 || !border_flag_.inclusive_start() || !border_flag_.inclusive_end())
              {
                TBSYS_LOG(WARN, "check border flag or length failed:length[%ld], flag[%u]",
                    start_key_.length(), border_flag_.get_data());
                ret = false;
              }
            }
          }
          return ret;
        }

        void ObNewRange::hex_dump(const int32_t log_level ) const
        {
          TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
              "table:%ld, border flag:%d\n", table_id_, border_flag_.get_data());
          TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
              "dump start key with hex format, length(%ld)", start_key_.length());
          start_key_.dump(log_level);
          TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
              "dump end   key with hex format, length(%ld)", end_key_.length());
          end_key_.dump(log_level);
        }

        int64_t ObNewRange::to_string(char* buffer, const int64_t length) const
        {
          int64_t pos = 0;

          if (pos < length)
          {
            databuff_printf(buffer, length, pos, "table_id:%lu,%s%s; %s%s",
                table_id_, border_flag_.inclusive_start() ? "[":"(", to_cstring(start_key_), to_cstring(end_key_), border_flag_.inclusive_end() ? "]":")");
          }

          return pos;
        }

        int64_t ObNewRange::hash() const
        {
          uint32_t hash_val = 0;
          int8_t flag = border_flag_.get_data();

          /**
           * if it's max value, the border flag maybe is right close,
           * ignore it.
           */
          if (end_key_.is_max_row())
          {
            flag = ObBorderFlag::MAX_VALUE;
          }
          hash_val = murmurhash2(&table_id_, sizeof(uint64_t), 0);
          hash_val = murmurhash2(&flag, sizeof(int8_t), hash_val);
          if (NULL != start_key_.ptr()
              && start_key_.length() > 0)
          {
            hash_val = start_key_.murmurhash2(hash_val);
          }
          if (NULL != end_key_.ptr()
              && end_key_.length() > 0)
          {
            hash_val = end_key_.murmurhash2(hash_val);
          }

          return hash_val;
        };

        int ObNewRange::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
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

        int ObNewRange::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
        {
          int ret = OB_SUCCESS;

          if (NULL == start_key_.get_obj_ptr() || NULL == end_key_.get_obj_ptr())
          {
            TBSYS_LOG(WARN, "start_key or end_key ptr is NULL, start_key=%s, end_key=%s",
                to_cstring(start_key_), to_cstring(end_key_));
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }

          if (OB_SUCCESS == ret)
          {
            ret = serialization::decode_vi64(buf, data_len, pos, (int64_t*)&table_id_);
          }

          if (OB_SUCCESS == ret)
          {
            int8_t flag = 0;
            ret = serialization::decode_i8(buf, data_len, pos, &flag);
            if (OB_SUCCESS == ret)
              border_flag_.set_data(flag);
          }

          if (OB_SUCCESS == ret)
          {
            ret = start_key_.deserialize(buf, data_len, pos);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to deserialize start key, ret=%d, buf=%p, data_len=%ld, pos=%ld",
                  ret, buf, data_len, pos);
            }
          }

          if (OB_SUCCESS == ret)
          {
            ret = end_key_.deserialize(buf, data_len, pos);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to deserialize end key, ret=%d, buf=%p, data_len=%ld, pos=%ld",
                  ret, buf, data_len, pos);
            }
          }

          return ret;
        }

        int64_t ObNewRange::get_serialize_size(void) const
        {
          int64_t total_size = 0;

          total_size += serialization::encoded_length_vi64(table_id_);
          total_size += serialization::encoded_length_i8(border_flag_.get_data());

          total_size += start_key_.get_serialize_size();
          total_size += end_key_.get_serialize_size();

          return total_size;
        }

  }
}
