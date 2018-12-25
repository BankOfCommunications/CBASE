/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_result.cpp
 *
 * Authors:
 *   Li Kai <yubai.lk@alipay.com>
 *
 */
#include "ob_ups_result.h"
#include "common/ob_define.h"
#include "common/serialization.h"

namespace oceanbase
{
  using namespace common;
  using namespace common::serialization;
  namespace sql
  {
    ObUpsResult::ObUpsResult() : allocator_(default_allocator_),
                                 error_code_(OB_SUCCESS),
                                 affected_rows_(0),
                                 warning_list_(NULL),
                                 warning_iter_(NULL),
                                 warning_count_(0),
                                 scanner_(),
                                 scanner_sel_buffer_(NULL),
                                 scanner_sel_length_(0)
    {
    }

    ObUpsResult::ObUpsResult(ObIAllocator &allocator) : allocator_(allocator),
                                                        error_code_(OB_SUCCESS),
                                                        affected_rows_(0),
                                                        warning_list_(NULL),
                                                        warning_iter_(NULL),
                                                        warning_count_(0),
                                                        scanner_(),
                                                        scanner_sel_buffer_(NULL),
                                                        scanner_sel_length_(0)
    {
    }

    ObUpsResult::~ObUpsResult()
    {
      clear();
    }

    void ObUpsResult::set_error_code(const int error_code)
    {
      error_code_ = error_code;
    }

    void ObUpsResult::set_affected_rows(const int64_t affected_rows)
    {
      affected_rows_ = affected_rows;
    }

    void ObUpsResult::add_warning_string(const char *buffer)
    {
      if (NULL != buffer)
      {
        int64_t length = strlen(buffer) + 1;
        StringNode *node = (StringNode*)allocator_.alloc(sizeof(StringNode) + length);
        if (NULL != node)
        {
          memcpy(node->buf, buffer, length);
          node->length = length;
          node->next = warning_list_;
          warning_list_ = node;
          warning_iter_ = warning_list_;
          warning_count_ += 1;
        }
      }
    }

    int ObUpsResult::set_scanner(const ObNewScanner &scanner)
    {
      int ret = OB_SUCCESS;
      char *buffer = (char*)allocator_.alloc(scanner.get_serialize_size());
      int64_t pos = 0;
      if (NULL == buffer)
      {
        TBSYS_LOG(WARN, "alloc memory fail");
        ret = OB_MEM_OVERFLOW;
      }
      else if (OB_SUCCESS != (ret = scanner.serialize(buffer, OB_MAX_PACKET_LENGTH, pos)))
      {
        TBSYS_LOG(WARN, "serialize scanner fail ret=%d buffer=%p len=%ld pos=%ld", ret, buffer, OB_MAX_PACKET_LENGTH, pos);
      }
      else
      {
        scanner_sel_buffer_ = buffer;
        scanner_sel_length_ = pos;
      }
      return ret;
    }

    int ObUpsResult::get_error_code() const
    {
      return error_code_;
    }

    int64_t ObUpsResult::get_affected_rows() const
    {
      return affected_rows_;
    }

    int64_t ObUpsResult::get_warning_count() const
    {
      return warning_count_;
    }

    const char *ObUpsResult::get_next_warning()
    {
      const char *ret = NULL;
      if (NULL != warning_iter_)
      {
        ret = warning_iter_->buf;
        warning_iter_ = warning_iter_->next;
      }
      return ret;
    }

    void ObUpsResult::reset_iter_warning()
    {
      warning_iter_ = warning_list_;
    }

    ObNewScanner &ObUpsResult::get_scanner()
    {
      return scanner_;
    }

    void ObUpsResult::clear()
    {
      StringNode *iter = warning_list_;
      while (NULL != iter)
      {
        StringNode *tmp = iter->next;
        allocator_.free(iter);
        iter = tmp;
      }
      if (NULL != scanner_sel_buffer_)
      {
        allocator_.free(scanner_sel_buffer_);
        scanner_sel_buffer_ = NULL;
      }
      scanner_sel_length_ = 0;
      default_allocator_.reuse();
      error_code_ = OB_SUCCESS;
      affected_rows_ = 0;
      warning_list_ = NULL;
      warning_iter_ = NULL;
      warning_count_ = 0;
      scanner_.reuse(); // TODO wait scanner support set_allocator
      scanner_sel_buffer_ = 0;
      scanner_sel_length_ = 0;
      trans_id_.reset();
    }

    int ObUpsResult::serialize_warning_list_(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = encode_i64(buf, buf_len, pos, warning_count_)))
      {
        TBSYS_LOG(WARN, "serialize warning_count_ fail, ret=%d buf=%p buf_len=%ld pos=%ld",
                  ret, buf, buf_len, pos);
      }
      const StringNode *iter = warning_list_;
      while (NULL != iter)
      {
        if (OB_SUCCESS != (ret = encode_i64(buf, buf_len, pos, iter->length)))
        {
          TBSYS_LOG(WARN, "serialize length fail, ret=%d buf=%p buf_len=%ld pos=%ld",
                    ret, buf, buf_len, pos);
          break;
        }
        if (buf_len < (pos + iter->length))
        {
          TBSYS_LOG(WARN, "serialize warning string fail, buf=%p buf_len=%ld pos=%ld length=%ld",
                    buf, buf_len, pos, iter->length);
          ret = OB_BUF_NOT_ENOUGH;
          break;
        }
        memcpy(buf + pos, iter->buf, iter->length);
        pos += iter->length;
        iter = iter->next;
      }
      return ret;
    }

    int ObUpsResult::deserialize_warning_list_(const char* buf, const int64_t data_len, int64_t& pos)
    {
      int ret = OB_SUCCESS;
      int64_t warning_count = 0;
      if (OB_SUCCESS != (ret = decode_i64(buf, data_len, pos, &warning_count)))
      {
        TBSYS_LOG(WARN, "deserialize warning_count fail, ret=%d buf=%p data_len=%ld pos=%ld",
                  ret, buf, data_len, pos);
      }
      for (int64_t i = 0; i < warning_count; i++)
      {
        int64_t length = 0;
        if (OB_SUCCESS != (ret = decode_i64(buf, data_len, pos, &length)))
        {
          TBSYS_LOG(WARN, "deserialize length fail, ret=%d buf=%p data_len=%ld pos=%ld",
                    ret, buf, data_len, pos);
          break;
        }
        if (data_len < (pos + length))
        {
          TBSYS_LOG(WARN, "deserialize warning string fail, buf=%p data_len=%ld pos=%ld length=%ld",
                    buf, data_len, pos, length);
          ret = OB_BUF_NOT_ENOUGH;
          break;
        }
        add_warning_string(buf + pos);
        pos += length;
      }
      return ret;
    }

    DEFINE_SERIALIZE(ObUpsResult)
    {
      int ret = OB_SUCCESS;
      if (NULL == buf
          || 0 >= (buf_len - pos))
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p buf_len=%ld pos=%ld",
                  buf, buf_len, pos);
        ret = OB_INVALID_ARGUMENT;
      }
      int64_t tmp_pos = pos;
      while (OB_SUCCESS == ret)
      {
        // serialize head flag
        if (OB_SUCCESS != (ret = encode_i64(buf, buf_len, tmp_pos, F_UPS_RESULT_START)))
        {
          TBSYS_LOG(WARN, "serialize F_UPS_RESULT_START fail, ret=%d buf=%p buf_len=%ld pos=%ld",
                    ret, buf, buf_len, tmp_pos);
          break;
        }

        // serialize error code flag
        if (OB_SUCCESS != (ret = encode_i8(buf, buf_len, tmp_pos, F_ERROR_CODE)))
        {
          TBSYS_LOG(WARN, "serialize F_ERROR_CODE fail, ret=%d buf=%p buf_len=%ld pos=%ld",
                    ret, buf, buf_len, tmp_pos);
          break;
        }
        // serialize error code value
        if (OB_SUCCESS != (ret = encode_i32(buf, buf_len, tmp_pos, error_code_)))
        {
          TBSYS_LOG(WARN, "serialize error_code_ fail, ret=%d buf=%p buf_len=%ld pos=%ld",
                    ret, buf, buf_len, tmp_pos);
          break;
        }

        // serialize affected rows flag
        if (OB_SUCCESS != (ret = encode_i8(buf, buf_len, tmp_pos, F_AFFECTED_ROWS)))
        {
          TBSYS_LOG(WARN, "serialize F_AFFECTED_ROWS fail, ret=%d buf=%p buf_len=%ld pos=%ld",
                    ret, buf, buf_len, tmp_pos);
          break;
        }
        // serialize affected rows value
        if (OB_SUCCESS != (ret = encode_i64(buf, buf_len, tmp_pos, affected_rows_)))
        {
          TBSYS_LOG(WARN, "serialize affected_rows_ fail, ret=%d buf=%p buf_len=%ld pos=%ld",
                    ret, buf, buf_len, tmp_pos);
          break;
        }

        // serialize warning list flag
        if (OB_SUCCESS != (ret = encode_i8(buf, buf_len, tmp_pos, F_WARNING_LIST)))
        {
          TBSYS_LOG(WARN, "serialize F_WARNING_LIST fail, ret=%d buf=%p buf_len=%ld pos=%ld",
                    ret, buf, buf_len, tmp_pos);
          break;
        }
        // serialize warning list value
        if (OB_SUCCESS != (ret = serialize_warning_list_(buf, buf_len, tmp_pos)))
        {
          TBSYS_LOG(WARN, "serialize affected_rows_ fail, ret=%d buf=%p buf_len=%ld pos=%ld",
                    ret, buf, buf_len, tmp_pos);
          break;
        }

        // serialize new scanner flag
        if (OB_SUCCESS != (ret = encode_i8(buf, buf_len, tmp_pos, F_NEW_SCANNER)))
        {
          TBSYS_LOG(WARN, "serialize F_NEW_SCANNER fail, ret=%d buf=%p buf_len=%ld pos=%ld",
                    ret, buf, buf_len, tmp_pos);
          break;
        }
        // serialize new scanner value
        if (NULL != scanner_sel_buffer_)
        {
          if (buf_len < (tmp_pos + scanner_sel_length_))
          {
            TBSYS_LOG(WARN, "serialize scanner_sel_buffer=%p scanner_sel_length=%ld fail, buf=%p buf_len=%ld pos=%ld",
                      scanner_sel_buffer_, scanner_sel_length_, buf, buf_len, tmp_pos);
            ret = OB_BUF_NOT_ENOUGH;
            break;
          }
          else
          {
            memcpy(buf + tmp_pos, scanner_sel_buffer_, scanner_sel_length_);
            tmp_pos += scanner_sel_length_;
          }
        }
        else
        {
          if (OB_SUCCESS != (ret = scanner_.serialize(buf, buf_len, tmp_pos)))
          {
            TBSYS_LOG(WARN, "serialize scanner_ fail, ret=%d buf=%p buf_len=%ld pos=%ld",
                      ret, buf, buf_len, tmp_pos);
            break;
          }
        }

        // serialize transaction id flag
        if (OB_SUCCESS != (ret = encode_i8(buf, buf_len, tmp_pos, F_TRANS_ID)))
        {
          TBSYS_LOG(WARN, "serialize F_TRANS_ID fail, ret=%d buf=%p buf_len=%ld pos=%ld",
                    ret, buf, buf_len, tmp_pos);
          break;
        }

        // serialize transaction id
        if (OB_SUCCESS != (ret = trans_id_.serialize(buf, buf_len, tmp_pos)))
        {
          TBSYS_LOG(WARN, "serialize affected_rows_ fail, ret=%d buf=%p buf_len=%ld pos=%ld",
                    ret, buf, buf_len, tmp_pos);
          break;
        }

        // serialize end flag
        if (OB_SUCCESS != (ret = encode_i8(buf, buf_len, tmp_pos, F_UPS_RESULT_END)))
        {
          TBSYS_LOG(WARN, "serialize F_UPS_RESULT_END fail, ret=%d buf=%p buf_len=%ld pos=%ld",
                    ret, buf, buf_len, tmp_pos);
          break;
        }

        pos = tmp_pos;
        break;
      }
      return ret;
    }

    DEFINE_DESERIALIZE(ObUpsResult)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;
      uint64_t start_flag = 0;
      clear();
      if (NULL == buf
          || 0 >= (data_len - pos))
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p data_len=%ld pos=%ld",
                  buf, data_len, pos);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = decode_i64(buf, data_len, tmp_pos, (int64_t*)&start_flag)))
      {
        TBSYS_LOG(WARN, "deserialize start_flag fail, ret=%d buf=%p data_len=%ld pos=%ld",
                  ret, buf, data_len, tmp_pos);
      }
      else if (F_UPS_RESULT_START != start_flag)
      {
        TBSYS_LOG(WARN, "start_flag=%lx do not mactch %lx, maybe this is not ObUpsResult",
                  start_flag, F_UPS_RESULT_START);
        ret = OB_INVALID_ARGUMENT;
      }
      while (OB_SUCCESS == ret)
      {
        uint8_t flag = 0;
        if (OB_SUCCESS != (ret = decode_i8(buf, data_len, tmp_pos, (int8_t*)&flag)))
        {
          TBSYS_LOG(WARN, "deserialize flag fail, ret=%d buf=%p data_len=%ld pos=%ld",
                    ret, buf, data_len, tmp_pos);
          break;
        }
        if (F_UPS_RESULT_END == flag)
        {
          pos = tmp_pos;
          break;
        }
        switch (flag)
        {
        case F_ERROR_CODE:
          if (OB_SUCCESS != (ret = decode_i32(buf, data_len, tmp_pos, &error_code_)))
          {
            TBSYS_LOG(WARN, "deserialize error_code_ fail, ret=%d buf=%p data_len=%ld pos=%ld",
                      ret, buf, data_len, tmp_pos);
          }
          break;
        case F_AFFECTED_ROWS:
          if (OB_SUCCESS != (ret = decode_i64(buf, data_len, tmp_pos, &affected_rows_)))
          {
            TBSYS_LOG(WARN, "deserialize affected_rows_ fail, ret=%d buf=%p data_len=%ld pos=%ld",
                      ret, buf, data_len, tmp_pos);
          }
          break;
        case F_WARNING_LIST:
          if (OB_SUCCESS != (ret = deserialize_warning_list_(buf, data_len, tmp_pos)))
          {
            TBSYS_LOG(WARN, "deserialize warning_list_ fail, ret=%d buf=%p data_len=%ld pos=%ld",
                      ret, buf, data_len, tmp_pos);
          }
          break;
        case F_NEW_SCANNER:
          if (OB_SUCCESS != (ret = scanner_.deserialize(buf, data_len, tmp_pos)))
          {
            TBSYS_LOG(WARN, "deserialize scanner_ fail, ret=%d buf=%p data_len=%ld pos=%ld",
                      ret, buf, data_len, tmp_pos);
          }
          break;
        case F_TRANS_ID:
          if (OB_SUCCESS != (ret = trans_id_.deserialize(buf, data_len, tmp_pos)))
          {
            TBSYS_LOG(WARN, "deserialize trans_id_ fail, ret=%d buf=%p data_len=%ld pos=%ld",
                      ret, buf, data_len, tmp_pos);
          }
          break;
        default:
          // unknow flag
          break;
        }
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObUpsResult)
    {
      int64_t ret = 0;
      ret += encoded_length_i64(F_UPS_RESULT_START);

      ret += encoded_length_i8(F_ERROR_CODE);
      ret += encoded_length_i32(error_code_);

      ret += encoded_length_i8(F_AFFECTED_ROWS);
      ret += encoded_length_i64(affected_rows_);

      ret += encoded_length_i8(F_WARNING_LIST);
      ret += encoded_length_i64(warning_count_);
      StringNode *iter = warning_list_;
      while (NULL != iter)
      {
        ret += encoded_length_i64(iter->length);
        ret += iter->length;
        iter = iter->next;
      }

      ret += encoded_length_i8(F_NEW_SCANNER);
      if (NULL != scanner_sel_buffer_)
      {
        ret += scanner_sel_length_;
      }
      else
      {
        ret += scanner_.get_serialize_size();
      }

      ret += encoded_length_i8(F_TRANS_ID);
      ret += trans_id_.get_serialize_size();

      ret += encoded_length_i8(F_UPS_RESULT_END);
      return ret;
    }
  } // end namespace sql
} // end namespace oceanbase
