/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_syschecker_rule.cpp for define syschecker test rule.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "common/ob_malloc.h"
#include "common/ob_atomic.h"
#include "common/murmur_hash.h"
#include "common/file_utils.h"
#include "common/file_directory_utils.h"
#include "ob_syschecker_rule.h"

namespace oceanbase
{
  namespace syschecker
  {
    using namespace common;
    using namespace serialization;

    const char* ObSyscheckerRule::ALPHA_NUMERICS =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz.-";

    //prime number in 1000
    const int64_t ObSyscheckerRule::PRIME_NUM[] =
    {
      2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,73,79,83,89,97,
      101,103,107,109,113,127,131,137,139,149,151,157,163,167,173,179,181,191,193,197,199,
      211,223,227,229,233,239,241,251,257,263,269,271,277,281,283,293,
      307,311,313,317,331,337,347,349,353,359,367,373,379,383,389,397,
      401,409,419,421,431,433,439,443,449,457,461,463,467,479,487,491,499,
      503,509,521,523,541,547,557,563,569,571,577,587,593,599,
      601,607,613,617,619,631,641,643,647,653,659,661,673,677,683,691,
      701,709,719,727,733,739,743,751,757,761,769,773,787,797,
      809,811,821,823,827,829,839,853,857,859,863,877,881,883,887,
      907,911,919,929,937,941,947,953,967,971,977,983,991,997
    };

    const char* READ_PARAM_FILE = "read_param_file.bin";
    const char* WRITE_PARAM_FILE = "write_param_file.bin";

    const char* OP_TYPE_STR[] =
    {
      "OP_NULL",
      "OP_ADD",
      "OP_UPDATE",
      "OP_INSERT",
      "OP_DELETE",
      "OP_MIX",
      "OP_GET",
      "OP_SCAN",
      "OP_SQL_GET",
      "OP_SQL_SCAN",
      "OP_MAX"
    };

    const char* OP_GEN_MODE_STR[] =
    {
      "GEN_NULL",
      "GEN_RANDOM",
      "GEN_RANDOM",
      "GEN_SEQ",
      "GEN_COMBO_RANDOM",
      "GEN_SPECIFIED",
      "GEN_NEW_KEY",
      "GEN_VALID_WRITE",
      "GEN_INVALID_WRITE",
      "GEN_VALID_ADD",
      "GEN_FROM_CONFIG"
    };

    const char* OBJ_TYPE_STR[] =
    {
      "ObNullType",
      "ObIntType",
      "ObFloatType",
      "ObDoubleType",
      "ObDateTimeType",
      "ObPreciseDateTimeType",
      "ObVarcharType",
      "ObSeqType",
      "ObCreateTimeType",
      "ObModifyTimeType",
      "ObExtendType",
      "ObMaxType"
    };

    ObOpParamGen::ObOpParamGen()
    : gen_op_(GEN_RANDOM), gen_table_name_(GEN_RANDOM),
      gen_row_key_(GEN_RANDOM), gen_row_count_(GEN_RANDOM),
      gen_cell_count_(GEN_RANDOM), gen_cell_(GEN_RANDOM)
    {

    }

    ObOpParamGen::~ObOpParamGen()
    {

    }

    void ObOpParamGen::display() const
    {
      fprintf(stderr, "  ObOpParamGen: \n"
                      "    gen_op_: %s \n"
                      "    gen_table_name_: %s \n"
                      "    gen_row_key_: %s \n"
                      "    gen_row_count_: %s \n"
                      "    gen_cell_count_: %s \n"
                      "    gen_cell_: %s \n",
              OP_GEN_MODE_STR[gen_op_], OP_GEN_MODE_STR[gen_table_name_],
              OP_GEN_MODE_STR[gen_row_key_], OP_GEN_MODE_STR[gen_row_count_],
              OP_GEN_MODE_STR[gen_cell_count_], OP_GEN_MODE_STR[gen_cell_]);
    }

    DEFINE_SERIALIZE(ObOpParamGen)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = get_serialize_size();

      if((NULL == buf) || (serialize_size + pos > buf_len))
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld,"
                        "serialize_size=%ld",
                  buf, buf_len, pos, serialize_size);
        ret = OB_ERROR;
      }

      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == encode_i32(buf, buf_len, pos, gen_op_))
          && (OB_SUCCESS == encode_i32(buf, buf_len, pos, gen_table_name_))
          && (OB_SUCCESS == encode_i32(buf, buf_len, pos, gen_row_key_))
          && (OB_SUCCESS == encode_i32(buf, buf_len, pos, gen_row_count_))
          && (OB_SUCCESS == encode_i32(buf, buf_len, pos, gen_cell_count_))
          && (OB_SUCCESS == encode_i32(buf, buf_len, pos, gen_cell_)))
      {
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "failed to serialzie ObParamGen, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, buf_len, pos);
        ret = OB_ERROR;
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObOpParamGen)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = get_serialize_size();

      if (NULL == buf || serialize_size + pos > data_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld,"
                        "serialize_size=%ld",
                  buf, data_len, pos, serialize_size);
        ret = OB_ERROR;
      }

      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == decode_i32(buf, data_len, pos,
                                       reinterpret_cast<int32_t*>(&gen_op_)))
          && (OB_SUCCESS == decode_i32(buf, data_len, pos,
                                       reinterpret_cast<int32_t*>(&gen_table_name_)))
          && (OB_SUCCESS == decode_i32(buf, data_len, pos,
                                       reinterpret_cast<int32_t*>(&gen_row_key_)))
          && (OB_SUCCESS == decode_i32(buf, data_len, pos,
                                       reinterpret_cast<int32_t*>(&gen_row_count_)))
          && (OB_SUCCESS == decode_i32(buf, data_len, pos,
                                       reinterpret_cast<int32_t*>(&gen_cell_count_)))
          && (OB_SUCCESS == decode_i32(buf, data_len, pos,
                                       reinterpret_cast<int32_t*>(&gen_cell_))))
      {
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "failed to deserialzie ObOpParamGen, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, data_len, pos);
        ret = OB_ERROR;
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObOpParamGen)
    {
      return(encoded_length_i32(gen_op_)
             + encoded_length_i32(gen_table_name_)
             + encoded_length_i32(gen_row_key_)
             + encoded_length_i32(gen_row_count_)
             + encoded_length_i32(gen_cell_count_)
             + encoded_length_i32(gen_cell_));
    }

    void ObOpCellParam::reset()
    {
      op_type_ = OP_NULL;
      gen_cell_ = GEN_RANDOM;
      column_name_.assign(NULL, 0);
      cell_type_ = common::ObNullType;
      varchar_len_ = 0;
      key_prefix_ = 0;
      value_.int_val_ = 0;
    }

    void ObOpCellParam::display() const
    {
      fprintf(stderr, "      op_type_: %s \n"
                      "      gen_cell_: %s \n"
                      "      column_name_: %.*s \n"
                      "      column_id_: %ld \n"
                      "      cell_type_: %s \n"
                      "      key_prefix_: %ld \n",
              OP_TYPE_STR[op_type_], OP_GEN_MODE_STR[gen_cell_],
              column_name_.length(), column_name_.ptr(), column_id_,
              OBJ_TYPE_STR[cell_type_], key_prefix_);
      switch (cell_type_)
      {
      case common::ObFloatType:
        fprintf(stderr, "      value_: %.4f \n", value_.float_val_);
        break;
      case common::ObDoubleType:
        fprintf(stderr, "      value_: %.4f \n", value_.double_val_);
        break;
      case common::ObVarcharType:
        fprintf(stderr, "      varchar_len_: %d \n"
                        "      value_: %p \n",
                varchar_len_, value_.varchar_val_);
        break;
      case common::ObNullType:
      case common::ObMinType:
      case common::ObMaxType:
      case common::ObExtendType:
      case common::ObSeqType:
        break;
      default:
        fprintf(stderr, "      value_: %ld \n\n", value_.int_val_);
        break;
      }
    }

    DEFINE_SERIALIZE(ObOpCellParam)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = get_serialize_size();

      if((NULL == buf) || (serialize_size + pos > buf_len))
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld,"
                        "serialize_size=%ld",
                  buf, buf_len, pos, serialize_size);
        ret = OB_ERROR;
      }

      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == encode_i32(buf, buf_len, pos, op_type_))
          && (OB_SUCCESS == encode_i32(buf, buf_len, pos, gen_cell_))
          && (OB_SUCCESS == encode_i32(buf, buf_len, pos, cell_type_))
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, key_prefix_)))
      {
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "failed to serialzie ObOpCellParam, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, buf_len, pos);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ret = column_name_.serialize(buf, buf_len, pos);
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObOpCellParam)
    {
      int ret = OB_SUCCESS;

      if (NULL == buf || data_len <= 0 || pos > data_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld,",
                  buf, data_len, pos);
        ret = OB_ERROR;
      }

      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == decode_i32(buf, data_len, pos,
                                       reinterpret_cast<int32_t*>(&op_type_)))
          && (OB_SUCCESS == decode_i32(buf, data_len, pos,
                                       reinterpret_cast<int32_t*>(&gen_cell_)))
          && (OB_SUCCESS == decode_i32(buf, data_len, pos,
                                       reinterpret_cast<int32_t*>(&cell_type_)))
          && (OB_SUCCESS == decode_i64(buf, data_len, pos, &key_prefix_)))
      {
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "failed to deserialzie ObOpCellParam, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, data_len, pos);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ret = column_name_.deserialize(buf, data_len, pos);
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObOpCellParam)
    {
      return(encoded_length_i32(op_type_)
             + encoded_length_i32(gen_cell_)
             + encoded_length_i32(cell_type_)
             + encoded_length_i64(key_prefix_)
             + column_name_.get_serialize_size());
    }

    void ObOpRowParam::reset()
    {
      op_type_ = OP_NULL;
      rowkey_len_ = 0;
      for (int64_t i = 0; i < cell_count_; ++i)
      {
        cell_[i].reset();
      }
      cell_count_ = 0;
    }

    void ObOpRowParam::display(const bool detail) const
    {
      fprintf(stderr, "    op_type_: %s \n"
                      "    cell_count_: %d \n"
                      "    rowkey_len: %d \n"
                      "    rowkey_: ",
              OP_TYPE_STR[op_type_], cell_count_, rowkey_len_);
      hex_dump_rowkey(rowkey_, rowkey_len_, true);
      if (detail)
      {
        for (int64_t i = 0; i < cell_count_; ++i)
        {
          fprintf(stderr, "    cell_[%ld]: \n", i);
          cell_[i].display();
        }
      }
    }

    DEFINE_SERIALIZE(ObOpRowParam)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = get_serialize_size();

      if((NULL == buf) || (serialize_size + pos > buf_len))
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld,"
                        "serialize_size=%ld",
                  buf, buf_len, pos, serialize_size);
        ret = OB_ERROR;
      }

      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == encode_i32(buf, buf_len, pos, op_type_))
          && (OB_SUCCESS == encode_i32(buf, buf_len, pos, cell_count_))
          && (OB_SUCCESS == encode_i32(buf, buf_len, pos, rowkey_len_)))
      {
        memcpy(buf + pos, rowkey_, rowkey_len_);
        pos += rowkey_len_;
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "failed to serialzie ObOpRowParam, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, buf_len, pos);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        for (int64_t i = 0; i < cell_count_ && OB_SUCCESS == ret; ++i)
        {
          ret = cell_[i].serialize(buf, buf_len, pos);
        }
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObOpRowParam)
    {
      int ret = OB_SUCCESS;

      if (NULL == buf || data_len <= 0 || pos > data_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld,",
                  buf, data_len, pos);
        ret = OB_ERROR;
      }

      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == decode_i32(buf, data_len, pos,
                                       reinterpret_cast<int32_t*>(&op_type_)))
          && (OB_SUCCESS == decode_i32(buf, data_len, pos, &cell_count_))
          && (OB_SUCCESS == decode_i32(buf, data_len, pos, &rowkey_len_)))
      {
        memcpy(rowkey_, buf + pos, rowkey_len_);
        pos += rowkey_len_;
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "failed to deserialzie ObOpRowParam, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, data_len, pos);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        for (int64_t i = 0; i < cell_count_ && OB_SUCCESS == ret; ++i)
        {
          ret = cell_[i].deserialize(buf, data_len, pos);
        }
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObOpRowParam)
    {
      int64_t total_size = 0;

      total_size += (encoded_length_i32(op_type_)
                     + encoded_length_i32(cell_count_)
                     + encoded_length_i32(rowkey_len_)
                     + rowkey_len_);

      for (int64_t i = 0; i < cell_count_; ++i)
      {
        total_size += cell_[i].get_serialize_size();
      }

      return total_size;
    }

    ObOpParam::ObOpParam()
    : loaded_(false), data_buf_(NULL),
      data_buf_size_(PARAM_DATA_BUF_SIZE), data_len_(0)
    {

    }

    ObOpParam::~ObOpParam()
    {
      if (NULL != data_buf_)
      {
        ob_free(data_buf_);
        data_buf_ = NULL;
      }
      data_len_ = 0;
    }

    void ObOpParam::reset()
    {
      invalid_op_ = false;
      is_read_ = true;
      is_wide_table_ = true;
      op_type_ = OP_NULL;
      table_name_.assign (NULL, 0);
      for (int64_t i = 0; i < row_count_; ++i)
      {
        row_[i].reset();
      }
      row_count_ = 0;
    }

    void ObOpParam::display() const
    {
      fprintf(stderr, "ObOpParam: \n"
                      "  invalid_op_: %s \n"
                      "  is_read_: %s \n"
                      "  is_wide_table_: %s \n"
                      "  op_type_: %s \n"
                      "  table_name_: %.*s \n"
                      "  table_id_: %ld \n"
                      "  row_count_: %ld \n",
              invalid_op_ ? "true" : "false",
              is_read_ ? "true" : "false",
              is_wide_table_ ? "true" : "false",
              OP_TYPE_STR[op_type_], table_name_.length(),
              table_name_.ptr(), table_id_, row_count_);
      param_gen_.display();
      for (int64_t i = 0; i < row_count_; ++i)
      {
        fprintf(stderr, "  row_[%ld]: \n", i);
        if (OP_SCAN == row_[i].op_type_ && i >= 1)
        {
          row_[i].display(false);
          continue;
        }
        row_[i].display();
      }
    }

    int ObOpParam::ensure_space(const int64_t size)
    {
      int ret           = OB_SUCCESS;
      char* new_buf     = NULL;
      int64_t data_len  = size > data_buf_size_
                          ? size : data_buf_size_;

      if (size <= 0)
      {
        TBSYS_LOG(WARN, "invalid data length, size=%ld", size);
        ret = OB_ERROR;
      }
      else if (NULL == data_buf_
               || (NULL != data_buf_ && size > data_buf_size_))
      {
        new_buf = static_cast<char*>(ob_malloc(data_len, ObModIds::TEST));
        if (NULL == new_buf)
        {
          TBSYS_LOG(WARN, "Problem allocating memory for data buffer");
          ret = OB_ERROR;
        }
        else
        {
          if (NULL != data_buf_)
          {
            ob_free(data_buf_);
            data_buf_ = NULL;
          }
          data_buf_size_ = static_cast<int32_t>(data_len);
          data_buf_ = new_buf;
        }
      }

      return ret;
    }

    int ObOpParam::write_param_to_file(const char* file_name)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = 0;
      int64_t pos             = 0;
      int32_t wrote_len       = 0;
      FileUtils file_util;

      if (NULL == file_name)
      {
        TBSYS_LOG(WARN, "invalid param, file_name=%p", file_name);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ret = file_util.open(file_name, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (ret < 0)
        {
          TBSYS_LOG(ERROR, "failed to open file=%s", file_name);
          ret = OB_ERROR;
        }
        else
        {
          ret = OB_SUCCESS;
        }
      }

      if (OB_SUCCESS == ret)
      {
        serialize_size = get_serialize_size();
        if (OB_SUCCESS == ret && serialize_size > 0
            && (OB_SUCCESS == (ret = ensure_space(serialize_size))))
        {
          data_len_ = static_cast<int32_t>(serialize_size);
          ret = serialize(data_buf_, data_len_, pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to serialize param, file=%s", file_name);
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        wrote_len = static_cast<int32_t>(file_util.write(data_buf_, data_len_));
        if (wrote_len != data_len_)
        {
          TBSYS_LOG(ERROR, "failed to write file=%s, wrote_len=%d, "
                           "data_len=%d", file_name, wrote_len, data_len_);
          ret = OB_ERROR;
        }
      }

      file_util.close();

      return ret;
    }

    int ObOpParam::load_param_from_file(const char* file_name)
    {
      int ret           = OB_SUCCESS;
      int64_t file_len  = 0;
      int32_t read_len  = 0;
      int64_t pos       = 0;
      FileUtils file_util;

      if (NULL == file_name)
      {
        TBSYS_LOG(WARN, "invalid param, file_name=%p", file_name);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        file_len = FileDirectoryUtils::get_size(file_name);
        if (file_len > 0)
        {
          ret = file_util.open(file_name, O_RDWR, 0644);
          if (ret < 0)
          {
            TBSYS_LOG(ERROR, "failed to open file=%s", file_name);
            ret = OB_ERROR;
          }
          else
          {
            ret = OB_SUCCESS;
          }
        }
      }

      if (OB_SUCCESS == ret
          && (OB_SUCCESS == (ret = ensure_space(file_len))))
      {
        read_len = static_cast<int32_t>(file_util.read(data_buf_, file_len));
        if (read_len != file_len)
        {
           TBSYS_LOG(ERROR, "failed to read file=%s, read_len=%d, "
                            "file_len=%ld", file_name, read_len, file_len);
           ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        data_len_ = read_len;
        ret = deserialize(data_buf_, data_len_, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to deserialize param, file=%s", file_name);
        }
      }

      if (OB_SUCCESS == ret)
      {
        loaded_ = true;
      }

      file_util.close();

      return ret;
    }

    DEFINE_SERIALIZE(ObOpParam)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = get_serialize_size();

      if((NULL == buf) || (serialize_size + pos > buf_len))
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld,"
                        "serialize_size=%ld",
                  buf, buf_len, pos, serialize_size);
        ret = OB_ERROR;
      }

      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == encode_i8(buf, buf_len, pos, invalid_op_))
          && (OB_SUCCESS == encode_i8(buf, buf_len, pos, is_read_))
          && (OB_SUCCESS == encode_i8(buf, buf_len, pos, is_wide_table_))
          && (OB_SUCCESS == encode_i32(buf, buf_len, pos, op_type_))
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, row_count_)))
      {
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "failed to serialzie ObOpParam, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, buf_len, pos);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ret = table_name_.serialize(buf, buf_len, pos);
      }

      if (OB_SUCCESS == ret)
      {
        ret = param_gen_.serialize(buf, buf_len, pos);
      }

      if (OB_SUCCESS == ret)
      {
        for (int64_t i = 0; i < row_count_ && OB_SUCCESS == ret; ++i)
        {
          ret = row_[i].serialize(buf, buf_len, pos);
        }
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObOpParam)
    {
      int ret = OB_SUCCESS;

      if (NULL == buf || data_len <= 0 || pos > data_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld,",
                  buf, data_len, pos);
        ret = OB_ERROR;
      }

      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == decode_i8(buf, data_len, pos,
                                      reinterpret_cast<int8_t*>(&invalid_op_)))
          && (OB_SUCCESS == decode_i8(buf, data_len, pos,
                                      reinterpret_cast<int8_t*>(&is_read_)))
          && (OB_SUCCESS == decode_i8(buf, data_len, pos,
                                      reinterpret_cast<int8_t*>(&is_wide_table_)))
          && (OB_SUCCESS == decode_i32(buf, data_len, pos,
                                       reinterpret_cast<int32_t*>(&op_type_)))
          && (OB_SUCCESS == decode_i64(buf, data_len, pos, &row_count_)))
      {
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "failed to deserialzie ObOpParam, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, data_len, pos);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ret = table_name_.deserialize(buf, data_len, pos);
      }

      if (OB_SUCCESS == ret)
      {
        ret = param_gen_.deserialize(buf, data_len, pos);
      }

      if (OB_SUCCESS == ret)
      {
        for (int64_t i = 0; i < row_count_ && OB_SUCCESS == ret; ++i)
        {
          ret = row_[i].deserialize(buf, data_len, pos);
        }
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObOpParam)
    {
      int64_t total_size = 0;

      total_size += (encoded_length_i8(invalid_op_)
                     + encoded_length_i8(is_read_)
                     + encoded_length_i8(is_wide_table_)
                     + encoded_length_i32(op_type_)
                     + encoded_length_i64(row_count_));

      total_size += table_name_.get_serialize_size();
      total_size += param_gen_.get_serialize_size();

      for (int64_t i = 0; i < row_count_; ++i)
      {
        total_size += row_[i].get_serialize_size();
      }

      return total_size;
    }

    ObSyscheckerRule::ObSyscheckerRule(ObSyscheckerSchema& syschecker_schema)
    : inited_(false), cur_max_prefix_(0), cur_max_suffix_(0),
      syschecker_schema_(syschecker_schema),
      write_rule_count_(0), read_rule_count_(0),
      random_block_(NULL), random_block_size_(RANDOM_BLOCK_SIZE),
      syschecker_count_(0), is_specified_read_param_(false),
      operate_full_row_(false)
    {
      memset(write_rule_, 0, sizeof(ObOpRule) * MAX_WRITE_RULE_NUM);
      memset(read_rule_, 0, sizeof(ObOpRule) * MAX_READ_RULE_NUM);
    }

    ObSyscheckerRule::~ObSyscheckerRule()
    {
      if (NULL != random_block_)
      {
        ob_free(random_block_);
        random_block_ = NULL;
      }
    }

    int ObSyscheckerRule::init_random_block()
    {
      int ret = OB_SUCCESS;

      if (NULL == random_block_)
      {
        if (random_block_size_ <= 0)
        {
          TBSYS_LOG(WARN, "invalid param, random_block_size_=%ld",
                    random_block_size_);
          ret = OB_ERROR;
        }

        if (OB_SUCCESS == ret)
        {
          random_block_ = reinterpret_cast<char*>(ob_malloc(random_block_size_, ObModIds::TEST));
          if (NULL == random_block_)
          {
            TBSYS_LOG(WARN, "failed to allocate memory for random char block");
            ret = OB_ERROR;
          }
        }

        if (OB_SUCCESS == ret && NULL != random_block_)
        {
          for (int64_t i = 0; i < random_block_size_; ++i)
          {
            random_block_[i] = ALPHA_NUMERICS[random() % ALPHA_NUMERICS_SIZE];
          }
        }
      }

      return ret;
    }

    int ObSyscheckerRule::init(const ObSyscheckerParam& param)
    {
      int ret = OB_SUCCESS;
      ObOpParamGen write_param_gen;
      ObOpRule write_rule;
      ObOpParamGen read_param_gen;
      ObOpRule read_rule;

      if (OB_SUCCESS == ret && (cur_max_prefix_ <= 0 || cur_max_suffix_ <= 0))
      {
        TBSYS_LOG(WARN, "invalid row key range, cur_max_prefix_=%lu, "
                        "cur_max_suffix_=%lu",
                  cur_max_prefix_, cur_max_suffix_);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        is_specified_read_param_ = param.is_specified_read_param();
        syschecker_count_ = param.get_syschecker_count();
        operate_full_row_ = param.is_operate_full_row();
        perf_test_ = param.is_perf_test();
        is_sql_read_ = param.is_sql_read();
        read_table_type_ = static_cast<int32_t>(param.get_read_table_type());
        write_table_type_ = static_cast<int32_t>(param.get_write_table_type());
        get_row_cnt_ = param.get_get_row_cnt();
        scan_row_cnt_ = param.get_scan_row_cnt();
        update_row_cnt_  = param.get_update_row_cnt();
        ret = init_random_block();
      }

      if (OB_SUCCESS == ret)
      {
        /**
         * now we just init all the rule with the same rule, we will add
         * more rule later
         */
        write_param_gen.gen_cell_ = GEN_VALID_WRITE;
        write_param_gen.gen_cell_count_ = GEN_VALID_WRITE;
        if (perf_test_)
        {
          write_param_gen.gen_op_ = GEN_FROM_CONFIG;
          write_param_gen.gen_table_name_ = GEN_FROM_CONFIG;
          write_param_gen.gen_row_count_ = GEN_FROM_CONFIG;
          write_param_gen.gen_row_key_ = GEN_FROM_CONFIG;
        }
        strcpy(write_rule.rule_name_, "write_rule");
        write_rule.param_gen_ = write_param_gen;
        write_rule.invalid_op_ = false;
        for (int64_t i = 0; i < MAX_WRITE_RULE_NUM; ++i)
        {
          write_rule_[i] = write_rule;
          if (i > MAX_WRITE_RULE_NUM * 9 / 10)
          {
            //get or scan sequence
            write_rule_[i].param_gen_.gen_row_key_ = GEN_NEW_KEY;
          }
        }
        write_rule_count_ = MAX_WRITE_RULE_NUM;
        if (!perf_test_)
        {
          write_rule_[0].invalid_op_ = true;  //the first rule is invalid rule
          write_rule_[0].param_gen_.gen_cell_count_ = GEN_INVALID_WRITE;
          write_rule_[0].param_gen_.gen_cell_ = GEN_INVALID_WRITE;
          write_rule_[1].invalid_op_ = true;  //the second rule is random rule
          write_rule_[1].param_gen_.gen_cell_count_ = GEN_RANDOM;
          write_rule_[1].param_gen_.gen_cell_ = GEN_RANDOM;
        }

        if (perf_test_)
        {
          read_param_gen.gen_op_ = GEN_FROM_CONFIG;
          read_param_gen.gen_row_count_ = GEN_FROM_CONFIG;
          read_param_gen.gen_table_name_ = GEN_FROM_CONFIG;
          read_param_gen.gen_row_key_ = GEN_FROM_CONFIG;
        }
        strcpy(read_rule.rule_name_, "read_rule");
        read_rule.param_gen_ = read_param_gen;
        read_rule.invalid_op_ = false;
        for (int64_t i = 0; i < MAX_READ_RULE_NUM; ++i)
        {
          read_rule_[i] = read_rule;
          if (!perf_test_ && i > MAX_READ_RULE_NUM / 2)
          {
            //get or scan sequence
            read_rule_[i].param_gen_.gen_row_key_ = GEN_SEQ;
          }
        }
        read_rule_count_ = MAX_READ_RULE_NUM;
        if (!perf_test_)
        {
          read_rule_[0].param_gen_.gen_row_key_ = GEN_NEW_KEY;
        }
        inited_ = true;
      }

      return ret;
    }

    ObOpType ObSyscheckerRule::get_random_read_op(const uint64_t random_num)
    {
      if (random_num % READ_OP_COUNT == 0 )
      {
        return is_sql_read_ ? OP_SQL_GET: OP_GET;
      }
      else
      {
        return is_sql_read_ ? OP_SQL_SCAN : OP_SCAN;
      }
    }

    ObOpType ObSyscheckerRule::get_config_read_op(const uint64_t random_num)
    {
      if (get_row_cnt_ > 0 && scan_row_cnt_ == 0)
      {
        return is_sql_read_ ? OP_SQL_GET: OP_GET;
      }
      else if (get_row_cnt_ == 0 && scan_row_cnt_ > 0)
      {
        return is_sql_read_ ? OP_SQL_SCAN : OP_SCAN;
      }
      else
      {
        return (random_num % READ_OP_COUNT == 0 ? OP_GET : OP_SCAN);
      }
    }

    ObOpType ObSyscheckerRule::get_random_write_op(const uint64_t random_num)
    {
      if (operate_full_row_)
      {
        //operate full row can't inlcude OP_MIX operation
        return (static_cast<ObOpType>(random_num % WRITE_OP_FULL_ROW_COUNT + OP_ADD));
      }
      else
      {
        return (static_cast<ObOpType>(random_num % WRITE_OP_COUNT + OP_ADD));
      }
    }

    ObOpType ObSyscheckerRule::get_cell_random_write_op(const uint64_t random_num)
    {
      return (static_cast<ObOpType>(random_num % CELL_WRITE_OP_COUNT + OP_ADD));
    }

    int ObSyscheckerRule::set_op_type(ObOpParam& op_param,
                                      const uint64_t random_num)
    {
      int ret = OB_SUCCESS;

      switch (op_param.param_gen_.gen_op_)
      {
      case GEN_RANDOM:
      case GEN_FROM_CONFIG:
        if (op_param.is_read_)
        {
          if (!perf_test_)
          {
            op_param.op_type_ = get_random_read_op(random_num);
          }
          else
          {
            op_param.op_type_ = get_config_read_op(random_num);
          }
        }
        else
        {
           op_param.op_type_ = get_random_write_op(random_num);
        }
        break;

      case GEN_SPECIFIED:
        //specified, check if it's valid
        if (OP_NULL == op_param.op_type_ || (op_param.is_read_
            && (op_param.op_type_ != OP_GET && op_param.op_type_ != OP_SCAN))
            || (!op_param.is_read_ && op_param.op_type_ >= OP_GET))
        {
          TBSYS_LOG(WARN, "spedified wrong opertion type, op_type=%d",
                    op_param.op_type_);
          ret = OB_ERROR;
        }
        break;

      case GEN_SEQ:
      case GEN_COMBO_RANDOM:
        TBSYS_LOG(WARN, "not implement type(%s) for generating operation type",
                  OP_GEN_MODE_STR[op_param.param_gen_.gen_op_]);
        ret = OB_ERROR;
        break;

      default:
        TBSYS_LOG(WARN, "uknown generation type=%s",
                  OP_GEN_MODE_STR[op_param.param_gen_.gen_op_]);
        ret = OB_ERROR;
        break;
      }

      return ret;
    }

    int ObSyscheckerRule::set_table_name(ObOpParam& op_param,
                                         const uint64_t random_num)
    {
      int ret = OB_SUCCESS;

      switch (op_param.param_gen_.gen_table_name_)
      {
      case GEN_RANDOM:
      case GEN_FROM_CONFIG:
        if (perf_test_ && read_table_type_ != ALL_TABLE)
        {
          if (read_table_type_ == WIDE_TABLE)
          {
            op_param.table_name_ = syschecker_schema_.get_wt_name();
            op_param.table_id_ = syschecker_schema_.get_wt_schema()->get_table_id();
            op_param.is_wide_table_ = true;
          }
          else if (read_table_type_ == JOIN_TABLE)
          {
            op_param.table_name_ = syschecker_schema_.get_jt_name();
            op_param.table_id_ = syschecker_schema_.get_jt_schema()->get_table_id();
            op_param.is_wide_table_ = false;
          }
        }
        else
        {
          if (random_num % ObSyscheckerSchema::TABLE_COUNT == 0)
          {
            op_param.table_name_ = syschecker_schema_.get_wt_name();
            op_param.table_id_ = syschecker_schema_.get_wt_schema()->get_table_id();
            op_param.is_wide_table_ = true;
          }
          else
          {
            op_param.table_name_ = syschecker_schema_.get_jt_name();
            op_param.table_id_ = syschecker_schema_.get_jt_schema()->get_table_id();
            op_param.is_wide_table_ = false;
          }
        }
        //no break, go through

      case GEN_SPECIFIED:
        if (NULL == op_param.table_name_.ptr()
            || op_param.table_name_.length() <= 0)
        {
          TBSYS_LOG(WARN, "get wrong table name from syschecker schema, "
                          "table_name=%p, length=%d",
                    op_param.table_name_.ptr(), op_param.table_name_.length());
          ret = OB_ERROR;
        }
        break;

      case GEN_SEQ:
      case GEN_COMBO_RANDOM:
        TBSYS_LOG(WARN, "not implement type(%s) for generating table name",
                  OP_GEN_MODE_STR[op_param.param_gen_.gen_table_name_]);
        ret = OB_ERROR;
        break;

      default:
        TBSYS_LOG(WARN, "uknown generation type=%s",
                  OP_GEN_MODE_STR[op_param.param_gen_.gen_table_name_]);
        ret = OB_ERROR;
        break;
      }

      return ret;
    }

    int ObSyscheckerRule::set_row_count(ObOpParam& op_param,
                                        const uint64_t random_num)
    {
      int ret = OB_SUCCESS;

      switch (op_param.param_gen_.gen_row_count_)
      {
      case GEN_RANDOM:
      case GEN_FROM_CONFIG:
        if (op_param.is_read_)
        {
          if (OP_GET == op_param.op_type_ || OP_SQL_GET == op_param.op_type_)
          {
            if (!op_param.invalid_op_)
            {
              if (!perf_test_)
              {
                op_param.row_count_ = random_num % (MAX_GET_ROW_COUNT - 1) + 1;
              }
              else
              {
                op_param.row_count_ = get_row_cnt_;
              }
            }
            else
            {
              op_param.row_count_ = random_num % (MAX_INVALID_ROW_COUNT - 1) + 1;
            }
          }
          else if (OP_SCAN == op_param.op_type_ || OP_SQL_SCAN == op_param.op_type_)
          {
            op_param.row_count_ = SCAN_PARAM_ROW_COUNT;
          }
        }
        else
        {
          if (!op_param.invalid_op_)
          {
            if (!perf_test_)
            {
              op_param.row_count_ = random_num % (MAX_WRITE_ROW_COUNT - 1) + 1;
            }
            else
            {
              op_param.row_count_ = update_row_cnt_;
            }
          }
          else
          {
            op_param.row_count_ = random_num % (MAX_INVALID_ROW_COUNT - 1) + 1;
          }
        }
        //go through

      case GEN_SPECIFIED:
        //specified, check if it's valid
        if (op_param.row_count_ <= 0)
        {
          TBSYS_LOG(WARN, "spedified wrong row count, row_count=%ld",
                    op_param.row_count_);
          ret = OB_ERROR;
        }
        break;

      case GEN_SEQ:
      case GEN_COMBO_RANDOM:
        TBSYS_LOG(WARN, "not implement type(%s) for generating row count",
                  OP_GEN_MODE_STR[op_param.param_gen_.gen_row_count_]);
        ret = OB_ERROR;
        break;

      default:
        TBSYS_LOG(WARN, "uknown generation type=%s",
                  OP_GEN_MODE_STR[op_param.param_gen_.gen_row_count_]);
        ret = OB_ERROR;
        break;
      }

      return ret;
    }

    void ObSyscheckerRule::set_row_op_type(const ObOpParam& op_param,
                                           ObOpRowParam& row_param,
                                           const uint64_t random_num)
    {
      if (OP_MIX != op_param.op_type_)
      {
        row_param.op_type_ = op_param.op_type_;
      }
      else
      {
        if (GEN_INVALID_WRITE == op_param.param_gen_.gen_cell_)
        {
          //delete operation always succes, no invalid case to choose
          row_param.op_type_ = get_cell_random_write_op(random_num);
        }
        else
        {
          row_param.op_type_ = get_random_write_op(random_num);
        }
      }
    }

    void ObSyscheckerRule::set_cell_op_type(const ObOpRowParam& row_param,
                                            ObOpCellParam& cell_param,
                                            const uint64_t random_num)
    {
      if (OP_MIX != row_param.op_type_)
      {
        cell_param.op_type_ = row_param.op_type_;
      }
      else
      {
        cell_param.op_type_ = get_cell_random_write_op(random_num);
      }
    }

    int ObSyscheckerRule::encode_row_key(const ObOpParam& op_param,
                                         ObOpRowParam& row_param,
                                         const int64_t prefix,
                                         const int64_t suffix)
    {
      int ret     = OB_SUCCESS;

      if (op_param.is_wide_table_)
      {
        row_param.rowkey_len_ = MAX_SYSCHECKER_ROWKEY_COLUMN_COUNT;
        row_param.rowkey_[0].set_int(prefix);
        if (suffix < 0)
        {
          row_param.rowkey_[1].set_max_value();
        }
        else
        {
          row_param.rowkey_[1].set_int(suffix);
        }
      }
      else
      {
        row_param.rowkey_len_ = MAX_SYSCHECKER_ROWKEY_COLUMN_COUNT / 2;
        row_param.rowkey_[0].set_int(suffix);
      }

      return ret;
    }

    int ObSyscheckerRule::set_row_key(const ObOpParam& op_param,
                                      ObOpRowParam& row_param,
                                      const uint64_t random_num,
                                      int64_t& prefix,
                                      int64_t& suffix)
    {
      int ret             = OB_SUCCESS;
      int64_t prefix_back = prefix;
      int64_t suffix_back = suffix;

      switch (op_param.param_gen_.gen_row_key_)
      {
      case GEN_RANDOM:
      case GEN_FROM_CONFIG:
        //skip key with prefix 0
        prefix = random_num % (cur_max_prefix_ - 1) + 1;
        suffix = random_num % cur_max_suffix_;
        if (OP_SCAN == op_param.op_type_ || OP_SQL_SCAN == op_param.op_type_)
        {
          if (op_param.is_wide_table_ && prefix_back > 0)
          {
            if (!perf_test_)
            {
              prefix = prefix_back + random_num % MAX_SCAN_ROW_COUNT;
              suffix = suffix_back;
            }
            else
            {
              prefix = prefix_back + scan_row_cnt_ - 1;
              suffix = -1;  //for performance test, the end key with FF suffix
            }
          }
          else if (op_param.is_wide_table_ && prefix_back == 0 && perf_test_)
          {
            suffix = 0; //for performance test, the start key with 00 suffix
          }
          else if (!op_param.is_wide_table_ && suffix_back > 0)
          {
            if (!perf_test_)
            {
              suffix = suffix_back + random_num % MAX_SCAN_ROW_COUNT;
            }
            else
            {
              suffix = suffix_back + scan_row_cnt_;
            }
          }
        }
        else if (OP_GET == op_param.op_type_ || OP_SQL_GET == op_param.op_type_)
        {
          //avoid adjacent rowkey is the same
          if (op_param.is_wide_table_ && prefix_back == prefix
              && suffix_back == suffix)
          {
            suffix = suffix_back + 1;
          }
          else if (!op_param.is_wide_table_ && suffix_back == suffix)
          {
            suffix = suffix_back + 1;
          }
        }
        ret = encode_row_key(op_param, row_param, prefix, suffix);
        break;

      case GEN_SEQ:
        //skip key with prefix 0
        prefix = random_num % (cur_max_prefix_ - 1) + 1;
        suffix = random_num % cur_max_suffix_;
        if (op_param.is_wide_table_ && prefix_back > 0)
        {
          if (OP_SCAN == op_param.op_type_)
          {
            prefix = prefix_back + random_num % MAX_SCAN_ROW_COUNT;
          }
          else
          {
            prefix = prefix_back + 1;
          }
        }
        else if (!op_param.is_wide_table_ && suffix_back > 0)
        {
          if (OP_SCAN == op_param.op_type_)
          {
            suffix = suffix_back + random_num % MAX_SCAN_ROW_COUNT;
          }
          else
          {
            suffix = suffix_back + 1;
          }
        }
        ret = encode_row_key(op_param, row_param, prefix, suffix);
        break;

      case GEN_NEW_KEY:
        if (op_param.is_wide_table_)
        {
          prefix = add_cur_max_prefix(syschecker_count_);
          suffix = random_num % cur_max_suffix_;
        }
        else
        {
          suffix = add_cur_max_suffix(syschecker_count_);
        }
        ret = encode_row_key(op_param, row_param, prefix, suffix);
        break;

      case GEN_SPECIFIED:
        //specified, check if it's valid
        if (row_param.rowkey_len_ <= 0)
        {
          TBSYS_LOG(WARN, "spedified wrong row key length, row_length=%d",
                    row_param.rowkey_len_);
          ret = OB_ERROR;
        }
        break;

      case GEN_COMBO_RANDOM:
        TBSYS_LOG(WARN, "not implement type(%s) for generating row key",
                  OP_GEN_MODE_STR[op_param.param_gen_.gen_row_key_]);
        ret = OB_ERROR;
        break;

      default:
        TBSYS_LOG(WARN, "uknown generation type=%s",
                  OP_GEN_MODE_STR[op_param.param_gen_.gen_row_key_]);
        ret = OB_ERROR;
        break;
      }

      return ret;
    }

    int ObSyscheckerRule::set_cell_count_per_row(const ObOpParam& op_param,
                                                 ObOpRowParam& row_param,
                                                 const uint64_t random_num)
    {
      int ret             = OB_SUCCESS;
      int64_t wt_size     = 0;
      int64_t wwt_size    = 0;
      int64_t wut_size    = 0;
      int64_t wat_size    = 0;
      int64_t jt_size     = 0;
      int64_t jwt_size    = 0;
      int64_t jut_size    = 0;
      int64_t jat_size    = 0;
      int64_t cell_count  = 0;
      ObGenMode gen_cell_count = GEN_RANDOM;

      if (operate_full_row_ && OP_ADD == row_param.op_type_
          && GEN_VALID_WRITE == op_param.param_gen_.gen_cell_count_)
      {
          gen_cell_count = GEN_VALID_ADD;
      }
      else
      {
        gen_cell_count = op_param.param_gen_.gen_cell_count_;
      }

      switch (gen_cell_count)
      {
      case GEN_RANDOM:
        if (op_param.is_wide_table_)
        {
          wt_size = syschecker_schema_.get_wt_column_count();
          if (wt_size <= 0)
          {
            TBSYS_LOG(WARN, "invalid wide table column count, wt_clumn_count=%ld, "
                            "gen_cell_count_type=%s",
                      wt_size, OP_GEN_MODE_STR[op_param.param_gen_.gen_cell_count_]);
            ret = OB_ERROR;
            break;
          }
          if (operate_full_row_)
          {
            cell_count = wt_size * MIN_OP_CELL_COUNT;
          }
          else
          {
            cell_count = (random_num % wt_size) * MIN_OP_CELL_COUNT;
          }
        }
        else
        {
          jt_size = syschecker_schema_.get_jt_column_count();
          if (jt_size <= 0)
          {
            TBSYS_LOG(WARN, "invalid join table column count, jt_clumn_count=%ld, "
                            "gen_cell_count_type=%s",
                      jt_size, OP_GEN_MODE_STR[op_param.param_gen_.gen_cell_count_]);
            ret = OB_ERROR;
            break;
          }
          if (operate_full_row_)
          {
            cell_count = jt_size * MIN_OP_CELL_COUNT;
          }
          else
          {
            cell_count = (random_num % jt_size) * MIN_OP_CELL_COUNT;
          }
        }
        row_param.cell_count_ = static_cast<int32_t>((0 == cell_count) ? MIN_OP_CELL_COUNT : cell_count);
        break;

      case GEN_VALID_WRITE:
        if (!op_param.is_read_)
        {
          //only choose witable column
          if (op_param.is_wide_table_)
          {
            wwt_size = syschecker_schema_.get_wt_writable_column_count();
            if (wwt_size <= 0)
            {
              TBSYS_LOG(WARN, "invalid wide table writable column count, "
                              "wt_writable_clumn_count=%ld, "
                              "gen_cell_count_type=%s",
                        wwt_size, OP_GEN_MODE_STR[op_param.param_gen_.gen_cell_count_]);
              ret = OB_ERROR;
              break;
            }
            if (operate_full_row_)
            {
              cell_count = wwt_size * MIN_OP_CELL_COUNT;
            }
            else
            {
              cell_count = (random_num % wwt_size) * MIN_OP_CELL_COUNT;
            }
          }
          else
          {
            jwt_size = syschecker_schema_.get_jt_writable_column_count();
            if (jwt_size <= 0)
            {
              TBSYS_LOG(WARN, "invalid join table writable column count, "
                              "jt_writable_clumn_count=%ld, "
                              "gen_cell_count_type=%s",
                        jwt_size, OP_GEN_MODE_STR[op_param.param_gen_.gen_cell_count_]);
              ret = OB_ERROR;
              break;
            }
            if (operate_full_row_)
            {
              cell_count = jwt_size * MIN_OP_CELL_COUNT;
            }
            else
            {
              cell_count = (random_num % jwt_size) * MIN_OP_CELL_COUNT;
            }
          }
          row_param.cell_count_ = static_cast<int32_t>((0 == cell_count) ? MIN_OP_CELL_COUNT : cell_count);
        }
        else{
          TBSYS_LOG(WARN, "read operation specify GEN_VALID_WRITE");
          ret = OB_ERROR;
        }
        break;

      case GEN_INVALID_WRITE:
        if (!op_param.is_read_)
        {
          //only choose unwitable column
          if (op_param.is_wide_table_)
          {
            wut_size = syschecker_schema_.get_wt_unwritable_column_count();
            if (wut_size <= 0)
            {
              TBSYS_LOG(WARN, "invalid wide table unwritable column count, "
                              "wt_unwritable_clumn_count=%ld, "
                              "gen_cell_count_type=%s",
                        wut_size, OP_GEN_MODE_STR[op_param.param_gen_.gen_cell_count_]);
              ret = OB_ERROR;
              break;
            }
            if (operate_full_row_)
            {
              cell_count = wut_size * MIN_OP_CELL_COUNT;
            }
            else
            {
              cell_count = (random_num % wut_size) * MIN_OP_CELL_COUNT;
            }
          }
          else
          {
            jut_size = syschecker_schema_.get_jt_unwritable_column_count();
            if (jut_size <= 0)
            {
              TBSYS_LOG(WARN, "invalid join table unwritable column count, "
                              "jt_unwritable_clumn_count=%ld, "
                              "gen_cell_count_type=%s",
                        jut_size, OP_GEN_MODE_STR[op_param.param_gen_.gen_cell_count_]);
              ret = OB_ERROR;
              break;
            }
            if (operate_full_row_)
            {
              cell_count = jut_size * MIN_OP_CELL_COUNT;
            }
            else
            {
              cell_count = (random_num % jut_size) * MIN_OP_CELL_COUNT;
            }
          }
          row_param.cell_count_ = static_cast<int32_t>((0 == cell_count) ? MIN_OP_CELL_COUNT : cell_count);
        }
        else{
          TBSYS_LOG(WARN, "read operation specify GEN_INVALID_WRITE");
          ret = OB_ERROR;
        }
        break;

      case GEN_VALID_ADD:
        if (!op_param.is_read_)
        {
          //only choose addable column
          if (op_param.is_wide_table_)
          {
            wat_size = syschecker_schema_.get_wt_addable_column_count();
            if (wat_size <= 0)
            {
              TBSYS_LOG(WARN, "invalid wide table addable column count, "
                              "wat_addable_clumn_count=%ld, "
                              "gen_cell_count_type=%s",
                        wat_size, OP_GEN_MODE_STR[op_param.param_gen_.gen_cell_count_]);
              ret = OB_ERROR;
              break;
            }
            if (operate_full_row_)
            {
              cell_count = wat_size * MIN_OP_CELL_COUNT;
            }
            else
            {
              cell_count = (random_num % wat_size) * MIN_OP_CELL_COUNT;
            }
          }
          else
          {
            jat_size = syschecker_schema_.get_jt_addable_column_count();
            if (jat_size <= 0)
            {
              TBSYS_LOG(WARN, "invalid join table addable column count, "
                              "jat_addable_clumn_count=%ld, "
                              "gen_cell_count_type=%s",
                        jat_size, OP_GEN_MODE_STR[op_param.param_gen_.gen_cell_count_]);
              ret = OB_ERROR;
              break;
            }
            if (operate_full_row_)
            {
              cell_count = jat_size * MIN_OP_CELL_COUNT;
            }
            else
            {
              cell_count = (random_num % jat_size) * MIN_OP_CELL_COUNT;
            }
          }
          row_param.cell_count_ = static_cast<int32_t>((0 == cell_count) ? MIN_OP_CELL_COUNT : cell_count);
        }
        else{
          TBSYS_LOG(WARN, "read operation specify GEN_VALID_ADD");
          ret = OB_ERROR;
        }
        break;

      case GEN_SPECIFIED:
        //specified, check if it's valid
        if (row_param.cell_count_ <= 0
            || row_param.cell_count_ % MIN_OP_CELL_COUNT != 0)
        {
          TBSYS_LOG(WARN, "spedified wrong cell count, cell_count=%d",
                    row_param.cell_count_);
          ret = OB_ERROR;
        }
        break;

      case GEN_SEQ:
      case GEN_COMBO_RANDOM:
      case GEN_FROM_CONFIG:
        TBSYS_LOG(WARN, "not implement type(%s) for generating row cell count",
                  OP_GEN_MODE_STR[op_param.param_gen_.gen_cell_count_]);
        ret = OB_ERROR;
        break;

      default:
        TBSYS_LOG(WARN, "uknown generation type=%s",
                  OP_GEN_MODE_STR[op_param.param_gen_.gen_cell_count_]);
        ret = OB_ERROR;
        break;
      }

      return ret;
    }

    int ObSyscheckerRule::set_column_name(const ObColumnPair& column_pair,
                                          ObOpCellParam& org_cell,
                                          ObOpCellParam& aux_cell)
    {
      int ret = OB_SUCCESS;

      if (NULL == column_pair.org_)
      {
        TBSYS_LOG(WARN, "original column is not existent, org=%p",
                  column_pair.org_);
        ret = OB_ERROR;
      }
      else
      {
        org_cell.column_name_ =
          syschecker_schema_.get_column_name(*column_pair.org_);
        org_cell.column_id_ = column_pair.org_->get_id();
        if (NULL != column_pair.aux_)
        {
          aux_cell.column_name_ =
            syschecker_schema_.get_column_name(*column_pair.aux_);
          aux_cell.column_id_ = column_pair.aux_->get_id();
        }
        else
        {
          //no auxiliary column, set original column twice
          aux_cell.column_name_ =
            syschecker_schema_.get_column_name(*column_pair.org_);
          aux_cell.column_id_ = column_pair.org_->get_id();
        }
      }

      return ret;
    }

    int ObSyscheckerRule::set_cell_type(const ObColumnPair& column_pair,
                                        ObOpCellParam& org_cell,
                                        ObOpCellParam& aux_cell)
    {
      int ret = OB_SUCCESS;

      if (NULL == column_pair.org_)
      {
        TBSYS_LOG(WARN, "original column is not existent, org=%p",
                  column_pair.org_);
        ret = OB_ERROR;
      }
      else
      {
        org_cell.cell_type_ = column_pair.org_->get_type();

        if (NULL != column_pair.aux_)
        {
          aux_cell.cell_type_ = column_pair.aux_->get_type();
        }
        else
        {
          //no auxiliary column, set original column twice
          aux_cell.cell_type_ = org_cell.cell_type_;
        }
      }

      return ret;
    }

    int ObSyscheckerRule::set_addable_cell_value(ObOpCellParam& org_cell,
                                                 ObOpCellParam& aux_cell)
    {
      int ret = OB_SUCCESS;

      switch (org_cell.cell_type_)
      {
      case ObIntType:
        org_cell.value_.int_val_ = 1;
        aux_cell.value_.int_val_ = -1;
        break;
      case ObFloatType:
        /**
         * for float type, the auxiliary column store the same value
         * with original column
         */
        org_cell.value_.float_val_ = 1.0;
        aux_cell.value_.float_val_ = 1.0;
        break;
      case ObDoubleType:
        /**
         * for double type, the auxiliary column store the same value
         * with original column
         */
        org_cell.value_.double_val_ = 1.0;
        aux_cell.value_.double_val_ = 1.0;
        break;
      case ObDateTimeType:
        org_cell.value_.time_val_ = 1;
        aux_cell.value_.int_val_ = -1;
        break;
      case ObPreciseDateTimeType:
        org_cell.value_.precisetime_val_ = 1;
        aux_cell.value_.int_val_ = -1;
        break;
      case ObVarcharType:
        /**
         * try to add varchar, must fail, and it can't affect the data
         * consistence, we just add some illegal value
         */
        org_cell.value_.varchar_val_ = (char*)1;
        aux_cell.value_.int_val_ = -1;
        break;
      case ObCreateTimeType:
        /**
         * try to add ObCreateTimeType, must fail, and it can't affect
         * the data consistence, we just add some illegal value
         */
        org_cell.value_.createtime_val_ = 1;
        aux_cell.value_.int_val_ = -1;
        break;
      case ObModifyTimeType:
        /**
         * try to add ObModifyTimeType, must fail, and it can't affect
         * the data consistence, we just add some illegal value
         */
        org_cell.value_.modifytime_val_ = 1;
        aux_cell.value_.int_val_ = -1;
        break;
      case ObSeqType:
      case ObExtendType:
      case ObMaxType:
      case ObMinType:
      case ObNullType:
        TBSYS_LOG(WARN, "wrong addable object type %s",
                  OBJ_TYPE_STR[org_cell.cell_type_]);
        ret = OB_ERROR;
        break;
      default:
        TBSYS_LOG(WARN, "unknown object type %s",
                  OBJ_TYPE_STR[org_cell.cell_type_]);
        ret = OB_ERROR;
        break;
      }

      return ret;
    }

    int ObSyscheckerRule::set_cell_varchar(const ObColumnPair& column_pair,
                                           ObOpCellParam& org_cell,
                                           ObOpCellParam& aux_cell,
                                           const uint64_t random_num)
    {
      int ret               = OB_SUCCESS;
      int64_t max_len       = 0;
      int64_t length        = 0;
      int64_t varchar_hash  = 0;
      struct MurmurHash2 hash;

      if (NULL == column_pair.org_)
      {
        TBSYS_LOG(WARN, "original column is not existent, org=%p",
                  column_pair.org_);
        ret = OB_ERROR;
      }
      else
      {
        if (column_pair.org_->get_type() != ObVarcharType)
        {
          TBSYS_LOG(WARN, "original column type is not ObVarcharType, "
                          "type=%d", column_pair.org_->get_type());
          ret = OB_ERROR;
        }
        else
        {
          max_len = column_pair.org_->get_size();
          length = random_num % max_len;
          length = (length < MIN_TEST_VARCHAR_LEN) ? MIN_TEST_VARCHAR_LEN : length;
          org_cell.varchar_len_ = static_cast<int32_t>(length);
          org_cell.value_.varchar_val_ =
            random_block_ + random_num % (random_block_size_ - max_len);
          varchar_hash = hash(org_cell.value_.varchar_val_, static_cast<int32_t>(length));
          aux_cell.value_.int_val_ = 0 - varchar_hash;
        }
      }

      return ret;
    }

    int ObSyscheckerRule::set_new_cell_value(const ObColumnPair& column_pair,
                                             ObOpCellParam& org_cell,
                                             ObOpCellParam& aux_cell,
                                             const uint64_t random_num)
    {
      int ret = OB_SUCCESS;

      //set null, mean delete cell
      if (random_num % (int64_t)ObMaxType == ObNullType)
      {
        org_cell.cell_type_ = ObNullType;
      }

      switch (org_cell.cell_type_)
      {
      case ObNullType:
        aux_cell.value_.int_val_ = 0;
        break;
      case ObIntType:
        org_cell.value_.int_val_ = random_num;
        aux_cell.value_.int_val_ = 0 - random_num;
        break;
      case ObFloatType:
        /**
         * for float type, the auxiliary column store the same value
         * with original column
         */
        org_cell.value_.float_val_ = static_cast<float>(random_num);
        aux_cell.value_.float_val_ = static_cast<float>(random_num);
        break;
      case ObDoubleType:
        /**
         * for double type, the auxiliary column store the same value
         * with original column
         */
        org_cell.value_.double_val_ = (double)random_num;
        aux_cell.value_.double_val_ = (double)random_num;
        break;
      case ObDateTimeType:
        org_cell.value_.time_val_ = random_num;
        aux_cell.value_.int_val_ = 0 - random_num;
        break;
      case ObPreciseDateTimeType:
        org_cell.value_.precisetime_val_ = random_num;
        aux_cell.value_.int_val_ = 0 - random_num;
        break;
      case ObVarcharType:
        ret = set_cell_varchar(column_pair, org_cell, aux_cell, random_num);
        break;
      case ObCreateTimeType:
        //can't update this type, but we try to
        org_cell.value_.createtime_val_ = random_num;
        aux_cell.value_.createtime_val_ = 0 - random_num;
        break;
      case ObModifyTimeType:
        //can't update this type, but we try to
        org_cell.value_.modifytime_val_ = random_num;
        aux_cell.value_.modifytime_val_ = 0 - random_num;
        break;
      case ObSeqType:
      case ObExtendType:
      case ObMaxType:
      case ObMinType:
        TBSYS_LOG(WARN, "wrong update or insert object type %s",
                  OBJ_TYPE_STR[org_cell.cell_type_]);
        ret = OB_ERROR;
        break;
      default:
        TBSYS_LOG(WARN, "unknown object type %s",
                  OBJ_TYPE_STR[org_cell.cell_type_]);
        ret = OB_ERROR;
        break;
      }

      return ret;
    }

    int ObSyscheckerRule::set_cell_value(const ObColumnPair& column_pair,
                                         ObOpCellParam& org_cell,
                                         ObOpCellParam& aux_cell,
                                         const uint64_t random_num)
    {
      int ret = OB_SUCCESS;

      switch (org_cell.op_type_)
      {
      case OP_ADD:
        ret = set_addable_cell_value(org_cell, aux_cell);
        break;
      case OP_UPDATE:
      case OP_INSERT:
        ret = set_new_cell_value(column_pair, org_cell,
                                 aux_cell, random_num);
        break;
      default:
        break;
      }

      return ret;
    }

    int ObSyscheckerRule::set_random_wt_cell(ObOpCellParam& org_cell,
                                             ObOpCellParam& aux_cell,
                                             const uint64_t random_num,
                                             const bool is_read,
                                             const int64_t column_idx)
    {
      int ret         = OB_SUCCESS;
      int64_t wt_size = 0;
      int64_t index   = 0;
      const ObColumnPair* column_pair = NULL;

      wt_size = syschecker_schema_.get_wt_column_count();
      if (wt_size <= 0)
      {
        TBSYS_LOG(WARN, "invalid wide table column count, wt_clumn_count=%ld, "
                        "gen_cell_type=GEN_RANDOM", wt_size);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        if (operate_full_row_)
        {
          index = column_idx;
        }
        else
        {
          index = random_num % wt_size;
        }
        column_pair = syschecker_schema_.get_wt_column(index);
        if (NULL != column_pair)
        {
          ret = set_column_name(*column_pair, org_cell, aux_cell);
        }
        else
        {
          TBSYS_LOG(WARN, "can't get column pair from wide table, "
                          "wt_size=%ld, index=%ld, column_pair=%p",
                    wt_size, index, column_pair);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_cell_type(*column_pair, org_cell, aux_cell);
        if (OB_SUCCESS == ret && !is_read)
        {
          ret = set_cell_value(*column_pair, org_cell, aux_cell, random_num);
        }
      }

      return ret;
    }

    int ObSyscheckerRule::set_random_jt_cell(ObOpCellParam& org_cell,
                                             ObOpCellParam& aux_cell,
                                             const uint64_t random_num,
                                             const bool is_read,
                                             const int64_t column_idx)
    {
      int ret         = OB_SUCCESS;
      int64_t jt_size = 0;
      int64_t index   = 0;
      const ObColumnPair* column_pair = NULL;

      jt_size = syschecker_schema_.get_jt_column_count();
      if (jt_size <= 0)
      {
        TBSYS_LOG(WARN, "invalid join table column count, jt_clumn_count=%ld, "
                        "gen_cell_type=GEN_RANDOM", jt_size);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        if (operate_full_row_)
        {
          index = column_idx;
        }
        else
        {
          index = random_num % jt_size;
        }
        column_pair = syschecker_schema_.get_jt_column(index);
        if (NULL != column_pair)
        {
          ret = set_column_name(*column_pair, org_cell, aux_cell);
        }
        else
        {
          TBSYS_LOG(WARN, "can't get column pair from join table, "
                          "jt_size=%ld, index=%ld, column_pair=%p",
                    jt_size, index, column_pair);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_cell_type(*column_pair, org_cell, aux_cell);
        if (OB_SUCCESS == ret && !is_read)
        {
          ret = set_cell_value(*column_pair, org_cell, aux_cell, random_num);
        }
      }

      return ret;
    }

    int ObSyscheckerRule::set_valid_wwt_cell(ObOpCellParam& org_cell,
                                             ObOpCellParam& aux_cell,
                                             const uint64_t random_num,
                                             const int64_t column_idx)
    {
      int ret           = OB_SUCCESS;
      int64_t wwt_size  = 0;
      int64_t index     = 0;
      const ObColumnPair* column_pair = NULL;

      wwt_size = syschecker_schema_.get_wt_writable_column_count();
      if (wwt_size <= 0)
      {
        TBSYS_LOG(WARN, "invalid wide table writable column count, "
                        "wwt_clumn_count=%ld, gen_cell_type=GEN_VALID_WRITE",
                  wwt_size);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        if (operate_full_row_)
        {
          index = column_idx;
        }
        else
        {
          index = random_num % wwt_size;
        }
        column_pair = syschecker_schema_.get_wt_writable_column(index);
        if (NULL != column_pair)
        {
          ret = set_column_name(*column_pair, org_cell, aux_cell);
        }
        else
        {
          TBSYS_LOG(WARN, "can't get column pair from wide table writable column, "
                          "wwt_size=%ld, index=%ld, column_pair=%p",
                    wwt_size, index, column_pair);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_cell_type(*column_pair, org_cell, aux_cell);
        if (OB_SUCCESS == ret)
        {
          ret = set_cell_value(*column_pair, org_cell, aux_cell, random_num);
        }
      }

      return ret;
    }

    int ObSyscheckerRule::set_valid_jwt_cell(ObOpCellParam& org_cell,
                                             ObOpCellParam& aux_cell,
                                             const uint64_t random_num,
                                             const int64_t column_idx)
    {
      int ret           = OB_SUCCESS;
      int64_t jwt_size  = 0;
      int64_t index     = 0;
      const ObColumnPair* column_pair = NULL;

      jwt_size = syschecker_schema_.get_jt_writable_column_count();
      if (jwt_size <= 0)
      {
        TBSYS_LOG(WARN, "invalid join table writable column count, "
                        "jwt_clumn_count=%ld, gen_cell_type=GEN_VALID_WRITE",
                  jwt_size);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        if (operate_full_row_)
        {
          index = column_idx;
        }
        else
        {
          index = random_num % jwt_size;
        }
        column_pair = syschecker_schema_.get_jt_writable_column(index);
        if (NULL != column_pair)
        {
          ret = set_column_name(*column_pair, org_cell, aux_cell);
        }
        else
        {
          TBSYS_LOG(WARN, "can't get column pair from join table writable column, "
                          "jwt_size=%ld, index=%ld, column_pair=%p",
                    jwt_size, index, column_pair);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_cell_type(*column_pair, org_cell, aux_cell);
        if (OB_SUCCESS == ret)
        {
          ret = set_cell_value(*column_pair, org_cell, aux_cell, random_num);
        }
      }

      return ret;
    }

    int ObSyscheckerRule::set_invalid_wut_cell(ObOpCellParam& org_cell,
                                               ObOpCellParam& aux_cell,
                                               const uint64_t random_num,
                                               const int64_t column_idx)
    {
      int ret           = OB_SUCCESS;
      int64_t wut_size  = 0;
      int64_t index     = 0;
      const ObColumnPair* column_pair = NULL;

      wut_size = syschecker_schema_.get_wt_unwritable_column_count();
      if (wut_size <= 0)
      {
        TBSYS_LOG(WARN, "invalid wide table unwritable column count, "
                        "wut_clumn_count=%ld, gen_cell_type=GEN_INVALID_WRITE",
                  wut_size);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        if (operate_full_row_)
        {
          index = column_idx;
        }
        else
        {
          index = random_num % wut_size;
        }
        column_pair = syschecker_schema_.get_wt_unwritable_column(index);
        if (NULL != column_pair)
        {
          ret = set_column_name(*column_pair, org_cell, aux_cell);
        }
        else
        {
          TBSYS_LOG(WARN, "can't get column pair from wide table unwritable "
                          "wut_size=%ld, index=%ld, column_pair=%p",
                    wut_size, index, column_pair);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_cell_type(*column_pair, org_cell, aux_cell);
        if (OB_SUCCESS == ret)
        {
          ret = set_cell_value(*column_pair, org_cell, aux_cell, random_num);
        }
      }

      return ret;
    }

    int ObSyscheckerRule::set_invalid_jut_cell(ObOpCellParam& org_cell,
                                               ObOpCellParam& aux_cell,
                                               const uint64_t random_num,
                                               const int64_t column_idx)
    {
      int ret           = OB_SUCCESS;
      int64_t jut_size  = 0;
      int64_t index     = 0;
      const ObColumnPair* column_pair = NULL;

      jut_size = syschecker_schema_.get_jt_unwritable_column_count();
      if (jut_size <= 0)
      {
        TBSYS_LOG(WARN, "invalid join table uwritable column count, "
                        "jut_clumn_count=%ld, gen_cell_type=GEN_INVALID_WRITE",
                  jut_size);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        if (operate_full_row_)
        {
          index = column_idx;
        }
        else
        {
          index = random_num % jut_size;
        }
        column_pair = syschecker_schema_.get_jt_unwritable_column(index);
        if (NULL != column_pair)
        {
          ret = set_column_name(*column_pair, org_cell, aux_cell);
        }
        else
        {
          TBSYS_LOG(WARN, "can't get column pair from join table unwritable column, "
                          "jut_size=%ld, index=%ld, column_pair=%p",
                    jut_size, index, column_pair);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_cell_type(*column_pair, org_cell, aux_cell);
        if (OB_SUCCESS == ret)
        {
          ret = set_cell_value(*column_pair, org_cell, aux_cell, random_num);
        }
      }

      return ret;
    }

    int ObSyscheckerRule::set_valid_wat_cell(ObOpCellParam& org_cell,
                                             ObOpCellParam& aux_cell,
                                             const uint64_t random_num,
                                             const int64_t column_idx)
    {
      int ret           = OB_SUCCESS;
      int64_t wat_size  = 0;
      int64_t index     = 0;
      const ObColumnPair* column_pair = NULL;

      wat_size = syschecker_schema_.get_wt_addable_column_count();
      if (wat_size <= 0)
      {
        TBSYS_LOG(WARN, "invalid wide table addable column count, "
                        "wat_clumn_count=%ld, gen_cell_type=GEN_VALID_ADD",
                  wat_size);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        if (operate_full_row_)
        {
          index = column_idx;
        }
        else
        {
          index = random_num % wat_size;
        }
        column_pair = syschecker_schema_.get_wt_addable_column(index);
        if (NULL != column_pair)
        {
          ret = set_column_name(*column_pair, org_cell, aux_cell);
        }
        else
        {
          TBSYS_LOG(WARN, "can't get column pair from wide table addable column, "
                          "wat_size=%ld, index=%ld, column_pair=%p",
                    wat_size, index, column_pair);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_cell_type(*column_pair, org_cell, aux_cell);
        if (OB_SUCCESS == ret)
        {
          ret = set_cell_value(*column_pair, org_cell, aux_cell, random_num);
        }
      }

      return ret;
    }

    int ObSyscheckerRule::set_valid_jat_cell(ObOpCellParam& org_cell,
                                             ObOpCellParam& aux_cell,
                                             const uint64_t random_num,
                                             const int64_t column_idx)
    {
      int ret           = OB_SUCCESS;
      int64_t jat_size  = 0;
      int64_t index     = 0;
      const ObColumnPair* column_pair = NULL;

      jat_size = syschecker_schema_.get_jt_addable_column_count();
      if (jat_size <= 0)
      {
        TBSYS_LOG(WARN, "invalid join table addable column count, "
                        "jat_clumn_count=%ld, gen_cell_type=GEN_VALID_ADD",
                  jat_size);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        if (operate_full_row_)
        {
          index = column_idx;
        }
        else
        {
          index = random_num % jat_size;
        }
        column_pair = syschecker_schema_.get_jt_addable_column(index);
        if (NULL != column_pair)
        {
          ret = set_column_name(*column_pair, org_cell, aux_cell);
        }
        else
        {
          TBSYS_LOG(WARN, "can't get column pair from join table writable column, "
                          "jat_size=%ld, index=%ld, column_pair=%p",
                    jat_size, index, column_pair);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_cell_type(*column_pair, org_cell, aux_cell);
        if (OB_SUCCESS == ret)
        {
          ret = set_cell_value(*column_pair, org_cell, aux_cell, random_num);
        }
      }

      return ret;
    }

    int ObSyscheckerRule::check_addable_cell_value(ObOpCellParam& org_cell,
                                                   ObOpCellParam& aux_cell)
    {
      int ret = OB_SUCCESS;

      switch (org_cell.cell_type_)
      {
      case ObIntType:
        if (org_cell.value_.int_val_ + aux_cell.value_.int_val_ != 0)
        {
          ret = OB_ERROR;
        }
        break;
      case ObFloatType:
        if (org_cell.value_.float_val_ - aux_cell.value_.float_val_ != 0)
        {
          ret = OB_ERROR;
        }
        break;
      case ObDoubleType:
        if (org_cell.value_.double_val_ - aux_cell.value_.double_val_ != 0)
        {
          ret = OB_ERROR;
        }
        break;
      case ObDateTimeType:
        if (org_cell.value_.time_val_ + aux_cell.value_.time_val_ != 0)
        {
          ret = OB_ERROR;
        }
        break;
      case ObPreciseDateTimeType:
        if (org_cell.value_.precisetime_val_ + aux_cell.value_.precisetime_val_ != 0)
        {
          ret = OB_ERROR;
        }
        break;
      case ObVarcharType:
        if (org_cell.value_.varchar_val_ + aux_cell.value_.int_val_ != 0)
        {
          ret = OB_ERROR;
        }
        break;
      case ObCreateTimeType:
        if (org_cell.value_.createtime_val_ + aux_cell.value_.int_val_ != 0)
        {
          ret = OB_ERROR;
        }
        break;
      case ObModifyTimeType:
        if (org_cell.value_.modifytime_val_ + aux_cell.value_.int_val_ != 0)
        {
          ret = OB_ERROR;
        }
        break;
      case ObSeqType:
      case ObExtendType:
      case ObMaxType:
      case ObMinType:
      case ObNullType:
        TBSYS_LOG(WARN, "wrong addable object type %s",
                  OBJ_TYPE_STR[org_cell.cell_type_]);
        ret = OB_ERROR;
        break;
      default:
        TBSYS_LOG(WARN, "unknown object type %s",
                  OBJ_TYPE_STR[org_cell.cell_type_]);
        ret = OB_ERROR;
        break;
      }

      return ret;
    }

    int ObSyscheckerRule::check_new_cell_value(ObOpCellParam& org_cell,
                                               ObOpCellParam& aux_cell)
    {
      int ret               = OB_SUCCESS;
      int64_t varchar_hash  = 0;
      struct MurmurHash2 hash;

      switch (org_cell.cell_type_)
      {
      case ObNullType:
        if (aux_cell.value_.int_val_ != 0)
        {
          ret = OB_ERROR;
        }
        break;
      case ObIntType:
        if (org_cell.value_.int_val_ + aux_cell.value_.int_val_ != 0)
        {
          ret = OB_ERROR;
        }
        break;
      case ObFloatType:
        if (org_cell.value_.float_val_ != aux_cell.value_.float_val_)
        {
          ret = OB_ERROR;
        }
        break;
      case ObDoubleType:
        if (org_cell.value_.double_val_ != aux_cell.value_.double_val_)
        {
          ret = OB_ERROR;
        }
        break;
      case ObDateTimeType:
        if (org_cell.value_.time_val_ + aux_cell.value_.int_val_ != 0)
        {
          ret = OB_ERROR;
        }
        break;
      case ObPreciseDateTimeType:
        if (org_cell.value_.precisetime_val_ + aux_cell.value_.int_val_ != 0)
        {
          ret = OB_ERROR;
        }
        break;
      case ObVarcharType:
        varchar_hash = hash(org_cell.value_.varchar_val_, org_cell.varchar_len_);
        if (aux_cell.value_.int_val_ + varchar_hash != 0)
        {
          ret = OB_ERROR;
        }
        break;
      case ObCreateTimeType:
        if (org_cell.value_.createtime_val_ + aux_cell.value_.createtime_val_ != 0)
        {
          ret = OB_ERROR;
        }
        break;
      case ObModifyTimeType:
        if (org_cell.value_.modifytime_val_ + aux_cell.value_.modifytime_val_ != 0)
        {
          ret = OB_ERROR;
        }
        break;
      case ObSeqType:
      case ObExtendType:
      case ObMaxType:
      case ObMinType:
        TBSYS_LOG(WARN, "wrong update object type %s",
                  OBJ_TYPE_STR[org_cell.cell_type_]);
        ret = OB_ERROR;
        break;
      default:
        TBSYS_LOG(WARN, "unknown object type %s",
                  OBJ_TYPE_STR[org_cell.cell_type_]);
        ret = OB_ERROR;
        break;
      }

      return ret;
    }

    int ObSyscheckerRule::check_cell_value(ObOpCellParam& org_cell,
                                           ObOpCellParam& aux_cell)
    {
      int ret = OB_SUCCESS;

      switch (org_cell.op_type_)
      {
      case OP_ADD:
        ret = check_addable_cell_value(org_cell, aux_cell);
        break;
      case OP_UPDATE:
      case OP_INSERT:
        ret = check_new_cell_value(org_cell, aux_cell);
        break;
      default:
        break;
      }

      return ret;
    }

    int ObSyscheckerRule::set_cell_pair(const ObOpParam& op_param,
                                        const ObOpRowParam& row_param,
                                        ObOpCellParam& org_cell,
                                        ObOpCellParam& aux_cell,
                                        const uint64_t random_num,
                                        const int64_t column_idx)
    {
      int ret = OB_SUCCESS;
      UNUSED(row_param);

      //change GEN_VALID_WRITE to GEN_VALID_ADD for add operation
      if (OP_ADD == org_cell.op_type_ && OP_ADD == aux_cell.op_type_
          && GEN_VALID_WRITE == op_param.param_gen_.gen_cell_)
      {
        org_cell.gen_cell_ = GEN_VALID_ADD;
        aux_cell.gen_cell_ = GEN_VALID_ADD;
      }
      else
      {
        org_cell.gen_cell_ = op_param.param_gen_.gen_cell_;
        aux_cell.gen_cell_ = op_param.param_gen_.gen_cell_;
      }

      switch (org_cell.gen_cell_)
      {
      case GEN_RANDOM:
        if (op_param.is_wide_table_)
        {
          ret = set_random_wt_cell(org_cell, aux_cell,
                                   random_num, op_param.is_read_, column_idx);
        }
        else
        {
          ret = set_random_jt_cell(org_cell, aux_cell,
                                   random_num, op_param.is_read_, column_idx);
        }
        break;

      case GEN_VALID_WRITE:
        if (!op_param.is_read_)
        {
          //only choose writable column
          if (op_param.is_wide_table_)
          {
            ret = set_valid_wwt_cell(org_cell, aux_cell, random_num, column_idx);
          }
          else
          {
            ret = set_valid_jwt_cell(org_cell, aux_cell, random_num, column_idx);
          }
        }
        else{
          TBSYS_LOG(WARN, "read operation specify GEN_VALID_WRITE");
          ret = OB_ERROR;
        }
        break;

      case GEN_INVALID_WRITE:
        if (!op_param.is_read_)
        {
          //only choose unwitable column
          if (op_param.is_wide_table_)
          {
            ret = set_invalid_wut_cell(org_cell, aux_cell, random_num, column_idx);
          }
          else
          {
            ret = set_invalid_jut_cell(org_cell, aux_cell, random_num, column_idx);
          }
        }
        else{
          TBSYS_LOG(WARN, "read operation specify GEN_INVALID_WRITE");
          ret = OB_ERROR;
        }
        break;

      case GEN_VALID_ADD:
        if (OP_ADD == org_cell.op_type_ )
        {
          //only choose addable column
          if (op_param.is_wide_table_)
          {
            ret = set_valid_wat_cell(org_cell, aux_cell, random_num, column_idx);
          }
          else
          {
            ret = set_valid_jat_cell(org_cell, aux_cell, random_num, column_idx);
          }
        }
        else{
          TBSYS_LOG(WARN, "not add operation specify GEN_VALID_ADD");
          ret = OB_ERROR;
        }
        break;

      case GEN_SPECIFIED:
        //nothing to do, we will check the vlaue of cell later
        break;

      case GEN_SEQ:
      case GEN_COMBO_RANDOM:
        TBSYS_LOG(WARN, "not implement type(%s) for generating row cell",
                  OP_GEN_MODE_STR[op_param.param_gen_.gen_cell_]);
        ret = OB_ERROR;
        break;

      default:
        TBSYS_LOG(WARN, "uknown generation type=%s",
                  OP_GEN_MODE_STR[op_param.param_gen_.gen_cell_]);
        ret = OB_ERROR;
        break;
      }

      if (OB_SUCCESS == ret)
      {
        //check if cell is valid
        ret = check_cell_value(org_cell, aux_cell);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "spedified wrong cell value");
        }
      }

      return ret;
    }

    int ObSyscheckerRule::set_cells_info(const ObOpParam& op_param,
                                         ObOpRowParam& row_param,
                                         const int64_t prefix,
                                         const uint64_t random_num)
    {
      int ret                 = OB_SUCCESS;
      ObOpCellParam* org_cell = NULL;
      ObOpCellParam* aux_cell = NULL;
      uint64_t prime_index    = random_num % PRIME_NUM_COUNT;
      uint64_t new_random     = random_num * PRIME_NUM[prime_index];

      for (int64_t j = 0;
           j < row_param.cell_count_ && OB_SUCCESS == ret;
           j += MIN_OP_CELL_COUNT)
      {
        prime_index = (new_random + j) % PRIME_NUM_COUNT;
        new_random = new_random * PRIME_NUM[prime_index];
        org_cell = &row_param.cell_[j];
        aux_cell = &row_param.cell_[j + 1];
        set_cell_op_type(row_param, *org_cell, new_random);
        set_cell_op_type(row_param, *aux_cell, new_random);
        org_cell->key_prefix_ = prefix;
        aux_cell->key_prefix_ = prefix;
        ret = set_cell_pair(op_param, row_param, *org_cell,
                            *aux_cell, new_random, j / 2);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "set_cell_pair failed, column_idx=%ld, cell_count=%d",
                    j / 2, row_param.cell_count_);
        }
      }

      return ret;
    }

    int ObSyscheckerRule::set_rows_info(ObOpParam& op_param,
                                        const uint64_t random_num)
    {
      int ret                 = OB_SUCCESS;
      ObOpRowParam* row_param = NULL;
      int64_t prefix          = 0;
      int64_t suffix          = 0;
      uint64_t prime_index    = random_num % PRIME_NUM_COUNT;
      uint64_t new_random     = random_num * PRIME_NUM[prime_index];

      ret = set_row_count(op_param, random_num);
      if (OB_SUCCESS == ret)
      {
        for (int64_t i = 0;
             i < op_param.row_count_ && OB_SUCCESS == ret; ++i)
        {
          prime_index = (new_random + i) % PRIME_NUM_COUNT;
          new_random = new_random * PRIME_NUM[prime_index];
          row_param = &op_param.row_[i];
          set_row_op_type(op_param, *row_param, new_random);

          ret = set_row_key(op_param, *row_param, new_random,
                            prefix, suffix);
          if (OB_SUCCESS == ret)
          {
            if (OP_DELETE == row_param->op_type_)
            {
              //delete operation needn't set cell info
              continue;
            }

            //for scan operation only the first row has cells info
            if (OP_SCAN == op_param.op_type_ && i >= 1)
            {
              continue;
            }
            ret = set_cell_count_per_row(op_param, *row_param, new_random);
          }

          if (OB_SUCCESS == ret)
          {
            if (op_param.is_wide_table_)
            {
              ret = set_cells_info(op_param, *row_param, prefix, new_random);
            }
            else
            {
              ret = set_cells_info(op_param, *row_param, suffix, new_random);
            }
          }
        }
      }

      return ret;
    }

    int ObSyscheckerRule::set_specify_param(ObOpParam& op_param)
    {
      int ret = OB_SUCCESS;

      if (!op_param.loaded_)
      {
        ret = op_param.load_param_from_file(READ_PARAM_FILE);
      }

      return ret;
    }

    int ObSyscheckerRule::get_next_write_param(ObOpParam& write_param)
    {
      int ret             = OB_SUCCESS;
      uint64_t random_num = random();
      ObOpRule* rule      = NULL;

      write_param.reset();

      rule = &write_rule_[random_num % write_rule_count_];
      write_param.invalid_op_ = rule->invalid_op_;
      write_param.is_read_ = false;
      write_param.param_gen_ = rule->param_gen_;

      ret = set_op_type(write_param, random_num);
      if (OB_SUCCESS == ret)
      {
        ret = set_table_name(write_param, random_num);
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_rows_info(write_param, random_num);
      }

      return ret;
    }

    int ObSyscheckerRule::get_next_read_param(ObOpParam& read_param)
    {
      int ret             = OB_SUCCESS;
      uint64_t random_num = random();
      ObOpRule* rule      = NULL;

      if (is_specified_read_param_)
      {
        //for debuging
        ret = set_specify_param(read_param);
      }
      else
      {
        read_param.reset();
        rule = &read_rule_[random_num % read_rule_count_];
        read_param.invalid_op_ = rule->invalid_op_;
        read_param.is_read_ = true;
        read_param.param_gen_ = rule->param_gen_;

        ret = set_op_type(read_param, random_num);
        if (OB_SUCCESS == ret)
        {
          ret = set_table_name(read_param, random_num);
        }

        if (OB_SUCCESS == ret)
        {
          ret = set_rows_info(read_param, random_num);
        }
      }

      return ret;
    }

    const int64_t ObSyscheckerRule::get_cur_max_prefix() const
    {
      return cur_max_prefix_;
    }

    int64_t ObSyscheckerRule::add_cur_max_prefix(const int64_t prefix)
    {
      return atomic_add(&cur_max_prefix_, prefix);
    }

    const int64_t ObSyscheckerRule::get_cur_max_suffix() const
    {
      return cur_max_suffix_;
    }

    int64_t ObSyscheckerRule::add_cur_max_suffix(const int64_t suffix)
    {
      return atomic_add(&cur_max_suffix_, suffix);
    }

    void hex_dump_rowkey(const void* data, const int32_t size,
                         const bool char_type)
    {
      /* dumps size bytes of *data to stdout. Looks like:
       * [0000] 75 6E 6B 6E 6F 77 6E 20
       * 30 FF 00 00 00 00 39 00 unknown 0.....9.
       * (in a single line of course)
       */

      unsigned const char *p = (unsigned char*)data;
      unsigned char c = 0;
      int n = 0;
      char bytestr[4] = {0};
      char addrstr[10] = {0};
      char hexstr[ 16*3 + 5] = {0};
      char charstr[16*1 + 5] = {0};

      for(n = 1; n <= size; n++)
      {
        if (n%16 == 1)
        {
          /* store address for this line */
          snprintf(addrstr, sizeof(addrstr), "%.4x",
              (int)((unsigned long)p-(unsigned long)data) );
        }

        c = *p;
        if (isprint(c) == 0)
        {
          c = '.';
        }

        /* store hex str (for left side) */
        snprintf(bytestr, sizeof(bytestr), "%02X ", *p);
        strncat(hexstr, bytestr, sizeof(hexstr)-strlen(hexstr)-1);

        /* store char str (for right side) */
        snprintf(bytestr, sizeof(bytestr), "%c", c);
        strncat(charstr, bytestr, sizeof(charstr)-strlen(charstr)-1);

        if (n % 16 == 0)
        {
          /* line completed */
          if (char_type)
            fprintf(stderr, "[%4.4s]   %-50.50s  %s\n",
                addrstr, hexstr, charstr);
          else
            fprintf(stderr, "[%4.4s]   %-50.50s\n",
                addrstr, hexstr);
          hexstr[0] = 0;
          charstr[0] = 0;
        }
        else if(n % 8 == 0)
        {
          /* half line: add whitespaces */
          strncat(hexstr, "  ", sizeof(hexstr)-strlen(hexstr)-1);
          strncat(charstr, " ", sizeof(charstr)-strlen(charstr)-1);
        }
        p++; /* next byte */
      }

      if (strlen(hexstr) > 0)
      {
        /* print rest of buffer if not empty */
        if (char_type)
          fprintf(stderr, "[%4.4s]   %-50.50s  %s\n",
              addrstr, hexstr, charstr);
        else
          fprintf(stderr, "[%4.4s]   %-50.50s\n",
              addrstr, hexstr);
      }
    }
  } // end namespace syschecker
} // end namespace oceanbase
