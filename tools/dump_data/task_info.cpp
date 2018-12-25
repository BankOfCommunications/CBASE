#include "task_info.h"

using namespace oceanbase::common;
using namespace oceanbase::tools;


DEFINE_SERIALIZE(TaskCounter)
{
  int ret = serialization::encode_vi64(buf, buf_len, pos, total_count_);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "serialize total count failed:count[%ld], ret[%d]", total_count_, ret);
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(buf, buf_len, pos, wait_count_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "serialize wait count failed:count[%ld], ret[%d]", wait_count_, ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(buf, buf_len, pos, doing_count_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "serialize doing count failed:count[%ld], ret[%d]", doing_count_, ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(buf, buf_len, pos, finish_count_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "serialize finish count failed:count[%ld], ret[%d]", finish_count_, ret);
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(TaskCounter)
{
  int ret = serialization::decode_vi64(buf, data_len, pos, &total_count_);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "deserialize total count failed:ret[%d]", ret);
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi64(buf, data_len, pos, &wait_count_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize wait count failed:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi64(buf, data_len, pos, &doing_count_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize wait count failed:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi64(buf, data_len, pos, &finish_count_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize finish count failed:ret[%d]", ret);
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(TaskCounter)
{
  int64_t total_size = 0;
  total_size += serialization::encoded_length_vi64(total_count_);
  total_size += serialization::encoded_length_vi64(wait_count_);
  total_size += serialization::encoded_length_vi64(doing_count_);
  total_size += serialization::encoded_length_vi64(finish_count_);
  return total_size;
}

int TaskInfo::set_param(const ObScanParam & param)
{
  // copy all names to param buffer
  int ret = OB_SUCCESS;
  int64_t size = 0;
  int64_t pos = 0;
  if (param_buffer_ != NULL)
  {
    dec_ref();
    param_buffer_ = NULL;
  }
  size = sizeof(ParamBuffer) + param.get_serialize_size();
  char * temp = (char *)ob_malloc(size);
  if (temp != NULL)
  {
    param_buffer_ = new(temp) ParamBuffer;
    if (NULL == param_buffer_)
    {
      TBSYS_LOG(ERROR, "check param buffer new failed:buffer[%p], temp[%p]", param_buffer_, temp);
      ret = OB_ERROR;
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "check ob malloc failed:buffer[%p], size[%ld]", param_buffer_, size);
    ret = OB_ERROR;
  }

  if (OB_SUCCESS == ret)
  {
    int ret = param.serialize(param_buffer_->buffer_, size, pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "serailize param failed:ret[%d]", ret);
    }
    else
    {
      pos = 0;
      ret = scan_param_.deserialize(param_buffer_->buffer_, size, pos);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "deserialize scan param failed:ret[%d]", ret);
      }
    }
  }
  return ret;
}


DEFINE_SERIALIZE(TaskInfo)
{
  int ret = serialization::encode_vi64(buf, buf_len, pos, timestamp_);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "serialize task timestamp failed:timestamp[%ld], ret[%d]", timestamp_, ret);
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(buf, buf_len, pos, task_token_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "serialize task token failed:token[%ld], ret[%d]", task_token_, ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(buf, buf_len, pos, task_id_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "serialize task id failed:task[%lu], ret[%d]", task_id_, ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(buf, buf_len, pos, task_limit_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "serialize task limit failed:limit[%lu], ret[%d]", task_limit_, ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(buf, buf_len, pos, first_index_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "serialize server index failed:index[%lu], ret[%d]", first_index_, ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(buf, buf_len, pos, table_id_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "serialize table_id failed:table_id[%lu], ret[%d]", table_id_, ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vstr(buf, buf_len, pos, table_name_.ptr(), table_name_.length());
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "serialize table name failed:ret[%d]",ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = servers_.serialize(buf, buf_len, pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "serialize location failed:task[%lu], ret[%d]", task_id_, ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = scan_param_.serialize(buf, buf_len, pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "serialize scan param failed:task[%lu], ret[%d]", task_id_, ret);
    }
  }

  return ret;
}

DEFINE_DESERIALIZE(TaskInfo)
{
  int ret = serialization::decode_vi64(buf, data_len, pos, &timestamp_);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "deserialize task timestamp failed:ret[%d]", ret);
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi64(buf, data_len, pos, &task_token_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize task token failed:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi64(buf, data_len, pos, (int64_t *)&task_id_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize task id failed:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi64(buf, data_len, pos, (int64_t *)&task_limit_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize task limit failed:task[%lu], ret[%d]", task_id_, ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi64(buf, data_len, pos, &first_index_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize first server index failed:task[%lu], ret[%d]", task_id_, ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi64(buf, data_len, pos, &table_id_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize table_id failed:task[%lu], ret[%d]", task_id_, ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    int64_t len = 0;
    const char * str = serialization::decode_vstr(buf, data_len, pos, &len);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserializ table name failed");
    }
    else
    {
      table_name_.assign_ptr(const_cast<char *>(str), int32_t(len));
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = servers_.deserialize(buf, data_len, pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize location failed:task[%lu], ret[%d]", task_id_, ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = scan_param_.deserialize(buf, data_len, pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize scan param failed:task[%lu], ret[%d]", task_id_, ret);
    }
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(TaskInfo)
{
  int64_t total_size = 0;
  total_size += serialization::encoded_length_vi64(timestamp_);
  total_size += serialization::encoded_length_vi64(task_token_);
  total_size += serialization::encoded_length_vi64(task_id_);
  total_size += serialization::encoded_length_vi64(task_limit_);
  total_size += serialization::encoded_length_vi64(first_index_);
  total_size += serialization::encoded_length_vi64(table_id_);
  total_size += servers_.get_serialize_size();
  total_size += scan_param_.get_serialize_size();
  return total_size;
}

