#include "task_stats.h"
#include "common/serialization.h"
#include "common/utility.h"

using namespace oceanbase::common;

DEFINE_SERIALIZE(TaskStats)
{
  int ret = serialization::encode_vi64(buf, buf_len, pos, stats_.size()); /* vector size */
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "can't serialize vector size, pos=%ld, size=%ld", pos, buf_len);
  }

  for(size_t i = 0;ret == OB_SUCCESS && i < stats_.size(); i++)
  {
    TaskStatItem item = stats_[i];
    ret = serialization::encode_i8(buf, buf_len, pos, item.stat_type);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "can't serialize TaskStats pos=%ld, size=%ld", pos, buf_len);
    }
    else
    {
      ret = serialization::encode_vi64(buf, buf_len, pos, item.stat_value);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "can't serialize item.stat_value, pos=%ld, size=%ld", pos, buf_len);
      }
    }
  }

  return ret;
}

DEFINE_DESERIALIZE(TaskStats)
{
  int64_t vec_size = 0;
  stats_.clear();

  int ret = serialization::decode_vi64(buf, data_len, pos, &vec_size); /* vector size */
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "can't serialize vector size, pos=%ld, size=%ld", pos, data_len);
  }
  else
  {
    for(int64_t i = 0;i < vec_size; i++)
    {
      TaskStatItem item;
      ret = serialization::decode_i8(buf, data_len, pos, &item.stat_type);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "can't deserialize TaskStats pos=%ld, size=%ld", pos, data_len);
      }
      else
      {
        ret = serialization::decode_vi64(buf, data_len, pos, &item.stat_value);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "can't serialize item.stat_value, pos=%ld, size=%ld", pos, data_len);
        }
      }
      if (ret != OB_SUCCESS)
      {
        break;
      }

      stats_.push_back(item);
    }
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(TaskStats)
{
  int64_t total_size = 0;
  total_size += serialization::encoded_length_vi64(stats_.size());

  for(size_t i = 0;i < stats_.size(); i++)
  {
    total_size += serialization::encoded_length_i8(stats_[i].stat_type);
    total_size += serialization::encoded_length_vi64(stats_[i].stat_value);
  }

  return total_size;
}

void TaskStats::dump(int64_t task_id)
{
  TBSYS_LOG(INFO, "Outputing Task Stats, task id [%ld]", task_id);

  for(size_t i = 0;i < stats_.size(); i++)
  {
    if (stats_[i].stat_type == TASK_SUCC_SCAN_COUNT)
    {
      TBSYS_LOG(INFO, "TASK_SUCC_SCAN_COUNT = %ld", stats_[i].stat_value);
    }
    if (stats_[i].stat_type == TASK_FAILED_SCAN_COUNT)
    {
      TBSYS_LOG(INFO, "TASK_FAILED_SCAN_COUNT = %ld", stats_[i].stat_value);
    }
    if (stats_[i].stat_type == TASK_TOTAL_RECORD_COUNT)
    {
      TBSYS_LOG(INFO, "TASK_TOTAL_RECORD_COUNT = %ld", stats_[i].stat_value);
    }
    if (stats_[i].stat_type == TASK_TOTAL_WRITE_BYTES)
    {
      TBSYS_LOG(INFO, "TASK_TOTAL_WRITE_BYTES = %ld", stats_[i].stat_value);
    }
    if (stats_[i].stat_type == TASK_TOTAL_REPONSE_BYTES)
    {
      TBSYS_LOG(INFO, "TASK_TOTAL_REPONSE_BYTES = %ld", stats_[i].stat_value);
    }
    if (stats_[i].stat_type == TASK_TOTAL_REQUEST_BYTES)
    {
      TBSYS_LOG(INFO, "TASK_TOTAL_REQUEST_BYTES = %ld", stats_[i].stat_value);
    }
  }
}

