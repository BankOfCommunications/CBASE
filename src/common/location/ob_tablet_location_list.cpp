
#include "ob_tablet_location_list.h"
#include "common/ob_tablet_info.h"
#include "common/serialization.h"

using namespace oceanbase::common;
using namespace oceanbase::common;


ObTabletLocationList::ObTabletLocationList()
{
  tablet_range_rowkey_buf_ = NULL;
  cur_count_ = 0;
  timestamp_ = 0;
}

ObTabletLocationList::~ObTabletLocationList()
{
}

ObTabletLocationList & ObTabletLocationList::operator = (const ObTabletLocationList & other)
{
  if (this != &other)
  {
    ObStringBuf * temp = tablet_range_rowkey_buf_;
    // using memcpy
    memcpy(this, &other, sizeof(ObTabletLocationList));
    tablet_range_rowkey_buf_ = temp;
  }
  return *this;
}

// list size is too small so using bubble sort asc
int ObTabletLocationList::sort(const ObServer & server)
{
  // asc 
  int ret = OB_SUCCESS;
  if (cur_count_ > 0)
  {
    int32_t server_ip = server.get_ipv4();
    ObTabletLocationItem temp;
    for (int64_t i = cur_count_ - 1; i > 0; --i)
    {
      for (int64_t j = 0; j < i; ++j)
      {
        if (abs(locations_[j].server_.chunkserver_.get_ipv4() - server_ip) > 
          abs(locations_[j+1].server_.chunkserver_.get_ipv4() - server_ip))
        {
          temp = locations_[j];
          locations_[j] = locations_[j+1];
          locations_[j+1] = temp;
        }
      }
    }
  }
  return ret;
}

void ObTabletLocationList::set_item_valid(const int64_t timestamp)
{
  timestamp_ = timestamp;
  for (int64_t i = 0; i < cur_count_; ++i)
  {
    locations_[i].err_times_ = 0;
  }
}

int64_t ObTabletLocationList::get_valid_count(void) const
{
  int64_t ret = 0;
  for (int64_t i = 0; i < cur_count_; ++i)
  {
    if (locations_[i].err_times_ < ObTabletLocationItem::MAX_ERR_TIMES)
    {
      ++ret;
    }
  }
  return ret;
}

int ObTabletLocationList::set_item_invalid(const ObTabletLocationItem & location)
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; i < cur_count_; ++i)
  {
    if (/*(locations_[i].server_.tablet_version_ == location.server_.tablet_version_)
        &&*/ (locations_[i].server_.chunkserver_ == location.server_.chunkserver_))
    {
      // set to max err times
      locations_[i].err_times_ = ObTabletLocationItem::MAX_ERR_TIMES;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}


int ObTabletLocationList::del(const int64_t index, ObTabletLocationItem & location)
{
  int ret = OB_SUCCESS;
  if ((index < 0) || (index >= cur_count_))
  {
    TBSYS_LOG(ERROR, "check index failed:index[%ld], count[%ld]", index, cur_count_);
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    location = locations_[index];
    if (index < (MAX_REPLICA_COUNT - 1))
    {
      memmove(&(locations_[index]), &(locations_[index + 1]), 
        (cur_count_ - index - 1) * sizeof(ObTabletLocation));
    }
    --cur_count_;
  }
  return ret;
} 


int ObTabletLocationList::add(const ObTabletLocation & location)
{
  int ret = OB_SUCCESS;
  if (cur_count_ < MAX_REPLICA_COUNT)
  {
    locations_[cur_count_].server_ = location;
    locations_[cur_count_].err_times_ = 0;
    ++cur_count_;
  }
  else
  {
    TBSYS_LOG(ERROR, "the items is full:count[%ld]", cur_count_);
    ret = OB_NO_EMPTY_ENTRY;
  }
  return ret;
}

void ObTabletLocationList::print_info(void) const
{
  TBSYS_LOG(DEBUG, "print tablet location servers:count[%ld], timestamp[%ld]",
    cur_count_, timestamp_);
  for (int64_t i = 0; i < cur_count_; ++i)
  {
    TBSYS_LOG(DEBUG, "check server error status:times[%lu]", locations_[i].err_times_);
    ObTabletLocation::dump(locations_[i].server_);
  }
}

// ObTabletLocationList
DEFINE_SERIALIZE(ObTabletLocationList)
{
  int ret = OB_SUCCESS;
  ret = serialization::encode_vi64(buf, buf_len, pos, timestamp_);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "serialize timestamp failed:timestamp[%ld], ret[%d]",
      timestamp_, ret);
  }
  else
  {
    ret = serialization::encode_vi64(buf, buf_len, pos, cur_count_);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize cur count failed:count[%ld], ret[%d]", 
        cur_count_, ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = tablet_range_.serialize(buf, buf_len, pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "serailize tablet range failed: ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    for (int64_t i = 0; i < cur_count_; ++i)
    {
      ret = serialization::encode_vi64(buf, buf_len, pos, locations_[i].err_times_);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "serialize err times failed:index[%ld], ret[%d]", i, ret);
        break;
      }
      else
      {
        ret = locations_[i].server_.serialize(buf, buf_len, pos);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "serailize location failed:index[%ld], ret[%d]", i, ret);
          break;
        }
      }
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObTabletLocationList)
{
  int ret = OB_SUCCESS;
  ObObj array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
  ObNewRange temp_range;
  temp_range.start_key_.assign(array, OB_MAX_ROWKEY_COLUMN_NUMBER);
  temp_range.end_key_.assign(array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);

  if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &timestamp_)))
  {
    TBSYS_LOG(ERROR, "deserialize timestamp failed:ret[%d], pos=%ld, data_len=%ld", ret, pos, data_len);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &cur_count_)))
  {
    TBSYS_LOG(ERROR, "deserialize cur count failed:ret[%d], pos=%ld, data_len=%ld", ret, pos, data_len);
  }
  else if (OB_SUCCESS != (ret = temp_range.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(ERROR, "deserialize tablet range failed: ret[%d], pos=%ld, data_len=%ld", ret, pos, data_len);
  }
  if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = set_tablet_range(temp_range))))
  {
    TBSYS_LOG(WARN,"fail to deep copy tablet range [err:%d], pos=%ld, data_len=%ld", ret, pos, data_len);
  }

  if (OB_SUCCESS == ret)
  {
    for (int64_t i = 0; i < cur_count_; ++i)
    {
      if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &(locations_[i].err_times_))))
      {
        TBSYS_LOG(ERROR, "deserialize err times failed:index[%ld], ret[%d], pos=%ld, data_len=%ld", i, ret, pos, data_len);
        break;
      }
      else if (OB_SUCCESS != (ret = locations_[i].server_.deserialize(buf, data_len, pos)))
      {
        TBSYS_LOG(ERROR, "deserialize location failed:index[%ld], ret[%d], pos=%ld, data_len=%ld", i, ret, pos, data_len);
        break;
      }
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObTabletLocationList)
{
  int64_t total_size = 0;
  total_size += serialization::encoded_length_vi64(timestamp_);
  total_size += serialization::encoded_length_vi64(cur_count_);
  total_size += tablet_range_.get_serialize_size();
  for (int64_t i = 0; i < cur_count_; ++i)
  {
    total_size += serialization::encoded_length_vi64(locations_[i].err_times_);
    total_size += locations_[i].server_.get_serialize_size();
  }
  return total_size;
}



