
#include "tablet_location.h"

using namespace oceanbase::common;
using namespace oceanbase::tools;

TabletLocation::TabletLocation()
{
  cur_count_ = 0;
}

TabletLocation::~TabletLocation()
{
}

// list size is too small so using bubble sort asc
int TabletLocation::sort(const ObServer & server)
{
  // asc 
  int ret = OB_SUCCESS;
  if (cur_count_ > 0)
  {
    int32_t server_ip = server.get_ipv4();
    ObTabletLocation temp;
    for (int64_t i = cur_count_ - 1; i > 0; --i)
    {
      for (int64_t j = 0; j < i; ++j)
      {
        if (abs(locations_[j].chunkserver_.get_ipv4() - server_ip) > 
            abs(locations_[j+1].chunkserver_.get_ipv4() - server_ip)) 
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

int TabletLocation::del(const int64_t index, ObTabletLocation & location)
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

bool TabletLocation::operator == (const TabletLocation & other)
{
  bool ret = (cur_count_ == other.cur_count_);
  if (true == ret)
  {
    for (int64_t i = 0; i < cur_count_; ++i)
    {
      ret = ((locations_[i].chunkserver_ == other.locations_[i].chunkserver_)
          && (locations_[i].tablet_version_ == other.locations_[i].tablet_version_));
      if (false == ret)
      {
        break;
      }
    }
  }
  return ret;
}

int TabletLocation::add(const ObTabletLocation & location)
{
  int ret = OB_SUCCESS;
  if (cur_count_ < MAX_REPLICA_COUNT)
  {
    locations_[cur_count_] = location;
    ++cur_count_;
  }
  else
  {
    TBSYS_LOG(ERROR, "the items is full:count[%ld]", cur_count_);
    ret = OB_NO_EMPTY_ENTRY;
  }
  return ret;
}

void TabletLocation::print_info(void) const
{
  for (int64_t i = 0; i < cur_count_; ++i)
  {
    ObTabletLocation::dump(locations_[i]);
  }
}

// TabletLocation
DEFINE_SERIALIZE(TabletLocation)
{
  int ret = serialization::encode_vi64(buf, buf_len, pos, cur_count_);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "serialize cur count failed:count[%ld], ret[%d]", cur_count_, ret); 
  }
  else
  {
    for (int64_t i = 0; i < cur_count_; ++i)
    {
      ret = locations_[i].serialize(buf, buf_len, pos);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "serailize location failed:index[%ld], ret[%d]", i, ret);
        break;
      }
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(TabletLocation)
{
  int ret = serialization::decode_vi64(buf, data_len, pos, &cur_count_);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "deserialize cur count failed:ret[%d]", ret);
  }
  else
  {
    for (int64_t i = 0; i < cur_count_; ++i)
    {
      ret = locations_[i].deserialize(buf, data_len, pos);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "deserialize location failed:index[%ld], ret[%d]", i, ret);
        break;
      }
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(TabletLocation)
{
  int64_t total_size = 0;
  total_size += serialization::encoded_length_vi64(cur_count_);
  for (int64_t i = 0; i < cur_count_; ++i)
  {
    total_size += locations_[i].get_serialize_size();
  }
  return total_size;
}

