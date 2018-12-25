/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*   
*   
*   Version: 0.1 2010-09-08
*   
*   Authors:
*          daoan(daoan@taobao.com)
*   
*
================================================================*/
#include "rootserver/ob_server_balance_info.h"
namespace oceanbase
{
  using namespace common;
  namespace rootserver
  {
    ObServerDiskInfo::ObServerDiskInfo():capacity_(0), used_(0)
    {

    }
    void ObServerDiskInfo::set_capacity(const int64_t capacity)
    {
      capacity_ = capacity;
    }
    void ObServerDiskInfo::set_used(const int64_t used)
    {
      used_ = used;
    }
    void ObServerDiskInfo::add_used(const int64_t add_value)
    {
      used_ += add_value;
    }
    int64_t ObServerDiskInfo::get_capacity() const
    {
      return capacity_;
    }
    int64_t ObServerDiskInfo::get_used() const
    {
      return used_;
    }
    int32_t ObServerDiskInfo::get_percent() const
    {
      int ret = -1;
      if (capacity_ > 0)
      {
        ret =(int) (used_ * 100 / capacity_ );
        if (ret > 100)
        {
          ret = 100;
        }
      }
      return ret;
    }

    double ObServerDiskInfo::get_utilization_ratio() const
    {
      double ret = -1;
      if (capacity_ > 0)
      {
        ret = (double)used_ / (double)capacity_;
        if (0 > ret)
        {
          ret = -1;
          TBSYS_LOG(WARN, "invalid utilization, used=%ld capacity=%ld ratio=%f", used_, capacity_, ret);
        }
        else if (100 < ret)
        {
          ret = 100;
          TBSYS_LOG(WARN, "invalid utilization, used=%ld capacity=%ld ratio=%f", used_, capacity_, ret);
        }
      }
      return ret;
    }
    
    void ObServerDiskInfo::dump() const
    {
      TBSYS_LOG(INFO, "disk info capacity = %ld used = %ld ratio=%d", capacity_, used_, get_percent());
    }
    DEFINE_SERIALIZE(ObServerDiskInfo)
    {
      int ret = 0;
      int64_t tmp_pos = pos;
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, capacity_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, used_);
      }
      
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }
    DEFINE_DESERIALIZE(ObServerDiskInfo)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &capacity_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &used_);
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }
    DEFINE_GET_SERIALIZE_SIZE(ObServerDiskInfo)
    {
      int64_t len = 0;
      len += serialization::encoded_length_vi64(capacity_);
      len += serialization::encoded_length_vi64(used_);
      return len;
    }
  }
}
