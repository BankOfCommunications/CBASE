/*
 *author:zhuyanchao [secondary_index_static_data_build.report]
 *date: 20150323
 **/
#include <algorithm>
#include "tbsys.h"
#include "utility.h"
#include "ob_tablet_histogram_report_info.h"
#include "ob_tablet_info.h"
#include "ob_tablet_histogram.h"

namespace oceanbase
{
  namespace common
  {
    // ObTabletHistogramReportInfo
    DEFINE_SERIALIZE(ObTabletHistogramReportInfo)
    {
      int ret = OB_SUCCESS;
      ret = tablet_info_.serialize(buf, buf_len, pos);
      if (ret == OB_SUCCESS)
        ret = tablet_location_.serialize(buf, buf_len, pos);
      if (ret == OB_SUCCESS)
          ret = static_index_histogram_.serialize(buf,buf_len,pos);
      return ret;
    }

    DEFINE_DESERIALIZE(ObTabletHistogramReportInfo)
    {
      int ret = OB_SUCCESS;
      ret = tablet_info_.deserialize(buf, data_len, pos);
      if (ret == OB_SUCCESS)
        ret = tablet_location_.deserialize(buf, data_len, pos);
      if (ret == OB_SUCCESS)
          ret = static_index_histogram_.deserialize(buf,data_len,pos);

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTabletHistogramReportInfo)
    {
      int64_t total_size = 0;

      total_size += tablet_info_.get_serialize_size();
      total_size += tablet_location_.get_serialize_size();
      total_size += static_index_histogram_.get_serialize_size();

      return total_size;
    }
    // ObTabletHistogramReportInfoList
    DEFINE_SERIALIZE(ObTabletHistogramReportInfoList)
    {
      int ret = OB_ERROR;

      int64_t size = tablet_list_.get_array_index();
      ret = serialization::encode_vi64(buf, buf_len, pos, size);
      TBSYS_LOG(DEBUG, "test::liumz, serialize size=%ld", size);

      if (ret == OB_SUCCESS)
      {
        for (int64_t i = 0; i < size; ++i)
        {
          ret = tablets_[i].serialize(buf, buf_len, pos);
          if (ret != OB_SUCCESS)
            break;
        }
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObTabletHistogramReportInfoList)
    {
      int ret = OB_ERROR;
      ObObj* ptr = NULL;

      int64_t size = 0;

      ret = serialization::decode_vi64(buf, data_len, pos, &size);
      TBSYS_LOG(DEBUG, "test::liumz, deserialize size=%ld", size);

      if (ret == OB_SUCCESS && size > 0)
      {
        for (int64_t i=0; i<size; ++i)
        {
          ObTabletHistogramReportInfo tablet;          
          ptr = reinterpret_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * OB_MAX_ROWKEY_COLUMN_NUMBER * 2));
          if (NULL == ptr)
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
          else
          {
            tablet.static_index_histogram_.set_allocator(&allocator_);
            tablet.tablet_info_.range_.start_key_.assign(ptr, OB_MAX_ROWKEY_COLUMN_NUMBER);
            tablet.tablet_info_.range_.end_key_.assign(ptr + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
            ret = tablet.deserialize(buf, data_len, pos);
            //tablet.static_index_histogram_.deep_copy(allocator_, tablet.static_index_histogram_);
          }
          if (ret != OB_SUCCESS)
            break;

          tablet_list_.push_back(tablet);
        }
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTabletHistogramReportInfoList)
    {
      int64_t total_size = 0;
      int64_t size = tablet_list_.get_array_index();
      total_size += serialization::encoded_length_vi64(size);

      if (size > 0)
      {
        for (int64_t i=0; i<size; ++i)
          total_size += tablets_[i].get_serialize_size();
      }

      return total_size;
    }
  }
}
