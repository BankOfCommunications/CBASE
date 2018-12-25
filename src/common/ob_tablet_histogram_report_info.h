/*
 *author:zhuyanchao [secondary_index_static_data_build.report]
 * 20150320
 *
 *
*/
#ifndef OCEANBASE_COMMON_OB_TABLET_HISTOGRAM_REPORT_INFO_H_
#define OCEANBASE_COMMON_OB_TABLET_HISTOGRAM_REPORT_INFO_H_

#include <tblog.h>
#include "ob_define.h"
#include "ob_server.h"
#include "ob_array_helper.h"
#include "page_arena.h"
#include "ob_range2.h"
//add wenghaixing [secondary index col checksum.h]20141210
#include "common/ob_column_checksum.h"
//add e
//add zhuyanchao [secondary index static data build]20150320
#include "common/ob_tablet_info.h"
#include "common/ob_tablet_histogram.h"
//add e

namespace oceanbase
{
  namespace common
  {
    struct ObTabletHistogramReportInfo
    {
      ObTabletInfo tablet_info_;
      ObTabletLocation tablet_location_;
      ObTabletHistogram static_index_histogram_;
      bool operator== (const ObTabletReportInfo &other) const;
      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    struct ObTabletHistogramReportInfoList
    {
      ObTabletHistogramReportInfo tablets_[OB_MAX_TABLET_NUMBER_PER_TABLE_CS];
      ObArrayHelper<ObTabletHistogramReportInfo> tablet_list_;
      ModuleArena allocator_;
      mutable tbsys::CRWLock lock_;
      ObTabletHistogramReportInfoList()
      {
        reset();
      }

      void reset()
      {
        tablet_list_.init(OB_MAX_TABLET_NUMBER_PER_TABLE_CS, tablets_);
        tablet_list_.clear();
      }

      int get_tablet_histogram(ObTabletHistogramReportInfo &tablets,int64_t index)
      {
        int ret = OB_SUCCESS;
        if(index >=0 && index < tablet_list_.get_array_index())
        {
          tablets=tablets_[index];
          //del liumz, [remove OB_ITER_END]20160314
          /*if(index == (tablet_list_.get_array_index()-1))
          {
            ret = OB_ITER_END;
          }*/
        }
        //add liumz, [add else branch]20160314:b
        else
        {
          ret = OB_INVALID_INDEX;
        }
        //add:e
        return ret;
      }

      inline int add_tablet(const ObTabletHistogramReportInfo& tablet)
      {
        int ret = OB_SUCCESS;
        tbsys::CWLockGuard guard(lock_);
        if (!tablet_list_.push_back(tablet))
          ret = OB_ARRAY_OUT_OF_RANGE;

        return ret;
      }
      NEED_SERIALIZE_AND_DESERIALIZE;
    };

  } // end namespace common
} // end namespace oceanbase

#endif //OCEANBASE_ROOTSERVER_COMMONDATA_H_
