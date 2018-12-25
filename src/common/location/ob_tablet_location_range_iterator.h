#ifndef OB_COMMON_LOCATION_RANGE_ITERATOR_H_
#define OB_COMMON_LOCATION_RANGE_ITERATOR_H_

#include "common/ob_define.h"
#include "common/ob_range2.h"
#include "common/ob_scan_param.h"
#include "common/ob_server.h"
#include "common/ob_chunk_server_item.h"
#include "common/ob_string_buf.h"
#include "ob_tablet_location_cache_proxy.h"
#include "ob_tablet_location_list.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace common
  {
    // get all the tablet according to given scan param
    class ObTabletLocationRangeIterator
    {
    public:
      ObTabletLocationRangeIterator();
      virtual ~ObTabletLocationRangeIterator();
    
    public:
      int initialize(ObTabletLocationCacheProxy * cache_proxy,
        const ObNewRange * iter_range, ScanFlag::Direction scan_direction, ObStringBuf * range_rowkey_buf);
      int next(common::ObChunkServerItem * replica_out, int32_t & replica_count_in_out,
        ObNewRange & tablet_range_out);
      inline bool end() const
      {
        return is_iter_end_;
      }

    public:
      // intersect two range r1, r2. intersect result is r3
      // if the intersect result is empty. return OB_EMPTY_RANGE, r3 will NOT be changed
      // @ObNewRange r1 range for intersect
      // @ObNewRange r2 range for intersect
      // @ObNewRange r3 range for intersect result
      // @ObStringBuf range_rowkey_buf rowkey buf for r3
      int range_intersect(const ObNewRange& r1, const ObNewRange& r2, 
        ObNewRange& r3, ObStringBuf& range_rowkey_buf) const;

    private:
      bool init_;
      bool is_iter_end_;
      const ObNewRange * org_scan_range_;        //origin scan parameter
      ScanFlag::Direction scan_direction_;
      ObNewRange next_range_;
      ObTabletLocationCacheProxy * cache_proxy_;
      ObStringBuf * range_rowkey_buf_;
    };
  }
}

#endif //OB_COMMON_LOCATION_RANGE_ITERATOR_H_

