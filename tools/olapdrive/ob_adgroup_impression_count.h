/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_adgroup_impfession_count.h for define adgroup impfession
 * count query classs. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_OLAPDRIVE_ADGROUP_IMPFESSION_COUNT_H
#define OCEANBASE_OLAPDRIVE_ADGROUP_IMPFESSION_COUNT_H

#include "ob_lz_query.h"

namespace oceanbase 
{ 
  namespace olapdrive
  {
    class ObAdgroupImpressionCount : public ObLzQuery
    {
    public:
      ObAdgroupImpressionCount(
        client::ObClient& client, ObOlapdriveMeta& meta,
        ObOlapdriveSchema& schema, ObOlapdriveParam& param,
        ObOlapdriveStat& stat);
      virtual ~ObAdgroupImpressionCount();

    public:
      virtual int check(const common::ObScanner &scanner, const int64_t time);
      virtual int prepare(common::ObScanParam &scan_param);
      virtual const char* get_test_name();

    private:
      int build_basic_column(common::ObScanParam& scan_param);
      int add_filter_column(common::ObScanParam& scan_param);
      int add_groupby_column(common::ObScanParam& scan_param);

    private:
      DISALLOW_COPY_AND_ASSIGN(ObAdgroupImpressionCount);
    };
  } // end namespace olapdrive
} // end namespace oceanbase

#endif //OCEANBASE_OLAPDRIVE_ADGROUP_IMPFESSION_COUNT_H
