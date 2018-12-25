/**
 * (C) 2010-2011 Taobao Inc.
 *
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_adgroup_bidword_effect_orderby.h for define adgroup 
 * bidword effect oderby query classs. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_OLAPDRIVE_ADGROUP_BIDWORD_EFFECT_ORDERBY_H
#define OCEANBASE_OLAPDRIVE_ADGROUP_BIDWORD_EFFECT_ORDERBY_H

#include "ob_lz_query.h"

namespace oceanbase 
{ 
  namespace olapdrive
  {
    class ObAdgroupBidwordEffectOrderby : public ObLzQuery
    {
    public:
      ObAdgroupBidwordEffectOrderby(
        client::ObClient& client, ObOlapdriveMeta& meta,
        ObOlapdriveSchema& schema, ObOlapdriveParam& param,
        ObOlapdriveStat& stat);
      virtual ~ObAdgroupBidwordEffectOrderby();

    public:
      virtual int check(const common::ObScanner &scanner, const int64_t time);
      virtual int add_special_column(common::ObScanParam& scan_param);
      virtual const char* get_test_name();

    private:
      int64_t get_adgroup_row_count(const ObScanInfo& scan_info,
                                    const ObAdgroup& adgroup);
      void get_next_bidword_id(const ObScanInfo& scan_info, 
                               const ObAdgroup& adgroup, 
                               int64_t& bidword_id);

    private:
      DISALLOW_COPY_AND_ASSIGN(ObAdgroupBidwordEffectOrderby);
    };
  } // end namespace olapdrive
} // end namespace oceanbase

#endif //OCEANBASE_OLAPDRIVE_ADGROUP_BIDWORD_EFFECT_ORDERBY_H
