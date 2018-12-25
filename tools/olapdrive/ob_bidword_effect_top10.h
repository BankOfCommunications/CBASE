/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_bidword_effect_top10.h for define bidword effect top 10 
 * query classs. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_OLAPDRIVE_BIDWORD_EFFECT_TOP10_H
#define OCEANBASE_OLAPDRIVE_BIDWORD_EFFECT_TOP10_H

#include "ob_lz_query.h"

namespace oceanbase 
{ 
  namespace olapdrive
  {
    class ObBidwordEffectTop10 : public ObLzQuery
    {
    public:
      ObBidwordEffectTop10(client::ObClient& client, ObOlapdriveMeta& meta,
                           ObOlapdriveSchema& schema, ObOlapdriveParam& param,
                           ObOlapdriveStat& stat);
      virtual ~ObBidwordEffectTop10();

    public:
      virtual int check(const common::ObScanner &scanner, const int64_t time);
      virtual int add_special_column(common::ObScanParam& scan_param);
      virtual const char* get_test_name();

    private:
      void get_next_bidword_id(const ObScanInfo& scan_info, 
                               const ObCampaign& campaign, 
                               int64_t& bidword_id);
      int64_t calc_result_row_count(const ObCampaign& campaign, 
                                    const int64_t shop_type);

    private:
      DISALLOW_COPY_AND_ASSIGN(ObBidwordEffectTop10);
    };
  } // end namespace olapdrive
} // end namespace oceanbase

#endif //OCEANBASE_OLAPDRIVE_BIDWORD_EFFECT_TOP10_H
