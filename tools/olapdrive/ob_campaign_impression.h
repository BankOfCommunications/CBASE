/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_campaign_impression.h for define campaign impression query
 * classs. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_OLAPDRIVE_CAMPAIGN_IMPRESSION_H
#define OCEANBASE_OLAPDRIVE_CAMPAIGN_IMPRESSION_H

#include "ob_lz_query.h"

namespace oceanbase 
{ 
  namespace olapdrive
  {
    class ObCampaignImpression : public ObLzQuery
    {
    public:
      ObCampaignImpression(client::ObClient& client, ObOlapdriveMeta& meta,
                           ObOlapdriveSchema& schema, ObOlapdriveParam& param,
                           ObOlapdriveStat& stat);
      virtual ~ObCampaignImpression();

    public:
      virtual int check(const common::ObScanner &scanner, const int64_t time);
      virtual int prepare(common::ObScanParam &scan_param);
      virtual const char* get_test_name();

    private:
      DISALLOW_COPY_AND_ASSIGN(ObCampaignImpression);
    };
  } // end namespace olapdrive
} // end namespace oceanbase

#endif //OCEANBASE_OLAPDRIVE_CAMPAIGN_IMPRESSION_H
