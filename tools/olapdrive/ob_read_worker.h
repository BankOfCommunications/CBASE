/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_read_worker.h for define read worker thread. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_OLAPDRIVE_READ_WORKER_H
#define OCEANBASE_OLAPDRIVE_READ_WORKER_H

#include <tbsys.h>
#include "client/ob_client.h"
#include "ob_olapdrive_param.h"
#include "ob_olapdrive_meta.h"
#include "ob_olapdrive_stat.h"
#include "ob_olapdrive_schema.h"
#include "ob_lz_common.h"
#include "ob_lz_query.h"
#include "ob_campaign_effect.h"
#include "ob_campaign_impression.h"
#include "ob_platform_pvtype_effect.h"
#include "ob_platform_pvtype_effect_daily.h"
#include "ob_adgroup_effect_top10.h"
#include "ob_adgroup_impression_count.h"
#include "ob_adgroup_effect_daily.h"
#include "ob_adgroup_bidword_effect_orderby.h"
#include "ob_bidword_effect_top10.h"
#include "ob_bidword_effect_daily.h"
#include "ob_several_adgroup_effect.h"
#include "ob_several_bidword_effect.h"
#include "ob_several_creative_effect.h"

namespace oceanbase 
{ 
  namespace olapdrive
  {
    class ObReadWorker : public tbsys::CDefaultRunnable
    {
    public:
      ObReadWorker(client::ObClient& client, ObOlapdriveMeta& meta,
                   ObOlapdriveSchema& schema, ObOlapdriveParam& param,
                   ObOlapdriveStat& stat);
      virtual ~ObReadWorker();

    public:
      virtual void run(tbsys::CThread* thread, void* arg);

    private:
      int init();
       
    private:
      DISALLOW_COPY_AND_ASSIGN(ObReadWorker);

      ObCampaignEffect camp_eff_;
      ObCampaignImpression camp_impr_;
      ObPlatformPvtypeEffect plat_pv_eff_;
      ObPlatformPvtypeEffectDaily plat_pv_daily_eff_;
      ObAdgroupEffectTop10 adgroup_eff_top_;
      ObAdgroupImpressionCount adgroup_impr_cnt_;
      ObAdgroupEffectDaily adgroup_eff_daily_;
      ObAdgroupBidwordEffectOrderby adgroup_bid_eff_ord_;
      ObBidwordEffectTop10 bid_eff_top_;
      ObBidwordEffectDaily bid_eff_daily_;
      ObSeveralAdgroupEffect sev_adgroup_eff_;
      ObSeveralBidwordEffect sev_bid_eff_;
      ObSeveralCreativeEffect sev_crt_eff_;

      ObLzQuery* lz_query_[13];
      int64_t lz_query_cnt_;
    };
  } // end namespace olapdrive
} // end namespace oceanbase

#endif //OCEANBASE_OLAPDRIVE_READ_WORKER_H
