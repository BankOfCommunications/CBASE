/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_read_worker.cpp for define read worker thread. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <math.h>
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "common/murmur_hash.h"
#include "common/ob_action_flag.h"
#include "common/ob_tsi_factory.h"
#include "ob_read_worker.h"

namespace oceanbase 
{ 
  namespace olapdrive 
  {
    using namespace tbsys;
    using namespace common;
    using namespace serialization;
    using namespace client;

    ObReadWorker::ObReadWorker(ObClient& client, ObOlapdriveMeta& meta,
                               ObOlapdriveSchema& schema, ObOlapdriveParam& param,
                               ObOlapdriveStat& stat)
    : camp_eff_(client, meta, schema, param, stat),
      camp_impr_(client, meta, schema, param, stat),
      plat_pv_eff_(client, meta, schema, param, stat),
      plat_pv_daily_eff_(client, meta, schema, param, stat),
      adgroup_eff_top_(client, meta, schema, param, stat),
      adgroup_impr_cnt_(client, meta, schema, param, stat),
      adgroup_eff_daily_(client, meta, schema, param, stat),
      adgroup_bid_eff_ord_(client, meta, schema, param, stat),
      bid_eff_top_(client, meta, schema, param, stat),
      bid_eff_daily_(client, meta, schema, param, stat),
      sev_adgroup_eff_(client, meta, schema, param, stat),
      sev_bid_eff_(client, meta, schema, param, stat),
      sev_crt_eff_(client, meta, schema, param, stat)
    {

    }

    ObReadWorker::~ObReadWorker()
    {

    }

    int ObReadWorker::init()
    {
      int ret = OB_SUCCESS;

      lz_query_[0] = &camp_eff_;
      lz_query_[1] = &camp_impr_;
      lz_query_[2] = &plat_pv_eff_;
      lz_query_[3] = &plat_pv_daily_eff_;
      lz_query_[4] = &adgroup_eff_top_;
      lz_query_[5] = &adgroup_impr_cnt_;
      lz_query_[6] = &adgroup_eff_daily_;
      lz_query_[7] = &adgroup_bid_eff_ord_;
      lz_query_[8] = &bid_eff_top_;
      lz_query_[9] = &bid_eff_daily_;
      lz_query_[10] = &sev_adgroup_eff_;
      lz_query_[11] = &sev_bid_eff_;
      lz_query_[12] = &sev_crt_eff_;
      lz_query_cnt_ = 13;

      return ret;
    }

    void ObReadWorker::run(CThread* thread, void* arg)
    {
      int err             = OB_SUCCESS;
      ObLzQuery* lz_query = NULL;
      ObScanParam scan_param;
      ObScanner scanner;
      static int64_t cnt  = 0;
      int64_t index = 0;
      UNUSED(thread);
      UNUSED(arg);

      if (OB_SUCCESS == init())
      {
        while (!_stop)
        {
          index = cnt++ % lz_query_cnt_;
          lz_query = lz_query_[random() % lz_query_cnt_];
          err = lz_query->scan(scan_param, scanner);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to run scan operation, err=%d", err);
            break;
          }
        }
      }
    }
  } // end namespace olapdrive
} // end namespace oceanbase
