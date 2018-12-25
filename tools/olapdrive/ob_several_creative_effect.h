/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_several_creative_effect.h for define several creative 
 * effect query classs. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_OLAPDRIVE_SEVERAL_CREATIVE_EFFECT_H
#define OCEANBASE_OLAPDRIVE_SEVERAL_CREATIVE_EFFECT_H

#include "ob_lz_query.h"

namespace oceanbase 
{ 
  namespace olapdrive
  {
    class ObSeveralCreativeEffect : public ObLzQuery
    {
    public:
      ObSeveralCreativeEffect(
        client::ObClient& client, ObOlapdriveMeta& meta,
        ObOlapdriveSchema& schema, ObOlapdriveParam& param,
        ObOlapdriveStat& stat);
      virtual ~ObSeveralCreativeEffect();

    public:
      virtual int check(const common::ObScanner &scanner, const int64_t time);
      virtual int add_special_column(common::ObScanParam& scan_param);
      virtual const char* get_test_name();

    private:
      DISALLOW_COPY_AND_ASSIGN(ObSeveralCreativeEffect);
    };
  } // end namespace olapdrive
} // end namespace oceanbase

#endif //OCEANBASE_OLAPDRIVE_SEVERAL_CREATIVE_EFFECT_H
