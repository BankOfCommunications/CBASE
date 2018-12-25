/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_ms_scan_param.h for define oceanbase mergeserver scan param operations
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef MERGESERVER_OB_MS_SCAN_PARAM_H_ 
#define MERGESERVER_OB_MS_SCAN_PARAM_H_
#include "common/ob_scan_param.h"
#include "common/ob_define.h"
namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerScanParam
    {
    public:
      ObMergerScanParam()
      {
        decoded_org_param_ = NULL;
      }
      ~ObMergerScanParam()
      {
        reset();
      }
      inline oceanbase::common::ObScanParam * get_cs_param(void)
      {
        return decoded_org_param_;
      }
      const oceanbase::common::ObScanParam * get_ms_param(void)const
      {
        return &ms_scan_param_;
      }
      inline void reset(void)
      {
        decoded_org_param_ = NULL;
        ms_scan_param_.reset();
      }

      int set_param(oceanbase::common::ObScanParam & param);
    private:
      oceanbase::common::ObScanParam  *decoded_org_param_;
      oceanbase::common::ObScanParam  ms_scan_param_;

    };
  }
}  
#endif /* MERGESERVER_OB_MS_SCAN_PARAM_H_ */
