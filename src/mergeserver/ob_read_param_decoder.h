/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_scan_param_decoder.h is for what ...
 *
 * Version: $id: ob_scan_param_decoder.h,v 0.1 3/29/2011 10:36a wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef MERGESERVER_OB_SCAN_PARAM_DECODER_H_ 
#define MERGESERVER_OB_SCAN_PARAM_DECODER_H_
#include "common/ob_scan_param.h"
#include "common/ob_get_param.h"
#include "common/ob_schema.h"
#include "common/ob_result.h"
namespace oceanbase
{
  namespace mergeserver
  {
    /// change all names (column name, table name) into ids or idx 
    int ob_decode_scan_param(oceanbase::common::ObScanParam & org_param, 
      const oceanbase::common::ObSchemaManagerV2 & schema_mgr,
      oceanbase::common::ObScanParam &decoded_param,
      common::ObResultCode *rc = NULL);
    int ob_decode_get_param(const oceanbase::common::ObGetParam & org_param, 
      const oceanbase::common::ObSchemaManagerV2 & schema_mgr, 
      oceanbase::common::ObGetParam &decoded_param,
      oceanbase::common::ObGetParam &org_param_with_name,
      common::ObResultCode *rc = NULL);
  }
}

#endif /* MERGESERVER_OB_SCAN_PARAM_DECODER_H_ */
