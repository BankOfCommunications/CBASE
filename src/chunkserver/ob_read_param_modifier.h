/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_read_param_modifier.h for modify scan param or get param. 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_READ_PARAM_MODIFIER_H_ 
#define OCEANBASE_CHUNKSERVER_READ_PARAM_MODIFIER_H_

#include "ob_cell_stream.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/ob_range2.h"
#include "common/ob_malloc.h"

namespace oceanbase
{
  namespace chunkserver
  {
    // check finish
    bool is_finish_scan(const common::ObScanParam & param, 
                        const common::ObNewRange & result_range);

    // get next get param
    // if all cell has gotten, return OB_ITER_END
    int get_next_param(const common::ObReadParam & org_read_param, 
                       const ObCellArrayHelper &org_get_cells, 
                       const int64_t got_cell_num, 
                       common::ObGetParam *get_param);

    // get next scan param
    // if all cell has gotten, return OB_ITER_END
    int get_next_param(const common::ObScanParam &org_scan_param, 
                       const common::ObScanner  &prev_scan_result,
                       common::ObScanParam *scan_param,
                       common::ObMemBuf &range_buffer);

    // adjust table version according to result from chunkserver
    int get_ups_param(common::ObScanParam & param, 
                      const common::ObScanner & cs_result,
                      common::ObMemBuf &range_buffer);

    // adjust table version according to result from chunkserver
    int get_ups_param(common::ObGetParam & param, 
                      const common::ObScanner & cs_result);

    // allocate range buffer
    int get_ups_param(common::ObScanParam & param,
                      common::ObMemBuf &range_buffer); /*add zhaoqiong [Truncate Table]:20160318*/
  }
}   

#endif //OCEANBASE_CHUNKSERVER_READ_PARAM_MODIFIER_H_
