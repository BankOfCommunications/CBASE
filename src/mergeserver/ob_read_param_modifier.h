/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_read_param_modifier.h is for what ...
 *
 * Version: $id: ob_read_param_modifier.h,v 0.1 10/25/2010 6:13p wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef OCEANBASE_MCERGESERVER_OB_READ_PARAM_MODIFIER_H_
#define OCEANBASE_MCERGESERVER_OB_READ_PARAM_MODIFIER_H_
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/ob_new_scanner.h"
#include "common/ob_string.h"
#include "common/ob_rowkey.h"
#include "common/ob_malloc.h"
#include "common/ob_range.h"
#include "common/ob_scan_param.h"
#include "sql/ob_sql_scan_param.h"
namespace oceanbase
{
  namespace mergeserver
  {
    // check finish of all scan
    bool is_finish_scan(const oceanbase::common::ScanFlag::Direction  scan_direction,
      const oceanbase::common::ObNewRange & org_range,
      const oceanbase::common::ObNewRange & result_range);
        // check finish
    bool is_finish_scan(const oceanbase::common::ObScanParam & param,
      const oceanbase::common::ObNewRange & result_range);

    /// @fn get next pram for next get
    /// @return if all has gotten, return OB_ITER_END
    int get_next_param(const oceanbase::common::ObGetParam & org_param,
      const oceanbase::common::ObScanner & result,
      int64_t & got_cell_num, bool & finish,
      oceanbase::common::ObGetParam *get_param);

    /// @fn get next scan param
    /// @return if all cell has gotten, return OB_ITER_END
    int get_next_param(const oceanbase::common::ObScanParam &org_scan_param,
      const oceanbase::common::ObScanner  &prev_scan_result,
      oceanbase::common::ObScanParam *scan_param,
      oceanbase::common::ObMemBuffer &range_buffer);

    int get_next_range(const oceanbase::sql::ObSqlScanParam &org_scan_param,
      const oceanbase::common::ObNewScanner &prev_scan_result,
      const int64_t prev_limit_offset,
      oceanbase::common::ObNewRange &cur_range,
      int64_t & cur_limit_offset, oceanbase::common::ObStringBuf &buf);

    int get_next_range(const oceanbase::common::ObScanParam &org_scan_param,
      const oceanbase::common::ObScanner &prev_scan_result,
      const int64_t prev_limit_offset,
      oceanbase::common::ObNewRange &cur_range,
      int64_t & cur_limit_offset, oceanbase::common::ObStringBuf &buf);


  }
}
#endif /* OCENBASE_MERGESERVER_OB_READ_PARAM_MODIFIER_H_ */
