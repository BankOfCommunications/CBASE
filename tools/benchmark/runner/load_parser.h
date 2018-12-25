/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: load_parser.h,v 0.1 2012/03/15 14:00:00 zhidong Exp $
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef LOAD_PARSER_H_
#define LOAD_PARSER_H_

#include "tbsys.h"
#include "common/ob_get_param.h"
#include "common/ob_scan_param.h"
#include "common/ob_fifo_stream.h"

namespace oceanbase
{
  namespace tools
  {
    class Request;
    class LoadParser
    {
    public:
      int is_consistency_read(const common::Request * request, bool & read_master) const;
    private:
      mutable common::ObGetParam get_param_;
      mutable common::ObScanParam scan_param_;
    };
  }
}


#endif //LOAD_PARSER_H_
