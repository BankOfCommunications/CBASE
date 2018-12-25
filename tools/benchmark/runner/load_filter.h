/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: load_filter.h,v 0.1 2012/03/15 14:00:00 zhidong Exp $
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef LOAD_FILTER_H_
#define LOAD_FILTER_H_

#include <set>
#include "load_define.h"
#include "load_parser.h"

namespace oceanbase
{
  namespace tools
  {
    class LoadFilter
    {
    public:
      LoadFilter();
      virtual ~LoadFilter();
    public:
      // set enable read master
      void read_master(const bool enable);
      // add this type to the white list
      void allow(const int32_t ptype);
      // remove this type from the white list
      void disallow(const int32_t ptype);
      // clear the white list to null
      void reset();
    public:
      // check packet need filtered; if return true pass, if false failed pass 
      virtual bool check(const int32_t ptype, const char packet[], const int64_t len) const;
    private:
      bool enable_read_master_;
      LoadParser packet_parser_;
      std::set<int32_t> white_list_;
    };
  }
}
#endif //LOAD_FILTER_H_
