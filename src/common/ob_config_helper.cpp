/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-02-20 17:20:32 fufeng.syd>
 * Version: $Id$
 * Filename: ob_config_helper.cpp
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#include "common/ob_config.h"
#include "ob_config_helper.h"

namespace oceanbase
{
  namespace common
  {
    bool ObConfigIpChecker::check(const ObConfigItem &t) const
    {
      struct sockaddr_in sa;
      int result = inet_pton(AF_INET, t.str(), &(sa.sin_addr));
      return result != 0;
    }
    bool ObConfigConsChecker::check(const ObConfigItem &t) const
    {
      return (NULL == left_ ? true : left_->check(t))
        && (NULL == right_ ? true : right_->check(t));
    }
    bool ObConfigGreaterThan::check(const ObConfigItem &t) const
    {
      return reinterpret_cast<const ObConfigIntegralItem&>(t) > val_;
    }
    bool ObConfigGreaterEqual::check(const ObConfigItem &t) const
    {
      return reinterpret_cast<const ObConfigIntegralItem&>(t) >= val_;
    }
    bool ObConfigLessThan::check(const ObConfigItem &t) const
    {
      return reinterpret_cast<const ObConfigIntegralItem&>(t) < val_;
    }
    bool ObConfigLessEqual::check(const ObConfigItem &t) const
    {
      return reinterpret_cast<const ObConfigIntegralItem&>(t) <= val_;
    }
  }
}
