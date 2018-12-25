/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_blacklist.h,v 0.1 2012/07/14 10:10:20 zhidong Exp $
 *
 * Authors:
 *   chuanhui <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OB_OCEAMBASE_BLACKLIST_H_
#define OB_OCEANBASE_BLACKLIST_H_

#include <stdint.h>
#include "tbsys.h"

namespace oceanbase
{
  namespace common
  {
    // thread safe blacklist for access selector
    class ObBlackList
    {
    public:
      ObBlackList();
      virtual ~ObBlackList();
    public:
      // init the algorithm for
      int init(const int32_t count, const int64_t alpha, const int64_t alpha_deno,
          const int64_t threshold, const int64_t threshold_deno);
      /// update the scores of the index pos
      int update(const bool succ, const int32_t index);
      /// if not in blacklist return true, otherwise return false
      bool check(const int32_t index) const;
      /// reset for washout the list
      void reset(void);
    public:
      static const int32_t MAX_LIST_COUNT = 32;
      static const double DEFAULT_DETA = 0.9f;
      static const double DEFAULT_THRESHOLD = 10.0f;
    private:
      mutable tbsys::CRWLock lock_;
      double alpha_;
      double threshold_;
      int32_t count_;
      double scores_[MAX_LIST_COUNT];
    };
  }
}

#endif //OB_OCEANBASE_BLACKLIST_H_
