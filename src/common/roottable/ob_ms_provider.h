/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ms_provider.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_MS_PROVIDER_H
#define _OB_MS_PROVIDER_H 1
#include "common/ob_define.h"
#include <stdint.h>
namespace oceanbase
{
  namespace common
  {
    class ObServer;
    class ObScanParam;
    class ObScanner;
    /**
     * interface to provide mergeserver list for the scan operation
     * @see ObScanHelperImpl
     */
    class ObMsProvider
    {
      public:
        ObMsProvider(){}
        virtual ~ObMsProvider(){}
        /**
         * find a suitable ms for scan operation
         *
         * @param scan_param [in] scan param to scan
         * @param retry_num [in] for retry_num'th retry of the scan
         * @param ms [in/out] as input it is the ms failed to scan
         *
         * @return OB_SUCCESS or
         *  - OB_ITER_END when there is no more mergeserver
         */
        virtual int get_ms(const ObScanParam &scan_param, const int64_t retry_num, ObServer &ms) = 0;

        /**
         * find a suitable ms for any operation
         *
         * @param retry_num [in] for retry_num'th retry of the operation
         * @param ms [in/out] as input it is the ms failed to do the operation
         *
         * @return OB_SUCCESS or
         *  - OB_ITER_END when there is no more mergeserver
         */
        virtual int get_ms(ObServer & ms) = 0;
      private:
        // disallow copy
        ObMsProvider(const ObMsProvider &other);
        ObMsProvider& operator=(const ObMsProvider &other);
    };
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_MS_PROVIDER_H */

