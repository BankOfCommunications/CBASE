/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_ups_provider.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_UPS_PROVIDER_H
#define _OB_UPS_PROVIDER_H 1

namespace oceanbase
{
  namespace common
  {
    class ObServer;
    
    /**
     * interface to provider updateserver for mutation
     * @see ObScanHelperImpl
     */
    class ObUpsProvider
    {
      public:
        ObUpsProvider(){}
        virtual ~ObUpsProvider(){}

        virtual int get_ups(ObServer &ups) = 0;
      private:
        // disallow copy
        ObUpsProvider(const ObUpsProvider &other);
        ObUpsProvider& operator=(const ObUpsProvider &other);
    };
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_UPS_PROVIDER_H */

