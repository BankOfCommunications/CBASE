/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_root_ups_provider.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ROOT_UPS_PROVIDER_H
#define _OB_ROOT_UPS_PROVIDER_H 1

#include "common/roottable/ob_ups_provider.h"
#include "common/ob_server.h"
namespace oceanbase
{
  namespace rootserver
  {
    class ObRootUpsProvider: public common::ObUpsProvider
    {
      public:
        ObRootUpsProvider(const common::ObServer &ups);
        virtual ~ObRootUpsProvider();
        virtual int get_ups(common::ObServer &ups);
      private:
        // disallow copy
        ObRootUpsProvider(const ObRootUpsProvider &other);
        ObRootUpsProvider& operator=(const ObRootUpsProvider &other);
      private:
        // data members
        common::ObServer ups_;
    };
  } // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_ROOT_UPS_PROVIDER_H */

