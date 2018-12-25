/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_read_ups_banlance.cpp for read load balance stratege 
 * utility class. 
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "time.h"
#include "common/ob_ups_info.h"
#include "ob_read_ups_balance.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;

    ObReadUpsBalance::ObReadUpsBalance()
    {
    }
    
    ObReadUpsBalance::~ObReadUpsBalance()
    {
    }
    
    int32_t ObReadUpsBalance::select_server(const ObUpsList & list, const ObServerType type)
    {
      int32_t ret = -1;
      int32_t sum_percent = list.get_sum_percentage(type);

      if (sum_percent > 0)
      {
        // no need using thread safe random
        int32_t cur_percent = 0;
        int32_t total_percent = 0;
        int32_t random_percent = static_cast<int32_t>(random() % sum_percent);
        for (int32_t i = 0; i < list.ups_count_; ++i)
        {
          cur_percent = list.ups_array_[i].get_read_percentage(type);
          total_percent += cur_percent;
          if ((random_percent < total_percent) && (cur_percent > 0))
          {
            ret = i;
            break;
          }
        }
      }

      return ret;
    }
  } // end namespace chunkserver
} // end namespace oceanbase
