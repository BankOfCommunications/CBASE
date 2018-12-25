/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 *         bloom_filter.cpp is for what ...
 *
 *  Version: $Id: bloom_filter.cpp 05/07/2013 10:44:18 AM qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#include "bloom_filter.h"

namespace oceanbase
{ 
  namespace common
  { 
    int destroy_bloom_filter(BloomFilter* bf)
    {
      if (NULL != bf)
      {
        bf->~BloomFilter();
        ob_tc_free(bf, ObModIds::OB_BLOOM_FILTER);
      }
      return OB_SUCCESS;
    }

  }
}


