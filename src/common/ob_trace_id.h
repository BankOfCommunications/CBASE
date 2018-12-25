/* (C) 2010-2012 Alibaba Group Holding Limited. 
 * 
 * This program is free software: you can redistribute it and/or 
 *  modify it under the terms of the GNU General Public License 
 *  version 2 as published by the Free Software Foundation.
 * 
 * Version: 0.1 
 * 
 * Authors: 
 *    Wu Di <lide.wd@taobao.com>
 */

#ifndef OB_TRACE_ID_H_
#define OB_TRACE_ID_H_

#include <stdint.h>
namespace oceanbase
{
  namespace common
  {
    class SeqGenerator
    {
      public:
        static uint32_t seq_generator_;
    };
    struct TraceId
    {
      TraceId();
      bool is_invalid();
      union
      {
        struct
        {
          uint16_t ip_;// ip后两个点分十进制数
          uint16_t port_;
          uint32_t seq_;
        } id;
        uint64_t uval_;
      };
    };
    struct IpPort
    {
      IpPort();
      uint16_t ip_;
      uint16_t port_;
    };
  }// namespace common
}// namespace oceanbase


#endif
