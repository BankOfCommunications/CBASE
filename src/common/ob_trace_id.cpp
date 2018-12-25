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

#include "ob_trace_id.h"

using namespace oceanbase;
using namespace oceanbase::common;

TraceId::TraceId()
{
  uval_ = 0;
}
bool TraceId::is_invalid()
{
  return uval_ == 0 ? true : false;
}
IpPort::IpPort()
{
  ip_ = 0;
  port_ = 0;
}
uint32_t SeqGenerator::seq_generator_ = 0;
