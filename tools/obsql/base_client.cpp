/**
 * (C) 2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * base_client.cpp for define base oceanbase client. 
 *
 * Authors:
 *   tianguan <tianguan.dgb@taobao.com>
 *
 */
#include "base_client.h"

namespace oceanbase 
{ 
  namespace obsql 
  {
    using namespace common;

    int BaseClient::init()
    {
      int ret = OB_SUCCESS;

      streamer_.setPacketFactory(&factory_);
      ret = client_.initialize(&transport_, &streamer_);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "initialize client manager failed.");
      }

      return ret;
    }
    
    int BaseClient::start()
    {
      return (transport_.start() ? OB_SUCCESS : OB_ERROR);
    }
    
    int BaseClient::stop()
    {
      return (transport_.stop() ? OB_SUCCESS : OB_ERROR);
    }
    
    int BaseClient::wait()
    {
      return (transport_.wait() ? OB_SUCCESS : OB_ERROR);
    }
  } // end namespace client
} // end namespace oceanbase
