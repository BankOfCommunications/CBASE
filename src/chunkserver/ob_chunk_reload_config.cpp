/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-23 21:50:42 fufeng.syd>
 * Version: $Id$
 * Filename: ob_chunk_reload_config.cpp
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#include "ob_chunk_server.h"
#include "ob_chunk_reload_config.h"

using namespace oceanbase;
using namespace oceanbase::chunkserver;

ObChunkReloadConfig::ObChunkReloadConfig()
  : chunk_server_(NULL)
{

}

ObChunkReloadConfig::~ObChunkReloadConfig()
{

}

void ObChunkReloadConfig::set_chunk_server(ObChunkServer &chunk_server)
{
  chunk_server_ = &chunk_server;
}

int ObChunkReloadConfig::operator ()()
{
  int ret = OB_SUCCESS;
  if (NULL == chunk_server_)
  {
    TBSYS_LOG(WARN, "NULL chunk server.");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = chunk_server_->reload_config();
  }
  return ret;
}
