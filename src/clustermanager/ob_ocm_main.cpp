/*
 * Copyright (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   Zhifeng YANG <zhifeng.yang@aliyun-inc.com>
 *     - some work details here
 */
/** 
 * @file ob_ocm_main.cpp
 * @author Zhifeng YANG <zhifeng.yang@aliyun-inc.com>
 * @date 2011-07-06
 * @brief 
 */

#include "common/base_main.h"
#include "common/ob_define.h"
#include "ob_ocm_server.h"
#include "ob_ocm_main.h"
#include <cstdio>
#include <malloc.h>
using namespace oceanbase::common;

namespace oceanbase
{
namespace clustermanager
{

OcmMain::OcmMain()
  :ocm_server_(NULL)
{
}

OcmMain::~OcmMain()
{
  if (NULL != ocm_server_)
  {
    delete ocm_server_;
    ocm_server_ = NULL;
  }
}

OcmMain* OcmMain::get_instance()
{
  if (NULL == instance_)
  {
    instance_ = new (std::nothrow) OcmMain();
  }
  return dynamic_cast<OcmMain*>(instance_);
}

int OcmMain::do_work()
{
  int ret = OB_SUCCESS;
  mallopt(M_MMAP_MAX, DEFAULT_MMAP_MAX_VAL);
  ob_init_memory_pool();
  ocm_server_ = new(std::nothrow) OcmServer();
  if (NULL == ocm_server_)
  {
    TBSYS_LOG(ERROR, "no memory");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = ocm_server_->start()))
  {
    TBSYS_LOG(ERROR, "ocm server start error, err=%d", ret);
  }
  return ret;
}

void OcmMain::do_signal(const int sig)
{
  TBSYS_LOG(INFO, "receive signal, sig=%d", sig);
  switch(sig)
  {
  case SIGTERM:
  case SIGINT:
    TBSYS_LOG(INFO, "start to stop the ocm server");
    if (NULL != ocm_server_)
    {
      ocm_server_->stop();
    }
    TBSYS_LOG(INFO, "the ocm server has stopped");
    break;
  default:
    break;
  }
}

} // end namespace clustermanager
} // end namespace oceanbase

using oceanbase::clustermanager::OcmMain;
int main(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  OcmMain *ocm = OcmMain::get_instance();
  if (NULL == ocm)
  {
    fprintf(stderr, "no memory\n");
  }
  else if (OB_SUCCESS != (ret = ocm->start(argc, argv, "cluster_manager")))
  {
    fprintf(stderr, "ocm start error, err=%d\n", ret);
  }
  else
  {   
    ocm->destroy();
  }
  return ret;
}

