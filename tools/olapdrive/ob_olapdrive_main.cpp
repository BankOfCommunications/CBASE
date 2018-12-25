/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_olapdrive_main.cpp for define olapdrive main scheduler. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <new>
#include "ob_olapdrive_main.h"
#include "common/ob_malloc.h"

namespace oceanbase 
{ 
  namespace olapdrive 
  {
    using namespace common;

    ObOlapdriveMain::ObOlapdriveMain()
    {

    }

    ObOlapdriveMain* ObOlapdriveMain::get_instance()
    {
      if (NULL == instance_)
      {
        instance_ = new (std::nothrow) ObOlapdriveMain();
      }

      return dynamic_cast<ObOlapdriveMain*>(instance_);
    }

    int ObOlapdriveMain::do_work()
    {
      return drive_.start();
    }

    void ObOlapdriveMain::do_signal(const int sig)
    {
      switch (sig)
      {
      case SIGTERM:
      case SIGINT:
        drive_.stop();
        break;
      default:
        break;
      }
    }
  } // end namespace olapdrive
} // end namespace oceanbase
