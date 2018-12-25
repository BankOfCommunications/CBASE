////===================================================================
 //
 // liboblog.cpp liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-05-23 by Yubai (yubai.lk@alipay.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 // 
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#include "common/ob_malloc.h"
#include "liboblog.h"
#include "ob_log_spec.h"
#include "ob_log_entity.h"

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
    ObLogSpecFactory::ObLogSpecFactory()
    {
      //TODO register signal handler
      ob_init_memory_pool();
    }

    ObLogSpecFactory::~ObLogSpecFactory()
    {
    }

    IObLogSpec *ObLogSpecFactory::construct_log_spec()
    {
      return new(std::nothrow) ObLogSpec();
    }

    IObLog *ObLogSpecFactory::construct_log()
    {
      return new(std::nothrow) ObLogEntity();
    }

    void ObLogSpecFactory::deconstruct(IObLogSpec *log_spec)
    {
      if (NULL != log_spec)
      {
        TBSYS_LOG(INFO, "IObLogSpec %p deconstruct", log_spec);
        delete log_spec;
      }
    }

    void ObLogSpecFactory::deconstruct(IObLog *log)
    {
      if (NULL != log)
      {
        TBSYS_LOG(INFO, "IObLog %p deconstruct", log);
        delete log;
      }
    }
  }
}

