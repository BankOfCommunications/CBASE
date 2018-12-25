////===================================================================
 //
 // ob_thread_single_instance.cpp common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2011-01-14 by Yubai (yubai.lk@taobao.com) 
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
#include "ob_tsi_factory.h"

namespace oceanbase
{
  namespace common
  {
    TSIFactory &get_tsi_fatcory()
    {
      static TSIFactory instance;
      return instance;
    }

    void tsi_factory_init()
    {
      get_tsi_fatcory().init();
    }

    void tsi_factory_destroy()
    {
      get_tsi_fatcory().destroy();
    }

    //void  __attribute__((constructor)) init_global_tsi_factory()
    //{
    //  get_tsi_fatcory().init();
    //}

    //void  __attribute__((destructor)) destroy_global_tsi_factory()
    //{
    //  get_tsi_fatcory().destroy();
    //}
  }
}

