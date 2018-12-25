////===================================================================
 //
 // ob_util_interface.h / hash / common / Oceanbase
 //
 // Copyright (C) 2011, 2012 Taobao.com, Inc.
 //
 // Created on 2012-01-25 by Yubai (yubai.lk@taobao.com) 
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

#ifndef  OCEANBASE_UPDATESERVER_UTIL_INTERFACE_H_
#define  OCEANBASE_UPDATESERVER_UTIL_INTERFACE_H_
#include "common/ob_define.h"

#define DEFINE_UTIL_INTERFACE_0(Function) \
  virtual int ui_#Function() \
  { \
    return common::OB_NOT_IMPLEMENT; \
  }

#define DEFINE_UTIL_INTERFACE_1(Function, ParamT1) \
  virtual int ui_##Function(ParamT1 param1) \
  { \
    UNUSED(param1); \
    return common::OB_NOT_IMPLEMENT; \
  }

#define DEFINE_UTIL_INTERFACE_2(Function, ParamT1, ParamT2) \
  virtual int ui_##Function(ParamT1 param1, ParamT2 param2) \
  { \
    UNUSED(param1); \
    UNUSED(param2); \
    return common::OB_NOT_IMPLEMENT; \
  }

#define DEFINE_UTIL_INTERFACE_3(Function, ParamT1, ParamT2, ParamT3) \
  virtual int ui_##Function(ParamT1 param1, ParamT2 param2, ParamT3 param3) \
  { \
    UNUSED(param1); \
    UNUSED(param2); \
    UNUSED(param3); \
    return common::OB_NOT_IMPLEMENT; \
  }

namespace oceanbase
{
  namespace updateserver
  {
    class ObUtilInterface
    {
      public:
        virtual ~ObUtilInterface() {};
      public:
        DEFINE_UTIL_INTERFACE_2(deserialize_mutator, common::ObDataBuffer&, common::ObMutator&);
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_UTIL_INTERFACE_H_

