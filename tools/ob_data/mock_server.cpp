
#include "mock_server.h"

namespace oceanbase
{
  namespace mergeserver
  {
    int MockServer::set_self(const char* dev_name, const int32_t port)
    {   
      int ret = OB_SUCCESS;
      int32_t ip = tbsys::CNetUtil::getLocalAddr(dev_name);
      if (0 == ip) 
      {   
        TBSYS_LOG(ERROR, "cannot get valid local addr on dev:%s.", dev_name);
        ret = OB_ERROR;
      }   
      if (OB_SUCCESS == ret)
      {   
        bool res = self_.set_ipv4_addr(ip, port);
        if (!res)
        {   
          TBSYS_LOG(ERROR, "server dev:%s, port:%d is invalid.", 
              dev_name, port);
          ret = OB_ERROR;
        }   
      }   
      return ret;
    }  

  }
}
