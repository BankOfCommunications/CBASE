#ifndef __OB_CLUSTER_SERVER_H_
#define __OB_CLUSTER_SERVER_H_

#include "utility.h"
#include "ob_server.h"
#include "ob_define.h"
#include "ob_vector.h"

namespace oceanbase
{
  namespace common
  {
    struct ObClusterServer
    {
      ObRole role;
      ObServer addr;
      ObClusterServer()
      {
        role = OB_INVALID;
      }
      bool operator < (const ObClusterServer & other) const
      {
        bool bret = false;
        int ret = strcmp(print_role(role), print_role(other.role));
        if (0 == ret)
        {
          char ip1[OB_IP_STR_BUFF] = "";
          char ip2[OB_IP_STR_BUFF] = "";
          addr.ip_to_string(ip1, sizeof (ip1));
          other.addr.ip_to_string(ip2, sizeof (ip2));
          ret = strcmp(ip1, ip2);
          if (0 == ret)
          {
            ret = addr.get_port() - other.addr.get_port();
          }
        }
        bret = (ret < 0);
        return bret;
      }
    };

    template <>
    struct ob_vector_traits<ObClusterServer>
    {
      typedef ObClusterServer * pointee_type;
      typedef ObClusterServer value_type;
      typedef const ObClusterServer const_value_type;
      typedef value_type* iterator;
      typedef const value_type* const_iterator;
      typedef int32_t difference_type;
    };
  }
}

#endif //__OB_CLUSTER_SERVER_H_
