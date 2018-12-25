#ifndef _BASE_CLIENT_H_
#define _BASE_CLIENT_H_

#include "common/ob_define.h"
#include "common/ob_base_client.h"
#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "common/ob_packet_factory.h"

namespace oceanbase
{
  namespace tools
  {
    class BaseClient:public common::ObBaseClient
    {
    public:
      int init(const common::ObServer & server);

      common::ObClientManager * get_client(void)
      {
        return &get_client_mgr();
      }

      common::ThreadSpecificBuffer * get_buffer(void)
      {
        return &rpc_buffer_;
      }

    protected:
      common::ObServer server_;
      common::ThreadSpecificBuffer rpc_buffer_;
    };
  }
}

#endif //_BASE_CLIENT_H_

