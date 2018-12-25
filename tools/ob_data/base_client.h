#ifndef _BASE_CLIENT_H_
#define _BASE_CLIENT_H_

#include "tbnet.h"
#include "common/ob_define.h"
#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "common/ob_packet_factory.h"

namespace oceanbase
{
  namespace tools
  {
    class BaseClient
    {
    public:
      BaseClient()
      {
        int ret = init();
        if (ret != common::OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "client init failed:ret[%d]", ret);
        }
      }

      virtual ~BaseClient()
      {
        destroy();
      }

      common::ObClientManager * get_client(void)
      {
        return &client_;
      }

      common::ThreadSpecificBuffer * get_buffer(void)
      {
        return &rpc_buffer_;
      }
    protected:
      virtual int init();

    private:
      virtual int destroy();

    private:
      tbnet::DefaultPacketStreamer streamer_;
      tbnet::Transport transport_;
      common::ObPacketFactory factory_;

    protected:
      common::ThreadSpecificBuffer rpc_buffer_;
      common::ObClientManager client_;
    };
  }
}

#endif //_BASE_CLIENT_H_

