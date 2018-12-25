#include "load_server.h"

using namespace oceanbase::common;
using namespace oceanbase::tools;

int LoadServer::initialize()
{
  int ret = set_packet_factory(&factory_);
  if (OB_SUCCESS == ret)
  {
    ret = client_manager_.initialize(get_transport(), get_packet_streamer());
    if (OB_SUCCESS == ret)
    {
      ret = ObSingleServer::initialize();
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "check single server init failed:ret[%d]", ret);
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "check client manager init failed:ret[%d]", ret);
    }
  }
  return ret;
}

int LoadServer::init(const int64_t thread, const int64_t queue, RpcStub * rpc,
  const char * dev, const int32_t port)
{
  int ret = OB_SUCCESS;
  if ((INVALID_STAT != status_) || (NULL == rpc) || (NULL == dev) || (port <= 1024))
  {
    TBSYS_LOG(ERROR, "check status or rpc or dev or port failed:"
        "status[%d], rpc[%p], dev[%s], port[%d]", status_, rpc, dev, port);
    ret = OB_ERROR;
  }
  else
  {
    rpc_ = rpc;
    set_dev_name(dev);
    set_listen_port(port);
    set_batch_process(false);
    ret = set_default_queue_size(queue);
    if (OB_SUCCESS == ret)
    {
      ret = set_thread_count(thread);
      if (OB_SUCCESS == ret)
      {
        status_ = PREPARE_STAT;
      }
    }
  }
  return ret;
}
