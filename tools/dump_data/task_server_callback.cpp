#include "task_server_callback.h"
#include "common/ob_packet.h"
#include "task_server.h"
using namespace oceanbase::tools;

int TaskServerCallback::process(easy_request_t* r)
{
  int ret = EASY_OK;

  if (NULL == r)
  {
    TBSYS_LOG(WARN, "request is NULL, r = %p", r);
    ret = EASY_BREAK;
  }
  else if (NULL == r->ipacket)
  {
    TBSYS_LOG(WARN, "request is NULL, r->ipacket = %p", r->ipacket);
    ret = EASY_BREAK;
  }
  else
  {
    TaskServer* server = (TaskServer*)r->ms->c->handler->user_data;
    ObPacket* packet = (ObPacket*)r->ipacket;
    packet->set_request(r);
    ret = server->handlePacket(packet);
    if (OB_SUCCESS == ret)
    {
      r->ms->c->pool->ref++;
      easy_atomic_inc(&r->ms->pool->ref);
      easy_pool_set_lock(r->ms->pool);
      ret = EASY_AGAIN;
    }
    else
    {
      ret = EASY_OK;
      TBSYS_LOG(WARN, "can not push packet(src is %s, pcode is %u) to packet queue",
          inet_ntoa_r(r->ms->c->addr), packet->get_packet_code());
    }
  }
  return ret;
}
