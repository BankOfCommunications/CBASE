#include "ob_lsync_callback.h"
#include "common/ob_packet.h"
#include "ob_lsync_server.h"
#include "tblog.h"
namespace oceanbase
{
  namespace lsync
  {
    int ObLsyncCallback::process(easy_request_t* r)
    {
      int ret = EASY_OK;

      if (NULL == r || NULL == r->ipacket)
      {
        TBSYS_LOG(WARN, "request is NULL, r = %p, r->ipacket = %p", r, r->ipacket);
        ret = EASY_BREAK;
      }
      else
      {
        ObLsyncServer* server = (ObLsyncServer*)r->ms->c->handler->user_data;
        ObPacket* packet = (ObPacket*)r->ipacket;
        packet->set_request(r);
        r->ms->c->pool->ref++;
        easy_atomic_inc(&r->ms->pool->ref);
        easy_pool_set_lock(r->ms->pool);
        server->handlePacket(packet);
        ret = EASY_AGAIN;
      }
      return ret;
    }
  }
}
