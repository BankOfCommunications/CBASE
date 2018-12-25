#include "ob_shadow_server.h"
#include "ob_tbnet_callback.h"

namespace oceanbase
{
  namespace common
  {
    ObShadowServer::ObShadowServer(ObBaseServer* master)
    {
      priority_ = NORMAL_PRI;
      if (master != NULL)
      {
        master_ = master;
      }
    }

    ObShadowServer::~ObShadowServer()
    {
      // does nothing
    }

    int ObShadowServer::initialize()
    {
      server_handler_.encode = ObTbnetCallback::encode;
      server_handler_.decode = ObTbnetCallback::decode;
      server_handler_.get_packet_id = ObTbnetCallback::get_packet_id;
      server_handler_.process = ObTbnetCallback::shadow_process;
      server_handler_.on_disconnect = ObTbnetCallback::on_disconnect;
      server_handler_.user_data = this;
      return OB_SUCCESS;
    }

    /*int ObShadowServer::start(bool need_wait)
    {
      int rc = OB_SUCCESS;
      int ret = EASY_OK;
      easy_listen_t *listen = NULL;
      
      rc = initialize();

      if (rc != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "initialize failed");
      }
      else
      {
        uint32_t local_ip = tbsys::CNetUtil::getLocalAddr(dev_name_);
        server_id_ = tbsys::CNetUtil::ipToAddr(local_ip, port_);
        if (OB_SUCCESS == rc)
        {
          if (NULL == (listen = easy_io_add_listen(NULL, port_, &server_handler_)))
          {
            TBSYS_LOG(ERROR, "easy_io_add_listen error, port: %d, %s", port_, strerror(errno));
            rc = OB_SERVER_LISTEN_ERROR;
          }
          else
          {
            TBSYS_LOG(INFO, "listen start, port = %d", port_);
          }
        }
      }
      UNUSED(need_wait);
      return ret;
      }*/
    
    int ObShadowServer::set_io_thread_count(int32_t io_thread_count)
    {
      int ret = OB_SUCCESS;
      if (io_thread_count < 1)
      {
        TBSYS_LOG(WARN, "invalid argument io thread count is %d", io_thread_count);
        ret = OB_ERROR;
      }
      else
      {
        io_thread_count_ = io_thread_count;
      }
      return ret;
    }

    void ObShadowServer::set_priority(const int32_t priority)
    {
      if (priority == NORMAL_PRI || priority == LOW_PRI)
      {
        priority_ = priority;
      }
    }

    int ObShadowServer::handlePacket(ObPacket *packet)
    {
      int rc = OB_SUCCESS;
      ObPacket* req = packet;
      req->set_packet_priority(priority_);
      if (master_ != NULL)
      {
        rc = master_->handlePacket(req);
      }
      else
      {
        TBSYS_LOG(ERROR, "shadow server's master is NULL");
        packet->free();
        rc = OB_ERROR;
      }
      
      return rc;
    }

    int ObShadowServer::handleBatchPacket(ObPacketQueue &packetQueue)
    {
      int ret = OB_SUCCESS;
      ObPacketQueue temp_queue;
      ObPacket *packet= packetQueue.get_packet_list();
      while (packet != NULL)
      {
        ObPacket* req = (ObPacket*) packet;
        req->set_packet_priority(priority_);
        if (master_ == NULL)
        {
          TBSYS_LOG(ERROR, "shadow server's master is NULL");
          packet = packet->get_next();
          req->free();
        }
        else
        {
          temp_queue.push(packet);
          packet = packet->get_next();
        }
      }

      if (temp_queue.size() > 0)
      {
        ret = master_->handleBatchPacket(temp_queue);
      }

      return ret;
    }

  } /* common */
} /* oceanbase */
