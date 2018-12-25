#include "common/ob_define.h"
#include "mock_node_server.h"
#include "mock_monitor_server.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace mms
  {
    namespace tests
    {
      MockMonitorServer::MockMonitorServer(int32_t port, int64_t retry_time, int64_t timeout, int64_t lease_interval, int64_t renew_interval)
      {
        port_ = port;
        retry_times_ = retry_time;
        timeout_ = timeout;
        lease_interval_ = lease_interval;
        renew_interval_ = renew_interval;
      }
      int MockMonitorServer::initialize()
      {
        int err = OB_SUCCESS;
        err = MockServer::initialize();
        if(err != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "fail to call MockServer initialize");
        }

        if(err == OB_SUCCESS)
        {
          set_listen_port(port_);
          self_.set_ipv4_addr(MockServer::ip_addr_, port_);
          err = monitor_.init(this, &(MockServer::client_manager_), lease_interval_, renew_interval_, timeout_, retry_times_);
          if(err != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "fail to init monitor");
          }
        }

        if(err == OB_SUCCESS)
        {
          handler_ = new(std::nothrow)MonitorHandler(this);
          if(handler_ == NULL)
          {
            err = OB_ALLOCATE_MEMORY_FAILED;
            TBSYS_LOG(WARN, "fail to allocate handler. err=%d", err);
          }
        }
        return err;
      }
      
      void MockMonitorServer::stop()
      {
        monitor_.destroy();
        MockServer::stop();
      }
      int MockMonitorServer::start_service()
      {
        int err = OB_SUCCESS;
        if(handler_ != NULL)
        {
          monitor_.set_node_register_handler(handler_);
          monitor_.set_slave_failure_handler(handler_);
          monitor_.set_change_master_handler(handler_);
        }
        else
        {
          err = OB_INNER_STAT_ERROR;
        }
        if(err == OB_SUCCESS)
        {
          err = monitor_.start();
        }
        if(err != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "fail to start. err=%d", err);
        }
        else
        {
          TBSYS_LOG(INFO, "start monitor succ~!");
        }
        return err;
      }

      tbnet::IPacketHandler::HPRetCode MockMonitorServer::handlePacket(tbnet::Connection* connection, tbnet::Packet *packet)
      {
        tbnet::IPacketHandler::HPRetCode rc = tbnet::IPacketHandler::FREE_CHANNEL;
        if (!packet->isRegularPacket())
        {
          TBSYS_LOG(WARN, "control packet, packet code: %d", ((tbnet::ControlPacket*)packet)->getCommand());
        }
        else
        {
          ObPacket* req = (ObPacket*) packet;
          req->set_connection(connection);
          int packet_code = req->get_packet_code();
          bool ps = false;
          switch(packet_code)
          {
            case OB_MMS_SERVER_REGISTER:
            case OB_MMS_SLAVE_DOWN:
              ps = monitor_.handlePacket(req);
              break;
            default:
              ps = MockServer::handlePacket(connection, packet);
              break;
          }
          if (!ps)
          {
            TBSYS_LOG(WARN, "overflow packet dropped, packet code: %d", req->getPCode());
            rc = tbnet::IPacketHandler::KEEP_CHANNEL;
          }
        }

        return rc;
      }
       bool MockMonitorServer::handlePacketQueue(tbnet::Packet *packet, void *args)
       {
         return MockServer::handlePacketQueue(packet, args);
       }

       int MockMonitorServer::do_request(ObPacket* base_packet)
      {
        int err = OB_SUCCESS;
        ObPacket* ob_packet = base_packet;
        int32_t packet_code = ob_packet->get_packet_code();
        err = ob_packet->deserialize();
        if(OB_SUCCESS == err)
        {
          switch(packet_code)
          {
            case OB_MMS_SERVER_REGISTER:
            case OB_MMS_SLAVE_DOWN:
              err = monitor_.handlePacket(ob_packet);
              break;
            default:
              err = OB_ERROR;
              TBSYS_LOG(ERROR, "wrong packet code:packet[%d]", packet_code);
              break;
          }
        }
        if(err != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "handle failed:ret[%d]", err);
        }
        return err;
      }
    }
  }
}

