#include "common/ob_define.h"
#include "mock_node_server.h"

using namespace oceanbase::common;
using namespace oceanbase::mms;
using namespace oceanbase::mms::tests;


MockNodeServer::MockNodeServer(int32_t port, int64_t retry_time, int64_t timeout, ObServer monitor, char* app_name, char* instance_name, char* hostname)
{
  port_ = port;
  retry_times_ = retry_time;
  timeout_ = timeout;
  monitor_.set_ipv4_addr(monitor.get_ipv4(), monitor.get_port());

  app_name_ = app_name;
  instance_name_ = instance_name;
  hostname_ = hostname;
  status_ = UNKNOWN;
}

MockNodeServer::~MockNodeServer()
{
  app_name_ = NULL;
  instance_name_ = NULL;
  hostname_ = NULL;
  if(handler_ != NULL)
  {
    delete handler_;
  }
  handler_ = NULL;
}

void MockNodeServer::stop()
{
  node_.destroy_();
  MockServer::stop();
}
int MockNodeServer::initialize()
{
  int err = MockServer::initialize();
  set_listen_port(port_);
  self_.set_ipv4_addr(MockServer::ip_addr_, port_);
  lease_.lease_interval = 7000 * 1000L;
  lease_.renew_interval = 4000 * 1000L;
  if(NULL == handler_)
  {
    handler_ = new(std::nothrow)Handler(this);
    if(handler_ == NULL)
    {
      err = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "fail to new handler. err= %d", err);
    }
  }
  return err;
}

int MockNodeServer::start_service()
{
  int err = OB_SUCCESS;
  if(handler_ != NULL)
  {
    node_.set_mms_stop_slave_handler(handler_);
    node_.set_slave_down_handler(handler_);
    node_.set_transfer_to_master_handler(handler_);
    node_.set_master_changed_handler(handler_);
    node_.set_least_timeout_handler(handler_);
  }
  else
  {
    err = OB_INNER_STAT_ERROR;
    TBSYS_LOG(ERROR, "hanlder should not be NULL, err=%d", err);
  } 
  if(OB_SUCCESS == err)
  {
    err = start_();
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to start. err=%d", err);
    }
  }   
  return err;
}


tbnet::IPacketHandler::HPRetCode MockNodeServer::handlePacket(tbnet::Connection* connection, tbnet::Packet *packet)
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
      case OB_MMS_HEART_BEAT:
      case OB_MMS_STOP_SLAVE:
      case OB_MMS_SLAVE_DOWN:
      case OB_MMS_TRANSFER_2_MASTER:
        ps = node_.handlePacket(req);
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

bool MockNodeServer::handlePacketQueue(tbnet::Packet *packet, void *args)
{
  return MockServer::handlePacketQueue(packet, args);
}

int MockNodeServer::do_request(ObPacket* base_packet)
{
  int err = OB_SUCCESS;
  ObPacket* req = static_cast<ObPacket*>(base_packet);                                                            
  int packet_code = req->get_packet_code();
  err = req->deserialize();
  if(OB_SUCCESS == err)
  {
    switch(packet_code)
    {
      case OB_MMS_HEART_BEAT:
      case OB_MMS_STOP_SLAVE:
      case OB_MMS_SLAVE_DOWN:
      case OB_MMS_TRANSFER_2_MASTER:
        err = node_.handlePacket(req);
        break;
      default:
        err = OB_ERROR;
        TBSYS_LOG(ERROR, "wrong packet code:packet[%d]", packet_code);
        break;
    }
  }
  if(err != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "check handle failed:ret[%d]", err);
  } 
  return err;
}

int MockNodeServer::start_()
{
  int err = OB_SUCCESS;
  err = node_.init(this, static_cast<int32_t>(MockServer::ip_addr_), port_, &(MockServer::client_manager_), lease_, &monitor_);
  if(err != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "fail to init node.");
  }
  if(err == OB_SUCCESS)
  {
    err = node_.start(app_name_, instance_name_, hostname_);
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to start node, err=%d", err);
    }
  }
  if(err == OB_SUCCESS)
  {
    err = node_.register_to_monitor(timeout_, retry_times_, monitor_, status_);
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "fail to register to monitor, err=%d", err);
    }
  }
  if(err == OB_SUCCESS)
  {
    if(status_ == SLAVE_NOT_SYNC)
    {
      TBSYS_LOG(INFO, "register succ,  as a slave");
    }
    else
    {
      if(status_ == MASTER_ACTIVE)
      {
        TBSYS_LOG(INFO, "register succ, as a master");
      }
    }
  }
  return err;
}


