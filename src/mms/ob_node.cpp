/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#include "ob_node.h"
#include "common/ob_define.h"
#include "updateserver/ob_ups_utils.h"
#include "common/ob_trace_log.h"

using namespace oceanbase::common;
using namespace oceanbase::mms;

ObNode::ObNode():rpc_buffer_(RESPONSE_PACKET_BUFFER_SIZE), thread_buffer_(RESPONSE_PACKET_BUFFER_SIZE),check_lease_task_(this)
{
  base_server_ = NULL;
  client_manager_ = NULL;
  status_ = UNKNOWN;
  host_name_ = NULL;
  app_name_ = NULL;
  instance_name_ = NULL;
  mms_stop_slave_handler_ = NULL;
  //transfer_state_handler_ = NULL;
  slave_down_handler_ = NULL;
  transfer_2_master_handler_ = NULL;
  master_changed_handler_ = NULL;
  queue_threads_num_ = DEFAULT_QUEUE_THREADS_NUM;
  queue_size_ = DEFAULT_QUEUE_SIZE;
  response_time_out_ = DEFAULT_RESPONSE_TIME_OUT;
}

ObNode::~ObNode()
{
  delete [] app_name_;
  app_name_ = NULL;
  delete [] instance_name_;
  instance_name_ = NULL;
  delete [] host_name_;
  host_name_ = NULL;
  base_server_ = NULL; 
  client_manager_ = NULL;                                                                                      
  app_name_ = NULL;
  instance_name_ = NULL;
  mms_stop_slave_handler_ = NULL;                                                                              
  //transfer_state_handler_ = NULL;                                                                              
  slave_down_handler_ = NULL;
  transfer_2_master_handler_ = NULL;
  master_changed_handler_ = NULL;
  destroy_();
}


int ObNode::init(ObBaseServer *server, int32_t ipv4, int32_t port, ObClientManager *client_manager, const ObLease &lease, ObServer *monitor_server)
{
  int err = OB_SUCCESS;
  if(server == NULL || client_manager == NULL || monitor_server == NULL)
  {
    TBSYS_LOG(WARN, "invalid param.");
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    base_server_ = server;
    // ObServer& self = server->get_self();
    self_server_.set_ipv4_addr(ipv4, port);
    renew_lease_(lease);
    client_manager_ = client_manager;
    monitor_server_.set_ipv4_addr(monitor_server->get_ipv4(), monitor_server->get_port());
    app_name_ = new(std::nothrow) char[OB_MAX_APP_NAME_LENGTH];
    instance_name_ = new(std::nothrow) char [OB_MAX_INSTANCE_NAME_LENGTH];
    host_name_ = new(std::nothrow) char[OB_MAX_HOST_NAME_LENGTH];
    if(NULL == app_name_ || instance_name_ == NULL || host_name_ == NULL)
    {
      TBSYS_LOG(WARN, "fail to malloc space");
      err = OB_ALLOCATE_MEMORY_FAILED;
    }
    if(OB_SUCCESS == err)
    {
      err = check_lease_timer_.init();
      if(OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "fail to init timer");
      }
    }
    if(OB_SUCCESS == err)
    {
      queue_thread_.setThreadParameter(queue_threads_num_, this, NULL);
    }
  }
  return err;
}

int ObNode::start(char *app_name, char *instance_name, char *hostname)
{
  int err = OB_SUCCESS;
  if(app_name == NULL || instance_name == NULL || hostname == NULL)
  {
    TBSYS_LOG(WARN, "invalid argument.");
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    memcpy(app_name_, app_name, strlen(app_name) + 1);
    memcpy(instance_name_, instance_name, strlen(instance_name) + 1);
    memcpy(host_name_, hostname, strlen(hostname) + 1);
    queue_thread_.start();
  }
  return err;
}

int ObNode::destroy_()
{
  int err = OB_SUCCESS;
  check_lease_timer_.destroy();
  queue_thread_.stop();
  queue_thread_.wait();
  return err;
}

bool ObNode::handlePacket(ObPacket *packet)
{
  bool res = true;
  if(NULL == packet)
  {
    TBSYS_LOG(WARN, "invalid packet.");
  }
  else
  {
    if(!queue_thread_.push(packet, queue_size_, false))
    {
      TBSYS_LOG(WARN, "packet queue temporarily full");
      res = false;
    }
  }
  return res;
}

bool ObNode::handlePacketQueue(tbnet::Packet *packet, void *args)
{
  UNUSED(args);
  bool ret = true;
  int return_code = OB_SUCCESS;

  ObPacket* ob_packet = reinterpret_cast<ObPacket*>(packet);
  int packet_code = ob_packet->get_packet_code();
  int version = ob_packet->get_api_version();
  return_code = ob_packet->deserialize();
  uint32_t channel_id = ob_packet->getChannelId();
  if(OB_SUCCESS != return_code)
  {
    TBSYS_LOG(ERROR, "packet deserialize error packet code is %d", packet_code);
  }
  else
  {
    int64_t packet_timewait = (0 == ob_packet->get_source_timeout()) ? DEFAULT_RESPONSE_TIME_OUT : ob_packet->get_source_timeout();
    ObDataBuffer *in_buf = ob_packet->get_buffer();
    if((ob_packet->get_receive_ts() + packet_timewait) < tbsys::CTimeUtil::getTime())
    {
      TBSYS_LOG(WARN, "packet wait too long time, receive_time=%ld, cur_time=%ld, packet_max_timewait=%ld, packet_code = %d", ob_packet->get_receive_ts(), tbsys::CTimeUtil::getTime(), packet_timewait, packet_code);
      return_code = OB_RESPONSE_TIME_OUT;
    }
    else if(NULL == in_buf)
    {
      TBSYS_LOG(WARN, "in_buff is NULL should not reach here");
      return_code = OB_INVALID_ARGUMENT;
    }
    else
    {
      tbnet::Connection* conn = ob_packet->get_connection();
      ThreadSpecificBuffer::Buffer* thread_buffer = thread_buffer_.get_buffer();
      if(thread_buffer != NULL)
      {
        thread_buffer->reset();
        ObDataBuffer out_buff(thread_buffer->current(), thread_buffer->remain());
        switch(packet_code)
        {
          case OB_MMS_HEART_BEAT:
            return_code = heartbeat(version, *in_buf, conn, channel_id, out_buff);
            break;
          case OB_MMS_STOP_SLAVE:
            return_code = stop_slave(version, *in_buf, conn, channel_id, out_buff);
            break;
          case OB_MMS_SLAVE_DOWN:
            return_code = slave_down(version, *in_buf, conn, channel_id, out_buff);
            break;
          case OB_MMS_TRANSFER_2_MASTER:
            return_code = transfer_2_master(version, *in_buf, conn, channel_id, out_buff);
            break;
          default:
            return_code = OB_ERROR;
            break;
        }

        if(OB_SUCCESS != return_code)
        {
          TBSYS_LOG(ERROR, "call func error packet_code is %d return code is %d", packet_code, return_code);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "get thread buffer error, ignore this packet");
      }
    }
  }
  return ret;
}

int ObNode::heartbeat(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buffer)
{
  int err = OB_SUCCESS;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  CLEAR_TRACE_LOG();
  FILL_TRACE_LOG("node heartbeat ack");
  if(version != MY_VERSION)
  {
    TBSYS_LOG(WARN, "version not equal. version=%d, MY_VERSION=%d", version, MY_VERSION);
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }

  ObLease lease;
  ObServer master;
  if(OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
  {
    err = lease.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to deserialize lease. err=%d", err);
    }
  }
  FILL_TRACE_LOG("deserialize lease. err=%d", err);

  if(OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
  {
    err = master.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to deserialize master_server. err=%d", err);
    }
  }
  FILL_TRACE_LOG("deserialize master node.err=%d", err);

  if(OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
  {
    result_msg.result_code_ = heartbeat_(lease, master); 
  }
  FILL_TRACE_LOG("heartbeat_. result_code_ =%d", result_msg.result_code_);

  if(OB_SUCCESS == err)
  {
    err = result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to serialize, err=%d", err);
    }
  }
  FILL_TRACE_LOG("serialize result_msg. err=%d", err);

  if(OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
  {
    err = serialization::encode_vi32(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position(), status_);
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "status serialize fail.err=%d", err);
    }
  }
  FILL_TRACE_LOG("serialize status. status_=%d, err=%d", status_, err);

  if(OB_SUCCESS == err)
  {
    err = base_server_->send_response(OB_MMS_HEART_BEAT_RESPONSE, MY_VERSION, out_buffer, conn, channel_id);
  }
  FILL_TRACE_LOG("send response.err=%d", err);
  PRINT_TRACE_LOG();
  return err;
}

int ObNode::stop_slave(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buffer)
{
  int err = OB_SUCCESS;
  UNUSED(in_buff);
  ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  if(MY_VERSION != version)
  {
    TBSYS_LOG(WARN, "version not equal. version=%d, MY_VERSION=%d", version, MY_VERSION);
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }

  if(OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_ )
  {
    if(mms_stop_slave_handler_ != NULL)
    {
      result_msg.result_code_ = mms_stop_slave_handler_->handleMMSStopSlave(); 
    }
    else
    {
      TBSYS_LOG(ERROR, "mms_stop_slave_handler not set");
      result_msg.result_code_ = OB_INNER_STAT_ERROR;
    }
  }

  if(OB_SUCCESS == err)
  {
    err = result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to serialize result_msg, err=%d", err);
    }
  }

  if(OB_SUCCESS == err)
  {
    err = base_server_->send_response(OB_MMS_STOP_SLAVE, MY_VERSION, out_buffer, conn, channel_id);
  }

  destroy_();
  return err;
}

int ObNode::transfer_state(const MMS_Status &new_state)
{
  int err = OB_SUCCESS;
  status_ = new_state;
  TBSYS_LOG(INFO, "transfer state to SLAVE_SYNC");
  return err;
}

int ObNode::slave_down(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn,
    const uint32_t channel_id, common::ObDataBuffer& out_buff)
{
  int err = OB_SUCCESS;
  ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  if(MY_VERSION != version)
  {
    TBSYS_LOG(WARN, "version not equal. version=%d, MY_VERSION=%d", version, MY_VERSION);
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }

  common::ObServer slave;
  if(err == OB_SUCCESS && OB_SUCCESS == result_msg.result_code_)
  {
    err = slave.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to deserialize slave server. err=%d", err);
    }
  }

  if(err == OB_SUCCESS && OB_SUCCESS == result_msg.result_code_)
  {
    if(slave_down_handler_ != NULL)
    {
      result_msg.result_code_ = slave_down_handler_->handleSlaveDown(slave);
    }
    else
    {
      result_msg.result_code_ = OB_INNER_STAT_ERROR;
      TBSYS_LOG(ERROR, "have not set slave_down handler.");
    }
  }

  if(OB_SUCCESS == err)
  {
    err = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to serialize result_msg, err=%d", err);
    }
  }

  if(OB_SUCCESS == err)
  {
    err = base_server_->send_response(OB_MMS_SLAVE_DOWN_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  return err;
}

int ObNode::transfer_2_master(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn,
    const uint32_t channel_id, common::ObDataBuffer& out_buff)
{
  UNUSED(in_buff);
  int err = OB_SUCCESS;
  ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  if(MY_VERSION != version)
  {
    TBSYS_LOG(WARN, "version not equal. version=%d, MY_VERSION=%d", version, MY_VERSION);
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }

  if(OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
  {
    if(status_ != SLAVE_SYNC)
    {
      TBSYS_LOG(ERROR, "can not transfer 2 master form %d", status_);
      result_msg.result_code_ = OB_ERROR;
    }
    else
    {
      status_ = MASTER_ACTIVE;
      if(transfer_2_master_handler_ != NULL)
      {
        result_msg.result_code_ = transfer_2_master_handler_->handleTransfer2Master();
      }
      else
      {
        TBSYS_LOG(ERROR, "transfer_2_master_handler not set");
        result_msg.result_code_ = OB_INNER_STAT_ERROR;
      }

    }
  }
  if(OB_SUCCESS == err)
  {
    err = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "result_msg.serialize error");
    }
  }

  if(OB_SUCCESS == err) 
  {
    err = base_server_->send_response(OB_MMS_TRANSFER_2_MASTER_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  return err;
}

int ObNode::register_to_monitor(const int64_t timeout, const int64_t retry_times, const ObServer &monitor, MMS_Status &status)
{
  int err = OB_ERROR;
  for(int64_t i = 0; (i < retry_times) && (err != OB_SUCCESS); i++)
  {
    int64_t timeu = tbsys::CTimeUtil::getMonotonicTime();
    err = register_to_monitor_(timeout, monitor, status);
    timeu = tbsys::CTimeUtil::getMonotonicTime() - timeu;
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "%ldth, register fail. err=%d", i, err);
      /* if(timeu < timeout)
         {
         TBSYS_LOG(INFO, "timeu=%ld not match timeout=%ld, will sleep %ld", timeu, timeout, timeout-timeu);
         int sleep_ret = precise_sleep(timeout - timeu);
         if(OB_SUCCESS != sleep_ret)
         {
         TBSYS_LOG(ERROR, "precise sleep, ret=%d", sleep_ret);
         }
         }*/
    }
    else
    {
      TBSYS_LOG(INFO, "regist to monitor succ!,start schedule");
      err = check_lease_timer_.schedule(check_lease_task_, DEFAULT_CHECK_LEASE_INTERVAL, true);
      if(OB_SUCCESS != err)
      { 
        TBSYS_LOG(WARN, "fail to schedule check_least_task, err = %d", err);                                     
      } 
    }
  }

  if(OB_SUCCESS != err)
  {
    TBSYS_LOG(ERROR, "can't register to monitor. monitor addr=%d", monitor.get_ipv4());
    err = OB_ERROR;
  }
  return err;
}

int ObNode::renew_lease(const int64_t timeout, const common::ObServer &monitor)
{
  int err = OB_SUCCESS;
  /*ObDataBuffer data_buffer;
    err = get_rpc_buffer(data_buffer);
    if(err != OB_SUCCESS)
    {
    TBSYS_LOG(WARN, "fail to get data buffer. err=%d", err);
    }

    if(OB_SUCCESS == err)
    {
    err = client_manager_->send_request(monitor, OB_MMS_RENEW_LEASE, MY_VERSION, timeout, data_buffer);
    if(err != OB_SUCCESS)
    {
    TBSYS_LOG(ERROR, "fail to send request. err=%d", err);
    }
    }
    int64_t pos = 0;
    ObResultCode result_msg;
    ObLease lease;
    if(OB_SUCCESS == err)
    {
    err = result_msg.deserialize(data_buffer.get_data(), data_buffer.get_capacity(), pos);
    if(OB_SUCCESS != err)
    {
    TBSYS_LOG(WARN, "fail to deserialize result_msg, err = %d", err);
    }
    else
    {
    err = result_msg.result_code_;
    }
    }

    if(OB_SUCCESS == err)
    {
    err = lease.deserialize(data_buffer.get_data(), data_buffer.get_capacity(), pos);
    if(OB_SUCCESS != err)
    {
    TBSYS_LOG(WARN, "fail to deserialize lease. err=%d", err);
    }
    }

    if(OB_SUCCESS == err)
    {
    err = renew_lease_(lease);
    }*/
  TBSYS_LOG(DEBUG, "node should renew lease.port=%d", self_server_.get_port());
  MMS_Status status;
  err = register_to_monitor_(timeout, monitor, status);
  return err;
}

int ObNode::heartbeat_(const ObLease &lease, const ObServer &master_server)
{
  int err = OB_SUCCESS;
  err = renew_lease_(lease);
  FILL_TRACE_LOG("renew lease. err=%d", err);
  if(err != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "fail to renew_lease_. err=%d", err);
  }

  if(OB_SUCCESS == err)
  {
    if(false == (master_server == master_server_))
    {
      master_server_.set_ipv4_addr(master_server.get_ipv4(), master_server.get_port());
      if(true ==(master_server == self_server_))
      {
        TBSYS_LOG(INFO, "I am the master now.");
      }
      else
      {
        if(master_changed_handler_ != NULL)
        {
          err = master_changed_handler_->handleMasterChanged();
          if(OB_SUCCESS != err)
          {
            TBSYS_LOG(ERROR, "fail to handleMasterChanged. err=%d", err);
          }
        }
        else
        {
          err = OB_INNER_STAT_ERROR;
        }
      }
    }
  }
  return err;
}

int ObNode::register_to_monitor_(const int64_t timeout, const ObServer &monitor, MMS_Status &status)
{
  int err = OB_SUCCESS;
  ObDataBuffer data_buffer;
  err = get_rpc_buffer(data_buffer);
  CLEAR_TRACE_LOG();
  if(err != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "fail to get data buffer. err=%d", err);
  }
  ObServerExt server_ext;
  server_ext.init(host_name_, self_server_);
  if(OB_SUCCESS == err)
  {
    err = server_ext.serialize(data_buffer.get_data(), data_buffer.get_capacity(), data_buffer.get_position());
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to serialize self_server_. err=%d", err);
    }
  }
  FILL_TRACE_LOG("step1: serialize server. err=%d", err);

  //step2: 序列化app_name
  if(OB_SUCCESS == err)
  {
    err = serialization::encode_vstr(data_buffer.get_data(), data_buffer.get_capacity(), data_buffer.get_position(), app_name_);
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to encode str of app_name. app_name_=%s, err=%d", app_name_, err);
    }
  }
  FILL_TRACE_LOG("step2:serialize app_name, app_name=%s, err=%d", app_name_, err);

  //step3:序列化instance_name
  if(OB_SUCCESS == err)
  {
    err = serialization::encode_vstr(data_buffer.get_data(), data_buffer.get_capacity(), data_buffer.get_position(), instance_name_);
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to encode str of app_name. instance_name_=%s, err=%d", instance_name_, err);
    }
  }
  FILL_TRACE_LOG("step3: serialize instacne_name. instance_name_=%s, err=%d", instance_name_, err);
  FILL_TRACE_LOG( "start to send request");
  
  //step4:send request
  if(OB_SUCCESS == err)
  {
    err = client_manager_->send_request(monitor, OB_MMS_SERVER_REGISTER, MY_VERSION, timeout, data_buffer);
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to send request to monitor, port=%d. err=%d", self_server_.get_port(), err);
    }
  }
  FILL_TRACE_LOG("step4:send request. err=%d, port=%d", err, self_server_.get_port());
  //step5: deserialize the result code
  int64_t pos = 0;
  if(OB_SUCCESS == err)
  {
    ObResultCode result_code;
    err = result_code.deserialize(data_buffer.get_data(), data_buffer.get_position(), pos);
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to deserialize result_code, err=%d", err);
    }
    else
    {
      err = result_code.result_code_;
    }
  }
  FILL_TRACE_LOG("setp5: deserialize result_code.err=%d", err);

  //step6: get the server role
  if(OB_SUCCESS == err)
  {
    err = serialization::decode_vi32(data_buffer.get_data(), data_buffer.get_position(), pos, reinterpret_cast<int32_t *>(&status));
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to decode status_, err = %d", err);
    }
    else
    {
      status_ = status;
    }  
  }
  FILL_TRACE_LOG("step6: decode status. err=%d", err);
  //step7: 设置lease时间
  if(OB_SUCCESS == err)
  {
    ObLease default_lease;
    default_lease.lease_time = tbsys::CTimeUtil::getTime();
    default_lease.lease_interval = DEFAULT_LEASE_INTERVAL;
    default_lease.renew_interval = DEFAULT_RENEW_INTERVAL;
    err = renew_lease_(default_lease);
  }
  FILL_TRACE_LOG("step7:renew lease. err=%d", err);
  PRINT_TRACE_LOG();
  return err;
}

int ObNode::renew_lease_(const ObLease &lease)
{
  int err = OB_SUCCESS;
  lease_.lease_time = lease.lease_time;
  lease_.lease_interval = lease.lease_interval;
  lease_.renew_interval = lease.renew_interval;
  FILL_TRACE_LOG("renew lease. lease_time=%ld, lease_interval=%ld, renew_interval=%ld", lease_.lease_time, lease_.lease_interval, lease_.renew_interval);
  return err;
}

int ObNode::get_rpc_buffer(common::ObDataBuffer &data_buffer)const
{
  int err = OB_SUCCESS;
  common::ThreadSpecificBuffer::Buffer* rpc_buffer = rpc_buffer_.get_buffer();
  if(NULL == rpc_buffer)
  {
    TBSYS_LOG(ERROR, "get thread rpc buff fail.buffer[%p]", rpc_buffer);
    err = OB_INNER_STAT_ERROR;
  }
  else
  {
    rpc_buffer->reset();
    data_buffer.set_data(rpc_buffer->current(), rpc_buffer->remain());
  }
  return err;
}

int ObNode::report_slave_down(const int64_t timeout, const int64_t retry_times, 
    const common::ObServer &monitor, const common::ObServer &slave)
{
  int err = OB_ERROR;                                                                   
  for(int64_t i = 0; (i < retry_times) && (err != OB_SUCCESS); i++)                           
  { 
    err = report_slave_down_(timeout, monitor, slave);                                 
    if(OB_SUCCESS != err)
    { 
      TBSYS_LOG(WARN, "%ldth, report fail. err=%d, port=%d", i, err, monitor.get_port());     
    }
    else                                                                                  
    { 
      TBSYS_LOG(INFO, "report slave down succ!");                                         
    }                                                                                     
  }                                                                                       

  if(OB_SUCCESS != err)                                                                   
  { 
    TBSYS_LOG(ERROR, "can't report to monitor, monitor addr=%d", monitor.get_ipv4());   
    err = OB_ERROR;
  }
  return err;
}

int ObNode::report_slave_down_(const int64_t timeout, const ObServer &monitor, const ObServer &slave)
{
  int err = OB_SUCCESS;

  ObDataBuffer data_buffer;
  err = get_rpc_buffer(data_buffer);
  if(err != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "fail to get rpc buffer. err=%d", err);
  }

  //step1:serialize slave
  if(OB_SUCCESS == err)
  {
    err = slave.serialize(data_buffer.get_data(), data_buffer.get_capacity(), data_buffer.get_position());
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to serialize slave node .err=%d", err);
    }
  }

  //step2:serialize app_name
  if(OB_SUCCESS == err)
  {
    err = serialization::encode_vstr(data_buffer.get_data(), data_buffer.get_capacity(), data_buffer.get_position(), app_name_);
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to serialize app_name.err=%d", err);
    }
  }

  //step3:serialize instance_name
  if(OB_SUCCESS == err)
  {
    err = serialization::encode_vstr(data_buffer.get_data(), data_buffer.get_capacity(), data_buffer.get_position(), instance_name_);
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to serialzie instance_name. err=%d", err);
    }
  }

  //step4:send request
  if(OB_SUCCESS == err)
  {
    err = client_manager_->send_request(monitor, OB_MMS_SLAVE_DOWN, MY_VERSION, timeout, data_buffer);
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to send request to monitor. err=%d, monitor addr=%di, port=%d", err, monitor.get_ipv4(), monitor.get_port());
    }
  }
  
  //step5:deserialize the result code
  int64_t pos = 0;
  if(OB_SUCCESS == err)
  {
    ObResultCode result_code;
    err = result_code.deserialize(data_buffer.get_data(), data_buffer.get_capacity(), pos);
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to deserialize result_code");
    }
    else
    {
      err = result_code.result_code_;
    }
  }
  return err;
}

int ObNode::handle_lease_timeout()
{
  int err = OB_SUCCESS;
  status_ = UNKNOWN;
  if(lease_timeout_handler_ != NULL)
  {
  err = lease_timeout_handler_->handleLeaseTimeout();
  if(OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "fail to handle lease timeout");
  }
  }
  else
  {
    TBSYS_LOG(WARN, "have not set lease_timeout_handler_");
    err = OB_INNER_STAT_ERROR;
  }
  return err;
}

int ObNode::set_mms_stop_slave_handler(IMMSStopSlave *mms_stop_slave_handler)
{
  UNUSED(mms_stop_slave_handler);
  int ret = OB_SUCCESS;
  mms_stop_slave_handler_ = mms_stop_slave_handler;
  return ret;
}
/*
int ObNode::set_transfer_state_handler(ITransferState *transfer_state_handler)
{
  UNUSED(transfer_state_handler);
  int ret = OB_SUCCESS;
  transfer_state_handler_ = transfer_state_handler;
  return ret;
}
*/
int ObNode::set_slave_down_handler(ISlaveDown *slave_down_handler)
{
  UNUSED(slave_down_handler);
  int ret = OB_SUCCESS;
  slave_down_handler_ = slave_down_handler;
  return ret;
}

int ObNode::set_transfer_to_master_handler(ITransfer2Master *transfer_2_master_handler)
{
  int ret = OB_SUCCESS;
  transfer_2_master_handler_ = transfer_2_master_handler;
  return ret;
}

int ObNode::set_master_changed_handler(IMasterChanged *master_changed_handler)
{
  int ret = OB_SUCCESS;
  master_changed_handler_ = master_changed_handler;
  return ret;
}

int ObNode::set_least_timeout_handler(ILeaseTimeout *lease_timeout_handler)
{
  int err = OB_SUCCESS;
  if(NULL == lease_timeout_handler)
  {
    TBSYS_LOG(WARN, "invalid argument");
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    lease_timeout_handler_ = lease_timeout_handler;
  }
  return err;  
}


