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
 *     rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 */

#include "ob_monitor.h"
#include "common/ob_result.h"
#include "common/ob_trace_log.h"
#include "common/ob_define.h"

using namespace oceanbase::common;
using namespace oceanbase::mms;
using namespace oceanbase::common::hash;

ObMonitor::ObMonitor():node_map_(), rpc_buffer_(RESPONSE_PACKET_BUFFER_SIZE), 
  thread_buffer_(RESPONSE_PACKET_BUFFER_SIZE)
{
  base_server_ = NULL;
  client_manager_ = NULL;
  node_register_handler_ = NULL;
  slave_failure_handler_ = NULL;
  change_master_handler_ = NULL;
  queue_threads_num_ = DEFAULT_QUEUE_THREADS_NUM;
  queue_size_ = DEFAULT_QUEUE_SIZE;
}

ObMonitor::~ObMonitor()
{
  base_server_ = NULL;
  client_manager_ = NULL;
  node_register_handler_ = NULL;
  slave_failure_handler_ = NULL;
  change_master_handler_ = NULL;
  //node_map_.clear();
  clear_();
  destroy();
}

int ObMonitor::clear_()
{
  node_map_t::iterator it = node_map_.begin();
  
  while(it != node_map_.end())
  {
    delete [] it->first.app_name;
    it->first.app_name = NULL;
    delete [] it->first.instance_name;
    it->first.instance_name = NULL;

    node_list_type::iterator it_list = it->second->node_list.begin();
    
    while(it_list != it->second->node_list.end())
    {
      delete (*it_list);
      it_list ++;
    }

    delete (it->second);
    it ++;
  }
  node_map_.clear();
  return OB_SUCCESS;
}

int ObMonitor::destroy()
{
  int err = OB_SUCCESS;
  heartbeat_timer_.destroy();
  queue_thread_.stop();
  queue_thread_.wait();
  return err;
}

int ObMonitor::init(ObBaseServer *server, ObClientManager *client_manager, int64_t lease_interval, int64_t renew_interval, int64_t timeout, int64_t retry_times)
{
  int err = OB_SUCCESS;
  if(server == NULL || client_manager == NULL)
  {
    TBSYS_LOG(WARN, "invalid argument");
    err = OB_INVALID_ARGUMENT;
  }
  if(OB_SUCCESS == err)
  {
    base_server_ = server;
    timeout_ = timeout;
    retry_times_ = retry_times;
    lease_interval_ = lease_interval;
    renew_interval_ = renew_interval;
    client_manager_ = client_manager;
    err = heartbeat_timer_.init();
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to init timer");
    }
  }
  if(OB_SUCCESS == err)
  {
    heartbeat_task_ = new (std::nothrow)ObMMSHeartbeatTask(this);
    if(heartbeat_task_ == NULL)
    {
      err = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "fail to new heartbeat_task_, err=%d", err);
    }
  }
  if(OB_SUCCESS == err)
  {
    queue_thread_.setThreadParameter(queue_threads_num_, this, NULL);
    if(0 != node_map_.create(NODE_HASH_SIZE))
    {
      err = OB_ERROR;
      TBSYS_LOG(ERROR, "create node map fail");
    }
  }
  return err;
}

int ObMonitor::start()
{
  int err = OB_SUCCESS;
  queue_thread_.start();
  err = heartbeat_timer_.schedule(*heartbeat_task_, DEFAULT_HEARTBEAT_INTERVAL, true);
  if(OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "fail to schedule heasrbeat task, err=%d", err);
  }
  return err;
}

//将包放入队列
bool ObMonitor::handlePacket(common::ObPacket *packet)
{
  bool err = true;
  if(NULL == packet)
  {
    TBSYS_LOG(WARN, "invalid packet.");
  }
  else
  {
    if(!queue_thread_.push(packet, queue_size_, false))
    {
      TBSYS_LOG(WARN, "packet queue temporarily full");
      err = false;
    }
  }
  return err;
}

//处理队列里的包 
bool ObMonitor::handlePacketQueue(tbnet::Packet *packet, void *args)
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
    int64_t packet_timewait = (0 == ob_packet->get_source_timeout()) ? timeout_ : ob_packet->get_source_timeout();
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
          //todo
          case OB_MMS_SERVER_REGISTER:
            return_code = node_register(version, *in_buf, conn, channel_id, out_buff);
            break;
          case OB_MMS_SLAVE_DOWN:
            return_code = slave_failure(version, *in_buf, conn, channel_id, out_buff);
            break;
            /*case OB_MMS_RENEW_LEASE:
              return_code = node_renew_lease(version, *in_buf, conn, channel_id, out_buff);
              break;*/
          default:
            TBSYS_LOG(WARN, "UNKNOWN packet.packet_code=%d", packet_code);
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

int ObMonitor::node_register(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn,
    const uint32_t channel_id, common::ObDataBuffer& out_buff)
{
  int err = OB_SUCCESS;
  ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  if(MY_VERSION != version)
  {
    TBSYS_LOG(WARN, "version not equal. version=%d, MY_VERSION=%ld", version, MY_VERSION);
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }

  //step1: deserialize the node and app_name instance_name
  ObServerExt node;
  char app_name[OB_MAX_APP_NAME_LENGTH];
  char instance_name[OB_MAX_INSTANCE_NAME_LENGTH];

  if(OB_SUCCESS == err && result_msg.result_code_ == OB_SUCCESS)
  {
    err = node.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to deserialize node server. err = %d", err);
    } 
  }

  int64_t len = 0;
  if(OB_SUCCESS == err && result_msg.result_code_ == OB_SUCCESS)
  {
    serialization::decode_vstr(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(),
        app_name, OB_MAX_APP_NAME_LENGTH, &len);
    if(-1 == len)
    {
      err = OB_ERROR;
    }
  }

  len = 0;
  if(OB_SUCCESS == err && result_msg.result_code_ == OB_SUCCESS)
  {
    serialization::decode_vstr(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(),
        instance_name, OB_MAX_INSTANCE_NAME_LENGTH, &len);
    if(-1 == len)
    {
      err = OB_ERROR;
    } 
  }

  MMS_Status status = UNKNOWN;
  bool is_master = false;
  //step2: set the node status
  if(OB_SUCCESS == err && result_msg.result_code_ == OB_SUCCESS)
  {
    result_msg.result_code_ = node_register_(node, app_name, instance_name, status);
    if(result_msg.result_code_ != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to register node. err = %d", result_msg.result_code_);
    }
    else if(status == MASTER_ACTIVE)
    {
      is_master = true;
    }
    else
    {
      is_master = false;
    }
  }

  //step3: handleNodeRegister 
  if(OB_SUCCESS == err && result_msg.result_code_ == OB_SUCCESS)                                               
  { 
    if(node_register_handler_ != NULL)                                                                         
    { 
      result_msg.result_code_ = node_register_handler_->handleNodeRegister(node, app_name, instance_name, is_master);              
    }
    else                                                                                                       
    { 
      TBSYS_LOG(ERROR, "have not set node_register_handler_");                                                 
      result_msg.result_code_ = OB_INNER_STAT_ERROR;                                                                               
    }
    if(result_msg.result_code_ != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to handleNodeRegister. err=%d", result_msg.result_code_);
    }
  }

  //step4: serialize result_code
  if(OB_SUCCESS == err)
  {
    err = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to serialize result_msg,err=%d", err);
    }
  }

  //step5: serialize status 
  if(OB_SUCCESS == err && result_msg.result_code_ == OB_SUCCESS)
  {
    err = serialization::encode_vi32(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position(), status);
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to serialize status, err =%d", err);
    }
  }

  //setp6: send response
  if(OB_SUCCESS == err)
  {
    err = base_server_->send_response(OB_MMS_NODE_REGISTER_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to send response. err=%d", err);
    }
  }
  return err;
}

int ObMonitor::slave_failure(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn,
    const uint32_t channel_id, common::ObDataBuffer& out_buff)
{
  int err = OB_SUCCESS;
  ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  if(MY_VERSION != version)
  {
    TBSYS_LOG(WARN, "version not equal. version=%d, MY_VERSION=%ld", version, MY_VERSION);
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }

  //step1: deserialize the slave node, app_name, instance_name
  ObServer slave_node;
  if(OB_SUCCESS == err && result_msg.result_code_ == OB_SUCCESS)
  {
    err = slave_node.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to deserialize slave node.err=%d", err);
    }
  }

  char app_name[OB_MAX_APP_NAME_LENGTH];
  char instance_name[OB_MAX_INSTANCE_NAME_LENGTH];

  int64_t len = 0; 
  if(OB_SUCCESS == err && result_msg.result_code_ == OB_SUCCESS)                                               
  { 
    serialization::decode_vstr(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(),             
        app_name, OB_MAX_APP_NAME_LENGTH, &len);                                                              
    if(-1 == len)                                                                                              
    { 
      err = OB_ERROR;                                                                                          
    }                                                                                                          
  }                                                                                                            

  len = 0;
  if(OB_SUCCESS == err && result_msg.result_code_ == OB_SUCCESS)                                               
  { 
    serialization::decode_vstr(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(),                
        instance_name, OB_MAX_INSTANCE_NAME_LENGTH, &len);                                                     
    if(-1 == len)                                                                                              
    { 
      err = OB_ERROR;                                                                                          
    }                                                                                                          
  }                 

  //step2: do the process
  if(OB_SUCCESS == err && result_msg.result_code_ == OB_SUCCESS)
  {
    result_msg.result_code_ = slave_failure_(slave_node, app_name, instance_name);
    if(OB_SUCCESS != result_msg.result_code_)
    {
      TBSYS_LOG(WARN, "fail to process slave failure, err=%d", result_msg.result_code_);
    }
  }

  //step3: serialize the resultcode
  if(OB_SUCCESS == err)
  {
    err = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to serialize the result code .err=%d", err);
    }
  }

  //step4: send response
  if(OB_SUCCESS == err)
  {
    err = base_server_->send_response(OB_MMS_SLAVE_FAILURE_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to send_response, err=%d", err);
    }
  }  
  return err;
}

int ObMonitor::exist_master(const char *app_name, const char *instance_name, bool &is_exist)
{
  int err = OB_SUCCESS;
  //node_list_type *pnodelist = NULL;
  mms_value *pvalue = NULL;
  is_exist = false;
  mms_key key;
  key.app_name = const_cast<char*>(app_name);
  key.instance_name = const_cast<char*>(instance_name);
  if(HASH_EXIST == node_map_.get(key, pvalue) && pvalue != NULL)
  {
    if(pvalue->has_master == true)
    {
      is_exist = true;
    }
  }
  return err;
}

int ObMonitor::select_master(node_list_type *pnodelist, ObServer &new_master)
{
  int err = OB_ERROR;
  if(pnodelist == NULL)
  {
    TBSYS_LOG(WARN, "invalid argument");
    err = OB_INVALID_ARGUMENT;
  }
  if(OB_ERROR == err)
  {
    node_list_type::iterator it = (*pnodelist).begin();
    while(it != (*pnodelist).end())
    {
      if((*it)->status == SLAVE_SYNC)
      {
        new_master.set_ipv4_addr((*it)->node_server.get_ipv4(), (*it)->node_server.get_port());
        (*it)->status = MASTER_ACTIVE;
        err = OB_SUCCESS;
        break;
      }
      it ++;
    }
  }
  return err;
}

bool ObMonitor::if_node_exist(node_list_type *pnodelist, const ObServer &node, node_list_type::iterator &it_server)
{
  int res = false;
  if(pnodelist != NULL)
  {
    node_list_type::iterator it = pnodelist->begin();
    while(it != pnodelist->end())
    {
      //ObServer server = node->get_server();
      if(node == (*it)->node_server)
      {
        res = true;
        it_server = it;
        break;
      }
      it ++;
    }
  }
  return res;
}

bool ObMonitor::if_node_exist(const ObServer &node, const char *app_name, const char *instance_name, node_list_type * &pnodelist, node_list_type::iterator &it_server)
{
  bool res = false;
  if(app_name == NULL || instance_name == NULL)
  {
    TBSYS_LOG(WARN, "invalid arugment.");
  }
  else
  {
    //node_list_type *pnodelist;
    mms_value *pvalue = NULL;
    mms_key key;
    key.app_name = const_cast<char *>(app_name);
    key.instance_name = const_cast<char *>(instance_name);
    if(HASH_EXIST == node_map_.get(key, pvalue))
    {
      if(pvalue != NULL)
      {
        pnodelist = &((*pvalue).node_list);
        node_list_type::iterator it = pnodelist->begin();
        while(it != pnodelist->end())
        {
          if(true ==((*it)->node_server == node))
          {
            res = true;
            it_server = it;
            break;
          }
          it++;
        }
      }
    }
  }
  return res;
}

int ObMonitor::add_register_node(const ObServer &node, const char *app_name, const char *instance_name, const MMS_Status status)
{
  int err = OB_SUCCESS;
  if(app_name == NULL || instance_name == NULL)
  {
    TBSYS_LOG(ERROR, "should not be here.");
    err = OB_INVALID_ARGUMENT;
  }
  //node_list_type * pnodelist = NULL;
  mms_value *pvalue = NULL;
  mms_key key;
  key.app_name = const_cast<char*>(app_name);
  key.instance_name = const_cast<char *>(instance_name);
  if(OB_SUCCESS == err)
  {
    if(HASH_EXIST == node_map_.get(key, pvalue))
    {
      node_list_type *pnodelist = &((*pvalue).node_list);
      node_list_type::iterator it_server = pnodelist->end();
      bool node_exist = if_node_exist(pnodelist, node, it_server);
      if(node_exist == true)
      {
        TBSYS_LOG(DEBUG, "re_register node. reset the node status");
        (*it_server)->is_valid = true;
        (*it_server)->lease.lease_time = tbsys::CTimeUtil::getTime();
        (*it_server)->lease.lease_interval = lease_interval_;
        (*it_server)->lease.renew_interval = renew_interval_;
      }
      else
      {
        TBSYS_LOG(DEBUG, "already have register node in the list, add node_info");
        node_info *node_server = new(std::nothrow) node_info;
        if(node_server == NULL)
        {
          TBSYS_LOG(WARN, "fail to new node_server.");
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          node_server->node_server.set_ipv4_addr(node.get_ipv4(), node.get_port());
          node_server->status = status;
          node_server->is_valid = true;
          node_server->lease.lease_time = tbsys::CTimeUtil::getTime();                                           
          node_server->lease.lease_interval = lease_interval_;                                             
          node_server->lease.renew_interval = renew_interval_;
          pnodelist->push_back(node_server);
        }
      }
    }
    else
    {
      TBSYS_LOG(DEBUG, "a new app+instance node register. add a map pair");
      key.app_name = new(std::nothrow)char [OB_MAX_APP_NAME_LENGTH];
      key.instance_name = new(std::nothrow)char [OB_MAX_INSTANCE_NAME_LENGTH];
      pvalue = new(std::nothrow) mms_value;
      node_info *node_server = new(std::nothrow)node_info;
      if(key.app_name == NULL || key.instance_name == NULL || node_server == NULL || pvalue == NULL)
      {
        TBSYS_LOG(WARN, "fail to allocate memory");
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        node_list_type *pnodelist = &(pvalue->node_list);
        memcpy(key.app_name, app_name, strlen(app_name) + 1);
        memcpy(key.instance_name, instance_name, strlen(instance_name) + 1);
        node_server->node_server.set_ipv4_addr(node.get_ipv4(), node.get_port());
        node_server->status = status;
        node_server->is_valid = true;
        node_server->lease.lease_time = tbsys::CTimeUtil::getTime();
        node_server->lease.lease_interval = lease_interval_;
        node_server->lease.renew_interval = renew_interval_;
        pnodelist->push_back(node_server);
        int hash_err = node_map_.set(key, pvalue);
        if(hash_err != HASH_INSERT_SUCC)
        {
          TBSYS_LOG(WARN, "fail to insert to node_map");
          err = OB_ERROR;
        }
        else
        {
          TBSYS_LOG(DEBUG, "add map pair succ~!");
        }
      }
    }
  }
  if(err == OB_SUCCESS && status == MASTER_ACTIVE)
  {
    pvalue->has_master = true;
    pvalue->master.set_ipv4_addr(node.get_ipv4(), node.get_port());
  }
  return err;
}

int ObMonitor::node_register_(const ObServerExt &node, const char *app_name, const char *instance_name, MMS_Status &status)
{
  int err = OB_SUCCESS;
  if(app_name == NULL || instance_name == NULL)
  {
    TBSYS_LOG(WARN, "invalid argument.");
    err = OB_INVALID_ARGUMENT;
  }

  if(OB_SUCCESS == err)
  {
    bool is_exist = false;
    exist_master(app_name, instance_name, is_exist);
    if(is_exist == false)
    {
      TBSYS_LOG(DEBUG, "no master exist, set the node be!");
      status = MASTER_ACTIVE;
    }
    else
    {
      TBSYS_LOG(DEBUG, "already have master. set the node be slave");
      status = SLAVE_NOT_SYNC;
    }
  }

  if(OB_SUCCESS == err)
  {
    ObServer server = node.get_server();
    err = add_register_node(server, app_name, instance_name, status);
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to add register node. err=%d", err);
    }
  }
  return err;
}

int ObMonitor::slave_failure_(const ObServer &slave, const char *app_name, const char *instance_name)
{
  int err = OB_SUCCESS;
  if(NULL == app_name || NULL == instance_name)
  {
    TBSYS_LOG(WARN, "invalid argument");
    err = OB_INVALID_ARGUMENT;
  }

  if(OB_SUCCESS == err)
  {
    node_list_type::iterator it;
    node_list_type *pnodelist = NULL;
    if(true == if_node_exist(slave, app_name, instance_name, pnodelist, it))
    {
      (*it)->status = UNKNOWN;
      pnodelist->erase(it);
    }
    else
    {
      TBSYS_LOG(WARN, "slave entity not exist");
      err = OB_ENTRY_NOT_EXIST;
    }
  }

  if(OB_SUCCESS == err)
  {
    if(NULL != slave_failure_handler_)
    {
      err = slave_failure_handler_->handleSlaveFailure(slave);
      if(err != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "fail to handler slave failure");
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "handleSlaveFailure should not be NULL");
      err = OB_INNER_STAT_ERROR;
    }
  }
  return err;
}

int ObMonitor::heartbeat(const int64_t timeout, const ObServer &master, const ObServer &node, MMS_Status &status)
{
  int err = OB_SUCCESS;
  ObDataBuffer data_buffer;
  err = get_rpc_buffer(data_buffer);

  if(OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "fail to get rpc buffer. err=%d", err);
  }

  //step1:serialize lease
  ObLease lease;
  lease.lease_time = tbsys::CTimeUtil::getTime();
  lease.lease_interval = lease_interval_;
  lease.renew_interval = renew_interval_;
  if(OB_SUCCESS == err)
  {
    err = lease.serialize(data_buffer.get_data(), data_buffer.get_capacity(), data_buffer.get_position());
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to serialize lease.err=%d", err);
    }
  }

  FILL_TRACE_LOG("1.serailize lease, err= %d", err);

  //step2:serialize master server 
  if(OB_SUCCESS == err)
  {
    err = master.serialize(data_buffer.get_data(), data_buffer.get_capacity(), data_buffer.get_position());
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to serialize master");
    }
  }
  FILL_TRACE_LOG("2.serialize master server. err=%d", err);
  //step3:send request to node
  if(OB_SUCCESS == err)
  {
    err = client_manager_->send_request(node, OB_MMS_HEART_BEAT, MY_VERSION, timeout, data_buffer);
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to send request. err=%d", err);
    }
  }
  FILL_TRACE_LOG("3. send request to node. node ip=%d, err=%d", node.get_ipv4(), err);
  //step4:deserizlie the result_code
  ObResultCode result_code;
  int64_t pos = 0;
  if(OB_SUCCESS == err)
  {
    err = result_code.deserialize(data_buffer.get_data(), data_buffer.get_position(), pos);
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to deserialize result_code");
    }
    else
    {
      err = result_code.result_code_;
    }
  }
  FILL_TRACE_LOG("4.deserialize result code. err=%d", err);

  //step5: deserializiton the node status
  if(OB_SUCCESS == err)
  {
    err = serialization::decode_vi32(data_buffer.get_data(), data_buffer.get_position(), pos, reinterpret_cast<int32_t*>(&status));
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to decode status.err=%d", err);
    }
  }
  FILL_TRACE_LOG("5.deserializiton the node status, status=%d, err=%d", status, err);
  return err;  
}


int ObMonitor::report_slave_failure(const int64_t timeout, const int64_t retry_times, const ObServer slave, const ObServer master)
{
  int err = OB_ERROR;
  for(int64_t i = 0; (i < retry_times) && (err != OB_SUCCESS); i++)
  {
    int64_t timeu = tbsys::CTimeUtil::getMonotonicTime();
    err = report_slave_failure_(timeout, slave, master);
    timeu = tbsys::CTimeUtil::getMonotonicTime() - timeu;
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "%ldth, report slave failure fail. err =%d", i, err);
    }
    else
    {
      TBSYS_LOG(INFO, "report slave failure to master succ!");
    }      
  }

  if(OB_SUCCESS != err)
  {
    TBSYS_LOG(ERROR, "can't report failure to master node. master addr =%d", master.get_ipv4());
    err = OB_ERROR;
  }
  return err;
}

int ObMonitor::get_rpc_buffer(common::ObDataBuffer &data_buffer)const                        
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

int ObMonitor::report_slave_failure_(const int64_t timeout, const ObServer &slave, const ObServer &master)
{
  int err = OB_SUCCESS;
  ObDataBuffer data_buffer;
  err = get_rpc_buffer(data_buffer);
  if(err != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "fail to get data buffer. err=%d", err);
  }

  //step 1:serialize slave node
  if(OB_SUCCESS == err)
  {
    err = slave.serialize(data_buffer.get_data(), data_buffer.get_capacity(), data_buffer.get_position());
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to serialize slave node. err=%d", err);
    }
  }

  //step2: send request
  if(OB_SUCCESS == err)
  {
    err = client_manager_->send_request(master, OB_MMS_SLAVE_DOWN, MY_VERSION, timeout, data_buffer);
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to send request to master. err=%d", err);
    }
  }
  //step3:deserialize the result code
  int64_t pos = 0;
  if(OB_SUCCESS == err)
  {
    ObResultCode result_code;
    err = result_code.deserialize(data_buffer.get_data(), data_buffer.get_capacity(), pos);
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to deserialize result_code, err=%d", err);
    }
    else
    {
      err = result_code.result_code_;
    }
  }
  return err;
}

int ObMonitor::change_master(const int64_t timeout, const int64_t retry_times, const ObServer &new_master)
{
  int err = OB_ERROR;
  for(int64_t i = 0; (i < retry_times) && (err != OB_SUCCESS); i++)                                                
  { 
    err = change_master_(timeout, new_master);
    if(OB_SUCCESS != err)                                                                                      
    { 
      TBSYS_LOG(WARN, "%ldth, report change master fail. err =%d", i, err);                                     
    }
    else                                                                                                       
    { 
      TBSYS_LOG(INFO, "report change master to master succ!");                                                 
    }                                                                                                          
  }                                                                                                            

  if(OB_SUCCESS != err)                                                                                        
  { 
    TBSYS_LOG(ERROR, "can't report to master node. master addr =%d, err=%d", new_master.get_ipv4(), err);               
  }
  return err;            
}

int ObMonitor::change_master_(const int64_t timeout, const ObServer& new_master)
{
  int err = OB_SUCCESS;
  ObDataBuffer data_buffer;
  err = get_rpc_buffer(data_buffer);
  if(err != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "fail to get data buffer. err=%d", err);
  }
 
  if(OB_SUCCESS == err)
  {
    err = client_manager_->send_request(new_master, OB_MMS_TRANSFER_2_MASTER, MY_VERSION, timeout, data_buffer);
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to send request to new master .err = %d", err);
    }
  }

  int64_t pos = 0;
  if(OB_SUCCESS == err)
  {
    ObResultCode result_code;
    err = result_code.deserialize(data_buffer.get_data(), data_buffer.get_position(), pos);
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to deserialize result_code. err=%d", err);
    }
    else
    {
      err = result_code.result_code_;
    }
  }
  return err;
}

int ObMonitor::set_node_register_handler(INodeRegister *node_register_handler)
{
  UNUSED(node_register_handler);
  int ret = OB_SUCCESS;
  node_register_handler_ = node_register_handler;
  return ret;
}

int ObMonitor::set_slave_failure_handler(ISlaveFailure *slave_failure_handler)
{
  UNUSED(slave_failure_handler);
  int ret = OB_SUCCESS;
  slave_failure_handler_ = slave_failure_handler;
  return ret;
}

int ObMonitor::set_change_master_handler(IChangeMaster *change_master_handler)
{
  int ret = OB_SUCCESS;
  change_master_handler_ = change_master_handler;
  return ret;
}
