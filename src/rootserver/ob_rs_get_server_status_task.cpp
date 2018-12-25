/*
* Version: $ObPaxos rs_switch$
*
* Authors:
*   huangjianwei
*
* Date:
*  20160726
*
*  --rootserver get server status info from inner table--
*
*/

#include "common/ob_define.h"
#include "rootserver/ob_rs_get_server_status_task.h"
#include "rootserver/ob_root_worker.h"

using namespace oceanbase;
using namespace oceanbase::common;
namespace oceanbase
{
  namespace rootserver
  {
    ObRsGetServerStatusTask::ObRsGetServerStatusTask()
    {
    }

    ObRsGetServerStatusTask::~ObRsGetServerStatusTask()
    {
    }

   int ObRsGetServerStatusTask::init(ObRootWorker *worker, common::ObSchemaService *schema_service)
    {
      int ret = OB_SUCCESS;
      if (NULL == worker || NULL == schema_service)
      {
        TBSYS_LOG(WARN, "invalid argument. worker = %p, schema_service = %p", worker, schema_service);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        worker_ = worker;
        schema_service_ =schema_service;
        //add pangtianze [Paxos rs_switch] 20170210:b
        last_check_server_time_ = tbsys::CTimeUtil::getTime();
        //add:e
      }
      return ret;
    }
    void ObRsGetServerStatusTask::runTimerTask(void)
    {
      if (NULL != worker_)
      {        
        //del pangtianze [Paxos] 20170630:b
        //if (ObRootElectionNode::OB_FOLLOWER ==  worker_->get_root_server().get_rs_node_role())
        //del:e
        {
            worker_->submit_get_server_status_task();
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "invalid argument, worker = %p", worker_);
      }
    }

    int64_t ObRsGetServerStatusTask::get_server_status_count() const
    {
      int64_t ret = 0;
      tbsys::CThreadGuard guard(&svr_status_mutex_);
      ret = server_status_array_.count();
      return ret;
    }

    //add huangjianwei [Paxos rs_switch] 20160729:b
    int ObRsGetServerStatusTask::get_server_status()
    {
      int ret = OB_SUCCESS;
      ServerStatus server_status;
      //add pangtianze [Paxos rs_switch] 20170424:b
      tmp_server_status_array_.clear();
      if (OB_SUCCESS != (ret = get_ms_list()))
      {
          TBSYS_LOG(WARN, "get merge server list failed, ret=%d", ret);
      }
      else if(OB_SUCCESS != (ret = get_cs_list()))
      {
          TBSYS_LOG(WARN, "get chunk server list failed, ret=%d", ret);
      }
      else
      {
          if (!array_compare())
          {
            //add pangtianze [Paxos rs_switch] 20170424:b
            if (OB_SUCCESS == ret)
            {
               tbsys::CThreadGuard guard(&svr_status_mutex_);
               server_status_array_ = tmp_server_status_array_;
            }
            //add:e
            TBSYS_LOG(INFO, "server status array changed");
            ret = fill_server_manager();
            if (OB_SUCCESS == ret)
            {
               TBSYS_LOG(INFO, "update server manager succ");
            }
            for (int32_t i = 0; i < server_status_array_.count(); i++)
            {
              server_status = server_status_array_.at(i);
              TBSYS_LOG(INFO,"server_type=%d, server_addr=%s, ms_sql_port=%d, svr_role =%d",server_status_array_.at(i).svr_type_,
                        to_cstring(server_status_array_.at(i).addr_), server_status.ms_sql_port_, server_status.svr_role_);
            }
          }
      }
      //add

      return ret;
    }
    //add:e
    //add pangtianze [Paxos rs_switch] 20170209:b
    int ObRsGetServerStatusTask::check_server_alive(const int64_t &lease_duration_time)
    {
        int ret = OB_SUCCESS;
        int64_t now = tbsys::CTimeUtil::getTime();

        if (NULL == worker_)
        {
            ret = OB_ERROR;
            TBSYS_LOG(ERROR, "worker_ is null");
        }
        else if (now - last_check_server_time_ <= lease_duration_time)
        {
            //check interval is 'lease_duration_time'
        }
        else if (lease_duration_time < 0)
        {
            ret = OB_INVALID_VALUE;
            TBSYS_LOG(WARN, "cs_lease_duraion_time is invalid, value=%ld", lease_duration_time);
        }        
        else
        {

           tbsys::CThreadGuard guard(&svr_status_mutex_);
           int64_t array_count = server_status_array_.count();
           last_check_server_time_ = now;
           for (int32_t i = 0; i < array_count; i++)
           {
              ServerStatus status =  server_status_array_.at(i);
              ObServerStatus *it =
                      worker_->get_root_server().get_server_manager().find_by_ip(status.addr_);

              if (it != worker_->get_root_server().get_server_manager().end()
                       && it->status_ != ObServerStatus::STATUS_DEAD && it->ms_status_ != ObServerStatus::STATUS_DEAD)
              {
                  //do notiong
              }
              else if (it != worker_->get_root_server().get_server_manager().end())
              {
                  //let ms or cs offline
                  if (OB_CHUNKSERVER == status.svr_type_)
                  {
                    if (it->status_ == ObServerStatus::STATUS_DEAD)
                    {
                       TBSYS_LOG(INFO, "chunkserver %s maybe down, remove it from inner table", status.addr_.to_cstring());
                       worker_->get_root_server().commit_task(SERVER_OFFLINE, OB_CHUNKSERVER, status.addr_, 0, "hb server version null");
                    }
                  }
                  else if (OB_MERGESERVER == status.svr_type_)
                  {
                      if (it->ms_status_ == ObServerStatus::STATUS_DEAD)
                      {
                          TBSYS_LOG(INFO, "mergeserver %s maybe down, remove it from inner table", status.addr_.to_cstring());
                          worker_->get_root_server().commit_task(SERVER_OFFLINE, OB_MERGESERVER, status.addr_, status.ms_sql_port_, "hb server version null");
                      }
                  }
                  else
                  {
                    TBSYS_LOG(WARN, "unknow server type, server=%s, type=%d", status.addr_.to_cstring(), status.svr_type_);
                    ret = OB_ERROR;
                  }
              }
              else if (it == worker_->get_root_server().get_server_manager().end())
              {
                //server not in server manager list,  let ms or cs offline
                if (OB_CHUNKSERVER == status.svr_type_)
                {
                  TBSYS_LOG(INFO, "chunkserver %s maybe down, remove it from inner table", status.addr_.to_cstring());
                  worker_->get_root_server().commit_task(SERVER_OFFLINE, OB_CHUNKSERVER, status.addr_, 0, "hb server version null");
                }
                else if (OB_MERGESERVER == status.svr_type_)
                {
                  TBSYS_LOG(INFO, "mergeserver %s maybe down, remove it from inner table", status.addr_.to_cstring());
                  worker_->get_root_server().commit_task(SERVER_OFFLINE, OB_MERGESERVER, status.addr_, status.ms_sql_port_, "hb server version null");
                }
                else
                {
                  TBSYS_LOG(WARN, "unknow server type, server=%s, type=%d", status.addr_.to_cstring(), status.svr_type_);
                  ret = OB_ERROR;
                }
              }
            }
        }
        return ret;
    }
    int32_t ObRsGetServerStatusTask::get_status_array_idx(common::ObServer &server, common::ObRole svr_type)
    {
        int32_t idx = OB_INVALID_INDEX;
        int64_t array_count = server_status_array_.count();
        for (int32_t i = 0; i < array_count; i++)
        {
            if (server_status_array_.at(i).addr_ == server
                    && server_status_array_.at(i).svr_type_ == svr_type)
            {
                idx = i;
            }
        }
        return idx;
    }
    //add pangtianze [Paxos rs_switch] 20170628:b
    bool ObRsGetServerStatusTask::array_compare()
    {
      //same return true
      bool ret = true;
      if (server_status_array_.count() != tmp_server_status_array_.count())
      {
        ret = false;
      }
      else
      {
        for (unsigned i = 0; i < server_status_array_.count(); i++)
        {
           ServerStatus status = server_status_array_.at(i);
           ServerStatus tmp_status = tmp_server_status_array_.at(i);
           if (!(status == tmp_status))
           {
               ret = false;
               break;
           }
        }
      }
      return ret;
    }
    int ObRsGetServerStatusTask::fill_server_manager()
    {
      int ret = OB_SUCCESS;
      if (NULL == worker_)
      {
         ret = OB_ERROR;
         TBSYS_LOG(ERROR, "worker_ is null");
      }
      else
      {
        ServerStatus status;
        worker_->get_root_server().get_server_manager().reset_server_info();
        for (unsigned i = 0; i < server_status_array_.count(); i++)
        {
          status = server_status_array_.at(i);
          if(status.svr_role_ == OB_CHUNKSERVER)
          {
             //fill cs
            worker_->get_root_server().get_server_manager().receive_hb(status.addr_, 0, false, false, 0, true);
          }
          else if (status.svr_role_ == OB_MERGESERVER)
          {
            //fill ms
            worker_->get_root_server().get_server_manager().receive_hb(status.addr_, 0, true, false, status.ms_sql_port_, true);
          }
        }
      }
      return ret;
    }
    //add:e
    //add pangtianze [Paxos rs_switch] 20170705:b
    int ObRsGetServerStatusTask::get_ms_list()
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      ObDataBuffer data_buff(buff_.ptr(), buff_.capacity());
      ObResultCode res;

      int32_t cluster_id = 0;

      if (OB_SUCCESS != (ret = serialization::encode_vi32(data_buff.get_data(), data_buff.get_capacity(),
                                                              data_buff.get_position(), cluster_id)))
      {
        TBSYS_LOG(WARN, "failed to encode cluster_id, ret=%d", ret);
      }

      if (OB_SUCCESS == ret)
      {
        //add pangtianze [Paxos] 02170717:b
        if (NULL == worker_)
        {
            ret = OB_ERROR;
            TBSYS_LOG(ERROR, "worker_ is null");
        }
        //add:e
        else if (OB_SUCCESS != (ret = worker_->get_client_manager()->send_request(worker_->get_rs_master(), OB_GET_MS_LIST, MY_VERSION, DEFAULT_TIMEOUT, data_buff)))
        {
          TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        data_buff.get_position() = 0;
        ret = res.deserialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "ObResultCode deserialize error, ret=%d", ret);
        }
      }

      int32_t ms_num = 0;
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi32(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position(), &ms_num);
        if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "decode ms num fail:ret[%d]", ret);
        }
        else
        {
          TBSYS_LOG(DEBUG, "ms server number[%d]", ms_num);
        }
      }

      ObServer ms;
      int64_t reserved = 0;
      //put ms list into server list
      ServerStatus server_status;
      for(int32_t i = 0;i<ms_num && OB_SUCCESS == ret;i++)
      {
        if (OB_SUCCESS != (ret = ms.deserialize(data_buff.get_data(),
                                                data_buff.get_capacity(),
                                                data_buff.get_position())))
        {
          TBSYS_LOG(WARN, "deserialize merge server fail, ret: [%d]", ret);
        }
        else if (OB_SUCCESS !=
                 (ret = serialization::decode_vi64(data_buff.get_data(),
                                                   data_buff.get_capacity(),
                                                   data_buff.get_position(),
                                                   &reserved)))
        {
          TBSYS_LOG(WARN, "deserializ merge server"
                    " reserver int64 fail, ret: [%d]", ret);
        }
        else
        {
          server_status.svr_type_ = OB_MERGESERVER;
          server_status.addr_ = ms;
          server_status.ms_sql_port_ = (int32_t)reserved;
          if (OB_SUCCESS != (ret = tmp_server_status_array_.push_back(server_status)))
          {
            TBSYS_LOG(WARN, "failed to push back ms server status, err=%d", ret);
            break;
          }
        }
      }
      return ret;
    }

    int ObRsGetServerStatusTask::get_cs_list()
    {
        int ret = OB_SUCCESS;
        static const int MY_VERSION = 1;

        std::vector<common::ObServer> new_cs;

        ObDataBuffer data_buff(buff_.ptr(), buff_.capacity());
        ObResultCode res;

        if (OB_SUCCESS == ret)
        {
          //add pangtianze [Paxos] 02170717:b
          if (NULL == worker_)
          {
              ret = OB_ERROR;
              TBSYS_LOG(ERROR, "worker_ is null");
          }
          //add:e
          else if (OB_SUCCESS != (ret = worker_->get_client_manager()->send_request(worker_->get_rs_master(), OB_GET_CS_LIST, MY_VERSION, DEFAULT_TIMEOUT, data_buff)))
          {
            TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
          }
        }

        if (OB_SUCCESS == ret)
        {
          data_buff.get_position() = 0;
          ret = res.deserialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "ObResultCode deserialize error, ret=%d", ret);
          }
        }

        int32_t cs_num = 0;
        if (OB_SUCCESS == ret)
        {
          ret = serialization::decode_vi32(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position(), &cs_num);
          if(OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "decode cs num fail:ret[%d]", ret);
          }
          else
          {
            TBSYS_LOG(DEBUG, "cs server number[%d]", cs_num);
          }
        }

        ObServer cs;
        int64_t reserved = 0;
        for(int32_t i = 0;i<cs_num && OB_SUCCESS == ret;i++)
        {
          if (OB_SUCCESS != (ret = cs.deserialize(data_buff.get_data(),
                                                  data_buff.get_capacity(),
                                                  data_buff.get_position())))
          {
            TBSYS_LOG(WARN, "deserialize chunk server fail, ret: [%d]", ret);
          }
          else if (OB_SUCCESS !=
                   (ret = serialization::decode_vi64(data_buff.get_data(),
                                                     data_buff.get_capacity(),
                                                     data_buff.get_position(),
                                                     &reserved)))
          {
            TBSYS_LOG(WARN, "deserializ chunk server"
                      " reserver int64 fail, ret: [%d]", ret);
          }
          else
          {
            new_cs.push_back(cs);
          }
        }

        //put cs list into server list
        ServerStatus server_status;
        for (unsigned i = 0; i < new_cs.size(); i++)
        {
            server_status.svr_type_ = OB_CHUNKSERVER;
            server_status.addr_ = new_cs[i];
            if (OB_SUCCESS != (ret = tmp_server_status_array_.push_back(server_status)))
            {
              TBSYS_LOG(WARN, "failed to push back cs server status, err=%d", ret);
              break;
            }
        }
        return ret;
    }
    //add:e
  }
}
