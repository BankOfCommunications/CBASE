#include "ob_ms_sub_scan_request.h"
#include "ob_ms_rpc_event.h"
#include "ob_read_param_modifier.h"

namespace oceanbase
{
  namespace mergeserver
  {
    ObMergerSubScanRequest::ObMergerSubScanRequest()
    {
      inited_ = false;
    }


    ObMergerSubScanRequest::~ObMergerSubScanRequest()
    {
    }


    int ObMergerSubScanRequest::init(ObScanParam *scan_param,
      ObNewRange & query_range,
      const int64_t limit_offset,
      const int64_t limit_count,
      const ObChunkServerItem cs_replicas[],
      const int32_t replica_count,
      const bool scan_full_tablet,
      ObStringBuf *buffer_pool)
    {
      int err = OB_SUCCESS;
      int i = 0;
      if (replica_count > ObTabletLocationList::MAX_REPLICA_COUNT)
      {
        TBSYS_LOG(WARN, "replica count exceeds max value. [replica_count:%d][max_count:%d]",
          replica_count, ObTabletLocationList::MAX_REPLICA_COUNT);
        err = OB_INVALID_ARGUMENT;
      }
      else if (replica_count <= 0)
      {
        TBSYS_LOG(WARN, "replica count exceeds min value. [replica_count:%d][min_count:%d]",
            replica_count, 1);
        err = OB_INVALID_ARGUMENT;
      }

      if (NULL == scan_param || NULL == buffer_pool)
      {
        TBSYS_LOG(WARN, "null pointer error! [scan_param=%p][bufffer_pool=%p]", scan_param, buffer_pool);
        err = OB_INVALID_ARGUMENT;
      }


      if (OB_SUCCESS == err)
      {
        for (i = 0; i < replica_count; i++)
        {
          cs_replicas_[i] = cs_replicas[i];
          cs_replicas_[i].status_ = ObChunkServerItem::UNREQUESTED;
        }
        for (i = 0; i < MAX_BACKUP_TASK_NUM; i++)
        {
          backup_agent_tasks_[i] = OB_INVALID_ID;  /// reset event id record.
        }
        /// no need to deep copy query_range
        query_range_ = query_range;
        scan_param_ = scan_param;
        buffer_pool_ = buffer_pool;

        /// for request time estimation
        scan_full_tablet_ = scan_full_tablet;
        limit_offset_ = limit_offset;
        limit_count_ = limit_count;
        total_replica_count_ = replica_count;
        tried_replica_count_ = 0;
        last_tried_replica_idx_ = -1;
        triggered_backup_task_count_ = 0;
        finished_backup_task_count_ = 0;
        result_ = NULL;
        event_id_pos_ = 0;
        inited_ = true;
      }
      return err;
    }


    void ObMergerSubScanRequest::reset()
    {
      query_range_.reset();
      scan_param_ = NULL;
      buffer_pool_ = NULL;
      result_ = NULL;

      scan_full_tablet_ = false;
      triggered_backup_task_count_ = 0;
      finished_backup_task_count_ = 0;

      limit_offset_ = 0;
      limit_count_ = 0;

      total_replica_count_ = 0;
      tried_replica_count_ = 0;
      last_tried_replica_idx_ = -1;
      inited_ = false;
      event_id_pos_ = 0;

      session_id_ = ObCommonRpcEvent::INVALID_SESSION_ID;
    }

    int ObMergerSubScanRequest::select_cs(ObChunkServerItem & selected_server)
    {
      int sel_replica = -1;
      int err = OB_SUCCESS;

      sel_replica = ObChunkServerTaskDispatcher::get_instance()->select_cs(reinterpret_cast<ObChunkServerItem*>(cs_replicas_),
        total_replica_count_, last_tried_replica_idx_);

      if (sel_replica < 0 || sel_replica >= total_replica_count_)
      {
        TBSYS_LOG(WARN, "no valid chunkserver selected. [sel_replica=%d]", sel_replica);
        err = OB_ERROR;
      }
      else
      {
        last_tried_replica_idx_ = sel_replica;
        selected_server = cs_replicas_[sel_replica];
        tried_replica_count_++;
        // TBSYS_LOG(DEBUG, "[tried_replica_count_:%d,total_replica_count_:%d]", tried_replica_count_, total_replica_count_);
      }
      return err;
    }


    /// create a rpc event for this sub request
    int ObMergerSubScanRequest::add_event(ObMergerRpcEvent *rpc_event,
      ObMergerRequest *client_request, ObChunkServerItem & selected_server)
    {
      int err = OB_SUCCESS;
      //int client_request_id = 0;

      if (false == inited_)
      {
        TBSYS_LOG(WARN, "ObMergerSubScanRequest class not inited. call init() first");
        err = OB_NOT_INIT;
      }
      else if (NULL == client_request || NULL == rpc_event)
      {
        TBSYS_LOG(WARN, "NULL pointer error. client_request=%p, rpc_event=%p", client_request, rpc_event);
        err = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == err)
      {
        err = select_cs(selected_server);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to select chunkserver [request_id:%lu]",
            client_request->get_request_id());
        }
        else
        {
          if (OB_SUCCESS != (err = rpc_event->init(client_request->get_request_id(), client_request)))
          {
            TBSYS_LOG(WARN, "fail to init rpc_event. [request_id:%lu]", client_request->get_request_id());
          }
          else
          {
            add_event_id(rpc_event->get_event_id());
            triggered_backup_task_count_++;
            /// TODO: more init?
          }
        }

        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "release allocated ObMergerRpcEvent object. [request_id:%lu]", client_request->get_request_id());
        }
      }
      if (OB_SUCCESS == err)
      {
        session_id_ = rpc_event->get_session_id();
        session_server_ = selected_server.addr_;
        session_rpc_event_id_ = rpc_event->get_event_id();
      }
      return err;
    }



    /// check if agent_event belong to this, and if agent_event is the first finished backup task
    /// if agent_event belong to this, set belong_to_this to true, and increment finished_backup_task_count_
    /// if agent_event belong to this, and it is not the first finished backup task, agent_event
    /// will be directly destroyed
    int ObMergerSubScanRequest::agent_event_finish(ObMergerRpcEvent * agent_event,
      bool &belong_to_this,
      bool &is_first)
    {
      int err = OB_SUCCESS;
      bool exist = false;
      is_first = false;

      if (false == inited_)
      {
        TBSYS_LOG(WARN, "ObMergerSubScanRequest class not inited. call init() first");
        err = OB_ERROR;
      }
      else if (NULL == agent_event)
      {
        TBSYS_LOG(WARN, "NULL pointer error. agent_event=NULL");
        err = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = check_event_id(agent_event->get_event_id(), exist)))
        {
          TBSYS_LOG(WARN, "check_event_id failed. [err=%d]", err);
        }
        else
        {
          if (true == exist)
          {
            belong_to_this = true;
            finished_backup_task_count_++;
            if (NULL == result_)
            {
              is_first = true;
            }
            if ((NULL == result_) && (OB_SUCCESS == agent_event->get_result_code()))  /// first finished backup task
            {
              // TBSYS_LOG(DEBUG, "new agent event arrived. add to result set. scanner=%p", &(agent_event->get_result()));
              result_ = agent_event;
            }
          }
          else
          {
            belong_to_this = false;
          }
        }
      }
      if ((NULL != agent_event) && (session_rpc_event_id_ == agent_event->get_event_id()))
      {
        /// session is end when server end this session or something wrong happened
        if ((ObCommonRpcEvent::INVALID_SESSION_ID == agent_event->get_session_id())
          || (OB_SUCCESS != agent_event->get_result_code()))
        {
          session_id_ = ObCommonRpcEvent::INVALID_SESSION_ID;
        }
      }
      return err;
    }


    int ObMergerSubScanRequest::add_event_id(uint64_t event_id)
    {
      int err = OB_SUCCESS;

      if (OB_INVALID_ID == event_id)
      {
        TBSYS_LOG(WARN, "invalid event_id [event_id:%ld]", event_id);
        err = OB_INVALID_ARGUMENT;
      }
      else if (0 <= event_id_pos_ && event_id_pos_ < MAX_BACKUP_TASK_NUM)
      {
        backup_agent_tasks_[event_id_pos_] = event_id;
        event_id_pos_++;
      }
      else
      {
        TBSYS_LOG(ERROR, "event_id[] size overflow. [event_id_pos_:%d][max size:%d][event_id:%ld]",
          event_id_pos_, MAX_BACKUP_TASK_NUM, event_id);
        err = OB_ERR_UNEXPECTED;
      }
      return err;
    }


    int ObMergerSubScanRequest::check_event_id(uint64_t event_id, bool &exist)
    {
      int err = OB_SUCCESS;
      int i = 0;

      if (event_id_pos_ >= MAX_BACKUP_TASK_NUM || event_id_pos_ < 0)
      {
        TBSYS_LOG(WARN, "event_id[] size overflow. [event_id_pos_:%d][max size:%d][event_id:%ld]",
          event_id_pos_, MAX_BACKUP_TASK_NUM, event_id);
        err = OB_SIZE_OVERFLOW;
      }
      else if (OB_INVALID_ID == event_id)
      {
        TBSYS_LOG(WARN, "invalid event id. [event_id=%lu]", event_id);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        exist = false;
        for (i = 0; i < event_id_pos_; i++)
        {
          if (backup_agent_tasks_[i] == event_id)
          {
            exist = true;
            break;
          }
        }
      }
      return err;
    }
  };
};
