#include "ob_ms_sql_sub_scan_request.h"
#include "ob_ms_sql_rpc_event.h"
//#include "ob_ms_request_event.h"
//#include "ob_cs_task_dispatcher.h"
#include "ob_read_param_modifier.h"

namespace oceanbase
{
  namespace mergeserver
  {
    ObMsSqlSubScanRequest::ObMsSqlSubScanRequest()
    {
      inited_ = false;
      reset();
    }


    ObMsSqlSubScanRequest::~ObMsSqlSubScanRequest()
    {
    }


    int ObMsSqlSubScanRequest::init(sql::ObSqlScanParam *scan_param,
      ObNewRange & query_range,
      const ObChunkServerItem cs_replicas[],
      const int32_t replica_count,
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
        /// no need to deep copy query_range
        query_range_ = query_range;
        query_version_ = 0; //add zhaoqiong [fixbug:tablet version info lost when query from ms] 20170228
        scan_param_ = scan_param;
        buffer_pool_ = buffer_pool;

        /// for request time estimation
        total_replica_count_ = replica_count;
        tried_replica_count_ = 0;
        total_tried_replica_count_ = 0;
        tablet_migrate_count_ = 0;
        last_tried_replica_idx_ = -1;
        inited_ = true;
      }
      return err;
    }


    void ObMsSqlSubScanRequest::reset()
    {
      query_range_.reset();
      query_version_ = 0; //add zhaoqiong [fixbug:tablet version info lost when query from ms] 20170228
      scan_param_ = NULL;
      buffer_pool_ = NULL;

      total_replica_count_ = 0;
      tried_replica_count_ = 0;
      total_tried_replica_count_ = 0;
      tablet_migrate_count_ = 0;
      last_tried_replica_idx_ = -1;
      inited_ = false;

      session_id_ = ObCommonSqlRpcEvent::INVALID_SESSION_ID;
    }

    int ObMsSqlSubScanRequest::select_cs(
      ObChunkServerItem & selected_server)
    {
      int sel_replica = -1;
      int err = OB_SUCCESS;

      sel_replica = ObChunkServerTaskDispatcher::get_instance()->select_cs(reinterpret_cast<ObChunkServerItem*>(cs_replicas_),
        total_replica_count_, last_tried_replica_idx_);

      if (sel_replica < 0 || sel_replica >= total_replica_count_)
      {
        TBSYS_LOG(WARN, "no valid chunkserver selected. [sel_replica=%d, total_tried_replica_count_=%d, last_tried_replica_idx_[=%d]", sel_replica, 
          total_replica_count_, last_tried_replica_idx_);
        err = OB_ERROR;
      }
      else
      {
        last_tried_replica_idx_ = sel_replica;
        selected_server = cs_replicas_[sel_replica];
        query_version_ = selected_server.tablet_version_; //add zhaoqiong [fixbug:tablet version info lost when query from ms] 20170228
        tried_replica_count_++;
        total_tried_replica_count_++;
        // TBSYS_LOG(DEBUG, "[tried_replica_count_:%d,total_replica_count_:%d]", tried_replica_count_, total_replica_count_);
      }

      if (false)
      {
        for (int i = 0; i < total_replica_count_; i++)
        {
          TBSYS_LOG(INFO, "cs=%s", to_cstring(cs_replicas_[i]));
        }
        TBSYS_LOG(INFO, "total_rep=%d, tried=%d, last=%d, cur=%s",
            total_replica_count_, tried_replica_count_, last_tried_replica_idx_, to_cstring(selected_server));
      }

      return err;
    }


    /// create a rpc event for this sub request
    int ObMsSqlSubScanRequest::add_event(ObMsSqlRpcEvent *rpc_event,
      ObMsSqlRequest *client_request, ObChunkServerItem & selected_server)
    {
      int err = OB_SUCCESS;
      //int client_request_id = 0;

      if (false == inited_)
      {
        TBSYS_LOG(WARN, "ObMsSqlSubScanRequest class not inited. call init() first");
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
            event_id_ = rpc_event->get_event_id();
            /// TODO: more init?
          }
        }

        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "release allocated ObMsSqlRpcEvent object. [request_id:%lu]", client_request->get_request_id());
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
    int ObMsSqlSubScanRequest::agent_event_finish(ObMsSqlRpcEvent * agent_event,
      bool &belong_to_this)
    {
      int err = OB_SUCCESS;

      if (false == inited_)
      {
        TBSYS_LOG(WARN, "ObMsSqlSubScanRequest class not inited. call init() first");
        err = OB_ERROR;
      }
      else if (NULL == agent_event)
      {
        TBSYS_LOG(WARN, "NULL pointer error. agent_event=NULL");
        err = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == err)
      {
        if (event_id_ == agent_event->get_event_id())
        {
          belong_to_this = true;
          if (OB_SUCCESS != (err = request_result_.add_scanner(&(agent_event->get_result()))))
          {
            TBSYS_LOG(WARN, "fail to add scanner to result:err[%d]", err);
          }
        }
        else
        {
          belong_to_this = false;
        }
      }
      if ((NULL != agent_event) && (session_rpc_event_id_ == agent_event->get_event_id()))
      {
        /// session is end when server end this session or something wrong happened
        if ((ObCommonSqlRpcEvent::INVALID_SESSION_ID == agent_event->get_session_id())
          || (OB_SUCCESS != agent_event->get_result_code()))
        {
          session_id_ = ObCommonSqlRpcEvent::INVALID_SESSION_ID;
        }
      }
      return err;
    }
  };
};
