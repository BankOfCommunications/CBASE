#ifndef _OB_MERGER_SQL_SUB_SCAN_REQUEST_H_
#define _OB_MERGER_SQL_SUB_SCAN_REQUEST_H_

#include "common/ob_thread_objpool.h"
#include "common/ob_malloc.h"
#include "common/ob_range2.h"
#include "common/ob_string_buf.h"
#include "common/ob_server.h"
#include "common/ob_new_scanner.h"
#include "common/ob_get_param.h"
#include "sql/ob_sql_scan_param.h"
#include "ob_ms_sql_rpc_event.h"
#include "ob_ms_sql_request.h"
#include "common/ob_chunk_server_item.h"
#include "ob_chunk_server_task_dispatcher.h"
#include <algorithm>
#include "ob_ms_request_result.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
  namespace mergeserver
  {

    class ObMsSqlRpcEvent;

    class ObMsSqlSubScanRequest
    {
    public:

      ObMsSqlSubScanRequest();

      ~ObMsSqlSubScanRequest();

      int init(ObSqlScanParam *scan_param,
        ObNewRange & query_range,
        const ObChunkServerItem cs_replicas[],
        const int32_t replica_count,
        ObStringBuf *buffer_pool);

      void reset();
      inline void reset_session();

      int select_cs(ObChunkServerItem & selected_server);


      /// add a rpc event to this sub request
      int add_event(ObMsSqlRpcEvent *rpc_event, ObMsSqlRequest *client_request,
        ObChunkServerItem & selected_server);

      /// check if agent_event belong to this, and if agent_event is the first finished backup task
      /// if agent_event belong to this, set belong_to_this to true, and increment finished_backup_task_count_
      int agent_event_finish(ObMsSqlRpcEvent * agent_event, bool &belong_to_this);

      /// called to check if this tablet has been scanned over or tablet has been divided
      /// if tablet has scanned over and tablet has not been divided, return OB_ITER_END
      /// int get_next_scan_param(ObSqlScanParam & next_param);
      /// int get_next_scan_range(ObNewRange & next_range);

      ObNewRange &get_sub_range();

      /// get the result of current sub request
      ObMsRequestResult * get_result();

      /// get statistics params
      inline int32_t triggered_backup_task_count() const;
      inline int32_t finished_backup_task_count() const;
      inline int32_t total_replica_count() const;
      inline int32_t tried_replica_count() const;
      inline int32_t total_tried_replica_count() const;
      inline int32_t last_tried_replica_idx() const;
      inline bool has_tablet_migrate() const;
      inline void set_tablet_migrate();

      /// result interface
      inline ObSqlScanParam * get_scan_param() const;
      inline const int64_t get_limit_offset() const;

      inline int reset_cs_replicas(const int32_t replica_cnt, const ObChunkServerItem *cs_vec);

      inline ObServer get_session_server()const;
      inline int64_t get_session_id()const;

      inline void get_cs_replicas(ObChunkServerItem * replica_buf, int64_t & replica_count)
      {
        if ((NULL != replica_buf) && (replica_count > 0))
        {
          memcpy(replica_buf, cs_replicas_, sizeof(ObChunkServerItem)*std::min<int64_t>(replica_count, total_replica_count_));
        }
        else
        {
          replica_count = 0;
        }
      }

    private:
      int add_event_id(uint64_t event_id);
      int check_event_id(uint64_t event_id, bool &exist);

    private:
      ///  does this sub request scan a full tablet
      uint64_t          event_id_;

      ObChunkServerItem     cs_replicas_[ObTabletLocationList::MAX_REPLICA_COUNT];
      int32_t           total_replica_count_;
      int32_t           tried_replica_count_;
      int32_t           total_tried_replica_count_;
      int32_t           tablet_migrate_count_;
      int32_t           last_tried_replica_idx_;
      /// does init() called
      bool              inited_;

      /// ThreadAllocator<ObMsSqlRpcEvent, MutilObjAllocator<ObMsSqlRpcEvent, 65536> > allocator_;
      ObNewRange           query_range_;
      int64_t           query_version_; //add zhaoqiong [fixbug:tablet version info lost when query from ms] 20170228
      ObMsRequestResult   request_result_;
      sql::ObSqlScanParam       *scan_param_;
      ObStringBuf       *buffer_pool_;

      /// session related, there should be only on session associated with a sub req, because backups tasks
      /// can't use session stream, backup tasks must use totally new scan request
      int64_t           session_id_;
      ObServer          session_server_;
      uint64_t          session_rpc_event_id_;
    };

    inline int ObMsSqlSubScanRequest::reset_cs_replicas(const int32_t replica_cnt, const ObChunkServerItem *cs_vec)
    {
      int err = oceanbase::common::OB_SUCCESS;
      if ((replica_cnt <= 0) || (NULL == cs_vec))
      {
        err = oceanbase::common::OB_INVALID_ARGUMENT;
      }
      if (oceanbase::common::OB_SUCCESS == err)
      {
        total_replica_count_ = 0;
        tried_replica_count_ = 0;
        last_tried_replica_idx_ = -1;
        for (int32_t i = 0; (i < replica_cnt) && (i < ObTabletLocationList::MAX_REPLICA_COUNT); i++)
        {
          cs_replicas_[i] = cs_vec[i];
          cs_replicas_[i].status_ = oceanbase::common::ObChunkServerItem::UNREQUESTED;
          total_replica_count_ ++;
        }
      }
      return err;
    }

    inline ObNewRange &ObMsSqlSubScanRequest::get_sub_range()
    {
      return query_range_;
    }


    inline ObSqlScanParam *ObMsSqlSubScanRequest::get_scan_param() const
    {
      int err = OB_SUCCESS;

      if (NULL != scan_param_)
      {

        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = scan_param_->set_range(query_range_))))
        {
          TBSYS_LOG(WARN, "fail to set scan_param [err=%d]", err);
        }
        //add zhaoqiong [fixbug:tablet version info lost when query from ms] 20170228:b
        if ((OB_SUCCESS == err) && (query_version_ > 0))
        {
            scan_param_->set_version(query_version_);
        }
        else
        {
          TBSYS_LOG(WARN, "fail to set scan_param query version [err=%d]", err);
        }
        //add:e
/*
        if ((OB_SUCCESS == err) &&
          (OB_SUCCESS != (err = scan_param_->set_limit(limit_count_, limit_offset_))))
        {
          TBSYS_LOG(WARN, "fail to set scan limit [limit_offset_=%ld][err=%d]", limit_offset_, err);
        }
*/
      }
      else
      {
        TBSYS_LOG(WARN, "null pointer error. [scan_param_=%p]", scan_param_);
      }
      return scan_param_;
    }

    inline int32_t ObMsSqlSubScanRequest::total_replica_count() const
    {
      return total_replica_count_;
    }


    inline int32_t ObMsSqlSubScanRequest::tried_replica_count() const
    {
      return tried_replica_count_;
    }

    inline int32_t ObMsSqlSubScanRequest::total_tried_replica_count() const
    {
      return total_tried_replica_count_;
    }

    inline bool ObMsSqlSubScanRequest::has_tablet_migrate() const
    {
      return (tablet_migrate_count_ > 0);
    }

    inline void ObMsSqlSubScanRequest::set_tablet_migrate()
    {
      tablet_migrate_count_++;
    }

    inline int32_t ObMsSqlSubScanRequest::last_tried_replica_idx() const
    {
      return last_tried_replica_idx_;
    }

    /// get the result of current sub request
    inline ObMsRequestResult * ObMsSqlSubScanRequest::get_result()
    {
      return &request_result_;
    }
    inline ObServer ObMsSqlSubScanRequest::get_session_server()const
    {
      return session_server_;
    }
    inline int64_t ObMsSqlSubScanRequest::get_session_id()const
    {
      return session_id_;
    }
    inline void ObMsSqlSubScanRequest::reset_session()
    {
      session_id_ = ObCommonSqlRpcEvent::INVALID_SESSION_ID;
    }
  } /* EOF mergeserver ns */
} /* EOF oceanbase ns */

#endif
