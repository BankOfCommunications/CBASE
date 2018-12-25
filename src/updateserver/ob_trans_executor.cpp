////===================================================================
 //
 // ob_trans_executor.cpp updateserver / Oceanbase
 //
 // Copyright (C) 2010, 2012, 2013 Taobao.com, Inc.
 //
 // Created on 2012-08-30 by Yubai (yubai.lk@taobao.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================

#include "common/ob_common_param.h"
#include "common/ob_profile_log.h"
#include "common/ob_profile_type.h"
#include "common/ob_trace_id.h"
#include "common/base_main.h"
#include "sql/ob_lock_filter.h"
#include "sql/ob_inc_scan.h"
#include "sql/ob_ups_modify.h"
#include "ob_ups_lock_filter.h"
#include "ob_ups_inc_scan.h"
#include "ob_memtable_modify.h"
#include "ob_update_server_main.h"
#include "ob_trans_executor.h"
#include "ob_session_guard.h"
#include "stress.h"
//add hongchen [LOCK_WAIT_TIMEOUT] 20161209:b
#include "tbsys.h"
//add hongchen [LOCK_WAIT_TIMEOUT] 20161209:e

#define UPS ObUpdateServerMain::get_instance()->get_update_server()

namespace oceanbase
{
  namespace updateserver
  {
    static int64_t get_slow_query_time_limit(int priority)
    {
      return PriorityPacketQueueThread::LOW_PRIV == priority? (int64_t)UPS.get_param().low_prio_slow_query_time_limit: (int64_t)UPS.get_param().normal_prio_slow_query_time_limit;
    }
#define BEGIN_SAMPLE_SLOW_QUERY(session_ctx) if (REACH_TIME_INTERVAL(100 * 1000)) { session_ctx->get_tlog_buffer().force_log_ = true; }
#define END_SAMPLE_SLOW_QUERY(session_ctx, pkt) if (session_ctx->is_slow_query(get_slow_query_time_limit(pkt.get_packet_priority()))) { session_ctx->get_tlog_buffer().force_print_ = true; }
    class FakeWriteGuard
    {
      public:
        FakeWriteGuard(SessionMgr& session_mgr):
          session_mgr_(session_mgr), session_descriptor_(INVALID_SESSION_DESCRIPTOR), fake_write_granted_(false)
        {
          int ret = OB_SUCCESS;
          RWSessionCtx *session_ctx = NULL;
          if (OB_SUCCESS != (ret = session_mgr.begin_session(ST_READ_WRITE, 0, -1, -1, session_descriptor_)))
          {
            TBSYS_LOG(WARN, "begin fake write session fail ret=%d", ret);
          }
          else if (NULL == (session_ctx = session_mgr.fetch_ctx<RWSessionCtx>(session_descriptor_)))
          {
            TBSYS_LOG(WARN, "fetch ctx fail session_descriptor=%u", session_descriptor_);
            ret = OB_TRANS_ROLLBACKED;
          }
          else
          {
            session_ctx->set_frozen();
            session_mgr.revert_ctx(session_descriptor_);
            fake_write_granted_ = true;
          }
        }
        ~FakeWriteGuard(){
          if (INVALID_SESSION_DESCRIPTOR == session_descriptor_)
          {} // do nothing
          else
          {
            session_mgr_.end_session(session_descriptor_, /*rollback*/ true);
          }
        }
        bool is_granted() { return fake_write_granted_; }
        //add chujiajia [Paxos ups_replication] 20160113:b
        void set_fake_write_granted(bool fake_write_granted)
        {
          fake_write_granted_ = fake_write_granted;
        }
        //add:e
      private:
        SessionMgr& session_mgr_;
        uint32_t session_descriptor_;
        bool fake_write_granted_;
    };
    TransExecutor::TransExecutor(ObUtilInterface &ui) : TransHandlePool(),
                                                        TransCommitThread(),
                                                        ui_(ui),
                                                        allocator_(),
                                                        session_ctx_factory_(),
                                                        session_mgr_(),
                                                        lock_mgr_(),
                                                        uncommited_session_list_(),
                                                        ups_result_buffer_(ups_result_memory_, OB_MAX_PACKET_LENGTH)
    {
      allocator_.set_mod_id(ObModIds::OB_UPS_TRANS_EXECUTOR_TASK);
      memset(ups_result_memory_, 0, OB_MAX_PACKET_LENGTH);

      memset(packet_handler_, 0, sizeof(packet_handler_));
      memset(trans_handler_, 0, sizeof(trans_handler_));
      memset(commit_handler_, 0, sizeof(commit_handler_));
      for (int64_t i = 0; i < OB_PACKET_NUM; i++)
      {
        packet_handler_[i] = phandle_non_impl;
        trans_handler_[i] = thandle_non_impl;
        commit_handler_[i] = chandle_non_impl;
      }

      packet_handler_[OB_FREEZE_MEM_TABLE] = phandle_freeze_memtable;
      packet_handler_[OB_UPS_MINOR_FREEZE_MEMTABLE] = phandle_freeze_memtable;
      packet_handler_[OB_UPS_MINOR_LOAD_BYPASS] = phandle_freeze_memtable;
      packet_handler_[OB_UPS_MAJOR_LOAD_BYPASS] = phandle_freeze_memtable;
      packet_handler_[OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE] = phandle_freeze_memtable;
      packet_handler_[OB_UPS_ASYNC_AUTO_FREEZE_MEMTABLE] = phandle_freeze_memtable;
      packet_handler_[OB_UPS_CLEAR_ACTIVE_MEMTABLE] = phandle_clear_active_memtable;
      packet_handler_[OB_UPS_ASYNC_CHECK_CUR_VERSION] = phandle_check_cur_version;
      packet_handler_[OB_UPS_ASYNC_CHECK_SSTABLE_CHECKSUM] = phandle_check_sstable_checksum;

      //add zhaoqiong [Truncate Table]:20160127:b
      trans_handler_[OB_TRUNCATE_TABLE] = thandle_write_trans;
      //add:e
      trans_handler_[OB_COMMIT_END] = thandle_commit_end;
      trans_handler_[OB_NEW_SCAN_REQUEST] = thandle_scan_trans;
      trans_handler_[OB_NEW_GET_REQUEST] = thandle_get_trans;
      trans_handler_[OB_SCAN_REQUEST] = thandle_scan_trans;
      trans_handler_[OB_GET_REQUEST] = thandle_get_trans;
      trans_handler_[OB_MS_MUTATE] = thandle_write_trans;
      trans_handler_[OB_WRITE] = thandle_write_trans;
      trans_handler_[OB_PHY_PLAN_EXECUTE] = thandle_write_trans;
      trans_handler_[OB_START_TRANSACTION] = thandle_start_session;
      trans_handler_[OB_UPS_ASYNC_KILL_ZOMBIE] = thandle_kill_zombie;
      trans_handler_[OB_UPS_SHOW_SESSIONS] = thandle_show_sessions;
      trans_handler_[OB_UPS_KILL_SESSION] = thandle_kill_session;
      trans_handler_[OB_END_TRANSACTION] = thandle_end_session;

      commit_handler_[OB_MS_MUTATE] = chandle_write_commit;
      commit_handler_[OB_WRITE] = chandle_write_commit;
      commit_handler_[OB_PHY_PLAN_EXECUTE] = chandle_write_commit;
      commit_handler_[OB_END_TRANSACTION] = chandle_write_commit;
      commit_handler_[OB_SEND_LOG] = chandle_send_log;
      commit_handler_[OB_FAKE_WRITE_FOR_KEEP_ALIVE] = chandle_fake_write_for_keep_alive;
      commit_handler_[OB_SLAVE_REG] = chandle_slave_reg;
      //add zhaoqiong [fixed for Backup]:20150811:b
      commit_handler_[OB_BACKUP_REG] = chandle_backup_reg;
      //add:e

      //add zhaoqiong [Truncate Table]:20160127:b
      commit_handler_[OB_TRUNCATE_TABLE] = chandle_write_commit;
      //add:e
      commit_handler_[OB_SWITCH_SCHEMA] = chandle_switch_schema;
      //add zhaoqiong [Schema Manager] 20150327:b
      commit_handler_[OB_SWITCH_SCHEMA_MUTATOR] = chandle_switch_schema_mutator;
      commit_handler_[OB_SWITCH_TMP_SCHEMA] = chandle_switch_tmp_schema;
      //add:e
      commit_handler_[OB_UPS_FORCE_FETCH_SCHEMA] = chandle_force_fetch_schema;
      commit_handler_[OB_UPS_SWITCH_COMMIT_LOG] = chandle_switch_commit_log;
      commit_handler_[OB_NOP_PKT] = chandle_nop;
    }

    TransExecutor::~TransExecutor()
    {
      destroy();
      session_mgr_.destroy();
      allocator_.destroy();
    }

    int TransExecutor::init(const int64_t trans_thread_num,
                            const int64_t trans_thread_start_cpu,
                            const int64_t trans_thread_end_cpu,
                            const int64_t commit_thread_cpu,
                            const int64_t commit_end_thread_num)
    {
      int ret = OB_SUCCESS;
      bool queue_rebalance = true;
      bool dynamic_rebalance = true;
      //add wangdonghui [ups_replication] 20170323 :b
      message_residence_time_us_ = 2000;
      last_commit_log_time_us_ = 0;
      message_residence_max_us_ =6000;
      /*send_interval_us_ = 0;
      count_ = 0; average_batch_size_[0] = 0;
      best_batch_size_ = 0;
      last_gather_time_ = 0;
      last_refresh_time_ = 0;
      best_send_interval_us_ = 0;
      need_refresh_ = true;
      best_message_residence_time_ = 0;*/
      //add :e
      TransHandlePool::set_cpu_affinity(trans_thread_start_cpu, trans_thread_end_cpu);
      TransCommitThread::set_cpu_affinity(commit_thread_cpu);
      if (OB_SUCCESS != (ret = allocator_.init(ALLOCATOR_TOTAL_LIMIT, ALLOCATOR_HOLD_LIMIT, ALLOCATOR_PAGE_SIZE)))
      {
        TBSYS_LOG(WARN, "init allocator fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = session_mgr_.init(MAX_RO_NUM, MAX_RP_NUM, MAX_RW_NUM, &session_ctx_factory_)))
      {
        TBSYS_LOG(WARN, "init session mgr fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = flush_queue_.init(FLUSH_QUEUE_SIZE)))
      {
        TBSYS_LOG(ERROR, "flush_queue.init(%ld)=>%d", FLUSH_QUEUE_SIZE, ret);
      }
      else if (OB_SUCCESS != (ret = TransCommitThread::init(TASK_QUEUE_LIMIT, FINISH_THREAD_IDLE)))
      {
        TBSYS_LOG(WARN, "init TransCommitThread fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = TransHandlePool::init(trans_thread_num, TASK_QUEUE_LIMIT, queue_rebalance, dynamic_rebalance)))
      {
        TBSYS_LOG(WARN, "init TransHandlePool fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = CommitEndHandlePool::init(commit_end_thread_num, TASK_QUEUE_LIMIT, queue_rebalance, false)))
      {
        TBSYS_LOG(WARN, "init CommitEndHandlePool fail ret=%d", ret);
      }
      else
      {
        fifo_stream_.init("run/updateserver.fifo", 100L * 1024L * 1024L);
        nop_task_.pkt.set_packet_code(OB_NOP_PKT);
        TBSYS_LOG(INFO, "TransExecutor init succ");
      }
      return ret;
    }

    void TransExecutor::destroy()
    {
      StressRunnable::stop_stress();
      TransHandlePool::destroy();
      TransCommitThread::destroy();
      CommitEndHandlePool::destroy();
      fifo_stream_.destroy();
    }

    void TransExecutor::on_commit_push_fail(void* ptr)
    {
      TBSYS_LOG(ERROR, "commit push fail, will kill self, task=%p", ptr);
      kill(getpid(), SIGTERM);
    }

    void TransExecutor::handle_packet(ObPacket &pkt)
    {
      int ret = OB_SUCCESS;
      int pcode = pkt.get_packet_code();
      TBSYS_LOG(DEBUG, "start handle packet pcode=%d", pcode);
      if (0 > pcode
          || OB_PACKET_NUM <= pcode)
      {
        easy_request_t *req = pkt.get_request();
        TBSYS_LOG(ERROR, "invalid packet code=%d src=%s",
                  pcode, NULL == req ? NULL : get_peer_ip(req));
        ret = OB_UNKNOWN_PACKET;
      }
      else if (!handle_in_situ_(pcode))
      {
        int64_t task_size = sizeof(Task) + pkt.get_buffer()->get_capacity();
        Task *task = (Task*)allocator_.alloc(task_size);
        if (NULL == task)
        {
          ret = OB_MEM_OVERFLOW;
        }
        else
        {
          task->reset();
          task->pkt = pkt;
          task->src_addr = get_easy_addr(pkt.get_request());
          char *data_buffer = (char*)task + sizeof(Task);
          memcpy(data_buffer, pkt.get_buffer()->get_data(), pkt.get_buffer()->get_capacity());
          task->pkt.get_buffer()->set_data(data_buffer, pkt.get_buffer()->get_capacity());
          task->pkt.get_buffer()->get_position() = pkt.get_buffer()->get_position();
          TBSYS_LOG(DEBUG, "task_size=%ld data_size=%ld pos=%ld",
                    task_size, pkt.get_buffer()->get_capacity(), pkt.get_buffer()->get_position());
          (task->pkt).set_receive_ts(tbsys::CTimeUtil::getTime());
          ret = push_task_(*task);
        }
      }
      else
      {
        int64_t packet_timewait = (0 == pkt.get_source_timeout()) ?
                                  UPS.get_param().packet_max_wait_time :
                                  pkt.get_source_timeout();
        // 1.等待正在运行的事务提交
        // 2.等待commitlog缓冲区中的日志刷到磁盘
        // 避免事务运行过程中由于冻结而操作了不同的active_memtable
        ret = session_mgr_.wait_write_session_end_and_lock(packet_timewait);
        if (OB_SUCCESS == ret)
        {
          ObSpinLockGuard guard(write_clog_mutex_);
          ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer_.get_buffer();
          if (NULL == my_buffer)
          {
            TBSYS_LOG(WARN, "get thread specific buffer fail");
            ret = OB_MEM_OVERFLOW;
          }
          else
          {
            my_buffer->reset();
            ObDataBuffer thread_buff(my_buffer->current(), my_buffer->remain());
            packet_handler_[pcode](pkt, thread_buff);
          }
          session_mgr_.unlock_write_session();
        }
        else
        {
          TBSYS_LOG(WARN, "wait_write_session_end_and_lock(pkt=%d, timeout=%ld)=>%d", pcode, packet_timewait, ret);
        }
      }
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "handle_pkt fail, ret=%d, pkt=%d", ret, pkt.get_packet_code());
        UPS.response_result(ret, pkt);
      }
    }

    bool TransExecutor::handle_in_situ_(const int pcode)
    {
      bool bret = false;
      if (OB_FREEZE_MEM_TABLE == pcode
          || OB_UPS_MINOR_FREEZE_MEMTABLE == pcode
          || OB_UPS_MINOR_LOAD_BYPASS == pcode
          || OB_UPS_MAJOR_LOAD_BYPASS == pcode
          || OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE == pcode
          || OB_UPS_ASYNC_AUTO_FREEZE_MEMTABLE == pcode
          || OB_UPS_CLEAR_ACTIVE_MEMTABLE == pcode
          || OB_UPS_ASYNC_CHECK_CUR_VERSION == pcode
          || OB_UPS_ASYNC_CHECK_SSTABLE_CHECKSUM == pcode)
      {
        bret = true;
      }
      return bret;
    }

    int TransExecutor::push_task_(Task &task)
    {
      int ret = OB_SUCCESS;
      switch (task.pkt.get_packet_code())
      {
        case OB_NEW_SCAN_REQUEST:
        case OB_NEW_GET_REQUEST:
        case OB_SCAN_REQUEST:
        case OB_GET_REQUEST:
        case OB_MS_MUTATE:
        case OB_WRITE:
        case OB_TRUNCATE_TABLE: //add zhaoqiong [Truncate table] 20160127
        case OB_PHY_PLAN_EXECUTE:
        case OB_START_TRANSACTION:
        case OB_UPS_ASYNC_KILL_ZOMBIE:
        case OB_UPS_SHOW_SESSIONS:
        case OB_UPS_KILL_SESSION:
        case OB_END_TRANSACTION:
          ret = TransHandlePool::push(&task, task.pkt.get_req_sign(), task.pkt.get_packet_priority());
          break;
        case OB_SEND_LOG:
        case OB_FAKE_WRITE_FOR_KEEP_ALIVE:
        case OB_SLAVE_REG:
        //add zhaoqiong [fixed for Backup]:20150811:b
        case OB_BACKUP_REG:
        //add:e
        case OB_SWITCH_SCHEMA:
        //add zhaoqiong [Schema Manager] 20150327:b
        case OB_SWITCH_SCHEMA_MUTATOR:
        case OB_SWITCH_TMP_SCHEMA:
        //add:e
        case OB_UPS_FORCE_FETCH_SCHEMA:
        case OB_UPS_SWITCH_COMMIT_LOG:
          ret = TransCommitThread::push(&task);
          break;
        default:
          TBSYS_LOG(ERROR, "unknown packet code=%d src=%s",
                    task.pkt.get_packet_code(), inet_ntoa_r(task.src_addr));
          ret = OB_UNKNOWN_PACKET;
          break;
      }
      return ret;
    }

    bool TransExecutor::wait_for_commit_(const int pcode)
    {
      bool bret = true;
      if (OB_MS_MUTATE == pcode
          || OB_WRITE == pcode
          || OB_PHY_PLAN_EXECUTE == pcode
          || OB_END_TRANSACTION == pcode
          || OB_NOP_PKT == pcode)
      {
        bret = false;
      }
      return bret;
    }

    bool TransExecutor::is_only_master_can_handle(const int pcode)
    {
      return OB_FAKE_WRITE_FOR_KEEP_ALIVE == pcode
        || OB_SWITCH_SCHEMA == pcode
        //add zhaoqiong [Schema Manager] 20150327:b
        || OB_SWITCH_SCHEMA_MUTATOR == pcode
        || OB_SWITCH_TMP_SCHEMA == pcode
        //add:e
        || OB_UPS_FORCE_FETCH_SCHEMA == pcode
        || OB_UPS_SWITCH_COMMIT_LOG == pcode;
    }

    int TransExecutor::fill_return_rows_(sql::ObPhyOperator &phy_op, ObNewScanner &scanner, sql::ObUpsResult &ups_result)
    {
      int ret = OB_SUCCESS;
      scanner.reuse();
      const ObRow *row = NULL;
      while (OB_SUCCESS == (ret = phy_op.get_next_row(row)))
      {
        if (NULL == row)
        {
          TBSYS_LOG(WARN, "row null pointer, phy_op=%p type=%d", &phy_op, phy_op.get_type());
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        if (OB_SUCCESS != (ret = scanner.add_row(*row)))
        {
          TBSYS_LOG(WARN, "add row to scanner fail, ret=%d %s", ret, to_cstring(*row));
          break;
        }
      }
      if (OB_ITER_END == ret)
      {
        if (OB_SUCCESS != (ret = ups_result.set_scanner(scanner)))
        {
          TBSYS_LOG(WARN, "set scanner to ups_result fail ret=%d", ret);
        }
      }
      return ret;
    }

    void TransExecutor::reset_warning_strings_()
    {
      tbsys::WarningBuffer *warning_buffer = tbsys::get_tsi_warning_buffer();
      if (NULL != warning_buffer)
      {
        warning_buffer->reset();
      }
    }

    void TransExecutor::fill_warning_strings_(sql::ObUpsResult &ups_result)
    {
      tbsys::WarningBuffer *warning_buffer = tbsys::get_tsi_warning_buffer();
      if (NULL != warning_buffer)
      {
        for (uint32_t i = 0; i < warning_buffer->get_total_warning_count(); i++)
        {
          ups_result.add_warning_string(warning_buffer->get_warning(i));
          TBSYS_LOG(DEBUG, "fill warning string idx=%d [%s]", i, warning_buffer->get_warning(i));
        }
        warning_buffer->reset();
      }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    void TransExecutor::handle_trans(void *ptask, void *pdata)
    {
      ob_reset_err_msg();
      int ret = OB_SUCCESS;
      thread_errno() = OB_SUCCESS;
      bool release_task = true;
      Task *task = (Task*)ptask;
      TransParamData *param = (TransParamData*)pdata;
      int64_t packet_timewait = (NULL == task || 0 == task->pkt.get_source_timeout()) ?
                                UPS.get_param().packet_max_wait_time :
                                task->pkt.get_source_timeout();
      int64_t process_timeout = packet_timewait - QUERY_TIMEOUT_RESERVE;
      if (NULL == task)
      {
        TBSYS_LOG(WARN, "null pointer task=%p", task);
      }
      else if (NULL == param)
      {
        TBSYS_LOG(WARN, "null pointer param data pdata=%p src=%s",
                  pdata, inet_ntoa_r(task->src_addr));
        ret = OB_ERR_UNEXPECTED;
      }
      else if (0 > task->pkt.get_packet_code()
              || OB_PACKET_NUM <= task->pkt.get_packet_code())
      {
        TBSYS_LOG(ERROR, "unknown packet code=%d src=%s",
                  task->pkt.get_packet_code(), inet_ntoa_r(task->src_addr));
        ret = OB_UNKNOWN_PACKET;
      }
      else if (OB_COMMIT_END != task->pkt.get_packet_code()
              && (process_timeout < 0 || process_timeout < (tbsys::CTimeUtil::getTime() - task->pkt.get_receive_ts())))
      {
        OB_STAT_INC(UPDATESERVER, UPS_STAT_PACKET_LONG_WAIT_COUNT, 1);
        ret = (OB_SUCCESS == task->last_process_retcode) ? OB_RESPONSE_TIME_OUT : task->last_process_retcode;
        TBSYS_LOG(WARN, "process timeout=%ld not enough cur_time=%ld receive_time=%ld packet_code=%d src=%s",
                  process_timeout, tbsys::CTimeUtil::getTime(), task->pkt.get_receive_ts(), task->pkt.get_packet_code(), inet_ntoa_r(task->src_addr));
      }
      else
      {
        //忽略log自带的前两个字段，trace id和chid，以后续的trace id和chid为准
        PROFILE_LOG(DEBUG, TRACE_ID SOURCE_CHANNEL_ID PCODE WAIT_TIME_US_IN_RW_QUEUE,
                    (task->pkt).get_trace_id(),
                    (task->pkt).get_channel_id(),
                    (task->pkt).get_packet_code(),
                    tbsys::CTimeUtil::getTime() - (task->pkt).get_receive_ts());
        release_task = trans_handler_[task->pkt.get_packet_code()](*this, *task, *param);
      }
      if (NULL != task)
      {
        if ((OB_SUCCESS != ret && !IS_SQL_ERR(ret) && OB_BEGIN_TRANS_LOCKED != ret)
            || (OB_SUCCESS != thread_errno() && !IS_SQL_ERR(thread_errno()) && OB_BEGIN_TRANS_LOCKED != thread_errno()))
        {
          if (REACH_TIME_INTERVAL(1 * 1000 * 1000))
          {
            TBSYS_LOG(WARN, "process fail ret=%d pcode=%d src=%s",
                      (OB_SUCCESS != ret) ? ret : thread_errno(), task->pkt.get_packet_code(), inet_ntoa_r(task->src_addr));
          }
        }
        if (OB_SUCCESS != ret)
        {
          UPS.response_result(ret, task->last_process_err_msg_ptr, task->pkt);
        }
        if (release_task)
        {
          allocator_.free(task);
          task = NULL;
        }
      }
    }

#define LOG_SESSION(header, session_ctx, task)                     \
  FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), header " packet wait=%ld start_time=%ld timeout=%ld src=%s fd=%d %s ctx=%p", \
                 tbsys::CTimeUtil::getTime() - task.pkt.get_receive_ts(), \
                 task.pkt.get_receive_ts(),                             \
                 task.pkt.get_source_timeout(),                         \
                 inet_ntoa_r(task.src_addr),                            \
                 get_fd(task.pkt.get_request()),                        \
                 to_cstring(task.sid),                                  \
                 session_ctx);

    int TransExecutor::get_session_type(const ObTransID& sid, SessionType& type, const bool check_session_expired)
    {
      int ret = OB_SUCCESS;
      SessionGuard session_guard(session_mgr_, lock_mgr_, ret);
      BaseSessionCtx *session_ctx = NULL;
      if (OB_SUCCESS != (ret = session_guard.fetch_session(sid, session_ctx, check_session_expired)))
      {
        TBSYS_LOG(WARN, "fetch_session(%s)=>%d", to_cstring(sid), ret);
      }
      else
      {
        type = session_ctx->get_type();
      }
      return ret;
    }

    bool TransExecutor::handle_write_trans_(Task &task, ObMutator &mutator, ObNewScanner &scanner)
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      ObTransReq req;
      SessionGuard session_guard(session_mgr_, lock_mgr_, ret);
      RWSessionCtx *session_ctx = NULL;
      ILockInfo *lock_info = NULL;
      int64_t pos = task.pkt.get_buffer()->get_position();
      int64_t packet_timewait = (0 == task.pkt.get_source_timeout()) ?
                                UPS.get_param().packet_max_wait_time :
                                task.pkt.get_source_timeout();
      int64_t process_timeout = packet_timewait - QUERY_TIMEOUT_RESERVE;
      req.start_time_ = task.pkt.get_receive_ts();
      req.timeout_ = process_timeout;
      req.idle_time_ = process_timeout;
      task.sid.reset();
      bool give_up_lock = true;
      if (!UPS.is_master_lease_valid())
      {
        ret = OB_NOT_MASTER;
        TBSYS_LOG(WARN, "ups master_lease is invalid, pcode=%d", task.pkt.get_packet_code());
      }
      else if (OB_SUCCESS != (ret = ui_.ui_deserialize_mutator(*task.pkt.get_buffer(), mutator)))
      {
        TBSYS_LOG(WARN, "deserialize mutator fail ret=%d", ret);
      }
      else
      {
        task.pkt.get_buffer()->get_position() = pos;
      }
      if (OB_SUCCESS != ret)
      {}
      else if (OB_SUCCESS != (ret = session_guard.start_session(req, task.sid, session_ctx)))
      {
        if (OB_BEGIN_TRANS_LOCKED == ret)
        {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = TransHandlePool::push(&task,
                  task.pkt.get_req_sign(),
                  task.pkt.get_packet_priority())))
          {
            ret = tmp_ret;
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "start_session()=>%d", ret);
        }
      }
      else
      {
        LOG_SESSION("mutator start", session_ctx, task);
        session_ctx->set_conflict_session_id(task.last_conflict_session_id);
        session_ctx->set_start_handle_time(tbsys::CTimeUtil::getTime());
        session_ctx->set_stmt_start_time(task.pkt.get_receive_ts());
        session_ctx->set_stmt_timeout(process_timeout);
        session_ctx->set_self_processor_index(TransHandlePool::get_thread_index() + TransHandlePool::get_thread_num());
        session_ctx->set_priority((PriorityPacketQueueThread::QueuePriority)task.pkt.get_packet_priority());
        if (ST_READ_WRITE != session_ctx->get_type())
        {
          ret = OB_TRANS_NOT_MATCH;
          TBSYS_LOG(ERROR, "session.type[%d] is not RW", session_ctx->get_type());
        }
        else if (NULL == (lock_info = session_ctx->get_lock_info()))
        {
          ret = OB_NOT_INIT;
          TBSYS_LOG(ERROR, "lock_info == NULL");
        }
        else if (OB_SUCCESS != (ret = UPS.get_table_mgr().apply(OB_WRITE != task.pkt.get_packet_code(), *session_ctx, *lock_info, mutator)))
        {
          if ((OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret
                || OB_ERR_SHARED_LOCK_CONFLICT == ret)
              && !session_ctx->is_session_expired()
              && !session_ctx->is_stmt_expired())
          {
            int tmp_ret = OB_SUCCESS;
            task.last_conflict_session_id = session_ctx->get_conflict_session_id();
            if (OB_SUCCESS != (tmp_ret = TransHandlePool::push(&task,
                                                               0,
                    task.pkt.get_packet_priority())))
            {
              TBSYS_LOG(WARN, "push back task fail, ret=%d conflict_processor_index=%ld task=%p",
                        tmp_ret, session_ctx->get_conflict_processor_index(), &task);
            }
            else
            {
              give_up_lock = false;
            }
          }
          if (give_up_lock)
          {
            OB_STAT_INC(UPDATESERVER, UPS_STAT_APPLY_FAIL_COUNT, 1);
            TBSYS_LOG(WARN, "table mgr apply fail ret=%d", ret);
          }
        }
        else
        {
          scanner.reuse();
          if (OB_SUCCESS != (ret = session_ctx->get_ups_result().set_scanner(scanner)))
          {
            TBSYS_LOG(ERROR, "session_ctx.set_scanner()=>%d", ret);
          }
          else if (OB_SUCCESS != (ret = session_ctx->get_ups_mutator().get_mutator().pre_serialize()))
          {
            TBSYS_LOG(ERROR, "session_ctx.mutator.pre_serialize()=>%d", ret);
          }
          else
          {
            TBSYS_LOG(DEBUG, "precommit end timeu=%ld", tbsys::CTimeUtil::getTime() - task.pkt.get_receive_ts());
            OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), APPLY, COUNT), 1);
            OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), APPLY, QTIME), session_ctx->get_start_handle_time() - session_ctx->get_stmt_start_time());
            OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), APPLY, TIMEU), tbsys::CTimeUtil::getTime() - session_ctx->get_start_handle_time());
            session_guard.revert();
            ret = TransCommitThread::push(&task);
            if (OB_SUCCESS != ret && task.sid.is_valid())
            {
              session_mgr_.end_session(task.sid.descriptor_, true);
            }
          }
        }
      }
      if (OB_SUCCESS != ret && OB_BEGIN_TRANS_LOCKED != ret && give_up_lock)
      {
        UPS.response_result(ret, task.pkt);
      }
      return (OB_SUCCESS != ret && OB_BEGIN_TRANS_LOCKED != ret && give_up_lock);
    }

    bool TransExecutor::handle_start_session_(Task &task, ObDataBuffer &buffer)
    {
      int &ret = thread_errno();
      bool need_free_task = true;
      ret = OB_SUCCESS;
      ObTransReq req;
      int64_t pos = task.pkt.get_buffer()->get_position();
      SessionGuard session_guard(session_mgr_, lock_mgr_, ret);
      BaseSessionCtx* session_ctx = NULL;
      task.sid.reset();
      if (!UPS.is_master_lease_valid())
      {
        ret = OB_NOT_MASTER;
        TBSYS_LOG(WARN, "ups master_lease is invalid, pcode=%d", task.pkt.get_packet_code());
      }
      else if (OB_SUCCESS != (ret = req.deserialize(task.pkt.get_buffer()->get_data(),
                                                    task.pkt.get_buffer()->get_capacity(),
                                                    pos)))
      {
        TBSYS_LOG(WARN, "deserialize session_req fail ret=%d", ret);
      }
      else
      {
        req.start_time_ = task.pkt.get_receive_ts();
      }
      if (OB_SUCCESS != ret)
      {}
      else if (OB_SUCCESS != (ret = session_guard.start_session(req, task.sid, session_ctx)))
      {
        if (OB_BEGIN_TRANS_LOCKED == ret)
        {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = TransHandlePool::push(&task,
                  task.pkt.get_req_sign(),
                  task.pkt.get_packet_priority())))
          {
            ret = tmp_ret;
          }
          else
          {
            need_free_task = false;
          }
        }
        else
        {
          TBSYS_LOG(WARN, "begin session fail ret=%d", ret);
        }
      }
      else
      {
        LOG_SESSION("session start", session_ctx, task);
        PRINT_TRACE_BUF(session_ctx->get_tlog_buffer());
      }
      if (need_free_task)
      {
        UPS.response_trans_id(ret, task.pkt, task.sid, buffer);
      }
      return need_free_task;
    }

    bool TransExecutor::handle_end_session_(Task &task, ObDataBuffer &buffer)
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      bool need_free_task = true;
      ObEndTransReq req;
      SessionType type = ST_READ_ONLY;
      UNUSED(buffer);
      task.sid.reset();
      const bool check_session_expired = true;
      int64_t pos = task.pkt.get_buffer()->get_position();
      if (!UPS.is_master_lease_valid())
      {
        ret = OB_NOT_MASTER;
        TBSYS_LOG(WARN, "ups master_lease is invalid, pcode=%d", task.pkt.get_packet_code());
      }
      else if (OB_SUCCESS != (ret = req.deserialize(task.pkt.get_buffer()->get_data(),
                                                    task.pkt.get_buffer()->get_capacity(),
                                                    pos)))
      {
        TBSYS_LOG(WARN, "deserialize session_req fail ret=%d", ret);
      }
      else if (!req.trans_id_.is_valid())
      {
        ret = OB_TRANS_NOT_MATCH;
        TBSYS_LOG(ERROR, "sid[%s] is invalid", to_cstring(task.sid));
      }
      if (OB_SUCCESS != ret)
      {
        UPS.response_result(ret, task.pkt);
      }
      else if (OB_SUCCESS != (ret = get_session_type(req.trans_id_, type, check_session_expired)))
      {
        UPS.response_result(req.rollback_? OB_SUCCESS: ret, task.pkt);
        TBSYS_LOG(WARN, "get_session_type(%s)=>%d", to_cstring(req.trans_id_), ret);
      }
      else
      {
        // sid 正确
        if (ST_READ_WRITE == type && !req.rollback_)
        {
          task.sid = req.trans_id_;
          if (OB_SUCCESS != (ret = TransCommitThread::push(&task)))
          {
            TBSYS_LOG(ERROR, "push task=%p to TransCommitThread fail, ret=%d %s", &task, ret, to_cstring(req.trans_id_));
            session_mgr_.end_session(req.trans_id_.descriptor_, true);
            UPS.response_result(OB_TRANS_ROLLBACKED, task.pkt);
          }
          else
          {
            need_free_task = false;
          }
        }
        else
        {
          ret = session_mgr_.end_session(req.trans_id_.descriptor_, req.rollback_);
          UPS.response_result(req.rollback_? OB_SUCCESS: ret, task.pkt);
        }
      }
      return need_free_task;
    }

    bool TransExecutor::handle_phyplan_trans_(Task &task,
                                             ObUpsPhyOperatorFactory &phy_operator_factory,
                                             sql::ObPhysicalPlan &phy_plan,
                                             ObNewScanner &new_scanner,
                                             ModuleArena &allocator,
                                             ObDataBuffer& buffer)
    {
      reset_warning_strings_();
      bool need_free_task = true;
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      int end_session_ret = OB_SUCCESS;
      SessionGuard session_guard(session_mgr_, lock_mgr_, end_session_ret);
      RWSessionCtx* session_ctx = NULL;
      int64_t packet_timewait = (0 == task.pkt.get_source_timeout()) ?
                                UPS.get_param().packet_max_wait_time :
                                task.pkt.get_source_timeout();
      int64_t process_timeout = packet_timewait - QUERY_TIMEOUT_RESERVE;
      int64_t pos = task.pkt.get_buffer()->get_position();
      bool with_sid = false;
      //task.sid.reset();  //add hongchen [LOCK_WAIT_TIMEOUT] 20161209
      bool give_up_lock = true;
      if (!UPS.is_master_lease_valid())
      {
        ret = OB_NOT_MASTER;
        TBSYS_LOG(WARN, "ups master_lease is invalid, pcode=%d", task.pkt.get_packet_code());
      }
      else if (OB_SUCCESS != (ret = phy_plan.deserialize_header(task.pkt.get_buffer()->get_data(),
                                                                task.pkt.get_buffer()->get_capacity(),
                                                                pos)))
      {
        TBSYS_LOG(WARN, "phy_plan.deseiralize_header ret=%d", ret);
      }
      else if ((with_sid = phy_plan.get_trans_id().is_valid()))
      {
        const bool check_session_expired = true;
        if (OB_SUCCESS != (ret = session_guard.fetch_session(phy_plan.get_trans_id(), session_ctx, check_session_expired)))
        {
          TBSYS_LOG(WARN, "Session has been killed, error %d, \'%s\'", ret, to_cstring(phy_plan.get_trans_id()));
        }
        else if (session_ctx->get_stmt_start_time() > task.pkt.get_receive_ts())
        {
          TBSYS_LOG(ERROR, "maybe an expired request, will skip it, last_stmt_start_time=%ld receive_ts=%ld",
                    session_ctx->get_stmt_start_time(), task.pkt.get_receive_ts());
          ret = (OB_SUCCESS == task.last_process_retcode) ? OB_STMT_EXPIRED : task.last_process_retcode;
        }
        else
        {
          CLEAR_TRACE_BUF(session_ctx->get_tlog_buffer());
          session_ctx->reset_stmt();
          task.sid = phy_plan.get_trans_id();
        }
        // add by maosy [Delete_Update_Function_isolation_RC] 20161228
        if(OB_SUCCESS==ret && phy_plan.get_trans_id ().read_times_ == EMPTY_ROW_CLEAR)
        {
            session_ctx->set_start_time_for_batch (0);
            ret = OB_ERR_BATCH_EMPTY_ROW ;
        }
        // add e
      }
      //add hongchen [LOCK_WAIT_TIMEOUT] 20161209:b
      else if (0 != task.pkt.get_last_conflict_lock_time())
      {
          const bool check_session_expired = true;
          if (OB_SUCCESS != (ret = session_guard.fetch_session(task.sid, session_ctx, check_session_expired)))
          {
            TBSYS_LOG(WARN, "Session has been killed, error %d, \'%s\'", ret, to_cstring(task.sid));
          }
          else if (session_ctx->get_stmt_start_time() > task.pkt.get_receive_ts())
          {
            TBSYS_LOG(ERROR, "maybe an expired request, will skip it, last_stmt_start_time=%ld receive_ts=%ld",
                      session_ctx->get_stmt_start_time(), task.pkt.get_receive_ts());
            ret = (OB_SUCCESS == task.last_process_retcode) ? OB_STMT_EXPIRED : task.last_process_retcode;
          }
          else
          {
            CLEAR_TRACE_BUF(session_ctx->get_tlog_buffer());
            session_ctx->reset_stmt();
            //add hongchen [LOCK_WAIT_TIMEOUT] 20161227:b
            //fix bug:save transid for case start_new_trans
            session_ctx->get_ups_result().set_trans_id(task.sid);
            //add hongchen [LOCK_WAIT_TIMEOUT] 20161227:e
          }
      }
      //add hongchen [LOCK_WAIT_TIMEOUT] 20161209:e
      else
      {
        task.sid.reset();  //add hongchen [LOCK_WAIT_TIMEOUT] 20161209
        phy_plan.get_trans_req().start_time_ = task.pkt.get_receive_ts();
        // add by maosy [Delete_Update_Function_isolation_RC] 20161228
        if(OB_SUCCESS==ret && phy_plan.get_trans_id ().read_times_ == EMPTY_ROW_CLEAR)
        {
            session_ctx->set_start_time_for_batch (0);
            ret = OB_ERR_BATCH_EMPTY_ROW ;
        }
        // add e
        else if (OB_SUCCESS != (ret = session_guard.start_session(phy_plan.get_trans_req(), task.sid, session_ctx)))
        {
          if (OB_BEGIN_TRANS_LOCKED == ret)
          {
            int tmp_ret = OB_SUCCESS;
            task.last_process_retcode = ret;
            if (OB_SUCCESS != (tmp_ret = TransHandlePool::push(&task,
                    task.pkt.get_req_sign(),
                    task.pkt.get_packet_priority())))
            {
              ret = tmp_ret;
            }
            else
            {
              need_free_task = false;
            }
          }
          else
          {
            TBSYS_LOG(ERROR, "start_session()=>%d", ret);
          }
        }
        else
        {
          if (phy_plan.get_start_trans())
          {
            session_ctx->get_ups_result().set_trans_id(task.sid);
          }
        }
      }
      if (OB_SUCCESS != ret)
      {}
      else
      {
        int64_t cur_time = tbsys::CTimeUtil::getTime();
        BEGIN_SAMPLE_SLOW_QUERY(session_ctx);
        LOG_SESSION("start_trans", session_ctx, task);
        OB_STAT_INC(UPDATESERVER, UPS_STAT_TRANS_WTIME, cur_time - task.pkt.get_receive_ts());
        session_ctx->set_conflict_session_id(task.last_conflict_session_id);
        session_ctx->set_last_proc_time(cur_time);

        session_ctx->set_start_handle_time(tbsys::CTimeUtil::getTime());
        session_ctx->set_stmt_start_time(task.pkt.get_receive_ts());
        session_ctx->set_stmt_timeout(process_timeout);
        session_ctx->set_self_processor_index(TransHandlePool::get_thread_index() + TransHandlePool::get_thread_num());
        session_ctx->set_priority((PriorityPacketQueueThread::QueuePriority)task.pkt.get_packet_priority());
        session_ctx->mark_stmt();
        //add hongchen [LOCK_WAIT_TIMEOUT] 20161209:b
        if (0 == task.pkt.get_last_conflict_lock_time())
        {
          task.pkt.set_lock_wait_timeout(UPS.get_param().lock_wait_time);
        }
        //add hongchen [LOCK_WAIT_TIMEOUT] 20161209:e
        session_ctx->set_pkt_ptr(&(task.pkt));  //add hongchen [LOCK_WAIT_TIMEOUT] 20161209
        sql::ObPhyOperator *main_op = NULL;
        phy_operator_factory.set_session_ctx(session_ctx);
        phy_operator_factory.set_table_mgr(&UPS.get_table_mgr());
        phy_plan.clear();
        allocator.reuse();
        phy_plan.set_allocator(&allocator);
        phy_plan.set_operator_factory(&phy_operator_factory);

        int64_t pos = task.pkt.get_buffer()->get_position();
        if (OB_SUCCESS != (ret = phy_plan.deserialize(task.pkt.get_buffer()->get_data(),
                                                      task.pkt.get_buffer()->get_capacity(),
                                                      pos)))
        {
          TBSYS_LOG(WARN, "deserialize phy_plan fail ret=%d", ret);
        }
        else
        {
          FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "phyplan allocator used=%ld total=%ld",
                        allocator.used(), allocator.total());
        }
        if (OB_SUCCESS != ret)
        {}
        else if (NULL == (main_op = phy_plan.get_main_query()))
        {
          TBSYS_LOG(WARN, "main query null pointer");
          ret = OB_ERR_UNEXPECTED;
        }
        else if (OB_SUCCESS != (ret = main_op->open())
                || OB_SUCCESS != (ret = fill_return_rows_(*main_op, new_scanner, session_ctx->get_ups_result())))
        {
          session_ctx->rollback_stmt();
          if ((OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret
                || OB_ERR_SHARED_LOCK_CONFLICT == ret)
              && !session_ctx->is_session_expired()
              && !session_ctx->is_stmt_expired())
          {
            int tmp_ret = OB_SUCCESS;
            uint32_t session_descriptor = session_ctx->get_session_descriptor();
            int64_t conflict_processor_index = 0;
            //del hongchen [LOCK_WAIT_TIMEOUT] 20161209:b
            /*
            if (!with_sid)
            {
              end_session_ret = ret;
            }
            */
            //del hongchen [LOCK_WAIT_TIMEOUT] 20161209:e
            task.last_conflict_session_id = session_ctx->get_conflict_session_id();
            session_ctx = NULL;
            session_guard.revert();
            task.last_process_retcode = ret;
            task.set_last_err_msg(ob_get_err_msg().ptr());
            if (OB_SUCCESS != (tmp_ret = TransHandlePool::push(&task,
                    conflict_processor_index,
                    task.pkt.get_packet_priority())))
            {
              TBSYS_LOG(WARN, "push back task fail, ret=%d conflict_processor_index=%ld task=%p",
                        tmp_ret, conflict_processor_index, &task);
              session_mgr_.end_session(session_descriptor, true);
            }
            else
            {
              give_up_lock = false;
              need_free_task = false;
            }
          }
          //add hongchen [LOCK_WAIT_TIMEOUT] optimize 20170216:b
          else
          {
            session_ctx->set_pkt_ptr(NULL);
          }
          //add hongchen [LOCK_WAIT_TIMEOUT] optimize 20170216:e
          if (OB_ERR_PRIMARY_KEY_DUPLICATE != ret
              && give_up_lock)
          {
            OB_STAT_INC(UPDATESERVER, UPS_STAT_APPLY_FAIL_COUNT, 1);
            //TBSYS_LOG(WARN, "main_op open fail ret=%d", ret);
          }
          main_op->close();
        }
        //else if (OB_SUCCESS != (ret = fill_return_rows_(*main_op, new_scanner, session_ctx->get_ups_result())))
        //{
        //  main_op->close();
        //  TBSYS_LOG(WARN, "fill result rows with main_op fail ret=%d", ret);
        //}
        else
        {
            // add by maosy [Delete_Update_Function_isolation_RC] 20161222
           if( phy_plan.get_trans_id ().read_times_==CLEAR_TIMESTAMP)
           {
               session_ctx->set_start_time_for_batch (0);
           }
           // add by maosy e
          session_ctx->set_pkt_ptr(NULL);  //add hongchen [LOCK_WAIT_TIMEOUT] optimize 20170216
          session_ctx->commit_stmt();
          main_op->close();
          fill_warning_strings_(session_ctx->get_ups_result());
          TBSYS_LOG(DEBUG, "precommit end timeu=%ld", tbsys::CTimeUtil::getTime() - task.pkt.get_receive_ts());
        }

        if (OB_SUCCESS != ret)
        {
          if (phy_plan.get_start_trans()
              && NULL != session_ctx
              && session_ctx->get_ups_result().get_trans_id().is_valid()
              && give_up_lock)
          {
            session_ctx->get_ups_result().set_error_code(ret);
            ret = OB_SUCCESS;
            session_ctx->get_ups_result().serialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
            const char *error_string = ob_get_err_msg().ptr();
            //mod hongchen [LOCK_WAIT_TIMEOUT] 20161209:b
            {
              tbsys::CThreadGuard guard(task.pkt.get_lock_for_lock_wait_para());
              if (task.pkt.get_need_response())
              {
                 UPS.response_buffer(ret, task.pkt, buffer, error_string);
                 task.pkt.set_need_response(false);
              }
            }
            //UPS.response_buffer(ret, task.pkt, buffer, error_string);
            //mod hongchen [LOCK_WAIT_TIMEOUT] 20161209:e
          }
        }
        else if (with_sid || phy_plan.get_start_trans())
        {
          session_ctx->get_ups_result().serialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
          //mod hongchen [LOCK_WAIT_TIMEOUT] 20161209:b
          {
            tbsys::CThreadGuard guard(task.pkt.get_lock_for_lock_wait_para());
            if (task.pkt.get_need_response())
            {
              UPS.response_buffer(ret, task.pkt, buffer);
              task.pkt.set_need_response(false);
            }
          }
          //UPS.response_buffer(ret, task.pkt, buffer);
          //mod hongchen [LOCK_WAIT_TIMEOUT] 20161209:e
          OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), APPLY, COUNT), 1);
          OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), APPLY, QTIME), session_ctx->get_start_handle_time() - session_ctx->get_stmt_start_time());
          OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), APPLY, TIMEU), tbsys::CTimeUtil::getTime() - session_ctx->get_start_handle_time());
          END_SAMPLE_SLOW_QUERY(session_ctx, task.pkt);
        }
        else
        {
          if (OB_SUCCESS != (ret = session_ctx->get_ups_mutator().get_mutator().pre_serialize()))
          {
            TBSYS_LOG(ERROR, "session_ctx.mutator.pre_serialize()=>%d", ret);
          }
          else
          {
            FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "ret=%d affected_rows=%ld", ret, session_ctx->get_ups_result().get_affected_rows());
            PRINT_TRACE_BUF(session_ctx->get_tlog_buffer());
            OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), APPLY, COUNT), 1);
            OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), APPLY, QTIME), session_ctx->get_start_handle_time() - session_ctx->get_stmt_start_time());
            OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), APPLY, TIMEU), tbsys::CTimeUtil::getTime() - session_ctx->get_start_handle_time());
            END_SAMPLE_SLOW_QUERY(session_ctx, task.pkt);

            cur_time = tbsys::CTimeUtil::getTime();
            OB_STAT_INC(UPDATESERVER, UPS_STAT_TRANS_HTIME, cur_time - session_ctx->get_last_proc_time());
            session_ctx->set_last_proc_time(cur_time);

            session_ctx = NULL;
            session_guard.revert();
            if (OB_SUCCESS != (ret = TransCommitThread::push(&task)))
            {
              TBSYS_LOG(WARN, "commit thread queue is full, ret=%d", ret);
            }
            else
            {
              need_free_task = false;
            }
            if (OB_SUCCESS != ret && task.sid.is_valid())
            {
              session_mgr_.end_session(task.sid.descriptor_, true);
            }
          }
        }
        if (NULL != session_ctx)
        {
          FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "ret=%d affected_rows=%ld", ret, session_ctx->get_ups_result().get_affected_rows());
          PRINT_TRACE_BUF(session_ctx->get_tlog_buffer());
        }
        if (OB_SUCCESS != ret
            && ((!with_sid && !phy_plan.get_start_trans()) || !IS_SQL_ERR(ret))
            && give_up_lock)
        {
          end_session_ret = ret;
          TBSYS_LOG(DEBUG, "need rollback session %s ret=%d", to_cstring(task.sid), ret);
        }
      }
      phy_plan.clear();
      if (OB_SUCCESS != ret && OB_BEGIN_TRANS_LOCKED != ret && give_up_lock)
      {
        //ret = (OB_ERR_SHARED_LOCK_CONFLICT == ret) ? OB_EAGAIN : ret;
        const char *error_string = ob_get_err_msg().ptr();
        //mod hongchen [LOCK_WAIT_TIMEOUT] 20161209:b
        {
          tbsys::CThreadGuard guard(task.pkt.get_lock_for_lock_wait_para());
          if (task.pkt.get_need_response())
          {
            UPS.response_result(ret, error_string, task.pkt);
            task.pkt.set_need_response(false);
          }
        }
        //UPS.response_result(ret, error_string, task.pkt);
        //mod hongchen [LOCK_WAIT_TIMEOUT] 20161209:e
      }
      //del hongchen [LOCK_WAIT_TIMEOUT] optimize 20170216:b
      /*
      //add hongchen [LOCK_WAIT_TIMEOUT] 20161209:b
      session_guard.revert();
      if (give_up_lock)
      {
         BaseSessionCtx* ctx = session_mgr_.get_session_ctx_map()->fetch(task.sid.descriptor_, FM_MUTEX_BLOCK);
         if (NULL != ctx)
         {
           ctx->set_pkt_ptr(NULL);
           session_mgr_.revert_ctx(task.sid.descriptor_);
         }
      }
      //add hongchen [LOCK_WAIT_TIMEOUT] 20161209:e
      */
      //del hongchen [LOCK_WAIT_TIMEOUT] optimize 20170216:e
      return need_free_task;
    }

    void TransExecutor::handle_get_trans_(ObPacket &pkt,
                                          ObGetParam &get_param,
                                          ObScanner &scanner,
                                          ObCellNewScanner &new_scanner,
                                          ObDataBuffer &buffer)
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      uint32_t session_descriptor = UINT32_MAX;
      ROSessionCtx *session_ctx = NULL;
      int64_t packet_timewait = (0 == pkt.get_source_timeout()) ?
                                UPS.get_param().packet_max_wait_time :
                                pkt.get_source_timeout();
      int64_t process_timeout = packet_timewait - QUERY_TIMEOUT_RESERVE;
      int64_t pos = pkt.get_buffer()->get_position();
      if (OB_SUCCESS != (ret = get_param.deserialize(pkt.get_buffer()->get_data(),
                                                    pkt.get_buffer()->get_capacity(),
                                                    pos)))
      {
        TBSYS_LOG(WARN, "deserialize get_param fail ret=%d", ret);
      }
      // del by maosy [Delete_Update_Function_isolation_RC]
      //add by maosy [Delete_Update_Function_for_snpshot] 20161215
//      else if(get_param.get_trans_id ().read_times_ >0)
//      {
//          TBSYS_LOG(WARN,"batch update or delete must have tranid %s",to_cstring(get_param.get_trans_id()));
//          ret = OB_ERR_BATCH_NOT_IN_TRANSACTION;
//      }
      //add by maosy 20161215
    //del by maosy 20161218
      else if (!UPS.can_serve_read_req(get_param.get_is_read_consistency(), get_param.get_version_range().get_query_version()))
      {
        if (REACH_TIME_INTERVAL(10 * 1000 * 1000))
        {
          TBSYS_LOG(WARN, "the scan request require consistency, ObiRole:%s RoleMgr:%s, query_version=%ld",
                    UPS.get_obi_role().get_role_str(), UPS.get_role_mgr().get_role_str(), get_param.get_version_range().get_query_version());
        }
        ret = OB_NOT_MASTER;
      }
      else if (OB_SUCCESS != (ret = session_mgr_.begin_session(ST_READ_ONLY, pkt.get_receive_ts(), process_timeout, process_timeout, session_descriptor)))
      {
        TBSYS_LOG(WARN, "begin session fail ret=%d", ret);
      }
      else if (NULL == (session_ctx = session_mgr_.fetch_ctx<ROSessionCtx>(session_descriptor)))
      {
        TBSYS_LOG(WARN, "fetch ctx fail session_descriptor=%u", session_descriptor);
        ret = OB_ERR_UNEXPECTED;
      }
      else if (session_ctx->is_session_expired())
      {
        session_mgr_.revert_ctx(session_descriptor);
        session_ctx = NULL;
        ret = OB_TRANS_ROLLBACKED;
      }
      else
      {
        BEGIN_SAMPLE_SLOW_QUERY(session_ctx);
        FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "start handle get, packet wait=%ld start_time=%ld timeout=%ld src=%s",
                      tbsys::CTimeUtil::getTime() - pkt.get_receive_ts(),
                      pkt.get_receive_ts(),
                      pkt.get_source_timeout(),
                      NULL == pkt.get_request() ? NULL : get_peer_ip(pkt.get_request()));
        thread_read_prepare();
        session_ctx->set_start_handle_time(tbsys::CTimeUtil::getTime());
        session_ctx->set_stmt_start_time(pkt.get_receive_ts());
        session_ctx->set_stmt_timeout(process_timeout);
        session_ctx->set_priority((PriorityPacketQueueThread::QueuePriority)pkt.get_packet_priority());
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
        if (get_param.get_data_mark_param().is_valid())
        {
            TBSYS_LOG(DEBUG,"mul_del::debug,UPS get real data mark param[%s]!",
                      to_cstring(get_param.get_data_mark_param()));
        }
        //add duyr 20160531:e
        if (OB_NEW_GET_REQUEST == pkt.get_packet_code())
        {
          new_scanner.reuse();
          common::ObRowDesc row_desc;
          if(OB_SUCCESS != (ret = ObNewScannerHelper::get_row_desc(get_param, true, row_desc)))
          {
            TBSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
          }
          else
          {
            new_scanner.set_row_desc(row_desc);
            ret = UPS.get_table_mgr().new_get(*session_ctx, get_param, new_scanner, pkt.get_receive_ts(), process_timeout);
          }
          if (OB_SUCCESS == ret)
          {
            UPS.response_scanner(ret, pkt, new_scanner, buffer);
          }
        }
        else
        {
          scanner.reset();
          ret = UPS.get_table_mgr().get(*session_ctx, get_param, scanner, pkt.get_receive_ts(), process_timeout);
          if (OB_SUCCESS == ret)
          {
            UPS.response_scanner(ret, pkt, scanner, buffer);
          }
        }
        FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "get from table mgr ret=%d", ret);
        //OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_GET_COUNT, 1);
        OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), GET, COUNT), 1);
        OB_STAT_INC(UPDATESERVER, UPS_STAT_GET_QTIME, session_ctx->get_start_handle_time() - session_ctx->get_session_start_time());
        //OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_GET_TIMEU, session_ctx->get_session_timeu());
        OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), GET, TIMEU), session_ctx->get_session_timeu());
        END_SAMPLE_SLOW_QUERY(session_ctx, pkt);
        thread_read_complete();
        session_ctx->set_last_active_time(tbsys::CTimeUtil::getTime());
        session_mgr_.revert_ctx(session_descriptor);
        session_mgr_.end_session(session_descriptor);
        log_get_qps_();
      }
      if (OB_SUCCESS != ret)
      {
        UPS.response_result(ret, pkt);
      }
    }

    //add zhujun [transaction read uncommit] 2016/3/22
    void TransExecutor::handle_get_trans_in_one(Task &task,
                                          ObGetParam &get_param,
                                          ObScanner &scanner,
                                          ObCellNewScanner &new_scanner,
                                          ObDataBuffer &buffer)
    {
      //TBSYS_LOG(INFO, "TransExecutor::handle_get_trans_in_one");
      //add zhujun
      common::ObPacket pkt = task.pkt;
      int end_session_ret = OB_SUCCESS;
      SessionGuard session_guard(session_mgr_, lock_mgr_, end_session_ret);
      const bool check_session_expired = true;
      //add:e
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      uint32_t session_descriptor = get_param.get_trans_id().descriptor_;
      //ROSessionCtx *session_ctx = NULL;
      RWSessionCtx* session_ctx = NULL;
      int64_t packet_timewait = (0 == pkt.get_source_timeout()) ?
                                UPS.get_param().packet_max_wait_time :
                                pkt.get_source_timeout();
      int64_t process_timeout = packet_timewait - QUERY_TIMEOUT_RESERVE;
      int64_t pos = pkt.get_buffer()->get_position();
      if (OB_SUCCESS != (ret = get_param.deserialize(pkt.get_buffer()->get_data(),
                                                    pkt.get_buffer()->get_capacity(),
                                                    pos)))
      {
        TBSYS_LOG(WARN, "deserialize get_param fail ret=%d", ret);
      }
      else if (!UPS.can_serve_read_req(get_param.get_is_read_consistency(), get_param.get_version_range().get_query_version()))
      {
        if (REACH_TIME_INTERVAL(10 * 1000 * 1000))
        {
          TBSYS_LOG(WARN, "the scan request require consistency, ObiRole:%s RoleMgr:%s, query_version=%ld",
                    UPS.get_obi_role().get_role_str(), UPS.get_role_mgr().get_role_str(), get_param.get_version_range().get_query_version());
        }
        ret = OB_NOT_MASTER;
      }
//      else if (OB_SUCCESS != (ret = session_mgr_.begin_session(ST_READ_ONLY, pkt.get_receive_ts(), process_timeout, process_timeout, session_descriptor)))
//      {
//        TBSYS_LOG(WARN, "begin session fail ret=%d", ret);
//      }
      else if (OB_SUCCESS != (ret = session_guard.fetch_session(get_param.get_trans_id(), session_ctx, check_session_expired)))
      {
        TBSYS_LOG(WARN, "Session has been killed, error %d, \'%s\'", ret, to_cstring(get_param.get_trans_id()));
      }
//      else if (NULL == (session_ctx = session_mgr_.fetch_ctx<ROSessionCtx>(session_descriptor)))
//      {
//        TBSYS_LOG(WARN, "fetch ctx fail session_descriptor=%u", session_descriptor);
//        ret = OB_ERR_UNEXPECTED;
//      }
      else if (session_ctx->is_session_expired())
      {
        session_mgr_.revert_ctx(session_descriptor);
        session_ctx = NULL;
        ret = OB_TRANS_ROLLBACKED;
      }
      else
      {
        int64_t cur_time = tbsys::CTimeUtil::getTime();
        BEGIN_SAMPLE_SLOW_QUERY(session_ctx);
        FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "start handle get, packet wait=%ld start_time=%ld timeout=%ld src=%s",
                      tbsys::CTimeUtil::getTime() - pkt.get_receive_ts(),
                      pkt.get_receive_ts(),
                      pkt.get_source_timeout(),
                      NULL == pkt.get_request() ? NULL : get_peer_ip(pkt.get_request()));
        thread_read_prepare();
        //add zhujun
        session_ctx->set_conflict_session_id(task.last_conflict_session_id);
        session_ctx->set_last_proc_time(cur_time);
        //add:e
        //add hbt:read_uncomomit
        //将事务开始时刻的已发布版本号记录下来
        int64_t trans_start_trans_id=session_ctx->get_trans_id();
        //让select语句读取当前时刻已提交的数据
        session_ctx->set_trans_id(session_mgr_.get_published_trans_id());
        // add by maosy [Delete_Update_Function_isolation_RC] 20161218
//add by hongchen [Delete_Update_Function_for_snpshot] 20161210
//        if (1 == get_param.get_trans_id().read_times_ && session_ctx->get_trans_id_for_batach () == INT64_MAX)
//        {
//            session_ctx->set_trans_id_for_batach (session_ctx->get_trans_id ());
//        }
//        else if (2 == get_param.get_trans_id().read_times_)
//        {
//            session_ctx->set_trans_id(session_ctx->get_trans_id_for_batach ());
//        }
//        else
//        {
//            session_ctx->set_trans_id_for_batach(INT64_MAX);
//        }
//add by hongchen  20161210
// add by maosy 20161218
        //add:e
        session_ctx->set_start_handle_time(tbsys::CTimeUtil::getTime());
        // add by maosy [Delete_Update_Function_isolation_RC] 20161222
        if(get_param.get_trans_id ().read_times_ == FIRST_SELECT_READ )
        {
            if(session_ctx->get_start_time_for_batch () ==0)
            {
                session_ctx->set_start_time_for_batch (session_ctx->get_trans_id ());
            }
            else
            {
                session_ctx->set_new_transid ();
            }
            TBSYS_LOG(DEBUG,"start time = %ld,session = %p",session_ctx->get_start_time_for_batch (),session_ctx);
        }
        // add e
        session_ctx->set_stmt_start_time(pkt.get_receive_ts());
        session_ctx->set_stmt_timeout(process_timeout);
        session_ctx->set_priority((PriorityPacketQueueThread::QueuePriority)pkt.get_packet_priority());
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
        if (get_param.get_data_mark_param().is_valid())
        {
            TBSYS_LOG(DEBUG,"mul_del::debug,UPS get real data mark param[%s]!is in one!",
                      to_cstring(get_param.get_data_mark_param()));
        }
        //add duyr 20160531:e
        if (OB_NEW_GET_REQUEST == pkt.get_packet_code())
        {
          new_scanner.reuse();
          common::ObRowDesc row_desc;
          if(OB_SUCCESS != (ret = ObNewScannerHelper::get_row_desc(get_param, true, row_desc)))
          {
            TBSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
          }
          else
          {
            new_scanner.set_row_desc(row_desc);
            ret = UPS.get_table_mgr().new_get(*session_ctx, get_param, new_scanner, pkt.get_receive_ts(), process_timeout);
          }
          if (OB_SUCCESS == ret)
          {
            UPS.response_scanner(ret, pkt, new_scanner, buffer);
          }
        }
        else
        {
          scanner.reset();
          ret = UPS.get_table_mgr().get(*session_ctx, get_param, scanner, pkt.get_receive_ts(), process_timeout);
          if (OB_SUCCESS == ret)
          {
            UPS.response_scanner(ret, pkt, scanner, buffer);
          }
        }
        FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "get from table mgr ret=%d", ret);
        //OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_GET_COUNT, 1);
        OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), GET, COUNT), 1);
        OB_STAT_INC(UPDATESERVER, UPS_STAT_GET_QTIME, session_ctx->get_start_handle_time() - session_ctx->get_session_start_time());
        //OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_GET_TIMEU, session_ctx->get_session_timeu());
        OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), GET, TIMEU), session_ctx->get_session_timeu());
        END_SAMPLE_SLOW_QUERY(session_ctx, pkt);
        thread_read_complete();
        session_ctx->set_last_active_time(tbsys::CTimeUtil::getTime());
        //add:hbt:read_uncommit
        //将事务开始时刻的已发布版本号放回到ctx中
        session_ctx->set_trans_id(trans_start_trans_id);
        session_guard.revert();
        //add:e
        //delete zhujun
        //session_mgr_.end_session(session_descriptor,false,true);
        log_get_qps_();
      }
      if (OB_SUCCESS != ret)
      {
        UPS.response_result(ret, pkt);
      }
    }



    void TransExecutor::handle_scan_trans_(ObPacket &pkt,
                                          ObScanParam &scan_param,
                                          ObScanner &scanner,
                                          ObCellNewScanner &new_scanner,
                                          ObDataBuffer &buffer)
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      uint32_t session_descriptor = UINT32_MAX;
      ROSessionCtx *session_ctx = NULL;
      int64_t packet_timewait = (0 == pkt.get_source_timeout()) ?
                                UPS.get_param().packet_max_wait_time :
                                pkt.get_source_timeout();
      int64_t process_timeout = packet_timewait - QUERY_TIMEOUT_RESERVE;
      int64_t pos = pkt.get_buffer()->get_position();
      if (OB_SUCCESS != (ret = scan_param.deserialize(pkt.get_buffer()->get_data(),
                                                      pkt.get_buffer()->get_capacity(),
                                                      pos)))
      {
        TBSYS_LOG(WARN, "deserialize get_param fail ret=%d", ret);
      }
      // add by maosy [Delete_Update_Function_isolation_RC] 20161218
      //add by maosy [Delete_Update_Function_for_snpshot] 20161215
//      else if(scan_param.get_trans_id().read_times_ >0)
//      {
//          TBSYS_LOG(WARN,"batch update or delete must have tranid %s",to_cstring(scan_param.get_trans_id()));
//          ret = OB_ERR_BATCH_NOT_IN_TRANSACTION;
//      }
      //del by maosy
    //del by maosy 20161218 e
      else if (!UPS.can_serve_read_req(scan_param.get_is_read_consistency(), scan_param.get_version_range().get_query_version()))
      {
        if (REACH_TIME_INTERVAL(10 * 1000 * 1000))
        {
          TBSYS_LOG(WARN, "the scan request require consistency, ObiRole:%s RoleMgr:%s, query_version=%ld",
                    UPS.get_obi_role().get_role_str(), UPS.get_role_mgr().get_role_str(), scan_param.get_version_range().get_query_version());
        }
        ret = OB_NOT_MASTER;
      }
      else if (OB_SUCCESS != (ret = session_mgr_.begin_session(ST_READ_ONLY, pkt.get_receive_ts(), process_timeout, process_timeout, session_descriptor)))
      {
        TBSYS_LOG(WARN, "begin session fail ret=%d", ret);
      }
      else if (NULL == (session_ctx = session_mgr_.fetch_ctx<ROSessionCtx>(session_descriptor)))
      {
        TBSYS_LOG(WARN, "fetch ctx fail session_descriptor=%u", session_descriptor);
        ret = OB_ERR_UNEXPECTED;
      }
      else if (session_ctx->is_session_expired())
      {
        session_mgr_.revert_ctx(session_descriptor);
        session_ctx = NULL;
        ret = OB_TRANS_ROLLBACKED;
      }
      else
      {
        BEGIN_SAMPLE_SLOW_QUERY(session_ctx);
        FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "start handle scan, packet wait=%ld start_time=%ld timeout=%ld src=%s",
                      tbsys::CTimeUtil::getTime() - pkt.get_receive_ts(),
                      pkt.get_receive_ts(),
                      pkt.get_source_timeout(),
                      NULL == pkt.get_request() ? NULL : get_peer_ip(pkt.get_request()));
        thread_read_prepare();
        session_ctx->set_start_handle_time(tbsys::CTimeUtil::getTime());
        session_ctx->set_stmt_start_time(pkt.get_receive_ts());
        session_ctx->set_stmt_timeout(process_timeout);
        session_ctx->set_priority((PriorityPacketQueueThread::QueuePriority)pkt.get_packet_priority());
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
        if (scan_param.get_data_mark_param().is_valid())
        {
            TBSYS_LOG(DEBUG,"mul_del::debug,UPS scan real data mark param[%s]!",
                      to_cstring(scan_param.get_data_mark_param()));
        }
        //add duyr 20160531:e
        if (OB_NEW_SCAN_REQUEST == pkt.get_packet_code())
        {
          new_scanner.reuse();
          common::ObRowDesc row_desc;
          if(OB_SUCCESS != (ret = ObNewScannerHelper::get_row_desc(scan_param, row_desc)))
          {
            TBSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
          }
          else
          {
            new_scanner.set_row_desc(row_desc);
            ret = UPS.get_table_mgr().new_scan(*session_ctx, scan_param, new_scanner, pkt.get_receive_ts(), process_timeout);
          }
#if 0
          if (OB_SUCCESS == ret)
          {
            ObUpsRow tmp_ups_row;
            tmp_ups_row.set_row_desc(row_desc);
            if (OB_SUCCESS != (ret = ObNewScannerHelper::print_new_scanner(new_scanner, tmp_ups_row, true)))
            {
              TBSYS_LOG(WARN, "print new scanner fail:ret[%d]", ret);
            }
          }
#endif
          if (OB_SUCCESS == ret)
          {
            UPS.response_scanner(ret, pkt, new_scanner, buffer);
          }
        }
        else
        {
          scanner.reset();
          ret = UPS.get_table_mgr().scan(*session_ctx, scan_param, scanner, pkt.get_receive_ts(), process_timeout);
          if (OB_SUCCESS == ret)
          {
            UPS.response_scanner(ret, pkt, scanner, buffer);
          }
        }
        FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "get from table mgr ret=%d", ret);
        //OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_SCAN_COUNT, 1);
        OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), SCAN, COUNT), 1);
        OB_STAT_INC(UPDATESERVER, UPS_STAT_SCAN_QTIME, session_ctx->get_start_handle_time() - session_ctx->get_session_start_time());
        //OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_SCAN_TIMEU, session_ctx->get_session_timeu());
        OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), SCAN, TIMEU), session_ctx->get_session_timeu());
        END_SAMPLE_SLOW_QUERY(session_ctx, pkt);
        thread_read_complete();
        session_ctx->set_last_active_time(tbsys::CTimeUtil::getTime());
        session_mgr_.revert_ctx(session_descriptor);
        session_mgr_.end_session(session_descriptor);
        log_scan_qps_();
      }
      if (OB_SUCCESS != ret)
      {
        UPS.response_result(ret, pkt);
      }
    }

    //add zhujun [transaction read uncommit] 2016/3/29
    void TransExecutor::handle_scan_trans_in_one(Task &task,
                            common::ObScanParam &scan_param,
                            common::ObScanner &scanner,
                            common::ObCellNewScanner &new_scanner,
                            common::ObDataBuffer &buffer)
    {
        //TBSYS_LOG(INFO, "TransExecutor::handle_scan_trans_in_one transaction id = %s",to_cstring(scan_param.get_trans_id()));
        //add zhujun
        common::ObPacket pkt = task.pkt;
        int end_session_ret = OB_SUCCESS;
        SessionGuard session_guard(session_mgr_, lock_mgr_, end_session_ret);
        const bool check_session_expired = true;
        //add:e
        int &ret = thread_errno();
        ret = OB_SUCCESS;
        uint32_t session_descriptor = scan_param.get_trans_id().descriptor_;
        RWSessionCtx* session_ctx = NULL;
//        ROSessionCtx *session_ctx = NULL;
        int64_t packet_timewait = (0 == pkt.get_source_timeout()) ?
                                  UPS.get_param().packet_max_wait_time :
                                  pkt.get_source_timeout();
        int64_t process_timeout = packet_timewait - QUERY_TIMEOUT_RESERVE;
        int64_t pos = pkt.get_buffer()->get_position();
        if (OB_SUCCESS != (ret = scan_param.deserialize(pkt.get_buffer()->get_data(),
                                                        pkt.get_buffer()->get_capacity(),
                                                        pos)))
        {
          TBSYS_LOG(WARN, "deserialize get_param fail ret=%d", ret);
        }
        else if (!UPS.can_serve_read_req(scan_param.get_is_read_consistency(), scan_param.get_version_range().get_query_version()))
        {
          if (REACH_TIME_INTERVAL(10 * 1000 * 1000))
          {
            TBSYS_LOG(WARN, "the scan request require consistency, ObiRole:%s RoleMgr:%s, query_version=%ld",
                      UPS.get_obi_role().get_role_str(), UPS.get_role_mgr().get_role_str(), scan_param.get_version_range().get_query_version());
          }
          ret = OB_NOT_MASTER;
        }
        /*
        else if (OB_SUCCESS != (ret = session_mgr_.begin_session(ST_READ_ONLY, pkt.get_receive_ts(), process_timeout, process_timeout, session_descriptor)))
        {
          TBSYS_LOG(WARN, "begin session fail ret=%d", ret);
        }
        else if (NULL == (session_ctx = session_mgr_.fetch_ctx<ROSessionCtx>(session_descriptor)))
        {
          TBSYS_LOG(WARN, "fetch ctx fail session_descriptor=%u", session_descriptor);
          ret = OB_ERR_UNEXPECTED;
        }*/
        else if (OB_SUCCESS != (ret = session_guard.fetch_session(scan_param.get_trans_id(), session_ctx, check_session_expired)))
        {
          TBSYS_LOG(WARN, "Session has been killed, error %d, \'%s\'", ret, to_cstring(scan_param.get_trans_id()));
        }
        else if (session_ctx->is_session_expired())
        {
          session_mgr_.revert_ctx(session_descriptor);
          session_ctx = NULL;
          ret = OB_TRANS_ROLLBACKED;
        }
        else
        {
          int64_t cur_time = tbsys::CTimeUtil::getTime();
          BEGIN_SAMPLE_SLOW_QUERY(session_ctx);
          FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "start handle scan, packet wait=%ld start_time=%ld timeout=%ld src=%s",
                        tbsys::CTimeUtil::getTime() - pkt.get_receive_ts(),
                        pkt.get_receive_ts(),
                        pkt.get_source_timeout(),
                        NULL == pkt.get_request() ? NULL : get_peer_ip(pkt.get_request()));
          thread_read_prepare();
          //add zhujun
          session_ctx->set_conflict_session_id(task.last_conflict_session_id);
          session_ctx->set_last_proc_time(cur_time);
          //add:e
          //add hbt:read_uncomomit
          //将事务开始时刻的已发布版本号记录下来
          int64_t trans_start_trans_id=session_ctx->get_trans_id();
          //让select语句读取当前时刻已提交的数据
          session_ctx->set_trans_id(session_mgr_.get_published_trans_id());
          // del by maosy [Delete_Update_Function_isolation_RC] 20161218
//add by hongchen [Delete_Update_Function_for_snpshot] 20161210
//          if (1 == scan_param.get_trans_id().read_times_ && session_ctx->get_trans_id_for_batach () == INT64_MAX)
//          {
//              session_ctx->set_trans_id_for_batach (session_ctx->get_trans_id ());
//          }
//          else if (2 == scan_param.get_trans_id().read_times_)
//          {
//              session_ctx->set_trans_id(session_ctx->get_trans_id_for_batach ());
//          }
//          else
//          {
//              session_ctx->set_trans_id_for_batach(INT64_MAX);
//          }
//add by hongchen  20161210
// del by maosy 20161218 e
          //add:e
          session_ctx->set_start_handle_time(tbsys::CTimeUtil::getTime());
          // add by maosy [Delete_Update_Function_isolation_RC] 20161222
          if(scan_param.get_trans_id ().read_times_ == FIRST_SELECT_READ )
          {
              if(session_ctx->get_start_time_for_batch () ==0)
              {
                  session_ctx->set_start_time_for_batch (session_ctx->get_trans_id ());
              }
              else
              {
                  session_ctx->set_new_transid ();
              }
              TBSYS_LOG(DEBUG,"start time = %ld,session = %p",session_ctx->get_start_time_for_batch (),session_ctx);
          }
          // add e
          session_ctx->set_stmt_start_time(pkt.get_receive_ts());
          session_ctx->set_stmt_timeout(process_timeout);
          session_ctx->set_priority((PriorityPacketQueueThread::QueuePriority)pkt.get_packet_priority());
          //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
          if (scan_param.get_data_mark_param().is_valid())
          {
              TBSYS_LOG(DEBUG,"mul_del::debug,UPS scan real data mark param[%s]!is in one",
                        to_cstring(scan_param.get_data_mark_param()));
          }
          //add duyr 20160531:e
          if (OB_NEW_SCAN_REQUEST == pkt.get_packet_code())
          {
            new_scanner.reuse();
            common::ObRowDesc row_desc;
            if(OB_SUCCESS != (ret = ObNewScannerHelper::get_row_desc(scan_param, row_desc)))
            {
              TBSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
            }
            else
            {
              new_scanner.set_row_desc(row_desc);
              ret = UPS.get_table_mgr().new_scan(*session_ctx, scan_param, new_scanner, pkt.get_receive_ts(), process_timeout);
            }
            if (OB_SUCCESS == ret)
            {
              UPS.response_scanner(ret, pkt, new_scanner, buffer);
            }
          }
          else
          {
            scanner.reset();
            ret = UPS.get_table_mgr().scan(*session_ctx, scan_param, scanner, pkt.get_receive_ts(), process_timeout);
            if (OB_SUCCESS == ret)
            {
              UPS.response_scanner(ret, pkt, scanner, buffer);
            }
          }
          FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "get from table mgr ret=%d", ret);
          //OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_SCAN_COUNT, 1);
          OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), SCAN, COUNT), 1);
          OB_STAT_INC(UPDATESERVER, UPS_STAT_SCAN_QTIME, session_ctx->get_start_handle_time() - session_ctx->get_session_start_time());
          //OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_SCAN_TIMEU, session_ctx->get_session_timeu());
          OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), SCAN, TIMEU), session_ctx->get_session_timeu());
          END_SAMPLE_SLOW_QUERY(session_ctx, pkt);
          thread_read_complete();
          session_ctx->set_last_active_time(tbsys::CTimeUtil::getTime());
          //add:hbt:read_uncommit
          //将事务开始时刻的已发布版本号放回到ctx中
          session_ctx->set_trans_id(trans_start_trans_id);
          session_guard.revert();
          //add:e
          //delete zhujun
          //session_mgr_.end_session(session_descriptor);
          log_scan_qps_();
        }
        if (OB_SUCCESS != ret)
        {
          UPS.response_result(ret, pkt);
        }

    }

    void TransExecutor::handle_kill_zombie_()
    {
      const bool force = false;
      session_mgr_.kill_zombie_session(force);
    }

    void TransExecutor::handle_show_sessions_(ObPacket &pkt,
                                              ObNewScanner &scanner,
                                              ObDataBuffer &buffer)
    {
      scanner.reuse();
      session_mgr_.show_sessions(scanner);
      UPS.response_scanner(OB_SUCCESS, pkt, scanner, buffer);
    }

    void TransExecutor::handle_kill_session_(ObPacket &pkt)
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      uint32_t session_descriptor = INVALID_SESSION_DESCRIPTOR;
      int64_t pos = pkt.get_buffer()->get_position();
      if (OB_SUCCESS != (ret = serialization::decode_vi32(pkt.get_buffer()->get_data(),
                                                          pkt.get_buffer()->get_capacity(),
                                                          pos,
                                                          (int32_t*)&session_descriptor)))
      {
        TBSYS_LOG(WARN, "deserialize session descriptor fail ret=%d", ret);
      }
      else
      {
        ret = session_mgr_.kill_session(session_descriptor);
        TBSYS_LOG(INFO, "kill session ret=%d sd=%u", ret, session_descriptor);
      }
      UPS.response_result(ret, pkt);
    }

    void *TransExecutor::on_trans_begin()
    {
      TransParamData *ret = NULL;
      void *buffer = ob_malloc(sizeof(TransParamData), ObModIds::OB_UPS_PARAM_DATA);
      if (NULL != buffer)
      {
        ret = new(buffer) TransParamData();
        ret->buffer.set_data(ret->cbuffer, sizeof(ret->cbuffer));
      }
      return ret;
    }

    void TransExecutor::on_trans_end(void *ptr)
    {
      if (NULL != ptr)
      {
        ob_free(ptr);
        ptr = NULL;
      }
    }

    void TransExecutor::log_scan_qps_()
    {
      static volatile uint64_t counter = 0;
      static const int64_t mod = 100000;
      static int64_t last_report_ts = 0;
      if (1 == (ATOMIC_ADD(&counter, 1) % mod))
      {
        int64_t cur_ts = tbsys::CTimeUtil::getTime();
        TBSYS_LOG(INFO, "SCAN total=%lu, SCAN_QPS=%ld", counter, 1000000 * mod/(cur_ts - last_report_ts));
        last_report_ts = cur_ts;
      }
    }

    void TransExecutor::log_get_qps_()
    {
      static volatile uint64_t counter = 0;
      static const int64_t mod = 100000;
      static int64_t last_report_ts = 0;
      if (1 == (ATOMIC_ADD(&counter, 1) % mod))
      {
        int64_t cur_ts = tbsys::CTimeUtil::getTime();
        TBSYS_LOG(INFO, "GET total=%lu, GET_QPS=%ld", counter, 1000000 * mod/(cur_ts - last_report_ts));
        last_report_ts = cur_ts;
      }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    bool is_write_packet(ObPacket& pkt)
    {
      bool ret = false;
      switch(pkt.get_packet_code())
      {
        case OB_MS_MUTATE:
        case OB_WRITE:
        case OB_PHY_PLAN_EXECUTE:
        case OB_TRUNCATE_TABLE: /*add zhaoqiong [Truncate Table]:20170519*/
        case OB_END_TRANSACTION:
          ret = true;
          break;
        default:
          ret = false;
      }
      return ret;
    }

    int64_t TransExecutor::get_commit_queue_len()
    {
      return session_mgr_.get_trans_seq().get_seq() - TransCommitThread::task_queue_.get_seq() + 1;
    }

    int64_t TransExecutor::get_seq(void* ptr)
    {
      int64_t seq = 0;
      Task& task = *((Task*)ptr);
      int64_t trans_id = tbsys::CTimeUtil::getTime();
      int ret = OB_SUCCESS;
      seq = session_mgr_.get_trans_seq().next(trans_id);
      //add zhaoqiong [Truncate Table]:20170704:b
      bool is_conflict = false;
      uint64_t table_id = OB_INVALID_ID;
      //add:e
      if (NULL == ptr)
      {
        TBSYS_LOG(ERROR, "commit queue, NULL ptr, will kill self");
        kill(getpid(), SIGTERM);
      }
      else if (is_write_packet(task.pkt))
      {
        {
          SessionGuard session_guard(session_mgr_, lock_mgr_, ret);
          RWSessionCtx* session_ctx = NULL;
          if (OB_SUCCESS != (ret = session_guard.fetch_session(task.sid, session_ctx)))
          {
            TBSYS_LOG(ERROR, "fetch_session(sid=%s)=>%d", to_cstring(task.sid), ret);
          }
          //add shili [LONG_TRANSACTION_LOG]  20160926:b
          //else if (!UPS.get_log_mgr().check_log_size(session_ctx->get_ups_mutator().get_serialize_size()))
          //{
          //  ret = OB_LOG_TOO_LARGE;
          //  TBSYS_LOG(ERROR, "mutator.size[%ld] too large",
          //            session_ctx->get_ups_mutator().get_serialize_size());
          //}
          else if (!UPS.get_log_mgr().check_mutator_size(session_ctx->get_ups_mutator().get_serialize_size(),
                                                         UPS.get_param().max_mutator_size))
          {
            ret = OB_LOG_TOO_LARGE;
            TBSYS_LOG(ERROR, "mutator.size[%ld] too large",
                      session_ctx->get_ups_mutator().get_serialize_size());
          }
          //add zhaoqiong [Truncate Table]:20170704:b
          else if (OB_SUCCESS == (ret = session_ctx->check_truncate_conflict(table_id, is_conflict)) && is_conflict)
          {
              TBSYS_LOG(ERROR, "check_truncate_conflict[table_id=%ld], session rollback[ret=%d]",table_id,ret);
              ret = OB_UD_PARALLAL_DATA_NOT_SAFE;
          }
          if (ret != OB_SUCCESS)
          {
              //do nothing
          }
          //add e
          else if (session_ctx->is_frozen())
          {
            task.sid.reset();
            TBSYS_LOG(ERROR, "session stat is frozen, maybe request duplicate, sd=%u",
                      session_ctx->get_session_descriptor());
          }
          else
          {
            session_ctx->set_trans_id(trans_id);
            session_ctx->get_checksum();
          }
        }
        if (OB_SUCCESS == ret && task.sid.is_valid()
            && OB_SUCCESS != (ret = session_mgr_.precommit(task.sid.descriptor_)))
        {
          TBSYS_LOG(ERROR, "precommit(%s)=>%d", to_cstring(task.sid), ret);
        }
        if (OB_SUCCESS != ret)
        {
          task.sid.reset();
        }
      }
      return seq;
    }

    void TransExecutor::handle_commit(void *ptask, void *pdata)
    {
      int ret = OB_SUCCESS;
      thread_errno() = OB_SUCCESS;
      bool release_task = true;
      Task *task = (Task*)ptask;
      CommitParamData *param = (CommitParamData*)pdata;
      if (NULL == task)
      {
        TBSYS_LOG(WARN, "null pointer task=%p", task);
      }
      else if (NULL == param)
      {
        TBSYS_LOG(WARN, "null pointer param data pdata=%p src=%s",
                  pdata, inet_ntoa_r(task->src_addr));
        ret = OB_ERR_UNEXPECTED;
      }
      else if (0 > task->pkt.get_packet_code()
              || OB_PACKET_NUM <= task->pkt.get_packet_code())
      {
        TBSYS_LOG(ERROR, "unknown packet code=%d src=%s",
                  task->pkt.get_packet_code(), inet_ntoa_r(task->src_addr));
        ret = OB_UNKNOWN_PACKET;
      }
      else if (OB_SUCCESS != (ret = handle_flushed_log_()))
      {
        TBSYS_LOG(ERROR, "handle_flushed_clog()=>%d", ret);
      }
      else
      {
        //忽略log自带的前两个字段，trace id和chid，以后续的trace id和chid为准
        PROFILE_LOG(DEBUG, TRACE_ID SOURCE_CHANNEL_ID PCODE WAIT_TIME_US_IN_COMMIT_QUEUE,
                    (task->pkt).get_trace_id(),
                    (task->pkt).get_channel_id(),
                    (task->pkt).get_packet_code(),
                    tbsys::CTimeUtil::getTime() - (task->pkt).get_receive_ts());
        if (wait_for_commit_(task->pkt.get_packet_code()))
        {
          {
            ObSpinLockGuard guard(write_clog_mutex_);
            commit_log_(); // 把日志缓存区刷盘，并且提交end_session和响应客户端的任务
          }
          if (is_only_master_can_handle(task->pkt.get_packet_code()))
          {
            FakeWriteGuard guard(session_mgr_);
            if (!guard.is_granted())
            {
              ret = OB_NOT_MASTER;
              TBSYS_LOG(WARN, "only master can handle pkt_code=%d", task->pkt.get_packet_code());
            }
            else
            {
              ObSpinLockGuard guard(write_clog_mutex_);
              // 这个分支不能加FakeWriteGuard, 否则备机收日志就没不能处理了
              release_task = commit_handler_[task->pkt.get_packet_code()](*this, *task, *param);
            }
          }
          else
          {
            ObSpinLockGuard guard(write_clog_mutex_);
            release_task = commit_handler_[task->pkt.get_packet_code()](*this, *task, *param);
          }
        }
        else
        {
          ObSpinLockGuard guard(write_clog_mutex_);
          release_task = commit_handler_[task->pkt.get_packet_code()](*this, *task, *param);
        }
      }
      if (NULL != task)
      {
        if (OB_SUCCESS != ret
            || OB_SUCCESS != thread_errno())
        {
          TBSYS_LOG(WARN, "process fail ret=%d pcode=%d src=%s",
                    (OB_SUCCESS != ret) ? ret : thread_errno(), task->pkt.get_packet_code(), inet_ntoa_r(task->src_addr));
        }
        if (OB_SUCCESS != ret)
        {
          UPS.response_result(ret, task->pkt);
        }
        if (release_task)
        {
          allocator_.free(task);
          task = NULL;
        }
      }
    }

    int TransExecutor::handle_write_commit_(Task &task)
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      if (!task.sid.is_valid())
      {
        ret = OB_TRANS_ROLLBACKED;
        TBSYS_LOG(WARN, "session is rollbacked, maybe precommit faile");
      }
      else
      {
        SessionGuard session_guard(session_mgr_, lock_mgr_, ret);
        RWSessionCtx* session_ctx = NULL;
        if (OB_SUCCESS != (ret = session_guard.fetch_session(task.sid, session_ctx)))
        {
          TBSYS_LOG(ERROR, "fetch_session(sid=%s)=>%d", to_cstring(task.sid), ret);
        }
        else if (OB_SUCCESS != (ret = session_mgr_.update_commited_trans_id(session_ctx)))
        {
          TBSYS_LOG(ERROR, "session_mgr.update_commited_trans_id(%s)=>%d", to_cstring(*session_ctx), ret);
        }
        else
        {
          if (0 != session_ctx->get_last_proc_time())
          {
            int64_t cur_time = tbsys::CTimeUtil::getTime();
            OB_STAT_INC(UPDATESERVER, UPS_STAT_TRANS_CTIME, cur_time - session_ctx->get_last_proc_time());
            session_ctx->set_last_proc_time(cur_time);
          }

          batch_start_time() = (0 == batch_start_time()) ? tbsys::CTimeUtil::getTime() : batch_start_time();
          int64_t cur_timestamp = session_ctx->get_trans_id();
          session_ctx->get_uc_info().uc_checksum = ob_crc64(session_ctx->get_uc_info().uc_checksum, &cur_timestamp, sizeof(cur_timestamp));
          if (cur_timestamp <= 0)
          {
            TBSYS_LOG(ERROR, "session_ctx.trans_id=%ld <= 0, will kill self", cur_timestamp);
            kill(getpid(), SIGTERM);
          }
          FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "trans_checksum=%lu trans_id=%ld",
                         session_ctx->get_uc_info().uc_checksum, cur_timestamp);
          //mod shili [LONG_TRANSACTION_LOG]  20160926:b
          //int ret = fill_log_(task, *session_ctx);
          //if (OB_SUCCESS != ret)
          //{
          //  if (OB_EAGAIN != ret)
          //  {
          //    TBSYS_LOG(WARN, "fill log fail ret=%d %s", ret, to_cstring(task.sid));
          //  }
          //  else if (OB_SUCCESS != (ret = commit_log_()))
          //  {
          //    TBSYS_LOG(WARN, "commit log fail ret=%d %s", ret, to_cstring(task.sid));
          //  }
          //  else if (OB_SUCCESS != (ret = fill_log_(task, *session_ctx)))
          //  {
          //    TBSYS_LOG(ERROR, "second fill log fail ret=%d %s serialize_size=%ld uncommited_number=%ld",
          //              ret, to_cstring(task.sid),
          //              session_ctx->get_ups_mutator().get_serialize_size(),
          //              uncommited_session_list_.size());
          //  }
          //  else
          //  {
          //    TBSYS_LOG(INFO, "second fill log succ %s", to_cstring(task.sid));
          //  }
          //}
          ret = fill_session_log_(task, *session_ctx);
          if(ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "fill session log fail,ret=%d,tid=%s", ret, to_cstring(task.sid));
          }
          //add e
        }
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "unexpect error, ret=%d %s, will kill self", ret, to_cstring(task.sid));
          kill(getpid(), SIGTERM);
        }
      }
      //modify wangdonghui [ups_replication] 20170323 :b TO_DO ups have different wait sync type
      /*
      if(cur_time_us - last_gather_time_ > 1000*1000)//800ms收集一次
      {
          last_gather_time_ = cur_time_us;
          if((average_batch_size_[count_] == 1 && !need_refresh_)||//负载变化
            (cur_time_us - last_refresh_time_ > 60*1000*1000 && !need_refresh_)||//1分钟定时刷新
            (send_interval_us_ - best_send_interval_us_ < 10))
          {
             count_ = 0;
             average_batch_size_[0] = 0;
             send_interval_us_ = 0;
             need_refresh_ = true;
             best_batch_size_ = 0;
          }
          if(send_interval_us_ < 5000 && need_refresh_)
          {
              if(best_batch_size_ < average_batch_size_[count_] * (send_interval_us_==0?0:1000000/send_interval_us_))
              {
                  best_batch_size_ = average_batch_size_[count_]* (send_interval_us_==0?0:1000000/send_interval_us_);
                  best_send_interval_us_ = send_interval_us_;
                  best_message_residence_time_ = message_residence_time_us_;
              }
              send_interval_us_ += UPS.get_wait_sync_type() == ObUpsLogMgr::WAIT_FLUSH ? 500 : 250;
              average_batch_size_[++count_] = 0;
          }
          else
          {
              if(need_refresh_)
              {
                  last_refresh_time_ = tbsys::CTimeUtil::getTime();
              }
              send_interval_us_ = best_send_interval_us_;
              if(!need_refresh_)
              {
                  best_batch_size_ = average_batch_size_[count_] * (send_interval_us_==0?0:1000000/send_interval_us_);
              }
              average_batch_size_[count_] = 0;
              need_refresh_ = false;
              //TBSYS_LOG(INFO, "WDH_TEST::message_residence_time_us_(%ld) send_time_interval(%ld) send_interval_us_(%ld) log_size(%ld)",
              //          message_residence_time_us_, cur_time_us - last_commit_log_time_us_, send_interval_us_,
              //          average_batch_size_[count_]);
          }
      }
      if(OB_SUCCESS == ret && cur_time_us - last_commit_log_time_us_ >= send_interval_us_)
      */
      if( UPS.get_wait_sync_type() == ObUpsLogMgr::WAIT_FLUSH )
      {
        //average_batch_size_[count_] = (average_batch_size_[count_] == 0 ? UPS.get_log_mgr().log_size()
        //                          : (average_batch_size_[count_] * 50 + UPS.get_log_mgr().log_size()) / 51);
        //TBSYS_LOG(INFO, "WDH_TEST::message_residence_time_us_(%ld) send_time_interval(%ld) send_interval_us_(%ld) log_size(%ld)",
        //          message_residence_time_us_, cur_time_us - last_commit_log_time_us_, send_interval_us_,
        //          average_batch_size_[count_]);
          int64_t cur_time_us = tbsys::CTimeUtil::getTime();
          if(OB_SUCCESS == ret
             && ((message_residence_time_us_ < message_residence_max_us_?
                 message_residence_time_us_: message_residence_max_us_) <= cur_time_us - last_commit_log_time_us_))

        {
            //TBSYS_LOG(INFO, "WDH_TEST::message_residence_time_us_(%ld) send_time_interval(%ld) log_size(%ld)",
            //                  message_residence_time_us_, cur_time_us - last_commit_log_time_us_,
            //                  UPS.get_log_mgr().log_size());
            ret = commit_log_();
            last_commit_log_time_us_ = tbsys::CTimeUtil::getTime();
        }
      }
      else
      {
        int64_t cur_time_us = tbsys::CTimeUtil::getTime();
        if (OB_SUCCESS == ret
            && (0 == TransCommitThread::get_queued_num()//next is ready 6000左右，700
                  || cur_time_us - last_commit_log_time_us_ >= message_residence_time_us_ * 5))
                  //|| MAX_BATCH_NUM <= uncommited_session_list_.size()))
        {
            //TBSYS_LOG(INFO, "WDH_TEST::message_residence_time_us_(%ld) send_time_interval(%ld) log_size(%ld)",
            //                  message_residence_time_us_, cur_time_us - last_commit_log_time_us_,
            //                  UPS.get_log_mgr().log_size());
            ret = commit_log_();
            last_commit_log_time_us_ = tbsys::CTimeUtil::getTime();
        }
      }
      //mod :e
      if (OB_SUCCESS != ret)
      {
        UPS.response_result(ret, task.pkt);
      }
      return ret;
    }

    void TransExecutor::on_commit_idle()
    {
      commit_log_();
      handle_flushed_log_();
      try_submit_auto_freeze_();
    }

    int TransExecutor::handle_response(ObAckQueue::WaitNode& node)
    {
      int ret = OB_SUCCESS;
      TBSYS_LOG(TRACE, "send_log response: %s", to_cstring(node));
      if (OB_SUCCESS != node.err_)
      {
        TBSYS_LOG(WARN, "send_log %s faile", to_cstring(node));
        //mod wangjiahao [Paxos ups_replication] 20150731 :b
        if (OB_LOG_NOT_SYNC != node.err_ && UPS.get_slave_mgr().need_to_del(node.server_, UPS.get_delete_ups_wait_time()))
        { //means that UPS offline or network err
          UPS.get_slave_mgr().delete_server(node.server_);
        }
        else
        {
          //do nothing
        }
        //mod :e
      }
      else
      {
        //add wangdonghui [ups_replication] one ups: writing to disk is slower than handle response 20170712 :b
        //fix bug: commit point is greater than max log id
        /*if(!node.server_.is_valid())
        {
          UPS.get_slave_mgr().set_last_reply_log_seq(node.server_, min(node.end_seq_, UPS.get_log_mgr().get_master_log_seq()));
        }
        else*/
        //add :e
        //add wangjiahao [Paxos ups_replication]  20150601:b
        UPS.get_slave_mgr().set_last_reply_log_seq(node.server_, node.end_seq_);
        //add:e
        UPS.get_log_mgr().get_clog_stat().add_net_us(node.start_seq_, node.end_seq_, node.get_delay());
        //add wangdonghui [ups_replication] 20170323 :b
        if(-1 == node.message_residence_time_us_) {}
        else
        {
            message_residence_time_us_ = (message_residence_time_us_ >> 1) + (node.message_residence_time_us_ >> 1);
            //TBSYS_LOG(INFO, "WDH_TEST::time: %ld, node: %ld", message_residence_time_us_, node.message_residence_time_us_);
        }
        //add :e
      }
      return ret;
    }

    int TransExecutor::on_ack(ObAckQueue::WaitNode& node)
    {
      int ret = OB_SUCCESS;
      TBSYS_LOG(TRACE, "on_ack: %s", to_cstring(node));
      if (OB_SUCCESS != (ret = TransCommitThread::push(&nop_task_)))
      {
        TBSYS_LOG(ERROR, "push nop_task to wakeup commit thread fail, %s, ret=%d", to_cstring(node), ret);
      }
      return ret;
    }
    //add wangjiahao [Paxos ups_replication]  20150601:b
    int TransExecutor::get_quorum_seq(int64_t &quorum_seq) const
    {
        int ret = OB_SUCCESS;
        if (UPS.get_role_mgr().get_role() == ObUpsRoleMgr::MASTER)
        {
            ret = UPS.get_slave_mgr().get_quorum_seq(quorum_seq);
        }
        else
        {
            ret = OB_NOT_MASTER;
        }
        return ret;
    }
    //add:e
    int TransExecutor::handle_flushed_log_()
    {
      int err = OB_SUCCESS;
      int64_t flushed_clog_id = UPS.get_log_mgr().get_flushed_clog_id();
      //add wangjiahao [Paxos ups_replication_tmplog] 20150629 :b
      if (!UPS.get_role_mgr().is_master())
      {
      }
      else if (OB_SUCCESS != UPS.get_log_mgr().write_cpoint(flushed_clog_id, true)) //function handle_flushed_log_() is a sigle thread method
      {
        TBSYS_LOG(WARN, "master write commit point fail");
      }
      //add :e
      int64_t flush_seq = 0;
      Task *task = NULL;
      while(true)
      {
        if (OB_SUCCESS != (err = flush_queue_.tail(flush_seq, (void*&)task))
            && OB_ENTRY_NOT_EXIST != err)
        {
          TBSYS_LOG(ERROR, "flush_queue_.tail()=>%d, will kill self", err);
          kill(getpid(), SIGTERM);
        }
        else if (OB_ENTRY_NOT_EXIST == err)
        {
          err = OB_SUCCESS;
          break;
        }
        else if (flush_seq > flushed_clog_id)
        {
          break;
        }
        else
        {
          int ret_ok = OB_SUCCESS;
          SessionGuard session_guard(session_mgr_, lock_mgr_, ret_ok);
          RWSessionCtx *session_ctx = NULL;
          if (OB_SUCCESS != (err = session_guard.fetch_session(task->sid, session_ctx)))
          {
            TBSYS_LOG(ERROR, "unexpected fetch_session fail ret=%d %s, will kill self", err, to_cstring(task->sid));
            kill(getpid(), SIGTERM);
          }
          else
          {
            task->pkt.set_packet_code(OB_COMMIT_END);
            session_guard.revert();
            session_mgr_.end_session(session_ctx->get_session_descriptor(), false, true, BaseSessionCtx::ES_PUBLISH);
            if (OB_SUCCESS != (err = CommitEndHandlePool::push(task)))
            {
              TBSYS_LOG(ERROR, "push(task=%p)=>%d, will kill self", task, err);
              kill(getpid(), SIGTERM);
            }
            else if (OB_SUCCESS != (err = flush_queue_.pop()))
            {
              TBSYS_LOG(ERROR, "flush_queue.consume_tail()=>%d", err);
            }
          }
        }
      }
      return err;
    }


    //add shili [LONG_TRANSACTION_LOG]  20160926:b
    int TransExecutor::fill_session_log_(Task &task, RWSessionCtx &session_ctx)
    {
      int ret= OB_SUCCESS;
      int64_t mutator_size = session_ctx.get_ups_mutator().get_serialize_size();
      if(UPS.get_log_mgr().is_normal_mutator_size(mutator_size))//small  transaction log
      {
        TBSYS_LOG(DEBUG,"mutator size:%ld",mutator_size);
        if(OB_SUCCESS!=(ret = fill_normal_log_(task, session_ctx)))
        {
          TBSYS_LOG(WARN, "fill small tran log fail,ret=%d", ret);
        }
      }
      else//long  transaction log
      {
        TBSYS_LOG(DEBUG,"big log mutator size,%ld",mutator_size);
        if(UPS.get_param().max_mutator_size <= mutator_size*2)
        {
          TBSYS_LOG(INFO,"big log mutator size,%ld",mutator_size);
        }

        if(OB_SUCCESS!=(ret = fill_big_log_(task, session_ctx)))
        {
          TBSYS_LOG(WARN, "fill long tran log fail,ret=%d", ret);
        }
      }
      return ret;
    }

    /*@berif  写 日志小于2M事务的 log 到 log buffer*/
    int TransExecutor::fill_normal_log_(Task &task, RWSessionCtx &session_ctx)
    {
      int ret= OB_SUCCESS;
      ret = fill_log_(task, session_ctx);
      if (OB_SUCCESS != ret)
      {
        if (OB_EAGAIN != ret)
        {
          TBSYS_LOG(WARN, "fill log fail ret=%d %s", ret, to_cstring(task.sid));
        }
        else if (OB_SUCCESS != (ret = commit_log_()))
        {
          TBSYS_LOG(WARN, "commit log fail ret=%d %s", ret, to_cstring(task.sid));
        }
        else if (OB_SUCCESS != (ret = fill_log_(task, session_ctx)))
        {
          TBSYS_LOG(ERROR, "second fill log fail ret=%d %s serialize_size=%ld uncommited_number=%ld",
                    ret, to_cstring(task.sid),
                    session_ctx.get_ups_mutator().get_serialize_size(),
                    uncommited_session_list_.size());
        }
        else
        {
          TBSYS_LOG(INFO, "second fill log succ %s", to_cstring(task.sid));
        }
      }
      return ret;
    }

    /*@berif 写入 long trans 全部log*/
    int TransExecutor::fill_big_log_(Task &task, RWSessionCtx &session_ctx)
    {
      int ret = OB_SUCCESS;
      MemTable *mt = NULL;
      char *buf = NULL;
      int64_t mutator_size = 0;
      UPS.get_big_log_writer().reset();
      if(0 != session_ctx.get_ups_mutator().get_mutator().size() ||
         (OB_PHY_PLAN_EXECUTE != task.pkt.get_packet_code() && OB_END_TRANSACTION != task.pkt.get_packet_code()))
      {
        if(NULL == (mt = session_ctx.get_uc_info().host))
        {
          ret = OB_ERR_UNEXPECTED;
        }
        else
        {
          int64_t uc_checksum = 0;
          uc_checksum = mt->calc_uncommited_checksum(session_ctx.get_uc_info().uc_checksum);
          int64_t old = uc_checksum;
          uc_checksum ^= session_ctx.get_checksum();
          session_ctx.get_ups_mutator().set_mutate_timestamp(session_ctx.get_trans_id());
          session_ctx.get_ups_mutator().set_memtable_checksum_before_mutate(mt->get_uncommited_checksum());
          session_ctx.get_ups_mutator().set_memtable_checksum_after_mutate(uc_checksum);
          uc_checksum = old;

          mutator_size = session_ctx.get_ups_mutator().get_serialize_size();
          if(NULL == (buf = (char *) ob_malloc(mutator_size, ObModIds::OB_UPS_BIG_LOG_DATA))) //需要手动释放内存
          {
            ret = OB_MEM_OVERFLOW;
            TBSYS_LOG(ERROR, "ob_malloc alloc(%ld)=>%d", mutator_size, ret);
          }
          else if(OB_SUCCESS != (ret = session_ctx.get_ups_mutator().pre_serialize(buf, mutator_size)))
          {
            TBSYS_LOG(WARN, "pre serialize  ups mutator fail,ret=%d", ret);
          }
          else
          {
            UPS.get_big_log_writer().assign(buf, (int32_t) mutator_size, OB_LOG_UPS_MUTATOR);
            if(OB_SUCCESS != (ret = UPS.get_big_log_writer().write_big_log(task.sid, session_ctx)))
            {
              TBSYS_LOG(WARN, "write big log fail,ret=%d", ret);
            }
            else
            {
              session_ctx.get_uc_info().uc_checksum = uc_checksum;
              mt->update_uncommited_checksum(session_ctx.get_uc_info().uc_checksum);
            }
          }

          if(NULL != buf)
          {
            ob_free(buf);//释放申请的内存
            buf = NULL;
            UPS.get_big_log_writer().assign(NULL,0);//ptr_指针为零
          }
        }
      }
      else
      {
        session_ctx.get_uc_info().host = NULL;
      }
      if(OB_SUCCESS == ret)
      {
        ObLogCursor filled_cursor;
        if(OB_SUCCESS != (ret = UPS.get_log_mgr().get_filled_cursor(filled_cursor)))
        {
          TBSYS_LOG(ERROR, "get_fill_cursor()=>%d", ret);
        }
        else if(0 != flush_queue_.push(filled_cursor.log_id_, &task))
        {
          ret = (OB_SUCCESS == ret) ? OB_MEM_OVERFLOW : ret;
          TBSYS_LOG(ERROR, "unexpected push task to uncommited_session_list fail list_size=%ld, will kill self",
                    uncommited_session_list_.size());
          kill(getpid(), SIGTERM);
        }
        else
        {
          // 保证在flush commit log成功后不会被kill掉  todo shili 是否需要重新设置
          session_ctx.set_frozen();
        }
      }
      FILL_TRACE_BUF(session_ctx.get_tlog_buffer(), "checksum=%lu affected_rows=%ld ret=%d",
                     (NULL == mt) ? 0 : mt->get_uncommited_checksum(), session_ctx.get_ups_result().get_affected_rows(),
                     ret);
      return ret;
    }
    //add e



    int TransExecutor::fill_log_(Task &task, RWSessionCtx &session_ctx)
    {
      int ret = OB_SUCCESS;
      MemTable *mt = NULL;
      if (0 != session_ctx.get_ups_mutator().get_mutator().size()
          || (OB_PHY_PLAN_EXECUTE != task.pkt.get_packet_code()
              && OB_END_TRANSACTION != task.pkt.get_packet_code()))
      {
        if (NULL == (mt = session_ctx.get_uc_info().host))
        {
          ret = OB_ERR_UNEXPECTED;
        }
        else
        {
          int64_t uc_checksum = 0;
          uc_checksum = mt->calc_uncommited_checksum(session_ctx.get_uc_info().uc_checksum);
          int64_t old = uc_checksum;
          uc_checksum ^= session_ctx.get_checksum();
          session_ctx.get_ups_mutator().set_mutate_timestamp(session_ctx.get_trans_id());
          session_ctx.get_ups_mutator().set_memtable_checksum_before_mutate(mt->get_uncommited_checksum());
          session_ctx.get_ups_mutator().set_memtable_checksum_after_mutate(uc_checksum);
          uc_checksum = old;

          ret = UPS.get_table_mgr().fill_commit_log(session_ctx.get_ups_mutator(), session_ctx.get_tlog_buffer());
          if (OB_SUCCESS == ret)
          {
            session_ctx.get_uc_info().uc_checksum = uc_checksum;
            mt->update_uncommited_checksum(session_ctx.get_uc_info().uc_checksum);
          }
        }
      }
      else
      {
        session_ctx.get_uc_info().host = NULL;
      }
      if (OB_SUCCESS == ret)
      {
        ObLogCursor filled_cursor;
        if (OB_SUCCESS != (ret = UPS.get_log_mgr().get_filled_cursor(filled_cursor)))
        {
          TBSYS_LOG(ERROR, "get_fill_cursor()=>%d", ret);
        }
        else if (0 != flush_queue_.push(filled_cursor.log_id_, &task))
        {
          ret = (OB_SUCCESS == ret) ? OB_MEM_OVERFLOW : ret;
          TBSYS_LOG(ERROR, "unexpected push task to uncommited_session_list fail list_size=%ld, will kill self", uncommited_session_list_.size());
          kill(getpid(), SIGTERM);
        }
        else
        {
          // 保证在flush commit log成功后不会被kill掉
          session_ctx.set_frozen();
        }
      }
      FILL_TRACE_BUF(session_ctx.get_tlog_buffer(), "checksum=%lu affected_rows=%ld ret=%d",
                    (NULL == mt) ? 0 : mt->get_uncommited_checksum(),
                    session_ctx.get_ups_result().get_affected_rows(), ret);
      return ret;
    }

    int TransExecutor::handle_commit_end_(Task &task, ObDataBuffer &buffer)
    {
      int ret = OB_SUCCESS;
      {
        int ret_ok = OB_SUCCESS;
        SessionGuard session_guard(session_mgr_, lock_mgr_, ret_ok);
        RWSessionCtx *session_ctx = NULL;
        if (OB_SUCCESS != (ret = session_guard.fetch_session(task.sid, session_ctx)))
        {
          TBSYS_LOG(ERROR, "unexpected fetch_session fail ret=%d %s, will kill self", ret, to_cstring(task.sid));
          kill(getpid(), SIGTERM);
        }
        else
        {

          session_ctx->get_ups_result().serialize(buffer.get_data(),
                                                  buffer.get_capacity(),
                                                  buffer.get_position());
        }
      }
      bool rollback = false;
      if (OB_SUCCESS != ret)
      {}
      else if (OB_SUCCESS != (ret = session_mgr_.end_session(task.sid.descriptor_, rollback)))
      {
        TBSYS_LOG(ERROR, "unexpected end_session fail ret=%d %s, will kill self", ret, to_cstring(task.sid));
        kill(getpid(), SIGTERM);
      }
      if (OB_SUCCESS == ret)
      {
        UPS.response_buffer(ret, task.pkt, buffer); // phyplan的话不能多线程执行
      }
      else
      {
        UPS.response_result(ret, task.pkt);
      }
      return ret;
    }

    int TransExecutor::commit_log_()
    {
      int ret = OB_SUCCESS;
      int64_t end_log_id = 0;
      //modify wangdonghui [ups_replication] 20170217 :b
      //if (0 < flush_queue_.size())
      if (0 < flush_queue_.size() && ObUpsRoleMgr::MASTER == UPS.get_role_mgr().get_role())
      //modify :e
      {
        CLEAR_TRACE_BUF(TraceLog::get_logbuffer());
        ret = UPS.get_log_mgr().async_flush_log(end_log_id, TraceLog::get_logbuffer());
        /*
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "flush commit log fail ret=%d uncommited_number=%ld, will kill self", ret, uncommited_session_list_.size());
          kill(getpid(), SIGTERM);
        }
        TBSYS_TRACE_LOG("[GROUP_COMMIT] %s", TraceLog::get_logbuffer().buffer);
        bool rollback = (OB_SUCCESS != ret);
        int64_t i = 0;
        ObList<Task*>::iterator iter;
        for (iter = uncommited_session_list_.begin(); iter != uncommited_session_list_.end(); iter++, i++)
        {
          Task *task = *iter;
          task->pkt.set_packet_code(OB_COMMIT_END);
          if (OB_SUCCESS != (ret = CommitEndHandlePool::push(task)))
          {
            TBSYS_LOG(ERROR, "push(task=%p)=>%d, will kill self", task, ret);
            kill(getpid(), SIGTERM);
          }
          continue;
          if (NULL == task)
          {
            TBSYS_LOG(ERROR, "unexpected task null pointer batch=%ld, will kill self", uncommited_session_list_.size());
            kill(getpid(), SIGTERM);
          }
          else
          {
            {
              int ret_ok = OB_SUCCESS;
              SessionGuard session_guard(session_mgr_, lock_mgr_, ret_ok);
              RWSessionCtx *session_ctx = NULL;
              if (OB_SUCCESS != (ret = session_guard.fetch_session(task->sid, session_ctx)))
              {
                TBSYS_LOG(ERROR, "unexpected fetch_session fail ret=%d %s, will kill self", ret, to_cstring(task->sid));
                kill(getpid(), SIGTERM);
              }
              else
              {
                FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "%sbatch=%ld:%ld", TraceLog::get_logbuffer().buffer, i, uncommited_session_list_.size());
                ups_result_buffer_.set_data(ups_result_memory_, OB_MAX_PACKET_LENGTH);
                session_ctx->get_ups_result().serialize(ups_result_buffer_.get_data(),
                                                        ups_result_buffer_.get_capacity(),
                                                        ups_result_buffer_.get_position());
              }
            }
            if (OB_SUCCESS != (ret = session_mgr_.end_session(task->sid.descriptor_, rollback)))
            {
              TBSYS_LOG(ERROR, "unexpected end_session fail ret=%d %s, will kill self", ret, to_cstring(task->sid));
              kill(getpid(), SIGTERM);
            }
            ret = rollback ? OB_TRANS_ROLLBACKED : ret;
            if (OB_PHY_PLAN_EXECUTE == task->pkt.get_packet_code()
                && OB_SUCCESS == ret)
            {
              UPS.response_buffer(ret, task->pkt, ups_result_buffer_);
            }
            else
            {
              UPS.response_result(ret, task->pkt);
            }
            allocator_.free(task);
            task = NULL;
          }
        }
        uncommited_session_list_.clear(); */
        OB_STAT_INC(UPDATESERVER, UPS_STAT_BATCH_COUNT, 1);
        OB_STAT_INC(UPDATESERVER, UPS_STAT_BATCH_TIMEU, tbsys::CTimeUtil::getTime() - batch_start_time());
        batch_start_time() = 0;
      }
      try_submit_auto_freeze_();
      return ret;
    }

    void TransExecutor::try_submit_auto_freeze_()
    {
      int err = OB_SUCCESS;
      static int64_t last_try_freeze_time = 0;
      if (TRY_FREEZE_INTERVAL < (tbsys::CTimeUtil::getTime() - last_try_freeze_time)
          && UPS.get_table_mgr().need_auto_freeze())
      {
        int64_t cur_ts = tbsys::CTimeUtil::getTime();
        if (OB_SUCCESS != (err = UPS.submit_auto_freeze()))
        {
          TBSYS_LOG(WARN, "submit_auto_freeze()=>%d", err);
        }
        else
        {
          TBSYS_LOG(INFO, "submit async auto freeze task, last_ts=%ld, cur_ts=%ld", last_try_freeze_time, cur_ts);
          last_try_freeze_time = cur_ts;
        }
      }
    }

    void *TransExecutor::on_commit_begin()
    {
      CommitParamData *ret = NULL;
      void *buffer = ob_malloc(sizeof(CommitParamData), ObModIds::OB_UPS_PARAM_DATA);
      if (NULL != buffer)
      {
        ret = new(buffer) CommitParamData();
        ret->buffer.set_data(ret->cbuffer, sizeof(ret->cbuffer));
      }
      return ret;
    }

    void TransExecutor::on_commit_end(void *ptr)
    {
      if (NULL != ptr)
      {
        ob_free(ptr);
        ptr = NULL;
      }
    }

    void TransExecutor::log_trans_info() const
    {
      TBSYS_LOG(INFO, "==========log trans executor start==========");
      TBSYS_LOG(INFO, "allocator info hold=%ld allocated=%ld", allocator_.hold(), allocator_.allocated());
      TBSYS_LOG(INFO, "session_mgr info flying session num ro=%ld rp=%ld rw=%ld",
                session_mgr_.get_flying_rosession_num(),
                session_mgr_.get_flying_rpsession_num(),
                session_mgr_.get_flying_rwsession_num());
      TBSYS_LOG(INFO, "queued_num trans_thread=%ld commit_thread=%ld",
                TransHandlePool::get_queued_num(),
                TransCommitThread::get_queued_num());
      TBSYS_LOG(INFO, "==========log trans executor end==========");
    }

    int &TransExecutor::thread_errno()
    {
      static __thread int thread_errno = OB_SUCCESS;
      return thread_errno;
    }

    int64_t &TransExecutor::batch_start_time()
    {
      static __thread int64_t batch_start_time = 0;
      return batch_start_time;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    void TransExecutor::phandle_non_impl(ObPacket &pkt, ObDataBuffer &buffer)
    {
      UNUSED(buffer);
      TBSYS_LOG(ERROR, "packet code=%d no implement phandler", pkt.get_packet_code());
      UPS.response_result(OB_NOT_IMPLEMENT, pkt);
    }

    void TransExecutor::phandle_freeze_memtable(ObPacket &pkt, ObDataBuffer &buffer)
    {
      UPS.ups_freeze_memtable(pkt.get_api_version(),
                              &pkt,
                              buffer,
                              pkt.get_packet_code());
    }

    void TransExecutor::phandle_clear_active_memtable(ObPacket &pkt, ObDataBuffer &buffer)
    {
      UNUSED(buffer);
      UPS.ups_clear_active_memtable(pkt.get_api_version(),
                                    pkt.get_request(),
                                    pkt.get_channel_id());
    }

    void TransExecutor::phandle_check_cur_version(ObPacket &pkt, ObDataBuffer &buffer)
    {
      UNUSED(pkt);
      UNUSED(buffer);
      UPS.ups_check_cur_version();
    }

    void TransExecutor::phandle_check_sstable_checksum(ObPacket &pkt, ObDataBuffer &buffer)
    {
      UNUSED(pkt);
      UNUSED(buffer);
      UPS.ups_commit_check_sstable_checksum(*pkt.get_buffer());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    bool TransExecutor::thandle_non_impl(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      UNUSED(host);
      UNUSED(pdata);
      TBSYS_LOG(ERROR, "packet code=%d no implement thandler", task.pkt.get_packet_code());
      UPS.response_result(OB_NOT_IMPLEMENT, task.pkt);
      return true;
    }

    bool TransExecutor::thandle_commit_end(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      pdata.buffer.get_position() = 0;
      host.handle_commit_end_(task, pdata.buffer);
      return true;
    }

    bool TransExecutor::thandle_scan_trans(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      if (common::PACKET_RECORDER_FLAG)
      {
        host.fifo_stream_.push(&task.pkt);
      }
      pdata.buffer.get_position() = 0;

      //modify zhujun [transaction read uncommit]2016/3/29
      int64_t pos = task.pkt.get_buffer()->get_position();
      if(OB_SUCCESS != pdata.scan_param.deserialize(task.pkt.get_buffer()->get_data(),
                                                          task.pkt.get_buffer()->get_capacity(),
                                                          pos)) {
          TBSYS_LOG(WARN, "deserialize scan_param fail");
      }
      //del by maosy [Delete_Update_Function_for_snpshot] 20161215
//add by hongchen [Delete_Update_Function_for_snpshot] 20161210
//      else if(0 < pdata.scan_param.get_trans_id().read_times_)
//      {
//          if (pdata.scan_param.get_trans_id().is_valid() &&
//                  NULL != host.session_mgr_.fetch_ctx<ROSessionCtx>(pdata.scan_param.get_trans_id().descriptor_))
//          {
//              host.session_mgr_.revert_ctx(pdata.scan_param.get_trans_id().descriptor_);
//              host.handle_scan_trans_in_one(task,pdata.scan_param, pdata.scanner, pdata.new_scanner, pdata.buffer);
//          }
//          else
//          {
//              TBSYS_LOG(WARN,"batch update or delete must have tranid %s",to_cstring(pdata.scan_param.get_trans_id()));
//              UPS.response_result(OB_ERR_BATCH_NOT_IN_TRANSACTION,  task.pkt);
//          }

//          //host.handle_get_trans_in_two(task,pdata.get_param,pdata.scanner,pdata.new_scanner,pdata.buffer);
//      }
//add by hongchen  20161210
      //del by maosy
      else if (pdata.scan_param.get_trans_id().is_valid() &&
                NULL != host.session_mgr_.fetch_ctx<ROSessionCtx>(pdata.scan_param.get_trans_id().descriptor_))
      {
          host.session_mgr_.revert_ctx(pdata.scan_param.get_trans_id().descriptor_);
          host.handle_scan_trans_in_one(task,pdata.scan_param,pdata.scanner,pdata.new_scanner,pdata.buffer);
      }
      else
      {
          host.handle_scan_trans_(task.pkt, pdata.scan_param, pdata.scanner, pdata.new_scanner, pdata.buffer);
      }
      return true;
    }

    bool TransExecutor::thandle_get_trans(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      if (common::PACKET_RECORDER_FLAG)
      {
        host.fifo_stream_.push(&task.pkt);
      }
      pdata.buffer.get_position() = 0;
      // modify zhujun [transaction read uncommit] 2016/3/22
      int64_t pos = task.pkt.get_buffer()->get_position();
      if(OB_SUCCESS != pdata.get_param.deserialize(task.pkt.get_buffer()->get_data(),
                                                          task.pkt.get_buffer()->get_capacity(),
                                                          pos)) {
          TBSYS_LOG(WARN, "deserialize get_param fail");
      }
      //del by maosy [Delete_Update_Function_for_snpshot] 20161215
//add by hongchen [Delete_Update_Function_for_snpshot] 20161210
//      else if(0 < pdata.get_param.get_trans_id().read_times_)
//      {
//          if (pdata.get_param.get_trans_id().is_valid() &&
//                  NULL != host.session_mgr_.fetch_ctx<ROSessionCtx>(pdata.get_param.get_trans_id().descriptor_))
//          {
//              host.session_mgr_.revert_ctx(pdata.get_param.get_trans_id().descriptor_);
//              host.handle_get_trans_in_one(task,pdata.get_param, pdata.scanner, pdata.new_scanner, pdata.buffer);
//          }
//          else
//          {
//              TBSYS_LOG(WARN,"batch update or delete must have tranid %s",to_cstring(pdata.get_param.get_trans_id()));
//              UPS.response_result(OB_ERR_BATCH_NOT_IN_TRANSACTION, task.pkt);
//          }

//          //host.handle_get_trans_in_two(task,pdata.get_param,pdata.scanner,pdata.new_scanner,pdata.buffer);
//      }
//add by hongchen  20161210
      //del by maosy
      //判断事务号是否有效，以及对应事务号的SESSION是否关闭
      else if(pdata.get_param.get_trans_id().is_valid() &&
              NULL != host.session_mgr_.fetch_ctx<ROSessionCtx>(pdata.get_param.get_trans_id().descriptor_))//判断是否已经提交过,并且sessio未被关闭
      {
          host.session_mgr_.revert_ctx(pdata.get_param.get_trans_id().descriptor_);
          host.handle_get_trans_in_one(task,pdata.get_param, pdata.scanner, pdata.new_scanner, pdata.buffer);

          //host.handle_get_trans_in_two(task,pdata.get_param,pdata.scanner,pdata.new_scanner,pdata.buffer);
      }
      else
      {
          host.handle_get_trans_(task.pkt, pdata.get_param, pdata.scanner, pdata.new_scanner, pdata.buffer);
      }
      //modify:e
      return true;
    }

    bool TransExecutor::thandle_write_trans(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      bool ret = true;
      pdata.buffer.get_position() = 0;
      if (OB_PHY_PLAN_EXECUTE == task.pkt.get_packet_code())
      {
        ret = host.handle_phyplan_trans_(task, pdata.phy_operator_factory, pdata.phy_plan, pdata.new_scanner, pdata.allocator, pdata.buffer);
      }
      else
      {
        ret = host.handle_write_trans_(task, pdata.mutator, pdata.new_scanner);
      }
      return ret;
    }

    bool TransExecutor::thandle_start_session(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      bool ret = true;
      pdata.buffer.get_position() = 0;
      ret = host.handle_start_session_(task, pdata.buffer);
      return ret;
    }

    bool TransExecutor::thandle_kill_zombie(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      UNUSED(task);
      UNUSED(pdata);
      host.handle_kill_zombie_();
      return true;
    }

    bool TransExecutor::thandle_show_sessions(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      pdata.buffer.get_position() = 0;
      host.handle_show_sessions_(task.pkt, pdata.new_scanner, pdata.buffer);
      return true;
    }

    bool TransExecutor::thandle_kill_session(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      UNUSED(pdata);
      host.handle_kill_session_(task.pkt);
      return true;
    }

    bool TransExecutor::thandle_end_session(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      pdata.buffer.get_position() = 0;
      return host.handle_end_session_(task, pdata.buffer);
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////

    bool TransExecutor::chandle_non_impl(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      UNUSED(pdata);
      TBSYS_LOG(ERROR, "packet code=%d no implement chandler", task.pkt.get_packet_code());
      UPS.response_result(OB_NOT_IMPLEMENT, task.pkt);
      return true;
    }

    bool TransExecutor::chandle_write_commit(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(pdata);
      int ret = host.handle_write_commit_(task);
      return (OB_SUCCESS != ret);
    }

    bool TransExecutor::chandle_send_log(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      int ret = false;
      pdata.buffer.get_position() = 0;
      if(OB_SUCCESS != UPS.ups_slave_write_log(task.pkt.get_api_version(),
                              *(task.pkt.get_buffer()),
                              task.pkt.get_request(),
                              task.pkt.get_channel_id(),
                              //add chujiajia [Paxos ups_replication] 20160107:b
                              task.pkt.get_cmt_log_seq(),
                              //add:e
                              pdata.buffer))
      {
        ret = true;
      }
      //add wangdonghui [ups_log_replication_optimize] 20161009 :b
      else
      {
        task.pkt.set_receive_ts(tbsys::CTimeUtil::getTime());
        UPS.ups_push_wait_flush(&task);
      }
      //add :e
      return ret;
    }

    bool TransExecutor::chandle_fake_write_for_keep_alive(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      UNUSED(task);
      UNUSED(pdata);
      UPS.ups_handle_fake_write_for_keep_alive();
      return true;
    }

    bool TransExecutor::chandle_slave_reg(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      pdata.buffer.get_position() = 0;
      UPS.ups_slave_register(task.pkt.get_api_version(),
                            *(task.pkt.get_buffer()),
                            task.pkt.get_request(),
                            task.pkt.get_channel_id(),
                            pdata.buffer);
      return true;
    }

   //add zhaoqiong [fixed for Backup]:20150811:b
    bool TransExecutor::chandle_backup_reg(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      pdata.buffer.get_position() = 0;
      UPS.ups_backup_register(task.pkt.get_api_version(),
                            *(task.pkt.get_buffer()),
                            task.pkt.get_request(),
                            task.pkt.get_channel_id(),
                            pdata.buffer);
      return true;
    }
  //add:e

    bool TransExecutor::chandle_switch_schema(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      UNUSED(pdata);
      UPS.ups_switch_schema(task.pkt.get_api_version(),
                            &(task.pkt),
                            *(task.pkt.get_buffer()));
      return true;
    }

    //add zhaoqiong [Schema Manager] 20150327:b
    bool TransExecutor::chandle_switch_schema_mutator(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      UNUSED(pdata);
      UPS.ups_switch_schema_mutator(task.pkt.get_api_version(),
                            &(task.pkt),
                            *(task.pkt.get_buffer()));
      return true;
    }
    bool TransExecutor::chandle_switch_tmp_schema(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      UNUSED(pdata);
      UNUSED(task);
      UPS.ups_switch_tmp_schema();
      return true;
    }

    //add:e

    bool TransExecutor::chandle_force_fetch_schema(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      UNUSED(pdata);
      UPS.ups_force_fetch_schema(task.pkt.get_api_version(),
                                 task.pkt.get_request(),
                                 task.pkt.get_channel_id());
      return true;
    }

    bool TransExecutor::chandle_switch_commit_log(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      pdata.buffer.get_position() = 0;
      UPS.ups_switch_commit_log(task.pkt.get_api_version(),
                                task.pkt.get_request(),
                                task.pkt.get_channel_id(),
                                pdata.buffer);
      return true;
    }

    bool TransExecutor::chandle_nop(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      UNUSED(task);
      UNUSED(pdata);
      //TBSYS_LOG(INFO, "handle nop");
      return false;
    }

    //add chujiajia [Paxos ups_replication] 20160113:b
    void TransExecutor::disable_fake_write_granted()
    {
      FakeWriteGuard guard(session_mgr_);
      guard.set_fake_write_granted(false);
    }
    void TransExecutor::enable_fake_write_granted()
    {
      FakeWriteGuard guard(session_mgr_);
      guard.set_fake_write_granted(true);
    }
    //add:e

  }
}
