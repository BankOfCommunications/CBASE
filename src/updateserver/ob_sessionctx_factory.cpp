////===================================================================
 //
 // ob_sessionctx_factory.cpp updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
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

#include "common/ob_mod_define.h"
#include "ob_sessionctx_factory.h"
#include "ob_update_server_main.h"

namespace oceanbase
{
  namespace updateserver
  {
    ObCellInfoNode* const TEValueUCInfo::INVALID_CELL_INFO_NODE = (ObCellInfoNode*)~0LL;
    RWSessionCtx::v4si RWSessionCtx::v4si_zero = {0,0,0,0};
    //add wangdonghui [ups_replication] 20170215 :b
#define UPS ObUpdateServerMain::get_instance()->get_update_server()
    //add:e
    RWSessionCtx::RWSessionCtx(const SessionType type,
                               SessionMgr &host,
                               FIFOAllocator &fifo_allocator,
                               const bool need_gen_mutator) : BaseSessionCtx(type, host),
                                                               CallbackMgr(),
                                                               mod_(fifo_allocator),
                                                               page_arena_(ALLOCATOR_PAGE_SIZE, mod_),
                                                               stmt_page_arena_(ALLOCATOR_PAGE_SIZE, mod_),
                                                               stmt_page_arena_wrapper_(stmt_page_arena_),
                                                               stat_(ST_ALIVE),
                                                               alive_flag_(true),
                                                               last_conflict_session_id_(0),
                                                               commit_done_(false),
                                                               need_gen_mutator_(need_gen_mutator),
                                                               ups_mutator_(page_arena_),
                                                               ups_result_(stmt_page_arena_wrapper_),
                                                               uc_info_(),
                                                               lock_info_(NULL),
                                                               publish_callback_list_(),
                                                               free_callback_list_()
{
    }

    RWSessionCtx::~RWSessionCtx()
    {
    }

    void RWSessionCtx::end(const bool need_rollback)
    {
      if (!commit_done_)
      {
        commit_prepare_list();
        commit_prepare_checksum();
        callback(need_rollback, *this);
        if (NULL != lock_info_)
        {
          lock_info_->on_trans_end();
        }
        int64_t *d = (int64_t*)&dml_count_;
        OB_STAT_INC(UPDATESERVER, UPS_STAT_DML_REPLACE_COUNT, d[OB_DML_REPLACE - 1]);
        OB_STAT_INC(UPDATESERVER, UPS_STAT_DML_INSERT_COUNT,  d[OB_DML_INSERT  - 1]);
        OB_STAT_INC(UPDATESERVER, UPS_STAT_DML_UPDATE_COUNT,  d[OB_DML_UPDATE  - 1]);
        OB_STAT_INC(UPDATESERVER, UPS_STAT_DML_DELETE_COUNT,  d[OB_DML_DELETE  - 1]);
        commit_done_ = true;
      }
    }

    void RWSessionCtx::publish()
    {
      bool rollback = false;
      publish_callback_list_.callback(rollback, *this);
    }

    void RWSessionCtx::on_free()
    {
      bool rollback = false;
      free_callback_list_.callback(rollback, *this);
    }

    int RWSessionCtx::add_publish_callback(ISessionCallback *callback, void *data)
    {
      return publish_callback_list_.add_callback_info(*this, callback, data);
    }

    int RWSessionCtx::add_free_callback(ISessionCallback *callback, void *data)
    {
      return free_callback_list_.add_callback_info(*this, callback, data);
    }

    void *RWSessionCtx::alloc(const int64_t size)
    {
      TBSYS_LOG(DEBUG, "session alloc %p size=%ld", this, size);
      //add zhaoqiong [fifo allocator bug] 20161206:b
      if (page_arena_.total() > 512*1024*1024)
      {
        TBSYS_LOG(TRACE, "session alloc memory too large: id[%d], trans_id[%ld] alloc %p cur_size=%ld",get_session_descriptor(), get_trans_id() , this, page_arena_.total());
      }
      //add:e
      return page_arena_.alloc(size);
    }

    void RWSessionCtx::reset()
    {
      ups_result_.clear();
      ups_mutator_.clear();
      stat_ = ST_ALIVE;
      alive_flag_ = true;
      last_conflict_session_id_ = 0;
      commit_done_ = false;
      stmt_page_arena_.free();
      page_arena_.free();
      CallbackMgr::reset();
      BaseSessionCtx::reset();
      uc_info_.reset();
      lock_info_ = NULL;
      publish_callback_list_.reset();
      free_callback_list_.reset();
      checksum_callback_.reset();
      checksum_callback_list_.reset();
      dml_count_ = v4si_zero;
      table_array_.clear(); //add zhaoqiong [Truncate Table]:20170704
    }

    ObUpsMutator &RWSessionCtx::get_ups_mutator()
    {
      return ups_mutator_;
    }

    //add zhaoqiong [Truncate Table]:20170704:b
    int RWSessionCtx::check_truncate_conflict(uint64_t &table_id, bool& is_conflict)
    {
        int ret = OB_SUCCESS;
        int err = OB_SUCCESS;
        table_id = OB_INVALID_ID;
        is_conflict = false;

        if(get_ups_mutator().is_normal_mutator())
        {
            int64_t i = 0;
            for(; i< table_array_.count()&& err == OB_SUCCESS; i++)
            {
                table_id = table_array_.at(i);
                err = is_truncate_conflict(table_id, is_conflict);
                TBSYS_LOG(DEBUG, "check conflict. table_id=%ld,is_conflict=%s,err=%d",table_id,is_conflict?"True":"False",err);
            }
            if (err != OB_SUCCESS)
            {
                TBSYS_LOG(ERROR, "failed to check_truncate_conflict,ret=%d", err);
                ret = err;
            }
        }
        return ret;
    }
    //add:e

    //add zhaoqiong [Truncate Table]:20170704
    int RWSessionCtx::is_truncate_conflict(uint64_t table_id, bool& is_conflict)
    {
        ObUpdateServerMain *main = ObUpdateServerMain::get_instance();
        int ret = OB_SUCCESS;
        is_conflict = false;
        int64_t last_truncate_timestamp = INT64_MAX;
        if (OB_SUCCESS !=(ret = main->get_update_server().get_table_mgr().get_last_truncate_timestamp_in_active
                          (get_session_descriptor(), table_id,last_truncate_timestamp)))
        {
            if (ret == OB_UD_PARALLAL_DATA_NOT_SAFE)
            {
                 TBSYS_LOG(WARN, "[table_id=%ld] is currently being truncated, ret=%d", table_id,ret);
            }
            else
            {
                TBSYS_LOG(WARN, "failed to get_last_truncate_timestamp_in_active[table_id=%ld], ret=%d", table_id,ret);
            }
        }
        else if ((last_truncate_timestamp != INT64_MAX) && (last_truncate_timestamp >= get_session_start_time()))
        {
            is_conflict = true;
            TBSYS_LOG(INFO, "[table_id=%ld]session_start_time_=%ld, last_truncate_timestamp=%ld",table_id,get_session_start_time(),last_truncate_timestamp);
        }

        return ret;
    }
    //add:e

    TransUCInfo &RWSessionCtx::get_uc_info()
    {
      return uc_info_;
    }

    TEValueUCInfo *RWSessionCtx::alloc_tevalue_uci()
    {
      TEValueUCInfo *ret = (TEValueUCInfo*)alloc(sizeof(TEValueUCInfo));
      if (NULL != ret)
      {
        ret->reset();
      }
      return ret;
    }

    int RWSessionCtx::init_lock_info(LockMgr& lock_mgr, const IsolationLevel isolation)
    {
      int ret = OB_SUCCESS;
      if (NULL == (lock_info_ = lock_mgr.assign(isolation, *this)))
      {
        TBSYS_LOG(WARN, "assign lock_info fail");
        ret = OB_MEM_OVERFLOW;
      }
      else if (OB_SUCCESS != (ret = lock_info_->on_trans_begin()))
      {
        TBSYS_LOG(WARN, "invoke on_trans_begin fail ret=%d", ret);
      }
      return ret;
    }

    ILockInfo *RWSessionCtx::get_lock_info()
    {
      return lock_info_;
    }

    int64_t RWSessionCtx::get_min_flying_trans_id()
    {
      return get_host().get_min_flying_trans_id();
    }

    void RWSessionCtx::flush_min_flying_trans_id()
    {
      get_host().flush_min_flying_trans_id();
    }

    sql::ObUpsResult &RWSessionCtx::get_ups_result()
    {
      return ups_result_;
    }

    const bool volatile &RWSessionCtx::is_alive() const
    {
      return alive_flag_;
    }

    bool RWSessionCtx::is_killed() const
    {
      return (ST_KILLING == stat_);
    }

    void RWSessionCtx::kill()
    {
      //modify wangdonghui [ups_replication] 20170215 :b
      //for kill frozen session when master switch to slave
      //if (ST_ALIVE != ATOMIC_CAS(&stat_, ST_ALIVE, ST_KILLING))
      if (ObUpsRoleMgr::MASTER != UPS.get_role_mgr().get_role())
      {
          stat_ = ST_KILLING;
          alive_flag_ = false;
          TBSYS_LOG(INFO, "session is being killed sd=%u stat=%d session_start_time=%ld stmt_start_time=%ld session_timeout=%ld stmt_timeout=%ld",
                    get_session_descriptor(), stat_, get_session_start_time(), get_stmt_start_time(), get_session_timeout(), get_stmt_timeout());
      }
      else if ( ST_ALIVE != ATOMIC_CAS(&stat_, ST_ALIVE, ST_KILLING))
      //mod:e
      {
        TBSYS_LOG(WARN, "session will not be killed sd=%u stat=%d session_start_time=%ld stmt_start_time=%ld session_timeout=%ld stmt_timeout=%ld",
                  get_session_descriptor(), stat_, get_session_start_time(), get_stmt_start_time(), get_session_timeout(), get_stmt_timeout());
      }
      else
      {
        TBSYS_LOG(INFO, "session is being killed sd=%u stat=%d session_start_time=%ld stmt_start_time=%ld session_timeout=%ld stmt_timeout=%ld",
                  get_session_descriptor(), stat_, get_session_start_time(), get_stmt_start_time(), get_session_timeout(), get_stmt_timeout());
        alive_flag_ = false;
      }
    }

    void RWSessionCtx::set_frozen()
    {
      Stat old_stat = stat_;
      stat_ = ST_FROZEN;
      if (ST_KILLING == old_stat)
      {
        TBSYS_LOG(INFO, "session has been set frozen, will not be killed, sd=%u", get_session_descriptor());
      }
    }

    bool RWSessionCtx::is_frozen() const
    {
      return (ST_FROZEN == stat_);
    }

    void RWSessionCtx::reset_stmt()
    {
      ups_result_.clear();
      stmt_page_arena_.reuse();
    }

    int RWSessionCtx::add_table_item(uint64_t table_id)
    {
        int ret = OB_SUCCESS;
        bool is_found =false;
        int64_t i=0;
        for(; i< table_array_.count();i++)
        {
            if(table_array_.at(i) == table_id)
            {
                is_found = true;
                break;
            }
        }
        if (!is_found)
        {
            ret = table_array_.push_back(table_id);
        }
        return ret;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    SessionCtxFactory::SessionCtxFactory() : mod_(ObModIds::OB_UPS_SESSION_CTX),
                                             allocator_(ALLOCATOR_PAGE_SIZE, mod_),
                                             ctx_allocator_()
    {
      if (OB_SUCCESS != ctx_allocator_.init(ALLOCATOR_TOTAL_LIMIT, ALLOCATOR_HOLD_LIMIT, ALLOCATOR_PAGE_SIZE))
      {
        TBSYS_LOG(ERROR, "init allocator fail");
      }
      else
      {
        ctx_allocator_.set_mod_id(ObModIds::OB_UPS_SESSION_CTX);
      }
    }

    SessionCtxFactory::~SessionCtxFactory()
    {
    }

    BaseSessionCtx *SessionCtxFactory::alloc(const SessionType type, SessionMgr &host)
    {
      char *buffer = NULL;
      BaseSessionCtx *ret = NULL;
      switch (type)
      {
      case ST_READ_ONLY:
        buffer = allocator_.alloc(sizeof(ROSessionCtx));
        if (NULL != buffer)
        {
         ret = new(buffer) ROSessionCtx(type, host);
        }
        break;
      case ST_REPLAY:
        buffer = allocator_.alloc(sizeof(RPSessionCtx));
        if (NULL != buffer)
        {
          ret = new(buffer) RPSessionCtx(type, host, ctx_allocator_);
        }
        break;
      case ST_READ_WRITE:
        buffer = allocator_.alloc(sizeof(RWSessionCtx));
        if (NULL != buffer)
        {
          ret = new(buffer) RWSessionCtx(type, host, ctx_allocator_);
        }
        break;
      default:
        TBSYS_LOG(WARN, "invalid session type=%d", type);
        break;
      }
      return ret;
    }

    void SessionCtxFactory::free(BaseSessionCtx *ptr)
    {
      UNUSED(ptr);
    }
  }
}

