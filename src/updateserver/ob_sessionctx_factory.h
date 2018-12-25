////===================================================================
 //
 // ob_sessionctx_factory.h updateserver / Oceanbase
 //
 // Copyright (C) 2010, 2013 Taobao.com, Inc.
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

#ifndef  OCEANBASE_UPDATESERVER_SESSIONCTX_FACTORY_H_
#define  OCEANBASE_UPDATESERVER_SESSIONCTX_FACTORY_H_
#include "common/ob_define.h"
#include "sql/ob_ups_result.h"
#include "ob_session_mgr.h"
#include "ob_ups_mutator.h"
#include "ob_table_engine.h"
#include "ob_lock_mgr.h"
#include "common/ob_fifo_allocator.h"

namespace oceanbase
{
  namespace updateserver
  {
    typedef BaseSessionCtx ROSessionCtx;


    struct TEValueUCInfo
    {
      static ObCellInfoNode* const INVALID_CELL_INFO_NODE;
      TEValue *value;
      uint32_t session_descriptor;
      int16_t uc_cell_info_cnt;
      int16_t uc_cell_info_size;
      ObCellInfoNode *uc_list_head;
      ObCellInfoNode *uc_list_tail;
      ObCellInfoNode *uc_list_tail_before_stmt;
      TEValueUCInfo()
      {
        reset();
      };
      void reset()
      {
        value = NULL;
        session_descriptor = INVALID_SESSION_DESCRIPTOR;
        uc_cell_info_cnt = 0;
        uc_cell_info_size = 0;
        uc_list_head = NULL;
        uc_list_tail = NULL;
        uc_list_tail_before_stmt = INVALID_CELL_INFO_NODE;
      };
    };

    struct TransUCInfo
    {
      int64_t uc_row_counter;
      uint64_t uc_checksum;
      MemTable *host;
      TransUCInfo()
      {
        reset();
      };
      void reset()
      {
        uc_row_counter = 0;
        uc_checksum = 0;
        host = NULL;
      };
    };

    class PageArenaWrapper : public ObIAllocator
    {
      public:
        PageArenaWrapper(common::ModuleArena &arena) : arena_(arena) {};
        ~PageArenaWrapper() {};
      public:
        void *alloc(const int64_t sz) {return arena_.alloc(sz);};
        void free(void *ptr) {arena_.free((char*)ptr);};
        void set_mod_id(int32_t mod_id) {UNUSED(mod_id);};
      private:
        common::ModuleArena &arena_;
    };

    class TEValueChecksumCallback : public ISessionCallback
    {
      public:
        TEValueChecksumCallback() : checksum_(0) {};
        void reset() {checksum_ = 0;};
        int cb_func(const bool rollback, void *data, BaseSessionCtx &session)
        {
          UNUSED(rollback);
          UNUSED(session);
          if (!rollback)
          {
            TEValue *te_value = (TEValue*)data;
            if (NULL != te_value->list_tail)
            {
              checksum_ ^= te_value->list_tail->modify_time;
            }
          }
          return OB_SUCCESS;
        };
        int64_t get_checksum() const {return checksum_;};
      private:
        int64_t checksum_;
    };

    class SessionMgr;
    class RWSessionCtx : public BaseSessionCtx, public CallbackMgr
    {
      const static int64_t INIT_TABLE_MAP_SIZE = 10;
      typedef int64_t v4si __attribute__ ((vector_size (32)));
      static v4si v4si_zero;
      enum Stat
      {
        // 状态转移:
        // ST_ALIVE --> ST_FROZEN
        // ST_FROZEN --> ST_FROZEN
        // ST_KILLING --> ST_FROZEN
        // ST_ALIVE --> ST_KILLING
        ST_ALIVE = 0,
        ST_FROZEN = 1,
        ST_KILLING = 2,
      };
      static const int64_t ALLOCATOR_PAGE_SIZE = 16L * 1024L;
      public:
        RWSessionCtx(const SessionType type, SessionMgr &host, common::FIFOAllocator &fifo_allocator, const bool need_gen_mutator=true);
        virtual ~RWSessionCtx();
      public:
        void end(const bool need_rollback);
        void publish();
        void on_free();
        void *alloc(const int64_t size);
        void reset();
        virtual void kill();
        int add_stmt_callback(ISessionCallback *callback, void *data)
        {
          return stmt_callback_list_.add_callback_info(*this, callback, data);
        }
        int add_publish_callback(ISessionCallback *callback, void *data);
        int add_free_callback(ISessionCallback *callback, void *data);
        int prepare_checksum_callback(void *data)
        {
          return checksum_callback_list_.prepare_callback_info(*this, &checksum_callback_, data);
        };
        void commit_prepare_checksum()
        {
          checksum_callback_list_.commit_prepare_list();
        };
        void rollback_prepare_checksum()
        {
          checksum_callback_list_.rollback_prepare_list(*this);
        };
        int64_t get_checksum()
        {
          checksum_callback_list_.callback(false, *this);
          return checksum_callback_.get_checksum();
        };
        void mark_stmt()
        {
          get_ups_mutator().get_mutator().mark();
          mark_stmt_total_row_counter_ = get_ups_result().get_affected_rows();
          mark_stmt_new_row_counter_ = get_uc_info().uc_row_counter;
          mark_stmt_checksum_ = get_uc_info().uc_checksum;
          mark_dml_count_ = dml_count_;
          stmt_callback_list_.reset();
        };
        void rollback_stmt()
        {
          get_ups_mutator().get_mutator().rollback();
          get_ups_result().set_affected_rows(mark_stmt_total_row_counter_);
          get_uc_info().uc_row_counter = mark_stmt_new_row_counter_;
          get_uc_info().uc_checksum = mark_stmt_checksum_;
          stmt_callback_list_.callback(true, *this);
          rollback_prepare_list(*this);
          rollback_prepare_checksum();
          dml_count_ = mark_dml_count_;
        };
        void commit_stmt()
        {
          stmt_callback_list_.callback(false, *this);
          commit_prepare_list();
          commit_prepare_checksum();
        };
        void inc_dml_count(const ObDmlType dml_type)
        {
          if (OB_DML_UNKNOW < dml_type
              && OB_DML_NUM > dml_type)
          {
            int64_t *d = (int64_t*)&dml_count_;
            d[dml_type - 1] += 1;
          }
          else
          {
            TBSYS_LOG(ERROR, "invalid dml_type=%d", dml_type);
          }
        };

        //add zhaoqiong [Truncate Table]:20170704:b
        int is_truncate_conflict(uint64_t table_id, bool &is_truncate);
        int check_truncate_conflict(uint64_t &table_id, bool &is_truncate);
        //add:e

      public:
        const bool get_need_gen_mutator() const { return need_gen_mutator_; }
        ObUpsMutator &get_ups_mutator();
        TransUCInfo &get_uc_info();
        TEValueUCInfo *alloc_tevalue_uci();
        int init_lock_info(LockMgr& lock_mgr, const IsolationLevel isolation);
        ILockInfo *get_lock_info();
        int64_t get_min_flying_trans_id();
        void flush_min_flying_trans_id();
        sql::ObUpsResult &get_ups_result();
        const bool volatile &is_alive() const;
        bool is_killed() const;
        void set_frozen();
        bool is_frozen() const;
        void reset_stmt();
        uint32_t get_conflict_session_id() const { return last_conflict_session_id_; }
        bool set_conflict_session_id(uint32_t session_id)
        { 
          bool is_conflict_sid_changed = (last_conflict_session_id_ != session_id);
          last_conflict_session_id_ = session_id;
          return is_conflict_sid_changed;
        }
        int add_table_item(uint64_t table_id);
      private:
        common::ObSEArray<uint64_t, INIT_TABLE_MAP_SIZE> table_array_; //add zhaoqiong [Truncate Table]:20170704
        common::ModulePageAllocator mod_;
        common::ModuleArena page_arena_;
        common::ModuleArena stmt_page_arena_;
        PageArenaWrapper stmt_page_arena_wrapper_;
        volatile Stat stat_;
        volatile bool alive_flag_;
        uint32_t last_conflict_session_id_;
        bool commit_done_;
        const bool need_gen_mutator_;
        ObUpsMutator ups_mutator_;
        sql::ObUpsResult ups_result_;
        TransUCInfo uc_info_;
        ILockInfo *lock_info_;
        CallbackMgr stmt_callback_list_;
        CallbackMgr publish_callback_list_;
        CallbackMgr free_callback_list_;
        TEValueChecksumCallback checksum_callback_;
        CallbackMgr checksum_callback_list_;
        int64_t mark_stmt_total_row_counter_;
        int64_t mark_stmt_new_row_counter_;
        uint64_t mark_stmt_checksum_;
        v4si mark_dml_count_;
        v4si dml_count_;
    };

    class RPSessionCtx: public RWSessionCtx
    {
      public:
        RPSessionCtx(const SessionType type, SessionMgr &host, common::FIFOAllocator &fifo_allocator): RWSessionCtx(type, host, fifo_allocator, false), is_replay_local_log_(false)
        {}
        ~RPSessionCtx() {}
        bool is_replay_local_log() const { return is_replay_local_log_; }
        void set_replay_local_log(bool is_replay_local_log) { is_replay_local_log_ = is_replay_local_log; }
        void kill() { TBSYS_LOG(WARN, "replay_session can not be killed: %s", to_cstring(*this)); }
      private:
        bool is_replay_local_log_;
    };

    class SessionCtxFactory : public ISessionCtxFactory
    {
      //mod zhaoqiong [fifo allocator bug] 20161118:b
      //static const int64_t ALLOCATOR_TOTAL_LIMIT = 5L * 1024L * 1024L * 1024L; 
      //static const int64_t ALLOCATOR_HOLD_LIMIT = static_cast<int64_t>(1.5 * 1024L * 1024L * 1024L); //1.5G //ALLOCATOR_TOTAL_LIMIT / 2;
      static const int64_t ALLOCATOR_TOTAL_LIMIT = 40L * 1024L * 1024L * 1024L;
      static const int64_t ALLOCATOR_HOLD_LIMIT = ALLOCATOR_TOTAL_LIMIT / 2; //ALLOCATOR_TOTAL_LIMIT / 2;
      //mod:e
      static const int64_t ALLOCATOR_PAGE_SIZE = 16L * 1024L * 1024L;
      public:
        SessionCtxFactory();
        ~SessionCtxFactory();
      public:
        BaseSessionCtx *alloc(const SessionType type, SessionMgr &host);
        void free(BaseSessionCtx *ptr);
      private:
        common::ModulePageAllocator mod_;
        common::ModuleArena allocator_;
        common::FIFOAllocator ctx_allocator_;
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_SESSIONCTX_FACTORY_H_
