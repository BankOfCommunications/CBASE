#include "../common/test_base.h"
#include "updateserver/ob_ups_table_mgr.h"
#include "updateserver/ob_trigger_handler.h"
#include "updateserver/ob_table_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;

namespace oceanbase
{
  namespace test
  {
    class MockLogMgr : public ObILogWriter
    {
      public:
        int switch_log_file(uint64_t &new_log_file_id)
        {
          TBSYS_LOG(INFO, "[MOCK] new_log_file_id=%lu", new_log_file_id);
          return OB_SUCCESS;
        };
        int write_replay_point(uint64_t replay_point)
        {
          TBSYS_LOG(INFO, "[MOCK] replay_point=%lu", replay_point);
          return OB_SUCCESS;
        };
    };

    class ObMockUps
    {
      public:
        ObMockUps(): table_mgr_(log_writer_) {}
        ~ObMockUps() {}
        updateserver::TableItem* get_active_table_item() {
          TableMgr* table_mgr = NULL;
          updateserver::TableItem * table_item = NULL;
          if (NULL == (table_mgr = get_table_mgr().get_table_mgr()))
          {
            TBSYS_LOG(ERROR, "table_mgr == NULL");
          }
          else if (NULL == (table_item = table_mgr->get_active_memtable()))
          {
            TBSYS_LOG(ERROR, "get_active_memtable()=>NULL");
          }
          return table_item;
        }

        struct SessionGuard
        {
          static const IsolationLevel lock_type = READ_COMMITED;
          static const SessionType type = ST_READ_WRITE;
          SessionGuard(ObMockUps& ups): session_ctx_(NULL), session_descriptor_(0), session_mgr_(ups.get_session_mgr()) {
            int err = OB_SUCCESS;
            updateserver::TableItem* table_item = NULL;
            if (OB_SUCCESS != (err = session_mgr_.begin_session(type, tbsys::CTimeUtil::getTime(), INT64_MAX, INT64_MAX, session_descriptor_)))
            {
              TBSYS_LOG(ERROR, "begin_session()=>%d", err);
            }
            else if (NULL == (session_ctx_ = (RWSessionCtx*)session_mgr_.fetch_ctx(session_descriptor_)))
            {
              TBSYS_LOG(ERROR, "fetch_ctx():FAILED");
            }
            else if (NULL == ups.get_lock_mgr().assign(lock_type, *session_ctx_))
            {
              TBSYS_LOG(ERROR, "assign_lock()=>NULL");
            }
            else if (NULL == (table_item = ups.get_active_table_item()))
            {
              TBSYS_LOG(ERROR, "get_active_memtable()=>NULL");
            }
            else
            {
              session_ctx_->get_uc_info().host = &table_item->get_memtable();
            }
          }
          ~SessionGuard() {
            session_mgr_.revert_ctx(session_descriptor_);
            session_mgr_.end_session(session_descriptor_);
          }
          RWSessionCtx* session_ctx_;
          uint32_t session_descriptor_;
          SessionMgr& session_mgr_;
        };
      public:
        int init(BaseConfig& cfg) {
          int err = OB_SUCCESS;
          CommonSchemaManagerWrapper schema_wrapper;
          tbsys::CConfig cfg_parser;
          UNUSED(cfg);
          QueryEngine::HASH_SIZE = 1000;
          if (OB_SUCCESS != (err = session_mgr_.init(1, 1, 100, &session_factory_)))
          {
            TBSYS_LOG(ERROR, "session_mgr.init()=>%d", err);
          }
          else if(OB_SUCCESS != (err = table_mgr_.init()))
          {
            TBSYS_LOG(ERROR, "table_mgr.init()=>%d", err);
          }
          else if (OB_SUCCESS != (err = table_mgr_.get_table_mgr()->sstable_scan_finished(2)))
          {
            TBSYS_LOG(ERROR, "sstable_scan-finished()=>%d", err);
          }
          else if (!schema_mgr_.parse_from_file(cfg.schema, cfg_parser))
          {
            err = OB_SCHEMA_ERROR;
          }
          else if (OB_SUCCESS != (err = schema_wrapper.set_impl(schema_mgr_)))
          {
            TBSYS_LOG(ERROR, "schema_wrapper.set_impl()=>%d", err);
          }
          else if (OB_SUCCESS != (err = table_mgr_.set_schemas(schema_wrapper)))
          {
            TBSYS_LOG(ERROR, "table_mgr.set_schema()=>%d", err);
          }
          return err;
        }
        LockMgr& get_lock_mgr() { return lock_mgr_; }
        SessionMgr& get_session_mgr() { return session_mgr_; }
        ObUpsTableMgr& get_table_mgr() { return table_mgr_; }
      protected:
        CommonSchemaManager schema_mgr_;
        SessionCtxFactory session_factory_;
        LockMgr lock_mgr_;
        SessionMgr session_mgr_;
        MockLogMgr mock_log_mgr_;
        ObUpsTableMgr table_mgr_;
        ObLogWriter log_writer_;
    };
  }
}; // end namespace oceanbase

