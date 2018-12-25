#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "common/ob_malloc.h"
#include "common/page_arena.h"
#include "updateserver/ob_session_mgr.h"
#include "updateserver/ob_trigger_handler.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace updateserver;
using namespace common;

class MockTable : public ISessionCallback
{
  public:
    MockTable() : counter_(0)
    {
    };
    ~MockTable()
    {
    };
  public:
    int callback(const bool rollback, void *data, BaseSessionCtx &session)
    {
      UNUSED(data);
      UNUSED(session);
      if (rollback)
      {
        counter_++;
      }
      return OB_SUCCESS;
    };
    int64_t get_counter() const
    {
      return  counter_;
    };
    void reset_counter()
    {
      counter_ = 0;
    };
  private:
    int64_t counter_;
};

class RWSessionCtx : public BaseSessionCtx, public CallbackMgr
{
  public:
    RWSessionCtx(SessionMgr &host) : BaseSessionCtx(ST_READ_WRITE, host),
                                     is_frozen_(false),
                                     is_killed_(false)
    {
      TBSYS_LOG(INFO, "use mock RWSessionCtx");
    };
  public:
    void reset()
    {
      BaseSessionCtx::reset();
      CallbackMgr::reset();
      is_frozen_ = false;
      is_killed_ = false;
    };
    void end(const bool need_rollback)
    {
      callback(need_rollback, *this);
    };
    void kill()
    {
      if (is_frozen_)
      {
        TBSYS_LOG(WARN, "session is frozen, can not kill, sd=%u", get_session_descriptor());
      }
      else
      {
        is_killed_ = true;
      }
    };
    void set_frozen()
    {
      is_frozen_ = true;
      if (is_killed_)
      {
        is_killed_ = false;
        TBSYS_LOG(INFO, "session has been set frozen, will not be killed, sd=%u", get_session_descriptor());
      }
    };
    bool is_killed() const
    {
      return is_killed_;
    };
  private:
    bool is_frozen_;
    bool is_killed_;
};

class SessionCtxFactory : public ISessionCtxFactory
{
  public:
    SessionCtxFactory() {};
    ~SessionCtxFactory() {};
  public:
    BaseSessionCtx *alloc(const SessionType type, SessionMgr &host)
    {
      if (ST_READ_WRITE == type)
      {
        return new(std::nothrow) RWSessionCtx(host);
      }
      else
      {
        return new(std::nothrow) BaseSessionCtx(type, host);
      }
    };
    void free(BaseSessionCtx *ptr)
    {
      delete ptr;
    };
};

TEST(TestSessionMgr, init)
{
  BaseSessionCtx *nil = NULL;
  SessionCtxFactory factory;
  SessionMgr sm;

  int ret = sm.init(2, 2, 2, &factory);
  EXPECT_EQ(OB_SUCCESS, ret);

  uint32_t sd = 0;
  ret = sm.begin_session(ST_READ_WRITE, tbsys::CTimeUtil::getTime(), 0, 0, sd);
  EXPECT_EQ(OB_SUCCESS, ret);

  BaseSessionCtx *ctx = sm.fetch_ctx(sd);
  EXPECT_NE(nil, ctx);
  sm.revert_ctx(sd);

  RWSessionCtx *rw_ctx = sm.fetch_ctx<RWSessionCtx>(sd);
  EXPECT_NE(nil, rw_ctx);
  sm.kill_zombie_session(false);
  sm.revert_ctx(sd);

  sm.kill_zombie_session(false);
  rw_ctx = sm.fetch_ctx<RWSessionCtx>(sd);
  EXPECT_EQ(nil, rw_ctx);

  ret = sm.begin_session(ST_READ_WRITE, tbsys::CTimeUtil::getTime(), 0, 0, sd);
  EXPECT_EQ(OB_SUCCESS, ret);
  rw_ctx = sm.fetch_ctx<RWSessionCtx>(sd);
  EXPECT_NE(nil, rw_ctx);
  sm.kill_zombie_session(false);
  rw_ctx->set_frozen();
  sm.revert_ctx(sd);
  sm.kill_zombie_session(false);

  int64_t tid = sm.get_min_flying_trans_id();
  EXPECT_EQ(-1, tid);

  ret = sm.wait_write_session_end_and_lock(1000000);
  EXPECT_EQ(OB_PROCESS_TIMEOUT, ret);
  sm.unlock_write_session();

  ret = sm.end_session(sd, false);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = sm.wait_write_session_end_and_lock(1000000);
  EXPECT_EQ(OB_SUCCESS, ret);
  sm.unlock_write_session();
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

