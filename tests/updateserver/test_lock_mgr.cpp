#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "common/ob_malloc.h"
#include "common/page_arena.h"
#include "updateserver/ob_lock_mgr.h"
#include "updateserver/ob_sessionctx_factory.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace updateserver;
using namespace common;

TEST(TestLockMgr, init)
{
  ILockInfo *nil = NULL;
  SessionMgr sm;
  FIFOAllocator allocator;
  allocator.init(1L<<30, 1L<<30, 1L<<22);
  RWSessionCtx ctx(ST_READ_WRITE, sm, allocator);
  LockMgr lm;

  ctx.set_session_start_time(tbsys::CTimeUtil::getTime());
  ctx.set_session_timeout(1000000);
  ILockInfo *li = lm.assign(REPEATABLE_READ, ctx);
  EXPECT_EQ(nil, li);

  li = lm.assign(SERIALIZABLE, ctx);
  EXPECT_EQ(nil, li);

  li = lm.assign(READ_COMMITED, ctx);
  EXPECT_NE(nil, li);

  ctx.set_session_descriptor(1024);
  TEKey k;
  TEValue r1;
  TEValue r2;

  int ret = li->on_trans_begin();
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = li->on_write_begin(k, r1);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = li->on_write_begin(k, r1);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = li->on_read_begin(k, r1);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = li->on_read_begin(k, r2);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = li->on_write_begin(k, r2);
  EXPECT_EQ(OB_SUCCESS, ret);

  RWSessionCtx ctx2(ST_READ_WRITE, sm, allocator);
  ctx2.set_session_start_time(tbsys::CTimeUtil::getTime());
  ctx2.set_session_timeout(1000000);
  ctx2.set_session_descriptor(4096);
  ILockInfo *li2 = lm.assign(READ_COMMITED, ctx2);
  EXPECT_NE(nil, li);
  ret = li2->on_write_begin(k, r1);
  EXPECT_EQ(OB_ERR_EXCLUSIVE_LOCK_CONFLICT, ret);

  EXPECT_EQ(true, r1.row_lock.is_exclusive_locked_by(1024));
  EXPECT_EQ(true, r2.row_lock.is_exclusive_locked_by(1024));

  ret = dynamic_cast<RCLockInfo*>(li)->cb_func(false, NULL, ctx);
  EXPECT_EQ(OB_SUCCESS, ret);

  EXPECT_TRUE(false == r1.row_lock.is_exclusive_locked_by(1024));
  EXPECT_TRUE(false == r2.row_lock.is_exclusive_locked_by(1024));
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
