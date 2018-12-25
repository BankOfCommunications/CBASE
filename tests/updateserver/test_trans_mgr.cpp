#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "common/ob_malloc.h"
#include "common/page_arena.h"
#include "updateserver/ob_trans_mgr.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace updateserver;
using namespace common;

class MockTable : public ITableEngine
{
  public:
    MockTable() : counter_(0)
    {
    };
    ~MockTable()
    {
    };
  public:
    int rollback(void *data)
    {
      UNUSED(data);
      counter_++;
      return OB_SUCCESS;
    };
    int commit(void *data)
    {
      UNUSED(data);
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

TEST(TestTransMgr, read_trans)
{
  TransMgr tm;

  int ret = tm.init(1024);
  EXPECT_EQ(OB_SUCCESS, ret);

  uint64_t td = 0;
  ret = tm.start_transaction(READ_TRANSACTION, td, -1);
  EXPECT_EQ(OB_SUCCESS, ret);

  TransNode *tn = tm.get_trans_node(td);
  TransNode *nil = NULL;
  EXPECT_NE(nil, tn);

  EXPECT_EQ(-1, tn->get_trans_id());

  EXPECT_EQ(-1, tn->get_min_flying_trans_id());

  ret = tm.end_transaction(td, false);
  EXPECT_EQ(OB_SUCCESS, ret);

  tm.destroy();
}

TEST(TestTransMgr, write_trans)
{
  TransMgr tm;

  int ret = tm.init(1024);
  EXPECT_EQ(OB_SUCCESS, ret);

  int64_t timestamp = tbsys::CTimeUtil::getTime();
  uint64_t td = 0;
  usleep(1000);
  ret = tm.start_transaction(WRITE_TRANSACTION, td, -1);
  EXPECT_EQ(OB_SUCCESS, ret);

  TransNode *tn = tm.get_trans_node(td);
  TransNode *nil = NULL;
  EXPECT_NE(nil, tn);

  EXPECT_LT(timestamp, tn->get_trans_id());
  timestamp = tn->get_trans_id();

  EXPECT_EQ(-1, tn->get_min_flying_trans_id());

  ret = tm.end_transaction(td, false);
  EXPECT_EQ(OB_SUCCESS, ret);

  EXPECT_EQ(tn->get_trans_id(), tn->get_min_flying_trans_id());

  ////////////////////////////////////////////////////////////////////////////////////////////////////

  ret = tm.start_transaction(READ_TRANSACTION, td, -1);
  EXPECT_EQ(OB_SUCCESS, ret);

  tn = tm.get_trans_node(td);
  EXPECT_NE(nil, tn);

  EXPECT_EQ(timestamp, tn->get_trans_id());

  EXPECT_EQ(timestamp, tn->get_min_flying_trans_id());

  ret = tm.end_transaction(td, false);
  EXPECT_EQ(OB_SUCCESS, ret);

  ////////////////////////////////////////////////////////////////////////////////////////////////////

  ret = tm.start_transaction(WRITE_TRANSACTION, td, -1);
  EXPECT_EQ(OB_SUCCESS, ret);

  tn = tm.get_trans_node(td);
  EXPECT_NE(nil, tn);

  EXPECT_LT(timestamp, tn->get_trans_id());

  EXPECT_EQ(timestamp, tn->get_min_flying_trans_id());

  ret = tm.end_transaction(td, true);
  EXPECT_EQ(OB_SUCCESS, ret);

  tm.destroy();
}

TEST(TestTransMgr, rollback_mutation)
{
  TransMgr tm;
  MockTable mock_table;

  int ret = tm.init(1024);
  EXPECT_EQ(OB_SUCCESS, ret);

  uint64_t td = 0;
  ret = tm.start_transaction(WRITE_TRANSACTION, td, -1);
  EXPECT_EQ(OB_SUCCESS, ret);

  TransNode *tn = tm.get_trans_node(td);
  TransNode *nil = NULL;
  EXPECT_NE(nil, tn);

  ret = tn->start_mutation();
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = tn->add_rollback_data(&mock_table, NULL);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = tn->end_mutation(true);
  EXPECT_EQ(OB_SUCCESS, ret);

  EXPECT_EQ(1, mock_table.get_counter());
  mock_table.reset_counter();

  ret = tm.end_transaction(td, true);
  EXPECT_EQ(OB_SUCCESS, ret);

  EXPECT_EQ(0, mock_table.get_counter());

  tm.destroy();
}

TEST(TestTransMgr, rollback_transaction)
{
  TransMgr tm;
  MockTable mock_table;

  int ret = tm.init(1024);
  EXPECT_EQ(OB_SUCCESS, ret);

  uint64_t td = 0;
  ret = tm.start_transaction(WRITE_TRANSACTION, td, -1);
  EXPECT_EQ(OB_SUCCESS, ret);

  TransNode *tn = tm.get_trans_node(td);
  TransNode *nil = NULL;
  EXPECT_NE(nil, tn);

  ret = tn->start_mutation();
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = tn->add_rollback_data(&mock_table, NULL);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = tn->end_mutation(false);
  EXPECT_EQ(OB_SUCCESS, ret);

  EXPECT_EQ(0, mock_table.get_counter());
  mock_table.reset_counter();

  ret = tm.end_transaction(td, true);
  EXPECT_EQ(OB_SUCCESS, ret);

  EXPECT_EQ(1, mock_table.get_counter());

  tm.destroy();
}

TEST(TestTransMgr, rollback_multi)
{
  TransMgr tm;
  MockTable mock_table;

  int ret = tm.init(1024);
  EXPECT_EQ(OB_SUCCESS, ret);

  uint64_t td = 0;
  ret = tm.start_transaction(WRITE_TRANSACTION, td, -1);
  EXPECT_EQ(OB_SUCCESS, ret);

  TransNode *tn = tm.get_trans_node(td);
  TransNode *nil = NULL;
  EXPECT_NE(nil, tn);

  for (int64_t i = 0; i < 10; i++)
  {
    ret = tn->start_mutation();
    EXPECT_EQ(OB_SUCCESS, ret);

    ret = tn->add_rollback_data(&mock_table, NULL);
    EXPECT_EQ(OB_SUCCESS, ret);

    ret = tn->add_rollback_data(&mock_table, NULL);
    EXPECT_EQ(OB_SUCCESS, ret);

    ret = tn->end_mutation(0 == (i % 2));
    EXPECT_EQ(OB_SUCCESS, ret);

    if (0 == (i % 2))
    {
      EXPECT_EQ(2 + i, mock_table.get_counter());
    }
  }

  ret = tm.end_transaction(td, false);
  EXPECT_EQ(OB_SUCCESS, ret);

  EXPECT_EQ(10, mock_table.get_counter());

  ////////////////////////////////////////////////////////////////////////////////////////////////////

  mock_table.reset_counter();
  ret = tm.start_transaction(WRITE_TRANSACTION, td, -1);
  EXPECT_EQ(OB_SUCCESS, ret);

  tn = tm.get_trans_node(td);
  EXPECT_NE(nil, tn);

  for (int64_t i = 0; i < 10; i++)
  {
    ret = tn->start_mutation();
    EXPECT_EQ(OB_SUCCESS, ret);

    ret = tn->add_rollback_data(&mock_table, NULL);
    EXPECT_EQ(OB_SUCCESS, ret);

    ret = tn->add_rollback_data(&mock_table, NULL);
    EXPECT_EQ(OB_SUCCESS, ret);

    ret = tn->end_mutation(0 == (i % 2));
    EXPECT_EQ(OB_SUCCESS, ret);

    if (0 == (i % 2))
    {
      EXPECT_EQ(2 + i, mock_table.get_counter());
    }
  }

  ret = tm.end_transaction(td, true);
  EXPECT_EQ(OB_SUCCESS, ret);

  EXPECT_EQ(20, mock_table.get_counter());
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

