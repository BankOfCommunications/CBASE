#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "common/ob_malloc.h"
#include "common/ob_row_compaction.h"
#include "updateserver/ob_memtable.h"
#include "updateserver/ob_table_engine.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace updateserver;

static const int64_t NODE_SIZE = 1<<21;

void build_tevalue(const int64_t node_num,
                   const int64_t varchar_num_pernode,
                   const int64_t int_num_pernode,
                   TEValue &tevalue,
                   CharArena &allocator)
{
  tevalue.reset();
  for (int64_t n = 0; n < node_num; n++)
  {
    uint64_t cid = 100;

    ObCellInfoNode *node = (ObCellInfoNode*)allocator.alloc(sizeof(ObCellInfoNode) + NODE_SIZE);
    assert(NULL != node);

    ObUpsCompactCellWriter ccw;
    ccw.init(node->buf, NODE_SIZE);
    for (int64_t v = 0; v < varchar_num_pernode; v++)
    {
      ObObj obj;
      ObString str(5, 5, "merge");
      obj.set_varchar(str);
      ccw.append(cid++, obj);
    }
    for (int64_t i = 0; i < int_num_pernode; i++)
    {
      ObObj obj;
      obj.set_int(i);
      ccw.append(cid++, obj);
    }
    ccw.row_finish();

    node->modify_time = -1;
    node->next = NULL;
    if (NULL == tevalue.list_head)
    {
      tevalue.list_head =  node;
    }
    else
    {
      tevalue.list_tail->next = node;
    }
    tevalue.list_tail = node;
  }
}

void run_test(const int64_t times,
              const int64_t node_num,
              const int64_t varchar_num_pernode,
              const int64_t int_num_pernode)
{
  SessionMgr sm;
  FIFOAllocator ffa;
  RWSessionCtx ctx(ST_READ_WRITE, sm, ffa);
  MemTable mt;

  TEKey te_key;
  te_key.table_id = 1001;
  ObObj obj;
  obj.set_int(8888);
  te_key.row_key.assign(&obj, 1);

  TEValue te_value;
  CharArena allocator;
  build_tevalue(node_num, varchar_num_pernode, int_num_pernode, te_value, allocator);

  int64_t timeu = tbsys::CTimeUtil::getTime();
  for (int64_t i = 0; i < times; i++)
  {
    TEValue tmp_value = te_value;
    /* mt.merge_(ctx, te_key, tmp_value); */
  }
  timeu = tbsys::CTimeUtil::getTime() - timeu;
  TBSYS_LOG(INFO, "timeu=%ld row_timeu=%0.2f speed=%0.2f", timeu, (double)timeu * 1.0 / (double)times, (double)times * 1.0 / (double)timeu);
}

TEST(TestMergePerf, run_test)
{
  ObRowCompaction *rc_iter = GET_TSI_MULT(ObRowCompaction, TSI_UPS_ROW_COMPACTION_1);
  FixedSizeBuffer<OB_MAX_PACKET_LENGTH> *tbuf = GET_TSI_MULT(FixedSizeBuffer<OB_MAX_PACKET_LENGTH>, TSI_UPS_FIXED_SIZE_BUFFER_2);
  UNUSED(rc_iter);
  UNUSED(tbuf);
  get_table_available_warn_size();
  run_test(10, 10, 10, 10);
  run_test(100, 10, 10, 10);
  run_test(1000, 10, 10, 10);
  run_test(10000, 10, 10, 10);
  run_test(100000, 10, 10, 10);

  run_test(10, 10, 0, 20);
  run_test(100, 10, 0, 20);
  run_test(1000, 10, 0, 20);
  run_test(10000, 10, 0, 20);
  run_test(100000, 10, 0, 20);
}

int main(int argc, char **argv)
{
  TBSYS_LOGGER.setLogLevel("info");
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

