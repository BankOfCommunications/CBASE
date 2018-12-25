#include <gtest/gtest.h>
#include <tbsys.h>

#include <ob_frozen_data_cache.h>

#include <common/ob_string.h>
#include <common/ob_se_array.h>
#include <common/ob_object.h>
#include <sql/ob_sql_read_param.h>

using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace oceanbase::sql;

TEST(TestObFrozenDataCache, basic)
{
  ObFrozenDataCache  cache;
  ObFrozenDataKey    key;
  ObFrozenDataKeyBuf key_buf;
  ObSqlReadParam     read_param;
  ObFrozenData       value;
  int64_t            cur_size_counter;
  ObRow              row;
  ObRowDesc          row_desc;
  ObObj              cell1; cell1.set_int(1000);
  ObObj              cell2; cell2.set_int(1001);
  ObCachedFrozenData data;
  const char *       sql_str = "ABCDEFG";
  const int32_t      sql_str_len = static_cast<int32_t>(strlen(sql_str));
  ObString           sql(sql_str_len, sql_str_len, sql_str);
  ObObj              bp; bp.set_int(100);
  ObSEArray<ObObj*, 42> params;
  read_param.set_table_id(1001, 1001);
  read_param.set_data_version(2);
  ASSERT_EQ(OB_SUCCESS, key_buf.alloc_buf(OB_MAX_PACKET_LENGTH));
  ASSERT_EQ(OB_SUCCESS, read_param.serialize(key_buf.buf, key_buf.buf_len, key_buf.pos));
  key.param_buf = key_buf.buf;
  key.len = key_buf.pos;
  ASSERT_EQ(OB_SUCCESS, params.push_back(&bp));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 16));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 17));
  row.set_row_desc(row_desc);
  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 16, cell1));
  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 17, cell2));
  ASSERT_EQ(OB_SUCCESS, value.add_row(row, cur_size_counter));
  ASSERT_EQ(OB_SUCCESS, cache.init(1L << 20));
  ASSERT_EQ(OB_SUCCESS, cache.put(key, value));
  ASSERT_EQ(OB_SUCCESS, cache.put(key, value));

  ASSERT_FALSE(data.has_data());
  ASSERT_EQ(OB_SUCCESS, cache.get(key, data));
  ASSERT_TRUE(data.has_data());
  const ObRow      * prow;
  const ObObj      * pcell;
  int64_t            v;
  data.set_row_desc(row_desc);
  ASSERT_EQ(OB_SUCCESS, data.get_next_row(prow));
  ASSERT_EQ(OB_SUCCESS, prow->get_cell(1001, 16, pcell));
  pcell->get_int(v);
  ASSERT_EQ(1000, v);
  ASSERT_EQ(OB_SUCCESS, prow->get_cell(1001, 17, pcell));
  pcell->get_int(v);
  ASSERT_EQ(1001, v);
  ASSERT_EQ(OB_ITER_END, data.get_next_row(prow));
  ASSERT_EQ(OB_SUCCESS, cache.revert(data));
  ASSERT_FALSE(data.has_data());
}

int main(int argc, char **argv)
{
  TBSYS_LOGGER.setLogLevel("INFO");
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
