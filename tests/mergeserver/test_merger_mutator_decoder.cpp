#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>

#include "common/ob_malloc.h"
#include "common/ob_schema.h"
#include "common/ob_cond_info.h"
#include "common/ob_common_param.h"
#include "common/ob_get_param.h"
#include "common/ob_mutator.h"
#include "ob_mutator_param_decoder.h"
#include "../common/test_rowkey_helper.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
static CharArena allocator_;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestMutatorParamDecoder: public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};


TEST_F(TestMutatorParamDecoder, test_decode)
{
  ObSchemaManagerV2 schema(100);
  tbsys::CConfig config;
  EXPECT_TRUE(schema.parse_from_file("schema.ini", config));
  ObCellInfo cell;
  ObCellInfo out_cell;
  char * no_name = (char*)"test_not_exist";
  char * tablename = (char*)"collect_info";
  char * columnname = (char*)"item_price";
  char * rowkey = (char*)"row_key";
  ObString table_name;
  ObString column_name;
  ObRowkey row_key;
  table_name.assign(tablename, static_cast<int32_t>(strlen(tablename)));
  column_name.assign(columnname, static_cast<int32_t>(strlen(columnname)));
  row_key = make_rowkey(rowkey, &allocator_);

  ObMutator mutator;
  int64_t count = 20;
  mutator.del_row(table_name, row_key);
  for (int64_t i = 0; i < count; ++i)
  {
    mutator.add(table_name, row_key, column_name, (int64_t)1, ObMutator::RETURN_NO_RESULT);
    mutator.add(table_name, row_key, column_name, i + 10, ObMutator::RETURN_UPDATE_RESULT);
  }
  mutator.del_row(table_name, row_key);
  
  ObUpdateCondition & cond = mutator.get_update_condition();
  ObObj obj;
  obj.set_int(1234);
  EXPECT_TRUE(OB_SUCCESS == cond.add_cond(table_name, row_key, column_name, LT, obj));
  EXPECT_TRUE(OB_SUCCESS == cond.add_cond(table_name, row_key, false));
  obj.set_int(1230);
  EXPECT_TRUE(OB_SUCCESS == cond.add_cond(table_name, row_key, column_name, GT, obj));
  EXPECT_TRUE(OB_SUCCESS == cond.add_cond(table_name, row_key, false));
  // succ
  ObMutator decode_mutator;
  ObGetParam org_param;
  ObGetParam decode_param;
  EXPECT_TRUE(OB_SUCCESS == ObMutatorParamDecoder::decode(mutator, schema, decode_mutator, org_param, decode_param));
  // decode mutator
  EXPECT_TRUE(decode_mutator.get_update_condition().get_count() == cond.get_count());
  // need return
  EXPECT_TRUE(org_param.get_cell_size() == count);
  // need prefetch
  EXPECT_TRUE(decode_param.get_cell_size() == (count + cond.get_count()));

  // failed
  table_name.assign(no_name, static_cast<int32_t>(strlen(no_name)));
  for (int64_t i = 0; i < count; ++i)
  {
    mutator.add(table_name, row_key, column_name, (int64_t)1, ObMutator::RETURN_NO_RESULT);
    mutator.add(table_name, row_key, column_name, i + 10, ObMutator::RETURN_UPDATE_RESULT);
  }
  org_param.reset();
  decode_param.reset();
  decode_mutator.reset();
  EXPECT_TRUE(OB_SUCCESS != ObMutatorParamDecoder::decode(mutator, schema, decode_mutator, org_param, decode_param));
}

