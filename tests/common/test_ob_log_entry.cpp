
#include <gtest/gtest.h>

#include "ob_log_entry.h"

#include "tbsys.h"
#include "ob_malloc.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace tests
  {
    namespace common
    {
      class TestObLogEntry: public ::testing::Test
      {
      public:
        virtual void SetUp()
        {
        }

        virtual void TearDown()
        {

        }

      };

      TEST_F(TestObLogEntry, test_fill_header)
      {
        ObLogEntry entry;
        char data[BUFSIZ];
        const int64_t buf_len = BUFSIZ + BUFSIZ;
        char buf[buf_len];
        int64_t buf_pos = 0;

        entry.seq_ = 123123;
        entry.cmd_ = OB_LOG_SWITCH_LOG;
        EXPECT_EQ(OB_INVALID_ARGUMENT, entry.fill_header(NULL, BUFSIZ));
        EXPECT_EQ(OB_INVALID_ARGUMENT, entry.fill_header(data, 0));
        EXPECT_EQ(OB_INVALID_ARGUMENT, entry.fill_header(data, -1));

        EXPECT_EQ(OB_SUCCESS, entry.fill_header(data, BUFSIZ));
        EXPECT_EQ(BUFSIZ, entry.get_log_data_len());
        EXPECT_EQ(OB_SUCCESS, entry.serialize(buf, buf_len, buf_pos));
        EXPECT_EQ(buf_pos, entry.get_serialize_size());

        ObLogEntry entry2;
        int64_t new_pos = 0;
        EXPECT_EQ(OB_SUCCESS, entry2.deserialize(buf, buf_len, new_pos));
        EXPECT_EQ(new_pos, buf_pos);
        EXPECT_EQ(BUFSIZ, entry2.get_log_data_len());
        EXPECT_EQ(OB_SUCCESS, entry2.check_data_integrity(data));
        data[2] ^= 0xff;
        EXPECT_EQ(OB_ERROR, entry2.check_data_integrity(data));
        data[2] ^= 0xff;
        EXPECT_EQ(OB_SUCCESS, entry2.check_data_integrity(data));
      }
    }
  }
}

int main(int argc, char** argv)
{
  TBSYS_LOGGER.setLogLevel("DEBUG");
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
