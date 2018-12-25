
#include <gtest/gtest.h>

#include "ob_repeated_log_reader.h"
#include "ob_log_reader.h"
#include "ob_log_writer.h"

#include "slave_mgr_4_test.h"

#include "tbsys.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace tests
  {
    namespace common
    {
      class TestObLogReader: public ::testing::Test
      {
      public:
        virtual void SetUp()
        {
          strcpy(log_dir, "tmp1234");

          char cmd[100];
          snprintf(cmd, 100, "mkdir %s", log_dir);
          system(cmd);
        }

        virtual void TearDown()
        {
          char cmd[100];
          snprintf(cmd, 100, "rm -r %s", log_dir);
          system(cmd);
        }

        char log_dir[BUFSIZ];
      };

      TEST_F(TestObLogReader, test_init)
      {
        ObRepeatedLogReader repeated_reader;
        ObLogReader log_reader;
        char lf[BUFSIZ];
        memset(lf, 'A', BUFSIZ);
        lf[BUFSIZ - 1] = '\0';
        ASSERT_EQ(OB_INVALID_ARGUMENT, log_reader.init(&repeated_reader, lf, 0, 0, false));
        ASSERT_EQ(OB_INVALID_ARGUMENT, log_reader.init(&repeated_reader, NULL, 0, 0, false));

        int64_t log_file_max_size = 1L << 24;
        uint64_t log_file_max_id = 1100;
        uint64_t log_max_seq = 22000;
        ObLogCursor start_cursor;

        SlaveMgr4Test sm4t;

        uint64_t new_log_file_id;
        ObLogWriter log_writer;
        ASSERT_EQ(OB_SUCCESS, log_writer.init(log_dir, log_file_max_size, sm4t.get_slave_mgr(), 0));
        ASSERT_EQ(OB_SUCCESS, log_writer.start_log(set_cursor(start_cursor, log_file_max_id, log_max_seq, 0)));
        ASSERT_EQ(OB_SUCCESS, log_writer.switch_log_file(new_log_file_id));
        ASSERT_EQ(OB_SUCCESS, log_writer.switch_log_file(new_log_file_id));
        ASSERT_EQ(OB_SUCCESS, log_writer.write_log(OB_LOG_UPS_MUTATOR, lf, 100));
        ASSERT_EQ(OB_SUCCESS, log_writer.flush_log());
        ASSERT_EQ(OB_SUCCESS, log_writer.switch_log_file(new_log_file_id));
        ASSERT_EQ(OB_SUCCESS, log_writer.write_log(OB_LOG_UPS_MUTATOR, lf, 100));
        ASSERT_EQ(OB_SUCCESS, log_writer.flush_log());
        ASSERT_EQ(OB_SUCCESS, log_writer.switch_log_file(new_log_file_id));


        LogCommand cmd1;
        uint64_t s1;
        char* buf1;
        int64_t len1;
        ObLogEntry entry;
        ASSERT_EQ(OB_SUCCESS, log_reader.init(&repeated_reader, log_dir, log_file_max_id, 0, false));

        ASSERT_EQ(OB_SUCCESS, log_reader.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_SUCCESS, log_reader.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_SUCCESS, log_reader.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_LOG_UPS_MUTATOR, cmd1);
        ASSERT_EQ(log_max_seq + 3, s1 + 1);
        ASSERT_EQ(100, len1);
        ASSERT_EQ(0, memcmp(buf1, lf, 100));

        ASSERT_EQ(OB_SUCCESS, log_reader.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_SUCCESS, log_reader.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_SUCCESS, log_reader.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_LOG_UPS_MUTATOR, cmd1);
        ASSERT_EQ(log_max_seq + 6, s1 + 1);
        ASSERT_EQ(100, len1);
        ASSERT_EQ(0, memcmp(buf1, lf, 100));

        ASSERT_EQ(OB_SUCCESS, log_reader.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_SUCCESS, log_reader.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_READ_NOTHING, log_reader.read_log(cmd1, s1, buf1, len1));

        ObLogReader log_reader3;
        ObRepeatedLogReader repeated_reader3;
        ASSERT_EQ(OB_SUCCESS, log_reader3.init(&repeated_reader3, log_dir, log_file_max_id, 0, true));
        log_reader3.set_max_log_file_id(0);
        ASSERT_EQ(OB_READ_NOTHING, log_reader3.read_log(cmd1, s1, buf1, len1));
        log_reader3.set_max_log_file_id(log_file_max_id);
        ASSERT_EQ(OB_SUCCESS, log_reader3.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_READ_NOTHING, log_reader3.read_log(cmd1, s1, buf1, len1));
        log_reader3.set_max_log_file_id(log_file_max_id + 1);
        ASSERT_EQ(OB_SUCCESS, log_reader3.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_READ_NOTHING, log_reader3.read_log(cmd1, s1, buf1, len1));
        log_reader3.set_max_log_file_id(log_file_max_id + 2);

        ASSERT_EQ(OB_SUCCESS, log_reader3.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_LOG_UPS_MUTATOR, cmd1);
        ASSERT_EQ(log_max_seq + 3, s1 + 1);
        ASSERT_EQ(100, len1);
        ASSERT_EQ(0, memcmp(buf1, lf, 100));

        ASSERT_EQ(OB_SUCCESS, log_reader3.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_SUCCESS, log_reader3.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_READ_NOTHING, log_reader3.read_log(cmd1, s1, buf1, len1));

        log_reader3.set_max_log_file_id(log_file_max_id + 3);

        ASSERT_EQ(OB_SUCCESS, log_reader3.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_LOG_UPS_MUTATOR, cmd1);
        ASSERT_EQ(log_max_seq + 6, s1 + 1);
        ASSERT_EQ(100, len1);
        ASSERT_EQ(0, memcmp(buf1, lf, 100));

        ASSERT_EQ(OB_SUCCESS, log_reader3.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_SUCCESS, log_reader3.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_READ_NOTHING, log_reader3.read_log(cmd1, s1, buf1, len1));

        log_reader3.set_max_log_file_id(log_file_max_id + 4);

        ASSERT_EQ(OB_READ_NOTHING, log_reader3.read_log(cmd1, s1, buf1, len1));

        ObLogWriter *log_writer2 = new ObLogWriter();
        ASSERT_EQ(OB_SUCCESS, log_writer2->init(log_dir, log_file_max_size, sm4t.get_slave_mgr(), 0));
        ASSERT_EQ(OB_SUCCESS, log_writer2->start_log(set_cursor(start_cursor, log_file_max_id - 10, log_max_seq, 0)));
        ASSERT_EQ(OB_SUCCESS, log_writer2->write_log(OB_LOG_UPS_MUTATOR, lf, 100));
        ASSERT_EQ(OB_SUCCESS, log_writer2->flush_log());
        ASSERT_EQ(OB_SUCCESS, log_writer2->switch_log_file(new_log_file_id));
        delete log_writer2;

        char cmd[100];
        snprintf(cmd, 100, "rm %s/%lu", log_dir, log_file_max_id - 9);
        system(cmd);

        ObLogReader log_reader2;
        ObRepeatedLogReader repeated_reader2;
        ASSERT_EQ(OB_SUCCESS, log_reader2.init(&repeated_reader2, log_dir, log_file_max_id - 10, 0, false));
        ASSERT_EQ(OB_SUCCESS, log_reader2.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_LOG_UPS_MUTATOR, cmd1);
        ASSERT_EQ(log_max_seq + 1, s1 + 1);
        ASSERT_EQ(100, len1);

        ASSERT_EQ(OB_SUCCESS, log_reader2.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_SUCCESS, log_reader2.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_FILE_NOT_EXIST, log_reader2.read_log(cmd1, s1, buf1, len1));
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
