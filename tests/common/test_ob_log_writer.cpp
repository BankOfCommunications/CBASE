
#include <gtest/gtest.h>
#include "slave_mgr_4_test.h"

#include "ob_log_writer.h"
#include "ob_file.h"

#include "tbsys.h"
#include "ob_malloc.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace tests
  {
    namespace common
    {
      class TestObLogWriter: public ::testing::Test
      {
      public:
        virtual void SetUp()
        {
          strcpy(log_dir, "tmp");
          log_file_max_size = 1L << 28;
          log_file_max_id = 101;
          log_max_seq = 10101;
        }

        virtual void TearDown()
        {

        }

        int create_file(int id)
        {
          char cmd[BUFSIZ];
          snprintf(cmd, BUFSIZ, "touch %s/%d", log_dir, id);
          return system(cmd);
        }

        int erase_file(int id)
        {
          char cmd[BUFSIZ];
          snprintf(cmd, BUFSIZ, "rm -f %s/%d", log_dir, id);
          return system(cmd);
        }

        int64_t get_write_size(int64_t size)
        {
          int64_t align_size = (size & ObLogWriter::LOG_FILE_ALIGN_MASK) + ObLogWriter::LOG_FILE_ALIGN_SIZE;
          if (align_size - size < ObLogEntry::get_header_size()) align_size += ObLogWriter::LOG_FILE_ALIGN_SIZE;
          return align_size;
        }

        char log_dir[BUFSIZ];
        int64_t log_file_max_size;
        uint64_t log_file_max_id;
        uint64_t log_max_seq;
        uint64_t log_file_id;
      };

      TEST_F(TestObLogWriter, test_appender)
      {
        ObFileAppender ap;
        ASSERT_EQ(ap.open(ObString(5, 5, (char*)"00000"), true, true), OB_SUCCESS);
        //int fd  = open("00000", O_WRONLY | O_CREAT, S_IRWXU);
        char buf[BUFSIZ];
        memset(buf, 0x0A, BUFSIZ);
//        while (true)
        {
          ASSERT_EQ(ap.append(buf, BUFSIZ - 11, true), OB_SUCCESS);
          //ASSERT_EQ(write(fd, buf, BUFSIZ - 11), BUFSIZ - 11);
          //fsync(fd);
        }
        //ap.close();
      }

      TEST_F(TestObLogWriter, test_init)
      {
        ObLogWriter log_writer;

        char buf[BUFSIZ];
        uint64_t new_log_file_id;

        ObSlaveMgr slave_mgr;
        EXPECT_EQ(OB_NOT_INIT, log_writer.start_log(1, 2));
        EXPECT_EQ(OB_NOT_INIT, log_writer.start_log(1));
        EXPECT_EQ(OB_NOT_INIT, log_writer.write_log(OB_LOG_UPS_MUTATOR, buf, BUFSIZ));
        EXPECT_EQ(OB_NOT_INIT, log_writer.flush_log());
        EXPECT_EQ(OB_NOT_INIT, log_writer.store_log(buf, BUFSIZ));
        EXPECT_EQ(OB_NOT_INIT, log_writer.switch_log_file(new_log_file_id));
        EXPECT_EQ(OB_NOT_INIT, log_writer.switch_to_log_file(123));
        EXPECT_EQ(OB_NOT_INIT, log_writer.write_checkpoint_log(log_file_id));

        EXPECT_EQ(OB_INVALID_ARGUMENT, log_writer.init(NULL, log_file_max_size, &slave_mgr, 0));
        EXPECT_EQ(OB_INVALID_ARGUMENT, log_writer.init(log_dir, log_file_max_size, NULL, 1));
        EXPECT_EQ(OB_INVALID_ARGUMENT, log_writer.init(NULL, log_file_max_size, NULL, 2));

        for (int i = 0; i < BUFSIZ; i++)
        {
          log_dir[i] = '0';
        }
        log_dir[BUFSIZ - 1] = '\0';
        EXPECT_EQ(OB_INVALID_ARGUMENT, log_writer.init(log_dir, log_file_max_size, &slave_mgr, 0));

        strcpy(log_dir, "tmp1234");
        EXPECT_EQ(OB_SUCCESS, log_writer.init(log_dir, log_file_max_size, &slave_mgr, 0));
        EXPECT_EQ(OB_INIT_TWICE, log_writer.init(log_dir, log_file_max_size, &slave_mgr, 0));
        EXPECT_EQ(OB_SUCCESS, log_writer.start_log(log_file_max_id, log_max_seq));
        ASSERT_EQ(0, log_writer.log_sync_type_);
        ASSERT_EQ(0, log_writer.cur_log_size_);
        ASSERT_EQ(log_file_max_size, log_writer.log_file_max_size_);
        ASSERT_EQ(log_file_max_id, log_writer.cur_log_file_id_);
        ASSERT_EQ(log_max_seq, log_writer.cur_log_seq_);
        ASSERT_EQ(&slave_mgr, log_writer.slave_mgr_);
        system("rm -r tmp1234");
      }

      TEST_F(TestObLogWriter, test_write_log)
      {
        SlaveMgr4Test sm4t;

        ObLogWriter log_writer;
        strcpy(log_dir, "tmp1234");
        EXPECT_EQ(OB_SUCCESS, log_writer.init(log_dir, log_file_max_size, sm4t.get_slave_mgr(), 0));
        EXPECT_EQ(OB_SUCCESS, log_writer.start_log(log_file_max_id, log_max_seq));

        char buf[BUFSIZ];
        EXPECT_EQ(OB_INVALID_ARGUMENT, log_writer.write_log(OB_LOG_UPS_MUTATOR, NULL, BUFSIZ));
        EXPECT_EQ(OB_INVALID_ARGUMENT, log_writer.write_log(OB_LOG_UPS_MUTATOR, buf, 0));
        EXPECT_EQ(OB_INVALID_ARGUMENT, log_writer.write_log(OB_LOG_UPS_MUTATOR, buf, -1));

        EXPECT_EQ(OB_SUCCESS, log_writer.write_log(OB_LOG_UPS_MUTATOR, buf, BUFSIZ));
        EXPECT_EQ(OB_SUCCESS, log_writer.write_log(OB_LOG_UPS_MUTATOR, 0, 0));

        ObLogEntry entry;
        int64_t pos = 0;
        EXPECT_EQ(0, strncmp(log_writer.log_buffer_.get_data() + entry.get_serialize_size(), buf, BUFSIZ));
        EXPECT_EQ(OB_SUCCESS, entry.deserialize(log_writer.log_buffer_.get_data(),
              log_writer.log_buffer_.get_position(), pos));
        EXPECT_EQ(BUFSIZ, entry.get_log_data_len());
        EXPECT_EQ(log_max_seq, entry.seq_);

        EXPECT_EQ(OB_SUCCESS, log_writer.flush_log());

        int t = static_cast<int32_t>((ObLogWriter::LOG_BUFFER_SIZE - ObLogWriter::LOG_FILE_ALIGN_SIZE) / (BUFSIZ + entry.get_serialize_size()));
        for (int i = 0; i < t; i++)
        {
          EXPECT_EQ(OB_SUCCESS, log_writer.write_log(OB_LOG_UPS_MUTATOR, buf, BUFSIZ));
        }
        EXPECT_EQ(OB_BUF_NOT_ENOUGH, log_writer.write_log(OB_LOG_UPS_MUTATOR, buf, BUFSIZ));
        EXPECT_EQ(OB_SUCCESS, log_writer.flush_log());
        EXPECT_EQ(OB_SUCCESS, log_writer.write_log(OB_LOG_UPS_MUTATOR, buf, BUFSIZ));
        EXPECT_EQ(OB_SUCCESS, log_writer.flush_log());

        EXPECT_EQ(OB_INVALID_ARGUMENT, log_writer.store_log(NULL, BUFSIZ));
        EXPECT_EQ(OB_INVALID_ARGUMENT, log_writer.store_log(buf, 0));
        EXPECT_EQ(OB_INVALID_ARGUMENT, log_writer.store_log(buf, -1));

        for (int i = 0; i < 10; i++)
        {
          EXPECT_EQ(OB_SUCCESS, log_writer.store_log(buf, BUFSIZ));
        }

        log_file_id = log_writer.cur_log_file_id_;
        log_writer.log_file_max_size_ = log_writer.cur_log_size_;

        EXPECT_EQ(OB_SUCCESS, log_writer.write_log(OB_LOG_UPS_MUTATOR, buf, BUFSIZ));
        EXPECT_EQ(log_file_id + 1, log_writer.cur_log_file_id_);
        EXPECT_EQ(OB_SUCCESS, log_writer.flush_log());

        EXPECT_EQ(OB_SUCCESS, log_writer.switch_to_log_file(log_writer.cur_log_file_id_ + 1));
        EXPECT_EQ(0, log_writer.cur_log_size_); 
        EXPECT_EQ(OB_ERROR, log_writer.switch_to_log_file(log_writer.cur_log_file_id_ + 2));

        uint64_t log_seq = log_writer.cur_log_seq_;
        uint64_t file_id = 0;
        EXPECT_EQ(OB_SUCCESS, log_writer.write_checkpoint_log(file_id));
        EXPECT_EQ(log_seq + 3, log_writer.cur_log_seq_);
        EXPECT_EQ(file_id + 1, log_writer.cur_log_file_id_);

        system("rm -r tmp1234");
      }

      TEST_F(TestObLogWriter, test_write_and_flush_log)
      {
        SlaveMgr4Test sm4t;

        ObLogWriter log_writer;
        strcpy(log_dir, "tmp1234");
        EXPECT_EQ(OB_SUCCESS, log_writer.init(log_dir, log_file_max_size, sm4t.get_slave_mgr(), 0));
        EXPECT_EQ(OB_SUCCESS, log_writer.start_log(log_file_max_id, log_max_seq));

        ObLogEntry entry;

        char buf[BUFSIZ];
        EXPECT_EQ(OB_INVALID_ARGUMENT, log_writer.write_and_flush_log(OB_LOG_UPS_MUTATOR, NULL, BUFSIZ));
        EXPECT_EQ(OB_INVALID_ARGUMENT, log_writer.write_and_flush_log(OB_LOG_UPS_MUTATOR, buf, 0));
        EXPECT_EQ(OB_INVALID_ARGUMENT, log_writer.write_and_flush_log(OB_LOG_UPS_MUTATOR, buf, -1));

        //EXPECT_EQ(OB_ERROR, log_writer.write_and_flush_log(OB_LOG_UPS_MUTATOR, buf, 111 - entry.get_serialize_size()));

        EXPECT_EQ(OB_SUCCESS, log_writer.write_and_flush_log(OB_LOG_UPS_MUTATOR, buf, BUFSIZ));


        EXPECT_EQ(log_writer.cur_log_size_, get_write_size(BUFSIZ + entry.get_serialize_size()));

        int t = static_cast<int32_t>(ObLogWriter::LOG_BUFFER_SIZE / get_write_size(BUFSIZ + entry.get_serialize_size()));
        for (int i = 0; i < t; i++)
        {
          EXPECT_EQ(OB_SUCCESS, log_writer.write_and_flush_log(OB_LOG_UPS_MUTATOR, buf, BUFSIZ));
        }

        log_file_id = log_writer.cur_log_file_id_;
        log_writer.log_file_max_size_ = log_writer.cur_log_size_;

        EXPECT_EQ(OB_SUCCESS, log_writer.write_and_flush_log(OB_LOG_UPS_MUTATOR, buf, BUFSIZ));
        EXPECT_EQ(log_file_id + 1, log_writer.cur_log_file_id_);

        EXPECT_EQ(OB_SUCCESS, log_writer.switch_to_log_file(log_writer.cur_log_file_id_ + 1));
        EXPECT_EQ(0, log_writer.cur_log_size_); 
        EXPECT_EQ(OB_ERROR, log_writer.switch_to_log_file(log_writer.cur_log_file_id_ + 2));

        system("rm -r tmp1234");
      }

      TEST_F(TestObLogWriter, test_align)
      {
        SlaveMgr4Test sm4t;

        ObLogWriter log_writer;
        strcpy(log_dir, "tmp1234");
        EXPECT_EQ(OB_SUCCESS, log_writer.init(log_dir, log_file_max_size, sm4t.get_slave_mgr(), 0));
        EXPECT_EQ(OB_SUCCESS, log_writer.start_log(log_file_max_id, log_max_seq));

        char buf[BUFSIZ];

        ObLogEntry entry;
        for (int i = 0; i < 10; i++)
        {
          EXPECT_EQ(OB_SUCCESS, log_writer.write_and_flush_log(OB_LOG_UPS_MUTATOR, buf, BUFSIZ));
          EXPECT_EQ(log_writer.cur_log_size_ / ObLogWriter::LOG_FILE_ALIGN_SIZE * ObLogWriter::LOG_FILE_ALIGN_SIZE,
              log_writer.cur_log_size_);
        }

        system("rm -r tmp1234");
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
