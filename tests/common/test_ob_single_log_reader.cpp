
#include <gtest/gtest.h>

#include "ob_repeated_log_reader.h"
#include "ob_log_writer.h"

#include "tbsys.h"

#include "slave_mgr_4_test.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace tests
  {
    namespace common
    {
      class TestObRepeatedLogReader: public ::testing::Test
      {
      public:
        virtual void SetUp()
        {
          strcpy(log_dir, "tmp");

          char cmd[100];
          snprintf(cmd, 100, "mkdir %s", log_dir);
          system(cmd);
        }

        virtual void TearDown()
        {
          char cmd[100];
          snprintf(cmd, 100, "rm -r %s", "tmp");
          system(cmd);

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

        char log_dir[BUFSIZ];
      };

      TEST_F(TestObRepeatedLogReader, test_tmp)
      {
        ObFileReader file;
        ASSERT_EQ(file.open(ObString(5, 5, (char*)"ttttt"), true), OB_FILE_NOT_EXIST);
      }

      TEST_F(TestObRepeatedLogReader, test_init)
      {
        ObRepeatedLogReader log_reader;
        char longf[BUFSIZ];
        memset(longf, 'A', BUFSIZ);
        longf[BUFSIZ - 1] = '\0';
        ASSERT_EQ(OB_INVALID_ARGUMENT, log_reader.init(NULL));
        ASSERT_EQ(OB_INVALID_ARGUMENT, log_reader.init(longf));
        ASSERT_EQ(OB_NOT_INIT, log_reader.open(100));
        ASSERT_EQ(OB_NOT_INIT, log_reader.close());

        // prepare log file 101
        int file1 = 101;
        ASSERT_EQ(0, create_file(file1));
        ASSERT_EQ(OB_SUCCESS, log_reader.init(log_dir));
        ASSERT_EQ(OB_INIT_TWICE, log_reader.init(log_dir));
        ASSERT_EQ(OB_SUCCESS, log_reader.reset());
        ASSERT_EQ(OB_SUCCESS, log_reader.init(log_dir));
        ASSERT_EQ(OB_SUCCESS, log_reader.open(file1));
        ASSERT_EQ(OB_SUCCESS, log_reader.close());
        ASSERT_EQ(0, erase_file(file1));

        LogCommand cmd1;
        uint64_t s1;
        char* log1;
        int64_t len1;
        int file2 = 12345;
        ASSERT_EQ(OB_SUCCESS, log_reader.close());
        ASSERT_EQ(OB_FILE_NOT_EXIST, log_reader.open(file2));
        // prepare log file file2
        ASSERT_EQ(0, create_file(file2));
        ASSERT_EQ(OB_SUCCESS, log_reader.open(file2));
        ASSERT_EQ(OB_READ_NOTHING, log_reader.read_log(cmd1, s1, log1, len1));
        ASSERT_EQ(OB_SUCCESS, log_reader.close());
        ASSERT_EQ(0, erase_file(file2));
      }

      TEST_F(TestObRepeatedLogReader, test_read_log)
      {
        fprintf(stderr, "============%lx %lx\n", ObLogWriter::LOG_FILE_ALIGN_MASK, ObLogWriter::LOG_FILE_ALIGN_SIZE);
        char buf[BUFSIZ];
        memset(buf, 0xAA, BUFSIZ);
        int64_t log_file_max_size = 1L << 24;
        uint64_t log_file_max_id = 1100;
        uint64_t log_max_seq = 22000;

        SlaveMgr4Test sm4t;

        ObLogWriter log_writer;
        ASSERT_EQ(OB_SUCCESS, log_writer.init("tmp", log_file_max_size, sm4t.get_slave_mgr(), 0));
        ASSERT_EQ(OB_SUCCESS, log_writer.start_log(log_file_max_id, log_max_seq));

        int single_file_item = 0;
        int flush_time = 0;
        const int time = 5000;
        for (int i = 0; i < time; i++)
        {
          int ret = log_writer.write_log(OB_LOG_UPS_MUTATOR, buf, BUFSIZ);
          if (OB_BUF_NOT_ENOUGH == ret)
          {
            if (log_writer.log_buffer_.get_position() / ObLogWriter::LOG_FILE_ALIGN_SIZE * ObLogWriter::LOG_FILE_ALIGN_SIZE != log_writer.log_buffer_.get_position()
                && 0 == single_file_item) flush_time++;
            //if ((log_writer.log_buffer_.get_position() & ObLogWriter::LOG_FILE_ALIGN_MASK) != log_writer.log_buffer_.get_position()) flush_time++;
            ASSERT_EQ(OB_SUCCESS, log_writer.flush_log());
            ASSERT_EQ(OB_SUCCESS, log_writer.write_log(OB_LOG_UPS_MUTATOR, buf, BUFSIZ));
          }
          if (log_writer.cur_log_file_id_ == log_file_max_id + 1 && 0 == single_file_item)
          {
            single_file_item = i;
          }
        }
        ASSERT_EQ(OB_SUCCESS, log_writer.flush_log());

        fprintf(stderr, "single_file_item=%d flush_time=%d\n", single_file_item, flush_time);
        ObRepeatedLogReader log_reader;
        LogCommand cmd1;
        uint64_t s1;
        char* buf1;
        int64_t len1;
        ASSERT_EQ(OB_SUCCESS, log_reader.init(log_dir));
        ASSERT_EQ(OB_SUCCESS, log_reader.open(log_file_max_id));
        for (int i = 0; i < single_file_item + flush_time; i++)
        {
          ASSERT_EQ(OB_SUCCESS, log_reader.read_log(cmd1, s1, buf1, len1));
          if (OB_LOG_UPS_MUTATOR == cmd1)
          {
            ASSERT_EQ(log_max_seq + i + 1, s1 + 1);
            ASSERT_EQ(BUFSIZ, len1);
            ASSERT_EQ(0, memcmp(buf1, buf, BUFSIZ));
          }
          else if (OB_LOG_NOP == cmd1)
          {
          }
          else
          {
            fprintf(stderr, "cmd=%d i=%d single_file_item=%d flush_time=%d\n", cmd1, i, single_file_item, flush_time);
            ASSERT_EQ(1, 0);
          }
        }
        ASSERT_EQ(OB_SUCCESS, log_reader.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_LOG_SWITCH_LOG, cmd1);
        uint64_t fid;
        int64_t ppos = 0;
        ASSERT_EQ(OB_SUCCESS, serialization::decode_i64(buf1, len1, ppos, (int64_t*)&fid));
        ASSERT_EQ(log_file_max_id, fid);
        ASSERT_EQ(OB_READ_NOTHING, log_reader.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_SUCCESS, log_reader.close());

        ASSERT_EQ(OB_SUCCESS, log_reader.open(log_file_max_id + 1));
        for (int i = 0; i < single_file_item + flush_time; i++)
        {
          ASSERT_EQ(OB_SUCCESS, log_reader.read_log(cmd1, s1, buf1, len1));
          if (OB_LOG_UPS_MUTATOR == cmd1)
          {
            ASSERT_EQ(log_max_seq + single_file_item + flush_time + i + 1, s1);
            ASSERT_EQ(BUFSIZ, len1);
            ASSERT_EQ(0, memcmp(buf1, buf, BUFSIZ));
          }
          else if (OB_LOG_NOP == cmd1)
          {
          }
          else
          {
            fprintf(stderr, "cmd=%d i=%d single_file_item=%d flush_time=%d\n", cmd1, i, single_file_item, flush_time);
            ASSERT_EQ(1, 0);
          }
        }
        ASSERT_EQ(OB_SUCCESS, log_reader.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_LOG_SWITCH_LOG, cmd1);
        ppos = 0;
        ASSERT_EQ(OB_SUCCESS, serialization::decode_i64(buf1, len1, ppos, (int64_t*)&fid));
        ASSERT_EQ(log_file_max_id + 1, fid);
        ASSERT_EQ(OB_READ_NOTHING, log_reader.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_SUCCESS, log_reader.close());

        const int data_len = 1024;
        char data[data_len];
        memset(data, 0x0A, data_len);
        ObLogEntry ten;
        ten.set_log_command(OB_LOG_UPS_MUTATOR);
        ten.set_log_seq(10101);
        ten.fill_header(data, data_len);

        char seb[BUFSIZ];
        int64_t sebp = 0;
        ASSERT_EQ(OB_SUCCESS, ten.serialize(seb, BUFSIZ, sebp));
        int lfd = open("tmp/10240", O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
        ASSERT_EQ(sebp, write(lfd, seb, sebp));
        ASSERT_EQ(data_len - 1, write(lfd, data, data_len - 1));
        ASSERT_EQ(0, close(lfd));
        ASSERT_EQ(OB_SUCCESS, log_reader.open(10240));
        ASSERT_EQ(OB_READ_NOTHING, log_reader.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_READ_NOTHING, log_reader.read_log(cmd1, s1, buf1, len1));
        lfd = open("tmp/10240", O_WRONLY | O_APPEND, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
        ASSERT_EQ(1, write(lfd, data + data_len - 1, 1));
        ASSERT_EQ(OB_SUCCESS, log_reader.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ((uint64_t)10101, s1);
        ASSERT_EQ(OB_LOG_UPS_MUTATOR, cmd1);
        ASSERT_EQ(data_len, len1);
        ASSERT_EQ(OB_READ_NOTHING, log_reader.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_SUCCESS, log_reader.close());
      }

      TEST_F(TestObRepeatedLogReader, test_read_log2)
      {
        char buf[BUFSIZ];
        memset(buf, 0xAA, BUFSIZ);
        uint64_t log_file_max_id = 10;

        SlaveMgr4Test sm4t;

        ObLogEntry entry;
        entry.set_log_seq(1100U);
        entry.set_log_command(OB_LOG_UPS_MUTATOR);
        entry.fill_header(buf, BUFSIZ);

        char outbuf[BUFSIZ];
        int64_t pos = 0;
        entry.serialize(outbuf, BUFSIZ, pos);
        pos--;

        FILE* outfp = fopen("tmp/10", "w");
        fwrite(outbuf, 1, pos, outfp);
        fclose(outfp);

        ObRepeatedLogReader log_reader;
        LogCommand cmd1;
        uint64_t s1;
        char* buf1;
        int64_t len1;
        ASSERT_EQ(OB_SUCCESS, log_reader.init("tmp"));
        ASSERT_EQ(OB_SUCCESS, log_reader.open(log_file_max_id));
        ASSERT_EQ(OB_READ_NOTHING, log_reader.read_log(cmd1, s1, buf1, len1));
        ASSERT_EQ(OB_SUCCESS, log_reader.close());

      }

      TEST_F(TestObRepeatedLogReader, test_corruption)
      {
        FILE* outfp = fopen("tmp/3333", "w");
        char outbuf[10];
        memset(outbuf, 0x00, 10);
        fwrite(outbuf, 1, 10, outfp);
        fclose(outfp);

        int64_t log_file_max_size = 1L << 24;
        uint64_t log_max_seq = 12;
        char buf[BUFSIZ];
        memset(buf, 0xAA, BUFSIZ);

        SlaveMgr4Test sm4t;

        ObLogWriter log_writer;
        ASSERT_EQ(OB_SUCCESS, log_writer.init("tmp", log_file_max_size, sm4t.get_slave_mgr(), 1));
        ASSERT_EQ(OB_LOG_NOT_ALIGN, log_writer.start_log(3333, log_max_seq));

        ASSERT_EQ(log_writer.write_log(OB_LOG_UPS_MUTATOR, buf, BUFSIZ), OB_SUCCESS);

        ASSERT_EQ(OB_SUCCESS, log_writer.flush_log());

        ObRepeatedLogReader log_reader;
        LogCommand cmd1;
        uint64_t s1;
        char* buf1;
        int64_t len1;
        ASSERT_EQ(OB_SUCCESS, log_reader.init("tmp"));
        ASSERT_EQ(OB_SUCCESS, log_reader.open(3333));
        ASSERT_EQ(OB_ERROR, log_reader.read_log(cmd1, s1, buf1, len1));
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
