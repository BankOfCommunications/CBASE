
#include <gtest/gtest.h>

#include "ob_log_dir_scanner.h"

#include "tbsys.h"
#include "ob_malloc.h"
#include <string>

using namespace oceanbase::common;
using namespace std;

namespace oceanbase
{
  namespace tests
  {
    namespace common
    {
      class TestObLogDirScanner: public ::testing::Test
      {
      public:
        virtual void SetUp()
        {
          log_dir = "tmp1234";

          char cmd[BUFSIZ];
          snprintf(cmd, BUFSIZ, "mkdir %s", log_dir.c_str());
          system(cmd);
        }

        virtual void TearDown()
        {

          char cmd[BUFSIZ];
          snprintf(cmd, BUFSIZ, "rm -r %s", log_dir.c_str());
          system(cmd);
        }

        int create_file(uint64_t id)
        {
          char cmd[BUFSIZ];
          snprintf(cmd, BUFSIZ, "touch %s/%lu", log_dir.c_str(), id);
          return system(cmd);
        }
        int create_file(char* file)
        {
          char cmd[BUFSIZ];
          snprintf(cmd, BUFSIZ, "touch %s/%s", log_dir.c_str(), file);
          return system(cmd);
        }

        int erase_file(uint64_t id)
        {
          char cmd[BUFSIZ];
          snprintf(cmd, BUFSIZ, "rm -f %s/%lu", log_dir.c_str(), id);
          return system(cmd);
        }

        string log_dir;
      };

      TEST_F(TestObLogDirScanner, test_init)
      {
        uint64_t start = 10101010101010;
        create_file(start);
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"noise1"));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"noise2"));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"noise3"));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"noise4"));
        ObLogDirScanner scanner;
        EXPECT_EQ(OB_SUCCESS, scanner.init(log_dir.c_str()));

        uint64_t log_id;
        EXPECT_EQ(OB_SUCCESS, scanner.get_min_log_id(log_id));
        EXPECT_EQ(start, log_id);
        EXPECT_EQ(OB_SUCCESS, scanner.get_max_log_id(log_id));
        EXPECT_EQ(start, log_id);

        uint64_t ckpt_id;
        EXPECT_EQ(OB_ENTRY_NOT_EXIST, scanner.get_max_ckpt_id(ckpt_id));
        EXPECT_EQ(0U, ckpt_id);
        EXPECT_FALSE(scanner.has_ckpt());
      }

      TEST_F(TestObLogDirScanner, test_init2)
      {
        int start = 1010;
        for (int i = 0; i < 200; i++)
        {
          create_file(i + start);
        }
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"noise1"));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"noise2"));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"noise3"));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"noise4"));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"4.checkpoint"));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"400.checkpoint"));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"1400.checkpoint"));
        ObLogDirScanner scanner;
        EXPECT_EQ(OB_SUCCESS, scanner.init(log_dir.c_str()));

        uint64_t log_id;
        EXPECT_EQ(OB_SUCCESS, scanner.get_min_log_id(log_id));
        EXPECT_EQ((uint64_t)start, log_id);
        EXPECT_EQ(OB_SUCCESS, scanner.get_max_log_id(log_id));
        EXPECT_EQ(uint64_t(start + 199), log_id);

        uint64_t ckpt_id;
        EXPECT_EQ(OB_SUCCESS, scanner.get_max_ckpt_id(ckpt_id));
        EXPECT_EQ(uint64_t(1400), ckpt_id);
        EXPECT_EQ(true, scanner.has_ckpt());
      }

      TEST_F(TestObLogDirScanner, test_init3)
      {
        uint64_t start = 1010;
        for (uint64_t i = 0U; i < 100; i+=2)
        {
          create_file(i + start);
        }
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"noise1"));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"noise2"));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"noise3"));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"44444."));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)" 44444"));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"444o44"));
        ObLogDirScanner scanner;
        EXPECT_EQ(OB_DISCONTINUOUS_LOG, scanner.init(log_dir.c_str()));

        uint64_t log_id;
        EXPECT_EQ(OB_SUCCESS, scanner.get_min_log_id(log_id));
        EXPECT_EQ(start + 100 - 2, log_id);
        EXPECT_EQ(OB_SUCCESS, scanner.get_max_log_id(log_id));
        EXPECT_EQ(start + 100 - 2, log_id);

        uint64_t ckpt_id;
        EXPECT_EQ(OB_ENTRY_NOT_EXIST, scanner.get_max_ckpt_id(ckpt_id));
        EXPECT_EQ(0U, ckpt_id);
        EXPECT_FALSE(scanner.has_ckpt());
      }

      TEST_F(TestObLogDirScanner, test_init4)
      {
        uint64_t start = 1010;
        for (uint64_t i = 0U; i < 100; i++)
        {
          create_file(i + start);
        }
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"44444444"));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"noise1"));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"noise2"));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"noise3"));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"44444."));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)" 44444"));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"444o44"));
        ObLogDirScanner scanner;
        EXPECT_EQ(OB_DISCONTINUOUS_LOG, scanner.init(log_dir.c_str()));

        uint64_t log_id;
        EXPECT_EQ(OB_SUCCESS, scanner.get_min_log_id(log_id));
        EXPECT_EQ((uint64_t)44444444, log_id);
        EXPECT_EQ(OB_SUCCESS, scanner.get_max_log_id(log_id));
        EXPECT_EQ((uint64_t)44444444, log_id);

        uint64_t ckpt_id;
        EXPECT_EQ(OB_ENTRY_NOT_EXIST, scanner.get_max_ckpt_id(ckpt_id));
        EXPECT_EQ(0U, ckpt_id);
        EXPECT_FALSE(scanner.has_ckpt());
      }

      TEST_F(TestObLogDirScanner, test_init5)
      {
        uint64_t start = 1010;
        for (uint64_t i = 0U; i < 100; i++)
        {
          create_file(i + start);
        }
        for (uint64_t i = 200U; i < 300; i++)
        {
          create_file(i + start);
        }
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"noise1"));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"noise2"));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"noise3"));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"44444."));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)" 44444"));
        EXPECT_EQ(OB_SUCCESS, create_file((char*)"444o44"));
        ObLogDirScanner scanner;
        EXPECT_EQ(OB_DISCONTINUOUS_LOG, scanner.init(log_dir.c_str()));

        uint64_t log_id;
        EXPECT_EQ(OB_SUCCESS, scanner.get_min_log_id(log_id));
        EXPECT_EQ(start + 200, log_id);
        EXPECT_EQ(OB_SUCCESS, scanner.get_max_log_id(log_id));
        EXPECT_EQ(start + 300 - 1, log_id);

        uint64_t ckpt_id;
        EXPECT_EQ(OB_ENTRY_NOT_EXIST, scanner.get_max_ckpt_id(ckpt_id));
        EXPECT_EQ(0U, ckpt_id);
        EXPECT_FALSE(scanner.has_ckpt());
      }
      
      TEST_F(TestObLogDirScanner, test_init6)
      {
        ObLogDirScanner scanner;
        EXPECT_EQ(OB_SUCCESS, scanner.init(log_dir.c_str()));

        uint64_t log_id;
        EXPECT_EQ(OB_ENTRY_NOT_EXIST, scanner.get_min_log_id(log_id));
        EXPECT_EQ(0U, log_id);
        EXPECT_EQ(OB_ENTRY_NOT_EXIST, scanner.get_max_log_id(log_id));
        EXPECT_EQ(0U, log_id);

        uint64_t ckpt_id;
        EXPECT_EQ(OB_ENTRY_NOT_EXIST, scanner.get_max_ckpt_id(ckpt_id));
        EXPECT_EQ(0U, ckpt_id);
        EXPECT_FALSE(scanner.has_ckpt());
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
