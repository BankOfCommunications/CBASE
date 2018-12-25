/*
 * Author:
 *   dragon <longf.sdc@sdc.com>
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <iostream>
#include <gtest/gtest.h>
#include "common/ob_trace_log.h"
#include "common/ob_malloc.h"

#define STR_BOOL(b) ((b) ? "true" : "false")
#define KB 1024
#define MAX_SIZE (4 * KB - 1)
#define TEST_TIMES 20

using namespace oceanbase;
using namespace common;
using namespace std;

namespace oceanbase
{
  namespace common
  {
    static int counter = 0;
    enum TYPE
    {
      UP_CHAR = 1,
      LOW_CHAR,
      NUM
    };

    /**
     * @brief getRandom
     * 生成1~max随机数字
     * @return
     */
    int getRandom(int max)
    {
      srand((unsigned int)time(NULL));
      return rand() % max + 1;
    }

    /**
     * @brief getLog
     * @param buf
     * @note 调用者负责释放申请的空间
     * @return 调用成功返回true,buf为申请到空间,内部填充随机数据；调用失败返回false
     */
    bool getLog(char *&buf)
    {
      int logSzKB = getRandom(MAX_SIZE); // 1B ~  4KB-1
      TBSYS_LOG(DEBUG, "alloc buf which size is %dB\n", logSzKB);

      if(NULL == buf)
      {
        void *addr = ob_tc_malloc(logSzKB, ObModIds::TEST_OB_TRACE_LOG);
        if(NULL == addr)
          return false;
        else
        {
          buf = new (addr) char[logSzKB];
        }
      }

      static char alphaNum[] = "1234567890"
                               "abcdefghijklmnopqrstuvwxyz"
                               "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                               "!@#$%^&*()-=~`+><,.;:'|";

      for(int i = 0; i < logSzKB - 1; i++)
      {
        buf[i] = alphaNum[rand() % (sizeof(alphaNum) - 1)];
      }

      buf[logSzKB-1] = '\0';
      return true;
    }

    TEST(TraceLogTest, LOGBUFOVERFLOW)
    {
      SET_FORCE_TRACE_LOG(true);
      counter = 0;
      EXPECT_TRUE(TraceLog::get_logbuffer().force_log_);
      while(true)
      {
        char *log = NULL;
        bool success = getLog(log);
        ASSERT_TRUE(success);
        sleep(1);
        FILL_TRACE_LOG("the %dth filled log is %s", ++counter, log);
        if(log)
        {
          ob_tc_free(log);
          log = NULL;
        }
        if(counter >= TEST_TIMES - 1) break;
      }
      PRINT_TRACE_LOG();
    }
  }
}

int main(int argc, char *argv[])
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
