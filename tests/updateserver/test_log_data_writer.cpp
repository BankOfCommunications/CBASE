/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#include "test_base.h"
#include "common/ob_log_data_writer.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;
namespace oceanbase
{
  namespace test
  {
    struct Config
    {
      const char* log_dir;
      int64_t file_size;
      int64_t du_percent;
      Config()
      {
        log_dir = "log_dir";
        file_size = 1<<26;
        du_percent = 10;}
      
    };

    class ObLogDataWriterTest: public ::testing::Test, public Config {
      public:
        ObLogDataWriterTest(){}
        ~ObLogDataWriterTest(){}
      protected:
        ObLogDataWriter log_writer;
      protected:
        virtual void SetUp(){
          srandom(static_cast<unsigned int>(time(NULL)));
        }
        virtual void TearDown(){
        }
    };
    TEST_F(ObLogDataWriterTest, init)
    {
      ObLogDataWriter writer;
      ObLogCursor start_cursor, end_cursor;
      ASSERT_EQ(OB_NOT_INIT, writer.write(start_cursor, end_cursor, NULL, 0));
      ASSERT_EQ(OB_INVALID_ARGUMENT, writer.init(NULL, file_size, du_percent, 0, NULL));
      ASSERT_EQ(OB_SUCCESS, writer.init(log_dir, file_size, du_percent, 0, NULL));
      ASSERT_EQ(OB_INIT_TWICE, writer.init(log_dir, file_size, du_percent, 0, NULL));
    }
    TEST_F(ObLogDataWriterTest, select)
    {
      // ObLogDataWriter writer;
      // ObLogCursor start_cursor, end_cursor;
      // ASSERT_EQ(OB_NOT_INIT, writer.write(start_cursor, end_cursor, NULL, 0));
      // ASSERT_EQ(OB_INVALID_ARGUMENT, writer.init(NULL, file_size, du_percent, NULL));
      // ASSERT_EQ(OB_SUCCESS, writer.init(log_dir, file_size, du_percent, NULL));
      // ASSERT_EQ(OB_INIT_TWICE, writer.init(log_dir, file_size, du_percent, NULL));
    }
  }
}
