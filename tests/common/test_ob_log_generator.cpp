#include "gtest/gtest.h"
#include "common/ob_malloc.h"
#include "common/ob_log_generator.h"
#include "common/utility.h"
#include "../updateserver/rwt.h"

namespace oceanbase
{
  namespace test
  {
    static int parse_log_buffer(const char* log_data, int64_t data_len, const ObLogCursor& start_cursor, ObLogCursor& end_cursor)
    {
      int err = OB_SUCCESS;
      int64_t pos = 0;
      int64_t tmp_pos = 0;
      int64_t file_id = 0;
      ObLogEntry log_entry;
      end_cursor = start_cursor;
      if (NULL == log_data || data_len <= 0 || !start_cursor.is_valid())
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "invalid argument, log_data=%p, data_len=%ld, start_cursor=%s",
                  log_data, data_len, start_cursor.to_str());
      }

      while (OB_SUCCESS == err && pos < data_len)
      {
        if (OB_SUCCESS != (err = log_entry.deserialize(log_data, data_len, pos)))
        {
          TBSYS_LOG(ERROR, "log_entry.deserialize(log_data=%p, data_len=%ld, pos=%ld)=>%d", log_data, data_len, pos, err);
        }
        else if (OB_SUCCESS != (err = log_entry.check_data_integrity(log_data + pos)))
        {
          TBSYS_LOG(ERROR, "log_entry.check_data_integrity()=>%d", err);
        }
        else
        {
          tmp_pos = pos;
        }

        if (OB_SUCCESS != err)
        {}
        else if (OB_LOG_SWITCH_LOG == log_entry.cmd_
                 && !(OB_SUCCESS == (err = serialization::decode_i64(log_data, data_len, tmp_pos, (int64_t*)&file_id)
                                     && start_cursor.log_id_ == file_id)))
        {
          TBSYS_LOG(ERROR, "decode switch_log failed(log_data=%p, data_len=%ld, pos=%ld)=>%d", log_data, data_len, tmp_pos, err);
        }
        else
        {
          pos += log_entry.get_log_data_len();
          if (OB_SUCCESS != (err = end_cursor.advance(log_entry)))
          {
            TBSYS_LOG(ERROR, "end_cursor[%ld].advance(%ld)=>%d", end_cursor.log_id_, log_entry.seq_, err);
          }
        }
      }
      if (OB_SUCCESS == err && pos != data_len)
      {
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "pos[%ld] != data_len[%ld]", pos, data_len);
      }

      return err;
    }

#define _cfg(k, v) getenv(k)?:v
#define _cfgi(k, v) atoll(getenv(k)?:v)
    struct Config
    {
      int64_t duration;
      int64_t log_buf_size;
      int64_t log_file_max_size;
      Config()
      {
        duration = _cfgi("duration", "5000000");
        log_buf_size = 1<<21;
        log_file_max_size = 1<<24;
      }
    };
    class ObLogGeneratorTest: public ::testing::Test, public Config, public RWT {
      public:
        ObLogGeneratorTest() {
          RWT::set(0, 1);
        }
        ~ObLogGeneratorTest() {}
      protected:
        int err;
        ObLogCursor consumed_cursor;
        ObLogGenerator log_generator;
      protected:
        virtual void SetUp(){
          srandom((unsigned int)time(NULL));
          set_cursor(consumed_cursor, 1, 1, 0);
          ASSERT_EQ(OB_SUCCESS, log_generator.init(log_buf_size, log_file_max_size));
          ASSERT_EQ(OB_SUCCESS, log_generator.start_log(consumed_cursor));
        }
        virtual void TearDown(){
          TBSYS_LOG(INFO, "consumed_cursor=%s", to_cstring(consumed_cursor));
        }
        void inspect(bool verbose=false){
          fprintf(stderr, "ObLogGeneratorTest{duration=%ld}\n", duration);
          if(verbose)
          {
          }
        }
        int consume_log(const char* buf, int64_t len) {
          int err = OB_SUCCESS;
          ObLogCursor end_cursor;
          if (len == 0)
          {}
          else if (OB_SUCCESS != (err = parse_log_buffer(buf, len, consumed_cursor, end_cursor)))
          {
            TBSYS_LOG(ERROR, "parse_log_buffer()=>%d", err);
          }
          else
          {
            consumed_cursor = end_cursor;
          }
          return err;
        }
        int report() {
          int err = OB_SUCCESS;
          ObLogCursor start_cursor;
          ObLogCursor end_cursor;
          log_generator.get_start_cursor(start_cursor);
          log_generator.get_end_cursor(end_cursor);
          TBSYS_LOG(INFO, "generator=[%s,%s],consumed_cursor=%s", to_cstring(start_cursor),
                    to_cstring(end_cursor), to_cstring(consumed_cursor));
          return err;
        }
        int read(const int64_t idx) {
          int err = OB_SUCCESS;
          UNUSED(idx);
          return err;
        }
        int write(const int64_t idx) {
          int err = OB_SUCCESS;
          ObLogCursor start_cursor;
          ObLogCursor end_cursor;
          char* buf = NULL;
          int64_t len = 0;
          static char buf_for_gen[102400];
          UNUSED(idx);
          while(!stop_ && OB_SUCCESS == err) {
            len = (random() % sizeof(buf_for_gen)) + 1;
            if (OB_SUCCESS != (err = log_generator.write_log(OB_LOG_NOP, buf_for_gen, len))
                && OB_BUF_NOT_ENOUGH != err)
            {
              TBSYS_LOG(ERROR, "fill_log()=>%d", err);
            }
            else if (OB_SUCCESS == err)
            {}
            else if (OB_SUCCESS != (err = log_generator.get_log(start_cursor, end_cursor, buf, len)))
            {
              TBSYS_LOG(ERROR, "get_log()=>%d", err);
            }
            else if (OB_SUCCESS != (err = consume_log(buf, len)))
            {
              TBSYS_LOG(ERROR, "consume_log(buf=%p[%ld])=>%d", buf, len, err);
            }
            else if (OB_SUCCESS != (err = log_generator.commit(end_cursor)))
            {
              TBSYS_LOG(ERROR, "commit_log(%s)=>%d", to_cstring(end_cursor), err);
            }
          }
          return err;
        }
    };
    TEST_F(ObLogGeneratorTest, GenLog){
      BaseWorker worker;
      ASSERT_EQ(0, PARDO(get_thread_num(), this, duration));
    }
  }
}
using namespace oceanbase::test;

int main(int argc, char** argv)
{
  int err = OB_SUCCESS;
  TBSYS_LOGGER.setLogLevel("INFO");
  if (OB_SUCCESS != (err = ob_init_memory_pool()))
    return err;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
