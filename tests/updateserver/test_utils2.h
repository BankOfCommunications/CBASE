#ifndef __OB_UPDATESERVER_TEST_UTILS2_H__
#define __OB_UPDATESERVER_TEST_UTILS2_H__

#include "stdarg.h"
#include "common/ob_define.h"
#include "common/data_buffer.h"
#include "common/thread_buffer.h"
#include "Time.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/time.h>

using namespace oceanbase::common;
namespace oceanbase
{
  namespace test
  {
#define core() (*(char*)(0) = 0)
    inline int64_t get_usec()
    {
      struct timeval time_val;
      gettimeofday(&time_val, NULL);
      return time_val.tv_sec*1000000 + time_val.tv_usec;
    }
    
    struct CBuf {
      CBuf(): buf_(NULL), len_(0), pos_(0) {}
      ~CBuf() {}
      char* buf_;
      int64_t len_;
      int64_t pos_;
      int set(char* buf, int64_t len) {
        buf_ = buf;
        len_ = len;
        pos_ = 0;
        return 0;
      }
    };

    struct BufHolder
    {
      int64_t n_pointers_;
      char* pointers_[256];
      BufHolder(): n_pointers_(0) {}
      ~BufHolder() {
        for(int i = 0; i < n_pointers_; i++) {
          free(pointers_[i]);
          n_pointers_ = 0;
        }
      }
      char* get_buf(const int64_t len) {
        char* buf = NULL;
        if (0 > len)
        {}
        else if (NULL == (buf = (char*)malloc(len)))
        {}
        else
        {
          pointers_[n_pointers_++] = buf;
        }
        return buf;
      }
    };

    char* sf(char* buf, const int64_t len, int64_t& pos, const char* format, va_list ap);
    char* sf(char* buf, const int64_t len, int64_t& pos, const char* format, ...)
      __attribute__ ((format (printf, 4, 5)));

    int sh(const char* cmd);
    char* popen(char* buf, const int64_t len, int64_t& pos, const char* cmd);
    struct MgTool {
      const static int64_t DEFAULT_CBUF_LEN = 1<<21;
      CBuf cbuf;
      BufHolder buf_holder;
      MgTool(int64_t limit=DEFAULT_CBUF_LEN) {
        cbuf.set(buf_holder.get_buf(limit), limit);
      }
      ~MgTool() {
      }

      int reset() {
        int err = 0;
        cbuf.pos_ = 0;
        return err;
      }

      char* sf(const char* format, ...) {
        va_list ap;
        char* result = NULL;
        va_start(ap, format);
        result = test::sf(cbuf.buf_, cbuf.len_, cbuf.pos_, format, ap);
        va_end(ap);
        return result;
      }

      int sh(const char* format, ...) {
        va_list ap;
        int err = 0;
        va_start(ap, format);
        err = test::sh(test::sf(cbuf.buf_, cbuf.len_, cbuf.pos_, format, ap));
        va_end(ap);
        return err;
      }

      char* popen(const char* format, ...) {
        va_list ap;
        char* result = NULL;
        va_start(ap, format);
        result = test::popen(cbuf.buf_, cbuf.len_, cbuf.pos_, test::sf(cbuf.buf_, cbuf.len_, cbuf.pos_, format, ap));
        va_end(ap);
        return result;
      }
    };
    ObDataBuffer& get_thread_buffer(ThreadSpecificBuffer& thread_buf, ObDataBuffer& buf);
    struct Vcalc
    {
      public:
        Vcalc(): count_(0), start_(0), end_(0) {}
        ~Vcalc() {}
        int start() {
          count_ = 0;
          start_ = tbsys::CTimeUtil::getTime();
          end_ = 0;
          return 0;
        }
        int end(int64_t count) {
          count_ = count;
          end_ = tbsys::CTimeUtil::getTime();
          return 0;
        }
        int report(const char* msg) {
          TBSYS_LOG(INFO, "%s: %ld/(%ld-%ld)=%.4lf/ms", msg, count_, end_, start_, 1e3 * (double)count_/(double)(end_ - start_));
          return 0;
        }
        int report(const char* msg, int64_t count) {
          end(count);
          report(msg);
          return 0;
        }
      int64_t count_;
      int64_t start_;
      int64_t end_;
    };
    class Profiler {
      public:
        Profiler(const char* msg, const int64_t n): msg_(msg), n_(n) { vcalc_.start(); }
        ~Profiler() { vcalc_.report(msg_, n_); }
      private:
        const char* msg_;
        int64_t n_;
        Vcalc vcalc_;
    };

    int rand_str(char* str, int64_t len);
    int64_t get_file_len(const char* path);
  }; // end namespace test
}; // end namespace oceanbase
#endif /* __OB_UPDATESERVER_TEST_UTILS2_H__ */
