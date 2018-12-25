#ifndef __OB_COMMON_TEST_BASE_H__
#define __OB_COMMON_TEST_BASE_H__

#include "gtest/gtest.h"
#include "rwt.h"
#include "common/ob_allocator.h"
#include "common/ob_malloc.h"
#include "log_tool/ob_utils.h"
#include "log_tool/builder.h"


using namespace oceanbase::common;
struct BufHolder
{
  BufHolder(int64_t limit) { buf_ = (char*)ob_malloc(limit, ObModIds::TEST); }
  ~BufHolder() { ob_free((void*)buf_); }
  char* buf_;
};

struct BaseConfig
{
  static const int64_t buf_limit = 1<<21;
  BufHolder buf_holder;
  ObDataBuffer buf;
  int64_t duration;
  const char* schema;
  int64_t table_id;
  BaseConfig(): buf_holder(buf_limit)
  {
    buf.set_data(buf_holder.buf_, buf_limit);
    duration = _cfgi("duration", "5000000");
    schema = "./test.schema";
    table_id = 1002;
  }
};

class FixedAllocator: public ObIAllocator
{
  public:
    FixedAllocator(char* buf, int64_t limit): buf_(buf), limit_(limit), pos_(0) {}
    virtual ~FixedAllocator() {}
  public:
    void reset() { pos_ = 0; }
    virtual void *alloc(const int64_t sz){
      void* ptr = NULL;
      int64_t pos = 0;
      if ((pos = __sync_add_and_fetch(&pos_, sz)) > limit_)
      {
        __sync_add_and_fetch(&pos_, -sz);
      }
      else
      {
        ptr = buf_ + pos;
      }
      return ptr;
    }
    virtual void free(void *ptr){ UNUSED(ptr); }
  private:
    char* buf_;
    int64_t limit_;
    int64_t pos_;
};

#define RWT_def(base) \
  TEST_F(base, Rand){ \
    ASSERT_EQ(0, PARDO(get_thread_num(), this, duration));      \
    ASSERT_EQ(0, check_error());                                \
  }
#endif /* __OB_COMMON_TEST_BASE_H__ */
