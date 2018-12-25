#ifndef OCEANBASE_COMMON_BTREE_TEST_TEST_KEY_H_
#define OCEANBASE_COMMON_BTREE_TEST_TEST_KEY_H_

#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <sys/time.h>

/**
 * TestKey测试程序
 */
namespace oceanbase
{
  namespace common
  {
#define TEST_KEY_MAX_SIZE 16
    class TestKey
    {
    public:
      TestKey() {}
      ~TestKey() {}
      void set_value(uint32_t j)
      {
        char *p = str_;
        size_ = 0;
        while(j != 0)
        {
          (*p++) = (char)((j & 0x0F) + 65);
          j >>= 4;
          size_ ++;
        }
        str_[size_] = '\0';
      }
      int32_t operator- (const TestKey &k) const
      {
        return strncmp(str_, k.str_, 8);
      }
      char *get_value()
      {
        return str_;
      }
    public:
      int32_t size_;
      char str_[TEST_KEY_MAX_SIZE-4];
    };

#define COLLECTINFO_KEY_MAX_SIZE 24
    class CollectInfo
    {
    public:
      CollectInfo() {}
      ~CollectInfo() {}
      void set_value(int64_t key)
      {
        user_id_ = key & 0xffff;
        type_ = key & 0xffff00000;
        item_id_ = key & 0xfff0000f0000L;
      }
      void operator= (const CollectInfo &k)
      {
        int64_t *addr = reinterpret_cast<int64_t *>(this);
        (*addr++) = k.user_id_;
        (*addr++) = k.type_;
        (*addr) = k.item_id_;
      }
      int32_t operator- (const CollectInfo &k) const
      {
        int64_t ret = user_id_ - k.user_id_;
        if (ret == 0)
        {
          ret = type_ - k.type_;
          if (ret == 0)
          {
            ret = item_id_ - k.item_id_;
          }
        }
        if (ret > 0) ret = 1;
        else if (ret < 0) ret = -1;
        return ret;
      }
    public:
      int64_t user_id_;
      int64_t type_;
      int64_t item_id_;
    };

    class KeyInfo
    {
    public:
      KeyInfo(int64_t a, int64_t b)
      {
        a_ = a;
        b_ = b;
      }
      void operator= (const KeyInfo &k)
      {
        a_ = k.a_;
        b_ = k.b_;
      }
      int32_t operator- (const KeyInfo &k) const
      {
        return (int32_t)(a_ - k.a_);
      }
      int64_t a_, b_;
    };

    static __inline__ int64_t getTime()
    {
      struct timeval t;
      gettimeofday(&t, NULL);
      return (static_cast<int64_t>(t.tv_sec) * static_cast<int64_t>(1000000) + static_cast<int64_t>(t.tv_usec));
    }
    static __inline__ int64_t atomic_add_return(int64_t i, volatile int64_t *v)
    {
      int64_t __i;
      __i = i;
      __asm__ __volatile__(
        "lock; xaddq %0, %1"
        :"+r" (i), "+m" ((*v))
        : : "memory");
      return i + __i;
    }
  }
}

#endif
