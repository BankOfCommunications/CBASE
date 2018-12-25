/*
 * =====================================================================================
 *
 *       Filename:  TestKeyStr.h
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2010年08月09日 14时58分04秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  ZongLuo (), zongluo@taobao.com
 *        Company:  TaoBao Inc.
 *
 * =====================================================================================
 */

#include <limits.h>
#include <key_btree.h>
#include <gtest/gtest.h>

namespace oceanbase
{
  namespace common
  {
    class TestKey
    {
    public:
      TestKey() {}
      TestKey(const int32_t size, const char *str)
      {
        set_value(size, str);
      }
      ~TestKey()
      {
        /*free(str_);*/
      }
      void set_value(const int32_t size, const char *str)
      {
        size_ = size;
        if (size_ > 12) size_ = 12;
        memcpy(str_, str, size_);
      }
      void set_value(const char *str)
      {
        //str_ = (char *)malloc(( strlen(str) + 1 ) * sizeof(char));
        set_value(strlen(str) + 1, str);
      }

      int32_t operator- (const TestKey &k) const
      {
        for(int32_t i = 0; i < size_ && i < k.size_; i++)
        {
          if (str_[i] < k.str_[i])
            return -1;
          else if (str_[i] > k.str_[i])
            return 1;
        }
        return (size_ - k.size_);
      }
      char *get_value()
      {
        return str_;
      }
    private:
      int32_t size_;
      //char *str_;
      char str_[ 12 ];

    public:
      static const int32_t TEST_COUNT;
    };
  }
}
