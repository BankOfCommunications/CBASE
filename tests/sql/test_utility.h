
#ifndef OCEANBASE_TEST_UTILITY_H_
#define OCEANBASE_TEST_UTILITY_H_

#include "stdint.h"
#include "string.h"
#include "errno.h"
#include "common/ob_define.h"
#include "tbsys.h"
#include "common/ob_rowkey.h"
#include "common/page_arena.h"
#include "common/ob_range2.h"

namespace oceanbase
{
  using namespace common;
  namespace sql
  {
    namespace test
    {
      void split(char *line, const char *delimiters, char **tokens, int32_t &count);

      template<class T>
      int convert(char *str, T &value)
      {
        int ret = OB_SUCCESS;
        if(NULL == str)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "str is null");
        }
        else
        {
          errno = 0;
          value = atoi(str);
          if(0 != errno)
          {
            ret = OB_ERROR;
            TBSYS_LOG(WARN, "convert [%s] to int fail", str);
          }
        }
        return ret;
      }

      int gen_rowkey(int64_t seq, CharArena &arena, ObRowkey &rowkey);
      int gen_new_range(int64_t start, int64_t end, CharArena &arena, ObNewRange &range);

      bool compare_rowkey(const char *str, const ObRowkey &rowkey);

    }
  }
}

#endif /* OCEANBASE_TEST_UTILITY_H_ */

