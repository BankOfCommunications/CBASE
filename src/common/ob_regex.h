////===================================================================
 //
 // ob_regex.h common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2011-03-19 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 // 只封装了对regex的调用，没有对内存使用做任何优化，慎用
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_COMMON_REGEX_H_
#define  OCEANBASE_COMMON_REGEX_H_

#include <regex.h>

namespace oceanbase
{
  namespace common
  {
    class ObRegex
    {
      public:
        ObRegex();
        ~ObRegex();
      public:
        bool init(const char* pattern, int flags);
        bool match(const char* text, int flags);
        void destroy(void);
      private:
        bool init_;
        regmatch_t* match_;
        regex_t reg_;
        size_t nmatch_;
    };
  }
}

#endif //OCEANBASE_COMMON_REGEX_H_

