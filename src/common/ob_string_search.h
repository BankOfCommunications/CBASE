#ifndef OB_STRING_SEARCH_H_
#define OB_STRING_SEARCH_H_

#include "ob_define.h"
#include "ob_string.h"

namespace oceanbase 
{ 
  namespace common 
  {
    enum MatchState
    {
      INIT = 0,
      START_WITH_PERCENT,
      ESCAPING,
      NORMAL,
      END_WITH_PERCENT,
    };
    class ObStringSearch
    {
    public:
      /// cal pattern print
      static uint64_t cal_print(const ObString & pattern);
      /// find pattern in text using kr algorithm
      static int64_t kr_search(const ObString & pattern, const ObString & text);
      static int64_t kr_search(const ObString & pattern, const uint64_t pattern_print, const ObString & text);
      /// find pattern in text using fast search algorithm
      static int64_t fast_search(const ObString & pattern, const ObString & text);
      static bool is_matched(const ObString &pattern_str, const ObString &text_str);
    
    private: 
      static uint64_t cal_print(const char * patter, const int64_t pat_len);
      static int64_t kr_search(const char * pattern, const int64_t pat_len, 
          const char* text, const int64_t text_len);
      
      static int64_t kr_search(const char * pattern, const int64_t pat_len, 
          const uint64_t pat_print, const char* text, const int64_t text_len);
      
      static int64_t fast_search(const char * pattern, const int64_t pat_len, 
          const char * text, const int64_t text_len);
      static const char * my_strstr(const char *haystack, const int32_t len1, const char *needle, const int32_t len2, const int32_t real_len ,int forward);
      static const char * is_str_equals(const char *left, const int32_t len1, const char* right, const int32_t len2);
      // there are no % and _ in pattern string
      static bool str_cmp(const char *text, const int32_t len1, const char *pattern, const int32_t len2);
      // check whether pattern string includes wildcard % or _
      // check if pattern like %abc% or %ab_% etc.
      // check if pattern consists of all %
      static void check_if_do_opt(const char *pattern, const int32_t len, bool &is_simple_pattern, bool &is_special_pattern, int & start, int & end, int & real_len, bool & all_percent);
    };
  }
}

#endif //OB_STRING_SEARCH_H_

