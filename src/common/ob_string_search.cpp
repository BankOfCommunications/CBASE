/*
 * (C) 2007-2010 Taobao Inc. 
 *
 * This program is free software; you can redistribute it and/or modify 
 * it under the terms of the GNU General Public License version 2 as 
 * published by the Free Software Foundation.
 *
 * ob_string_search.cpp is for what ...
 *
 * Version: ***: ob_string_search.cpp  Thu Mar 24 10:47:24 2011 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji.hcm <fangji.hcm@taobao.com>
 *     -some work detail if you want 
 *
 */

#include "tblog.h"
#include "ob_string_search.h"
#include "ob_string.h"

#define ESCAPE '\\'
namespace oceanbase
{
  namespace common
  {
    void ObStringSearch::check_if_do_opt(const char *pattern, const int32_t len, bool &is_simple_pattern, bool &is_special_pattern, int & start, int & end, int & real_len, bool & all_percent)
    {
      real_len = 0;
      int escape_count = 0;
      bool start_with_percent = false;
      bool end_with_percent = false;
      bool include_percent = false;
      is_simple_pattern = true;
      enum MatchState state = INIT;
      int i = 0;
      for (; i < len ;++i)
      {
        switch(state)
        {
          case INIT:
            if (pattern[i] == '_')
            {
              is_simple_pattern = false;
              return ;
            }
            else if (pattern[i] == '%')
            {
              is_simple_pattern = false;
              start_with_percent = true;
              state = START_WITH_PERCENT;
            }
            else if (pattern[i] == '\\')
            {
              state = ESCAPING;
            }
            else
            {
              state = NORMAL;
            }
            break;
          case START_WITH_PERCENT:
            if (pattern[i] == '%')
            {
            }
            else if (pattern[i] == '\\')
            {
              start = i;
              escape_count++;
              state = ESCAPING;
            }
            else
            {
              start = i;
              state = NORMAL;
            }
            break;
          case ESCAPING:
            state = NORMAL;
            break;
          case NORMAL:
            if (pattern[i] == '_')
            {
              is_simple_pattern = false;
            }
            else if (pattern[i] == '%')
            {
              is_simple_pattern = false;
              end = i - 1;
              state = END_WITH_PERCENT;
            }
            else if(pattern[i] == '\\')
            {
              escape_count++;
              state = ESCAPING;

            }
            break;
          case END_WITH_PERCENT:
            if (pattern[i] == '%')
            {
            }
            else if(pattern[i] == '\\')
            {
              include_percent = true;
              state = ESCAPING;
            }
            else
            {
              include_percent = true;
              state = NORMAL;
            }
            break;
        }
      }
      if (state == END_WITH_PERCENT)
      {
        end_with_percent = true;
      }
      is_special_pattern = (start_with_percent && end_with_percent && !include_percent);
      if (is_special_pattern)
      {
        real_len = (end - start + 1) - escape_count;
      }
      if (state == START_WITH_PERCENT)
      {
        all_percent = true;
      }
    }
    bool ObStringSearch::str_cmp(const char *text, const int32_t len1, const char *pattern, const int32_t len2)
    {
      int j = 0;
      int k = 0;
      while (k < len2)
      {
        if (j >= len1)
          return false;
        if (pattern[k] == ESCAPE)
        {
          // ESCAPE is the last character of pattern string
          if (k == len2 - 1)
          {
            if (text[j] == ESCAPE && j == len1 - 1)
              return true;
            else
              return false;
          }
          else
          {
            if (pattern[k + 1] == text[j])
            {
              k += 2;
              j++;
            }
            else
            {
              return false;
            }
          }
        }
        else
        {
          if (pattern[k] == text[j])
          {
            k++;
            j++;
          }
          else
          {
            return false;
          }
        }
      }
      if (k == len2 && j == len1)
        return true;
      else
        return false;
    }
    bool ObStringSearch::is_matched(const ObString & pattern_str, const ObString & text_str)
    {
      const char *index = NULL;
      const char *pattern = pattern_str.ptr();
      const char *text = text_str.ptr();
      const int32_t len2 = pattern_str.length();
      const int32_t len1 = text_str.length();
      bool all_percent = false;
      bool is_simple_pattern = true;
      bool is_special_pattern = false;
      int start = -1;
      int end = -1;
      int real_len = 0;
      check_if_do_opt(pattern, len2, is_simple_pattern, is_special_pattern , start, end, real_len, all_percent);
      // opt one :check whether pattern string includes wildcard % or _
      if (is_simple_pattern)
      {
        // not include wildcard % and _ in pattern
        return str_cmp(text, len1, pattern, len2);
      }
      // opt two: check whether the pattern string is like '%abc%'
      if (is_special_pattern)
      {
        index = my_strstr(text, len1, pattern + start, end - start + 1, real_len, 1);
        if (index != NULL)
        {
          return true;
        }
        else
        {
          return false;
        }
      }
      if (all_percent)
      {
        return true;
      }
      int p1 = 0;
      int p2 = 0;
      int p3 = 0;
      bool is_matched = false;
			//p1:text
			//p2:pattern
      while(p1 < len1 && p2 < len2)
      {
        // \a or \b etc..  as if \ is not present
        // two '\' , means a ordinary '\'
        if (pattern[p2] == ESCAPE)
        {
          // last character of pattern string is '\'
          if (p2 == len2 -1)
          {
            if (text[p1] == ESCAPE)
            {
              p1++;
              p2++;
            }
            else
            {
              is_matched = false;
              break;
            }
          }
          else
          {
            if (text[p1] == pattern[p2 + 1])
            {
              p1++;
              p2 += 2;
            }
            else
            {
              is_matched = false;
              break;
            }
          }
        }
        else if (pattern[p2] == '%')
        {
          // find the next wildcard '%',not include ordinary %
          p3 = p2 + 1;
          int escape_suc_count = 0;
          bool last_star = false;
          while (p3 < len2)
          {
            if (pattern[p3] == ESCAPE)
            {
              if (p3 == len2 - 1)
              {
                p3++;
                break;
              }
              else
              {
                escape_suc_count++;
                p3 += 2;
              }
            }
            // find the next wildcard %
            else if (pattern[p3] == '%')
            {
              if (p3 == len2 - 1)
              {
                last_star = true;
              }
              break;
            }
            else
            {
              p3++;
            }
          }
          
          // p2 和 p3 是pattern串的相邻的两个 '%'
          // 现在需要从文本串的[p1, len1 - 1]这个范围中，去匹配(p2, p3)之间的子串
          int literal_len = p3 - p2 - 1;
          int real_len = literal_len - escape_suc_count;
          // means ordinary '%' is the last character of pattern string
          if (last_star == true && literal_len == 0)
          {
            is_matched = true;
            break;
          }
          // means there is no next '%'
          else if (p3 == len2)
          {
            // match this substring from the end of the text pattern from p1
            index = my_strstr(text + p1,len1 - p1, pattern + p2 + 1, literal_len, real_len, 0);
            if (index != NULL)
            {
              is_matched = true;
              break;
            }
            else
            {
              is_matched = false;
              break;
            }
          }
          else
          {
            index = my_strstr(text + p1, len1 - p1, pattern + p2 + 1, literal_len , real_len , 1);
            if (index == NULL)
            {
              is_matched = false;
              break;
            }
            else
            {
              p1 += literal_len;
              p2 = p3;
            }
          }

        }
        else
        {
          if (pattern[p2] == '_' || text[p1] == pattern[p2])
          {
            p1++;
            p2++;
          }
          else
          {
            is_matched = false;
            break;
          }

        }
      }
      if (p1 == len1)
      {
				if (p2 == len2)
				{
					is_matched = true;
				}
				else if (p2 < len2)
				{
					int i = p2 ;
					for (;i < len2; ++i)
					{
						if (pattern[i] != '%')
						{
							break;
						}
					}
					if (i == len2)
					{
						is_matched = true;
					}
				}
      }
      return is_matched;

    }
    const char * ObStringSearch::is_str_equals(const char *left, const int32_t len1, const char* right, const int32_t len2)
    {
      // len1 equals the real len of right string

      // text
      int p1 = 0;
      // pattern
      int p2 = 0;
      while (p2 < len2)
      {
        if (right[p2] == ESCAPE)
        {
          // last character of pattern is ESCAPE
          if (p2 == len2 - 1)
          {
            if (left[p1] == ESCAPE && p1 == len1 - 1)
            {
              return left;
            }
            else
            {
              return NULL;
            }
          }
          else
          {
            if (left[p1] == right[p2 + 1])
            {
              p2 += 2;
              p1++;
            }
            else
            {
              return NULL;
            }
          }
        }
        else if (right[p2] == '_' || right[p2] == left[p1])
        {
          p1++;
          p2++;
        }
        else
        {
          return NULL;
        }
      }
      if (p2 == len2 && p1 == len1)
      {
        return left;
      }
      else
      {
        return NULL;
      }
    }
    const char * ObStringSearch::my_strstr(const char *haystack, const int32_t len1, const char *needle, const int32_t len2, const int32_t real_len, int forward)
    {
      const char *index = NULL;
      int i = 0;
      if (forward == 1)
      {
        for (i = 0; i <= len1 - real_len; ++i)
        {
          index = is_str_equals(haystack + i, real_len, needle, len2);
          if (index != NULL)
            return index;
        }
        // not found
        if (i == len1 - real_len + 1)
          return NULL;
      }
      else
      {
        index = is_str_equals(haystack + len1 - real_len, real_len , needle, len2);
        return index;
      }
      return NULL;

    }

    uint64_t ObStringSearch::cal_print(const ObString & pattern)
    {
      const char* pat = pattern.ptr();
      const int64_t pat_len = pattern.length();
      return cal_print(pat, pat_len);
    }

    int64_t ObStringSearch::kr_search(const ObString & pattern, const ObString & text)
    {
      const char* pat = pattern.ptr();
      const int64_t pat_len = pattern.length();
      const char* txt = text.ptr();
      const int64_t txt_len = text.length();
      return kr_search(pat, pat_len, txt, txt_len);
    }

    int64_t ObStringSearch::kr_search(const ObString & pattern, const uint64_t pattern_print, const ObString & text)
    {
      const char* pat = pattern.ptr();
      const int64_t pat_len = pattern.length();
      const char* txt = text.ptr();
      const int64_t txt_len = text.length();
      return kr_search(pat, pat_len, pattern_print, txt, txt_len);
    }

    int64_t ObStringSearch::fast_search(const ObString & pattern, const ObString & text)
    {
      const char* pat = pattern.ptr();
      const int64_t pat_len = pattern.length();
      const char* txt = text.ptr();
      const int64_t txt_len = text.length();
      return fast_search(pat, pat_len, txt, txt_len);
    }

    uint64_t ObStringSearch::cal_print(const char* pattern, const int64_t pat_len)
    {
      uint64_t pat_print = 0;
      if (NULL == pattern || 0 >= pat_len)
      {
        TBSYS_LOG(WARN, "invalid argument pattern=%p, pattern_len=%ld",
                  pattern, pat_len);
      }
      else
      {
        for(int64_t i = 0; i<pat_len; ++i)
        {
          pat_print = (pat_print << 4) + pat_print + pattern[i];
        }
      }
      return pat_print;
    }

    int64_t ObStringSearch::kr_search(const char* pattern, const int64_t pat_len, const char* text, const int64_t text_len)
    {
      int64_t ret = -1;
      uint64_t shift = 1;
      uint64_t text_print = 0;
      uint64_t pat_print = 0;
      
      if (NULL == pattern || 0 >= pat_len
          || NULL == text || 0 >= text_len)
      {
        TBSYS_LOG(WARN, "invalid argument pattern=%p, pattern_len=%ld, text=%p, text_len=%ld",
                  pattern, pat_len, text, text_len);
      }
      else
      {
        for(int64_t i = 0; i<pat_len; ++i)
        {
          text_print = (text_print << 4) + text_print + text[i];
          pat_print = (pat_print << 4) + pat_print + pattern[i];
          shift = (shift << 4) + shift;
        }
 
        int64_t pos = 0;

        while(pos <= text_len - pat_len)
        {
          if(pat_print == text_print && 0 == memcmp(pattern, text+pos, pat_len))
          {
            ret = pos;
            break;
          }
          text_print = (text_print << 4) + text_print -  text[pos]*shift + text[pos+pat_len];
          ++pos;
        }
      }
      return ret;
    }

    int64_t ObStringSearch::kr_search(const char* pattern, const int64_t pat_len, const uint64_t pat_print,
                                      const char* text,    const int64_t text_len)
    {
      int64_t ret = -1;
      uint64_t shift = 1;
      uint64_t text_print = 0;
     
      if (NULL == pattern || 0 >= pat_len
          || NULL == text || 0 >= text_len)
      {
        TBSYS_LOG(WARN, "invalid argument pattern=%p, pattern_len=%ld, text=%p, text_len=%ld",
                  pattern, pat_len, text, text_len);
      }
      else
      {
        for(int64_t i = 0; i<pat_len; ++i)
        {
          text_print = (text_print << 4) + text_print + text[i];
          shift = (shift << 4) + shift;
        }
      
        int64_t pos = 0;
      
        while(pos <= text_len - pat_len)
        {
          if(pat_print == text_print && 0 == memcmp(pattern, text+pos, pat_len))
          {
            ret = pos;
            break;
          }
          text_print = (text_print << 4) + text_print -  text[pos]*shift + text[pos+pat_len];
          ++pos;
        }
      }
      return ret;
    }

    int64_t ObStringSearch::fast_search(const char* pattern, const int64_t pat_len,
                                        const char* text,    const int64_t text_len)
    {
      long mask;
      int64_t skip = 0;
      int64_t i, j, mlast, w;

      if (NULL == pattern || 0 >= pat_len
          || NULL == text || 0 >= text_len)
      {
        TBSYS_LOG(WARN, "invalid argument pattern=%p, pattern_len=%ld, text=%p, text_len=%ld",
                  pattern, pat_len, text, text_len);
      }
      else
      {
        w = text_len - pat_len;

        if (w < 0)
          return -1;

        /* look for special cases */
        if (pat_len <= 1) {
          if (pat_len <= 0)
            return -1;
          /* use special case for 1-character strings */
          for (i = 0; i < text_len; i++)
            if (text[i] == pattern[0])
              return i;
          return -1;
        }

        mlast = pat_len - 1;

        /* create compressed boyer-moore delta 1 table */
        skip = mlast - 1;
        /* process pattern[:-1] */
        for (mask = i = 0; i < mlast; i++) {
          mask |= (1 << (pattern[i] & 0x1F));
          if (pattern[i] == pattern[mlast])
            skip = mlast - i - 1;
        }
        /* process pattern[-1] outside the loop */
        mask |= (1 << (pattern[mlast] & 0x1F));

        for (i = 0; i <= w; i++) {
          /* note: using mlast in the skip path slows things down on x86 */
          if (text[i+pat_len-1] == pattern[pat_len-1]) {
            /* candidate match */
            for (j = 0; j < mlast; j++)
              if (text[i+j] != pattern[j])
                break;
            if (j == mlast) {
              /* got a match! */
              return i;
            }
            /* miss: check if next character is part of pattern */
            if (!(mask & (1 << (text[i+pat_len] & 0x1F))))
              i = i + pat_len;
            else
              i = i + skip;
          } else {
            /* skip: check if next character is part of pattern */
            if (!(mask & (1 << (text[i+pat_len] & 0x1F))))
              i = i + pat_len;
          }
        }
      }
      return -1;
    }
  }//namespace common
}//namespace oceanbase
