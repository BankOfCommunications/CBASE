#include <ctype.h>
#include <string.h>
#include "common_func.h"
#include "common/utility.h"
#include "common/ob_malloc.h"


using namespace oceanbase::common;

char* trim(char* str)
{
  char* ptr;

  if (str == NULL)
    return NULL;

  ptr = str + strlen(str) - 1;
  
  while (isspace(*str))
    str++;
  while ((ptr > str) && isspace(*ptr))
    *ptr-- = '\0';

  return str;
}

char* get_token(char *&cmdstr)
{
  char *token;

  token = trim(cmdstr);
  if (strlen(token) == 0)
  return NULL;

  for (cmdstr = token; !isspace(*cmdstr) && *cmdstr != '\0'; cmdstr++)
    *cmdstr = tolower(*cmdstr);
  while(*cmdstr != '\0' && isspace(*cmdstr))
  {
    cmdstr++;
    *(cmdstr - 1) = '\0';
  }

  if (*token == '\0')
    return NULL;
  else
    return token;
}

char* putback_token(char *token, char *cmdstr)
{
  char *ptr;
  
  for(ptr = token + strlen(token); ptr < cmdstr; ptr++)
    *ptr = ' ';

  return token;
}

char* string_split_by_str(char *&str1, 
                           char *str2, 
                           bool is_case_sensitive,
                           bool whole_words_only)
{
  char *p = NULL;
  char *front_str = str1;
  char *local_str1 = str1;
  char *local_str2 = str2;

  if (str1 == NULL || str2 == NULL || strlen(str1) == 0 || strlen(str2) == 0)
    return NULL;
  
  if (!is_case_sensitive)
  {
    local_str1 = static_cast<char*>(ob_malloc(strlen(str1) + 1));
    local_str2 = static_cast<char*>(ob_malloc(strlen(str2) + 1));
    for (p = str1; *p != '\0'; p++)
    {
      *(local_str1 + (p - str1)) = tolower(*p);
    }
    *(local_str1 + (p - str1)) = '\0';
    for (p = str2; *p != '\0'; p++)
    {
      *(local_str2 + (p - str2)) = tolower(*p);
    }
    *(local_str2 + (p - str2)) = '\0';
  }

  p = local_str1;
  while (true)
  {
    p = strstr(p, local_str2);

    if (whole_words_only && p)
    {
      if ((p == local_str1 || isspace(*(p - 1))) 
        && (isspace(*(p + strlen(local_str2))) || *(p + strlen(local_str2)) == '\0'))
        break;
      else
        continue;
    }
    else
    {
      break;
    }
  
  }
  
  if (p == NULL)
    front_str =  NULL;
  else
  {
    if (p == local_str1)
      front_str = NULL;
    else
    {
      *(str1 + (p - local_str1 - 1)) = '\0';
      front_str = trim(str1);
    }
    str1 = trim(str1 + (p + strlen(str2) - local_str1));
  }

  if (!is_case_sensitive)
  {
    ob_free(local_str1);
    ob_free(local_str2);
  }
  
  return front_str;
}

char* string_split_by_ch(char *&str, const int ch)
{
  char *p = NULL;
  char *front_str = str;

  if (str == NULL)
    return NULL;

  p = strchr(str, ch);
  
  if (p == NULL)
    front_str = NULL;
  else
  {
    if (p == str)
      front_str = NULL;
    else
    {
      *p = '\0';
      front_str = trim(front_str);
    }
    str = trim(p + 1);
  }

  return front_str;
}


int parse_number_range(const char *number_string, oceanbase::common::ObArrayHelper<int32_t>& disk_array)
{
  int ret = OB_ERROR;
  if (NULL != strstr(number_string, ","))
  {
    int32_t size = static_cast<int32_t>(disk_array.get_array_size());
    ret = parse_string_to_int_array(number_string, ',', 
                                    disk_array.get_base_address(), 
                                    size);
    if (ret) return ret;
  }
  else if (NULL != strstr(number_string, "~"))
  {
    int32_t min_max_array[2];
    min_max_array[0] = 0;
    min_max_array[1] = 0;
    int tmp_size = 2;
    ret = parse_string_to_int_array(number_string, '~', min_max_array, tmp_size);
    if (ret) return ret;
    int32_t min = min_max_array[0];
    int32_t max = min_max_array[1];
    int32_t index = 0;
    for (int i = min; i <= max; ++i)
    {
      if (index > disk_array.get_array_size()) return OB_SIZE_OVERFLOW;
      disk_array.push_back(i);
    }
  }
  else
  {
    disk_array.push_back(strtol(number_string, NULL, 10));
  }
  return OB_SUCCESS;
}

char* duplicate_str(const char *str)
{
  char    *new_str = NULL;
  int64_t len;
    
  if (!str)
    return NULL;

  len = strlen(str);
  new_str = (char*)ob_malloc(len + 1);
  if (!new_str)
  {
    fprintf(stderr, "No more memorey!\n");
    exit(-1);
  }
  memcpy(new_str, str, len + 1);
  new_str[len] = '\0';

  return new_str;
}

int64_t atoi64(const char *str)
{
  int64_t sign = 1;
  int64_t Number = 0;
  int64_t x = 0;
  int64_t len = strlen(str);

  if (str[0] == '-')
  {
    sign = -1;
    x = 1;
  }
  
  for(; x < len && str[x] >='0' && str[x]<='9'; x++)
    Number = Number * 10 + (str[x] - '0');

  return Number * sign;
}

char* strlwr(char *str)
{
  if (str == NULL)
    return NULL;

  for(char *cp = str; *cp != '\0'; ++cp)
  {
    if( 'A' <= *cp && *cp <= 'Z' )
    *cp += 'a' - 'A';
  }
  return str ;
}

char* strupr(char *str)
{
  for(char *cp = str; *cp != '\0'; ++cp)
  {
    if( 'a' <= *cp && *cp <= 'z' )
    *cp -= 'a' - 'A';
  }
  return str;
}

