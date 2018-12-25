#include <string.h>
#include "parse_malloc.h"
#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"

using namespace oceanbase::common;

/* get memory from the thread obStringBuf, and not release untill thread quits */
void *parse_malloc(const size_t nbyte, void *malloc_pool)
{
  void *ptr = NULL;
  size_t headlen = sizeof(int64_t);
  ObStringBuf* alloc_buf = static_cast<ObStringBuf*>(malloc_pool);
  if (alloc_buf == NULL)
  {
    TBSYS_LOG(ERROR, "parse_malloc gets string buffer error");
  }
  else if (nbyte <= 0)
  {
    TBSYS_LOG(ERROR, "wrong size %ld of parse_malloc", nbyte);
  }
  else if (NULL == (ptr = alloc_buf->alloc(headlen + nbyte)))
  {
    TBSYS_LOG(ERROR, "alloc memory failed");
  }
  else
  {
    *((int64_t *)ptr) = nbyte;
    ptr = (char*)ptr + headlen;
  }
  return ptr;
}

/* ptr must point to a memory allocated by parse_malloc.cpp */
void *parse_realloc(void *ptr, size_t nbyte, void *malloc_pool)
{
  void *new_ptr = NULL;
  ObStringBuf* alloc_buf = static_cast<ObStringBuf*>(malloc_pool);
  if (alloc_buf == NULL)
  {
    TBSYS_LOG(ERROR, "parse_malloc gets string buffer error");
  }
  else if (nbyte <= 0)
  {
    TBSYS_LOG(ERROR, "wrong size %ld of parse_malloc", nbyte);
  }
  else if (ptr == NULL)
  {
    new_ptr = parse_malloc(nbyte, malloc_pool);
  }
  else
  {
    size_t headlen = sizeof(int64_t);
    int64_t obyte = *(int64_t *)((char *)ptr - headlen);
    if ((new_ptr = alloc_buf->realloc((char*)ptr - headlen, obyte + headlen, nbyte + headlen)) != NULL)
    {
      *((int64_t *)new_ptr) = nbyte;
      new_ptr = (char*)new_ptr + headlen;
    }
    else
    {
      TBSYS_LOG(ERROR, "alloc memory failed");
    }
  }
  return new_ptr;
}

char *parse_strndup(const char *str, size_t nbyte, void *malloc_pool)
{
  char *new_str = NULL;
  if (str)
  {
    if ((new_str = (char *)parse_malloc(nbyte, malloc_pool)) != NULL)
    {
      memmove(new_str, str, nbyte);
    }
    else
    {
      TBSYS_LOG(ERROR, "parse_strdup gets string buffer error");
    }
  }
  return new_str;
}

char *parse_strdup(const char *str, void *malloc_pool)
{
  char *new_str = NULL;
  if (str)
  {
    new_str = parse_strndup(str, strlen(str) + 1, malloc_pool);
  }
  return new_str;
}

void parse_free(void *ptr)
{
  UNUSED(ptr);
  /* do nothing, we don't really free the memory */
}
