
#include "test_utility.h"

using namespace oceanbase::sql;


void test::split(char *line, const char *delimiters, char **tokens, int32_t &count)
{
  count = 0;
  char *pch = strtok(line, delimiters);
  while(pch != NULL)
  {
    tokens[count ++] = pch;
    pch = strtok(NULL, delimiters);
  }
}

int test::gen_rowkey(int64_t seq, CharArena &arena, ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  char key[100];
  sprintf(key, "rowkey_%05ld", seq);

  char* key_buf = arena.alloc(strlen(key));
  memcpy(key_buf, key, strlen(key));

  ObString rowkey_str;
  rowkey_str.assign_ptr(key_buf, (int32_t)strlen(key));

  ObObj *rowkey_obj = (ObObj *)arena.alloc(sizeof(ObObj));
  rowkey_obj->set_varchar(rowkey_str);

  rowkey.assign(rowkey_obj, 1);
  return ret;
}

int test::gen_new_range(int64_t start, int64_t end, CharArena &arena, ObNewRange &range)
{
  int ret = OB_SUCCESS;
  gen_rowkey(start, arena, range.start_key_);
  gen_rowkey(end, arena, range.end_key_);
  return ret;
}

bool test::compare_rowkey(const char *str, const ObRowkey &rowkey)
{
  bool ret = false;
  ObString rowkey_ptr;
  rowkey.ptr()[0].get_varchar(rowkey_ptr);
  ret = (0 == strncmp(str, rowkey_ptr.ptr(), rowkey_ptr.length()));
  return ret;
}

