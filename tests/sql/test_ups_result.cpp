#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include "ob_malloc.h"
#include "sql/ob_ups_result.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace common;
using namespace sql;

TEST(TestObUpsResult, check)
{
  ObUpsResult ur;
  const char *ws[3] = {"hello", "world", ""};

  ur.set_error_code(OB_SUCCESS);
  ur.set_affected_rows(1);
  for (int64_t i = 0; i < 3; i++)
  {
    ur.add_warning_string(ws[i]);
  }

  EXPECT_EQ(OB_SUCCESS, ur.get_error_code());
  EXPECT_EQ(1, ur.get_affected_rows());
  EXPECT_EQ(3, ur.get_warning_count());
  int64_t idx = 2;
  const char *str = NULL;
  while (NULL != (str = ur.get_next_warning()))
  {
    EXPECT_EQ(0, strcmp(str, ws[idx--]));
  }
  EXPECT_EQ(-1, idx);
}

class FailAllocator : public ObIAllocator
{
  public:
    FailAllocator() {};
    ~FailAllocator() {};
  public:
    void *alloc(const int64_t sz) {UNUSED(sz); return NULL;};
    void free(void *ptr) {UNUSED(ptr);};
};

TEST(TestObUpsResult, alloc_fail)
{
  FailAllocator fa;
  ObUpsResult ur(fa);
  const char *nil = NULL;
  const char *ws[3] = {"hello", "world", ""};

  ur.set_error_code(OB_SUCCESS);
  ur.set_affected_rows(1);
  for (int64_t i = 0; i < 3; i++)
  {
    ur.add_warning_string(ws[i]);
  }

  EXPECT_EQ(OB_SUCCESS, ur.get_error_code());
  EXPECT_EQ(1, ur.get_affected_rows());
  EXPECT_EQ(0, ur.get_warning_count());
  EXPECT_EQ(nil, ur.get_next_warning());
}

TEST(TestObUpsResult, serialize)
{
  ObUpsResult ur;
  const char *ws[3] = {"hello", "world", ""};

  ur.set_error_code(OB_SUCCESS);
  ur.set_affected_rows(1);
  for (int64_t i = 0; i < 3; i++)
  {
    ur.add_warning_string(ws[i]);
  }

  EXPECT_EQ(OB_SUCCESS, ur.get_error_code());
  EXPECT_EQ(1, ur.get_affected_rows());
  EXPECT_EQ(3, ur.get_warning_count());
  int64_t idx = 2;
  const char *str = NULL;
  while (NULL != (str = ur.get_next_warning()))
  {
    EXPECT_EQ(0, strcmp(str, ws[idx--]));
  }
  EXPECT_EQ(-1, idx);

  int64_t length = ur.get_serialize_size() + 1024;
  char *buffer = new char[length];
  int64_t pos = 0;
  int ret = ur.serialize(buffer, length, pos);
  EXPECT_EQ(OB_SUCCESS, ret);
  //EXPECT_EQ(length, pos);
  delete[] buffer;
}

TEST(TestObUpsResult, deserialize)
{
  ObUpsResult ur;
  const char *ws[3] = {"hello", "world", ""};

  ur.set_error_code(OB_SUCCESS);
  ur.set_affected_rows(1);
  for (int64_t i = 0; i < 3; i++)
  {
    ur.add_warning_string(ws[i]);
  }

  EXPECT_EQ(OB_SUCCESS, ur.get_error_code());
  EXPECT_EQ(1, ur.get_affected_rows());
  EXPECT_EQ(3, ur.get_warning_count());
  int64_t idx = 2;
  const char *str = NULL;
  while (NULL != (str = ur.get_next_warning()))
  {
    EXPECT_EQ(0, strcmp(str, ws[idx--]));
  }
  EXPECT_EQ(-1, idx);

  int64_t length = ur.get_serialize_size() + 1024;
  char *buffer = new char[length];
  int64_t pos = 0;
  int ret = ur.serialize(buffer, length, pos);
  EXPECT_EQ(OB_SUCCESS, ret);
  //EXPECT_EQ(length, pos);

  length = pos;
  pos = 0;
  ret = ur.deserialize(buffer, length, pos);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(length, pos);
  EXPECT_EQ(OB_SUCCESS, ur.get_error_code());
  EXPECT_EQ(1, ur.get_affected_rows());
  EXPECT_EQ(3, ur.get_warning_count());
  idx = 0;
  while (NULL != (str = ur.get_next_warning()))
  {
    EXPECT_EQ(0, strcmp(str, ws[idx++]));
  }
  EXPECT_EQ(3, idx);

  delete[] buffer;
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv); 
  int ret = RUN_ALL_TESTS();
  return ret;
}

