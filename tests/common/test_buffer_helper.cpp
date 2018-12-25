#include <gtest/gtest.h>
#include "common/ob_buffer_helper.h"

using namespace testing;
using namespace oceanbase;
using namespace common;

#define OK(value) ASSERT_EQ(common::OB_SUCCESS, (value))

struct Foo
{
  int8_t k1;
  int16_t k2;
};

char buf[1024];

TEST(TESTObBufferHelper, Writer)
{
  char tmp_buf[8];
  ObBufferWriter writer(tmp_buf, 8);

  OK(writer.write<int64_t>(32));
  ASSERT_EQ(common::OB_BUF_NOT_ENOUGH, writer.write<char>('c'));
}

TEST(TESTObBufferHelper, reader)
{
  ObBufferWriter writer(buf);
  ObBufferReader reader(buf);

  writer.write<uint16_t>(13);
  writer.write<int64_t>(32);

  const char* str = "oceanbase test";
  ObString varchar;
  varchar.assign_ptr(const_cast<char*>(str), (int32_t)strlen(str));

  writer.write_varchar(varchar);


  writer.write<int64_t>(32);

  Foo foo;
  foo.k1 = 3;
  foo.k2 = 24;

  writer.write<Foo>(foo);

  writer.write<int64_t>(32);

  const uint16_t *int_value = NULL;
  OK(reader.get<uint16_t>(int_value)); 

  ASSERT_EQ(13, *int_value);
  //ASSERT_EQ(13, reader.get<uint16_t>());
  ASSERT_EQ(32, reader.get<int64_t>());

  ASSERT_EQ(0, memcmp(str, reader.cur_ptr(), strlen(str)));
  reader.skip(strlen(str));

  ASSERT_EQ(32, reader.get<int64_t>());

  Foo* foo1 = reader.get_ptr<Foo>();
  ASSERT_EQ(3, foo1->k1);
  ASSERT_EQ(24, foo1->k2);

  ASSERT_EQ(32, reader.get<int64_t>());

}

TEST(TESTObBufferHelper, WriterAndReader)
{
  ObBufferWriter writer(buf);
  ObBufferReader reader(buf);

  writer.write<uint16_t>(13);
  writer.write<int64_t>(32);

  const char* str = "oceanbase test";
  ObString varchar;
  varchar.assign_ptr(const_cast<char*>(str), (int32_t)strlen(str));

  writer.write_varchar(varchar);

  writer.write<int64_t>(32);

  Foo foo;
  foo.k1 = 3;
  foo.k2 = 24;

  writer.write<Foo>(foo);

  writer.write<int64_t>(32);

  ASSERT_EQ(13, reader.get<uint16_t>());
  ASSERT_EQ(32, reader.get<int64_t>());

  ASSERT_EQ(0, memcmp(str, reader.cur_ptr(), strlen(str)));
  reader.skip(strlen(str));

  ASSERT_EQ(32, reader.get<int64_t>());

  Foo* foo1 = reader.get_ptr<Foo>();
  ASSERT_EQ(3, foo1->k1);
  ASSERT_EQ(24, foo1->k2);

  ASSERT_EQ(32, reader.get<int64_t>());
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

