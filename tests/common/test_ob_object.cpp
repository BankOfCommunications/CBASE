#include "ob_object.h"
#include "ob_define.h"
#include "ob_action_flag.h"
#include <gtest/gtest.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "tbnet.h"
#include <fcntl.h>

using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;

void write_buf_to_file(const char *filename,const char *buf,int64_t size)
{
  if (NULL == buf || size <= 0 || NULL == filename)
    return;
  int fd = open(filename,O_RDWR|O_CREAT,0755);
  if (fd < 0)
    return;
  if (write(fd,buf,size) != size)
  {
    printf("write buf to file error\n");
  }
  close(fd);
  return;
}

TEST(ObObj, test_apply)
{
  ObObj t1;
  t1.set_int(80469200);
  ObObj t2;
  t2.set_int(16, true);
  t1.apply(t2);
  int64_t val = 0;
  ASSERT_EQ(OB_SUCCESS, t1.get_int(val));
  ASSERT_EQ(80469216, val);
}

TEST(ObObj, test_bug)
{
  ObObj obj;
  obj.set_int(100, false);
  char buf[2048];
  int64_t len = 2048;
  int64_t pos = 0;
  ASSERT_EQ(obj.serialize(buf, len, pos), OB_SUCCESS);

  pos = 0;
  ObObj temp;
  temp.set_int(200, true);
  temp.reset();

  ASSERT_EQ(temp.deserialize(buf, len, pos), OB_SUCCESS);
  ASSERT_EQ(temp == obj, true);

  ObObj test;
  test.set_int(300, true);
  test = temp;
  ASSERT_EQ(test == temp, true);
}

TEST(ObObj,Serialize_bool)
{
  ObObj t;
  t.set_bool(true);

  int64_t len = t.get_serialize_size();

  ASSERT_EQ(len,2);

  char *buf = (char *)malloc(len);

  int64_t pos = 0;
  ASSERT_EQ(t.serialize(buf,len,pos),OB_SUCCESS);
  ASSERT_EQ(pos,len);

  ObObj f;
  pos = 0;
  ASSERT_EQ(f.deserialize(buf,len,pos),OB_SUCCESS);
  ASSERT_EQ(pos,len);

  bool r = false;
  bool l = false;

  ASSERT_EQ(f.get_type(),t.get_type());
  f.get_bool(r);

  t.get_bool(l);
  ASSERT_EQ(r,l);
  free(buf);
}



TEST(ObObj,Serialize_int_add)
{
  ObObj t;
  t.set_int(9900);

  int64_t len = t.get_serialize_size();

  ASSERT_EQ(len,3);

  char *buf = (char *)malloc(len);

  int64_t pos = 0;
  ASSERT_EQ(t.serialize(buf,len,pos),OB_SUCCESS);
  ASSERT_EQ(pos,len);

  ObObj f;
  pos = 0;
  ASSERT_EQ(f.deserialize(buf,len,pos),OB_SUCCESS);
  ASSERT_EQ(pos,len);

  int64_t r = 0;
  int64_t l = 0;

  ASSERT_EQ(f.get_type(),t.get_type());
  f.get_int(r);

  t.get_int(l);
  ASSERT_EQ(r,l);

  free(buf);
}

TEST(ObObj,Serialize_int)
{
  ObObj t;
  t.set_int(5900,true);

  int64_t len = t.get_serialize_size();

  ASSERT_EQ(len,3);

  char *buf = (char *)malloc(len);

  int64_t pos = 0;
  ASSERT_EQ(t.serialize(buf,len,pos),OB_SUCCESS);
  ASSERT_EQ(pos,len);

  ObObj f;
  pos = 0;
  ASSERT_EQ(f.deserialize(buf,len,pos),OB_SUCCESS);
  ASSERT_EQ(pos,len);

  int64_t r = 0;
  int64_t l = 0;

  ASSERT_EQ(f.get_type(),t.get_type());
  f.get_int(r);

  t.get_int(l);
  ASSERT_EQ(r,l);
  free(buf);
}

TEST(ObObj,int_boundary)
{
  int64_t need_len = 0;

  int64_t data[] = {1,28,-1,INT32_MIN,static_cast<int64_t>(UINT32_MAX),INT64_MAX,INT64_MIN,UINT64_MAX,124543900};
  for(uint32_t i=0;i<sizeof(data)/sizeof(data[0]);++i)
  {
    need_len += serialization::encoded_length_int(data[i]);
  }
  char *buf = (char *)malloc(need_len);
  ObObj t;
  int64_t pos = 0;

  for(uint32_t i=0;i<sizeof(data)/sizeof(data[0]);++i)
  {
    t.reset();
    t.set_int(data[i]);
    ASSERT_EQ(t.serialize(buf,need_len,pos),OB_SUCCESS);
  }

  pos = 0;

  int64_t v = 0;
  for(uint32_t i=0;i<sizeof(data)/sizeof(data[0]);++i)
  {
    t.reset();
    ASSERT_EQ(t.deserialize(buf,need_len,pos),OB_SUCCESS);
    ASSERT_EQ(t.get_int(v),OB_SUCCESS);
    ASSERT_EQ(v,data[i]);
  }
  free(buf);
}


TEST(ObObj,Serialize_datetime)
{
  ObObj t1;
  t1.set_datetime(1348087492,true);

  int64_t len = t1.get_serialize_size();

  char *buf = (char *)malloc(len);

  int64_t pos = 0;
  ASSERT_EQ(t1.serialize(buf,len,pos),OB_SUCCESS);

  ObObj f;
  pos = 0;
  ASSERT_EQ(f.deserialize(buf,len,pos),OB_SUCCESS);

  ASSERT_EQ(f.get_type(),t1.get_type());


  ObDateTime l = 0;
  ObDateTime r = 0;

  f.get_datetime(l);
  t1.get_datetime(r);

  ASSERT_EQ(l,r);

  free(buf);

}

TEST(ObObj,Serialize_precise_datetime)
{
  ObObj t1;
  t1.set_precise_datetime(221348087492);

  int64_t len = t1.get_serialize_size();

  char *buf = (char *)malloc(len);

  int64_t pos = 0;
  ASSERT_EQ(t1.serialize(buf,len,pos),OB_SUCCESS);

  ObObj f;
  pos = 0;
  ASSERT_EQ(f.deserialize(buf,len,pos),OB_SUCCESS);

  ASSERT_EQ(f.get_type(),t1.get_type());


  ObPreciseDateTime l = 0;
  ObPreciseDateTime r = 0;

  f.get_precise_datetime(l);
  t1.get_precise_datetime(r);

  ASSERT_EQ(l,r);

  free(buf);
}

TEST(ObObj,Serialize_modifytime)
{
  ObModifyTime data[] = {1,28,1,static_cast<int64_t>(UINT32_MAX),INT64_MAX,124543900,221348087492};
  int64_t need_len = 0;
  for(uint32_t i=0;i<sizeof(data)/sizeof(data[0]);++i)
  {
    need_len += serialization::encoded_length_modifytime(data[i]);
  }
  char *buf = (char *)malloc(need_len);
  ObObj t;
  int64_t pos = 0;

  for(uint32_t i=0;i<sizeof(data)/sizeof(data[0]);++i)
  {
    t.reset();
    t.set_modifytime(data[i]);
    ASSERT_EQ(t.serialize(buf,need_len,pos),OB_SUCCESS);
  }

  pos = 0;

  ObModifyTime v = 0;
  for(uint32_t i=0;i<sizeof(data)/sizeof(data[0]);++i)
  {
    t.reset();
    ASSERT_EQ(t.deserialize(buf,need_len,pos),OB_SUCCESS);
    ASSERT_EQ(t.get_modifytime(v),OB_SUCCESS);
    ASSERT_EQ(v,data[i]);
  }

  free(buf);
}

TEST(ObObj,Serialize_createtime)
{
  ObCreateTime data[] = {1,28,1,static_cast<int64_t>(UINT32_MAX),INT64_MAX,124543900,221348087492};
  int64_t need_len = 0;
  for(uint32_t i=0;i<sizeof(data)/sizeof(data[0]);++i)
  {
    need_len += serialization::encoded_length_createtime(data[i]);
  }
  char *buf = (char *)malloc(need_len);
  ObObj t;
  int64_t pos = 0;

  for(uint32_t i=0;i<sizeof(data)/sizeof(data[0]);++i)
  {
    t.reset();
    t.set_createtime(data[i]);
    ASSERT_EQ(t.serialize(buf,need_len,pos),OB_SUCCESS);
  }

  pos = 0;

  ObCreateTime v = 0;
  for(uint32_t i=0;i<sizeof(data)/sizeof(data[0]);++i)
  {
    t.reset();
    ASSERT_EQ(t.deserialize(buf,need_len,pos),OB_SUCCESS);
    ASSERT_EQ(t.get_createtime(v),OB_SUCCESS);
    ASSERT_EQ(v,data[i]);
  }
  free(buf);
}


TEST(ObObj,extend_type)
{
  ObObj t1;
  t1.set_ext(90);

  int64_t len = t1.get_serialize_size();

  ASSERT_EQ(len,2);

  char *buf = (char *)malloc(len);

  int64_t pos = 0;
  ASSERT_EQ(t1.serialize(buf,len,pos),OB_SUCCESS);

  ObObj f;
  pos = 0;
  ASSERT_EQ(f.deserialize(buf,len,pos),OB_SUCCESS);

  int64_t e = 0;
  ASSERT_EQ(f.get_ext(e),0);

  ASSERT_EQ(e,90);
  free(buf);
}

TEST(ObObj,Serialize)
{
  ObObj t1;
  ObObj t2;
  ObObj t3;
  ObObj t4;
  ObObj t5;
  ObObj t6;
  ObObj t7;
  ObObj t8;
  ObObj t9;
  char buf[1024];
  int64_t len = 1024;

  //int
  t1.set_int(9871345);
  int64_t pos = 0;
  ASSERT_EQ(t1.serialize(buf,len,pos),OB_SUCCESS);
  //str
  ObString d;
  const char *tmp = "Hello";
  d.assign_ptr(const_cast<char *>(tmp),5);
  t2.set_varchar(d);
  ASSERT_EQ(t2.serialize(buf,len,pos),OB_SUCCESS);
  //null
  t3.set_null();
  ASSERT_EQ(t3.serialize(buf,len,pos),OB_SUCCESS);
  //datetime
  t6.set_datetime(1234898766);
  ASSERT_EQ(t6.serialize(buf,len,pos),OB_SUCCESS);
  //precise_datetime
  t7.set_precise_datetime(2348756909000000L);
  ASSERT_EQ(t7.serialize(buf,len,pos),OB_SUCCESS);
  //bool
  t8.set_bool(true);
  ASSERT_EQ(t8.serialize(buf,len,pos),OB_SUCCESS);
  t9.set_bool(false);
  ASSERT_EQ(t9.serialize(buf,len,pos),OB_SUCCESS);

  write_buf_to_file("Object",buf,pos);

  ObObj f1;
  ObObj f2;
  ObObj f3;
  ObObj f4;
  ObObj f5;
  ObObj f6;
  ObObj f7;
  ObObj f8;
  ObObj f9;
  pos = 0;
  //int
  ASSERT_EQ(f1.deserialize(buf,len,pos),OB_SUCCESS);
  int64_t r = 0;
  int64_t l = 0;
  ASSERT_EQ(f1.get_type(),t1.get_type());
  f1.get_int(r);
  t1.get_int(l);
  ASSERT_EQ(r,l);
  //printf("%ld,%ld\n",r,l);

  //str
  ASSERT_EQ(f2.deserialize(buf,len,pos),OB_SUCCESS);
  ASSERT_EQ(f2.get_type(),t2.get_type());
  ObString str;
  ASSERT_EQ(f2.get_varchar(str),OB_SUCCESS);
  //printf("str:%s,len:%d,d:%s,len:%d\n",str.ptr(),str.length(),d.ptr(),d.length());
  ASSERT_TRUE( str == d);

  //null
  ASSERT_EQ(f3.deserialize(buf,len,pos),OB_SUCCESS);
  ASSERT_EQ(f3.get_type(),t3.get_type());

  //second
  ASSERT_EQ(f6.deserialize(buf,len,pos),OB_SUCCESS);
  ASSERT_EQ(f6.get_type(),t6.get_type());

  ObDateTime rt = 0;
  ObDateTime lt = 0;

  f6.get_datetime(rt);
  t6.get_datetime(lt);
  ASSERT_EQ(rt,lt);

  //microsecond
  ASSERT_EQ(f7.deserialize(buf,len,pos),OB_SUCCESS);
  ASSERT_EQ(f7.get_type(),t7.get_type());

  ObPreciseDateTime rm = 0;
  ObPreciseDateTime lm = 0;

  f7.get_precise_datetime(rm);
  t7.get_precise_datetime(lm);
  ASSERT_EQ(rm,lm);

  //bool

  bool lb = false;
  bool rb = true;
  ASSERT_EQ(f8.deserialize(buf,len,pos),OB_SUCCESS);
  ASSERT_EQ(f8.get_type(),t8.get_type());
  f8.get_bool(lb);
  t8.get_bool(rb);
  ASSERT_EQ(lb, rb);

  ASSERT_EQ(f9.deserialize(buf,len,pos),OB_SUCCESS);
  ASSERT_EQ(f9.get_type(),t9.get_type());
  f9.get_bool(lb);
  t9.get_bool(rb);
  ASSERT_EQ(lb, rb);

}

TEST(ObObj, performance)
{
  // int64_t start_time = tbsys::CTimeUtil::getTime();
  //const int64_t MAX_COUNT = 10 * 1000 * 1000L;
  const int64_t MAX_COUNT = 1000L;
  ObObj obj;
  char buf[2048];
  int64_t len = 2048;
  int64_t pos = 0;
  int64_t data = 0;
  for (int64_t i = 0; i < MAX_COUNT; ++i)
  {
    obj.set_int(i);
    pos = 0;
    ASSERT_EQ(obj.serialize(buf, len, pos), OB_SUCCESS);
    pos = 0;
    ASSERT_EQ(obj.deserialize(buf, len, pos), OB_SUCCESS);
    ASSERT_EQ(obj.get_int(data), OB_SUCCESS);
    ASSERT_EQ(data, i);
  }

  const char *tmp = "Hello12344556666777777777777545352454254254354354565463241242354345345345235345";
  ObObj obj2;
  ObString string;
  ObString string2;
  string.assign_ptr(const_cast<char *>(tmp), 1024);
  obj2.set_varchar(string);
  for (register int64_t i = 0; i < MAX_COUNT; ++i )
  {
    pos = 0;
    ASSERT_EQ(obj2.serialize(buf, len, pos), OB_SUCCESS);
    pos = 0;
    ASSERT_EQ(obj2.deserialize(buf, len, pos), OB_SUCCESS);
    ASSERT_EQ(obj2.get_varchar(string2), OB_SUCCESS);
  }
  // int64_t end_time = tbsys::CTimeUtil::getTime();
  //printf("using time:%ld\n", end_time - start_time);
}

TEST(ObObj,NOP)
{
  ObObj src_obj;
  ObObj mut_obj;
  int64_t val = 0;
  int64_t mutation = 5;
  ///create time
  src_obj.set_ext(ObActionFlag::OP_NOP);
  mut_obj.set_createtime(mutation);
  ASSERT_EQ(src_obj.apply(mut_obj),OB_SUCCESS);
  ASSERT_EQ(src_obj.get_createtime(val),OB_SUCCESS);
  ASSERT_EQ(val, mutation);
  ASSERT_FALSE(src_obj.get_add());

  ///create time
  src_obj.set_ext(ObActionFlag::OP_NOP);
  mut_obj.set_modifytime(mutation);
  ASSERT_EQ(src_obj.apply(mut_obj),OB_SUCCESS);
  ASSERT_EQ(src_obj.get_modifytime(val),OB_SUCCESS);
  ASSERT_EQ(val, mutation);
  ASSERT_FALSE(src_obj.get_add());

  ///precise time
  src_obj.set_ext(ObActionFlag::OP_NOP);
  mut_obj.set_precise_datetime(mutation);
  ASSERT_EQ(src_obj.apply(mut_obj),OB_SUCCESS);
  ASSERT_EQ(src_obj.get_precise_datetime(val),OB_SUCCESS);
  ASSERT_EQ(val, mutation);
  ASSERT_FALSE(src_obj.get_add());

  src_obj.set_ext(ObActionFlag::OP_NOP);
  mut_obj.set_precise_datetime(mutation, true);
  ASSERT_EQ(src_obj.apply(mut_obj),OB_SUCCESS);
  ASSERT_EQ(src_obj.get_precise_datetime(val),OB_SUCCESS);
  ASSERT_EQ(val, mutation);
  ASSERT_TRUE(src_obj.get_add());

  ///date time
  src_obj.set_ext(ObActionFlag::OP_NOP);
  mut_obj.set_datetime(mutation);
  ASSERT_EQ(src_obj.apply(mut_obj),OB_SUCCESS);
  ASSERT_EQ(src_obj.get_datetime(val),OB_SUCCESS);
  ASSERT_EQ(val, mutation);
  ASSERT_FALSE(src_obj.get_add());

  src_obj.set_ext(ObActionFlag::OP_NOP);
  mut_obj.set_datetime(mutation, true);
  ASSERT_EQ(src_obj.apply(mut_obj),OB_SUCCESS);
  ASSERT_EQ(src_obj.get_datetime(val),OB_SUCCESS);
  ASSERT_EQ(val, mutation);
  ASSERT_TRUE(src_obj.get_add());

  /// int
  src_obj.set_ext(ObActionFlag::OP_NOP);
  mut_obj.set_int(mutation);
  ASSERT_EQ(src_obj.apply(mut_obj),OB_SUCCESS);
  ASSERT_EQ(src_obj.get_int(val),OB_SUCCESS);
  ASSERT_EQ(val, mutation);
  ASSERT_FALSE(src_obj.get_add());

  src_obj.set_ext(ObActionFlag::OP_NOP);
  mut_obj.set_int(mutation, true);
  ASSERT_EQ(src_obj.apply(mut_obj),OB_SUCCESS);
  ASSERT_EQ(src_obj.get_int(val),OB_SUCCESS);
  ASSERT_EQ(val, mutation);
  ASSERT_TRUE(src_obj.get_add());

  /// obstring
  const char * cname = "cname";
  ObString str;
  ObString res;
  str.assign((char*)cname, static_cast<int32_t>(strlen(cname)));
  src_obj.set_ext(ObActionFlag::OP_NOP);
  mut_obj.set_varchar(str);
  ASSERT_EQ(src_obj.apply(mut_obj),OB_SUCCESS);
  ASSERT_EQ(src_obj.get_varchar(res),OB_SUCCESS);
  ASSERT_EQ((uint64_t)res.length(), strlen(cname));
  ASSERT_EQ(memcmp(res.ptr(),cname, res.length()), 0);
  ASSERT_FALSE(src_obj.get_add());
}

TEST(ObObj, size_of_obj)
{
  ASSERT_EQ(16U, sizeof(ObObj));
}

class ObObjDecimalTest: public ::testing::Test
{
  public:
    ObObjDecimalTest();
    virtual ~ObObjDecimalTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObObjDecimalTest(const ObObjDecimalTest &other);
    ObObjDecimalTest& operator=(const ObObjDecimalTest &other);
  protected:
    void test(const char* dec_str, const bool is_add);
};

ObObjDecimalTest::ObObjDecimalTest()
{
}

ObObjDecimalTest::~ObObjDecimalTest()
{
}

void ObObjDecimalTest::SetUp()
{
}

void ObObjDecimalTest::TearDown()
{
}

void ObObjDecimalTest::test(const char* n1_str, const bool is_add)
{
  ObObj obj1;
  ObNumber n1;
  ASSERT_EQ(OB_SUCCESS, n1.from(n1_str));
  ASSERT_EQ(OB_SUCCESS, obj1.set_decimal(n1, 38, 38, is_add));
  // serialize & deserialize
  static const int16_t BUF_SIZE = 2048;
  char buf[BUF_SIZE];
  int64_t len = BUF_SIZE;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, obj1.serialize(buf, len, pos));
  len = pos;
  pos = 0;
  ObObj obj2;
  ObNumber n2;
  bool ret_is_add;
  ASSERT_EQ(OB_SUCCESS, obj2.deserialize(buf, len, pos));
  ASSERT_EQ(OB_SUCCESS, obj2.get_decimal(n2, ret_is_add));
  // verify
  ASSERT_EQ(is_add, ret_is_add);
  n2.to_string(buf, BUF_SIZE);
  ASSERT_STREQ(n1_str, buf);
}

TEST_F(ObObjDecimalTest, basic_test)
{
  test("0", false);
  test("0", true);
  test("-1", false);
  test("-1", true);
  test("1", false);
  test("1", true);
  test("-123456789.123456789", false);
  test("-123456789.123456789", true);
  test("123456789.123456789", false);
  test("123456789.123456789", true);
}

TEST_F(ObObjDecimalTest, test_apply)
{
  ObObj obj1;
  ObNumber n1;
  ASSERT_EQ(OB_SUCCESS, n1.from("12345.0"));
  ASSERT_EQ(OB_SUCCESS, obj1.set_decimal(n1, 38, 38));
  ObObj obj2;
  ASSERT_EQ(OB_SUCCESS, obj2.set_decimal(n1, 38, 38, true));
  ASSERT_EQ(OB_SUCCESS, obj1.apply(obj2));
  // verify
  static const int16_t BUF_SIZE = 2048;
  char buf[BUF_SIZE];
  ASSERT_EQ(OB_SUCCESS, obj1.get_decimal(n1));
  n1.to_string(buf, BUF_SIZE);
  ASSERT_STREQ("24690.0", buf);

  n1.from("13579.0");
  ASSERT_EQ(OB_SUCCESS, obj2.set_decimal(n1, 38, 38));
  ASSERT_EQ(OB_SUCCESS, obj1.apply(obj2));
  ASSERT_EQ(OB_SUCCESS, obj1.get_decimal(n1));
  n1.to_string(buf, BUF_SIZE);
  ASSERT_STREQ("13579.0", buf);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
