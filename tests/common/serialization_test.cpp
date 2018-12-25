#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "common/serialization.h"
#include <gtest/gtest.h>
#include <stdint.h>
#include "common/ob_define.h"
using namespace oceanbase;
using namespace oceanbase::common;

static const int64_t BUF_SIZE = 64;

void write_buf_to_file(const char *filename,const char *buf,int64_t size)
{
  if (NULL == buf || size <= 0 || NULL == filename)
    return;
//  int fd = open(filename,O_RDWR|O_CREAT,0755);
//  if (fd < 0)
//    return;
//  if (write(fd,buf,size) != size)
//  {
//    printf("write buf to file error\n");
//  }
//  close(fd);
  return;
}

TEST(serialization,int8)
{
  char *buf = static_cast<char *>(malloc(BUF_SIZE));
  int64_t pos = 0;

  for(int i=0;i<BUF_SIZE;++i)
  {
    ASSERT_EQ(serialization::encode_i8(buf,BUF_SIZE,pos,static_cast<int8_t>(i)),OB_SUCCESS);
  }
  write_buf_to_file("serialization_int8",buf,BUF_SIZE);
  ASSERT_EQ(pos,BUF_SIZE);
  pos = 0;
  for(int i=0;i<BUF_SIZE;++i)
  {
    int8_t t = 0;
    ASSERT_EQ(serialization::decode_i8(buf,BUF_SIZE,pos,&t),OB_SUCCESS);
    ASSERT_EQ(t,i);
  }
  
  uint8_t k = 255;
  pos = 0;
  ASSERT_EQ(serialization::encode_i8(buf,BUF_SIZE,pos,static_cast<int8_t>(k)),OB_SUCCESS);
  uint8_t v = 0;
  pos = 0;
  ASSERT_EQ(serialization::decode_i8(buf,BUF_SIZE,pos,reinterpret_cast<int8_t *>(&v)),OB_SUCCESS);
  ASSERT_EQ(v,k);
  ASSERT_EQ(pos,1);
  free(buf);

}

TEST(serialization,int16)
{
  char *buf = static_cast<char *>(malloc(BUF_SIZE));
  int64_t pos = 0;
  ASSERT_EQ(serialization::encode_i16(buf,BUF_SIZE,pos,20000),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_i16(buf,BUF_SIZE,pos,INT16_MAX),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_i16(buf,BUF_SIZE,pos,INT16_MIN),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_i16(buf,BUF_SIZE,pos,static_cast<int16_t>(UINT16_MAX)),OB_SUCCESS);
  ASSERT_EQ(pos,8);
  write_buf_to_file("serialization_int16",buf,pos);
  pos = 0;
  int16_t t = 0;
  ASSERT_EQ(serialization::decode_i16(buf,BUF_SIZE,pos,&t),OB_SUCCESS);
  ASSERT_EQ(t,20000);
  ASSERT_EQ(serialization::decode_i16(buf,BUF_SIZE,pos,&t),OB_SUCCESS);
  ASSERT_EQ(t,INT16_MAX); 
  ASSERT_EQ(serialization::decode_i16(buf,BUF_SIZE,pos,&t),OB_SUCCESS);
  ASSERT_EQ(t,INT16_MIN);
  uint16_t v = 0;
  ASSERT_EQ(serialization::decode_i16(buf,BUF_SIZE,pos,reinterpret_cast<int16_t *>(&v)),OB_SUCCESS);
  ASSERT_EQ(v,UINT16_MAX);
  ASSERT_EQ(pos,8);
  free(buf);
}

TEST(serialization,int32)
{
  char *buf = static_cast<char *>(malloc(BUF_SIZE));
  int64_t pos = 0;
  ASSERT_EQ(serialization::encode_i32(buf, BUF_SIZE,pos,280813453),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_i32(buf, BUF_SIZE,pos,INT32_MAX),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_i32(buf, BUF_SIZE,pos,INT32_MIN),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_i32(buf, BUF_SIZE,pos,static_cast<int32_t>(UINT32_MAX)),OB_SUCCESS);
  ASSERT_EQ(pos,16);
  write_buf_to_file("serialization_int32",buf,pos);
  pos = 0;
  int32_t t = 0;
  ASSERT_EQ(serialization::decode_i32(buf,BUF_SIZE,pos,&t),OB_SUCCESS);
  ASSERT_EQ(t,280813453);
  ASSERT_EQ(serialization::decode_i32(buf,BUF_SIZE,pos,&t),OB_SUCCESS);
  ASSERT_EQ(t,INT32_MAX);
  ASSERT_EQ(serialization::decode_i32(buf,BUF_SIZE,pos,&t),OB_SUCCESS);
  ASSERT_EQ(t,INT32_MIN);
  uint32_t v = 0;
  ASSERT_EQ(serialization::decode_i32(buf,BUF_SIZE,pos,reinterpret_cast<int32_t *>(&v)),OB_SUCCESS);
  ASSERT_EQ(v,UINT32_MAX);
  ASSERT_EQ(pos,16);
  free(buf);
}

TEST(serialization,int64)
{
  char *buf = static_cast<char *>(malloc(BUF_SIZE));
  int64_t pos = 0;
  ASSERT_EQ(serialization::encode_i64(buf,BUF_SIZE,pos,15314313412536),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_i64(buf,BUF_SIZE,pos,INT64_MAX),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_i64(buf,BUF_SIZE,pos,INT64_MIN),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_i64(buf,BUF_SIZE,pos,static_cast<int64_t>(UINT64_MAX)),OB_SUCCESS);
  ASSERT_EQ(pos,32);
  write_buf_to_file("serialization_int64",buf,pos);
  pos = 0;
  int64_t t = 0;
  ASSERT_EQ(serialization::decode_i64(buf,BUF_SIZE,pos,&t),OB_SUCCESS);
  ASSERT_EQ(t,15314313412536); 
  ASSERT_EQ(serialization::decode_i64(buf,BUF_SIZE,pos,&t),OB_SUCCESS);
  ASSERT_EQ(t,INT64_MAX);
  ASSERT_EQ(serialization::decode_i64(buf,BUF_SIZE,pos,&t),OB_SUCCESS);
  ASSERT_EQ(t,INT64_MIN);

  uint64_t v = 0;
  ASSERT_EQ(serialization::decode_i64(buf,BUF_SIZE,pos,reinterpret_cast<int64_t *>(&v)),OB_SUCCESS);
  ASSERT_EQ(v,UINT64_MAX);
  ASSERT_EQ(pos,32);
  free(buf);
}

TEST(serialization,vint32)
{
  int64_t need_len = serialization::encoded_length_vi32(1311317890);
  need_len += serialization::encoded_length_vi32(1);
  need_len += serialization::encoded_length_vi32(INT32_MAX);
  need_len += serialization::encoded_length_vi32(INT32_MIN);
  need_len += serialization::encoded_length_vi32(static_cast<int32_t>(UINT32_MAX));
  char *buf = static_cast<char *>(malloc(need_len));
  assert(buf != NULL);
  int64_t pos = 0;
  ASSERT_EQ(serialization::encode_vi32(buf,need_len,pos,1311317890),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_vi32(buf,need_len,pos,1),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_vi32(buf,need_len,pos,INT32_MAX),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_vi32(buf,need_len,pos,INT32_MIN),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_vi32(buf,need_len,pos,static_cast<int32_t>(UINT32_MAX)),OB_SUCCESS);
  ASSERT_EQ(pos,need_len);
  write_buf_to_file("serialization_vint32",buf,pos);
  pos = 0;
  int32_t t = 0;
  ASSERT_EQ(serialization::decode_vi32(buf,need_len,pos,&t),OB_SUCCESS);
  ASSERT_EQ(t,1311317890); 
  ASSERT_EQ(serialization::decode_vi32(buf,need_len,pos,&t),OB_SUCCESS);
  ASSERT_EQ(t,1); 
  ASSERT_EQ(serialization::decode_vi32(buf,need_len,pos,&t),OB_SUCCESS);
  ASSERT_EQ(t,INT32_MAX);
  ASSERT_EQ(serialization::decode_vi32(buf,need_len,pos,&t),OB_SUCCESS);
  ASSERT_EQ(t,INT32_MIN);

  uint32_t v = 0;
  ASSERT_EQ(serialization::decode_vi32(buf,need_len,pos,reinterpret_cast<int32_t *>(&v)),OB_SUCCESS);
  ASSERT_EQ(v,UINT32_MAX);

  ASSERT_EQ(pos,need_len);
  free(buf);
}


TEST(serialization,vint64)
{

  int64_t need_len = (serialization::encoded_length_vi64(15/*311317890*/));
  need_len += (serialization::encoded_length_vi64(INT64_MAX));
  need_len += (serialization::encoded_length_vi64(INT64_MIN));
  need_len += (serialization::encoded_length_vi64(static_cast<int64_t>(UINT64_MAX)));

  char *buf = static_cast<char *>(malloc(need_len));
  assert(buf != NULL);
  int64_t pos = 0;
  ASSERT_EQ(serialization::encode_vi64(buf,need_len,pos,15/*311317890*/),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_vi64(buf,need_len,pos,INT64_MAX),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_vi64(buf,need_len,pos,INT64_MIN),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_vi64(buf,need_len,pos,static_cast<int64_t>(UINT64_MAX)),OB_SUCCESS);
  ASSERT_EQ(pos,need_len);
  write_buf_to_file("serialization_vint64",buf,pos);
  pos = 0;
  int64_t t = 0;
  ASSERT_EQ(serialization::decode_vi64(buf,need_len,pos,&t),OB_SUCCESS);
  ASSERT_EQ(t,15/*311317890*/); 
  ASSERT_EQ(serialization::decode_vi64(buf,need_len,pos,&t),OB_SUCCESS);
  ASSERT_EQ(t,INT64_MAX);
  ASSERT_EQ(serialization::decode_vi64(buf,need_len,pos,&t),OB_SUCCESS);
  ASSERT_EQ(t,INT64_MIN);

  uint64_t v = 0;
  ASSERT_EQ(serialization::decode_vi64(buf,need_len,pos,reinterpret_cast<int64_t *>(&v)),OB_SUCCESS);
  ASSERT_EQ(v,UINT64_MAX);
  ASSERT_EQ(pos,need_len);

  free(buf);
}

TEST(serialization,_bool_)
{
  char *buf = static_cast<char *>(malloc(BUF_SIZE));
  int64_t pos = 0;
  ASSERT_EQ(serialization::encode_bool(buf,BUF_SIZE,pos,true),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_bool(buf,BUF_SIZE,pos,false),OB_SUCCESS);
  bool val = false;
  ASSERT_EQ(serialization::encode_bool(buf,BUF_SIZE,pos,val),OB_SUCCESS);
  val = true;
  ASSERT_EQ(serialization::encode_bool(buf,BUF_SIZE,pos,val),OB_SUCCESS);
  ASSERT_EQ(pos,4);
  write_buf_to_file("serialization_bool",buf,pos);
  pos = 0;
  bool t;
  ASSERT_EQ(serialization::decode_bool(buf,BUF_SIZE,pos,&t),OB_SUCCESS);
  ASSERT_EQ(t,true);
  ASSERT_EQ(serialization::decode_bool(buf,BUF_SIZE,pos,&t),OB_SUCCESS);
  ASSERT_EQ(t,false);
  ASSERT_EQ(serialization::decode_bool(buf,BUF_SIZE,pos,&t),OB_SUCCESS);
  ASSERT_EQ(t,false);
  ASSERT_EQ(serialization::decode_bool(buf,BUF_SIZE,pos,&t),OB_SUCCESS);
  ASSERT_EQ(t,true);
  ASSERT_EQ(pos,4);
  free(buf);
}
TEST(serialization,_buf_)
{
  const char *buf_1 = "aust_for_test_1";
  const char *buf_2 = "just_for_test_2";
  const char *buf_3 = 0;
  const char *buf_4 = "test_4";

  int64_t need_len = serialization::encoded_length_vstr(buf_1);
  need_len += serialization::encoded_length_vstr(buf_2);
  need_len += serialization::encoded_length_vstr(buf_3);
  need_len += serialization::encoded_length_vstr(buf_4);
  
  char *buf = static_cast<char *>(malloc(need_len));
  int64_t pos = 0;
  ASSERT_EQ(serialization::encode_vstr(buf,need_len,pos,buf_1),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_vstr(buf,need_len,pos,buf_2),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_vstr(buf,need_len,pos,buf_3),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_vstr(buf,need_len,pos,buf_4),OB_SUCCESS);
  ASSERT_EQ(pos,need_len);
  write_buf_to_file("serialization_buf",buf,pos);
  pos = 0;
  int64_t len_1;
  int64_t len_2;
  int64_t len_3;
  int64_t len_4;
  const char *b1 = serialization::decode_vstr(buf,need_len,pos,&len_1);
  ASSERT_EQ(len_1,static_cast<int64_t>(static_cast<int64_t>(strlen(buf_1) + 1)));
  ASSERT_EQ(strncmp(b1,buf_1,strlen(b1)),0);
  const char *b2 = serialization::decode_vstr(buf,need_len,pos,&len_2);
  ASSERT_EQ(len_2,static_cast<int64_t>(static_cast<int64_t>(strlen(buf_2) + 1)));
  ASSERT_EQ(strncmp(b2,buf_2,strlen(b2)),0);
  const char *b3 = serialization::decode_vstr(buf,need_len,pos,&len_3);
  (void)b3;
  ASSERT_EQ(len_3,0);
  const char *b4 = serialization::decode_vstr(buf,need_len,pos,&len_4);
  ASSERT_EQ(len_4,static_cast<int64_t>(static_cast<int64_t>(strlen(buf_4) + 1)));
  ASSERT_EQ(strncmp(b4,buf_4,strlen(b4)),0);
  ASSERT_EQ(need_len,pos);
  free(buf);
}

TEST(serialization,_buf_copy)
{
  const char *buf_1 = "aust_for_test_1";
  const char *buf_2 = "just_for_test_2";
  const char *buf_3 = 0;
  const char *buf_4 = "test_4";

  int64_t need_len = serialization::encoded_length_vstr(buf_1);
  need_len += serialization::encoded_length_vstr(buf_2);
  need_len += serialization::encoded_length_vstr(buf_3);
  need_len += serialization::encoded_length_vstr(buf_4);
  
  char *buf = static_cast<char *>(malloc(need_len));
  int64_t pos = 0;
  ASSERT_EQ(serialization::encode_vstr(buf,need_len,pos,buf_1),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_vstr(buf,need_len,pos,buf_2),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_vstr(buf,need_len,pos,buf_3),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_vstr(buf,need_len,pos,buf_4),OB_SUCCESS);
  ASSERT_EQ(pos,need_len);
  pos = 0;
  int64_t len_1;
  int64_t len_2;
  int64_t len_3;
  int64_t len_4;

  char *buf_out = static_cast<char *>(malloc(need_len));
  int64_t tt = serialization::decoded_length_vstr(buf,need_len,pos);
  ASSERT_EQ(tt,static_cast<int64_t>(static_cast<int64_t>(strlen(buf_1) + 1)));
  const char *b1 = serialization::decode_vstr(buf,need_len,pos,buf_out,need_len,&len_1);
  ASSERT_EQ(len_1,static_cast<int64_t>(strlen(buf_1) + 1));
  ASSERT_EQ(strncmp(b1,buf_1,strlen(b1)),0);
  const char *b2 = serialization::decode_vstr(buf,need_len,pos,buf_out,need_len,&len_2);
  ASSERT_EQ(len_2,static_cast<int64_t>(strlen(buf_2) + 1));
  ASSERT_EQ(strncmp(b2,buf_2,strlen(b2)),0);
  const char *b3 = serialization::decode_vstr(buf,need_len,pos,buf_out,need_len,&len_3);
  (void)b3;
  ASSERT_EQ(len_3,0);
  const char *b4 = serialization::decode_vstr(buf,need_len,pos,buf_out,need_len,&len_4);
  ASSERT_EQ(len_4,static_cast<int64_t>(strlen(buf_4) + 1));
  ASSERT_EQ(strncmp(b4,buf_4,strlen(b4)),0);
  ASSERT_EQ(need_len,pos);
  free(buf);
  free(buf_out);
}
#if 0
TEST(serialization,inti)
{
  int64_t need_len = 0;
  need_len += serialization::encoded_length_int(1);
  need_len += serialization::encoded_length_int(28);
  need_len += serialization::encoded_length_int(-1);
  need_len += serialization::encoded_length_int(INT32_MIN);
  need_len += serialization::encoded_length_int(static_cast<int64_t>(UINT32_MAX));
  need_len += serialization::encoded_length_int(INT64_MAX);
  need_len += serialization::encoded_length_int(INT64_MIN);
  need_len += serialization::encoded_length_int(UINT64_MAX);
  need_len += serialization::encoded_length_int(124543900);


  char *buf = static_cast<char *>(malloc(need_len));
  assert(buf != NULL);
  int64_t pos = 0;
  ASSERT_EQ(serialization::encode_int(buf,need_len,pos,1),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_int(buf,need_len,pos,28),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_int(buf,need_len,pos,-1),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_int(buf,need_len,pos,INT32_MIN),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_int(buf,need_len,pos,UINT32_MAX),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_int(buf,need_len,pos,INT64_MAX),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_int(buf,need_len,pos,INT64_MIN),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_int(buf,need_len,pos,static_cast<int64_t>(UINT64_MAX)),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_int(buf,need_len,pos,124543900),OB_SUCCESS);

  printf("need:%ld\n",need_len);
  ASSERT_EQ(pos,need_len);
  write_buf_to_file("serialization_inti",buf,pos);
  pos = 0;
  int64_t t = 0;
  int8_t op = 0;
  ASSERT_EQ(serialization::decode_int(buf,need_len,pos,t,op),OB_SUCCESS);
  ASSERT_EQ(t,1); 
  ASSERT_EQ(serialization::decode_int(buf,need_len,pos,t,op),OB_SUCCESS);
  ASSERT_EQ(t,28); 
  ASSERT_EQ(serialization::decode_int(buf,need_len,pos,t,op),OB_SUCCESS);
  ASSERT_EQ(t,-1);
  ASSERT_EQ(serialization::decode_int(buf,need_len,pos,t,op),OB_SUCCESS);
  ASSERT_EQ(t,INT32_MIN);
  ASSERT_EQ(serialization::decode_int(buf,need_len,pos,t,op),OB_SUCCESS);
  ASSERT_EQ(t,UINT32_MAX);
  ASSERT_EQ(serialization::decode_int(buf,need_len,pos,t,op),OB_SUCCESS);
  ASSERT_EQ(t,INT64_MAX);
  ASSERT_EQ(serialization::decode_int(buf,need_len,pos,t,op),OB_SUCCESS);
  ASSERT_EQ(t,INT64_MIN);

  ASSERT_EQ(serialization::decode_int(buf,need_len,pos,t,op),OB_SUCCESS);
  uint64_t  v = static_cast<uint64_t>(t);
  ASSERT_EQ(v,UINT64_MAX);

  ASSERT_EQ(serialization::decode_int(buf,need_len,pos,t,op),OB_SUCCESS);
  ASSERT_EQ(t,124543900);

  ASSERT_EQ(pos,need_len);
  free(buf);
}

TEST(serialization,_str_buf_)
{
  const char *buf_1 = "aust_for_test_1";
  const char *buf_2 = "just_for_test_2";
  const char *buf_3 = NULL;
  const char *buf_4 = "test_4";

  int64_t need_len = serialization::encoded_length_str(strlen(buf_1));
  need_len += serialization::encoded_length_str(strlen(buf_2));
  need_len += serialization::encoded_length_str(0);
  need_len += serialization::encoded_length_str(strlen(buf_4));
  
  char *buf = static_cast<char *>(malloc(need_len));
  int64_t pos = 0;
  ASSERT_EQ(serialization::encode_str(buf,need_len,pos,buf_1,strlen(buf_1)),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_str(buf,need_len,pos,buf_2,strlen(buf_2)),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_str(buf,need_len,pos,buf_3,0),OB_SUCCESS);
  ASSERT_EQ(serialization::encode_str(buf,need_len,pos,buf_4,strlen(buf_4)),OB_SUCCESS);
  ASSERT_EQ(pos,need_len);
  write_buf_to_file("serialization_buf",buf,pos);
  pos = 0;
  int64_t len_1;
  int64_t len_2;
  int64_t len_3;
  int64_t len_4;
  const char *b1 = serialization::decode_str(buf,need_len,pos,len_1);
  ASSERT_EQ(len_1,static_cast<int64_t>(static_cast<int64_t>(strlen(buf_1))));
  ASSERT_EQ(strncmp(b1,buf_1,strlen(buf_1)),0);
  const char *b2 = serialization::decode_str(buf,need_len,pos,len_2);
  ASSERT_EQ(len_2,static_cast<int64_t>(static_cast<int64_t>(strlen(buf_2))));
  ASSERT_EQ(strncmp(b2,buf_2,strlen(buf_2)),0);
  const char *b3 = serialization::decode_str(buf,need_len,pos,len_3);
  (void)b3;
  ASSERT_EQ(len_3,0);
  const char *b4 = serialization::decode_str(buf,need_len,pos,len_4);
  ASSERT_EQ(len_4,static_cast<int64_t>(static_cast<int64_t>(strlen(buf_4))));
  ASSERT_EQ(strncmp(b4,buf_4,strlen(buf_4)),0);
  ASSERT_EQ(need_len,pos);
  free(buf);
}

#endif
int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
