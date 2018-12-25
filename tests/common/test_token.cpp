#include <string.h>
#include <stdlib.h>
#include<gtest/gtest.h>
#include "common/ob_token.h"
#include "common/ob_encrypt.h"
#include "common/ob_string.h"
#include "common/ob_define.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace std;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class TestToken : public ::testing::Test
{
  public:
    void SetUp()
    {
    }
    void TearDown()
    {
    }
};

//static int stringTobyte(char* dest, const char* src, int64_t src_len);
TEST_F(TestToken, stringTobyge)
{
  DES_crypt crypt;
  //(1)src 为空
  {
    char *src = NULL;
    char *dest = NULL;
    EXPECT_EQ(OB_INVALID_ARGUMENT, crypt.stringTobyte(dest, 0, src, 0));
  }

  char src[9] = "rongxuan";
  int64_t src_len = strlen(src);

  //(2)dest的位数不够src_len * 8
  {
    char dest[15];
    EXPECT_EQ(OB_INVALID_ARGUMENT, crypt.stringTobyte(dest, 15, src, src_len));
  }

  //(3)dest 为空
  {
    char *dest = NULL;
    EXPECT_EQ(OB_INVALID_ARGUMENT, crypt.stringTobyte(dest, 0, src, src_len));
  }

  //(4)dest的长度不是8的倍数
  {
    char dest[63];
    EXPECT_EQ(OB_INVALID_ARGUMENT, crypt.stringTobyte(dest, 63, src, src_len));
  }

  //(5)正确处理
  {
    char dest[64];
    EXPECT_EQ(0, crypt.stringTobyte(dest, 64, src, src_len));
  }

  //(6)dest的长度是8的倍数，且大于src_len * 8
  {
    char dest[128];
    EXPECT_EQ(0, crypt.stringTobyte(dest, 128, src, src_len));
  }
}

///int DES_crypt::byteTostring(char* dest, const int64_t dest_len, const char *src, const int64_t src_len)
TEST_F(TestToken, byteTostring)
{
  DES_crypt crypt;
  
  //(1)src 为空
  {
    char *src = NULL;
    char dest[64];
    EXPECT_EQ(OB_INVALID_ARGUMENT, crypt.byteTostring(dest, 64, src, 0));
  }

  //(2)src的长度不为8的倍数
  {
    char src[63];
    char dest[64];
    EXPECT_EQ(OB_INVALID_ARGUMENT, crypt.byteTostring(dest, 64, src, 63));
  }
 
  //(3)dest为空
  {
    char src[64];
    char *dest = NULL;
    EXPECT_EQ(OB_INVALID_ARGUMENT, crypt.byteTostring(dest, 0, src, 64));
  }

  //(4)dest的长度太小
  {
    char src[64];
    char dest[5];
    EXPECT_EQ(OB_INVALID_ARGUMENT, crypt.byteTostring(dest, 5, src, 64));
  }

  //(5)src的长度大于 src_len / 8,但内容不合法
  {
    char src[64];
    char dest[9];
    EXPECT_EQ(OB_INVALID_ARGUMENT, crypt.byteTostring(dest, 9, src, 64));
  }

  //(6)正确执行,长度合法，内容合法
  {
    char src[64];
    for(int i = 0; i < 64; i++)
    {
      if(i % 2 == 0)
      {
        src[i] = 0;
      }
      else
      {
        src[i] = 1;
      }
    }
    char dest[9];
    EXPECT_EQ(0, crypt.byteTostring(dest, 9, src, 64));
  }

  //(7) 先stringTobyte，然后byteTostring
  {
    char src[9] = "rongxuan";
    char dest[72];
    EXPECT_EQ(0, crypt.stringTobyte(dest, 72, src, strlen(src)));
    char src_to[9];
    EXPECT_EQ(0, crypt.byteTostring(src_to, 9, dest, strlen(src) * 8));
    EXPECT_EQ(0, strncmp(src, src_to, strlen(src)));
  }
}

//static int pad(char* dest, const int64_t dest_len,  int64_t &slen);
TEST_F(TestToken, pad)
{
  DES_crypt crypt;

  //(1)dest 为空
  {
    char *dest = NULL;
    int64_t dest_len = 9;
    int64_t src = 9;
    EXPECT_EQ(OB_INVALID_ARGUMENT, crypt.pad(dest, dest_len, src));
  }

  //(2)dest的空间不够
  {
    char dest[6];
    int64_t dest_len = 6;
    int64_t src = 9;
    EXPECT_EQ(OB_INVALID_ARGUMENT, crypt.pad(dest, dest_len, src));
  }

  //(3)dest的空间大于slen，但是小于OB_MAX_TOKEN_BUFFER_LENGTH(80)
  {
    char dest[10];
    int64_t dest_len = 10;
    int64_t src = 6;
    EXPECT_EQ(OB_INVALID_ARGUMENT, crypt.pad(dest, dest_len, src));
  }

  //(4)正确执行
  {
    char dest[90];
    int64_t dest_len = 90;
    int64_t src = 7;
    EXPECT_EQ(0, crypt.pad(dest, dest_len, src));
    for(int i = 7; i < OB_MAX_TOKEN_BUFFER_LENGTH; i++)
    {
      printf("%c", dest[i]);
    }
    printf("\n");
  }
}

//int DES_crypt::unpad(char *src, int64_t &slen)
TEST_F(TestToken, unpad)
{
  DES_crypt crypt;
  //(1)src = NULL
  {
    char *src = NULL;
    int64_t slen = 9;
    EXPECT_EQ(OB_INVALID_ARGUMENT, crypt.unpad(src, slen));
  }

  char dest[90] = "rongxuan";
  int64_t dest_len = 90;
  int64_t src = strlen(dest);
  EXPECT_EQ(0, crypt.pad(dest, dest_len, src));
  
  //(2)slen 太小
  {
    int64_t dest_len_too_short = 6;
    EXPECT_EQ(OB_INVALID_ARGUMENT, crypt.unpad(dest, dest_len_too_short));
  }

  //(3)slen 小
  {
    int64_t dest_len_too_short = 45;
    EXPECT_EQ(OB_INVALID_ARGUMENT, crypt.unpad(dest, dest_len_too_short));
  }

  //(4)slen刚好
  {
    int64_t dest_len_right = 80;
    EXPECT_EQ(0, crypt.unpad(dest, dest_len_right));
  }

  //(5)slen 大于80
  {
    int64_t dest_len_big = 90;
    EXPECT_EQ(OB_INVALID_ARGUMENT, crypt.unpad(dest, dest_len_big));
  }

  //(6)先pad然后unpad
  {
    DES_crypt crypt;
    char dest[90] = "rongxuan";
    int64_t dest_len = 90;
    int64_t src = strlen(dest);
    EXPECT_EQ(0, crypt.pad(dest, dest_len, src));
    EXPECT_EQ(src, (static_cast<int64_t>(80)));
    for(int64_t i = 0; i < src; i++)
    {
      printf("%c", dest[i]);
    }
    printf("\n");
    EXPECT_EQ(0, crypt.unpad(dest, src));
    int64_t len = strlen(dest);
    EXPECT_EQ(src, len);
    for(int64_t i = 0; i < src; i++)
    {
      printf("%c", dest[i]);
    }
    printf("\n");
  }
}

//des_encrypt(const ObString &user_name, const int64_t timestamp, const int64_t skey, char *buffer, int64_t &buffer_length)
//des_decrypt(ObString &user_name, int64_t &timestamp, const int64_t skey, char *buffer, int64_t buffer_length)
TEST_F(TestToken, des_encrypt)
{
  //(1)user_name为空
  {
    ObString user_name;
    int64_t timestamp = 0;
    int64_t skey = 12;
    char buffer[256];
    int64_t buffer_length = 256;
    DES_crypt crypt;
    EXPECT_EQ(OB_INVALID_ARGUMENT, crypt.des_encrypt(user_name, timestamp, skey, buffer, buffer_length));
    //(2)user_name不为空,buffer为空
    char name[16] = "rongxuan";
    ObString user_name_2(16, static_cast<int32_t>(strlen(name)), name);
    char *buffer_null = NULL;
    int64_t length_null = 0;
    EXPECT_EQ(OB_INVALID_ARGUMENT, crypt.des_encrypt(user_name_2, timestamp, skey, buffer_null, length_null));
  }
  //(3)buffer不为空，但buffer_length很小
  {
    char name[16] = "rongxuan";
    int32_t name_length = static_cast<int32_t>(strlen(name));
    ObString user_name(16, name_length, name);
    int64_t timestamp = 2011;
    int64_t skey = 2345;
    char buffer[25];
    int64_t buffer_length = 25;
    DES_crypt crypt;
    EXPECT_EQ(OB_INVALID_ARGUMENT, crypt.des_encrypt(user_name, timestamp, skey, buffer, buffer_length));
  }
  //(4)满足要求
    char name[16] = "rongxuan";
    int32_t name_length = static_cast<int32_t>(strlen(name));
    ObString user_name(16, name_length, name);
    int64_t timestamp = 2011;
    int64_t skey = 2345;
    char buffer[256];
    int64_t buffer_length = 256;
    DES_crypt crypt;
    EXPECT_EQ(0, crypt.des_encrypt(user_name, timestamp, skey, buffer, buffer_length));
    
    TBSYS_LOG(INFO, "**********start to decrypt************");
     //////开始解密
    //des_decrypt(ObString &user_name, int64_t &timestamp, const int64_t skey, char *buffer, int64_t buffer_length)
    
    //(5)密钥不对
    {
      int64_t skey_error = 1234;
    ObString name_error;
    int64_t timestamp_error;
    EXPECT_EQ(OB_DECRYPT_FAILED, crypt.des_decrypt(name_error, timestamp_error, skey_error, buffer, buffer_length));
    }

    //(6)buffer_length不对
    {
      ObString name;
      int64_t timestamp;
      int64_t buffer_length = 90;
      EXPECT_EQ(OB_INVALID_ARGUMENT, crypt.des_decrypt(name, timestamp, skey, buffer, buffer_length));
    }

    //(7)buffer为空
    {
      ObString name;
      int64_t timestamp;
      int64_t buffer_length = 80;
      char *buffer = NULL;
      EXPECT_EQ(OB_INVALID_ARGUMENT, crypt.des_decrypt(name, timestamp, skey, buffer, buffer_length));
    }

    //(8)buffer内容不能解密
    {
      ObString name;
      int64_t timestamp;
      char buffer[90] = "232543543";
      int64_t buffer_length = 80;
      EXPECT_EQ(OB_DECRYPT_FAILED, crypt.des_decrypt(name, timestamp, skey, buffer, buffer_length));
    }

    //(9)正确解密
    {
      char name[16] = "rongxuan";
      int32_t name_length = static_cast<int32_t>(strlen(name));
      ObString user_name(16, name_length, name);
      int64_t timestamp = 2011;
      int64_t skey = 2345;
      char buffer_to[256];
      memset(buffer_to, '$', 256);
      int64_t buffer_length = 256;
      DES_crypt crypt;
      EXPECT_EQ(0, crypt.des_encrypt(user_name, timestamp, skey, buffer_to, buffer_length));
      ObString name_str;
      int64_t timestamp_to;
      EXPECT_EQ(0, crypt.des_decrypt(name_str, timestamp_to, skey, buffer_to, 80));
      EXPECT_EQ(timestamp, timestamp_to);
      EXPECT_EQ(true, (user_name == name_str));
    }
}

TEST_F(TestToken, set_username)
{
  ObToken token;


    char buffer[6] = {"nihao"};
    ObString str(6, static_cast<int32_t>(strlen(buffer) + 1), buffer);
    token.set_username(str);

  ObString out;
  token.get_username(out);
  EXPECT_EQ(0, strcmp("nihao", out.ptr()));
  EXPECT_EQ(true, (str == out));
}

TEST_F(TestToken, set_timestamp)
{
  ObToken token;
  token.set_timestamp(1234);
  int64_t out = 0;
  token.get_timestamp(out);
  EXPECT_EQ(1234, out);
}

TEST_F(TestToken, encrypt)
{
  int length = 0;
//  for(int i = 0; i < 1000; i++)
  {
  //  srandom(i);
    length = static_cast<int32_t>(random()%32);
    char buffer[32];
    for(int j = 0; j < length; j++)
    {
      srandom(j);
     // buffer[j] = random()%25 + 97;
      buffer[j] = 'a';
    }
    TBSYS_LOG(INFO, "buffer = %s", buffer);
    ObToken token;
    ObString str(32, static_cast<int32_t>(strlen(buffer)), buffer);
    token.set_username(str);
    token.set_timestamp(1234);
    int64_t skey_version = 1;
    int64_t skey = 1234;
    //setp 1 加密
    EXPECT_EQ(0, token.encrypt(skey_version, skey));  
    //step 2 序列化
    char *buf = new char[1024];
    int64_t pos = 0;
    EXPECT_EQ(0, token.serialize(buf, 1024, pos));

    ////////////////////////////
    //setp 3 反序列化
    ObToken token_to;
    pos = 0;
    EXPECT_EQ(0, token_to.deserialize(buf, 1024, pos));

    ///序列化
    char buf_to[1024];
    pos = 0;
    EXPECT_EQ(0, token_to.serialize(buf_to, 1024, pos));

    ///反序列化
    ObToken token_2;
    pos = 0;
    EXPECT_EQ(0, token_2.deserialize(buf_to, 1024, pos));

    //解密

    //setp 4 解码  
    EXPECT_EQ(0, token_2.decrypt(skey));

    //setp 5 get
    ObString out_str;
    token_2.get_username(out_str);
    int64_t out_time = 0;
    token_2.get_timestamp(out_time);
    //EXPECT_EQ(0, strncmp("nihao", out_str.ptr(), out_str.length()));
    //EXPECT_EQ(0, strcmp(out_str.ptr(), buffer));
    TBSYS_LOG(INFO, "out_str.length=%d", out_str.length());
    TBSYS_LOG(INFO, "out_str.ptr =%.*s", out_str.length(),out_str.ptr());
    TBSYS_LOG(INFO, "out_str.ptr =%s", out_str.ptr());
    //EXPECT_EQ(strlen(buffer), out_str.length());
    EXPECT_EQ(1234, out_time);

    //解密以后直接进行序列化
    char buffer_serialize[1024];
    pos = 0;
    token_2.serialize(buffer_serialize, 1024, pos);
    ObToken token_serialize;
    pos = 0;
    token_serialize.deserialize(buffer_serialize, 1024, pos);
    ObString out_serialize;
    token_serialize.get_username(out_serialize);
    int64_t time_serialize = 0;
    token_serialize.get_timestamp(time_serialize);
    ////不支持解密以后，直接序列化反序列化，然后取值的操作
    //EXPECT_EQ(true, (out_str == out_serialize));
    //EXPECT_EQ(out_time, time_serialize);
    //TBSYS_LOG(INFO, "length = %d", out_serialize.length());
    //TBSYS_LOG(INFO, "ptr=%.*s", out_serialize.length(), out_serialize.ptr());
    delete[] buf; 
  }
}

