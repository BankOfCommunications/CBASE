#include "tokenizer.h"
#include <gtest/gtest.h>

using namespace std;
char empty = '\0';

TEST(tools, token_char_str)
{
  int ret = 0;

  //abc bcd efg NULL
  char case1[] = "abc\1bcd\1efg\1";
  char case2[] = "abc\1bcd";
  char case3[] = "abc\n";
  char case4[] = "abc\1bcd\1efg\n";

  //case1
  char *token1[4];
  int nr_token1 = 4;
  int dlen1 = strlen(case1) + 1;
  ret = Tokenizer::tokenize(case1, dlen1, '\1', nr_token1, token1);
  ASSERT_EQ(ret, 0);
  EXPECT_STREQ(token1[0], "abc");
  EXPECT_STREQ(token1[1], "bcd");
  EXPECT_STREQ(token1[2], "efg");
  EXPECT_STREQ(token1[3], &empty);
  
  //case2
  char *token2[2];
  int nr_token2 = 2;
  int dlen2 = strlen(case2) + 1;
  ret = Tokenizer::tokenize(case2, dlen2, '\1', nr_token2, token2);
  ASSERT_EQ(ret, 0);
  EXPECT_STREQ(token2[0], "abc");
  EXPECT_STREQ(token2[1], "bcd");

  //case3
  char *token3[2];
  int nr_token3 = 2;
  int dlen3 = strlen(case3) + 1;
  ret = Tokenizer::tokenize(case3, dlen3, '\1', nr_token3, token3);
  ASSERT_EQ(ret, 0);
  EXPECT_STREQ(token3[0], "abc");
  EXPECT_STREQ(token3[1], &empty);

  //case4
  char *token4[3];
  int nr_token4 = 3;
  int dlen4 = strlen(case4);
  ret = Tokenizer::tokenize(case4, dlen4, '\1', nr_token4, token4);
  ASSERT_EQ(ret, 0);
  EXPECT_STREQ(token4[0], "abc");
  EXPECT_STREQ(token4[1], "bcd");
  EXPECT_STREQ(token4[2], "efg");
}

TEST(tools, string_tokenize)
{
  vector<string> result;

  string case1 = "abc,def,ghi";
  string case2 = "abc,def,ghi,";
  string case3 = ",";
  string case4 = "";

  //case1
  Tokenizer::tokenize(case1,result, ',');
  EXPECT_EQ(result.size(), 3);
  EXPECT_STREQ(result[0].c_str(), "abc");
  EXPECT_STREQ(result[1].c_str(), "def");
  EXPECT_STREQ(result[2].c_str(), "ghi");

  //case2
  result.clear();
  Tokenizer::tokenize(case2,result, ',');
  EXPECT_EQ(result.size(), 4);
  EXPECT_STREQ(result[0].c_str(), "abc");
  EXPECT_STREQ(result[1].c_str(), "def");
  EXPECT_STREQ(result[2].c_str(), "ghi");
  EXPECT_STREQ(result[3].c_str(), "");

  //case3
  result.clear();
  Tokenizer::tokenize(case3,result, ',');
  EXPECT_EQ(result.size(), 2);
  EXPECT_STREQ(result[0].c_str(), "");
  EXPECT_STREQ(result[1].c_str(), "");

  //case3
  result.clear();
  Tokenizer::tokenize(case4,result, ',');
  EXPECT_EQ(result.size(), 1);
  EXPECT_STREQ(result[0].c_str(), "");

}

TEST(tokenize, test_token_info)
{
  TokenInfo info[10];
  string case1 = "abc\1bcd\1efg\1";
  string case2 = "abc\1bcd";
  string case3 = "abc";
  string case4 = "abc\1bcd\1efg";

  //case1
  char *token1[4];
  int nr_token1 = 4;
  Tokenizer::tokenize(case1, '\1', nr_token1, info);
  ASSERT_EQ(nr_token1, 4);
  EXPECT_STREQ(string(info[0].token, info[0].len).c_str(), "abc");
  EXPECT_STREQ(string(info[1].token, info[1].len).c_str(), "bcd");
  EXPECT_STREQ(string(info[2].token, info[2].len).c_str(), "efg");
  EXPECT_STREQ(string(info[3].token, info[3].len).c_str(), &empty);
  
  //case2
  char *token2[2];
  int nr_token2 = 2;
  Tokenizer::tokenize(case2, '\1', nr_token2, info);
  ASSERT_EQ(nr_token2, 2);
  EXPECT_STREQ(string(info[0].token, info[0].len).c_str(), "abc");
  EXPECT_STREQ(string(info[1].token, info[1].len).c_str(), "bcd");

  //case3
  char *token3[2];
  int nr_token3 = 2;
  Tokenizer::tokenize(case3, '\1', nr_token3, info);
  ASSERT_EQ(nr_token3, 1);
  EXPECT_STREQ(string(info[0].token, info[0].len).c_str(), "abc");

  //case4
  char *token4[3];
  int nr_token4 = 5;
  Tokenizer::tokenize(case4, '\1', nr_token4, info);
  ASSERT_EQ(nr_token4, 3);
  EXPECT_STREQ(string(info[0].token, info[0].len).c_str(), "abc");
  EXPECT_STREQ(string(info[1].token, info[1].len).c_str(), "bcd");
  EXPECT_STREQ(string(info[2].token, info[2].len).c_str(), "efg");
}

TEST(tokenize, test_token_slice)
{
  TokenInfo info[10];
  string case1_str = "abc\1bcd\1efgabcd";
  string case2_s = "abc\1bcd";
  string case3_s = "abc";
  string case4_s = "abc\1bcd\1efg";

  Slice case1(case1_str.data(), strlen("abc\1bcd\1efg"));
  Slice case2(case2_s);
  Slice case3(case3_s);
  Slice case4(case4_s);

  //case1
  char *token1[4];
  int nr_token1 = 4;
  Tokenizer::tokenize(case1, '\1', nr_token1, info);
  ASSERT_EQ(nr_token1, 3);
  EXPECT_STREQ(string(info[0].token, info[0].len).c_str(), "abc");
  EXPECT_STREQ(string(info[1].token, info[1].len).c_str(), "bcd");
  EXPECT_STREQ(string(info[2].token, info[2].len).c_str(), "efg");
//  EXPECT_STREQ(string(info[3].token, info[3].len).c_str(), &empty);
  
  //case2
  char *token2[2];
  int nr_token2 = 2;
  Tokenizer::tokenize(case2, '\1', nr_token2, info);
  ASSERT_EQ(nr_token2, 2);
  EXPECT_STREQ(string(info[0].token, info[0].len).c_str(), "abc");
  EXPECT_STREQ(string(info[1].token, info[1].len).c_str(), "bcd");

  //case3
  char *token3[2];
  int nr_token3 = 2;
  Tokenizer::tokenize(case3, '\1', nr_token3, info);
  ASSERT_EQ(nr_token3, 1);
  EXPECT_STREQ(string(info[0].token, info[0].len).c_str(), "abc");

  //case4
  char *token4[3];
  int nr_token4 = 5;
  Tokenizer::tokenize(case4, '\1', nr_token4, info);
  ASSERT_EQ(nr_token4, 3);

  EXPECT_STREQ(string(info[0].token, info[0].len).c_str(), "abc");
  EXPECT_STREQ(string(info[1].token, info[1].len).c_str(), "bcd");
  EXPECT_STREQ(string(info[2].token, info[2].len).c_str(), "efg");
}

TEST(tokenize, test_short_delima)
{
  string case1_s = "abc\1\tbcd\1\t\1efg\009abcd";
  string case2_s = "abc\1b\tcd";
  string case3_s = "abc";
  string case4_s = "abc\1\tbcd\1\tefg";
  string case5_s  = "abc\1\tbcd\1\tcde\1\tdef\1\t\1\t";
  string case6_s = "\1\tabc";

  Slice case1(case1_s);
  Slice case2(case2_s);
  Slice case3(case3_s);
  Slice case4(case4_s);
  Slice case5(case5_s);
  Slice case6(case6_s);

  TokenInfo token[10];
  RecordDelima delima(1, 9);

  int nr_token = 3;
  Tokenizer::tokenize(case1, delima, nr_token, token);
  ASSERT_EQ(nr_token, 3);

  EXPECT_STREQ(string(token[0].token, token[0].len).c_str(), "abc");
  EXPECT_STREQ(string(token[1].token, token[1].len).c_str(), "bcd");
  EXPECT_STREQ(string(token[2].token, token[2].len).c_str(), "\1efg\09abcd");

  //case2
  nr_token = 1;
  Tokenizer::tokenize(case2, delima, nr_token, token);
  ASSERT_EQ(nr_token, 1);

  EXPECT_STREQ(string(token[0].token, token[0].len).c_str(), "abc\1b\tcd");

  //case3
  nr_token = 3;
  Tokenizer::tokenize(case3, delima, nr_token, token);
  ASSERT_EQ(nr_token, 1);

  EXPECT_STREQ(string(token[0].token, token[0].len).c_str(), "abc");

  //case4
  nr_token = 7;
  Tokenizer::tokenize(case4, delima, nr_token, token);
  ASSERT_EQ(nr_token, 3);

  EXPECT_STREQ(string(token[0].token, token[0].len).c_str(), "abc");
  EXPECT_STREQ(string(token[1].token, token[1].len).c_str(), "bcd");
  EXPECT_STREQ(string(token[2].token, token[2].len).c_str(), "efg");
  
  //case5
  nr_token = 5;
  Tokenizer::tokenize(case5, delima, nr_token, token);
  ASSERT_EQ(nr_token, 5);

  EXPECT_STREQ(string(token[0].token, token[0].len).c_str(), "abc");
  EXPECT_STREQ(string(token[1].token, token[1].len).c_str(), "bcd");
  EXPECT_STREQ(string(token[2].token, token[2].len).c_str(), "cde");
  EXPECT_STREQ(string(token[3].token, token[3].len).c_str(), "def");
  EXPECT_STREQ(string(token[4].token, token[4].len).c_str(), "");

  //case6
  nr_token = 3;
  Tokenizer::tokenize(case6, delima, nr_token, token);
  ASSERT_EQ(nr_token, 2);

  EXPECT_STREQ(string(token[0].token, token[0].len).c_str(), "");
  EXPECT_STREQ(string(token[1].token, token[1].len).c_str(), "abc");
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc,argv);  
  return RUN_ALL_TESTS();
}


