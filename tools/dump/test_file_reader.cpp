#include "file_reader.h"
#include <gtest/gtest.h>
#include "db_utils.h"
#include <vector>

using namespace oceanbase::common;
using namespace std;

TEST(file_reader, coding)
{
  uint32_t value = 123456; 
  char buf[8];
  encode_int32(buf, value);
  uint32_t value1 = decode_int32(buf);
  ASSERT_EQ(value1, value);
}

typedef vector<string> TestRecord;
typedef vector<TestRecord> TestRecords;

string gen_str(bool single)
{
  static string str;
  str.clear();
  char *dict = "\01\02\t\nabcedfghijklmnopqrstuvwxyz";
  char *single_dict = "abcedfghijklmnopqrstuvwxyz";

  int len = rand() % 51;

  char last_char = 0;
  char curr_char = 0;

  for(int i = 0;i < len; i++) {
    while (1) {
      if (single) {
        int idx = rand() % strlen(single_dict);
        curr_char = single_dict[idx];
      } else {
        int idx = rand() % strlen(dict);
        curr_char = dict[idx];
      }

      if (curr_char == '\t' && last_char == 1) {
        continue;
      }

      if (curr_char == '\n' && last_char == 2) {
        continue;
      }

      break;
    }

    str += curr_char;
    last_char = curr_char;
  }

  return str;
}

TestRecords all_records;
void gen_file(vector<string> &v, bool single)
{
  static char buf[1024 * 1024];
  
  all_records.clear();
  int nr = rand() % 10000;
  for(int i = 0;i < nr; i++) {
    TestRecord record;
    string col1, col2, col3, col4;

    col1 = gen_str(single);
    col2 = gen_str(single);
    col3 = gen_str(single);
    col4 = gen_str(single);

    record.push_back(col1);
    record.push_back(col2);
    record.push_back(col3);
    record.push_back(col4);

    if (!single) {
      snprintf(buf, 1024 * 1024 , "%s\1\t%s\1\t%s\1\t%s\2\n", col1.c_str(), col2.c_str(), col3.c_str(), col4.c_str());
    } else {
      snprintf(buf, 1024 * 1024 , "%s\1%s\1%s\1%s\n", col1.c_str(), col2.c_str(), col3.c_str(), col4.c_str());
    }

    v.push_back(buf);
    all_records.push_back(record);
  }
}

void touch_file(bool single)
{
  FileUtils file;
  int ret = file.open("test1.txt", O_WRONLY | O_TRUNC | O_CREAT, 0644);
  ASSERT_NE(ret, 0);
  
  vector<string> v;
  gen_file(v, single);

  for (size_t i = 0;i < v.size();i++) {
    file.write(v[i].c_str(), v[i].size());
  }
  file.close();
}

void test_reader(const RecordDelima &delima, const RecordDelima &rec_delima)
{
  FileReader reader("test1.txt");
  int ret = reader.open();
  ASSERT_EQ(ret, 0);

  int i = 0;
  int64_t rec_nr = 0;
  TokenInfo tokens[10];
  int nr_token = 10;

  while (!reader.eof()) {
    RecordBlock block;

    ret = reader.get_records(block, rec_delima, delima);
    ASSERT_EQ(ret, 0);
    Slice slice;

    rec_nr += block.get_rec_num();
    while (block.next_record(slice)) {
      Tokenizer::tokenize(slice, delima, nr_token, tokens);
      ASSERT_EQ(nr_token, 4);

      ASSERT_STREQ(string(tokens[0].token, tokens[0].len).c_str(), all_records[i][0].c_str());
      ASSERT_STREQ(string(tokens[1].token, tokens[1].len).c_str(), all_records[i][1].c_str());
      ASSERT_STREQ(string(tokens[2].token, tokens[2].len).c_str(), all_records[i][2].c_str());
      ASSERT_STREQ(string(tokens[3].token, tokens[3].len).c_str(), all_records[i][3].c_str());
      i++;
    }
  }
  ASSERT_EQ(i, all_records.size());
}

TEST(file_reader, reader)
{
  int ret = 0;

  {
    touch_file(false);

    RecordDelima delima(1,9);
    RecordDelima rec_delima(2, 10);

    test_reader(delima, rec_delima);
  }

#if 0
  {
    touch_file(true);

    RecordDelima delima(1);
    RecordDelima rec_delima(10);

    test_reader(delima, rec_delima);
  }
#endif
}

int main(int argc, char *argv[])
{
  TBSYS_LOGGER.setLogLevel("INFO"); 
  srand(time(NULL));
  ::testing::InitGoogleTest(&argc,argv);  
  return RUN_ALL_TESTS();
}

