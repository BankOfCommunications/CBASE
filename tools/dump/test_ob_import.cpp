#include "ob_import.h"
#include "ob_import_param.h"
#include "common/serialization.h"
#include "common/file_utils.h"
#include <gtest/gtest.h>
#include <string>
#include <vector>

using namespace oceanbase::common;
using namespace oceanbase::api;
using namespace std;


void gen_file(vector<string> &v)
{
  char buf[4096];
  string str;
  char *date = "2011-08-09 03:10:12";

  for(int i = 0;i < 1000000; i++) {
    snprintf(buf, 4096, "%d\01%d\01%s\01%f\01%d\01%d\01%s\01%s\01%s\n", 
             i, i, date, (float)i, 123, i, date, date, date);
    str = buf;
    v.push_back(str);
  }
}

void gen_data_file(vector<string> &v)
{
  int ret = 0;
  FileUtils file;
  ret = file.open("test1.txt", O_WRONLY | O_TRUNC | O_CREAT, 0644);
  ASSERT_NE(ret, 0);
  
  gen_file(v);
  TBSYS_LOG(DEBUG, "vector size = %d", v.size());

  for (size_t i = 0;i < v.size();i++) {
    file.write(v[i].c_str(), v[i].size());
  }
  file.close();
}

class TestRowBuilder {
  public:
    TestRowBuilder(ObRowBuilder *builder) {
      builder_ = builder;
    }

    void test_create_rowkey() {
      char buf[17];
      memset(buf, 0, 17);
      int64_t pos = 0;
      int ret = serialization::encode_i64(buf, 17, pos, 123);
      ASSERT_EQ(ret, 0);
      
      ret = serialization::encode_i8(buf, 17 , pos, 23);
      ASSERT_EQ(ret, 0);

      ret = serialization::encode_i64(buf,  17 , pos, 456);
      ASSERT_EQ(ret, 0);

      char *key1 = "123";
      char *key2 = "23";
      char *key3 = "456";

      TokenInfo tokens[50];
      tokens[0].token = key3;
      tokens[0].len = strlen(key3);

      tokens[4].token = key2;
      tokens[4].len = strlen(key2);

      tokens[5].token = key1;
      tokens[5].len = strlen(key1);

      ObString rowkey;
      ret = builder_->create_rowkey(rowkey, tokens);
      ASSERT_EQ(ret, 0);
      ASSERT_STREQ(string(buf, 17).c_str(), string(rowkey.ptr(), rowkey.length()).c_str());
      char hex[128];
      int len = hex_to_str(buf, 17, hex, 128);
      fprintf(stderr, "hex1:%s\n", hex);
      hex[2 * len] = 0;

      len = hex_to_str(rowkey.ptr(), rowkey.length(), hex, 128);
      hex[2 * len] = 0;
      fprintf(stderr, "hex2:%s\n", hex);

      bool equal = !memcmp(buf, rowkey.ptr(), 17);
      ASSERT_EQ(equal, 1);
    }

    void test_setup_content() {
      TokenInfo tokens[50];
      tokens[1].token = "123456";
      tokens[1].len = strlen("123456");

      tokens[3].token = "123.456";
      tokens[3].len = strlen("123.456");

      tokens[2].token = "abcdefghijklmn";
      tokens[2].len = strlen("abcdefghijklmn");

      tokens[6].token = "2011-08-17 12:30:00";
      tokens[6].len = strlen("2011-08-17 12:30:00");

      tokens[7] = tokens[6];
      tokens[8] = tokens[6];

      {
        OceanbaseDb db("", 123);
        DbTranscation *tnx;
        db.start_tnx(tnx);
        RowMutator *mutator;
        tnx->insert_mutator("test1", ObString(), mutator);
        builder_->setup_content(mutator, tokens);
        tnx->free_row_mutator(mutator);
        db.end_tnx(tnx);
      }
    }

    void setup(ImportParam *param) {

      TableParam tparam;
      int ret = param->get_table_param("test1", tparam);
      ASSERT_EQ(ret, 0);
      ret = builder_->set_column_desc(tparam.col_descs);
      ASSERT_EQ(ret, 0);

      for(size_t i = 0;i < tparam.col_descs.size(); i++) {
        ColumnDesc desc = tparam.col_descs[i];

        if (builder_->columns_desc_[desc.offset].schema)
          EXPECT_STREQ(desc.name.c_str(), builder_->columns_desc_[desc.offset].schema->get_name());
      }

      /*
         ret = builder_->set_rowkey_desc(tparam.rowkey_descs);
         ASSERT_EQ(ret, 0);
      */

      for(size_t i = 0;i < tparam.rowkey_descs.size(); i++) {
        RowkeyDesc desc = tparam.rowkey_descs[i];
        ASSERT_EQ(builder_->rowkey_desc_[desc.pos].type, desc.type);
        ASSERT_EQ(builder_->rowkey_desc_[desc.pos].offset, desc.offset);
      }
    }

    void test_build_tnx()
    {
      vector<string> v;
      vector<string> v1;
      gen_data_file(v);
      OceanbaseDb db("", 123);
      DbTranscation *tnx;
      db.start_tnx(tnx);

      FileReader reader("test1.txt");
      int ret = reader.open();
      ASSERT_EQ(ret, 0);
      int token_nr = OB_MAX_COLUMN_NUMBER;
      TokenInfo tokens[OB_MAX_COLUMN_NUMBER];
      memset(tokens, 0, sizeof(tokens));

      std::list<RecordBlock> blocks;

      while (!reader.eof()) {
        RecordBlock block;
        ret = reader.get_records(block, '\n', '\01');
        ASSERT_EQ(ret, 0);
        blocks.push_back(block);
      }

      while (!blocks.empty()) {
        RecordBlock block;
        block = blocks.front();
        blocks.pop_front();
        Slice slice;

        while (block.next_record(slice)) {
          Tokenizer::tokenize(slice, '\00', token_nr, tokens);
          string str;
          for(int i = 0;i < token_nr; i++) {
            if (i != 0)
              str.append("\01");
            str.append(tokens[i].token, tokens[i].len);
          }
          str.append("\n");
          v1.push_back(str);
//          TBSYS_LOG(DEBUG, "%s", str.c_str());
//          TBSYS_LOG(DEBUG, "slice = %s", slice.ToString().c_str());
        }

//        ret = builder_->build_tnx(block, tnx);
//        ASSERT_EQ(ret, 0);
      }

      TBSYS_LOG(DEBUG, "v.size() = %d", v.size());
      ASSERT_EQ(v.size(), v1.size());
      for(size_t i = 0;i < v.size(); i++) {
        ASSERT_STREQ(v[i].c_str(), v1[i].c_str());
      }

      db.end_tnx(tnx);
    }

    ObRowBuilder *builder_;
};

TEST(import, ob_import)
{
  tbsys::CConfig c1;
  ObSchemaManagerV2  *mm = new (std::nothrow)ObSchemaManagerV2(tbsys::CTimeUtil::getTime());
  int ret = mm->parse_from_file("ob-schema.ini", c1);
  ASSERT_EQ(ret, true);

//  OceanbaseDb db("172.24.70.9", 2500);
//  db.init();

//  ret = db.fetch_schema(schema);
//  ASSERT_EQ(ret, OB_SUCCESS);

  ImportParam param;
  ret = param.load("import.conf");
  ASSERT_EQ(ret, 0);

  ObRowBuilder builder(mm, "test1", 9);
  TestRowBuilder test_builder(&builder);
  test_builder.setup(&param);
  test_builder.test_create_rowkey();
  test_builder.test_setup_content();
  test_builder.test_build_tnx();
  delete mm;
}

int main(int argc, char *argv[])
{
  OceanbaseDb::global_init(NULL, "DEBUG");
  ::testing::InitGoogleTest(&argc,argv);  
  return RUN_ALL_TESTS();
}

