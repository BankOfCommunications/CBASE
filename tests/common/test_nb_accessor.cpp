
#include "common/nb_accessor/ob_nb_accessor.h"
#include "common/roottable/ob_scan_helper.h"
#include "common/ob_client_proxy.h"
#include "common/ob_range.h"
#include "common/utility.h"
#include "gtest/gtest.h"
#include "test_rowkey_helper.h"

using namespace oceanbase;
using namespace common;
using namespace nb_accessor;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))


static CharArena allocator_;

const char* value_str = "shanghai";
void gen_scanner(ObScanner& scanner);

class MockClientProxy : public ObScanHelper
{
public:
  MockClientProxy();
  virtual int scan(const ObScanParam& scan_param,ObScanner& scanner) const;
  virtual int mutate(ObMutator& mutator);
  virtual int get(const ObGetParam& get_param, ObScanner& scanner);

  //add dragon [repair_test] 2016-12-30 b
  virtual int scan(const ObScanParam& scan_param, ObScanner &out, ServerSession* ssession = NULL) const;
  virtual int scan_for_next_packet(const common::ServerSession& ssession, ObScanner& scanner) const;
  virtual void set_scan_timeout(const int64_t timeout);
  //add e

public:
  ObMutator* mutator_;
  ObGetParam* get_param_;
  ObScanParam* scan_param_;
};

MockClientProxy::MockClientProxy()
  :mutator_(NULL),
  get_param_(NULL),
  scan_param_(NULL)
{
}

int MockClientProxy::scan(const ObScanParam& scan_param, ObScanner &out, ServerSession* ssession) const
{
  UNUSED(scan_param);
  UNUSED(out);
  UNUSED(ssession);
  return OB_SUCCESS;
}

int MockClientProxy::scan_for_next_packet(const common::ServerSession& ssession, ObScanner& scanner) const
{
  UNUSED(ssession);
  UNUSED(scanner);
  return OB_SUCCESS;
}

void MockClientProxy::set_scan_timeout(const int64_t timeout)
{
  UNUSED(timeout);
}

int MockClientProxy::scan(const ObScanParam& scan_param,ObScanner& scanner) const
{
  printf("%s\n", to_cstring(scan_param.get_range()->start_key_) );
  gen_scanner(scanner);
  const_cast<MockClientProxy*>(this)->scan_param_ = const_cast<ObScanParam*>(&scan_param);
  return OB_SUCCESS;
}

int MockClientProxy::mutate(ObMutator& mutator) 
{
  this->mutator_ = const_cast<ObMutator*>(&mutator);
  return OB_SUCCESS;
}

int MockClientProxy::get(const ObGetParam& get_param,ObScanner& scanner)
{
  gen_scanner(scanner);
  this->get_param_ = const_cast<ObGetParam*>(&get_param);
  return OB_SUCCESS;
}

TEST(TestObNbAccessor, SC)
{
  SC sc("name");
  sc("hello")("kkk");
  ASSERT_EQ(OB_SUCCESS, sc.get_exec_status());
  ASSERT_EQ(3, sc.count());

}


#define STR(const_str) ObString(0, static_cast<int32_t>(strlen(const_str)), const_cast<char*>(const_str))

TEST(TestObNbAccessor, insert)
{
  MockClientProxy mock_client;
  ObNbAccessor accessor;
  accessor.init(&mock_client);

  const char* str= "123456";
  ObRowkey rowkey = make_rowkey(str, &allocator_);
  accessor.insert("test_table", rowkey, KV("name", STR("jianming"))("age", 1L));

  ObMutatorCellInfo* cell = NULL;
  ASSERT_TRUE(mock_client.mutator_ != NULL);

  mock_client.mutator_->next_cell();
  mock_client.mutator_->get_cell(&cell);

  ASSERT_TRUE(cell->cell_info.table_name_ == STR("test_table"));
  ASSERT_TRUE(cell->cell_info.row_key_ == rowkey);
  printf("%.*s, %d\n", cell->cell_info.column_name_.length(), cell->cell_info.column_name_.ptr(), cell->cell_info.column_name_.length());
  ASSERT_TRUE(cell->cell_info.column_name_ == STR("name"));

  ObString value;
  cell->cell_info.value_.get_varchar(value);
  ASSERT_TRUE(value == STR("jianming"));

  mock_client.mutator_->next_cell();
  mock_client.mutator_->get_cell(&cell);

  ASSERT_TRUE(cell->cell_info.table_name_ == STR("test_table"));
  ASSERT_TRUE(cell->cell_info.row_key_ == rowkey);
  ASSERT_TRUE(cell->cell_info.column_name_ == STR("age"));

  int64_t age = 0;
  cell->cell_info.value_.get_int(age);
  ASSERT_EQ(age, 1);

}

//TEST(TestObNbAccessor, get)
//{
//  MockClientProxy mock_client;
//  ObNbAccessor accessor;
//  accessor.init(&mock_client);
//
//
//  ObRowkey rowkey = make_rowkey("12345", &allocator_);
//
//  QueryRes* res = NULL;
//  accessor.get(res, "test_table", rowkey, SC("name")("value"));
//  
//  ObGetParam* get_param = mock_client.get_param_;
//
//  ObCellInfo* cell = (*get_param)[0];
//  ASSERT_TRUE(cell->column_name_ == STR("name"));
//  ASSERT_TRUE(cell->table_name_ == STR("test_table"));
//
//  cell = (*get_param)[1];
//  ASSERT_TRUE(cell->column_name_ == STR("value"));
//  ASSERT_TRUE(cell->table_name_ == STR("test_table"));
//}

TEST(TestObNbAccessor, scan)
{
  MockClientProxy mock_client;
  ObNbAccessor accessor;
  accessor.init(&mock_client);

  char* start_key = (char*)"jianming";

  QueryRes* res = NULL;
  ObNewRange range;
  range.start_key_ =  make_rowkey(start_key, &allocator_);

  OK(accessor.scan(res, "test_table", range, SC("name")("value")));
  ASSERT_TRUE(NULL != res);

  const ObNewRange* range2 = mock_client.scan_param_->get_range();
  ASSERT_TRUE(range2->start_key_ == range.start_key_);

  accessor.release_query_res(res);
  
}

char t[100];
char c[100];
char rowkey[100];

void gen_scanner(ObScanner& scanner)
{
  ObCellInfo cell_info;
  strcpy(t, "test_table");
  strcpy(c, "name");
  strcpy(rowkey, "10000");

  cell_info.table_name_.assign_ptr(t, static_cast<int32_t>(strlen(t)));
  //cell_info.table_id_ = 3;
  cell_info.column_name_.assign_ptr(c, static_cast<int32_t>(strlen(c)));
  //cell_info.column_id_ = 2;

  cell_info.row_key_ = make_rowkey(rowkey, &allocator_);

  cell_info.value_.set_int(3);

  ASSERT_EQ(OB_SUCCESS, scanner.add_cell(cell_info));

  strcpy(c, "value");
  cell_info.column_name_.assign_ptr(c, static_cast<int32_t>(strlen(c)));
  //cell_info.column_id_ = 3;

  cell_info.value_.set_varchar(ObString(0, static_cast<int32_t>(strlen(value_str)), (char*)value_str));

  ASSERT_EQ(OB_SUCCESS, scanner.add_cell(cell_info));

  cell_info.value_.set_int(3);
  strcpy(c, "name");
  cell_info.column_name_.assign_ptr(c, static_cast<int32_t>(strlen(c)));
  //cell_info.column_id_ = 2;

  strcpy(rowkey, "10001");
  cell_info.row_key_ = make_rowkey(rowkey, &allocator_);

  ASSERT_EQ(OB_SUCCESS, scanner.add_cell(cell_info));

  cell_info.value_.set_varchar(ObString(0, static_cast<int32_t>(strlen(value_str)), (char*)value_str));
  strcpy(c, "value");
  cell_info.column_name_.assign_ptr(c, static_cast<int32_t>(strlen(c)));
  //cell_info.column_id_ = 3;

  ASSERT_EQ(OB_SUCCESS, scanner.add_cell(cell_info));

  scanner.set_is_req_fullfilled(true, 0);
}

TEST(TestObNbAccessor, QueryRes)
{
  ObNbAccessor accessor;
  QueryRes res;

  gen_scanner(*(res.get_scanner()));

  ASSERT_EQ(OB_SUCCESS, res.init(SC("name")("value")));

  int err = OB_SUCCESS;

  while(true)
  {
    ObCellInfo* cell = NULL;
    int64_t tmp = -1;
    ObString str;
    TableRow* iter = NULL;

    err = res.next_row();
    if(OB_SUCCESS == err)
    {
      break;
    }
    res.get_row(&iter);

    cell = iter->get_cell_info("name");
    ASSERT_TRUE(NULL != cell);
    tmp = -1;
    cell->value_.get_int(tmp);
    ASSERT_EQ(3, tmp);
    
    cell = iter->get_cell_info("value");
    ASSERT_TRUE(NULL != cell);
    cell->value_.get_varchar(str);
    ASSERT_TRUE(ObString(0, static_cast<int32_t>(strlen(value_str)), (char*)value_str) == str);

    cell = iter->get_cell_info((int64_t)0);
    ASSERT_TRUE(NULL != cell);
    tmp = 0;
    cell->value_.get_int(tmp);
    ASSERT_EQ(3, tmp);

    cell = iter->get_cell_info((int64_t)1);
    ASSERT_TRUE(NULL != cell);
    cell->value_.get_varchar(str);
    ASSERT_TRUE(ObString(0, static_cast<int32_t>(strlen(value_str)), (char*)value_str) == str);

  }

}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv); 
  return RUN_ALL_TESTS();
}

