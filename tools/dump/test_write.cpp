#include <map>
#include <vector>
#include <string>
#include <iostream>
#include <gtest/gtest.h>
#include "oceanbase_db.h"
#include "db_utils.h"
#include "db_record_set.h"

using namespace std;
using namespace oceanbase::api;
using namespace oceanbase::common;

typedef map<string, ObObj> RowData;

string make_rowkey(int64_t key)
{
  int64_t key_buf;
  key_buf = key;
  reverse((char *)&key_buf, (char *)&key_buf + 8);
  return string((char *)&key_buf, 8);
}

vector<string> get_columns()
{
  vector<string> columns;
  columns.push_back("data1");
  columns.push_back("data2");
  columns.push_back("data3");
  columns.push_back("data4");
  columns.push_back("data5");
//  columns.push_back("data6");
//  columns.push_back("data7");

  return columns;
}

int get_from_db(OceanbaseDb *db,int64_t key, DbRecordSet &rs)
{
  int ret = OB_SUCCESS;

  string buf = make_rowkey(key);

  ObString rowkey;
  rowkey.assign_ptr(const_cast<char *>(buf.c_str()), buf.length());
  string table = "test1";
  vector<string> columns = get_columns();
  ret = db->get(table, columns, rowkey, rs);

  return ret;
}

int append_row(OceanbaseDb *db, string table, ObString &rowkey, RowData &row)
{
  RowData::iterator itr = row.begin();
  DbTranscation *tnx = NULL;
  RowMutator *mutator = NULL;

  int ret = db->start_tnx(tnx);
  if (ret == OB_SUCCESS) {
    ret = tnx->insert_mutator(table.c_str(), rowkey, mutator);

    //iterator all column in map
    if (ret == OB_SUCCESS) {
      while (itr != row.end()) {
        ret = mutator->add(itr->first, itr->second);

        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "can't add cell,col=%s", itr->first);
          break;
        }
        itr++;
      }
    }
  } else {
    TBSYS_LOG(ERROR, "can't start tnx");
  }

  if (ret == OB_SUCCESS) {
    ret = tnx->commit();
  } else {
    ret = tnx->abort();
  }

  if (mutator != NULL) {
    tnx->free_row_mutator(mutator);
  }

  if (tnx != NULL) {
    db->end_tnx(tnx); 
  }

  return ret;
}

//column_info=1,2,data1,int
//column_info=1,3,data2,precise_datetime
//column_info=1,4,data3,double
//column_info=1,5,data4,varchar,1380
//column_info=1,6,data5,datetime
//column_info=1,7,data6,modify_time
//column_info=1,8,data7,create_time
TEST(OB_API,test_write)
{
  RowData row;
  int ret = OB_SUCCESS;

  ObObj obj;
  OceanbaseDb db("172.24.70.9", 2500);
  db.init();

  ObServer server;
  ret = db.get_update_server(server);
  ASSERT_EQ(ret, OB_SUCCESS);

  for(int i = 1;i <= 100 ; i++) {
    //data1 is int
    obj.set_int(rand());
    row["data1"] = obj;

    //data2 is datetime
    obj.set_precise_datetime(rand());
    row["data2"] = obj;

    //data3 is double
    obj.set_double(rand() * 12.3);
    row["data3"] = obj;

    //data4
    char *str = "shiwenhui";
    ObString varchar;
    varchar.assign_ptr(str, strlen(str));
    obj.set_varchar(varchar);
    row["data4"] = obj;

    //data5 datetime
    obj.set_datetime(rand());
    row["data5"] = obj;

    //data6 modify_time
    obj.set_modifytime(gen_usec());
//    row["data6"] = obj;

    //data7 createtime
    obj.set_createtime(rand());
//    row["data7"] = obj;

    ObString rowkey;
    string key_buf = make_rowkey(i);
    rowkey.assign_ptr(const_cast<char *>(key_buf.c_str()), key_buf.length());
    ret = append_row(&db, "test1", rowkey, row);
    ASSERT_EQ(ret, OB_SUCCESS);

    DbRecordSet rs;
    rs.init();
    ret = get_from_db(&db, i, rs);
    ASSERT_EQ(ret, OB_SUCCESS);

    RowData::iterator itr = row.begin();
    DbRecordSet::Iterator rs_itr = rs.begin();
    ObCellInfo *cell;

    while (itr != row.end()) {
      ret = rs_itr->get(itr->first.c_str(), &cell);
      ASSERT_EQ(ret, OB_SUCCESS);
      bool equal = itr->second == cell->value_;
      ASSERT_EQ(equal, true);
      itr++;
    }
  }
}

int main(int argc, char *argv[])
{
  const char *host = NULL;
  unsigned short port = 0;

  int ret = 0;
  while ((ret = getopt(argc, argv, "h:p:")) != -1) {
    switch(ret) {
     case 'h':
       host = optarg;
       break;
     case 'p':
       port = (unsigned short)atol(optarg);
       break;
    }
  }

  //  if (!host || !port) {
  //    fprintf(stderr, "%s -h host -p port\n", argv[0]);
  //    exit(0);
  //  }

  OceanbaseDb::global_init(NULL, "DEBUG");
  ::testing::InitGoogleTest(&argc,argv);  
  return RUN_ALL_TESTS();

  /*
     OceanbaseDb db(host, port);
     db.init();

     char buf[128];
     if (ret != OB_SUCCESS) {
     fprintf(stderr, "can't get_update_server ,ret=%d\n", ret);
     return 1;
     }

     server.to_string(buf, 128);
     fprintf(stderr, "update server ip : %s\n", buf);

     db.end_tnx(tnx);
     */
}
