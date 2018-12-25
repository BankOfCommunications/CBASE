#include "ob_scanner.h"
#include "gtest/gtest.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "common/ob_action_flag.h"
#include <string>

using namespace oceanbase;
using namespace common;
using namespace std;

void print_cell_info(const ObCellInfo & ci)
{
  fprintf(stdout, "table_name=[%.*s] column_name=[%.*s] row_key=[%s] "
      "table_id=[%lu] column_id=[%lu]  value=[%s] \n",
      ci.table_name_.length(), ci.table_name_.ptr(),
      ci.column_name_.length(),ci.column_name_.ptr(),
      to_cstring(ci.row_key_),
      ci.table_id_, ci.column_id_, to_cstring(ci.value_));
}

TEST(TestObScanner, add_cell)
{
  ObScanner fem;
  fem.set_is_req_fullfilled(true, 1011);
  fem.set_data_version(170);

  char buf[BUFSIZ];
  int64_t pospos = 0;
  fem.serialize(buf, BUFSIZ, pospos);

  FILE *fd;
  fd = fopen("tmptest", "w+");
  fwrite(buf, 1, pospos, fd);
  fclose(fd);

  ObScanner os;
  ObScanner ost;
  /*** added by wushi ***/
  int64_t fullfilled_item = 0;
  int64_t tmp = 0;
  bool is_fullfilled = true;
  bool tmp_bool = false;
  /// initialize
  ASSERT_EQ(os.get_is_req_fullfilled(tmp_bool, fullfilled_item),0);
  EXPECT_FALSE(tmp_bool);
  ASSERT_EQ(fullfilled_item,0);
  /// set fullfilled
  is_fullfilled = true;
  fullfilled_item = 1000;
  os.set_is_req_fullfilled(is_fullfilled,fullfilled_item);
  ASSERT_EQ(os.get_is_req_fullfilled(tmp_bool, tmp),0);
  ASSERT_EQ(tmp_bool,is_fullfilled);
  ASSERT_EQ(tmp,fullfilled_item);

  /*** added by wushi ***/

  ObCellInfo oci;

  ObObj rowkey_obj;
  rowkey_obj.set_int(4);

  oci.table_name_.assign((char*)"table1", 6);
  oci.row_key_.assign(&rowkey_obj, 1);
  oci.column_name_.assign((char*)"column1", 7);
  oci.value_.set_int(0xff);

  ost.set_mem_size_limit(20);
  ASSERT_EQ(OB_SIZE_OVERFLOW, ost.add_cell(oci));
  os.set_mem_size_limit(1024 * 1024 * 2);
  ASSERT_EQ(OB_SUCCESS, os.add_cell(oci));

  oci.column_name_.assign((char*)"column2", 7);
  oci.value_.set_int(0xfe);
  ASSERT_EQ(OB_SUCCESS, os.add_cell(oci));

  ObScannerIterator iter = os.begin();
  ObCellInfo tci;
  ASSERT_EQ(OB_SUCCESS, iter.get_cell(tci));

  ASSERT_EQ(string("table1"), string(tci.table_name_.ptr(), tci.table_name_.length()));
  ASSERT_TRUE(rowkey_obj == tci.row_key_.get_obj_ptr()[0]);
  ASSERT_EQ(string("column1"), string(tci.column_name_.ptr(), tci.column_name_.length()));
  ASSERT_EQ(OB_SUCCESS, tci.value_.get_int(tmp));
  ASSERT_EQ(0xff, tmp);

  iter++;
  ASSERT_EQ(OB_SUCCESS, iter.get_cell(tci));

  ASSERT_EQ(string("table1"), string(tci.table_name_.ptr(), tci.table_name_.length()));
  ASSERT_TRUE(rowkey_obj  == tci.row_key_.get_obj_ptr()[0]);
  ASSERT_EQ(string("column2"), string(tci.column_name_.ptr(), tci.column_name_.length()));
  ASSERT_EQ(OB_SUCCESS, tci.value_.get_int(tmp));
  ASSERT_EQ(0xfe, tmp);

  iter++;

  ASSERT_EQ(true, os.end() == iter);

  //ObCellInfo oci;
  char row_key_buffer[1024];

  oci.table_name_.assign((char*)"table1", 6);
  sprintf(row_key_buffer, "row1");
  ObString obstr(0, 4, row_key_buffer);
  rowkey_obj.set_varchar(obstr);
  oci.row_key_.assign(&rowkey_obj, 1);
  oci.column_name_.assign((char*)"column1", 7);
  oci.value_.set_int(0xff);

  ASSERT_EQ(OB_SUCCESS, os.add_cell(oci));

  oci.table_name_.assign((char*)"table1", 6);
  sprintf(row_key_buffer, "row1");
  obstr.assign_ptr(row_key_buffer, static_cast<int32_t>(strlen(row_key_buffer)));
  rowkey_obj.set_varchar(obstr);
  oci.row_key_.assign(&rowkey_obj, 1);
  oci.column_name_.assign((char*)"column2", 7);
  oci.value_.set_int(0xee);

  os.set_mem_size_limit(20);
  ASSERT_EQ(OB_SIZE_OVERFLOW, os.add_cell(oci));
  os.set_mem_size_limit(1024 * 1024 * 2);
  ASSERT_EQ(OB_SUCCESS, os.add_cell(oci));

  oci.table_name_.assign((char*)"table1", 6);
  sprintf(row_key_buffer, "row2");
  obstr.assign_ptr(row_key_buffer, static_cast<int32_t>(strlen(row_key_buffer)));
  rowkey_obj.set_varchar(obstr);
  oci.row_key_.assign(&rowkey_obj, 1);
  oci.column_name_.assign((char*)"column1", 7);
  oci.value_.set_int(0xdd);

  ASSERT_EQ(OB_SUCCESS, os.add_cell(oci));

  oci.table_name_.assign((char*)"table2", 6);
  sprintf(row_key_buffer, "row2");
  obstr.assign_ptr(row_key_buffer, static_cast<int32_t>(strlen(row_key_buffer)));
  rowkey_obj.set_varchar(obstr);
  oci.row_key_.assign(&rowkey_obj, 1);
  oci.column_name_.assign((char*)"column1", 7);
  oci.value_.set_int(0xcc);

  ASSERT_EQ(OB_SUCCESS, os.add_cell(oci));

  oci.table_name_.assign((char*)"table3", 6);
  sprintf(row_key_buffer, "row2");
  obstr.assign_ptr(row_key_buffer, static_cast<int32_t>(strlen(row_key_buffer)));
  rowkey_obj.set_varchar(obstr);
  oci.row_key_.assign(&rowkey_obj, 1);
  oci.column_name_.assign((char*)"column1", 7);
  oci.value_.set_int(0xbb);

  ASSERT_EQ(OB_SUCCESS, os.add_cell(oci));

  char buffer[2048];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, os.serialize(buffer, 1024, pos));
  ASSERT_EQ(pos, os.get_serialize_size());
  fd = fopen("./test.data.before_rollback", "w+");
  fwrite(buffer, 1, 1024, fd);
  fclose(fd);

  for (iter = os.begin(); iter != os.end(); iter++)
  {
    ObCellInfo ci;
    ASSERT_EQ(OB_SUCCESS, iter.get_cell(ci));

    print_cell_info(ci);
  }
  fprintf(stdout, "==============================\n");
  os.reset_iter();
  while (OB_SUCCESS == os.next_cell())
  {
    ObCellInfo *ci = NULL;
    bool is_row_changed;
    ASSERT_EQ(OB_SUCCESS, os.get_cell(&ci, &is_row_changed));

    print_cell_info(*ci);
  }
  os.reset_iter();
  fprintf(stdout, "==============================\n");

  ASSERT_EQ(OB_SUCCESS, os.rollback());

  fprintf(stdout, "size=%ld\n", os.get_serialize_size());
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, os.serialize(buffer, 1024, pos));
  ASSERT_EQ(pos, os.get_serialize_size());
  fd = fopen("./test.data.after_rollback", "w+");
  fwrite(buffer, 1, 1024, fd);
  fclose(fd);

  for (iter = os.begin(); iter != os.end(); iter++)
  {
    ObCellInfo ci;
    ASSERT_EQ(OB_SUCCESS, iter.get_cell(ci));
    print_cell_info(ci);
    ASSERT_EQ(OB_SUCCESS, iter.get_cell(ci));
    print_cell_info(ci);
  }
  fprintf(stdout, "==============================\n");
  os.reset_iter();
  while (OB_SUCCESS == os.next_cell())
  {
    ObCellInfo *ci = NULL;
    bool is_row_changed;
    ASSERT_EQ(OB_SUCCESS, os.get_cell(&ci, &is_row_changed));
    print_cell_info(*ci);
    ASSERT_EQ(OB_SUCCESS, os.get_cell(&ci, &is_row_changed));
    print_cell_info(*ci);
  }
  os.reset_iter();
  fprintf(stdout, "==============================\n");

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, os.deserialize(buffer, os.get_serialize_size(), pos));
  ASSERT_EQ(pos, os.get_serialize_size());

  for (iter = os.begin(); iter != os.end(); iter++)
  {
    ObCellInfo ci;
    ASSERT_EQ(OB_SUCCESS, iter.get_cell(ci));
    print_cell_info(ci);
    ASSERT_EQ(OB_SUCCESS, iter.get_cell(ci));
    print_cell_info(ci);
  }
  fprintf(stdout, "==============================\n");
  while (OB_SUCCESS == os.next_cell())
  {
    ObCellInfo *ci = NULL;
    bool is_row_changed;
    ASSERT_EQ(OB_SUCCESS, os.get_cell(&ci, &is_row_changed));
    print_cell_info(*ci);
  }
  os.reset_iter();

  /// set fullfilled
//  ASSERT_EQ(os.get_is_req_fullfilled(tmp_bool, tmp),0);
//  ASSERT_EQ(tmp_bool,is_fullfilled);
//  ASSERT_EQ(tmp,fullfilled_item);
  {
    ObObj start_key_obj, end_key_obj;
    start_key_obj.set_int(11111);
    end_key_obj.set_int(22222);
    ObNewRange range;
    range.border_flag_.set_inclusive_start();
    range.start_key_.assign(&start_key_obj, 1);
    range.end_key_.assign(&end_key_obj, 1);
    ObNewRange range2;
    range2.border_flag_.set_inclusive_start();
    range2.start_key_.assign(&start_key_obj, 1);
    range2.end_key_.assign(&end_key_obj, 1);

    int64_t pos = 0;
    int64_t data_len = 0;
    is_fullfilled = true;
    fullfilled_item = 1212;
    ObScanner os1;
    ObScanner os2;
    ASSERT_EQ(os1.set_is_req_fullfilled(is_fullfilled, fullfilled_item),0);
    os1.set_data_version(100);
    ASSERT_EQ(os1.set_range(range),0);
    range.border_flag_.set_inclusive_end();
    char buf[1024];
    ASSERT_EQ(os1.serialize(buf,sizeof(buf),pos),0);
    data_len = pos;
    pos = 0;
    ASSERT_EQ(os2.deserialize(buf, data_len, pos),0);
    memset(buf, 0x00, 1024);

    ASSERT_EQ(os2.get_is_req_fullfilled(tmp_bool, tmp),0);
    EXPECT_EQ(is_fullfilled, tmp_bool);
    ASSERT_EQ(fullfilled_item, tmp);
    ASSERT_EQ(os1.get_data_version(),100);

    ObNewRange range3;
    ASSERT_EQ(OB_SUCCESS, os2.get_range(range3));
    ASSERT_EQ(range2.border_flag_.get_data(), range3.border_flag_.get_data());
    ASSERT_EQ(range3.start_key_, range2.start_key_);
    ASSERT_EQ(range3.end_key_, range2.end_key_);

    const int times = 1000000;
    ObScanner os3;
    ObCellInfo cinull;
    cinull.table_name_.assign((char*)"hello", 5);
    cinull.value_.set_ext(ObActionFlag::OP_INSERT);
    ASSERT_EQ(OB_ERROR, os3.add_cell(cinull));
    cinull.value_.set_ext(ObActionFlag::OP_DEL_ROW);
    ASSERT_EQ(OB_SUCCESS, os3.add_cell(cinull));
    cinull.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
    ASSERT_EQ(OB_SUCCESS, os3.add_cell(cinull));
    int i = 0;
    for (; i < times; i++)
    {
      if (OB_SUCCESS != os3.add_cell(oci))
      {
        break;
      }
    }
    const int64_t buf_len = 1 << 22;
    char* buff = (char*)ob_malloc(buf_len, ObModIds::TEST);
    int64_t ppos = 0;
    ASSERT_NE(OB_SUCCESS, os3.serialize(buff, 10, ppos));
    ASSERT_EQ(OB_SUCCESS, os3.serialize(buff, buf_len, ppos));

    int64_t npos = 0;
    ObScanner os4;
    ASSERT_EQ(OB_ERROR, os4.deserialize(buff, 10, npos));
    ASSERT_EQ(OB_SUCCESS, os4.deserialize(buff, ppos, npos));
    ASSERT_EQ(npos, ppos);

    int64_t otmp;
    oci.value_.get_int(otmp);
    ObCellInfo *oo;
    int t = 0;
    ASSERT_EQ(OB_SUCCESS, os4.next_cell());
    ASSERT_EQ(OB_SUCCESS, os4.get_cell(&oo));
    ASSERT_EQ(0, oo->row_key_.length());
    ASSERT_EQ(string(cinull.table_name_.ptr(), cinull.table_name_.length()), string(oo->table_name_.ptr(), oo->table_name_.length()));
    ASSERT_EQ(OB_SUCCESS, oo->value_.get_ext(tmp));
    ASSERT_EQ(+ObActionFlag::OP_DEL_ROW, tmp);

    ASSERT_EQ(OB_SUCCESS, os4.next_cell());
    ASSERT_EQ(OB_SUCCESS, os4.get_cell(&oo));
    ASSERT_EQ(string(cinull.table_name_.ptr(), cinull.table_name_.length()), string(oo->table_name_.ptr(), oo->table_name_.length()));
    ASSERT_EQ(OB_SUCCESS, oo->value_.get_ext(tmp));
    ASSERT_EQ(+ObActionFlag::OP_ROW_DOES_NOT_EXIST, tmp);

    while (OB_SUCCESS == os4.next_cell())
    {
      ASSERT_EQ(OB_SUCCESS, os4.get_cell(&oo));

      t++;

      ASSERT_EQ(string(oci.table_name_.ptr(), oci.table_name_.length()), string(oo->table_name_.ptr(), oo->table_name_.length()));
      ASSERT_EQ(oci.row_key_, oo->row_key_);
      ASSERT_EQ(string(oci.column_name_.ptr(), oci.column_name_.length()), string(oo->column_name_.ptr(), oo->column_name_.length()));
      ASSERT_EQ(OB_SUCCESS, oo->value_.get_int(tmp));
      ASSERT_EQ(otmp, tmp);
    }

    ASSERT_EQ(t, i);

    ob_free(buff);
  }

  ObCellInfo st;
  ObObj obj;
  obj.set_int(10);

  st.table_name_.assign((char*)"table1", 6);
  st.row_key_.assign(&obj, 1);
  st.column_name_.assign((char*)"column1", 7);
  st.value_.set_int(0xff);

  ObScanner ss;
  ss.add_cell(st);

  st.table_name_.assign((char*)"table2", 6);
  st.row_key_.assign(&obj, 1);
  st.column_name_.assign((char*)"column1", 7);
  st.value_.set_int(0xff);

  ss.add_cell(st);

  ObCellInfo *sto;
  bool is_row_changed;

  ASSERT_EQ(OB_SUCCESS, ss.next_cell());
  ASSERT_EQ(OB_SUCCESS, ss.get_cell(&sto, &is_row_changed));
  ASSERT_EQ(true, is_row_changed);
  ASSERT_EQ(OB_SUCCESS, ss.next_cell());
  ASSERT_EQ(OB_SUCCESS, ss.get_cell(&sto, &is_row_changed));
  ASSERT_EQ(true, is_row_changed);
  ASSERT_EQ(OB_ITER_END, ss.next_cell());
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ObScanner *app = new(NULL) ObScanner();
  fprintf(stdout, "app=%p\n", app);

  TBSYS_LOGGER.setLogLevel("INFO");

  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
