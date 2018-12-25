#include "ob_scanner.h"
#include "gtest/gtest.h"
#include "common/ob_malloc.h"
#include "common/ob_action_flag.h"
#include "common/ob_buffer.h"
#include <string>

using namespace oceanbase;
using namespace common;
using namespace std;

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

  ObRange range;
  range.border_flag_.set_inclusive_start();
  range.start_key_.assign("11111", 5);
  range.end_key_.assign("22222", 5);
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
  ASSERT_EQ(OB_SUCCESS, os.set_range(range));

  /*** added by wushi ***/

  ObCellInfo oci;

  oci.table_name_.assign("table1", 6);
  oci.row_key_.assign("row1", 4);
  oci.column_name_.assign("column1", 7);
  oci.value_.set_int(0xff);

  ost.set_mem_size_limit(20);
  ASSERT_EQ(OB_SIZE_OVERFLOW, ost.add_cell(oci));
  os.set_mem_size_limit(1024 * 1024 * 2);
  ASSERT_EQ(OB_SUCCESS, os.add_cell(oci));

  oci.column_name_.assign("column2", 7);
  oci.value_.set_int(0xfe);
  ASSERT_EQ(OB_SUCCESS, os.add_cell(oci));

  ObScannerIterator iter = os.begin();
  ObCellInfo tci;
  ASSERT_EQ(OB_SUCCESS, iter.get_cell(tci));

  ASSERT_EQ(string("table1"), string(tci.table_name_.ptr(), tci.table_name_.length()));
  ASSERT_EQ(string("row1"), string(tci.row_key_.ptr(), tci.row_key_.length()));
  ASSERT_EQ(string("column1"), string(tci.column_name_.ptr(), tci.column_name_.length()));
  ASSERT_EQ(OB_SUCCESS, tci.value_.get_int(tmp));
  ASSERT_EQ(0xff, tmp);

  iter++;
  ASSERT_EQ(OB_SUCCESS, iter.get_cell(tci));

  ASSERT_EQ(string("table1"), string(tci.table_name_.ptr(), tci.table_name_.length()));
  ASSERT_EQ(string("row1"), string(tci.row_key_.ptr(), tci.row_key_.length()));
  ASSERT_EQ(string("column2"), string(tci.column_name_.ptr(), tci.column_name_.length()));
  ASSERT_EQ(OB_SUCCESS, tci.value_.get_int(tmp));
  ASSERT_EQ(0xfe, tmp);

  iter++;

  ASSERT_EQ(true, os.end() == iter);

  //ObCellInfo oci;
  char row_key_buffer[1024];

  oci.table_name_.assign("table1", 6);
  sprintf(row_key_buffer, "row1");
  oci.row_key_.assign(row_key_buffer, 4);
  oci.column_name_.assign("column1", 7);
  oci.value_.set_int(0xff);

  ASSERT_EQ(OB_SUCCESS, os.add_cell(oci));

  oci.table_name_.assign("table1", 6);
  sprintf(row_key_buffer, "row1");
  oci.row_key_.assign(row_key_buffer, 4);
  oci.column_name_.assign("column2", 7);
  oci.value_.set_int(0xee);

  os.set_mem_size_limit(20);
  ASSERT_EQ(OB_SIZE_OVERFLOW, os.add_cell(oci));
  os.set_mem_size_limit(1024 * 1024 * 2);
  ASSERT_EQ(OB_SUCCESS, os.add_cell(oci));

  oci.table_name_.assign("table1", 6);
  sprintf(row_key_buffer, "row2");
  oci.row_key_.assign(row_key_buffer, 4);
  oci.column_name_.assign("column1", 7);
  oci.value_.set_int(0xdd);

  ASSERT_EQ(OB_SUCCESS, os.add_cell(oci));

  oci.table_name_.assign("table2", 6);
  sprintf(row_key_buffer, "row2");
  oci.row_key_.assign(row_key_buffer, 4);
  oci.column_name_.assign("column1", 7);
  oci.value_.set_int(0xcc);

  ASSERT_EQ(OB_SUCCESS, os.add_cell(oci));

  oci.table_name_.assign("table3", 6);
  sprintf(row_key_buffer, "row2");
  oci.row_key_.assign(row_key_buffer, 4);
  oci.column_name_.assign("column1", 7);
  oci.value_.set_int(0xbb);

  ASSERT_EQ(OB_SUCCESS, os.add_cell(oci));

  fprintf(stdout, "size=%ld\n", os.get_serialize_size());
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
    fprintf(stdout, "table_name=[%.*s] row_key=[%.*s] column_name=[%.*s]\n",
        ci.table_name_.length(), ci.table_name_.ptr(),
        ci.row_key_.length(), ci.row_key_.ptr(),
        ci.column_name_.length(), ci.column_name_.ptr());
  }
  fprintf(stdout, "==============================\n");
  while (OB_SUCCESS == os.next_cell())
  {
    ObCellInfo *ci = NULL;
    bool is_row_changed;
    ASSERT_EQ(OB_SUCCESS, os.get_cell(&ci, &is_row_changed));
    fprintf(stdout, "table_name=[%.*s] row_key=[%.*s] column_name=[%.*s] "
            "table_id=[%lu] column_id=[%lu] row_changed=[%d]\n",
            ci->table_name_.length(), ci->table_name_.ptr(),
            ci->row_key_.length(), ci->row_key_.ptr(),
            ci->column_name_.length(),ci->column_name_.ptr(),
            ci->table_id_, ci->column_id_, is_row_changed);
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
    fprintf(stdout, "table_name=[%.*s] row_key=[%.*s] column_name=[%.*s]\n",
        ci.table_name_.length(), ci.table_name_.ptr(),
        ci.row_key_.length(), ci.row_key_.ptr(),
        ci.column_name_.length(), ci.column_name_.ptr());
    ASSERT_EQ(OB_SUCCESS, iter.get_cell(ci));
    fprintf(stdout, "table_name=[%.*s] row_key=[%.*s] column_name=[%.*s]\n",
        ci.table_name_.length(), ci.table_name_.ptr(),
        ci.row_key_.length(), ci.row_key_.ptr(),
        ci.column_name_.length(), ci.column_name_.ptr());
  }
  fprintf(stdout, "==============================\n");
  while (OB_SUCCESS == os.next_cell())
  {
    ObCellInfo *ci = NULL;
    bool is_row_changed;
    ASSERT_EQ(OB_SUCCESS, os.get_cell(&ci, &is_row_changed));
    fprintf(stdout, "table_name=[%.*s] row_key=[%.*s] column_name=[%.*s] "
            "table_id=[%lu] column_id=[%lu] row_changed=[%d]\n",
            ci->table_name_.length(), ci->table_name_.ptr(),
            ci->row_key_.length(), ci->row_key_.ptr(),
            ci->column_name_.length(),ci->column_name_.ptr(),
            ci->table_id_, ci->column_id_, is_row_changed);
    ASSERT_EQ(OB_SUCCESS, os.get_cell(&ci, &is_row_changed));
    fprintf(stdout, "table_name=[%.*s] row_key=[%.*s] column_name=[%.*s] "
            "table_id=[%lu] column_id=[%lu] row_changed=[%d]\n",
            ci->table_name_.length(), ci->table_name_.ptr(),
            ci->row_key_.length(), ci->row_key_.ptr(),
            ci->column_name_.length(),ci->column_name_.ptr(),
            ci->table_id_, ci->column_id_, is_row_changed);
  }
  os.reset_iter();
  fprintf(stdout, "==============================\n");

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, os.deserialize(buffer, os.get_serialize_size(), pos));
  fprintf(stdout, "serialize_size=%ld pos=%ld", os.get_serialize_size(), pos);
  ASSERT_EQ(pos, os.get_serialize_size());

  for (iter = os.begin(); iter != os.end(); iter++)
  {
    ObCellInfo ci;
    ASSERT_EQ(OB_SUCCESS, iter.get_cell(ci));
    fprintf(stdout, "table_name=[%.*s] row_key=[%.*s] column_name=[%.*s]\n",
        ci.table_name_.length(), ci.table_name_.ptr(),
        ci.row_key_.length(), ci.row_key_.ptr(),
        ci.column_name_.length(), ci.column_name_.ptr());
    ASSERT_EQ(OB_SUCCESS, iter.get_cell(ci));
    fprintf(stdout, "table_name=[%.*s] row_key=[%.*s] column_name=[%.*s]\n",
        ci.table_name_.length(), ci.table_name_.ptr(),
        ci.row_key_.length(), ci.row_key_.ptr(),
        ci.column_name_.length(), ci.column_name_.ptr());
  }
  fprintf(stdout, "==============================\n");
  while (OB_SUCCESS == os.next_cell())
  {
    ObCellInfo *ci = NULL;
    bool is_row_changed;
    ASSERT_EQ(OB_SUCCESS, os.get_cell(&ci, &is_row_changed));
    fprintf(stdout, "table_name=[%.*s] row_key=[%.*s] column_name=[%.*s] "
            "table_id=[%lu] column_id=[%lu] row_changed=[%d]\n",
            ci->table_name_.length(), ci->table_name_.ptr(),
            ci->row_key_.length(), ci->row_key_.ptr(),
            ci->column_name_.length(),ci->column_name_.ptr(),
            ci->table_id_, ci->column_id_, is_row_changed);
  }
  os.reset_iter();

  /// set fullfilled
//  ASSERT_EQ(os.get_is_req_fullfilled(tmp_bool, tmp),0);
//  ASSERT_EQ(tmp_bool,is_fullfilled);
//  ASSERT_EQ(tmp,fullfilled_item);
  {
    ObRange range;
    range.border_flag_.set_inclusive_start();
    range.start_key_.assign("11111", 5);
    range.end_key_.assign("22222", 5);
    ObRange range2;
    range2.border_flag_.set_inclusive_start();
    range2.start_key_.assign("11111", 5);
    range2.end_key_.assign("22222", 5);

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

    ObRange range3;
    ASSERT_EQ(OB_SUCCESS, os2.get_range(range3));
    ASSERT_EQ(range2.border_flag_.get_data(), range3.border_flag_.get_data());
    ASSERT_EQ(string(range3.start_key_.ptr(), range3.start_key_.length()),
              string(range2.start_key_.ptr(), range2.start_key_.length()));
    ASSERT_EQ(string(range3.end_key_.ptr(), range3.end_key_.length()),
              string(range2.end_key_.ptr(), range2.end_key_.length()));

    const int times = 1000000;
    ObScanner os3;
    ObCellInfo cinull;
    cinull.table_name_.assign("hello", 5);
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
    char* buff = (char*)ob_malloc(buf_len);
    int64_t ppos = 0;
    ASSERT_EQ(OB_BUF_NOT_ENOUGH, os3.serialize(buff, 10, ppos));
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
      ASSERT_EQ(string(oci.row_key_.ptr(), oci.row_key_.length()), string(oo->row_key_.ptr(), oo->row_key_.length()));
      ASSERT_EQ(string(oci.column_name_.ptr(), oci.column_name_.length()), string(oo->column_name_.ptr(), oo->column_name_.length()));
      ASSERT_EQ(OB_SUCCESS, oo->value_.get_int(tmp));
      ASSERT_EQ(otmp, tmp);
    }

    ASSERT_EQ(t, i);

    ob_free(buff);
  }

  ObCellInfo st;

  st.table_name_.assign("table1", 6);
  st.row_key_.assign("row1", 4);
  st.column_name_.assign("column1", 7);
  st.value_.set_int(0xff);

  ObScanner ss;
  ss.add_cell(st);

  st.table_name_.assign("table2", 6);
  st.row_key_.assign("row1", 4);
  st.column_name_.assign("column1", 7);
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

TEST(TestObScanner, row_num)
{
  ObScanner scanner;
  const int table_num = 3;
  const int row_num = 20;
  const int column_num = 5;
  buffer table_name[table_num];
  buffer row_key[row_num];
  buffer column_name[column_num];
  for (int i = 0; i < table_num; i++)
  {
    table_name[i].assigne("T").appende(i);
  }
  for (int i = 0; i < row_num; i++)
  {
    row_key[i].assigne("R").appende(i);
  }
  for (int i = 0; i < column_num; i++)
  {
    column_name[i].assigne("C").appende(i);
  }

  for (int i = 0; i < table_num; i++)
  {
    for (int j = 0; j < row_num; j++)
    {
      for (int k = 0; k < column_num; k++)
      {
        ObCellInfo cell;
        cell.table_name_ = table_name[i].get_obstring();
        cell.row_key_ = row_key[j].get_obstring();
        cell.column_name_ = column_name[k].get_obstring();
        cell.value_.set_int(1);
        scanner.add_cell(cell);
      }
    }
  }

  ASSERT_EQ(scanner.get_row_num(), table_num * row_num);
  ASSERT_EQ(scanner.get_cell_num(), table_num * row_num * column_num);
  scanner.set_whole_result_row_num(1000);

  buffer buf(1 << 21);
  scanner.serialize(buf.ptre(), buf.capacity(), buf.length());
  printf("%ld\n", buf.length());
  ObScanner dscanner;
  int64_t pos = 0;
  dscanner.deserialize(buf.ptre(), buf.length(), pos);

  EXPECT_EQ(dscanner.get_row_num(), table_num * row_num);
  EXPECT_EQ(dscanner.get_cell_num(), table_num * row_num * column_num);
  EXPECT_EQ(dscanner.get_whole_result_row_num(), 1000);

  scanner.clear();
  scanner.set_mem_size_limit(256);
  int j = 0, k = 0;

  int err = OB_SUCCESS;
  for (; j < row_num; j++)
  {
    k = 0;
    for (; k < column_num; k++)
    {
      ObCellInfo cell;
      cell.table_name_ = table_name[0].get_obstring();
      cell.row_key_ = row_key[j].get_obstring();
      cell.column_name_ = column_name[k].get_obstring();
      cell.value_.set_int(1);
      if (OB_SUCCESS != (err = scanner.add_cell(cell)))
      {
        break;
      }
    }
    if (OB_SUCCESS != err)
    {
      break;
    }
  }
  if (OB_SUCCESS != err)
  {
    scanner.rollback();
  }

  EXPECT_EQ(scanner.get_row_num(), j);
  EXPECT_EQ(scanner.get_cell_num(), j * column_num);

  buf.length() = 0;
  scanner.serialize(buf.ptre(), buf.capacity(), buf.length());
  printf("%ld\n", buf.length());
  dscanner.reset();
  pos = 0;
  dscanner.deserialize(buf.ptre(), buf.length(), pos);

  EXPECT_EQ(dscanner.get_row_num(), j);
  EXPECT_EQ(dscanner.get_cell_num(), j * column_num);
}

TEST(TestObScanner, get_row)
{
  ObScanner scanner;
  const int table_num = 10;
  const int row_num = 20;
  const int column_num = 10;
  buffer table_name[table_num];
  buffer row_key[row_num];
  buffer column_name[column_num];
  for (int i = 0; i < table_num; i++)
  {
    table_name[i].assigne("T").appende(i);
  }
  for (int i = 0; i < row_num; i++)
  {
    row_key[i].assigne("R").appende(i);
  }
  for (int i = 0; i < column_num; i++)
  {
    column_name[i].assigne("C").appende(i);
  }

  for (int i = 0; i < table_num; i++)
  {
    for (int j = 0; j < row_num; j++)
    {
      for (int k = 0; k < column_num; k++)
      {
        ObCellInfo cell;
        cell.table_name_ = table_name[i].get_obstring();
        cell.row_key_ = row_key[j].get_obstring();
        cell.column_name_ = column_name[k].get_obstring();
        cell.value_.set_int(1);
        scanner.add_cell(cell);
      }
    }
  }

  ObScanner::RowIterator it = scanner.row_begin();
  ASSERT_EQ(it ==  scanner.row_begin(), true);

  for (int i = 0; i < table_num; i++)
  {
    for (int j = 0; j < row_num; j++)
    {
      ObCellInfo *row;
      int64_t num = 0;
      ASSERT_EQ(OB_SUCCESS, scanner.next_row());
      ASSERT_EQ(OB_SUCCESS, scanner.get_row(&row, &num));
      ASSERT_EQ(column_num, num);
    }
  }
  ASSERT_EQ(OB_ITER_END, scanner.next_row());

  buffer buf(1 << 21);
  scanner.serialize(buf.ptre(), buf.capacity(), buf.length());
  printf("%ld\n", buf.length());
  ObScanner dscanner;
  int64_t pos = 0;
  dscanner.deserialize(buf.ptre(), buf.length(), pos);

  for (int i = 0; i < table_num; i++)
  {
    for (int j = 0; j < row_num; j++)
    {
      ObCellInfo *row;
      int64_t num = 0;
      ASSERT_EQ(OB_SUCCESS, dscanner.next_row());
      ASSERT_EQ(OB_SUCCESS, dscanner.get_row(&row, &num));
      ASSERT_EQ(column_num, num);
    }
  }
  ASSERT_EQ(OB_ITER_END, dscanner.next_row());

  scanner.clear();
  scanner.set_mem_size_limit(256);
  int j = 0, k = 0;

  int err = OB_SUCCESS;
  for (; j < row_num; j++)
  {
    for (k = 0; k < column_num; k++)
    {
      ObCellInfo cell;
      cell.table_name_ = table_name[0].get_obstring();
      cell.row_key_ = row_key[j].get_obstring();
      cell.column_name_ = column_name[k].get_obstring();
      cell.value_.set_int(1);
      if (OB_SUCCESS != (err = scanner.add_cell(cell)))
      {
        break;
      }
    }
    if (OB_SUCCESS != err)
    {
      break;
    }
  }
  if (OB_SUCCESS != err)
  {
    scanner.rollback();
  }

  for (int i = 0; i < j; i++)
  {
    ObCellInfo *row;
    int64_t num = 0;
    ASSERT_EQ(OB_SUCCESS, scanner.next_row());
    ASSERT_EQ(OB_SUCCESS, scanner.get_row(&row, &num));
    ASSERT_EQ(column_num, num);
  }
  ASSERT_EQ(OB_ITER_END, scanner.next_row());

  buf.length() = 0;
  scanner.serialize(buf.ptre(), buf.capacity(), buf.length());
  printf("%ld\n", buf.length());
  dscanner.reset();
  pos = 0;
  dscanner.deserialize(buf.ptre(), buf.length(), pos);
  printf("row_num=%ld\n", dscanner.get_row_num());

  for (int i = 0; i < j; i++)
  {
    ObCellInfo *row;
    int64_t num = 0;
    ASSERT_EQ(OB_SUCCESS, dscanner.next_row());
    ASSERT_EQ(OB_SUCCESS, dscanner.get_row(&row, &num));
    ASSERT_EQ(column_num, num);
  }
  ASSERT_EQ(OB_ITER_END, dscanner.next_row());
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

