#include <gtest/gtest.h>
#include <stdlib.h>
#include <time.h>
#include <vector>
#include <string>
#include <stdlib.h>
#include <time.h>
#include <iostream>
#include "common/ob_cell_array.h"
#include "common/ob_define.h"
#include "common/ob_cache.h"
#include "common/ob_string.h"
#include "common/ob_action_flag.h"
#include "common/ob_scan_param.h"
#include "test_rowkey_helper.h"

using namespace oceanbase::common;
using namespace testing;
using namespace std;

static CharArena allocator_;

namespace
{
  class VRowComp
  {
  public:
    /// @fn constructor
    VRowComp(ObCellArray::OrderDesc * order_desc, int32_t order_column_num,
             vector<ObCellInfo> *cell_2darray[])
    {
      desc_ = order_desc;
      desc_size_ = order_column_num;
      cell_2darray_ = cell_2darray;
    }

    virtual ~VRowComp()
    {
    }
    /// @fn compare tow cell row
    bool operator()(const int32_t idx1, const int32_t idx2)
    {
      int result = 0;
      for (int32_t i = 0; i < desc_size_ && 0 == result; i ++)
      {
        if ((*cell_2darray_[idx1])[desc_[i].cell_idx_].value_
            < (*cell_2darray_[idx2])[desc_[i].cell_idx_].value_)
        {
          result = -1;
        }
        else if ((*cell_2darray_[idx1])[desc_[i].cell_idx_].value_
                 > (*cell_2darray_[idx2])[desc_[i].cell_idx_].value_)
        {
          result = 1;
        }
        else
        {
          result = 0;
        }
        if (ObScanParam::DESC == desc_[i].order_)
        {
          result *= -1;
        }
      }
      return(result < 0);
    }
  private:
    ObCellArray::OrderDesc *desc_;
    int32_t   desc_size_;
    vector<ObCellInfo> **cell_2darray_;
  };
}

TEST(ObCellArray, basic)
{
  ObCellArray array;
  ObInnerCellInfo *cell = NULL;
  EXPECT_EQ(array.get_cell_size(),0);
  EXPECT_EQ(array.get_memory_size_used(),0);
  EXPECT_LT(array.get_cell(0,cell),0);
}

TEST(ObCellArray, append)
{
  vector<string> rowkeys;
  vector<ObCellInfo> cell_vec;
  ObCellArray cell_array;
  ObCellInfo cur_cell;
  ObString obstr;
  TestRowkeyHelper rh(&allocator_);
  char buf[128]="";
  char str_char = 'a';
  int64_t val;
  int len = 0;
  const int64_t cell_num = static_cast<int64_t>(1024*1024*1.5);
  ObInnerCellInfo *cell_out = NULL;
  for (int64_t i = 0; i < cell_num; i ++)
  {
    val = random();
    if (val < 0)
    {
      val *= -1;
    }
    str_char = static_cast<char>('a' + val%26);
    len = static_cast<int>(val%sizeof(buf) + 1);
    memset(buf,str_char,len);
    string str(buf,buf+len);
    obstr.assign(const_cast<char*>(str.c_str()),static_cast<int32_t>(str.size()));
    rh = obstr;
    rowkeys.push_back(str);
    cur_cell.row_key_ = rh;
    cur_cell.value_.set_int(val);

    cell_vec.push_back(cur_cell);
    EXPECT_EQ(cell_array.append(cur_cell, cell_out),0);
    EXPECT_TRUE(cell_out->value_ == cur_cell.value_);
    EXPECT_TRUE(cell_out->row_key_ == rh);
    EXPECT_TRUE(cell_out->row_key_.ptr() != rh.ptr());
    EXPECT_EQ(cell_array.get_cell_size(), i + 1);
  }

  ObCellArray::iterator begin = cell_array.begin();
  ObCellArray::iterator end = cell_array.end();
  ObInnerCellInfo *cell = NULL;
  for (int64_t i = 0; i < cell_num; i ++, begin ++)
  {
    EXPECT_EQ(cell_array.get_cell(i,cell),0);
    EXPECT_TRUE(cell->row_key_ == cell_vec[i].row_key_);
    EXPECT_TRUE(cell->value_ == cell_vec[i].value_);
    EXPECT_TRUE(begin->row_key_ == cell_vec[i].row_key_);
    EXPECT_TRUE(begin->value_ == cell_vec[i].value_);

    /// next cell
    EXPECT_EQ(cell_array.next_cell(),0);
    EXPECT_EQ(cell_array.get_cell(&cell),0);
    EXPECT_TRUE(cell->row_key_ == cell_vec[i].row_key_);
    EXPECT_TRUE(cell->value_ == cell_vec[i].value_);
  }
  EXPECT_EQ(cell_array.next_cell(),OB_ITER_END);

  cell_array.clear();
  EXPECT_EQ(cell_array.get_cell_size(),0);
  EXPECT_EQ(cell_array.get_memory_size_used(),0);

  for (int64_t i = 0; i < cell_num; i ++)
  {
    EXPECT_EQ(cell_array.append(cell_vec[i], cell_out),0);
    EXPECT_EQ(cell_array.get_cell_size(), i + 1);
  }

  begin = cell_array.begin();
  end = cell_array.end();
  for (int64_t i = 0; i < cell_num; i ++, begin ++)
  {
    EXPECT_EQ(cell_array.get_cell(i,cell),0);
    EXPECT_TRUE(cell->row_key_ == cell_vec[i].row_key_);
    EXPECT_TRUE(cell->value_ == cell_vec[i].value_);
    EXPECT_TRUE(begin->row_key_ == cell_vec[i].row_key_);
    EXPECT_TRUE(begin->value_ == cell_vec[i].value_);
    /// next cell
    EXPECT_EQ(cell_array.next_cell(),0);
    EXPECT_EQ(cell_array.get_cell(&cell),0);
    EXPECT_TRUE(cell->row_key_ == cell_vec[i].row_key_);
    EXPECT_TRUE(cell->value_ == cell_vec[i].value_);
    EXPECT_TRUE(begin->row_key_ == cell_vec[i].row_key_);
    EXPECT_TRUE(begin->value_ == cell_vec[i].value_);
  }
  EXPECT_EQ(cell_array.next_cell(),OB_ITER_END);
  //
  cell_array.clear();
  for (int64_t i = 0; i < cell_num; i ++)
  {
    EXPECT_EQ(cell_array.append(cell_vec[i], cell_out),0);
    EXPECT_EQ(cell_array.get_cell_size(), i + 1);
  }
  begin = cell_array.begin();
  end = cell_array.end();
  for (int64_t i = 0; i < cell_num; i ++, begin ++)
  {
    EXPECT_EQ(cell_array.get_cell(i,cell),0);
    EXPECT_TRUE(cell->row_key_ == cell_vec[i].row_key_);
    EXPECT_TRUE(cell->value_ == cell_vec[i].value_);
    EXPECT_TRUE(begin->row_key_ == cell_vec[i].row_key_);
    EXPECT_TRUE(begin->value_ == cell_vec[i].value_);
    /// next cell
    EXPECT_EQ(cell_array.next_cell(),0);
    EXPECT_EQ(cell_array.get_cell(&cell),0);
    EXPECT_TRUE(cell->row_key_ == cell_vec[i].row_key_);
    EXPECT_TRUE(cell->value_ == cell_vec[i].value_);
    EXPECT_TRUE(begin->row_key_ == cell_vec[i].row_key_);
    EXPECT_TRUE(begin->value_ == cell_vec[i].value_);
  }
  // reset all the cell
  cell_array.reset();
  for (int64_t i = 0; i < cell_num; i ++)
  {
    EXPECT_EQ(cell_array.append(cell_vec[i], cell_out),0);
    EXPECT_EQ(cell_array.get_cell_size(), i + 1);
  }
  begin = cell_array.begin();
  end = cell_array.end();
  for (int64_t i = 0; i < cell_num; i ++, begin ++)
  {
    EXPECT_EQ(cell_array.get_cell(i,cell),0);
    EXPECT_TRUE(cell->row_key_ == cell_vec[i].row_key_);
    EXPECT_TRUE(cell->value_ == cell_vec[i].value_);
    EXPECT_TRUE(begin->row_key_ == cell_vec[i].row_key_);
    EXPECT_TRUE(begin->value_ == cell_vec[i].value_);
    /// next cell
    EXPECT_EQ(cell_array.next_cell(),0);
    EXPECT_EQ(cell_array.get_cell(&cell),0);
    EXPECT_TRUE(cell->row_key_ == cell_vec[i].row_key_);
    EXPECT_TRUE(cell->value_ == cell_vec[i].value_);
    EXPECT_TRUE(begin->row_key_ == cell_vec[i].row_key_);
    EXPECT_TRUE(begin->value_ == cell_vec[i].value_);
  }
}

TEST(ObCellArray, apply)
{
  vector<string> rowkeys;
  vector<ObCellInfo> cell_vec;
  vector<string>     cell_val;
  ObCellArray cell_array;
  ObCellInfo cur_cell;
  ObString obstr;
  char buf[128]="";
  char str_char = 'a';
  int64_t val;
  int len = 0;
  const int64_t cell_num = static_cast<int64_t>( 1024*1024*1.5);
  ObInnerCellInfo *cell_out = NULL;
  int64_t not_appended_num = cell_num;
  int64_t appended_num = 0;
  for (int64_t i = 0; i < cell_num; i ++)
  {
    val = random();
    if (val < 0)
    {
      val *= -1;
    }
    str_char = static_cast<char>('a' + val%26);
    len = static_cast<int>(val%sizeof(buf) + 1);
    memset(buf,str_char,len);
    string str(buf,buf+len);
    obstr.assign(const_cast<char*>(str.c_str()),static_cast<int32_t>(str.size()));
    rowkeys.push_back(str);
    cur_cell.row_key_ = TestRowkeyHelper(obstr, &allocator_);
    cur_cell.value_.set_varchar(obstr);

    cell_vec.push_back(cur_cell);
  }
  ObCellInfo src;
  ObInnerCellInfo * out = NULL;
  while (not_appended_num > 0)
  {
    int64_t cur_cell_num = 0;
    if (not_appended_num < 10)
    {
      cur_cell_num = not_appended_num;
    }
    else
    {
      cur_cell_num = random()%not_appended_num;
    }
    for (int64_t i = 0; i < cur_cell_num; ++i)
    {
      src.table_id_ = cell_vec[i].table_id_;
      src.column_id_ = cell_vec[i].column_id_;
      src.row_key_ = cell_vec[appended_num].row_key_;
      EXPECT_EQ(cell_array.append(src, out), 0);
      EXPECT_TRUE(out->row_key_ == src.row_key_);
      EXPECT_EQ(out->table_id_, src.table_id_);
      EXPECT_EQ(out->column_id_, src.column_id_);
      //assert(cell_array.apply(cell_vec[appended_num],appended_num,out) == 0);
      EXPECT_EQ(cell_array.apply(cell_vec[appended_num],appended_num,out),0);
      appended_num ++;
      not_appended_num --;
    }
    EXPECT_EQ(cell_array.get_cell_size(), appended_num);
  }

  for (int64_t i = 0; i < cell_num; i ++)
  {
    int64_t apply_offset = random()%cell_num;
    ObObj new_val;
    ObCellInfo new_cell;
    str_char = static_cast<char>('a' + val%26);
    len = static_cast<int>(val%sizeof(buf) + 1);
    memset(buf,str_char,len);
    string str(buf,buf+len);
    cell_val.push_back(str);
    obstr.assign(const_cast<char*>(cell_val[cell_val.size()-1].c_str()), static_cast<int32_t>(cell_val[cell_val.size()-1].size()));
    new_val.set_varchar(obstr);
    new_cell = cell_vec[apply_offset];
    EXPECT_EQ(cell_vec[apply_offset].value_.apply(new_val), 0);
    new_cell.value_ = new_val;
    EXPECT_EQ(cell_array.apply(new_cell, apply_offset, cell_out),0);
  }

  ObInnerCellInfo * cell = NULL;
  ObCellArray::iterator begin = cell_array.begin();
  ObCellArray::iterator end = cell_array.end();
  for (int64_t i = 0; i < cell_num; i ++, begin ++)
  {
    EXPECT_EQ(cell_array.get_cell(i,cell_out),0);
    EXPECT_TRUE(cell_out->row_key_ == cell_vec[i].row_key_);
    EXPECT_TRUE(cell_out->value_ == cell_vec[i].value_);
    EXPECT_TRUE(begin->row_key_ == cell_vec[i].row_key_);
    EXPECT_TRUE(begin->value_ == cell_vec[i].value_);
    /// next cell
    EXPECT_EQ(cell_array.next_cell(),0);
    EXPECT_EQ(cell_array.get_cell(&cell),0);
    EXPECT_TRUE(cell->row_key_ == cell_vec[i].row_key_);
    EXPECT_TRUE(cell->value_ == cell_vec[i].value_);
  }
  EXPECT_EQ(cell_array.next_cell(),OB_ITER_END);
}

TEST(ObCellArray, order)
{
  vector<string> rowkeys;
  vector<ObCellInfo> cell_vec;
  ObCellArray cell_array;
  ObCellInfo cur_cell;
  ObString obstr;
  char buf[128]="";
  char str_char = 'a';
  int64_t val;
  int len = 0;
  const int64_t cell_num = static_cast<int64_t>( 1024*1024*1.5);
  const int64_t row_width = 128;
  const int64_t row_size = cell_num/row_width;
  const int64_t max_val_num = cell_num/1024;
  ObInnerCellInfo *cell_out = NULL;
  vector<ObCellInfo> * cell_2darray[row_size];
  int32_t cell_2darray_index[row_size];
  for (int64_t row_idx = 0; row_idx < row_size; row_idx ++)
  {
    cell_2darray_index[row_idx] = static_cast<int32_t>(row_idx);
    for (int64_t cell_idx = 0; cell_idx < row_width; cell_idx ++)
    {
      val = random();
      if (val < 0)
      {
        val *= -1;
      }
      str_char = static_cast<char>('a' + val%26);
      len = static_cast<int32_t>(val%sizeof(buf) + 1);
      memset(buf,str_char,len);
      string str(buf,buf+len);
      obstr.assign(const_cast<char*>(str.c_str()),static_cast<int32_t>(str.size()));
      rowkeys.push_back(str);
      cur_cell.row_key_ = TestRowkeyHelper(obstr, &allocator_);
      cur_cell.value_.set_int(val%max_val_num);
      if (0 == cell_idx)
      {
        cell_2darray[row_idx]  = new vector< ObCellInfo >;
      }
      cell_2darray[row_idx]->push_back(cur_cell);
      EXPECT_EQ(cell_array.append(cur_cell, cell_out),0);
    }
  }
  for (int64_t row_idx = 0; row_idx < row_size; row_idx ++)
  {
    for (int64_t cell_idx = 0; cell_idx < row_width; cell_idx ++)
    {
      EXPECT_EQ((*cell_2darray[row_idx])[cell_idx].value_.get_type(), ObIntType);
    }
  }
  ObCellArray::OrderDesc order_desc[3];
  order_desc[0].cell_idx_ = 50;
  order_desc[0].order_ = ObScanParam::DESC;
  order_desc[1].cell_idx_ = 3;
  order_desc[1].order_ = ObScanParam::ASC;
  order_desc[2].cell_idx_ = 70;
  order_desc[2].order_ = ObScanParam::DESC;
  VRowComp vrow_cmp(order_desc,sizeof(order_desc)/sizeof(ObCellArray::OrderDesc), cell_2darray);
  std::sort(cell_2darray_index, cell_2darray_index + row_size,vrow_cmp);
  EXPECT_EQ(cell_array.orderby(row_width,order_desc,
                               sizeof(order_desc)/sizeof(ObCellArray::OrderDesc)), 0);
  ObInnerCellInfo * cell_info = NULL;
  for (int64_t row_idx = 0; row_idx < row_size; row_idx ++)
  {
    for (int64_t cell_idx = 0; cell_idx < row_width; cell_idx ++)
    {
      EXPECT_EQ(cell_array.next_cell(),0);
      EXPECT_EQ(cell_array.get_cell(&cell_info),0);

      EXPECT_TRUE(cell_info->row_key_
                  == cell_2darray[cell_2darray_index[row_idx]]->operator [](cell_idx).row_key_);
      EXPECT_TRUE(cell_info->value_
                  == cell_2darray[cell_2darray_index[row_idx]]->operator [](cell_idx).value_);
    }
  }
  EXPECT_EQ(cell_array.next_cell(),OB_ITER_END);
  bool bingo1 = false;
  bool bingo2 = false;
  bool bingo3 = false;
  for (int64_t row_idx = 1; row_idx < row_size; row_idx ++)
  {
    vector<ObCellInfo> & prev_row = *cell_2darray[cell_2darray_index[row_idx - 1]];
    vector<ObCellInfo> & cur_row = *cell_2darray[cell_2darray_index[row_idx]];
    /*
    EXPECT_TRUE((cur_row[order_desc[0].cell_idx_].value_ < prev_row[order_desc[0].cell_idx_].value_)
                || (cur_row[order_desc[0].cell_idx_].value_ == prev_row[order_desc[0].cell_idx_].value_
                    && (cur_row[order_desc[1].cell_idx_].value_ > prev_row[order_desc[1].cell_idx_].value_))
                || (cur_row[order_desc[0].cell_idx_].value_ == prev_row[order_desc[0].cell_idx_].value_
                    && (cur_row[order_desc[1].cell_idx_].value_ == prev_row[order_desc[1].cell_idx_].value_)
                    && (cur_row[order_desc[2].cell_idx_].value_ < prev_row[order_desc[2].cell_idx_].value_))
               );
               */
    if (cur_row[order_desc[0].cell_idx_].value_ == prev_row[order_desc[0].cell_idx_].value_)
    {
      bingo1 = true;
    }
    if (cur_row[order_desc[0].cell_idx_].value_ == prev_row[order_desc[0].cell_idx_].value_
        && (cur_row[order_desc[1].cell_idx_].value_ == prev_row[order_desc[1].cell_idx_].value_))
    {
      bingo2 = true;
    }
    if (cur_row[order_desc[0].cell_idx_].value_ == prev_row[order_desc[0].cell_idx_].value_
        && (cur_row[order_desc[1].cell_idx_].value_ == prev_row[order_desc[1].cell_idx_].value_)
        && (cur_row[order_desc[2].cell_idx_].value_ < prev_row[order_desc[2].cell_idx_].value_))
    {
      bingo3 = true;
    }
  }
  if (bingo1)
  {
    cout<<"bingo1"<<endl;
  }
  if (bingo2)
  {
    cout<<"bingo2"<<endl;
  }
  if (bingo3)
  {
    cout<<"bingo3"<<endl;
  }


  /// test limit
  int64_t limit_offset = 1111;
  int64_t limit_count = (row_size - limit_offset)/2;
  cell_array.reset_iterator();
  EXPECT_EQ(cell_array.limit(limit_offset,limit_count,row_width),0);
  for (int row_idx = static_cast<int>(limit_offset);
      row_idx < limit_count + limit_offset && row_idx < row_size;
      row_idx++)
  {
    for (int64_t cell_idx = 0; cell_idx < row_width; cell_idx ++)
    {
      EXPECT_EQ(cell_array.next_cell(),0);
      EXPECT_EQ(cell_array.get_cell(&cell_info),0);

      EXPECT_TRUE(cell_info->row_key_
                  == cell_2darray[cell_2darray_index[row_idx]]->operator [](cell_idx).row_key_);
      EXPECT_TRUE(cell_info->value_
                  == cell_2darray[cell_2darray_index[row_idx]]->operator [](cell_idx).value_);
    }
  }
  EXPECT_EQ(cell_array.next_cell(),OB_ITER_END);
}

int main(int argc, char **argv)
{
  srandom(static_cast<unsigned int>(time(NULL)));
  ob_init_memory_pool(64*1024);
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
