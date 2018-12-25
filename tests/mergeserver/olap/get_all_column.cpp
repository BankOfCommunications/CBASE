#include <arpa/inet.h>
#include "common/ob_define.h"
#include "get_all_column.h"
#include "olap.h"
#include "../../common/test_rowkey_helper.h"
using namespace oceanbase;
using namespace oceanbase::common;
GetSingleRowAllColumn GetSingleRowAllColumn::static_case_;
static CharArena allocator_;

bool GetSingleRowAllColumn::check_result(const uint32_t min_key_include, const uint32_t max_key_include, ObScanner &result,
  void *arg)
{
  bool res = true;
  int err = OB_SUCCESS;
  UNUSED(min_key_include);
  UNUSED(max_key_include);
  ObGetParam *get_param = reinterpret_cast<ObGetParam*>(arg);
  uint32_t big_endian_rowkey_val = *(uint32_t*)(((*get_param)[0])->row_key_.ptr());
  uint32_t rowkey_val = ntohl(big_endian_rowkey_val);
  ObCellInfo target_cell;
  target_cell.row_key_ = (*get_param)[0]->row_key_;
  target_cell.table_name_ = msolap::target_table_name;
  if (result.get_cell_num() != msolap::max_column_id - msolap::min_column_id + 1)
  {
    TBSYS_LOG(WARN,"result cell number error [got:%ld,expcted:%ld]", result.get_cell_num(), 
      msolap::max_column_id - msolap::min_column_id + 1);
    err = OB_ERR_UNEXPECTED;
  }
  int64_t got_cell_num = 0;
  ObCellInfo *cur_cell = NULL;
  int64_t intval = 0;
  for (got_cell_num = 0; (got_cell_num < result.get_cell_num()) && (OB_SUCCESS == err); got_cell_num++)
  {
    if (OB_SUCCESS != (err = result.next_cell()))
    {
      TBSYS_LOG(WARN,"fail to get next cell from result [err:%d]", err);
    }
    if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = result.get_cell(&cur_cell))))
    {
      TBSYS_LOG(WARN,"fail to get next cell from result [err:%d]", err);
    }
    if ((OB_SUCCESS == err) 
      && ((cur_cell->table_name_ != msolap::target_table_name) 
      || (cur_cell->row_key_ != target_cell.row_key_) 
      || (cur_cell->column_name_.length() != 1)
      || (OB_SUCCESS != cur_cell->value_.get_int(intval))))
    {
      TBSYS_LOG(WARN,"cell content error");
      err = OB_ERR_UNEXPECTED;
    }
    if ((OB_SUCCESS == err) && ((cur_cell->column_name_.ptr()[0] < 'a') || (cur_cell->column_name_.ptr()[0] > 'z')))
    {
      TBSYS_LOG(WARN,"cell content error");
      err = OB_ERR_UNEXPECTED;
    }
    if (OB_SUCCESS == err)
    {
      int64_t expect_intval = 0;
      char c_name = cur_cell->column_name_.ptr()[0];
      switch (c_name)
      {
      case 'a':
      case 'b':
      case 'c':
      case 'd':
        expect_intval = ((unsigned char*)&big_endian_rowkey_val)[c_name - 'a'];
        break;
      default:
        expect_intval = rowkey_val;
      }
      if (expect_intval != intval)
      {
        TBSYS_LOG(WARN,"intvalue error [column_name:%c,expect_val:%ld,got_val:%ld,rowkey:%u]", c_name, 
          expect_intval, intval, rowkey_val);
        err = OB_ERR_UNEXPECTED;
      }
    }
  }
  if ((OB_SUCCESS == err) && (got_cell_num != msolap::max_column_id - msolap::min_column_id + 1))
  {
    TBSYS_LOG(WARN,"fail to get enough cells from result");
    err = OB_ERR_UNEXPECTED;
  }
  if ((OB_SUCCESS == err) && (OB_ITER_END != result.next_cell()))
  {
    TBSYS_LOG(WARN,"fail to get enough cells from result");
    err = OB_ERR_UNEXPECTED;
  }
  if (OB_SUCCESS != err)
  {
    res = false;
  }
  return res;
}

int GetSingleRowAllColumn::form_get_param(ObGetParam &get_param, const uint32_t min_key_include, const uint32_t max_key_include,
  void *&arg)
{
  int err = OB_SUCCESS;
  arg = NULL;
  uint32_t start_key_val = htonl(static_cast<int32_t>(random()%(max_key_include - min_key_include + 1) + min_key_include));
  char key[32];
  snprintf(key, 32, "%d", start_key_val);
  get_param.reset(true);
  ObCellInfo cell;
  cell.table_name_ = msolap::target_table_name;
  cell.column_name_ = ObGetParam::OB_GET_ALL_COLUMN_NAME;
  cell.row_key_ = make_rowkey(key, &allocator_);
  if (OB_SUCCESS != (err = get_param.add_cell(cell)))
  {
    TBSYS_LOG(WARN,"fail to add cell to ObGetParam [err:%d]", err);
  }
  if (OB_SUCCESS == err)
  {
    arg = &get_param;
  }
  return err;
}



