#include <arpa/inet.h>
#include "common/ob_define.h"
#include "multi_parellel_get.h"
#include "olap.h"
#include <assert.h>
#include "../../common/test_rowkey_helper.h"
using namespace oceanbase;
using namespace oceanbase::common;
MultiGetP MultiGetP::static_case_;
static CharArena allocator_;
namespace
{
  const int64_t get_cell_count = 8192;
}

bool MultiGetP::check_result(const uint32_t min_key_include, const uint32_t max_key_include, ObScanner &result,
  void *arg)
{
  bool res = true;
  int err = OB_SUCCESS;
  UNUSED(min_key_include);
  UNUSED(max_key_include);
  ObGetParam *get_param = reinterpret_cast<ObGetParam*>(arg);
  ObCellInfo *cur_cell = NULL;

  if ((OB_SUCCESS == err) && (result.get_cell_num() != get_param->get_cell_size()))
  {
    TBSYS_LOG(WARN,"result cell count not correct [got_cell_count:%ld,request_cell_count:%ld]", result.get_cell_num(), 
      get_param->get_cell_size());
    err = OB_ERR_UNEXPECTED;
  }
  for (int64_t i = 0; (i < get_param->get_cell_size()) && (OB_SUCCESS == err); i++)
  {
    if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = result.next_cell())))
    {
      TBSYS_LOG(WARN,"fail to get next cell [err:%d]", err);
    }
    if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = result.get_cell(&cur_cell))))
    {
      TBSYS_LOG(WARN,"fail to get next cell [err:%d]", err);
    }
    if (OB_SUCCESS == err)
    {
      if ((cur_cell->table_name_ != (*get_param)[i]->table_name_)
        || (cur_cell->column_name_ != (*get_param)[i]->column_name_)
        || (cur_cell->row_key_ != (*get_param)[i]->row_key_)
        || !(msolap::olap_check_cell(*cur_cell)))
      {
        TBSYS_LOG(WARN,"cell error");
        err = OB_ERR_UNEXPECTED;
      }
    }
  }
  if ((OB_SUCCESS == err) && (result.next_cell() != OB_ITER_END))
  {
    TBSYS_LOG(WARN,"result not ended as expected");
    err = OB_ERR_UNEXPECTED;
  }
  if (OB_SUCCESS != err)
  {
    res = false;
  }
  return res;
}

int MultiGetP::form_get_param(ObGetParam &get_param, const uint32_t min_key_include, const uint32_t max_key_include,
  void *&arg)
{
  int err = OB_SUCCESS;
  arg = NULL;
  get_param.reset(true);
  char c_name;

  ObCellInfo cell;
  cell.table_name_ = msolap::target_table_name;
  cell.column_name_.assign(&c_name, sizeof(c_name));
  int64_t cell_count = get_cell_count;
  if (cell_count > max_key_include -  min_key_include + 1)
  {
    cell_count = max_key_include -  min_key_include + 1;
  }

  int64_t random_selector = (max_key_include -  min_key_include + 1)/cell_count;
  /// bool get_last_cell = false;

  uint32_t start_key = min_key_include;
  uint32_t start_key_val = 0;
  char strkey[32];
  for (int64_t cell_idx = 0; 
    (OB_SUCCESS == err) && (start_key <= max_key_include) && (cell_idx < cell_count);
    cell_idx ++,start_key = static_cast<int32_t>(start_key + random_selector))
  {
    uint32_t cur_key = static_cast<uint32_t>(start_key + random() % random_selector);
    c_name = msolap::olap_get_column_name(msolap::min_column_id + random()%(msolap::max_column_id - msolap::min_column_id + 1));
    start_key_val = htonl(cur_key);
    snprintf(strkey, 32, "%d", start_key_val);
    cell.row_key_ = make_rowkey(strkey, &allocator_);
    assert(cell.column_name_.ptr()[0] <= 'z'  && cell.column_name_.ptr()[0] >= 'a');
    if (OB_SUCCESS != (err = get_param.add_cell(cell)))
    {
      TBSYS_LOG(WARN,"fail to add cell to ObGetParam [err:%d,c_name:%c]", err, c_name);
    }
  }
  if (OB_SUCCESS == err)
  {
    arg = &get_param;
  }
  return err;
}



