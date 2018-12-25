#include <arpa/inet.h>
#include "common/ob_define.h"
#include "common/ob_action_flag.h"
#include "get_not_exist_rows.h"
#include "olap.h"
#include "../../common/test_rowkey_helper.h"

using namespace oceanbase;
using namespace oceanbase::common;
GetNotExistRows GetNotExistRows::static_case_;
static CharArena allocator_;

bool GetNotExistRows::check_result(const uint32_t min_key_include, const uint32_t max_key_include, ObScanner &result,
  void *arg)
{
  bool res = true;
  int err = OB_SUCCESS;
  UNUSED(min_key_include);
  UNUSED(max_key_include);
  ObGetParam *get_param = reinterpret_cast<ObGetParam*>(arg);
  if(result.get_cell_num() != get_param->get_cell_size())
  {
    TBSYS_LOG(WARN,"result cell number error [got:%ld,expcted:%ld]", result.get_cell_num(),  get_param->get_cell_size());
    err = OB_ERR_UNEXPECTED;
  }
  int64_t got_cell_num = 0;
  ObCellInfo *cur_cell = NULL;
  for (got_cell_num = 0; (got_cell_num < get_param->get_cell_size()) && (OB_SUCCESS == err); got_cell_num++)
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
      || (cur_cell->row_key_ != (*get_param)[got_cell_num]->row_key_) 
      || (ObActionFlag::OP_ROW_DOES_NOT_EXIST != cur_cell->value_.get_ext())))
    {
      TBSYS_LOG(WARN,"cell content error");
      err = OB_ERR_UNEXPECTED;
    }
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

int GetNotExistRows::form_get_param(ObGetParam &get_param, const uint32_t min_key_include, const uint32_t max_key_include,
  void *&arg)
{
  UNUSED(min_key_include);
  int err = OB_SUCCESS;
  arg = NULL;
  const int32_t get_cell_count = 32;
  const uint32_t random_selector = 1024*16;
  uint32_t start_key_val = static_cast<uint32_t>(max_key_include + 1 + random()%random_selector);
  uint32_t key;
  char strkey[32];
  get_param.reset(true);
  ObCellInfo cell;
  cell.table_name_ = msolap::target_table_name;
  cell.column_name_ = ObGetParam::OB_GET_ALL_COLUMN_NAME;
  for (int32_t i = 0; (OB_SUCCESS == err) && (i < get_cell_count); i++, start_key_val ++)
  {
    key = htonl(start_key_val);
    snprintf(strkey, 32, "%d", key);
    cell.row_key_ = make_rowkey(strkey, &allocator_);
    if (OB_SUCCESS != (err = get_param.add_cell(cell)))
    {
      TBSYS_LOG(WARN,"fail to add cell to ObGetParam [err:%d]", err);
    }
  } 
  if (OB_SUCCESS == err)
  {
    arg = &get_param;
  }
  return err;
}



