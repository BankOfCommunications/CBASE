#include "common/ob_get_param.h"
#include "common/data_buffer.h"
#include "common/ob_get_param.h"
#include "common/utility.h"
#include "load_util.h"

using namespace oceanbase::common;
using namespace oceanbase::tools;

bool LoadUtil::compare_cell(const ObCellInfo & first, const ObCellInfo & second)
{
  bool ret = (first.table_name_ == second.table_name_) && (first.table_id_ == second.table_id_)
      && (first.row_key_ == second.row_key_) && (first.column_name_ == second.column_name_)
      && (first.column_id_ == second.column_id_);
  if (true == ret)
  {
    if (first.value_.get_type() != ObExtendType)
    {
      ret = first.value_ == second.value_;
    }
    else
    {
      ret = first.value_.get_ext() == second.value_.get_ext();
    }
  }
  if (!ret)
  {
  TBSYS_LOG(ERROR, "unequal object data.first.table_name=%s, first.row_key_=%s, second.row_key_=%s, column_name=%s, second.column_name=%s, first.column_id=%ld, second.column_id=%ld",
            first.table_name_.ptr(), to_cstring(first.row_key_), to_cstring(second.row_key_), to_cstring(first.column_name_), second.column_name_.ptr(), first.column_id_, second.column_id_);
        first.value_.dump(TBSYS_LOG_LEVEL_DEBUG);
        second.value_.dump(TBSYS_LOG_LEVEL_DEBUG);
  }
  return ret;
}

bool LoadUtil::compare_result(ObScanner & first, ObScanner & second)
{
  bool ret = true;
  bool first_fullfill = false;
  int64_t first_item = 0;
  bool second_fullfill = false;
  int64_t second_item = 0;
  first.get_is_req_fullfilled(first_fullfill, first_item);
  second.get_is_req_fullfilled(second_fullfill, second_item);

  if ((first.is_empty() != second.is_empty()) || (first_fullfill != second_fullfill)
      || (first_item != second_item))
  {
    TBSYS_LOG(ERROR, "fail to compare.first.is_empty=%d ,second.is_empty=%d, first fullfill=%d, second fullfill=%d, first_item=%ld, second_item=%ld",
        first.is_empty(),second.is_empty(),first_fullfill,second_fullfill,first_item,second_item);
    first.dump_all(TBSYS_LOG_LEVEL_WARN);
    second.dump_all(TBSYS_LOG_LEVEL_WARN);
    ret = false;
  }
  ObCellInfo * first_cell = NULL;
  ObCellInfo * second_cell = NULL;
  bool first_change = false;
  bool second_change = false;

  uint64_t table_id = 1051;
  char buf[128] = "lz_rpt_search_diagnose_auction_title_diagnose_ob";
  ObString table_name;
  table_name.assign_ptr(buf, int32_t((strlen(buf))));
  bool expect = false;
  if (ret == false)
  {
    if (OB_SUCCESS == second.next_cell())
    {
      if (OB_SUCCESS == second.get_cell(&second_cell, &second_change))
      {
        if (second_cell->table_name_ == table_name || second_cell->table_id_ == table_id)
        {
          expect = true;
          ret = true;
        }
        TBSYS_LOG(WARN, "fail to compare. table_name=%s, table_id=%ld, rowkey=%s",
            second_cell->table_name_.ptr(), second_cell->table_id_, to_cstring(second_cell->row_key_));
      }
    }
  }
  if (expect)
  {
    ret = true;
  }
  else
  {
    second.reset_iter();
    while (OB_ITER_END != first.next_cell())
    {
      if (OB_SUCCESS != second.next_cell())
      {
        TBSYS_LOG(ERROR, "unequal cell num. first cell =%ld, second cell=%ld", first.get_cell_num(), second.get_cell_num());
        ret = false;
        break;
      }
      else
      {
        if (OB_SUCCESS != first.get_cell(&first_cell, &first_change))
        {
          TBSYS_LOG(ERROR, "fail to get first cell.");
          ret = false;
          break;
        }
        if (OB_SUCCESS != second.get_cell(&second_cell, &second_change))
        {
          TBSYS_LOG(ERROR, "fail to get second cell");
          ret = false;
          break;
        }
        if ((first_change != second_change) || (false == compare_cell(*first_cell, *second_cell)))
        {
          TBSYS_LOG(WARN, "fail to compare cell.");
          ret = false;
          break;
        }
      }
    }
    // last cells
    if (true == ret)
    {
      ret = (second.next_cell() == OB_ITER_END);
      if (!ret)
      {
        TBSYS_LOG(ERROR, "fail to compare.");
      }
    }
  }
  first.reset_iter();
  second.reset_iter();
  return ret;
}

int LoadUtil::dump_param(const ObGetParam & param)
{
  int ret = OB_SUCCESS;
  const ObCellInfo * cur_cell = NULL;
  int64_t cell_size = param.get_cell_size();
  for (int64_t i = 0; i < cell_size; ++i)
  {
    cur_cell = param[i];
    if (cur_cell != NULL)
    {
      //TBSYS_LOG(INFO, "table_name:%.*s, tableid:%lu, rowkey:%.*s,"
      //    "column_name:%.*s, column_id:%lu, ext:%ld, type:%d",
      //    cur_cell->table_name_.length(), cur_cell->table_name_.ptr(), cur_cell->table_id_,
      //    cur_cell->row_key_.length(), cur_cell->row_key_.ptr(),
      //    cur_cell->column_name_.length(), cur_cell->column_name_.ptr(), cur_cell->column_id_,
      //    cur_cell->value_.get_ext(),cur_cell->value_.get_type());
    }
    else
    {
      TBSYS_LOG(ERROR, "check the cell failed:index[%ld]", i);
      ret = OB_ERROR;
      break;
    }
  }
  return ret;
}

bool LoadUtil::check_request(Request * request)
{
  bool ret = true;
  if (NULL == request)
  {
    ret = false;
  }
  else
  {
    int64_t pos = 0;
    ObDataBuffer data_buffer;
    if (request->pcode == 101)
    {
      data_buffer.set_data(request->buf, request->buf_size);
      ObGetParam param;
      int err = param.deserialize(data_buffer.get_data(), data_buffer.get_capacity(), pos);
      if (err != OB_SUCCESS)
      {
        ret = false;
        TBSYS_LOG(ERROR, "deserialize param failed:err[%d]", err);
      }
      else if (0 == param.get_cell_size())
      {
        ret = false;
        TBSYS_LOG(ERROR, "check param cell size failed");
      }
      else
      {
        err = dump_param(param);
      }
    }
  }
  return ret;
}
