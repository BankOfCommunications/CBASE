#include "common/ob_define.h"
#include "common/ob_range.h"
#include "common/utility.h"
#include "common/ob_get_param.h"
#include "common/ob_scan_param.h"
#include "common/ob_scanner.h"
#include "ob_ms_dump_util.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

void ObMergerDumpUtil::dump(const ObGetParam & param)
{
  ObCellInfo * cur_cell = NULL;
  int64_t count = param.get_cell_size();
  for (int64_t i = 0; i < count; ++i)
  {
    cur_cell = param[i];
    if (NULL != cur_cell)
    {
      TBSYS_LOG(INFO, "tableid:%lu,rowkey:%.*s,column_id:%lu,ext:%ld,type:%d",
          cur_cell->table_id_,
          cur_cell->row_key_.length(), cur_cell->row_key_.ptr(), cur_cell->column_id_,
          cur_cell->value_.get_ext(), cur_cell->value_.get_type());
      hex_dump(cur_cell->row_key_.ptr(), cur_cell->row_key_.length(), true, TBSYS_LOG_LEVEL_INFO);
      cur_cell->value_.dump();
    }
    else
    {
      TBSYS_LOG(WARN, "get cell failed:index[%ld]", i);
      break;
    }
  }
}

void ObMergerDumpUtil::dump(const ObScanParam & param)
{
  const ObRange * range = param.get_range();
  if (NULL == range)
  {
    TBSYS_LOG(WARN, "check scan param range failed:table_id[%lu], range[%p]",
        param.get_table_id(), range);
  }
  else
  {
    char range_buffer[256] = "";
    range->to_string(range_buffer, sizeof(range_buffer));
    TBSYS_LOG(INFO, "scan:table_id[%lu], range[%s], direction[%s]",
        param.get_table_id(), range_buffer,
        (param.get_scan_direction() == ObScanParam::FORWARD) ? "forward": "backward");
  }
}

void ObMergerDumpUtil::dump(const ObScanner & scanner)
{
  int ret = OB_SUCCESS;
  ObCellInfo * cur_cell = NULL;
  bool is_fullfilled = false;
  int64_t count = 0;
  ret = scanner.get_is_req_fullfilled(is_fullfilled, count);
  TBSYS_LOG(INFO, "fullfill item count:fullfill[%d], count[%ld]", is_fullfilled, count);
  ObScanner & result = const_cast<ObScanner &>(scanner);
  while (result.next_cell() == OB_SUCCESS)
  {
    ret = result.get_cell(&cur_cell);
    if (OB_SUCCESS == ret)
    {
      TBSYS_LOG(INFO, "tableid:%lu,rowkey:%.*s,column_id:%lu,ext:%ld,type:%d",
          cur_cell->table_id_,
          cur_cell->row_key_.length(), cur_cell->row_key_.ptr(), cur_cell->column_id_,
          cur_cell->value_.get_ext(), cur_cell->value_.get_type());
      hex_dump(cur_cell->row_key_.ptr(), cur_cell->row_key_.length(), true, TBSYS_LOG_LEVEL_INFO);
      cur_cell->value_.dump();
    }
    else if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get cell failed:ret[%d]", ret);
      break;
    }
  }
  result.reset_iter();
}
