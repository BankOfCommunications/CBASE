#include "common/ob_get_param.h"
#include "common/ob_scan_param.h"
#include "common/ob_scanner.h"
#include "common/utility.h"
#include "ob_ms_scanner_encoder.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

void ObMergerScannerEncoder::output(const common::ObCellInfo & cell)
{
  TBSYS_LOG(DEBUG, "tableid:%lu, table_name:%.*s, rowkey:%s,"
      "column_id:%lu, column_name:%.*s, ext:%ld, type:%d",
      cell.table_id_, cell.table_name_.length(), cell.table_name_.ptr(),
      to_cstring(cell.row_key_),
      cell.column_id_, cell.column_name_.length(), cell.column_name_.ptr(),
      cell.value_.get_ext(), cell.value_.get_type());
}

int ObMergerScannerEncoder::encode(const ObGetParam & get_param, const ObScanner & org_scanner,
    ObScanner & encoded_scanner)
{
  int ret = OB_SUCCESS;
  ObCellInfo * cur_cell = NULL;
  ObCellInfo * encode_cell = NULL;
  int64_t cell_index = 0;
  ObScanner & input = const_cast<ObScanner &> (org_scanner);
  while (OB_SUCCESS == (ret = input.next_cell()))
  {
    ret = input.get_cell(&cur_cell);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get cell from org scanner failed:ret[%d]", ret);
      break;
    }
    // output(*cur_cell);
    encode_cell = get_param[cell_index];
    if (NULL == encode_cell)
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "check encode cell failed:index[%ld]", cell_index);
      break;
    }
    encode_cell->value_ = cur_cell->value_;
    ret = encoded_scanner.add_cell(*encode_cell);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "add cell to encoded scanner failed:ret[%d]", ret);
      break;
    }
    ++cell_index;
  }
  // double check size
  if (cell_index != get_param.get_cell_size())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check result scanner cell count not equal get param cell size:"
        "count[%ld], size[%ld]", cell_index, get_param.get_cell_size());
  }
  input.reset_iter();
  // fill all data succ
  if (OB_ITER_END == ret)
  {
    ret = OB_SUCCESS;
  }
  return ret;
}

