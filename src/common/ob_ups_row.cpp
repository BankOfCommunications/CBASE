
#include "ob_ups_row.h"
using namespace oceanbase::common;

ObUpsRow::ObUpsRow() 
  : ObRow(),
  is_delete_row_(false),
  is_all_nop_(false)
{
}

ObUpsRow::~ObUpsRow()
{
}

int ObUpsRow::reset()
{
  int ret = OB_SUCCESS;
  if(NULL == row_desc_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "row_desc_ is null, set row_desc first");
  }

  ObObj nop_cell;
  nop_cell.set_ext(ObActionFlag::OP_NOP);

  if(OB_SUCCESS == ret)
  {
    for(int64_t i=row_desc_->get_rowkey_cell_count();i<row_desc_->get_column_num();i++)
    {
      this->raw_set_cell(i, nop_cell);
    }
    is_all_nop_ = true;
  }
  return ret;
}

void ObUpsRow::set_is_delete_row(bool is_delete_row)
{
  is_delete_row_ = is_delete_row;
}

bool ObUpsRow::get_is_delete_row() const
{
  return is_delete_row_;
}

