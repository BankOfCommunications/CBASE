#include "ob_ups_compact_cell_writer.h"

using namespace oceanbase;
using namespace updateserver;

ObUpsCompactCellWriter::ObUpsCompactCellWriter() : mem_tank_(NULL),
                                                   is_row_deleted_(false)
{
}

ObUpsCompactCellWriter::~ObUpsCompactCellWriter()
{
}

void ObUpsCompactCellWriter::init(char *buf, int64_t size, MemTank *mem_tank)
{
  ObCompactCellWriter::init(buf, size);
  mem_tank_ = mem_tank;
}

int ObUpsCompactCellWriter::write_varchar(const ObObj &value, ObObj *clone_value)
{
  int ret = OB_SUCCESS;
  ObString varchar_value;
  ObString varchar_written;

  value.get_varchar(varchar_value);
  if(varchar_value.length() > INT32_MAX)
  {
    ret = OB_SIZE_OVERFLOW;
    TBSYS_LOG(WARN, "varchar is too long:[%d]", varchar_value.length());
  }
  if(OB_SUCCESS == ret)
  {
    ret = buf_writer_.write<int32_t>((int32_t)(varchar_value.length()));
  }
  if(OB_SUCCESS == ret)
  {
    if(varchar_value.length() <= (int64_t)sizeof(const char*))
    {
      ret = buf_writer_.write_varchar(varchar_value, &varchar_written);
    }
    else
    {
      if(NULL != mem_tank_)
      {
        ret = mem_tank_->write_string(varchar_value, &varchar_written);
        if(OB_SUCCESS == ret)
        {
          ret = buf_writer_.write<const char*>(varchar_written.ptr());
        }
      }
      else
      {
        ret = buf_writer_.write<const char*>(varchar_value.ptr());
      }
    }
  }
  if(OB_SUCCESS == ret && NULL != clone_value)
  {
    clone_value->set_varchar(varchar_written);
  }
  return ret;
}

