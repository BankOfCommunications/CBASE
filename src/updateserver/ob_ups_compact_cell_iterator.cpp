#include "ob_ups_compact_cell_iterator.h"

using namespace oceanbase::updateserver;

int ObUpsCompactCellIterator::parse_varchar(ObBufferReader &buf_reader, ObObj &value) const
{
  int ret = OB_SUCCESS;
  const int32_t *varchar_len = NULL;
  ObString str_value;

  ret = buf_reader.get<int32_t>(varchar_len);
  if(OB_SUCCESS == ret)
  {
    if(*varchar_len <= (int32_t)sizeof(const char*))
    {
      str_value.assign_ptr(const_cast<char*>(buf_reader.cur_ptr()), *varchar_len);
      buf_reader.skip(*varchar_len);
      value.set_varchar(str_value);
    }
    else
    {
      const char* ptr = buf_reader.get<const char*>();
      str_value.assign_ptr(const_cast<char*>(ptr), *varchar_len);
      value.set_varchar(str_value);
    }
  } 
  return ret;
}


