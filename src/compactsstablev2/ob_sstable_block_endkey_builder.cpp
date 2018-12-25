#include "ob_sstable_block_endkey_builder.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    int ObSSTableBlockEndkeyBuilder::add_item(const common::ObRowkey& row_key)
    {
      int ret = OB_SUCCESS;
      
      char* buf = NULL;
      int64_t size = 0;
      ObCompactCellWriter row;

      while(true)
      {
        if (OB_SUCCESS != (ret = buf_.get_free_buf(buf, size)))
        {
          TBSYS_LOG(WARN, "buf get free buf error:ret=%d,buf=%p,size=%ld",
              ret, buf, size);
          break;
        }
        else if (OB_SUCCESS != (ret = row.init(buf, size, DENSE)))
        {
          if (OB_SUCCESS != (ret = buf_.alloc_block()))
          {
            TBSYS_LOG(WARN, "buf alloc block error:ret=%d", ret);
            break;
          }
          else
          {
            continue;
          }
        }
        else if (OB_SUCCESS != (ret = row.append_rowkey(row_key)))
        {
          if (OB_SUCCESS != (ret = buf_.alloc_block()))
          {
            TBSYS_LOG(WARN, "buf alloc block error:ret=%d", ret);
            break;
          }
          else
          {
            continue;
          }
        }
        else if (OB_SUCCESS != (ret = buf_.set_write_size(row.size())))
        {
          TBSYS_LOG(WARN, "buf set writer size error:ret=%d,row.size()=%ld",
              ret, row.size());
          break;
        }
        else
        {
          break;
        }
      }

      return ret;
    }
  }//end namespace compactsstablev2
}//end namespace oceanbase
