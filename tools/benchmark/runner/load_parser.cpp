#include "common/ob_packet.h"
#include "load_parser.h"

using namespace oceanbase::tools;
using namespace oceanbase::common;

int LoadParser::is_consistency_read(const common::Request * request, bool & read_master) const
{
  int ret = OB_SUCCESS;
  read_master = false;
  if (NULL == request)
  {
    TBSYS_LOG(WARN, "check request failed:ptr[%p]", request);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    // is read request
    int64_t pos = 0;
    ObDataBuffer data_buffer;
    data_buffer.set_data(const_cast<common::Request *>(request)->buf, (int64_t)request->buf_size);
    switch (request->pcode)
    {
    case OB_GET_REQUEST:
      {
        ret = get_param_.deserialize(data_buffer.get_data(), data_buffer.get_capacity(), pos);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "get param deserialize failed:pos[%ld], len[%d], ret[%d]", pos, request->buf_size, ret);
        }
        else
        {
          read_master = get_param_.get_is_read_consistency();
        }
        break;
      }
    case OB_SCAN_REQUEST:
      {
        ret = scan_param_.deserialize(data_buffer.get_data(), data_buffer.get_capacity(), pos);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "scan param deserialize failed:pos[%ld], len[%d], ret[%d]", pos, request->buf_size, ret);
        }
        else
        {
          read_master = scan_param_.get_is_read_consistency();
        }
        break;
      }
    default:
      {
        break;
      }
    }
  }
  return ret;
}
