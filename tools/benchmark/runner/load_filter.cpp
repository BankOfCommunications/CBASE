#include "load_filter.h"
#include "common/ob_packet.h"
#include "common/ob_fifo_stream.h"

using namespace oceanbase::common;
using namespace oceanbase::tools;

LoadFilter::LoadFilter()
{
  enable_read_master_ = true;
}

LoadFilter::~LoadFilter()
{
}

void LoadFilter::read_master(const bool enable)
{
  enable_read_master_ = enable;
}

void LoadFilter::allow(const int32_t type)
{
  white_list_.insert(type);
}

void LoadFilter::disallow(const int32_t type)
{
  white_list_.erase(type);
}

void LoadFilter::reset(void)
{
  white_list_.clear();
}

bool LoadFilter::check(const int32_t type, const char packet[], const int64_t data_len) const
{
  bool ret = false;
  if (FIFO_PKT_REQUEST == type)
  {
    const common::Request * request = reinterpret_cast<const common::Request *> (packet);
    if ((NULL == request) || (data_len < int64_t(sizeof(common::Request) + request->buf_size)))
    {
      TBSYS_LOG(WARN, "check request packet failed");
    }
    else
    {
      // request type filter
      if (white_list_.find(request->pcode) != white_list_.end())
      {
        ret = true;
      }
      bool read_master = false;
      // need filter read master
      if ((true == ret) && (false == enable_read_master_))
      {
        int err = packet_parser_.is_consistency_read(request, read_master);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "check packet is consistency read failed:filter[%d], err[%d]", ret, err);
        }
        else
        {
          ret = !read_master;
        }
      }
      TBSYS_LOG(DEBUG, "check request:code[%d], enable[%d], master[%d], filter[%d], len[%d]",
          request->pcode, enable_read_master_, read_master, ret, request->buf_size);
    }
  }
  return ret;
}

