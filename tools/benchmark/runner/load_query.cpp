#include "load_query.h"

using namespace oceanbase::common;
using namespace oceanbase::tools;

QueryInfo::QueryInfo()
{
  id_ = 0;
  packet_ = NULL;
}

QueryInfo::~QueryInfo()
{
  reset();
}

void QueryInfo::reset()
{
  //TBSYS_LOG(DEBUG, "reset queryinfo. packet=%p", packet_);
  id_ = 0;
  packet_ = NULL;
}

DEFINE_SERIALIZE(QueryInfo)
{
  assert(packet_ != NULL);
  int ret = serialization::encode_vi64(buf, buf_len, pos, id_);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "serialize id failed:id[%ld], ret[%d]", id_, ret);
  }
  else
  {
    memcpy(buf + pos, packet_, sizeof(common::FIFOPacket) + packet_->buf_size);
  }
  return ret;
}

DEFINE_DESERIALIZE(QueryInfo)
{
  assert(packet_ == NULL);
  int ret = serialization::decode_vi64(buf, data_len, pos, &id_);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "deserialize id failed:id[%ld], ret[%d]", id_, ret);
  }
  else
  {
    const FIFOPacket * header = reinterpret_cast<const common::FIFOPacket*> (buf + pos);
    packet_ = reinterpret_cast<common::FIFOPacket *>(malloc(sizeof(common::FIFOPacket) + header->buf_size));
    assert(packet_ != NULL);
    memcpy(packet_, buf + pos, sizeof(common::FIFOPacket) + header->buf_size);
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(QueryInfo)
{
  assert(packet_ != NULL);
  int64_t size = serialization::encoded_length_vi64(id_);
  size += sizeof(common::FIFOPacket) + packet_->buf_size;
  return size;
}

