#include "packet_queue.h"
#include "obmysql/packet/ob_mysql_command_packet.h"
#include "obmysql/ob_mysql_util.h"
#include <mysql.h>
using namespace oceanbase;
using namespace oceanbase::tools;
using namespace oceanbase::common;
using namespace oceanbase::obmysql;

PacketQueue::PacketQueue()
{
  discart_count_ = 0;
  array_ = NULL;
}

PacketQueue::~PacketQueue()
{
  //fd_thread_map_.destroy();
  if (array_ != NULL)
  {
    delete []array_;
  }
}
int64_t PacketQueue::size(void)const
{
  int64_t ret = 0;
  for (int32_t i = 0; i < thread_count_; i++)
  {
    ret += array_[i].count_;
  }
  //TBSYS_LOG(INFO, "packet queue size is %ld", ret);
  return ret;
}
int PacketQueue::init(const int64_t thread_count)
{
  int ret = OB_SUCCESS;
  thread_count_ = thread_count;
  array_ = new HeadArray [thread_count];
  if (array_ == NULL)
  {
    TBSYS_LOG(WARN, "fail to alloc array.");
    ret = OB_ERROR;
  }
  else
  {
    TBSYS_LOG(INFO, "thread count = %ld", thread_count_ );
  }
  //else if (OB_SUCCESS != (ret = fd_thread_map_.create(hash::cal_next_prime(512))) != OB_SUCCESS )
  //{
  //  TBSYS_LOG(WARN, "fail to create fd_thread_map, ret=%d", ret);
  //}
  return ret;
}

int PacketQueue::get_iovect(const QueryInfo* query_info, struct iovec *buffer, int &count)
{
  int ret = OB_SUCCESS;
  if (NULL == query_info)
  {
    TBSYS_LOG(WARN, "INVALID ARGUMENT. query_info is null");
    ret = OB_INVALID_ARGUMENT;
  }
  ObMySQLCommandPacketRecord *record = NULL;
  if (OB_SUCCESS == ret)
  {
    int64_t length = sizeof(ObMySQLCommandPacketRecord);
    buffer[0].iov_base = query_info->packet_->buf;
    buffer[0].iov_len = length;
    record = reinterpret_cast<ObMySQLCommandPacketRecord *>(buffer[0].iov_base);
    if (query_info->packet_->buf_size == length)
    {
      count = 1;
    }
    else
    {
      if (record->cmd_type_ == COM_STMT_EXECUTE
          && record->obmysql_type_ == OBMYSQL_EXECUTE)
      {
        count = 3;
        buffer[1].iov_len = record->pkt_length_;
        buffer[1].iov_base = query_info->packet_->buf +  length;
        buffer[2].iov_base = query_info->packet_->buf +  length + record->pkt_length_;
        buffer[2].iov_len = query_info->packet_->buf_size - record->pkt_length_ - length;
      }
      else
      {
        buffer[1].iov_base = query_info->packet_->buf +  length;
        buffer[1].iov_len = query_info->packet_->buf_size - length;
        count = 2;
      }
    }
  }
  //if (OB_SUCCESS == ret)
  //{
  //  if ((record->cmd_type_ == COM_STMT_EXECUTE && record->obmysql_type_ != OBMYSQL_EXECUTE)
  //      || (record->cmd_type_ == COM_STMT_PREPARE && record->obmysql_type_ != OBMYSQL_PREPARE))
  //  {
  //    ret = OB_DISCARD_PACKET;
  //  }
  //}
  return ret;
}
int PacketQueue::filter_packet(struct iovec *buffer, const int count)
{
  int ret = OB_SUCCESS;
  UNUSED(count);
  if (NULL == buffer)
  {
    TBSYS_LOG(WARN, "INVALID ARGUMENT. buffer is NULL");
    ret = OB_INVALID_ARGUMENT;
  }
  char *buff = NULL;
  int64_t length = 0;
  obmysql::ObMySQLCommandPacketRecord *record;
  if (OB_SUCCESS == ret)
  {
    record = reinterpret_cast<obmysql::ObMySQLCommandPacketRecord *>(buffer[0].iov_base);
    if (record->cmd_type_ == COM_STMT_EXECUTE && record->obmysql_type_ != OBMYSQL_EXECUTE)
    {
      ret = OB_DISCARD_PACKET;
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (record->cmd_type_ == COM_STMT_PREPARE && record->obmysql_type_ != OBMYSQL_PREPARE)
    {
      ret = OB_DISCARD_PACKET;
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = OB_ERROR;
    if (record->cmd_type_ == COM_STMT_EXECUTE)
    {
      ret = OB_SUCCESS;
      buff = reinterpret_cast<char*>(buffer[2].iov_base);
      length = buffer[2].iov_len;
    }
    else if (record->cmd_type_ == COM_QUERY || record->cmd_type_ == COM_STMT_PREPARE)
    {
      ret = OB_SUCCESS;
      buff = reinterpret_cast<char*>(buffer[1].iov_base);
      length = buffer[1].iov_len;
    }
  }
  if (OB_SUCCESS == ret)
  {
    char tmpbuff[OB_MAX_SQL_LENGTH];
    memcpy(tmpbuff, buff, length);
    tmpbuff[length] = '\0';
    if (NULL == strcasestr(tmpbuff, "select"))
    {
      ret = OB_DISCARD_PACKET;
    }
    if (NULL != strcasestr(tmpbuff, "insert"))
    {
      ret = OB_DISCARD_PACKET;
    }
    else if (NULL != strcasestr(tmpbuff, "update"))
    {
      ret = OB_DISCARD_PACKET;
    }
    else if (NULL != strcasestr(tmpbuff, "replace"))
    {
      ret = OB_DISCARD_PACKET;
    }
    else if (NULL != strcasestr(tmpbuff, "delete"))
    {
      ret = OB_DISCARD_PACKET;
    }
    if (ret == OB_DISCARD_PACKET)
    {
      TBSYS_LOG(DEBUG, "discard packet. str=%s", tmpbuff);
    }
  }
  if (OB_ERROR == ret)
  {
    ret = OB_SUCCESS;
  }
  return ret;
}

void PacketQueue::print_iovect(struct iovec *buffers, int count)
{
  ObMySQLCommandPacketRecord *record;
  int i = 0;
  record = reinterpret_cast<ObMySQLCommandPacketRecord *>(buffers[i].iov_base);
  //if (count == 2 && record->cmd_type_ == COM_QUERY)
  if (count == 2)
  {
    char* ptr = reinterpret_cast<char *>(buffers[1].iov_base);
    TBSYS_LOG(INFO, "socket fd = %d, addr = %s, packet length = %d, packet seq = %d, sesseion seq =%d, login type = %d, cmd type = %d, count=%d, sql content =%s",
        record->socket_fd_, inet_ntoa_r(record->addr_), record->pkt_length_, record->pkt_seq_, record->cseq_, record->obmysql_type_, record->cmd_type_, count, ptr);
  }
  else if (count == 3)
  {
    char* ptr = reinterpret_cast<char *>(buffers[2].iov_base);
    TBSYS_LOG(INFO, "socket fd = %d, addr = %s, packet length = %d, packet seq = %d, sesseion seq =%d, login type = %d, cmd type = %d, count=%d, excute sql =%s",
        record->socket_fd_, inet_ntoa_r(record->addr_), record->pkt_length_, record->pkt_seq_, record->cseq_, record->obmysql_type_, record->cmd_type_, count, ptr);
  }
}
int PacketQueue::select_queue(int &fd, struct iovec *buffer, int count)
{
  int ret = OB_SUCCESS;
  if (buffer == NULL)
  {
    TBSYS_LOG(WARN, "invalid argument. buffer is null.");
    ret = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == ret)
  {
    fd = int((reinterpret_cast<ObMySQLCommandPacketRecord *>(buffer[0].iov_base)->cseq_) % (thread_count_));
    if (count == 1)
    {
      //TBSYS_LOG(DEBUG, "dispatch packet. socket_fd=%d, packet_index=%d, queue_total_size=%ld, cmd_type=%d",
        // reinterpret_cast<ObMySQLCommandPacketRecord *>(buffer[0].iov_base)->cseq_, fd, array_[fd].total_count_, reinterpret_cast<ObMySQLCommandPacketRecord *>(buffer[0].iov_base)->cmd_type_);
    }
    else if (count == 2)
    {
      //TBSYS_LOG(TRACE, "dispatch packet. socket_fd=%d, packet_index=%d, queue_total_size=%ld, cmd_type=%d, str=%.*s",
        //reinterpret_cast<ObMySQLCommandPacketRecord *>(buffer[0].iov_base)->cseq_, fd, array_[fd].total_count_, reinterpret_cast<ObMySQLCommandPacketRecord *>(buffer[0].iov_base)->cmd_type_, int32_t(buffer[1].iov_len), reinterpret_cast<char*>(buffer[1].iov_base));
    }
    else
    {
      //TBSYS_LOG(TRACE, "dispatch packet. socket_fd=%d, packet_index=%d, queue_total_size=%ld, cmd_type=%d, str=%.*s",
        //  reinterpret_cast<ObMySQLCommandPacketRecord *>(buffer[0].iov_base)->cseq_, fd, array_[fd].total_count_, reinterpret_cast<ObMySQLCommandPacketRecord *>(buffer[0].iov_base)->cmd_type_, int32_t(buffer[2].iov_len), reinterpret_cast<char*>(buffer[2].iov_base));

    }
  }
  return ret;
}

int PacketQueue::push(const QueryInfo& query_info, const bool read_only)
{
  int ret = OB_SUCCESS;

  struct iovec buffers[3];
  int count = 0;
  if (OB_SUCCESS == ret)
  {
    ret = get_iovect(&query_info, buffers, count);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get iovec. ret=%d", ret);
    }
    else
    {
      //print_iovect(buffers, count);
    }
  }
 UNUSED(read_only);
  if (OB_SUCCESS == ret)
  {
    ret = filter_packet(buffers, count);
    if (OB_DISCARD_PACKET == ret)
    {
      discart_count_ ++;
      //TBSYS_LOG(DEBUG, "discart packet. discart_count=%ld", discart_count_);
      //print_iovect(buffers, count);
    }
  }

  int fd = 0;
  //根据FD来选择队列
  if (OB_SUCCESS == ret)
  {
    ret = select_queue(fd, buffers, count);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to select queue. ret=%d", ret);
    }
    else
    {
    }
  }
  //push queue
  if (OB_SUCCESS == ret)
  {
    array_[fd].lock_.lock();
    int size = int(array_[fd].queue_.size());
    while (size >= MAX_COUNT)
    {
      array_[fd].lock_.unlock();
      usleep(3);
      array_[fd].lock_.lock();
      size = int(array_[fd].queue_.size());
    }
    if (size < MAX_COUNT)
    {
      array_[fd].queue_.push_back(query_info);
      array_[fd].count_ ++;
      array_[fd].total_count_++;
    }
    array_[fd].lock_.unlock();
  }
  if (OB_DISCARD_PACKET == ret)
  {
    ret = OB_SUCCESS;
  }
  return ret;
}

