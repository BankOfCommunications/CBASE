#include "load_define.h"
#include "load_manager.h"
#include "obmysql/packet/ob_mysql_command_packet.h"
#include "obmysql/ob_mysql_util.h"
using namespace oceanbase::common;
using namespace oceanbase::tools;
using namespace oceanbase::obmysql;

LoadManager::LoadManager()
{
  max_count_ = DEFAULT_COUNT;
  memset(&result_, 0, sizeof(result_));
}

LoadManager::~LoadManager()
{
}

void LoadManager::init(const int64_t max_count, const int64_t thread_count)
{
  max_count_ = max_count;
  load_queue_.init(thread_count);
}

void LoadManager::status(Statics & data) const
{
  data = result_;
}

int64_t LoadManager::size(void) const
{
  return load_queue_.size();
}

void LoadManager::set_finish(void)
{
  result_.share_info_.is_finish_ = true;
}

bool LoadManager::get_finish(void) const
{
  return result_.share_info_.is_finish_;
}

int LoadManager::push(const QueryInfo & query, const bool read_only, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  UNUSED(timeout);
  load_queue_.push(query, read_only);
  return ret;
}

int LoadManager::get_statement_id(const int socket_fd, uint32_t &statement_id)
{
  int ret = OB_SUCCESS;
  //int packet_fd = int (socket_fd % load_queue_.thread_count_ -1);
  int packet_fd = int (socket_fd % load_queue_.thread_count_);
  bool find = false;
  int retry_time = 0;
  while (!find && retry_time < 10)
  {
    ObList<QueryInfo>::iterator it = load_queue_.array_[packet_fd].queue_.begin();
    for (; it != load_queue_.array_[packet_fd].queue_.end(); ++it)
    {
      struct iovec iovect[2];
      int count = 0;
      ret = PacketQueue::get_iovect(&(*it), iovect, count);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to ge tiovect, ret=%d", ret);
        break;
      }
      ObMySQLCommandPacketRecord *record;
      record = reinterpret_cast<ObMySQLCommandPacketRecord *>(iovect[0].iov_base);
      if (record->cseq_ == socket_fd)
      {
        //if (record->cmd_type_ != COM_STMT_PREPARE
             if (record->obmysql_type_ != OBMYSQL_PREPARE)
        {
          TBSYS_LOG(ERROR, "input file error. the first packet in socket_df[%d] is not prepare_response. type=%d", socket_fd, record->obmysql_type_);
          ret = OB_ERROR;
          find = false;
        }
        else
        {
          //char *pos = reinterpret_cast<char*>(iovect[1].iov_base) + 1;
          //ObMySQLUtil::get_uint4(pos, statement_id);
          statement_id = uint32_t(record->stmt_id_);
          TBSYS_LOG(DEBUG, "get statement id =%d, socked_fd=%d", statement_id, socket_fd);
          find = true;
        }
        break;
      }
    }
    if (find)
    {
      break;
    }
    else
    {
      retry_time ++;
      usleep(100);
    }
  }
  if (find)
  {
  }
  else
  {
    TBSYS_LOG(WARN, "fail to get statement id. socket_fd=%d", socket_fd);
  }
  return ret;
}
void LoadManager::dump()
{
  for (int64_t i = 0; i < load_queue_.thread_count_; i++)
  {
    TBSYS_LOG(DEBUG, "queue index=%ld, packet count=%ld", i, load_queue_.array_[i].queue_.size());
  }
}
int LoadManager::pop(const int64_t pid, QueryInfo & query, const int64_t timeout)
{
  UNUSED(timeout);
  int64_t end_time = timeout + tbsys::CTimeUtil::getTime();
  int64_t size = 0;
  int ret = OB_ERROR;
  int64_t fd = pid;
  do
  {
    load_queue_.array_[fd].lock_.lock();
    size = load_queue_.array_[fd].queue_.size();
    if (size > 0)
    {
      load_queue_.array_[fd].queue_.pop_front(query);
      load_queue_.array_[fd].count_--;
      load_queue_.array_[fd].lock_.unlock();
      ret = OB_SUCCESS;
      TBSYS_LOG(DEBUG, "pop packet. packet queue index=%ld, remain_count=%ld, total_count=%ld",
      fd, load_queue_.array_[fd].count_, load_queue_.array_[fd].total_count_);

      break;
    }
    else
    {
      load_queue_.array_[fd].lock_.unlock();
      if (tbsys::CTimeUtil::getTime() > end_time)
      {
        break;
      }
    }
  } while (true);
  return ret;
}

void LoadManager::filter(void)
{
  atomic_inc(reinterpret_cast<uint64_t *>(&(result_.share_info_.filter_count_)));
}

void LoadManager::error(void)
{
  atomic_inc(reinterpret_cast<uint64_t *>(&(result_.compare_err_)));
}

void LoadManager::consume(const bool old, const bool succ, const int64_t data,
    const int64_t row, const int64_t cell, const int64_t time)
{
  ConsumeInfo & info = (false == old) ? result_.new_server_: result_.old_server_;
  atomic_inc(reinterpret_cast<uint64_t *>(&(info.consume_count_)));
  if (true == succ)
  {
    info.return_res_ += data;
    info.return_rows_ += row;
    info.return_cells_ += cell;
    ++info.succ_count_;
    info.succ_time_used_ += time;
  }
  else
  {
    ++info.err_count_;
    info.err_time_used_ += time;
  }
}

