#include <mysql.h>
#include <violite.h>
#include "load_manager.h"
#include "load_util.h"
#include "load_rpc.h"
#include "load_consumer.h"
#include "load_producer.h"
#include "obmysql/packet/ob_mysql_command_packet.h"
#include "obmysql/ob_mysql_util.h"
#include "common/utility.h"
using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::tools;
struct st_vio;
LoadConsumer::LoadConsumer()
{
  index_ = 0;
  manager_ = NULL;
}

LoadConsumer::~LoadConsumer()
{
}

void LoadConsumer::init(const ControlParam & param, LoadManager * manager)
{
  times_ = 0;
  count_ = 0;
  time_ = 0;
  param_ = param;
  manager_ = manager;
}

void LoadConsumer::run(tbsys::CThread* thread, void* arg)
{
  int ret = OB_SUCCESS;
  UNUSED(arg);
  UNUSED(thread);
  int64_t start_time= tbsys::CTimeUtil::getTime();
  int64_t begin = tbsys::CTimeUtil::getTime();
  int64_t end = tbsys::CTimeUtil::getTime();
  int64_t process_count = 0;
  if (NULL == manager_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check load manager failed:load[%p]", manager_);
  }
  else
  {
    TBSYS_LOG(DEBUG, "consumer thread start.");
    QueryInfo query;
    int64_t query_count = 0;
    int64_t execute_count = 0;
    int64_t prepare_count = 0;
    int64_t filter_count = 0;
    int64_t err_count = 0;
    int64_t tmp_count = 0;
    int64_t succ_count = 0;
    //=================
    int64_t tmp_query_count = 0;
    int64_t tmp_execute_count = 0;
    int64_t tmp_prepare_count = 0;
    int64_t tmp_filter_count = 0;
    int64_t tmp_err_count = 0;
    int64_t tmp_succ_count = 0;
    int64_t timeout = 1000;
    int64_t tid = index_;
    index_ ++;
    TBSYS_LOG(DEBUG, "thread = %ld index = %ld", syscall(__NR_gettid), tid);
    hash::ObHashMap<int, MYSQL> fd_connect;
    fd_connect.create(hash::cal_next_prime(512));
    hash::ObHashMap<std::pair<int, uint32_t>, uint32_t> stmt_map;
    stmt_map.create(hash::cal_next_prime(512));
    int64_t retry_times = 0;
    int64_t time = tbsys::CTimeUtil::getTime();
    while (!_stop)
    {
      ret = manager_->pop(tid, query, timeout);
      if (OB_SUCCESS == ret)
      {
        end = tbsys::CTimeUtil::getTime();
        time = tbsys::CTimeUtil::getTime();
        retry_times = 0;
        process_count ++;
        struct iovec iovect[3];
        int count = 0;
        ret = PacketQueue::get_iovect(&query, iovect, count);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to get iovect.");
        }
        else
        {
          //PacketQueue::print_iovect(iovect, count);
        }
        obmysql::ObMySQLCommandPacketRecord *record;
        record = reinterpret_cast<obmysql::ObMySQLCommandPacketRecord *>(iovect[0].iov_base);
        int cmd = record->cmd_type_;
        switch (cmd)
        {
          case COM_END:
            {
              if (record->obmysql_type_ == OBMYSQL_LOGIN)
              {
                TBSYS_LOG(DEBUG, "start to do login");
                ret = do_login(iovect, count, fd_connect);
              }
              else if (record->obmysql_type_ == OBMYSQL_LOGOUT)
              {
                TBSYS_LOG(DEBUG, "start to do logout");
                ret = do_logout(iovect, count, fd_connect);
              }
            }
            break;
          case COM_STMT_PREPARE:
            {
              if (record->obmysql_type_ == OBMYSQL_PREPARE)
              {
                TBSYS_LOG(DEBUG, "start to do prepare, prepare str=%s", reinterpret_cast<char*>(iovect[1].iov_base));
                prepare_count++;
                ret = do_com_prepare(iovect, count, fd_connect, stmt_map);
              }
              else
              {
                filter_count++;
                //nothing to do
              }
            }
            break;
          case COM_STMT_EXECUTE:
            {
              if (OBMYSQL_EXECUTE == record->obmysql_type_)
              {
                TBSYS_LOG(DEBUG, "start to do execute. prepare stmt =%s", reinterpret_cast<char*>(iovect[2].iov_base));
                execute_count++;
                ret = do_com_execute(iovect, count, fd_connect, stmt_map);
              }
              else
              {
                filter_count++;
                //nothing to do
              }
            }
            break;
          case COM_STMT_CLOSE:
            TBSYS_LOG(DEBUG, "start to do close stmt.");
            ret = do_com_close_stmt(iovect, count, fd_connect, stmt_map);
            break;
          case COM_QUERY:
            {
              TBSYS_LOG(DEBUG, "start to query: socket_fd=%d, str=%s",
                  (reinterpret_cast<ObMySQLCommandPacketRecord *>(iovect[0].iov_base))->cseq_, reinterpret_cast<char*>(iovect[1].iov_base));

              query_count++;
              ret = do_com_query(iovect, count, fd_connect);
            }
            break;
          default:
            TBSYS_LOG(DEBUG, "unsupport type. tpye=%d", cmd);
            break;
        }
        LoadProducer::destroy_request(query);
        if (OB_SUCCESS == ret)
        {
          succ_count ++;
        }
        else
        {
          err_count++;
        }
      }
      else
      {
        usleep(200);
        int64_t now = tbsys::CTimeUtil::getTime();
        if (now - time > 60 * 1000 * 1000)
        {
          TBSYS_LOG(DEBUG, "retry 1 minute, and fetch no packet. exit;");
          break;
        }
      }
      int64_t cur_time = tbsys::CTimeUtil::getTime();
      if (cur_time - start_time > 10 * 1000 * 1000)
      {
        int64_t qps = prepare_count-tmp_prepare_count + query_count - tmp_query_count + execute_count-tmp_execute_count;
        TBSYS_LOG(INFO, "statistic: thread_index=%ld, consumer count =%ld, query_count=%ld, prepare_count=%ld, execute_count=%ld, filter_count=%ld, err_count=%ld, total_count=%ld",
            tid, qps, query_count - tmp_query_count, prepare_count-tmp_prepare_count, execute_count-tmp_execute_count, filter_count-tmp_filter_count, err_count-tmp_err_count, succ_count-tmp_succ_count);
        tmp_count = process_count;
        start_time = cur_time;
        tmp_query_count = query_count;
        tmp_prepare_count = prepare_count;
        tmp_execute_count = execute_count;
        tmp_filter_count = filter_count;
        tmp_err_count = err_count;
        tmp_succ_count = succ_count;
      }
    }
  }
  count_ += process_count;
  time_ = (end - begin)/1000000;
  TBSYS_LOG(INFO, "consumer thread is stop!, count=%ld, total_time=%ld", process_count, (end - begin)/1000000);
}
void LoadConsumer::end()
{
}
int LoadConsumer::do_login(const struct iovec *iovect, const int count, hash::ObHashMap<int, MYSQL> &fd_connect)
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(DEBUG, "start to do login");
  UNUSED(count);
  obmysql::ObMySQLCommandPacketRecord *record;
  record = reinterpret_cast<obmysql::ObMySQLCommandPacketRecord *>(iovect[0].iov_base);

  if (NULL == iovect || record->obmysql_type_!= OBMYSQL_LOGIN)
  {
    TBSYS_LOG(WARN, "invalid argument. iovect is %p, login_type=%d", iovect, record->obmysql_type_);
    ret = OB_INVALID_ARGUMENT;
  }
  MYSQL my_;
  char host[128];
  int port = param_.new_server_.get_port();
  param_.new_server_.ip_to_string(host, 128);
  if (OB_SUCCESS == ret)
  {
    ret = fd_connect.get((reinterpret_cast<ObMySQLCommandPacketRecord *>(iovect[0].iov_base))->cseq_, my_);
    if (ret == -1 || hash::HASH_NOT_EXIST == ret)
    {
      TBSYS_LOG(DEBUG, "new connect need build. fd=%d", (reinterpret_cast<ObMySQLCommandPacketRecord *>(iovect[0].iov_base))->cseq_);
      if (0 != mysql_library_init(0, NULL, NULL))
      {
        ret = OB_ERROR;
        fprintf(stderr, "could not init mysql library\n");
      }
      else if (NULL == mysql_init(&my_))
      {
        fprintf(stderr, "failed to init libmysqlclient\n");
        ret = OB_ERROR;
        fprintf(stderr, "%s\n", mysql_error(&my_));
      }
      else if(NULL == mysql_real_connect(&my_, host, param_.user_name_, param_.password_, "test", port, NULL, 0))
      {
        fprintf(stderr, "failed to connect mysql server\n");
        ret = OB_ERROR;
        fprintf(stderr, "%s\n", mysql_error(&my_));
      }
      else
      {
        TBSYS_LOG(DEBUG, "build connect success! socket_fd=%d, pipe_fd lable=%d", record->cseq_, my_.net.vio->sd);
        ret = OB_SUCCESS;
        fd_connect.set(record->cseq_, my_);
      }
    }
    else
    {
      TBSYS_LOG(WARN, "mysql connect already in map. check it. fd=%d", record->cseq_);
      ret = OB_ERROR;
    }
  }
  return ret;
}

int LoadConsumer::do_logout(const struct iovec *iovect, const int count, hash::ObHashMap<int, MYSQL> &fd_connect)
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(DEBUG, "start to do logout");
  UNUSED(count);
  obmysql::ObMySQLCommandPacketRecord *record;
  record = reinterpret_cast<obmysql::ObMySQLCommandPacketRecord *>(iovect[0].iov_base);
  if (NULL == iovect || record->obmysql_type_!= OBMYSQL_LOGOUT)
  {
    TBSYS_LOG(WARN, "invalid argument. iovect is %p, login_type=%d", iovect, record->obmysql_type_);
    ret = OB_INVALID_ARGUMENT;
  }
  MYSQL my_;
  if (OB_SUCCESS == ret)
  {
    ret = fd_connect.get((reinterpret_cast<ObMySQLCommandPacketRecord *>(iovect[0].iov_base))->cseq_, my_);
    if (ret == -1 || hash::HASH_NOT_EXIST == ret)
    {
      TBSYS_LOG(DEBUG, "fail to find mysql connect. ret=%d", ret);
    }
    else if (ret == hash::HASH_EXIST)
    {
      ret = OB_SUCCESS;
      MYSQL tmp_my_;
      TBSYS_LOG(DEBUG, "erase hash item in fd_connect. cseq=%d", record->cseq_);
      fd_connect.erase(record->cseq_, &tmp_my_);
      //double check
      if (hash::HASH_EXIST == fd_connect.get(record->cseq_, tmp_my_))
      {
        TBSYS_LOG(ERROR, "fail to earse hash item in fd_connect. cseq=%d", record->cseq_);
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "logout, socket_fd=%d, pipe_fd=%d", record->cseq_, my_.net.vio->sd);
   // mysql_close(&my_);
  }
  return ret;
}
int LoadConsumer::do_com_prepare(const struct iovec *iovect, const int count,
    hash::ObHashMap<int, MYSQL> &fd_connect,
    hash::ObHashMap<std::pair<int, uint32_t>, uint32_t> &statement_map)
{
  int ret = OB_SUCCESS;
  UNUSED(iovect);
  UNUSED(count);
  UNUSED(fd_connect);
  UNUSED(statement_map);
  obmysql::ObMySQLCommandPacketRecord *record;
  record = reinterpret_cast<obmysql::ObMySQLCommandPacketRecord *>(iovect[0].iov_base);
  UNUSED(count);
  if (NULL == iovect)
  {
    TBSYS_LOG(WARN, "invalid argument. iovect is null");
    ret = OB_INVALID_ARGUMENT;
  }
  MYSQL my_;
  char host[128];
  int port = param_.new_server_.get_port();
  param_.new_server_.ip_to_string(host, 128);

  if (OB_SUCCESS == ret)
  {
    ret = fd_connect.get(record->cseq_, my_);
    if (ret == -1 || hash::HASH_NOT_EXIST == ret)
    {
      TBSYS_LOG(DEBUG, "new connect need build. fd=%d", record->cseq_);

      if (0 != mysql_library_init(0, NULL, NULL))
      {
        ret = OB_ERROR;
        fprintf(stderr, "could not init mysql library\n");
      }
      else if (NULL == mysql_init(&my_))
      {
        fprintf(stderr, "failed to init libmysqlclient\n");
        ret = OB_ERROR;
        fprintf(stderr, "%s\n", mysql_error(&my_));
      }
      else if(NULL == mysql_real_connect(&my_, host, param_.user_name_, param_.password_, "test", port, NULL, 0))
      {
        fprintf(stderr, "failed to connect mysql server\n");
        ret = OB_ERROR;
        fprintf(stderr, "%s\n", mysql_error(&my_));
      }
      else
      {
        TBSYS_LOG(DEBUG, "prepare: socket_fd=%d, pipe_fd=%d, not find in map. str=%s", record->cseq_, my_.net.vio->sd, reinterpret_cast<char*>(iovect[1].iov_base));
        ret = OB_SUCCESS;
        fd_connect.set(record->cseq_, my_);
      }
    }
    else
    {
      TBSYS_LOG(DEBUG, "prepare: socket_fd=%d, pipe_fd=%d, find in map. str=%.*s", record->cseq_, my_.net.vio->sd, int32_t(iovect[1].iov_len), reinterpret_cast<char*>(iovect[1].iov_base));
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "write prepare packet to fd, pipe_fd=%d", my_.net.vio->sd);
   ret = write_packet_to_fd(my_.net.vio->sd, iovect ,count);
   if (OB_SUCCESS != ret)
   {
     TBSYS_LOG(WARN, "fail to write prepare statement to fd, ret=%d", ret);
   }
  }
  uint32_t real_statement_id = 0;
  uint32_t replay_statement_id = 0;
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "start to read prepare result.fd=%d", my_.net.vio->sd);
    ret = read_prepare_result(my_.net.vio->sd, real_statement_id);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get statememt id from fd. ret=%d", ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "get statement id for real prepare. real statement_id=%d", real_statement_id);
    }
  }
  if (OB_SUCCESS == ret)
  {
    //get replay statement id from replayfile
    replay_statement_id = (uint32_t)record->stmt_id_;
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get statement id. socket_fd=%d,ret=%d", record->cseq_, ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "get statement id for replay process. real statement id=%d, replay statement_id=%d", real_statement_id, replay_statement_id);
    }
  }
  if (OB_SUCCESS == ret)
  {
    //save statement id
    std::pair<int, uint32_t> tmp;
    tmp = std::make_pair(record->cseq_, replay_statement_id);
    statement_map.set(std::make_pair(record->cseq_, replay_statement_id), real_statement_id);
    TBSYS_LOG(DEBUG, "save fd: socket id=%d, replay_statement_id=%d, real_statement_id=%d",
        record->cseq_, replay_statement_id, real_statement_id);
  }
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "prepare success. socket_id=%d, replay_statement_id=%d, real_statement_id=%d, str=%.*s",
        record->cseq_, replay_statement_id, real_statement_id, int32_t(iovect[1].iov_len), reinterpret_cast<char*>(iovect[1].iov_base));
  }
  return ret;
}

int LoadConsumer::do_com_execute(const struct iovec *iovect, const int count,
    hash::ObHashMap<int, MYSQL> &fd_connect,
    hash::ObHashMap<std::pair<int, uint32_t>, uint32_t> &statement_map)
{
  int ret = OB_SUCCESS;
  UNUSED(iovect);
  UNUSED(count);
  UNUSED(fd_connect);
  MYSQL my_;
  ObMySQLCommandPacketRecord *record;
  record = reinterpret_cast<ObMySQLCommandPacketRecord *>(iovect[0].iov_base);
  uint32_t statement_id = 0;
  uint32_t replay_statement_id = 0;
  char *pos = reinterpret_cast<char*>(iovect[1].iov_base);
  if (OB_SUCCESS == ret)
  {
    ObMySQLUtil::get_uint4(pos, replay_statement_id);
    TBSYS_LOG(DEBUG, "get content from execute packet.replay_statement_id=%d",
        replay_statement_id);
    ret = statement_map.get(std::make_pair(record->cseq_, replay_statement_id), statement_id);
    if (ret == -1 || hash::HASH_NOT_EXIST == ret)
    {
      TBSYS_LOG(DEBUG, "fail to get statement id. socket_fd=%d, replay statement id = %d, retry to prepare first",
          record->cseq_, replay_statement_id);
      ret = OB_FIND_OUT_OF_RANGE;
    }
    else if (ret == hash::HASH_EXIST)
    {
      ret = OB_SUCCESS;
      TBSYS_LOG(DEBUG, "get statement_id in map, socket_fd=%d, replay statement id = %d, real statement id =%d, prepare stmt=%s",
          record->cseq_, replay_statement_id, statement_id, reinterpret_cast<char*>(iovect[2].iov_base));
    }
  }
  if (OB_FIND_OUT_OF_RANGE == ret && record->obmysql_type_ == OBMYSQL_EXECUTE)
  {
    struct iovec buffers[2];
    ObMySQLCommandPacketRecord new_record;
    new_record.pkt_length_ = uint32_t(iovect[2].iov_len + 1);
    new_record.pkt_seq_ = 0;
    new_record.cseq_ = record->cseq_;
    new_record.stmt_id_ = replay_statement_id;
    new_record.cmd_type_ = COM_STMT_PREPARE;
    buffers[0].iov_base = &new_record;
    buffers[0].iov_len = sizeof(new_record);
    buffers[1].iov_base = iovect[2].iov_base;
    buffers[1].iov_len = iovect[2].iov_len;

    TBSYS_LOG(DEBUG, "prepare statement_id not found, need to prepare first, prepare_str=%s, str=%s",
        reinterpret_cast<char*>(buffers[1].iov_base), reinterpret_cast<char*>(iovect[2].iov_base));
    ret = do_com_prepare(buffers, 2, fd_connect, statement_map);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to do prepare. ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = fd_connect.get(record->cseq_, my_);
    if (ret == -1 || hash::HASH_NOT_EXIST == ret)
    {
      TBSYS_LOG(WARN, "fail to find my instance. record_fd=%d, ret=%d", record->cseq_, ret);
      ret = OB_ERROR;
    }
    else if (ret == hash::HASH_EXIST)
    {
      ret = OB_SUCCESS;
    }
  }
  pos = reinterpret_cast<char*>(iovect[1].iov_base);
  if (OB_SUCCESS == ret)
  {
    ObMySQLUtil::get_uint4(pos, replay_statement_id);
    ret = statement_map.get(std::make_pair(record->cseq_, replay_statement_id), statement_id);
    if (ret == -1 || hash::HASH_NOT_EXIST == ret)
    {
      TBSYS_LOG(ERROR, "fail to get statement id. socket_fd=%d, replay statement id = %d",
          record->cseq_, replay_statement_id);
      ret = OB_FIND_OUT_OF_RANGE;
    }
    else if (ret == hash::HASH_EXIST)
    {
      ret = OB_SUCCESS;
      TBSYS_LOG(DEBUG, "get statement_id, socket_fd=%d, replay statement id = %d, real statement id =%d, prepare stmt = %s",
          record->cseq_, replay_statement_id, statement_id, reinterpret_cast<char*>(iovect[2].iov_base));
    }
  }

  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "write execute packet to fd. fife_fd=%d, stmt=%s", my_.net.vio->sd, reinterpret_cast<char*>(iovect[2].iov_base));
    ret = write_stmt_packet_to_fd(my_.net.vio->sd, iovect, count, statement_id);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to write stmt packet to fd. ret=%d", ret);
    }
  }
  //get response
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "read execute result, fife_fd=%d", my_.net.vio->sd);
    ret = read_execute_result(my_.net.vio->sd);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to read execute result from fd, ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "execute success. satement_id=%d, replay_statement_id=%d, prepare stmt=%s",
        statement_id, replay_statement_id, reinterpret_cast<char*>(iovect[2].iov_base));
  }
  return ret;
}

int LoadConsumer::do_com_close_stmt(const struct iovec *iovect, const int count, hash::ObHashMap<int, MYSQL> &fd_connect,
    hash::ObHashMap<std::pair<int, uint32_t>, uint32_t> &statement_map)
{
  int ret = OB_SUCCESS;
  UNUSED(iovect);
  UNUSED(count);
  UNUSED(fd_connect);
  MYSQL my_;
  ObMySQLCommandPacketRecord *record;
  record = reinterpret_cast<ObMySQLCommandPacketRecord *>(iovect[0].iov_base);
  if (OB_SUCCESS == ret)
  {
    ret = fd_connect.get(record->cseq_, my_);
    if (ret == -1 || hash::HASH_NOT_EXIST == ret)
    {
      TBSYS_LOG(DEBUG, "fail to find my instance. ret=%d", ret);
      ret = OB_ERROR;
    }
    else if (ret == hash::HASH_EXIST)
    {
      ret = OB_SUCCESS;
    }
  }
  //get replay statement id
  uint32_t statement_id = 0;
  uint32_t replay_statement_id = 0;
  char *pos = reinterpret_cast<char*>(iovect[1].iov_base);
  if (OB_SUCCESS == ret)
  {
    ObMySQLUtil::get_uint4(pos, replay_statement_id);
    ret = statement_map.get(std::make_pair(record->cseq_, replay_statement_id), statement_id);
    if (ret == -1 || hash::HASH_NOT_EXIST == ret)
    {
      TBSYS_LOG(DEBUG, "fail to find statement_id. ret=%d", ret);
      ret = OB_ERROR;
    }
    else if (ret == hash::HASH_EXIST)
    {
      ret = OB_SUCCESS;
      uint32_t tmp_statement_id = statement_id;
      TBSYS_LOG(DEBUG, "get replay statement id = %d, real statement id =%d",
          replay_statement_id, statement_id);
      TBSYS_LOG(DEBUG, "earse hash item in statement_map, cseq=%d, replay_statemet_id=%d", record->cseq_, replay_statement_id);
      statement_map.erase(std::make_pair(record->cseq_, replay_statement_id), &tmp_statement_id);
      //double check earse
      if (hash::HASH_EXIST == statement_map.get(std::make_pair(record->cseq_, replay_statement_id), tmp_statement_id))
      {
        TBSYS_LOG(ERROR, "fail to earse hash item in statement_map. cseq=%d, replay_statement_id=%d", record->cseq_, replay_statement_id);
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "write close stmt packet to fd. fd=%d", my_.net.vio->sd);
    ret = write_stmt_packet_to_fd(my_.net.vio->sd, iovect, count, statement_id);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to write stmt packet to fd. ret=%d, statement_id=%d", ret, statement_id);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = read_close_stmt_result(my_.net.vio->sd);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to read close stmt result. socket fd=%d, ret=%d", my_.net.vio->sd, ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "close stmt success.statement_id=%d, replay statement_id=%d",
        statement_id, replay_statement_id);
  }
  return ret;
}
int LoadConsumer::write_stmt_packet_to_fd(int fd, const struct iovec *iovect, const int count, const uint32_t statement_id)
{
  int ret = OB_SUCCESS;
  UNUSED(count);
  int64_t start_pos = 0;
  char buffer[OB_MAX_PACKET_LENGTH];
  int64_t length = OB_MAX_PACKET_LENGTH;
  uint32_t pkt_len = uint32_t(iovect[1].iov_len) + 1;
  ObMySQLCommandPacketRecord *record;
  record = reinterpret_cast<ObMySQLCommandPacketRecord *>(iovect[0].iov_base);
  //build new execute buff
  if (OB_SUCCESS != (ret = ObMySQLUtil::store_int3(buffer, length, pkt_len, start_pos)))
  {
    TBSYS_LOG(ERROR, "serialize packet header size failed, buffer=%p, buffer length=%ld, packet length=%d, pos=%ld",
        buffer, length, pkt_len, start_pos);
  }
  else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int1(buffer, length, record->pkt_seq_, start_pos)))
  {
    TBSYS_LOG(ERROR, "serialize packet header seq failed, buffer=%p, buffer length=%ld, seq number=%u, pos=%ld",
        buffer, length, record->pkt_seq_, start_pos);
  }
  else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int1(buffer, length, record->cmd_type_,start_pos)))
  {
    TBSYS_LOG(ERROR, "serialize packet command failed, buffer=%p, buffer length=%ld, cmd_type=%d, pos=%ld", buffer, length, record->cmd_type_, start_pos);
  }
  else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int4(buffer, length, statement_id, start_pos)))
  {
    TBSYS_LOG(ERROR, "serialize statement id failed. buffer=%p, buffer_length=%ld, statement_id=%d, pso=%ld", buffer, length, statement_id, start_pos);
  }
  else
  {
    char *pos = reinterpret_cast<char*>(iovect[1].iov_base) + 4;
    memcpy(buffer + start_pos, pos, iovect[1].iov_len - 4);
    write(fd, buffer, OB_MYSQL_PACKET_HEADER_SIZE + pkt_len);
    TBSYS_LOG(DEBUG, "write stmp packet info to fd success, packet seq=%d", record->pkt_seq_);
    hex_dump(buffer, (int32_t)OB_MYSQL_PACKET_HEADER_SIZE + pkt_len);
  }
  return ret;
}
int LoadConsumer::write_packet_to_fd(int fd, const struct iovec *iovect, const int count)
{
  int ret = OB_SUCCESS;
  UNUSED(count);
  char buffer[OB_MAX_PACKET_LENGTH];
  int64_t length = OB_MAX_PACKET_LENGTH;
  uint32_t pkt_len = uint32_t(iovect[1].iov_len) + 1;
  TBSYS_LOG(DEBUG, "packet length=%d", pkt_len);
  int64_t start_pos = 0;
  ObMySQLCommandPacketRecord *record;
  record = reinterpret_cast<ObMySQLCommandPacketRecord *>(iovect[0].iov_base);
  if (OB_SUCCESS == ret)
  {
    //TBSYS_LOG(DEBUG, "start to serialize to buffer.");
    if (OB_SUCCESS != (ret = ObMySQLUtil::store_int3(buffer, length, pkt_len, start_pos)))
    {
      TBSYS_LOG(ERROR, "serialize packet header size failed, buffer=%p, buffer length=%ld, packet length=%d, pos=%ld",
          buffer, length, pkt_len, start_pos);
    }
    else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int1(buffer, length, record->pkt_seq_, start_pos)))
    {
      TBSYS_LOG(ERROR, "serialize packet header seq failed, buffer=%p, buffer length=%ld, seq number=%u, pos=%ld",
          buffer, length, record->pkt_seq_, start_pos);
    }
    else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int1(buffer, length, record->cmd_type_,start_pos)))
    {
      TBSYS_LOG(ERROR, "serialize packet command failed, buffer=%p, buffer length=%ld, cmd_type=%d, pos=%ld", buffer, length, record->cmd_type_, start_pos);
    }
    else
    {
      memcpy(buffer + start_pos, iovect[1].iov_base, iovect[1].iov_len);
      int64_t len = (int64_t)write(fd, buffer, OB_MYSQL_PACKET_HEADER_SIZE + pkt_len);
      TBSYS_LOG(DEBUG, "write:  socket_fd=%d, pipe_fd=%d, write_len=%ld, packet_seq=%d, str=%s", record->cseq_, fd, len, record->pkt_seq_, reinterpret_cast<char*>(iovect[1].iov_base));
      hex_dump(buffer, (int32_t)OB_MYSQL_PACKET_HEADER_SIZE + pkt_len);
    }
  }
  return ret;
}
int LoadConsumer::read_packet(const int fd, char* buf, const int64_t buf_length)
{
  int ret = OB_SUCCESS;
  //read packet header
  ret = read_fd(fd, buf, OB_MYSQL_PACKET_HEADER_SIZE, buf_length);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to read packet header from fd. fd=%d", fd);
  }
  //get packet length
  ObMySQLPacketHeader header;
  char *pos = buf;
  if (OB_SUCCESS == ret)
  {
    ObMySQLUtil::get_uint3(pos, header.length_);
    ObMySQLUtil::get_uint1(pos, header.seq_);
  }
  //read packet content
  if (OB_SUCCESS == ret)
  {
    ret = read_fd(fd, buf + OB_MYSQL_PACKET_HEADER_SIZE, header.length_, buf_length - OB_MYSQL_PACKET_HEADER_SIZE);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to read packet content from fd. fd=%d, content length=%d, seq=%d",
          fd, header.length_, header.seq_);
    }
  }
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "dump:read packet from fd. fd=%d", fd);
    hex_dump(buf, (int32_t)(OB_MYSQL_PACKET_HEADER_SIZE + header.length_));
  }
  return ret;
}
int LoadConsumer::read_fd(const int fd, char* buf, const int64_t size, const int64_t buf_length)
{
  int ret = OB_SUCCESS;
  if (size > buf_length || NULL == buf || fd < 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid argument. buf is %p, fd = %d, length=%ld, size=%ld", buf, fd, buf_length, size);
  }
  int64_t count = 0;
  int64_t read_size = 0;
  int64_t remain_size = size;
  if (OB_SUCCESS == ret)
  {
    //TBSYS_LOG(DEBUG, "read_time =%ld, read size =%ld", times_, size);
    times_++;
    while (true)
    {
      count = read(fd, buf + read_size, remain_size);
      read_size += count;
      if (count == remain_size)
      {
        break;
      }
      remain_size = remain_size - count;
    }
  }
  return ret;
}
int LoadConsumer::read_close_stmt_result(const int fd)
{
  int ret = OB_SUCCESS;
  UNUSED(fd);
  TBSYS_LOG(DEBUG, "No response is sent back to the client.");
  return ret;
}
int LoadConsumer::read_execute_result(const int fd)
{
  int ret = OB_SUCCESS;
  int read_ret = OB_SUCCESS;
  bool is_finish = false;
  //first packet
  ObMySQLPacketHeader header;
  char buffer[OB_MAX_PACKET_LENGTH];
  read_ret = read_packet(fd, buffer, OB_MAX_PACKET_LENGTH);
  //read_ret = read_fd(fd, buffer, OB_MYSQL_PACKET_HEADER_SIZE, OB_MAX_PACKET_LENGTH);
  char *pos = buffer;
  ObMySQLUtil::get_uint3(pos, header.length_);
  ObMySQLUtil::get_uint1(pos, header.seq_);
  TBSYS_LOG(DEBUG, "packet length=%d, packet seq=%d", header.length_, header.seq_);
  //read_ret = read_fd(fd, buffer, 1, OB_MAX_PACKET_LENGTH);
  //pos = buffer;
  uint8_t result_type = 0;
  ObMySQLUtil::get_uint1(pos, result_type);
  CountType column_count;
  int64_t count = 0;
  if (result_type == 0x00 || result_type == 0xff)
  {
    is_finish = true;
    //read_fd(fd, buffer, header.length_ - 1, OB_MAX_PACKET_LENGTH);
    TBSYS_LOG(DEBUG, "result type is ok, or err. result_type=%d", result_type);
  }
  else if (result_type < 0xfb)
  {
    column_count.value_32 = (int32_t)result_type;
    count = result_type;
   //read_fd(fd, buffer, header.length_ - 1, OB_MAX_PACKET_LENGTH);
  }
  else if (result_type == 0xfc)
  {
    //read_fd(fd, buffer, 2, OB_MAX_PACKET_LENGTH);
    //pos = buffer;
    ObMySQLUtil::get_uint2(pos,column_count.value_16);
    count = column_count.value_16;
    //read_fd(fd, buffer, header.length_ - 3, OB_MAX_PACKET_LENGTH);
  }
  else if (result_type == 0xfd)
  {
    //read_fd(fd, buffer, 3, OB_MAX_PACKET_LENGTH);
    //pos = buffer;
    ObMySQLUtil::get_uint3(pos, column_count.value_32);
    count = column_count.value_32;
    //read_fd(fd, buffer, header.length_ - 4, OB_MAX_PACKET_LENGTH);
  }
  else if (result_type == 0xfe)
  {
    //read_fd(fd, buffer, 8, OB_MAX_PACKET_LENGTH);
    //pos = buffer;
    ObMySQLUtil::get_uint8(pos, column_count.value_64);
    count = column_count.value_64;
    //read_fd(fd, buffer, header.length_ - 9, OB_MAX_PACKET_LENGTH);
  }
  //columndefinition columns
  if (!is_finish && OB_SUCCESS == ret)
  {
    for (int64_t i = 0; i < count; i++)
    {
      read_packet(fd, buffer, OB_MAX_PACKET_LENGTH);
      //read_fd(fd, buffer, OB_MYSQL_PACKET_HEADER_SIZE, OB_MAX_PACKET_LENGTH);
      pos = buffer;
      ObMySQLUtil::get_uint3(pos, header.length_);
      ObMySQLUtil::get_uint1(pos, header.seq_);
      TBSYS_LOG(DEBUG, "packet length=%d, packet seq=%d", header.length_, header.seq_);
      //read_fd(fd, buffer, header.length_, OB_MAX_PACKET_LENGTH);
      TBSYS_LOG(DEBUG, "column definition [%ld], str=%s, seq=%d", i, buffer + 1, header.seq_);
    }
  }
  //eof
  if (OB_SUCCESS == ret && !is_finish && count > 0)
  {
    read_packet(fd, buffer, OB_MAX_PACKET_LENGTH);
    //read_fd(fd, buffer, OB_MYSQL_PACKET_HEADER_SIZE, OB_MAX_PACKET_LENGTH);
    char * pos = buffer;
    ObMySQLUtil::get_uint3(pos, header.length_);
    ObMySQLUtil::get_uint1(pos, header.seq_);
    TBSYS_LOG(DEBUG, "packet length=%d, packet seq=%d", header.length_, header.seq_);
    //read_fd(fd, buffer, header.length_, OB_MAX_PACKET_LENGTH);
    TBSYS_LOG(DEBUG, "read execute result eof. length=%d, seq=%d", header.length_, header.seq_);
  }
  // ProtocolBinary::ResultsetRow
  //eof
  if (OB_SUCCESS == ret && !is_finish)
  {
    while(true)
    {
      read_ret = read_packet(fd, buffer, OB_MAX_PACKET_LENGTH);
      //read_ret = read_fd(fd, buffer, OB_MYSQL_PACKET_HEADER_SIZE, OB_MAX_PACKET_LENGTH);
      if (read_ret != OB_SUCCESS)
      {
        break;
      }
      TBSYS_LOG(DEBUG, "packet length=%d, packet seq=%d", header.length_, header.seq_);
      pos = buffer;
      ObMySQLUtil::get_uint3(pos, header.length_);
      ObMySQLUtil::get_uint1(pos, header.seq_);
      TBSYS_LOG(DEBUG, "packet header: length=%d, seq=%d,", header.length_, header.seq_);
      //read_ret = read_fd(fd, buffer, 1, OB_MAX_PACKET_LENGTH);
      //if (read_ret != OB_SUCCESS)
      //{
        //break;
      //}
      //pos = buffer;
      ObMySQLUtil::get_uint1(pos, result_type);
      TBSYS_LOG(DEBUG, "result type: type=%d", result_type);
      //read_ret = read_fd(fd, buffer, header.length_ - 1, OB_MAX_PACKET_LENGTH);
      //if (read_ret != OB_SUCCESS)
      //{
      //  break;
      //}
      TBSYS_LOG(DEBUG, "resultset: type=%d, length=%d, seq=%d, str=%s", result_type, header.length_, header.seq_, buffer);
      if (result_type == 0xfe)
      {
        TBSYS_LOG(DEBUG, "end of the read process.");
        is_finish = true;
        break;
      }
    }
    if (read_ret != OB_SUCCESS)
    {
      ret = read_ret;
      hex_dump(buffer, OB_MYSQL_PACKET_HEADER_SIZE);
    }
  }
  return ret;
}
int LoadConsumer::read_prepare_result(const int fd, uint32_t &statement_id)
{
  int ret = OB_SUCCESS;
  //first packet
  ObMySQLPacketHeader header;
  char buffer[OB_MAX_PACKET_LENGTH];
  read_packet(fd, buffer, OB_MAX_PACKET_LENGTH);
  //read_fd(fd, buffer, OB_MYSQL_PACKET_HEADER_SIZE, OB_MAX_PACKET_LENGTH);
  char * pos = buffer;
  ObMySQLUtil::get_uint3(pos, header.length_);
  ObMySQLUtil::get_uint1(pos, header.seq_);
  TBSYS_LOG(DEBUG, "read prepare reuslt, fd=%d, packet length=%d, packet seq=%d", fd, header.length_, header.seq_);
  uint8_t status = 0;  //1
  //int statement_id = 0;  //4
  uint16_t column_count = 0; //2
  uint16_t param_count = 0;  //2
  //reserved_1 (1)  //warning_count (2)
  //read_fd(fd, buffer, header.length_, OB_MAX_PACKET_LENGTH);
  //pos = buffer;
  ObMySQLUtil::get_uint1(pos, status);
  ObMySQLUtil::get_uint4(pos, statement_id);
  ObMySQLUtil::get_uint2(pos, column_count);
  ObMySQLUtil::get_uint2(pos, param_count);
  TBSYS_LOG(DEBUG, "read prepare prarm: pipe_fd=%d, total_lenght=%d, status=%d, statement_id=%d, column_count=%d, param_count=%d", fd, header.length_, status, statement_id, column_count, param_count);
  if (status == 0)
  {
    // param group
    for (int i = 0; i < param_count; i++)
    {
      read_packet(fd, buffer, OB_MAX_PACKET_LENGTH);
      //read_fd(fd, buffer, OB_MYSQL_PACKET_HEADER_SIZE, OB_MAX_PACKET_LENGTH);
      char * pos = buffer;
      ObMySQLUtil::get_uint3(pos, header.length_);
      ObMySQLUtil::get_uint1(pos, header.seq_);
      TBSYS_LOG(DEBUG, "read prepare param group[%d]: packet length=%d, packet seq=%d", i, header.length_, header.seq_);
      //read_fd(fd, buffer, header.length_, OB_MAX_PACKET_LENGTH);
    }
    //eof
    if (param_count > 0)
    {
      read_packet(fd, buffer, OB_MAX_PACKET_LENGTH);
      //read_fd(fd, buffer, OB_MYSQL_PACKET_HEADER_SIZE, OB_MAX_PACKET_LENGTH);
      pos = buffer;
      ObMySQLUtil::get_uint3(pos, header.length_);
       ObMySQLUtil::get_uint1(pos, header.seq_);
        TBSYS_LOG(DEBUG, "read prepare eof: packet length=%d, packet seq=%d", header.length_, header.seq_);
      //read_fd(fd, buffer, header.length_, OB_MAX_PACKET_LENGTH);
    }
    //column group
    for (int i = 0; i < column_count; i++)
    {
      read_packet(fd, buffer, OB_MAX_PACKET_LENGTH);
      //read_fd(fd, buffer, OB_MYSQL_PACKET_HEADER_SIZE, OB_MAX_PACKET_LENGTH);
      char * pos = buffer;
      ObMySQLUtil::get_uint3(pos, header.length_);
      ObMySQLUtil::get_uint1(pos, header.seq_);
      TBSYS_LOG(DEBUG, "read prepare column group[%d]:packet length=%d, packet seq=%d", i, header.length_, header.seq_);
      //read_fd(fd, buffer, header.length_, OB_MAX_PACKET_LENGTH);
    }
    if (column_count > 0)
    {
      //eof
      read_packet(fd, buffer, OB_MAX_PACKET_LENGTH);
      //read_fd(fd, buffer, OB_MYSQL_PACKET_HEADER_SIZE, OB_MAX_PACKET_LENGTH);
      pos = buffer;
      ObMySQLUtil::get_uint3(pos, header.length_);
      ObMySQLUtil::get_uint1(pos, header.seq_);
      TBSYS_LOG(DEBUG, "read prepare eof, packet length=%d, packet seq=%d", header.length_, header.seq_);
      //read_fd(fd, buffer, header.length_, OB_MAX_PACKET_LENGTH);
    }
  }
  else
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "prepare failed. state=%d, pipe_fd=%d, statement_id=%d, str=%s",
        status, fd, statement_id, buffer);
  }
  return ret;
}

int LoadConsumer::read_query_result(const int fd)
{
  int ret = OB_SUCCESS;
  bool is_finish = false;
  //首先读packethead,4个字节
  ObMySQLPacketHeader header;
  uint8_t result_type;
  char buffer[OB_MAX_PACKET_LENGTH];
  read_packet(fd, buffer, OB_MAX_PACKET_LENGTH);
  //read_fd(fd, buffer, OB_MYSQL_PACKET_HEADER_SIZE, OB_MAX_PACKET_LENGTH);
  char *pos = buffer;
  ObMySQLUtil::get_uint3(pos, header.length_);
  ObMySQLUtil::get_uint1(pos, header.seq_);
  TBSYS_LOG(DEBUG, "packet length=%d, packet seq=%d", header.length_, header.seq_);
  //read_fd(fd, buffer, 1, OB_MAX_PACKET_LENGTH);
  //pos = buffer;
  ObMySQLUtil::get_uint1(pos, result_type);
  CountType field_count;
  int64_t count = 0;
  if (result_type == 0x00 || result_type == 0xff)
  {
    is_finish = true;
    //read_fd(fd, buffer, header.length_ - 1, OB_MAX_PACKET_LENGTH);
    TBSYS_LOG(DEBUG, "result: type is %d, pipe fd=%d, 0x00 is ok, and 0xff is error, finish read result.", result_type, fd);
  }
  else if (result_type == 0Xfb)
  {
    is_finish = true;
     //read_fd(fd, buffer, header.length_ - 1, OB_MAX_PACKET_LENGTH);
    TBSYS_LOG(DEBUG, "not support result type. result_type=%d", result_type);
  }
  else if (result_type < 0xfb)
  {
    count = result_type;
    //read_fd(fd, buffer, header.length_ - 1, OB_MAX_PACKET_LENGTH);
  }
  else if (result_type == 0xfc)
  {
    //read_fd(fd, buffer, 2, OB_MAX_PACKET_LENGTH);
    //pos = buffer;
    ObMySQLUtil::get_uint2(pos, field_count.value_16);
    count = field_count.value_16;
    //read_fd(fd, buffer, header.length_ - 3, OB_MAX_PACKET_LENGTH);
  }
  else if (result_type == 0xfd)
  {
    //read_fd(fd, buffer, 3, OB_MAX_PACKET_LENGTH);
    //pos = buffer;
    ObMySQLUtil::get_uint3(pos, field_count.value_32);
    count = field_count.value_32;
    //read_fd(fd, buffer, header.length_ - 4, OB_MAX_PACKET_LENGTH);
  }
  else if (result_type == 0xfe)
  {
    //read_fd(fd, buffer, 8, OB_MAX_PACKET_LENGTH);
    //pos = buffer;
    ObMySQLUtil::get_uint8(pos, field_count.value_64);
    count = field_count.value_64;
    //read_fd(fd, buffer, header.length_ - 9, OB_MAX_PACKET_LENGTH);
  }
  TBSYS_LOG(DEBUG, "is_finish =%s, ret=%d, column_count=%ld", is_finish?"true":"false", ret, count);
  if (is_finish)
  {
    ret = OB_SUCCESS;
  }
  if (OB_SUCCESS == ret && !is_finish)
  {
    //fields
    for (int32_t i = 0; i < count; i++)
    {
      read_packet(fd, buffer, OB_MAX_PACKET_LENGTH);
      //read_fd(fd, buffer, OB_MYSQL_PACKET_HEADER_SIZE, OB_MAX_PACKET_LENGTH);
      pos = buffer;
      ObMySQLUtil::get_uint3(pos, header.length_);
      ObMySQLUtil::get_uint1(pos, header.seq_);
      TBSYS_LOG(DEBUG, "packet length=%d, packet seq=%d", header.length_, header.seq_);
      //read_fd(fd, buffer, header.length_, OB_MAX_PACKET_LENGTH);
      TBSYS_LOG(DEBUG, "column define[%d]: %s", i, buffer + 1);
    }
  }
  //eof
  if (OB_SUCCESS == ret && !is_finish)
  {
    read_packet(fd, buffer, OB_MAX_PACKET_LENGTH);
    //read_fd(fd, buffer, OB_MYSQL_PACKET_HEADER_SIZE, OB_MAX_PACKET_LENGTH);
    pos = buffer;
    ObMySQLUtil::get_uint3(pos, header.length_);
    ObMySQLUtil::get_uint1(pos, header.seq_);
    TBSYS_LOG(DEBUG, "eof, packet length=%d, packet seq=%d", header.length_, header.seq_);
    //pos = buffer;
    //read_fd(fd, buffer, 1, OB_MAX_PACKET_LENGTH);
    ObMySQLUtil::get_uint1(pos, result_type);
    //read_fd(fd, buffer, header.length_ - 1, OB_MAX_PACKET_LENGTH);
  }
  //rows
  if (OB_SUCCESS == ret && !is_finish)
  {
    int64_t i = 0;
    while(true)
    {
      read_packet(fd, buffer, OB_MAX_PACKET_LENGTH);
      //read_fd(fd, buffer, OB_MYSQL_PACKET_HEADER_SIZE, OB_MAX_PACKET_LENGTH);
      pos = buffer;
      ObMySQLUtil::get_uint3(pos, header.length_);
      ObMySQLUtil::get_uint1(pos, header.seq_);
      TBSYS_LOG(DEBUG, "packet length=%d, packet seq=%d", header.length_, header.seq_);
      //read_fd(fd, buffer, 1, OB_MAX_PACKET_LENGTH);
      //pos = buffer;
      ObMySQLUtil::get_uint1(pos, result_type);
      //read_fd(fd, buffer, header.length_ - 1, OB_MAX_PACKET_LENGTH);
      if (result_type == 0xfe || result_type == 0xff)
      {
        TBSYS_LOG(DEBUG, "end of the read process.");
        is_finish = true;
        break;
      }
      TBSYS_LOG(DEBUG, "result rows[%ld]: %s", ++i, buffer);
    }
  }
  return ret;
}
int LoadConsumer::do_com_query(const struct iovec *iovect, const int count, hash::ObHashMap<int, MYSQL> &fd_connect)
{
  int ret = OB_SUCCESS;
  UNUSED(count);
  if (NULL == iovect)
  {
    TBSYS_LOG(WARN, "invalid argument. iovect is null");
    ret = OB_INVALID_ARGUMENT;
  }
  MYSQL my_;
char host[128];
  int port = param_.new_server_.get_port();
  param_.new_server_.ip_to_string(host, 128);


  if (OB_SUCCESS == ret)
  {
    ret = fd_connect.get((reinterpret_cast<ObMySQLCommandPacketRecord *>(iovect[0].iov_base))->cseq_, my_);
    if (ret == -1 || hash::HASH_NOT_EXIST == ret)
    {
      TBSYS_LOG(DEBUG, "new connect need build. fd=%d, str=%s",
          (reinterpret_cast<ObMySQLCommandPacketRecord *>(iovect[0].iov_base))->cseq_,
          reinterpret_cast<char*>(iovect[1].iov_base));

      if (0 != mysql_library_init(0, NULL, NULL))
      {
        ret = OB_ERROR;
        fprintf(stderr, "could not init mysql library\n");
      }
      else if (NULL == mysql_init(&my_))
      {
        fprintf(stderr, "failed to init libmysqlclient\n");
        ret = OB_ERROR;
        fprintf(stderr, "%s\n", mysql_error(&my_));
      }
      else if(NULL == mysql_real_connect(&my_, host, param_.user_name_, param_.password_, "test", port, NULL, 0))
      {
        fprintf(stderr, "failed to connect mysql server\n");
        ret = OB_ERROR;
        fprintf(stderr, "%s\n", mysql_error(&my_));
      }
      else
      {
        //TBSYS_LOG(DEBUG, "build real connect success!");
        ret = OB_SUCCESS;
        fd_connect.set((reinterpret_cast<ObMySQLCommandPacketRecord *>(iovect[0].iov_base))->cseq_, my_);
      }
    }
    else if (ret == hash::HASH_EXIST)
    {
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "write query to fd. fd=%d", my_.net.vio->sd);
    ret = write_packet_to_fd(my_.net.vio->sd, iovect, count);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to write packet to fd, ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "read query result from fd. fd=%d, query str=%s", my_.net.vio->sd, reinterpret_cast<char*>(iovect[1].iov_base));
    ret = read_query_result(my_.net.vio->sd);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get response. ret=%d", ret);
    }
    else
    {
      //TBSYS_LOG(DEBUG, "read query result from fd success. fd=%d, query str=%s", my_.net.vio->sd, reinterpret_cast<char*>(iovect[1].iov_base));
    }
  }
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "query success: str=%.*s", int32_t(iovect[1].iov_len), reinterpret_cast<char*>(iovect[1].iov_base));
  }
  return ret;
}
