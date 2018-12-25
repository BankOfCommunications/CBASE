#include <string.h>
#include "ob_mysql_loginer.h"
#include "packet/ob_mysql_ok_packet.h"
#include "packet/ob_mysql_error_packet.h"
#include "packet/ob_mysql_handshake_packet.h"
#include "tblog.h"
#include "common/data_buffer.h"
#include "common/utility.h"
#include "ob_mysql_util.h"
#include "common/ob_privilege_manager.h"
#include "common/hash/ob_hashutils.h"
#include "common/ob_errno.h"
#include "sql/ob_sql_session_info.h"
#include "ob_mysql_server.h"
using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::common::hash;

ObMySQLClientCapability::ObMySQLClientCapability(uint32_t ca): capability_(ca)
{

}
ObMySQLLoginer::LoginInfo::LoginInfo()
  :capability_(0), max_packet_size_(0),character_set_(0)
   , user_name_(),db_name_(),auth_response_()
{
  db_name_.reset();
}
ObMySQLLoginer::ObMySQLLoginer()
  :buffer_(BUFFER_SIZE),privilege_mgr_(NULL),server_(NULL)
{

}

ObMySQLLoginer::~ObMySQLLoginer()
{

}
void ObMySQLLoginer::set_obmysql_server(ObMySQLServer* server)
{
  server_ = server;
}
void ObMySQLLoginer::set_privilege_manager(ObPrivilegeManager *privilege_mgr)
{
  privilege_mgr_ = privilege_mgr;
}
int ObMySQLLoginer::login(easy_connection_t* c, sql::ObSQLSessionInfo *& session)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(server_ != NULL);
  if (NULL == c)
  {
    TBSYS_LOG(WARN, "invalid argument c is %p", c);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    LoginInfo login_info;
    ret = handshake(c);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "send hand shake packet failed");
    }
    else
    {
      ret = parse_packet(c, login_info);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "parse client auth packet failed");
      }
      else
      {
        ret = check_privilege(c, session, login_info);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "login failed, err=%d", ret);
        }
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    OB_STAT_INC(OBMYSQL, SUCC_LOGIN_COUNT);
  }
  else
  {
    OB_STAT_INC(OBMYSQL, FAIL_LOGIN_COUNT);
  }
  return ret;
}

ThreadSpecificBuffer::Buffer* ObMySQLLoginer::get_buffer() const
{
  return buffer_.get_buffer();
}

int ObMySQLLoginer::handshake(easy_connection_t* c)
{
  int ret = OB_SUCCESS;
  if (NULL == c)
  {
    TBSYS_LOG(ERROR, "invalide argument c is %p", c);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    //TODO
    //gather oceanbase states construct handshake buffer send to client
    ObMySQLHandshakePacket packet;

    //int32_t length = packet.get_serialize_size();
    //length += OB_MYSQL_PACKET_HEADER_SIZE;

    ThreadSpecificBuffer::Buffer* thread_buffer = buffer_.get_buffer();
    if (NULL == thread_buffer)
    {
      TBSYS_LOG(ERROR, "get thread buffer error, ignore");
      ret = OB_ERROR;
    }
    else
    {
      ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
      packet.set_thread_id(c->seq);
      out_buffer.get_position() = 0;
      ret = packet.serialize(out_buffer.get_data(),
                             out_buffer.get_capacity(), out_buffer.get_position());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "ObMySQLHandshakPacket serialize failed, buffer is %p, buffer len is %ld,"
                  "pos is %ld", out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
      }
      else
      {
        ret = write_data(c->fd, out_buffer.get_data(), out_buffer.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write packet data to client failed fd is %d, buffer is %p, length is %ld",
                    c->fd, out_buffer.get_data(), out_buffer.get_position());
          ret = OB_ERR_WRITE_AUTH_ERROR;
        }
        else
        {
          TBSYS_LOG(INFO, "new client login, peer=%s",
                    inet_ntoa_r(c->addr));
        }
      }
    }
  }
  return ret;
}

int ObMySQLLoginer::parse_packet(easy_connection_t* c, LoginInfo& login_info)
{
  int ret = OB_SUCCESS;
  int read_size = OB_MYSQL_PACKET_HEADER_SIZE;
  char* len_pos = NULL;
  if (NULL == c)
  {
    TBSYS_LOG(ERROR, "invalid argument c is %p", c);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    common::ThreadSpecificBuffer::Buffer* thread_buffer = get_buffer();
    if (NULL == thread_buffer)
    {
      TBSYS_LOG(ERROR, "get thread buffer failed");
      ret = OB_ERROR;
    }
    else
    {
      ObDataBuffer in_buffer(thread_buffer->current(), thread_buffer->remain());
      //read packet header first
      TBSYS_LOG(DEBUG, "start read from %s %d bytes", inet_ntoa_r(c->addr),
                read_size);
      ret = read_data(c->fd, in_buffer.get_data(), read_size);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "read packet header failed");
        ret = OB_ERR_PROTOCOL_NOT_RECOGNIZE;
      }
      else
      {
        uint32_t packet_len = 0;
        len_pos = in_buffer.get_data();
        ObMySQLUtil::get_uint3(len_pos, packet_len);
        TBSYS_LOG(DEBUG, "start read from %s %u bytes", inet_ntoa_r(c->addr),
                  packet_len);
        in_buffer.get_position() = OB_MYSQL_PACKET_HEADER_SIZE;
        ret = read_data(c->fd, in_buffer.get_data() + in_buffer.get_position(),
                        packet_len);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "read packet data failed, ret=%d", ret);
          ret = OB_ERR_PROTOCOL_NOT_RECOGNIZE;
        }
        else
        {
          TBSYS_LOG(DEBUG, "readed from %s %u bytes", inet_ntoa_r(c->addr),
                    packet_len);
          len_pos = in_buffer.get_data() + in_buffer.get_position();
          uint32_t capability_flags = 0;
          uint32_t max_packet_size = 0;
          uint8_t character_set = 0;
          uint8_t auth_response_len = 0;
          int32_t username_len = -1;
          int32_t db_len = -1;
          ObMySQLUtil::get_uint4(len_pos, capability_flags);
          login_info.capability_.capability_ = capability_flags;
          ObMySQLUtil::get_uint4(len_pos, max_packet_size);//16MB
          login_info.max_packet_size_ = max_packet_size;
          ObMySQLUtil::get_uint1(len_pos, character_set);
          login_info.character_set_ = character_set;
          len_pos += 23;//23 bytes reserved
          username_len = static_cast<int32_t>(strlen(len_pos));
          login_info.user_name_.assign_ptr(len_pos, username_len);
          len_pos += username_len + 1;
          ObMySQLUtil::get_uint1(len_pos, auth_response_len);
          login_info.auth_response_.assign_ptr(len_pos, static_cast<int32_t>(auth_response_len));
          len_pos += auth_response_len;
          //modify wenghaixing [database manage]20150617
          //db_len = static_cast<int32_t>(strlen(len_pos));
          db_len = packet_len - 34 - username_len - auth_response_len-1;
          //modify e
          login_info.db_name_.assign_ptr(len_pos, db_len);
          len_pos += db_len;
        }
      }
    }
  }
  return ret;
}

int ObMySQLLoginer::insert_new_session(easy_connection_t* c, sql::ObSQLSessionInfo *&session)
{
  int ret = OB_SUCCESS;
  session->set_session_id(c->seq);
  session->set_conn(c);
  ret = session->set_peer_addr(get_peer_ip(c));
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "set session peer addr failed");
  }
  else
  {
    //not over write
    ret = server_->get_session_mgr()->set((sql::ObSQLSessionKey&)c->seq, session, 0);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "insert new session failed ret is %d, key is %d", ret, c->seq);
    }
    else
    {
      int64_t session_num = server_->get_session_mgr()->get_session_count();
      TBSYS_LOG(INFO, "[LOGIN] new session, session_key=%d session=%s username=%.*s total_sessions_num=%ld",
                c->seq, to_cstring(*session),
                session->get_user_name().length(), session->get_user_name().ptr(), session_num);
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObMySQLLoginer::check_privilege(easy_connection_t* c, sql::ObSQLSessionInfo *&session, LoginInfo& login_info)
{
  int ret = OB_SUCCESS;
  int send_err = OB_SUCCESS;
  ObMySQLPacket *packet = NULL;
  ObMySQLErrorPacket err_packet;
  ObMySQLOKPacket ok_packet;
  const ObPrivilege ** pp_privilege = NULL;
  if (NULL == c)
  {
    TBSYS_LOG(ERROR, "invalid argument c is %p", c);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    //TBSYS_LOG(WARN, "before login");
    //ob_print_mod_memory_usage();
    if (server_->has_too_many_sessions())
    {
      TBSYS_LOG(WARN, "there are too many sessions, refuse this client");
      ret = OB_ERR_TOO_MANY_SESSIONS;
      err_packet.set_oberrcode(ret);
      ObString message = ObString::make_string("we have too many sessoins");
      int err = err_packet.set_message(message);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "set message failed, ret=%d", err);
      }
      packet = &err_packet;
    }
    else if (NULL == privilege_mgr_)
    {
      ret = OB_ERR_SERVER_IN_INIT;
      TBSYS_LOG(WARN, "privilege manager not set, equals null");
      err_packet.set_oberrcode(ret);
      int err = err_packet.set_message(ObString::make_string(ob_strerror(ret)));
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "set message failed, ret=%d", err);
      }
      packet = &err_packet;
    }
    else
    {
      // step 1 : check privilege
      if (OB_SUCCESS != (ret = privilege_mgr_->get_newest_privilege(pp_privilege)))
      {
        TBSYS_LOG(WARN, "get privilege failed, ret=%d", ret);

        err_packet.set_oberrcode(OB_ERR_SERVER_IN_INIT);
        int err = err_packet.set_message(ObString::make_string(ob_strerror(OB_ERR_SERVER_IN_INIT)));
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "set message failed, ret=%d", err);
        }
        packet = &err_packet;
      }
      else if (OB_SUCCESS != (ret = (*pp_privilege)->user_is_valid(login_info.user_name_, login_info.auth_response_)))
      {
        TBSYS_LOG(WARN, "user '%.*s' login failed,ret=%d", login_info.user_name_.length(), login_info.user_name_.ptr(), ret);
        err_packet.set_oberrcode(ret);
        ObString message;
        if (OB_ERR_USER_EMPTY == ret)
        {
          message = ObString::make_string("username is empty");
        }
        else if (OB_ERR_USER_NOT_EXIST == ret)
        {
          message = ObString::make_string("username not exists");
        }
        else if (OB_ERR_USER_IS_LOCKED == ret)
        {
          message = ObString::make_string("user is locked");
        }
        else if (OB_ERR_WRONG_PASSWORD == ret)
        {
          message = ObString::make_string("wrong password");
        }
        int err = err_packet.set_message(message);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "set message failed, ret=%d", err);
        }
        packet = &err_packet;
      }
      //add wenghaixing [database manage]20150610
      else if (OB_SUCCESS != (ret = (*pp_privilege)->user_db_is_valid(login_info.user_name_, login_info.db_name_)))
      {
        TBSYS_LOG(WARN, "user [%.*s] has no access to db [%.*s]",login_info.user_name_.length(), login_info.user_name_.ptr(),
                  login_info.db_name_.length(),login_info.db_name_.ptr());
        err_packet.set_oberrcode(ret);
        ObString message;
        if(OB_ERR_DATABASE_NOT_EXSIT == ret)
        {
          message = ObString::make_string("database is not exist");
        }
        else if(OB_ERR_NO_ACCESS_TO_DATABASE == ret)
        {
          message = ObString::make_string("no access to database");
        }
        int err = err_packet.set_message(message);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "set message failed, ret=%d", err);
        }
        packet = &err_packet;
      }
      //add e
      else if (NULL == (session = server_->get_session_pool().alloc()))
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "ob malloc failed, ret=%d", OB_ALLOCATE_MEMORY_FAILED);
        packet = &err_packet;
      }
      else
      {
        if (OB_SUCCESS != (ret = session->init(*(server_->get_block_allocator()))))
        {
          packet = &err_packet;
          TBSYS_LOG(WARN, "init session info failed, ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret =session->set_ps_store(server_->get_ps_store())))
        {
          TBSYS_LOG(WARN, "set ps store to session failed, ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = session->set_username(login_info.user_name_)))
        {
          TBSYS_LOG(WARN, "add username %.*s to session failed, ret=%d", login_info.user_name_.length(), login_info.user_name_.ptr(), ret);
          err_packet.set_oberrcode(ret);
          ObString message = ObString::make_string("internal server error");
          int err = err_packet.set_message(message);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "set message failed, ret=%d", err);
          }
          packet = &err_packet;
        }
        //add wenghaixing [database manage]20150701
        //set db name in session info when has db name
        else
        {
          ObString db_name;
          if (login_info.db_name_.length() > 0)
          {
            db_name = login_info.db_name_;
          }
          else
          {
            db_name = ObString::make_string(OB_DEFAULT_DB_NAME);
          }
          if (OB_SUCCESS != (ret = session->set_db_name(db_name)))
          {
            TBSYS_LOG(WARN, "add dbname %.*s to session failed, ret=%d", db_name.length(), db_name.ptr(), ret);
            err_packet.set_oberrcode(ret);
            ObString message = ObString::make_string("internal server error");
            int err = err_packet.set_message(message);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "set message failed, ret=%d", err);
            }
            packet = &err_packet;
          }
        }

        //add e
        //modify wenghaixing [database manage]20150701
        //else
        if(OB_SUCCESS == ret)
        //modify e
        {
          session->set_interactive(login_info.capability_.CapabilityFlags.CLIENT_INTERACTIVE);
          session->set_version_provider(server_->get_env().merge_service_);
          session->set_config_provider(server_->get_config());
        }
        // step 2: load system params
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = server_->load_system_params(*session)))
          {
            TBSYS_LOG(WARN, "failed to load system params, ret=%d", ret);
            err_packet.set_oberrcode(ret);
            ObString message = ObString::make_string("login error, internal server error");
            int err = err_packet.set_message(message);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "set message failed, ret=%d", err);
            }
            packet = &err_packet;
          }
          else if (OB_SUCCESS != (ret = insert_new_session(c, session)))
          {
            TBSYS_LOG(ERROR, "failed to insert new session, ret=%d, key is %d", ret, c->seq);
            err_packet.set_oberrcode(ret);
            ObString message = ObString::make_string("login error, internal server error");
            int err = err_packet.set_message(message);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "set message failed, ret=%d", err);
            }
            packet = &err_packet;
          }
        }
        if (OB_SUCCESS != ret)
        {
          if (NULL != session)
          {
            server_->get_session_pool().free(session);
            session = NULL;
          }
        }
        else
        {
          packet = &ok_packet;
        }
      }
    }
    common::ThreadSpecificBuffer::Buffer* thread_buffer = get_buffer();
    if (NULL != thread_buffer)
    {
      thread_buffer->reset();
      ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
      out_buffer.get_position() = 0;

      send_err = packet->encode(out_buffer.get_data(), out_buffer.get_capacity(),
                             out_buffer.get_position());
      if (OB_SUCCESS != send_err)
      {
        TBSYS_LOG(ERROR, "serialize packet failed, err=%d", send_err);
      }
      else
      {
        send_err = write_data(c->fd, out_buffer.get_data(), out_buffer.get_position());
        if (OB_SUCCESS != send_err)
        {
          TBSYS_LOG(WARN, "write packet to mysql client failed, fd=%d, buffer=%p,"
                    "length=%ld, err=%d", c->fd, out_buffer.get_data(), out_buffer.get_position(), send_err);
        }
        else
        {
          TBSYS_LOG(DEBUG, "send packet to %s bytes", inet_ntoa_r(c->addr));
        }
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "get thread buffer failed");
      send_err = OB_ERROR;
    }
    //TBSYS_LOG(WARN, "after login");
    //ob_print_mod_memory_usage();
  }
  if (pp_privilege != NULL)
  {
    int tmp_err = privilege_mgr_->release_privilege(pp_privilege);
    if (OB_SUCCESS != tmp_err)
    {
      TBSYS_LOG(WARN, "release privilege failed, ret=%d", tmp_err);
    }
  }
  return (ret == OB_SUCCESS) ? send_err : ret;
}


int ObMySQLLoginer::write_data(int fd, char* buffer, size_t length)
{
  int ret = OB_SUCCESS;
  if (fd < 0 || NULL == buffer || length <= 0)
  {
    TBSYS_LOG(ERROR, "invalid argument fd=%d, buffer=%p, length=%zd", fd, buffer, length);
    ret = OB_ERROR;
  }
  else
  {
    const char* buff = buffer;
    ssize_t count = 0;
    while (OB_SUCCESS == ret && length > 0 && (count = write(fd, buff, length)) != 0)
    {
      if (-1 == count)
      {
        if (errno == EINTR)
        {
          continue;
        }
        else
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "write data faild, errno is %d, errstr is %s", errno, strerror(errno));
        }

      }
      buff += count;
      length -= count;
    }
  }

  return ret;
}

int ObMySQLLoginer::read_data(int fd, char* buffer, size_t length)
{
  int ret = OB_SUCCESS;
  static const int64_t timeout = 1000000;//1s
  if (fd < 0 || NULL == buffer || length <= 0)
  {
    TBSYS_LOG(ERROR, "invalid argument fd=%d, buffer=%p, length=%zd", fd, buffer, length);
    ret = OB_ERROR;
  }
  else
  {
    char* buff = buffer;
    ssize_t count = 0;
    int64_t trycount = 0;
    int64_t start_time = tbsys::CTimeUtil::getTime();
    while (OB_SUCCESS == ret && length > 0 && (count = read(fd, buff, length)) != 0)
    {
      trycount ++;
      if (trycount % 100 == 0 && tbsys::CTimeUtil::getTime() - start_time > timeout)
      {
        TBSYS_LOG(WARN, "read data(fd=%d) timeout. try read count is %ld, count is %zu, length left is %zu, errno is %d,"
                  "errstring is %s start_time is %ld", fd, trycount, count, length, errno, strerror(errno), start_time);
        ret = OB_ERROR;
      }
      if (OB_SUCCESS == ret)
      {
        if (-1 == count)
        {
          if (errno == EINTR || errno == EAGAIN)
          {
            continue;
          }
          else
          {
            ret = OB_ERROR;
            TBSYS_LOG(WARN, "read data faild, errno is %d, errstr is %s", errno, strerror(errno));
          }
        }
        buff += count;
        length -= count;
      }
    }
    if (0 != length)
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "read not return enough data need %zu more bytes", length);
    }
  }
  return ret;
}
