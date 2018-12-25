#include "obmysql/ob_mysql_server.h"
#include "ob_sql_session_mgr.h"
#include "common/hash/ob_hashutils.h"
#include "ob_sql_session_info_traversal.h"

using namespace oceanbase::sql;
using namespace oceanbase::common::hash;

ObSQLSessionMgr::ObSQLSessionMgr(): sql_server_(NULL)
{

}

ObSQLSessionMgr::~ObSQLSessionMgr()
{

}

void ObSQLSessionMgr::set_sql_server(obmysql::ObMySQLServer *server)
{
  sql_server_ = server;
}

int64_t ObSQLSessionMgr::get_session_count() const
{
  return session_map_.size();
}

int ObSQLSessionMgr::kill_session(uint32_t session_id, bool is_query)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *info = NULL;
  do
  {
    ret = get(session_id, info);
    if (OB_SUCCESS != ret)
    {
      if (OB_ENTRY_NOT_EXIST == ret)
      {
        TBSYS_LOG(WARN, "session not found session key is %u", session_id);
      }
      else if (OB_LOCK_NOT_MATCH == ret)
      {
        TBSYS_LOG(WARN, "found session error, session key is %u retry after 10ms", session_id);
        usleep(10*1000);
        continue;
      }
      else
      {
        TBSYS_LOG(WARN, "failed to get session id=%u, ret=%d", session_id, ret);
      }
    }
    else
    {
      TBSYS_LOG(WARN, "kill session is_query=%c session_id=%u", is_query? 'Y':'N', session_id);
      info->set_session_state(is_query? QUERY_KILLED:SESSION_KILLED);
      info->update_last_active_time();
      if (!is_query)
      {
        easy_connection_t *c = info->get_conn();
        if (NULL != c)
        {
          TBSYS_LOG(INFO, "kill connection %s", easy_connection_str(c));
          easy_connection_destroy_dispatch(c);
          //c->event_status = EASY_EVENT_DESTROY;
          //easy_connection_wakeup(c);
          info->set_conn(NULL); // the connection is invalid now
        }
        else
        {
          TBSYS_LOG(WARN, "get conn from session info is null");
        }
      }
      info->unlock();
    }
  } while(false);
  return ret;
}

int ObSQLSessionMgr::get(ObSQLSessionKey key, ObSQLSessionInfo *&info)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfoRead sreader;

  ret = session_map_.atomic(key, sreader);
  if (hash::HASH_EXIST == ret)
  {
    if (OB_SUCCESS == (ret = sreader.get_rc()))
    {
      info = sreader.get_session_info();
    }
    else
    {
      TBSYS_LOG(WARN, "can not get read lock ret is %d", ret);
    }
  }
  else if (hash::HASH_NOT_EXIST == ret)
  {
    TBSYS_LOG(WARN, "session not exist key is %d", key);
    ret = OB_ENTRY_NOT_EXIST;
  }
  else
  {
    TBSYS_LOG(WARN, "can not get session info key is %d ret is %d", key, ret);
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObSQLSessionMgr::create(int64_t hash_bucket)
{
  return session_map_.create(hash_bucket);
}

int ObSQLSessionMgr::set(ObSQLSessionKey key, ObSQLSessionInfo *&info, int flag)
{
  int ret = OB_SUCCESS;
  ret = session_map_.set(key, info, flag);
  if (hash::HASH_INSERT_SUCC != ret)
  {
    TBSYS_LOG(WARN, "insert new session failed, err=%d key=%d", ret, key);
  }
  else
  {
    TBSYS_LOG(INFO, "new session insert, session_key=%d session=%s",
              key, to_cstring(*info));
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObSQLSessionMgr::erase(ObSQLSessionKey key)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfoWrite swriter;
  TBSYS_LOG(DEBUG, "erase session key is %d", key);
  ret = session_map_.atomic(key, swriter);
  if (hash::HASH_EXIST == ret)
  {
    if (OB_SUCCESS == (ret = swriter.get_rc()))
    {
      ObSQLSessionInfo *info = swriter.get_session_info();
      //for ps cache close_all_stmt
      if (NULL != info)
      {
        TBSYS_LOG(DEBUG, "close all prepared statement in session");
        ret = info->close_all_stmt();
        if (OB_SUCCESS != ret)
        {
          info->unlock();
          TBSYS_LOG(WARN, "close stmt failed need retry ret=%d", ret);
        }
      }
      else
      {
        TBSYS_LOG(DEBUG, "session info is NULL");
      }

      if (OB_SUCCESS == ret)
      {
        ret = session_map_.erase(key);
        if (hash::HASH_EXIST == ret)
        {
          int64_t sessions_num = session_map_.size();
          TBSYS_LOG(INFO, "[QUIT] delete session, session_key=%d session=%s sessions_num=%ld",
                    key, to_cstring(*info), sessions_num);
          sql_server_->get_session_pool().free(info);
          info = NULL;
          ret = OB_SUCCESS;
        }
        else
        {
          TBSYS_LOG(WARN, "erase session failed ret is %d", ret);
        }
      }
    }
    else
    {
      TBSYS_LOG(WARN, "can not get write lock of session key is %d,rc=%d", key,ret);
      ret = OB_NEED_RETRY;
    }
  }
  else if (hash::HASH_NOT_EXIST == ret)
  {
    TBSYS_LOG(INFO, "session not exist");
    ret = OB_SUCCESS;
  }
  else
  {
    TBSYS_LOG(WARN, "can not free session info ret is %d", ret);
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

const ObServer* ObSQLSessionMgr::get_server() const
{
  return sql_server_->get_server();
}

int ObSQLSessionMgr::get_scanner(ObNewScanner &scanner)
{
  int ret = OB_SUCCESS;
  int64_t total_session_cnt = 0;
  // create row desc
  ObRowDesc row_desc;
  int32_t column_id = OB_APP_MIN_COLUMN_ID;
  for (int index = 0; index < 10 && OB_SUCCESS == ret; ++index)
  {
    ret = row_desc.add_column_desc(OB_ALL_SERVER_SESSION_TID, column_id++);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "add column desc failed ret is %d column_id=%d", ret, column_id);
    }
  }
  if (OB_SUCCESS == ret)
  {
    row_desc.set_rowkey_cell_count(1);
    ObRow row;
    ObObj obj;
    //try read lock session and push into list when locked
    ObSQLSessionInfoTraversal straversal(&session_map_);
    session_map_.foreach(straversal);
    HashMapPair<ObSQLSessionKey, ObSQLSessionInfo*> entry;
    ObList<HashMapPair<ObSQLSessionKey, ObSQLSessionInfo*> > *list = straversal.get_session_list();
    ObSQLSessionInfo *session = NULL;
    ObServer* server = sql_server_->get_server();
    uint64_t ser_ip = server->get_ipv4();
    uint64_t id = 0;
    char ip[32];
    server->ip_to_string(ip, 32);
    while(list->pop_front(entry) != -1)
    {
      id = entry.first;
      row.set_row_desc(row_desc);
      session = entry.second;
      const ObSQLSessionState state = session->get_session_state();

      if (OB_LIKELY(OB_SUCCESS == ret))
      {
        obj.set_int(id<<32|ser_ip);
        if ((ret = row.set_cell(OB_ALL_SERVER_SESSION_TID, OB_APP_MIN_COLUMN_ID, obj)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Add 'Id' to ObRow failed");
        }
      }

      if (OB_LIKELY(OB_SUCCESS == ret))
      {
        obj.set_varchar(session->get_user_name());
        if ((ret = row.set_cell(OB_ALL_SERVER_SESSION_TID, 1 + OB_APP_MIN_COLUMN_ID, obj)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Add 'User' to ObRow failed");
        }
      }

      if (OB_LIKELY(OB_SUCCESS == ret))
      {
        obj.set_varchar(ObString(32, static_cast<ObString::obstr_size_t>(strlen(session->get_peer_addr())),
                                   session->get_peer_addr()));
        if ((ret = row.set_cell(OB_ALL_SERVER_SESSION_TID, 2 + OB_APP_MIN_COLUMN_ID, obj)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Add 'Host' to ObRow failed");
        }
      }

      if (OB_LIKELY(OB_SUCCESS == ret))
      {
        obj.set_varchar(ObString::make_string("oceanbase"));
        if ((ret = row.set_cell(OB_ALL_SERVER_SESSION_TID, 3 + OB_APP_MIN_COLUMN_ID, obj)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Add 'db' to ObRow failed");
        }
      }

      if (OB_LIKELY(OB_SUCCESS == ret))
      {
        if (QUERY_ACTIVE == state)
        {
          obj.set_varchar(session->get_current_query_string());
        }
        else
        {
          obj.set_varchar(ObString::make_string("NULL"));
        }
        if ((ret = row.set_cell(OB_ALL_SERVER_SESSION_TID, 4 + OB_APP_MIN_COLUMN_ID, obj)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Add 'Command' to ObRow failed");
        }
      }

      if (OB_LIKELY(OB_SUCCESS == ret))
      {
        if (QUERY_ACTIVE == state)
        {
          obj.set_int(tbsys::CTimeUtil::getTime() - session->get_query_start_time());
        }
        else
        {
          obj.set_int(0);
        }
        if ((ret = row.set_cell(OB_ALL_SERVER_SESSION_TID, 5 + OB_APP_MIN_COLUMN_ID, obj)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Add 'Time' to ObRow failed");
        }
      }

      if (OB_LIKELY(OB_SUCCESS == ret))
      {
        obj.set_varchar(ObString::make_string(session->get_session_state_str()));
        if ((ret = row.set_cell(OB_ALL_SERVER_SESSION_TID, 6 + OB_APP_MIN_COLUMN_ID, obj)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Add 'State' to ObRow failed");
        }
      }

      if (OB_LIKELY(OB_SUCCESS == ret))
      {
        obj.set_varchar(ObString::make_string("NULL"));
        if ((ret = row.set_cell(OB_ALL_SERVER_SESSION_TID, 7 + OB_APP_MIN_COLUMN_ID, obj)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Add 'Info' to ObRow failed");
        }
      }

      if (OB_LIKELY(OB_SUCCESS == ret))
      {
        obj.set_varchar(ObString(32, static_cast<ObString::obstr_size_t>(strlen(ip)), ip));
        if ((ret = row.set_cell(OB_ALL_SERVER_SESSION_TID, 8 + OB_APP_MIN_COLUMN_ID, obj)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Add 'MergeServer' to ObRow failed");
        }
      }

      if (OB_LIKELY(OB_SUCCESS == ret))
      {
        obj.set_int(id);
        if ((ret = row.set_cell(OB_ALL_SERVER_SESSION_TID, 9 + OB_APP_MIN_COLUMN_ID, obj)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Add 'Index' to ObRow failed");
        }
      }

      if (OB_SUCCESS == ret)
      {
        scanner.add_row(row);
        total_session_cnt++;
      }
      session->unlock();
    }

    if (total_session_cnt > 0)
    {
      ObObj id;
      id.set_type(ObIntType);
      id.set_int(OB_INVALID_ID);
      ObRowkey rowkey(&id, 1);
      scanner.set_last_row_key(rowkey);
    }
    scanner.set_is_req_fullfilled(true, total_session_cnt);
  }
  return ret;
}

void ObSQLSessionMgr::runTimerTask()
{
  check_session_timeout();
}

void ObSQLSessionMgr::check_session_timeout()
{
  ObSQLSessionInfoTraversal straversal(&session_map_);
  session_map_.foreach(straversal);
  HashMapPair<ObSQLSessionKey, ObSQLSessionInfo*> entry;
  ObList<HashMapPair<ObSQLSessionKey, ObSQLSessionInfo*> > *list = straversal.get_session_list();
  ObSQLSessionInfo *session = NULL;

  while(-1 != list->pop_front(entry))
  {
    session = entry.second;
    if (NULL == session)
    {
      TBSYS_LOG(ERROR, "session must not null");
      continue;
    }

    if (SESSION_SLEEP != session->get_session_state() && QUERY_KILLED != session->get_session_state())
    {
      session->unlock();// session get from straversal is all locked
      continue;
    }

    if (session->is_timeout())
    {
      session->set_session_state(SESSION_KILLED);
      easy_connection_t *c = session->get_conn();
      if (NULL == c)
      {
        TBSYS_LOG(ERROR, "connection of seesion %u must not null", entry.first);
      }
      else
      {
        TBSYS_LOG(INFO, "session %u timeout kill connection %s", entry.first, easy_connection_str(c));
        easy_connection_destroy_dispatch(c);
        //c->event_status = EASY_EVENT_DESTROY;
        //easy_connection_wakeup(c);
        session->set_conn(NULL); // the connection is invalid now
      }
    }
    session->unlock(); // session get from straversal is all locked
  }
}
