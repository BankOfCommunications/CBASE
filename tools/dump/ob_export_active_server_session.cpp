/*
 * =====================================================================================
 *
 *       Filename:  ob_export_active_server_session.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2014年12月08日 13时33分05秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  zhangcd 
 *   Organization:  
 *
 * =====================================================================================
 */

#include "ob_export_active_server_session.h"

using namespace oceanbase::common;

int ActiveServerSession::pop_by_producer_id(int producer_id, ObServer& server, int64_t& session_id)
{
  int ret = OB_EXPORT_EXIST;
  map_lock_.lock();
  std::map<int, ServerSession>::iterator ss = map.find(producer_id);
  if(ss == map.end())
  {
    ret = OB_EXPORT_NOT_EXIST;
  }
  else
  {
    server = ss->second.server;
    session_id = ss->second.session_id;
    map.erase(ss);
  }
  map_lock_.unlock();
  return ret;
}

int ActiveServerSession::pop_one(ObServer& server, int64_t& session_id)
{
  int ret = OB_EXPORT_EXIST;
  map_lock_.lock();
  std::map<int, ServerSession>::iterator ss = map.begin();
  if(ss == map.end())
  {
    ret = OB_EXPORT_EMPTY;
  }
  else
  {
    server = ss->second.server;
    session_id = ss->second.session_id;
    map.erase(ss);
  }
  map_lock_.unlock();
  return ret;
}

int ActiveServerSession::push_by_id(int producer_id, ObServer server, int64_t session_id)
{
  int ret = OB_EXPORT_SUCCESS;
  map_lock_.lock();
  ServerSession ss;
  ss.server = server;
  ss.session_id = session_id;
  map.insert(make_pair(producer_id, ss));
  map_lock_.unlock();
  return ret;
}

