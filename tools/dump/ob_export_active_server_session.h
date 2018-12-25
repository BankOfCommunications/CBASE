/*
 * =====================================================================================
 *
 *       Filename:  ob_export_active_server_session.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2014年12月08日 13时29分07秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  zhangcd
 *   Organization:  
 *
 * =====================================================================================
 */
#ifndef OB_EXPORT_ACTIVE_SERVER_SESSION_H
#define OB_EXPORT_ACTIVE_SERVER_SESSION_H
#include "common/ob_server.h"
#include "tbsys.h"
#include <map>


/*
 * =====================================================================================
 *        Class:  ActiveServerSession
 *  Description:  当前所有有效查询的server和session_id
 * =====================================================================================
 */
class ActiveServerSession
{
  struct ServerSession
  {
    oceanbase::common::ObServer server;
    int64_t session_id;
  };

private:
  std::map<int, ServerSession> map;
  tbsys::CThreadMutex map_lock_;
public:
  int pop_by_producer_id(int producer_id, oceanbase::common::ObServer& server, int64_t& session_id);
  int push_by_id(int producer_id, oceanbase::common::ObServer server, int64_t session_id);
  int pop_one(oceanbase::common::ObServer& server, int64_t& session_id);
};

#endif
