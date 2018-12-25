/*
 *
 * This program is free software; you can redistribute it and/or modify 
 * it under the terms of the GNU General Public License version 2 as 
 * published by the Free Software Foundation.
 *
 * ob_sql_session_info_traversal.h is for what ...
 *
 * Version: ***: ob_sql_session_info_traversal.h  Fri May 24 10:51:04 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want 
 *
 */
#ifndef OB_SQL_SESSION_INFO_TRAVERSAL_H_
#define OB_SQL_SESSION_INFO_TRAVERSAL_H_

#include "ob_sql_session_info.h"
#include "ob_sql_session_info_callback.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_list.h"

using namespace oceanbase::common::hash;
namespace oceanbase
{
  namespace sql
  {
    class ObSQLSessionInfoTraversal
    {
    public:
      ObSQLSessionInfoTraversal(ObHashMap<ObSQLSessionKey, ObSQLSessionInfo*> *map);
      ~ObSQLSessionInfoTraversal();

      void operator()(HashMapPair<ObSQLSessionKey, ObSQLSessionInfo*> entry);
      inline common::ObList<HashMapPair<ObSQLSessionKey, ObSQLSessionInfo*> > * get_session_list()
      {
        return &list_;
      }
    private:
      common::hash::ObHashMap<ObSQLSessionKey, ObSQLSessionInfo*> *map_;
      common::ObList<HashMapPair<ObSQLSessionKey, ObSQLSessionInfo*> > list_;
    };
  }
}
#endif
