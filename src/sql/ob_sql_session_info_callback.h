/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_session_info_map.h is for what ...
 *
 * Version: ***: ob_sql_session_info_map.h  Thu May 23 15:36:09 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_SESSION_INFO_MAP_H_
#define OB_SQL_SESSION_INFO_MAP_H_

#include "ob_sql_session_info.h"
#include "common/hash/ob_hashmap.h"

namespace oceanbase
{
  namespace sql
  {
    typedef int32_t ObSQLSessionKey;
    class ObSQLSessionInfoRead
    {
    public:
      ObSQLSessionInfoRead():rc_(common::OB_SUCCESS), info_(NULL)
      {

      }

      ~ObSQLSessionInfoRead()
      {

      }

      void operator()(common::hash::HashMapPair<ObSQLSessionKey, ObSQLSessionInfo*> entry)
      {
        if (NULL != entry.second)
        {
          rc_ = entry.second->try_read_lock();
          info_ = entry.second;
          if (common::OB_SUCCESS != rc_)
          {
            TBSYS_LOG(WARN, "failed to lock session info, session_id=%u", entry.first);
            rc_ = OB_LOCK_NOT_MATCH;
          }
        }
        else
        {
          rc_ = common::OB_INVALID_ARGUMENT;
          TBSYS_LOG(ERROR, "entry.second must not null");
        }
      }

      int get_rc() const
      {
        return rc_;
      }

      ObSQLSessionInfo* get_session_info() const
      {
        return info_;
      }

    private:
      int rc_;
      ObSQLSessionInfo* info_;
    };

    class ObSQLSessionInfoWrite
    {
    public:
      ObSQLSessionInfoWrite():rc_(common::OB_SUCCESS), info_(NULL)
      {

      }

      ~ObSQLSessionInfoWrite()
      {

      }

      void operator()(common::hash::HashMapPair<ObSQLSessionKey, ObSQLSessionInfo*> entry)
      {
        if (NULL != entry.second)
        {
          rc_ = entry.second->try_write_lock();
          info_ = entry.second;
        }
        else
        {
          rc_ = common::OB_INVALID_ARGUMENT;
        }
      }

      int get_rc() const
      {
        return rc_;
      }

      ObSQLSessionInfo* get_session_info() const
      {
        return info_;
      }
    private:
      int rc_;
      ObSQLSessionInfo* info_;
    };
  }
}
#endif
