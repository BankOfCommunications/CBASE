/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 *
 */
#include "tbsys.h"
#include "ob_log_src.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace updateserver
  {
    ObCachedLogSrc::ObCachedLogSrc(): cache_(NULL), backup_(NULL)
    {
    }

    ObCachedLogSrc::~ObCachedLogSrc()
    {
    }

    bool ObCachedLogSrc::is_inited() const
    {
      return NULL != cache_ && NULL != backup_;
    }

    int ObCachedLogSrc::init(IObLogSrc* cache, IObLogSrc* backup)
    {
      int err = OB_SUCCESS;
      if(is_inited())
      {
        err = OB_INIT_TWICE;
        TBSYS_LOG(ERROR, "init()=>%d", err);
      }
      else if (NULL == cache || NULL == backup)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "init(cache=%p, backup=%p): invalid argument", cache, backup);
      }
      else
      {
        cache_ = cache;
        backup_ = backup;
      }
      return err;
    }

    int ObCachedLogSrc:: get_log(const int64_t start_id, int64_t& end_id,
                                 char* buf, const int64_t len, int64_t& read_count)

    {
      int err = OB_SUCCESS;
      read_count = 0;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "get_log(): not init.");
      }
      else if (read_count <= 0
               && OB_SUCCESS != (err = cache_->get_log(start_id, end_id, buf, len, read_count)))
      {
        TBSYS_LOG(ERROR, "cache[%p]->get_log(start_id=%ld, buf=%p, len=%ld)=>%d",
                  cache_, start_id, buf, len, err);
      }
      else if (read_count <= 0
               && OB_SUCCESS != (err = (backup_->get_log(start_id, end_id, buf, len, read_count))))
      {
        TBSYS_LOG(ERROR, "backup[%p]->get_log(start_id=%ld, buf=%p, len=%ld)=>%d",
                  backup_, start_id, buf, len, err);
      }
      return err;
    }
  } // end namespace updateserver
} // end namespace oceanbase
