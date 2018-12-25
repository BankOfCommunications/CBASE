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

#ifndef __OCEANBASE_UPDATE_SERVER_OB_LOG_SRC_H__
#define __OCEANBASE_UPDATE_SERVER_OB_LOG_SRC_H__
#include <common/ob_define.h>

namespace oceanbase
{
  namespace updateserver
  {
    class IObLogSrc
    {
      public:
        IObLogSrc(){}
        virtual ~IObLogSrc(){}
        virtual int get_log(const int64_t start_id, int64_t& end_id,
                    char* buf, const int64_t len, int64_t& read_count) = 0;
      protected:
        DISALLOW_COPY_AND_ASSIGN(IObLogSrc);
    };

    class ObCachedLogSrc: public IObLogSrc
    {
      public:
        ObCachedLogSrc();
        virtual ~ObCachedLogSrc();
        bool is_inited() const;
        int init(IObLogSrc* cache, IObLogSrc* backup);
        virtual int get_log(const int64_t start_id, int64_t& end_id,
                    char* buf, const int64_t len, int64_t& read_count);
      private:
        IObLogSrc* cache_;        
        IObLogSrc* backup_;
    };
  } // end namespace updateserver
} // end namespace oceanbase
#endif //__OCEANBASE_UPDATE_SERVER_OB_LOG_CACHE_H__
