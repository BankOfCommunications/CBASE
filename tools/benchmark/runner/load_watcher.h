/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: load_watcher.h,v 0.1 2012/03/15 14:00:00 zhidong Exp $
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef LOAD_WATCHER_H_
#define LOAD_WATCHER_H_

#include "tbsys.h"

namespace oceanbase
{
  namespace tools
  {
    class LoadRunner;
    class LoadManager;
    class LoadWatcher:public tbsys::CDefaultRunnable
    {
    public:
      LoadWatcher();
      virtual ~LoadWatcher();
    public:
      void init(const bool compare, const int64_t max_count, const int64_t interval,
          LoadRunner * runner, const LoadManager * manager);
      virtual void run(tbsys::CThread* thread, void* arg);
    private:
      static const int64_t DEFAULT_INTERVAL = 10 * 1000 * 1000L; // 10s
      bool compare_;
      int64_t max_count_;
      int64_t interval_;
      LoadRunner * runner_;
      const LoadManager * manager_;
    };
  }
}


#endif // LOAD_WATCHER_H_
