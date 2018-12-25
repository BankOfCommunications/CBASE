/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: load_manager.h,v 0.1 2012/03/15 14:00:00 zhidong Exp $
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef LOAD_MANAGER_H_
#define LOAD_MANAGER_H_

#include <queue>
#include "tbsys.h"
#include "common/ob_scanner.h"
#include "common/ob_server.h"
#include "common/ob_fifo_stream.h"
#include "load_query.h"
#include "load_statics.h"
#include "packet_queue.h"
namespace oceanbase
{
  namespace tools
  {
    class LoadManager
    {
    public:
      LoadManager();
      virtual ~LoadManager();
    public:
      void init(const int64_t max_count, const int64_t thread_count);
      int push(const QueryInfo & query, const bool read_only = false, const int64_t timeout = 0);
      int pop(const int64_t fd, QueryInfo & query, const int64_t timeout = 0);
    public:
      void filter(void);
      void dump();
      void consume(const bool old, const bool succ, const int64_t data,
          const int64_t row, const int64_t cell, const int64_t time);
      void error(void);
      void status(Statics & data) const;
      int get_statement_id(const int socket_fd, uint32_t &statement_id);
      int64_t size(void) const;
    public:
      void set_finish(void);
      bool get_finish(void) const;
    private:
      int query(QueryInfo & query, const bool old, const common::ObServer & server,
          common::ObScanner & result);
    private:
      static const int64_t DEFAULT_COUNT = 8 * 1024 * 1024L;
      int64_t max_count_;
      // one thread push
      //tbsys::CThreadCond load_lock_;
      PacketQueue load_queue_;
      //std::queue<QueryInfo> load_queue_;
      // status info
      Statics result_;
    };
  }
}


#endif // LOAD_MANAGER_H_
