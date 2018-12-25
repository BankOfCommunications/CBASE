/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */

#ifndef __OCEANBASE_COMMON_OB_LOG_CURSOR_H__
#define __OCEANBASE_COMMON_OB_LOG_CURSOR_H__
#include "ob_define.h"
#include "tbsys.h"
#include "serialization.h"
#include "ob_log_entry.h"
#include "ob_spin_rwlock.h"

namespace oceanbase
{
  namespace common
  {
    struct ObLogCursor
    {
      int64_t file_id_;
      int64_t log_id_;
      int64_t offset_;
      ObLogCursor();
      ~ObLogCursor();
      bool is_valid() const;
      int to_start();
      void reset();
      int serialize(char* buf, int64_t len, int64_t& pos) const;
      int deserialize(const char* buf, int64_t len, int64_t& pos) const;
      char* to_str() const;
      int64_t to_string(char* buf, const int64_t len) const;
      int this_entry(ObLogEntry& entry, const LogCommand cmd, const char* log_data, const int64_t data_len) const;
      int next_entry(ObLogEntry& entry, const LogCommand cmd, const char* log_data, const int64_t data_len) const;
      //add wangjiahao [Paxos ups_replication_tmpglog] 20150715 :b
      int next_entry(ObLogEntry& entry, const LogCommand cmd, const char* log_data, const int64_t data_len, const int64_t term) const;
      //add :e
      int advance(const ObLogEntry& entry);
      int advance(LogCommand cmd, int64_t seq, const int64_t data_len);
      bool newer_than(const ObLogCursor& that) const;
      bool equal(const ObLogCursor& that) const;
    };

    ObLogCursor& set_cursor(ObLogCursor& cursor, const int64_t file_id, const int64_t log_id, const int64_t offset);
    class ObAtomicLogCursor
    {
      public:
        ObAtomicLogCursor(){}
        ~ObAtomicLogCursor(){}
        int get_cursor(ObLogCursor& cursor) const;
        int set_cursor(ObLogCursor& cursor);
      private:
        ObLogCursor log_cursor_;
        mutable common::SpinRWLock cursor_lock_;
    };
  }; // end namespace common
}; // end namespace oceanbase
#endif // __OCEANBASE_COMMON_OB_LOG_CURSOR_H__
