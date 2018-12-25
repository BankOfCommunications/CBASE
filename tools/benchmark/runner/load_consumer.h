/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: load_consumer.h,v 0.1 2012/03/15 14:00:00 zhidong Exp $
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef LOAD_CONSUMER_H_
#define LOAD_CONSUMER_H_

#include <mysql/mysql.h>
#include "tbsys.h"
#include "load_param.h"
#include "common/hash/ob_hashmap.h"
namespace oceanbase
{
  namespace tools
  {
    struct QueryInfo;
    class LoadRpc;
    class LoadManager;
    union CountType
    {
      uint8_t value_8;
      uint16_t value_16;
      uint32_t value_32;
      uint64_t value_64;
    };
    class LoadConsumer:public tbsys::CDefaultRunnable
    {
      public:
        LoadConsumer();
        virtual ~LoadConsumer();
      public:
        void init(const ControlParam & param,LoadManager * manager);
        void end();
        virtual void run(tbsys::CThread* thread, void* arg);
      private:
        int do_com_query(const struct iovec *iovect, const int count, common::hash::ObHashMap<int, MYSQL> &fd_connect);
        int do_com_prepare(const struct iovec *iovect, const int count, common::hash::ObHashMap<int, MYSQL> &fd_connect,
            common::hash::ObHashMap<std::pair<int, uint32_t>, uint32_t> &statement_id);
        int do_com_execute(const struct iovec *iovect, const int count, common::hash::ObHashMap<int, MYSQL> &fd_connect,
            common::hash::ObHashMap<std::pair<int, uint32_t>, uint32_t> &statement_id);
        int do_com_close_stmt(const struct iovec *iovect, const int count, common::hash::ObHashMap<int, MYSQL> &fd_connect,
            common::hash::ObHashMap<std::pair<int, uint32_t>, uint32_t> &statement_map);
        int do_login(const struct iovec *iovect, const int count, common::hash::ObHashMap<int, MYSQL> &fd_connect);
        int do_logout(const struct iovec *iovect, const int count, common::hash::ObHashMap<int, MYSQL> &fd_connect);

        int write_stmt_packet_to_fd(int fd, const struct iovec *iovect, const int count, const uint32_t statement_id);
        int write_packet_to_fd(int fd, const struct iovec *iovect, const int count);
        int read_packet(const int fd, char* buf, const int64_t buf_lenth);
        int read_fd(const int fd, char* buf, const int64_t size, const int64_t buf_lenth);
        int read_from_fd(int fd, int cmd_type);
        int read_close_stmt_result(const int fd);
        int read_execute_result(const int fd);
        int read_prepare_result(const int fd, uint32_t &statement_id);
        int read_query_result(const int fd);
      private:
        int64_t times_;
        int64_t index_;
        int64_t count_;
        int64_t time_;
        common::hash::ObHashMap<int, MYSQL> fd_connect_;
        ControlParam param_;
        LoadManager * manager_;
    };
  }
}

#endif // LOAD_CONSUMER_H_
