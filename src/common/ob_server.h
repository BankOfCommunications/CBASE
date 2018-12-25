
/*
 *   (C) 2007-2010 Taobao Inc.
 *
 *   This program is free software; you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License version 2 as
 *   published by the Free Software Foundation.
 *
 *
 *
 *   Version: 0.1
 *
 *   Authors:
 *      qushan <qushan@taobao.com>
 *        - base data structure, maybe modify in future
 *
 */
#ifndef OCEANBASE_COMMON_OB_SERVER_H_
#define OCEANBASE_COMMON_OB_SERVER_H_

#include <assert.h>
#include "ob_define.h"
#include "serialization.h"

namespace oceanbase
{
  namespace common
  {
    class ObServer
    {
      public:
        static const int32_t IPV4 = 4;
        static const int32_t IPV6 = 6;
        static const int64_t MAX_IP_PORT_SQL_LENGTH = MAX_IP_ADDR_LENGTH + 10;      // copy from 0.5

        ObServer()
          :
          //add lbzhong [Paxos Cluster.Balance] 20160706:b
          cluster_id_(0),
          //add:e
          version_(IPV4), port_(0)

        {
          this->ip.v4_ = 0;
          memset(&this->ip.v6_, 0xA6, sizeof(ip.v6_));
        }

        ObServer(const int32_t version, const char* ip, const int32_t port)
        {
          assert(version == IPV4 || version == IPV6);
          if (version == IPV4)
            set_ipv4_addr(ip, port);
          // TODO ipv6 addr?
          //add lbzhong [Paxos Cluster.Balance] 20160706:b
          cluster_id_ = 0;
          //add:e
        }

        void reset()
        {
          port_ = 0;
          this->ip.v4_ = 0;
          memset(&this->ip.v6_, 0, sizeof(ip.v6_));
          //add lbzhong [Paxos Cluster.Balance] 20160706:b
          cluster_id_ = 0;
          //add:e
        }

        static uint32_t convert_ipv4_addr(const char *ip);

        int64_t to_string(char* buffer, const int64_t size) const;
        bool ip_to_string(char* buffer, const int32_t size) const;
        const char* to_cstring() const; // use this carefully, the content of the returned buffer will be modified by the next call

        bool set_ipv6_addr(const char* ip, const int32_t port);     // copy from 0.5
        bool set_ipv4_addr(const char* ip, const int32_t port);
        bool set_ipv4_addr(const int32_t ip, const int32_t port);

        int64_t get_ipv4_server_id() const;

        bool operator ==(const ObServer& rv) const;
        bool operator !=(const ObServer& rv) const;
        bool operator < (const ObServer& rv) const;
        bool compare_by_ip(const ObServer& rv) const;
        bool is_same_ip(const ObServer& rv) const;
        int32_t get_version() const;
        int32_t get_port() const;
        uint32_t get_ipv4() const;
        uint64_t get_ipv6_high() const;
        uint64_t get_ipv6_low() const;
        void set_port(int32_t port);
        void set_max();
        bool is_valid() const;
        void reset_ipv4_10(int ip = 10);
        int parse_from_cstring(const char* ip_str);   // copy from 0.5
        //add bingo [Paxos MS restart] 20170306:b
        void set_cluster_id(int32_t cluster_id);
        //add:e

        NEED_SERIALIZE_AND_DESERIALIZE;
        //add lbzhong [Paxos Cluster.Balance] 20160706:b
        //public
        int32_t cluster_id_;  //not used in compare operators
        //add:e
      private:
        int32_t version_;
        int32_t port_;
        struct {
          uint32_t v4_;
          uint32_t v6_[4];
        } ip;
    };
    //add hongchen [UNLIMIT_TABLE] 20161031:b
    struct ServerSession
    {
      ObServer server_;
      int64_t session_id_;
    };
    //add hongchen [UNLIMIT_TABLE] 20161031:e
  } // end namespace common
} // end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_SERVER_H_

