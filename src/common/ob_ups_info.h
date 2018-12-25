/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_rs_ms_message.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_RS_MS_MESSAGE_H
#define _OB_RS_MS_MESSAGE_H 1

#include "ob_server.h"
namespace oceanbase
{
  namespace common
  {
    enum ObUpsStat
    {
      UPS_SLAVE = 0,       ///< slave
      UPS_MASTER = 1,      ///< the master
    };

    enum ObServerType
    {
      CHUNK_SERVER = 100,
      MERGE_SERVER = 200,
    };

    struct ObUpsInfo
    {
      common::ObServer addr_;
      int32_t inner_port_;
      ObUpsStat stat_;          // do not use this in 0.2.1
      int8_t ms_read_percentage_;
      int8_t cs_read_percentage_;
      int16_t reserve2_;
      int32_t reserve3_;
      ObUpsInfo();
      inline int8_t get_read_percentage(const ObServerType type = MERGE_SERVER) const;
      inline common::ObServer get_server(const ObServerType type = MERGE_SERVER) const;
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize(const char* buf, const int64_t buf_len, int64_t& pos);
    };

    inline ObUpsInfo::ObUpsInfo()
      :inner_port_(0), stat_(UPS_SLAVE),
       ms_read_percentage_(0), cs_read_percentage_(0),
       reserve2_(0), reserve3_(0)
    {
    }

    inline int8_t ObUpsInfo::get_read_percentage(const ObServerType type/*= MERGE_SERVER*/) const
    {
      int8_t percentage = 0;
      if (CHUNK_SERVER == type)
      {
        percentage = cs_read_percentage_;
      }
      else if (MERGE_SERVER == type)
      {
        percentage = ms_read_percentage_;
      }
      return percentage;
    }

    inline common::ObServer ObUpsInfo::get_server(const  ObServerType type/*= MERGE_SERVER*/) const
    {
      common::ObServer server = addr_;
      if (CHUNK_SERVER == type)
      {
        server.set_port(inner_port_);
      }
      return server;
    }

    struct ObUpsList
    {
      static const int32_t MAX_UPS_COUNT = 8;

      ObUpsInfo ups_array_[MAX_UPS_COUNT];
      int32_t ups_count_;
      int32_t reserve_;
      int32_t sum_ms_percentage_;
      int32_t sum_cs_percentage_;

      ObUpsList();
      ObUpsList& operator= (const ObUpsList &other);
      // the sum is calculated when deserialize not calculate every time
      // must deserializ at first when using this interface
      // if type not in (ObServerType) return 0
      inline int32_t get_sum_percentage(const ObServerType type = MERGE_SERVER) const;
      //add bingo [Paxos Cluster.Balance] 20161115:b
      inline void filt_by_cluster(int32_t cluster_id, ObUpsList& cluster_list);
      //add:e
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize(const char* buf, const int64_t buf_len, int64_t& pos);
      void print() const;
      void print(char* buf, const int64_t buf_len, int64_t &pos) const;
    };

    inline ObUpsList::ObUpsList()
      :ups_count_(0), reserve_(0), sum_ms_percentage_(0), sum_cs_percentage_(0)
    {
    }

    inline int32_t ObUpsList::get_sum_percentage(const ObServerType type/*= MERGE_SERVER*/) const
    {
      int32_t sum = 0;
      if (CHUNK_SERVER == type)
      {
        sum = sum_cs_percentage_;
      }
      else if (MERGE_SERVER == type)
      {
        sum = sum_ms_percentage_;
      }
      return sum;
    }

    //add bingo [Paxos Cluster.Balance] 20161115:b
    inline void ObUpsList::filt_by_cluster(int32_t cluster_id, ObUpsList& cluster_list)
    {
      int32_t count = 0;
      int32_t master_ups_cluster_id = 0;
      for (int64_t i = 0; i < ups_count_; ++i)
      {
        if(ups_array_[i].addr_.cluster_id_ == cluster_id)
        {
          cluster_list.ups_array_[count] = ups_array_[i];
          count++;
          cluster_list.sum_ms_percentage_ = cluster_list.sum_ms_percentage_ +  ups_array_[i].ms_read_percentage_;
          cluster_list.sum_cs_percentage_ = cluster_list.sum_cs_percentage_ +  ups_array_[i].cs_read_percentage_;
        }

        if(UPS_MASTER == ups_array_[i].stat_)
        {
          master_ups_cluster_id = ups_array_[i].addr_.cluster_id_;
        }
      }
      cluster_list.ups_count_ = count;

      // no ups in the cluster, or zero quantity of the cluster, move to get ups from master ups cluster
      if(count == 0 || cluster_list.sum_ms_percentage_== 0 || cluster_list.sum_cs_percentage_ == 0)
      {
        count = 0;
        for (int64_t i = 0; i < ups_count_; ++i)
        {
          if(ups_array_[i].addr_.cluster_id_ == master_ups_cluster_id)
          {
            cluster_list.ups_array_[count] = ups_array_[i];
            count++;
            cluster_list.sum_ms_percentage_ = cluster_list.sum_ms_percentage_ +  ups_array_[i].ms_read_percentage_;
            cluster_list.sum_cs_percentage_ = cluster_list.sum_cs_percentage_ +  ups_array_[i].cs_read_percentage_;
          }
        }
        cluster_list.ups_count_ = count;
        cluster_list.print();
      }

    }
    //add:e

  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_RS_MS_MESSAGE_H */

