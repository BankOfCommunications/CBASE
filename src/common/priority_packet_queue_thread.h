/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: priority_packet_queue_thread.h,v 0.1 2011/03/16 10:00:00 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - modify PacketQueueThread to support multiple task queues with different priority
 *
 */

#ifndef OCEANBASE_COMMON_PRIORITY_PACKET_QUEUE_THREAD_H
#define OCEANBASE_COMMON_PRIORITY_PACKET_QUEUE_THREAD_H

#include "ob_packet.h"
#include "ob_packet_queue.h"
#include "ob_packet_queue_handler.h"
#include "ob_trace_id.h"

namespace oceanbase {
namespace common {

class PriorityPacketQueueThread : public tbsys::CDefaultRunnable
{
public:
  enum QueuePriority {
    HIGH_PRIV = -1,
    NORMAL_PRIV = 0,
    LOW_PRIV = 1,
  };

public:
    // 构造
  PriorityPacketQueueThread();

  // 构造
  PriorityPacketQueueThread(int threadCount, ObPacketQueueHandler *handler, void *args);

  // 析构
  ~PriorityPacketQueueThread();

  // 参数设置
  void setThreadParameter(int threadCount, ObPacketQueueHandler *handler, void *args);

  // stop
  void stop(bool waitFinish = false);

  // push
  bool push(ObPacket *packet, int maxQueueLen = 0, bool block = true, int priority = NORMAL_PRIV);

  // Runnable 接口
  void run(tbsys::CThread *thread, void *arg);

  ObPacket* head(int priority)
  {
    return _queues[priority].head();
  }

  ObPacket* tail(int priority)
  {
    return _queues[priority].tail();
  }

  size_t size(int priority)
  {
    return _queues[priority].size();
  }

  size_t size()
  {
    return _queues[NORMAL_PRIV].size() + _queues[LOW_PRIV].size();
  }

  int64_t get_low_priv_cur_percent()
  {
    return _percent[LOW_PRIV];
  }
  void set_ip_port(const IpPort & ip_port);
  void set_low_priv_cur_percent(const int64_t low_priv_percent)
  {
    if (low_priv_percent <= LOW_PRIV_MAX_PERCENT && low_priv_percent >= LOW_PRIV_MIN_PERCENT)
    {
      _percent[LOW_PRIV] = static_cast<int32_t>(low_priv_percent);
      _percent[NORMAL_PRIV] = static_cast<int32_t>(100 - low_priv_percent);
      _sum = 100;
    }
    else
    {
      // set to default value
      _percent[LOW_PRIV] = 10;
      _percent[NORMAL_PRIV] = 90;
      _sum = 100;
    }
  }

private:
  // pop packet from packet queue
  ObPacket* pop_packet_(const int64_t priority);

public:
  static const int64_t LOW_PRIV_MAX_PERCENT = 90;
  static const int64_t LOW_PRIV_MIN_PERCENT = 10;

private:
  static const int64_t QUEUE_NUM = 2;
  static const int64_t DEF_WAIT_TIME_MS = 10;   // 10ms
  static const int64_t MAX_WAIT_TIME_MS = 1000; // 1000ms
  // The work thread fetches several(3, by default) normal tasks and then try to fetch a low priv task
  static const int64_t CONTINOUS_NORMAL_TASK_NUM = 3;

private:
  ObPacketQueue _queues[QUEUE_NUM];
  ObPacketQueueHandler *_handler;
  tbsys::CThreadCond _cond[QUEUE_NUM];
  tbsys::CThreadCond _pushcond[QUEUE_NUM];
  void *_args;
  bool _waitFinish;       // 等待完成

  bool _waiting[QUEUE_NUM];
  int32_t _percent[QUEUE_NUM];
  int32_t _sum;
  IpPort ip_port_;
};

}
}

#endif

