/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: batch_packet_queue_thread.h,v 0.1 2010/09/30 10:00:00 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - modify PacketQueueThread to support use mode of UpdateServer row mutation
 *
 */

#ifndef OCEANBASE_COMMON_BATCH_PACKET_QUEUE_THREAD_H
#define OCEANBASE_COMMON_BATCH_PACKET_QUEUE_THREAD_H

#include "ob_packet.h"
#include "ob_packet_queue.h"
#include "ob_switch.h"

namespace oceanbase {
namespace common {

// packet queue的处理线程
class IBatchPacketQueueHandler {
public:
    virtual ~IBatchPacketQueueHandler() {}
    virtual bool handleBatchPacketQueue(const int64_t packet_num, ObPacket** packet, void *args) = 0;
};

class BatchPacketQueueThread : public tbsys::CDefaultRunnable {
public:
    // 构造
    BatchPacketQueueThread();

    // 构造
    BatchPacketQueueThread(int threadCount, IBatchPacketQueueHandler *handler, void *args);

    // 析构
    ~BatchPacketQueueThread();

    // 参数设置
    void setThreadParameter(int threadCount, IBatchPacketQueueHandler *handler, void *args);

    // stop
    void stop(bool waitFinish = false);

    void clear();

    // push
    bool push(ObPacket *packet, int maxQueueLen = 0, bool block = true);

    // pushQueue
    void pushQueue(ObPacketQueue &packetQueue, int maxQueueLen = 0);

    // Runnable 接口
    void run(tbsys::CThread *thread, void *arg);

    // 是否计算处理速度
    void setStatSpeed();

    // 设置限速
    void setWaitTime(int t);

    ObPacket *head()
    {
        return _queue.head();
    }
    ObPacket *tail()
    {
        return _queue.tail();
    }
    size_t size()
    {
        return _queue.size();
    }

    bool notify_state_change()
    {
      return switch_.wait_on();
    }
    bool wait_state_change_ack()
    {
      return switch_.wait_off();
    }
public:
    static const int64_t MAX_BATCH_NUM = 1024;

private:
    //void PacketQueueThread::checkSendSpeed()
    void checkSendSpeed();

private:
    ObSwitch switch_;
    ObPacketQueue _queue;
    IBatchPacketQueueHandler *_handler;
    tbsys::CThreadCond _cond;
    tbsys::CThreadCond _pushcond;
    void *_args;
    bool _waitFinish;       // 等待完成

    // 限制发送速度
    int _waitTime;
    int64_t _speed_t1;
    int64_t _speed_t2;
    int64_t _overage;

    // 是否正在等待
    bool _waiting;
};

}
}

#endif

