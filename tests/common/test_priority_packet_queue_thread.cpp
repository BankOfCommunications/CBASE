/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: test_priority_packet_queue_thread.cpp,v 0.1 2011/03/17 16:40:29 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include <iostream>
#include <sstream>
#include <algorithm>
#include <gtest/gtest.h>
#include "tblog.h"
#include "priority_packet_queue_thread.h"
#include "ob_packet.h"

using namespace std;
using namespace tbnet;
using namespace oceanbase::common;

class TestPacket : public tbnet::Packet
{
  public:
    TestPacket()
    {
    }
    virtual ~TestPacket()
    {
    }

  public:
    virtual bool encode(DataBuffer *)
    {
      return false;
    }

    virtual bool decode(DataBuffer *, PacketHeader *)
    {
      return false;
    }
};

namespace oceanbase
{
namespace tests
{
namespace common
{

class TestPriorityPacketQueueThread : public ::testing::Test
{
public:
  virtual void SetUp()
  {

  }

  virtual void TearDown()
  {

  }
};

TEST_F(TestPriorityPacketQueueThread, test_push)
{
  TestPacket* packet1 = new TestPacket;
  TestPacket* packet2 = new TestPacket;
  int priority = PriorityPacketQueueThread::NORMAL_PRIV;
  PriorityPacketQueueThread priority_thread;
  bool res = priority_thread.push(packet1, 1, true, priority);
  EXPECT_TRUE(res);
  res = priority_thread.push(packet2, 1, false, priority);
  EXPECT_FALSE(res);
  res = priority_thread.push(packet2, 2, false, priority);
  EXPECT_TRUE(res);

  EXPECT_TRUE(((tbnet::Packet*) packet1) == priority_thread.head(priority));
  EXPECT_TRUE(((tbnet::Packet*) packet2) == priority_thread.tail(priority));
  EXPECT_EQ(2, (int) priority_thread.size(priority));
}

} // end namespace common
} // end namespace tests
} // end namespace oceanbase


int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

