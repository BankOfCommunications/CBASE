/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tc_factory_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include <gtest/gtest.h>
#include "common/ob_tc_factory.h"
using namespace oceanbase::common;

class Base: public DLink
{
  public:
    Base(){};
    virtual ~Base(){};
    virtual int32_t get_type() = 0;
  private:
    // types and constants
  private:
    // disallow copy
    Base(const Base &other);
    Base& operator=(const Base &other);
    // function members
  private:
    // data members
    int32_t a_;
};

typedef ObGlobalFactory<Base, 2, 1> base_global_factory_t;
typedef ObTCFactory<Base, 2, 1> base_tc_factory_t;

class D1 : public Base
{
  public:
    D1(){};
    virtual ~D1(){};
    virtual int32_t get_type() {return 0;};
  private:
    // types and constants
  private:
    // disallow copy
    D1(const D1 &other);
    D1& operator=(const D1 &other);
    // function members
  private:
    // data members
    int32_t d1_;
};

REGISTER_CREATOR(base_global_factory_t, Base, D1, 0);

class D2 : public Base
{
  public:
    D2(){};
    virtual ~D2(){};
    virtual int32_t get_type() {return 1;};
  private:
    // types and constants
  private:
    // disallow copy
    D2(const D2 &other);
    D2& operator=(const D2 &other);
    // function members
  private:
    // data members
    int32_t d2_;
    uint64_t d3_;
};

REGISTER_CREATOR(base_global_factory_t, Base, D2, 1);

class ObTCFactoryTest: public ::testing::Test
{
  public:
    ObTCFactoryTest();
    virtual ~ObTCFactoryTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObTCFactoryTest(const ObTCFactoryTest &other);
    ObTCFactoryTest& operator=(const ObTCFactoryTest &other);
  protected:
    // data members
};

ObTCFactoryTest::ObTCFactoryTest()
{
}

ObTCFactoryTest::~ObTCFactoryTest()
{
}

void ObTCFactoryTest::SetUp()
{
}

void ObTCFactoryTest::TearDown()
{
}

TEST_F(ObTCFactoryTest, basic_test)
{
  for (int i = 0; i < 1000000; ++i)
  {
    Base *b1 = base_tc_factory_t::get_instance()->get(0);
    ASSERT_TRUE(NULL != b1);
    ASSERT_TRUE(NULL != dynamic_cast<D1*>(b1));
    base_tc_factory_t::get_instance()->put(b1);

    Base *b2 = base_tc_factory_t::get_instance()->get(1);
    ASSERT_TRUE(NULL != b2);
    ASSERT_TRUE(NULL != dynamic_cast<D2*>(b2));
    base_tc_factory_t::get_instance()->put(b2);
    // if (i % 10000 == 0)
    // {
    //   base_global_factory_t::get_instance()->stat();
    //   base_tc_factory_t::get_instance()->stat();
    // }
  }
}

class TestRunnable : public tbsys::CDefaultRunnable
{
  public:
    void run(tbsys::CThread *thread, void *arg)
  {
    UNUSED(arg);
    TBSYS_LOG(DEBUG, "start thread %p", thread);
    for (int i = 0; i < 1000000; ++i)
    {
      Base *b1 = base_tc_factory_t::get_instance()->get(0);
      ASSERT_TRUE(NULL != b1);
      ASSERT_TRUE(NULL != dynamic_cast<D1*>(b1));
      base_tc_factory_t::get_instance()->put(b1);

      Base *b2 = base_tc_factory_t::get_instance()->get(1);
      ASSERT_TRUE(NULL != b2);
      ASSERT_TRUE(NULL != dynamic_cast<D2*>(b2));
      base_tc_factory_t::get_instance()->put(b2);
    }
    base_tc_factory_t::get_instance()->stat();
  }
};

TEST_F(ObTCFactoryTest, mt_basic_test)
{
  TestRunnable mt[50];
  for (int i = 0; i < 50; ++i)
  {
    mt[i].start();
  }

  for (int i = 0; i < 50; ++i)
  {
    mt[i].stop();
    mt[i].wait();
  }
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  int err = base_global_factory_t::get_instance()->init();

  if (0 != err)
  {
    fprintf(stderr, "failed to init global factory\n");
    abort();
  }
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
