#include <unistd.h>
#include <stdint.h>
#include <iostream>
#include <ob_fifo_allocator.h>
#include "thread.h"
#include "btree_base.h"
#include "gtest/gtest.h"
#include "ob_buffer.h"

using namespace oceanbase::common;
using namespace oceanbase::common::cmbtree;
using namespace oceanbase::updateserver;

int64_t g_ObjectNumber = 10000000;
int64_t g_RunningTime  = 60;

class Number
{
  public:
    int64_t value;
    Number() : value(0) {}
    ~Number() {}
    Number(const int64_t & v) : value(v) {}
    int64_t operator - (const Number & r) const
    {
      return value - r.value;
    }
    bool operator == (const Number & r) const
    {
      return value == r.value;
    }
    bool operator != (const Number & r) const
    {
      return value != r.value;
    }
    bool operator < (const Number & r) const
    {
      return value < r.value;
    }
    bool operator > (const Number & r) const
    {
      return value > r.value;
    }
    std::string to_string() const
    {
      std::ostringstream oss;
      oss << value;
      return oss.str();
    }
    const char * to_cstring() const
    {
      static char buf[BUFSIZ];
      snprintf(buf, BUFSIZ, "%ld", value);
      return buf;
    }
};

typedef FIFOAllocator                   ALLOCATOR_TYPE;
typedef Number                          KEY_TYPE;
typedef Number                          VALUE_TYPE;
typedef BtreeBase<KEY_TYPE, VALUE_TYPE> BTREE_TYPE;

struct PutOp
{
  PutOp(KEY_TYPE k, VALUE_TYPE v) : key(k), value(v) {}
  KEY_TYPE   key;
  VALUE_TYPE value;
};

typedef ObFixedQueue<PutOp> QUEUE_TYPE;

enum CheckType
{
  GET            = 0,
  SCAN_FORWARD   = 1,
  SCAN_BACKWARD  = 2,
  CHECK_TYPE_MAX = 3,
};

const char * getCheckTypeStr(CheckType type)
{
  switch (type)
  {
    case GET:
      return "GET";
    case SCAN_FORWARD:
      return "SCAN_FORWARD";
    case SCAN_BACKWARD:
      return "SCAN_BACKWARD";
    default:
      return "UNKNOWN";
  }
}

struct CheckOp
{
  CheckType  type;
  KEY_TYPE   start_key;
  KEY_TYPE   end_key;
  KEY_TYPE   key;
  VALUE_TYPE value;
  int32_t    start_exclude;
  int32_t    end_exclude;
};

static ALLOCATOR_TYPE g_Allocator;

int64_t microseconds()
{
  struct timeval tv; 
  gettimeofday(&tv, NULL);
  return tv.tv_sec * 1000000L + tv.tv_usec;
}

class MemPoolThread
{
  public:
    void * operator new(std::size_t size) throw(std::bad_alloc)
    {
      void * p = g_Allocator.alloc(size);
      if (NULL == p)
      {
        static const std::bad_alloc nomem;
        throw (nomem);
      }
      return p;
    }

    void operator delete(void * p) throw()
    {
      if (NULL != p)
      {
        g_Allocator.free(p);
      }
    }
};

class Checker : public MemPoolThread
{
  public:
    Checker(BTREE_TYPE & b) : b_(b),
        failedCounter(CHECK_TYPE_MAX), checkCounter(CHECK_TYPE_MAX)
    {
    }
  public:
    bool check(PutOp * op)
    {
      bool ret = false;
      CheckOp * checkOp = genCheckOp(op);
      switch (checkOp->type)
      {
        case GET:
          ret = checkGet(checkOp);
          break;
        case SCAN_FORWARD:
        case SCAN_BACKWARD:
          ret = checkScan(checkOp);
          break;
        default:
          std::cerr << "internal error, unknown check type" << std::endl;
          ret = false;
      }
      if (!ret)
      {
        failedCounter[checkOp->type]++;
      }
      checkCounter[checkOp->type]++;
      delete checkOp;
      return ret;
    }
    void printCheckError(CheckOp * checkOp, const char * keys = NULL)
    {
      std::cerr << "Check error: KEY: " << checkOp->key.value
                << "  VALUE: " << checkOp->value.value
                << "  START KEY: " << checkOp->start_key.value
                << "  END KEY: " << checkOp->end_key.value
                << "  START EXCLUDE: " << checkOp->start_exclude
                << "  END EXCLUDE: " << checkOp->end_exclude
                << "  TYPE: " << getCheckTypeStr(checkOp->type)
                << "  Scanned keys: " << keys << std::endl;
    }
    bool checkGet(CheckOp * checkOp)
    {
      VALUE_TYPE value;
      int ret = b_.get(checkOp->start_key, value);
      EXPECT_EQ(ret, ERROR_CODE_OK);
      EXPECT_EQ(value, checkOp->value);
      if (ERROR_CODE_OK != ret || value != checkOp->value)
      {
        //check_buffer.appende("\0", 1);
        //printCheckError(checkOp, check_buffer.ptr());
        printCheckError(checkOp, "");
        while (true);
        return false;
      }
      else
      {
        //TBSYS_LOG(INFO, "here");
        //printCheckError(checkOp, "");
        return true;
      }
    }
    bool checkScan(CheckOp * checkOp)
    {
      bool ret = false;
      BTREE_TYPE::TScanHandle handle;
      int err = b_.get_scan_handle(handle);
      EXPECT_EQ(err, ERROR_CODE_OK);
      if (ERROR_CODE_OK != err) return false;
      err = b_.set_key_range(handle, checkOp->start_key, checkOp->start_exclude,
          checkOp->end_key, checkOp->end_exclude);
      EXPECT_EQ(err, ERROR_CODE_OK);
      if (ERROR_CODE_OK != err) return false;
      KEY_TYPE   key;
      VALUE_TYPE value;
      bool       found = false;
      sbuffer<102400>  buffer;
      err = b_.get_next(handle, key, value);
      //std::cout << "err = " << err << std::endl;
      //if (ERROR_CODE_OK == err) std::cout << key.value << std::endl;
      while (ERROR_CODE_OK == err)
      {
        buffer.appende(" ");
        buffer.appende(key.value);
        if (key == checkOp->key)
        {
          found = true;
          EXPECT_EQ(value, checkOp->value);
          if (value != checkOp->value)
          {
            buffer.append("\0", 1);
            printCheckError(checkOp, buffer.ptr());
            ret = false;
          }
          else
          {
            ret = true;
          }
        }
        else if ((SCAN_FORWARD == checkOp->type
                      && (key < checkOp->start_key || key > checkOp->end_key))
              || (SCAN_BACKWARD == checkOp->type
                      && (key > checkOp->start_key || key < checkOp->end_key)))
        {
          TBSYS_LOG(ERROR, "finding error");
          buffer.append("\0", 1);
          printCheckError(checkOp, buffer.ptr());
          //while (true);
        }
        err = b_.get_next(handle, key, value);
      }
      EXPECT_TRUE(found);
      if (!found)
      {
        buffer.append("\0", 1);
        printCheckError(checkOp, buffer.ptr());
        ret = false;
        while (true);
      }
      else
      {
        //printCheckError(checkOp, buffer.ptr());
      }
      return ret;
    }
    CheckOp * genCheckOp(PutOp * putOp)
    {
      //check_buffer.appende(" ").appende(putOp->key.value);
      CheckOp * checkOp = new CheckOp();
      int64_t step = rand() % 100;
      checkOp->type  = static_cast<CheckType>(rand() % CHECK_TYPE_MAX);
      checkOp->key   = putOp->key;
      checkOp->value = putOp->value;
      switch (checkOp->type)
      {
        case GET:
          checkOp->start_key = putOp->key;
          break;
        case SCAN_FORWARD:
          checkOp->start_key = putOp->key.value - step;
          checkOp->end_key   = putOp->key.value + step;
          if (0 != step)
          {
            checkOp->start_exclude = rand() % 2;
            checkOp->end_exclude = rand() % 2;
          }
          else
          {
            checkOp->start_exclude = 0;
            checkOp->end_exclude = 0;
          }
          break;
        case SCAN_BACKWARD:
          checkOp->start_key = putOp->key.value + step;
          checkOp->end_key   = putOp->key.value - step;
          if (0 != step)
          {
            checkOp->start_exclude = rand() % 2;
            checkOp->end_exclude = rand() % 2;
          }
          else
          {
            checkOp->start_exclude = 0;
            checkOp->end_exclude = 0;
          }
          break;
        default:
          std::cerr << "internal error, unknown check type" << std::endl;
      }
      return checkOp;
    }
  protected:
    BTREE_TYPE     & b_;
  public:
    std::vector<int64_t> failedCounter;
    std::vector<int64_t> checkCounter;
};

class PutThread : public MemPoolThread, public Thread
{
  public:
    PutThread(BTREE_TYPE & b, QUEUE_TYPE & q)
      : b_(b), q_(q), qps_(0), checker_(b), ok_(true)
    {
    }
    uint64_t rand()
    {
      return seed_ = ((seed_ * 214013L + 2531011L) >> 16);
    }
    void * run(void * arg)
    {
      int64_t counter = 0;
      int64_t start_time = microseconds();
      seed_ = microseconds();
      //sbuffer<102400> buffer;
      while (!stop_)
      {
        int64_t key = static_cast<int64_t>(rand() % g_ObjectNumber);
        //int64_t key = static_cast<int64_t>(rand());
        int64_t value = key * key;
        b_.put(Number(key), Number(value), true);
        //b_.put(Number(key), Number(value), false);
        //TBSYS_LOG(DEBUG, "%ld", key);
        //buffer.appende(" ").appende(key);
        PutOp * op = new PutOp(Number(key), Number(value));
        if (!checker_.check(op))
        {
          ok_ = false;
          while (true);
        }
        q_.push(op);
        ++counter;
        //if (counter > 200) break;
        //usleep(10000);
      }
      //buffer.appende("\0", 1);
      //TBSYS_LOG(DEBUG, "%s", buffer.ptre());
      int64_t end_time = microseconds();
      int64_t elapsed_time = end_time - start_time;
      qps_ = counter * 1000000 / elapsed_time;
      TBSYS_LOG(INFO, "Thread did QPS: %ld", qps_);
      return NULL;
    }
    int64_t get_qps() const
    {
      return qps_;
    }
  protected:
    BTREE_TYPE     & b_;
    QUEUE_TYPE     & q_;
    int64_t          qps_;
    uint64_t         seed_;
    Checker          checker_;
    bool             ok_;
};

class CheckThread : public MemPoolThread, public Thread
{
  public:
    CheckThread(BTREE_TYPE & b, QUEUE_TYPE & q)
      : b_(b), q_(q), checker_(b), ok_(true)
    {
    }
    uint64_t rand()
    {
      return seed_ = ((seed_ * 214013L + 2531011L) >> 16);
    }
    //sbuffer<102400> check_buffer;
    void * run(void * arg)
    {
      int64_t counter = 0;
      seed_ = microseconds();
      while (!stop_)
      {
        PutOp * op;
        int ret = q_.pop(op);
        if (OB_SUCCESS == ret)
        {
          if (NULL == op)
          {
            std::cerr << "internal error, pop NULL value" << std::endl;
          }
          else
          {
            counter++;
            if (!checker_.check(op))
            {
              ok_ = false;
            }
            delete op;
          }
        }
      }
      TBSYS_LOG(INFO, "Check did count: %ld", counter);
      for (int i = 0; i < CHECK_TYPE_MAX; i++)
      {
        TBSYS_LOG(INFO, "%s Failed count : %ld  Check count : %ld",
            getCheckTypeStr(static_cast<CheckType>(i)),
            checker_.failedCounter[i], checker_.checkCounter[i]);
      }
      return NULL;
    }
  protected:
    BTREE_TYPE     & b_;
    QUEUE_TYPE     & q_;
    uint64_t         seed_;
    Checker          checker_;
    bool             ok_;
};

void test(BTREE_TYPE & b)
{
  BTREE_TYPE::TScanHandle handle;
  int err = b.get_scan_handle(handle);
  err = b.set_key_range(handle, Number(152608669791997), 0,
      Number(152618669792007), 0);
  KEY_TYPE   key;
  VALUE_TYPE value;
  err = b.get_next(handle, key, value);
  while (ERROR_CODE_OK == err)
  {
    TBSYS_LOG(INFO, "K: %ld  V: %ld", key.value, value.value);
    err = b.get_next(handle, key, value);
  }
}

TEST(Btree, highly_concurrent)
{
  ob_init_memory_pool();
  g_Allocator.init(1L << 30, 1L << 26, 1L << 20);
  BTREE_TYPE btree;
  btree.init();
  QUEUE_TYPE queue;
  queue.init(1L << 20);
  int64_t cores_num = 2;//sysconf(_SC_NPROCESSORS_ONLN);
  int64_t PUT_THREAD_NUM = cores_num / 2;
  int64_t CHECK_THREAD_NUM = cores_num / 2;
  typedef std::vector<PutThread>       PutThreadVector;
  typedef std::vector<CheckThread>     CheckThreadVector;
  typedef PutThreadVector::iterator    PutThreadIter;
  typedef CheckThreadVector::iterator  CheckThreadIter;
  PutThreadVector   pt(PUT_THREAD_NUM, PutThread(btree, queue));
  CheckThreadVector ct(CHECK_THREAD_NUM, CheckThread(btree, queue));
  for (PutThreadIter i = pt.begin(); i != pt.end(); i++)
  {
    i->start();
  }
  for (CheckThreadIter i = ct.begin(); i != ct.end(); i++)
  {
    i->start();
  }
  sleep(g_RunningTime);
  for (PutThreadIter i = pt.begin(); i != pt.end(); i++)
  {
    i->stop();
  }
  for (CheckThreadIter i = ct.begin(); i != ct.end(); i++)
  {
    i->stop();
  }
  for (PutThreadIter i = pt.begin(); i != pt.end(); i++)
  {
    i->wait();
  }
  for (CheckThreadIter i = ct.begin(); i != ct.end(); i++)
  {
    i->wait();
  }
  btree.dump_mem_info();
  std::cout << "OBJECT COUNTEER: " << btree.get_object_count() << std::endl;
  //test(btree);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  if (argc >= 2)
  {
    g_ObjectNumber = atol(argv[1]);
  }
  if (argc >= 3)
  {
    g_RunningTime = atol(argv[2]);
  }
  return RUN_ALL_TESTS();
}
