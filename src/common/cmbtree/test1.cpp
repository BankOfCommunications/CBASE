#include <ob_malloc.h>
#include "btree_base.h"
#include "gtest/gtest.h"
#include "thread.h"

using namespace oceanbase::common;
using namespace oceanbase::common::cmbtree;

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
    Number operator* (Number & r)
    {
      return Number(value * r.value);
    }
    Number& operator= (int64_t & a)
    {
      value = a;
      return *this;
    }
    Number& operator+= (int64_t a)
    {
      value += a;
      return *this;
    }
    Number& operator++ ()
    {
      value ++;
      return *this;
    }
    Number operator++ (int)
    {
      Number o = *this;
      value ++;
      return o;
    }
    bool operator< (const Number & r) const
    {
      return value < r.value;
    }
    bool operator<= (const Number & r) const
    {
      return value <= r.value;
    }
    bool operator== (const Number & r) const
    {
      return value == r.value;
    }
};

typedef BtreeBase<int64_t, int64_t> INT_BTREE;
typedef BtreeBase<Number,  Number> NUMBER_BTREE;

TEST(Btree, basic_exception1)
{
  INT_BTREE btree;
  int64_t k = 0;
  int64_t v = 0;
  ASSERT_EQ(ERROR_CODE_FAIL, btree.put(10, 10));
  ASSERT_EQ(ERROR_CODE_FAIL, btree.get(10, v));
  ASSERT_EQ(ERROR_CODE_FAIL, btree.get_min_key(k));
  ASSERT_EQ(ERROR_CODE_FAIL, btree.get_max_key(k));
  {
    INT_BTREE::TScanHandle handle;
    ASSERT_EQ(ERROR_CODE_FAIL, btree.get_scan_handle(handle));
    ASSERT_EQ(ERROR_CODE_FAIL, btree.set_key_range(handle, 10, 0, 20, 0));
    ASSERT_EQ(ERROR_CODE_FAIL, btree.get_next(handle, k, v));
    ASSERT_EQ(0, btree.get_object_count());
    btree.clear();
    ASSERT_EQ(ERROR_CODE_FAIL, btree.reset());
    btree.destroy();
  }
  ASSERT_EQ(ERROR_CODE_OK, btree.init());
  ASSERT_EQ(ERROR_CODE_FAIL, btree.init());
  ASSERT_EQ(ERROR_CODE_NOT_FOUND, btree.get(10, v));
  ASSERT_EQ(ERROR_CODE_NOT_FOUND, btree.get_min_key(k));
  ASSERT_EQ(ERROR_CODE_NOT_FOUND, btree.get_max_key(k));
  {
    INT_BTREE::TScanHandle handle;
    ASSERT_EQ(ERROR_CODE_OK, btree.get_scan_handle(handle));
    ASSERT_EQ(ERROR_CODE_OK, btree.set_key_range(handle, 10, 0, 20, 0));
    ASSERT_EQ(ERROR_CODE_NOT_FOUND, btree.get_next(handle, k, v));
  }
  ASSERT_EQ(0, btree.get_object_count());
  btree.clear();
  ASSERT_EQ(ERROR_CODE_NOT_FOUND, btree.get(10, v));
  ASSERT_EQ(ERROR_CODE_OK, btree.reset());
  ASSERT_EQ(ERROR_CODE_OK, btree.put(10, 10));
  ASSERT_EQ(ERROR_CODE_OK, btree.get_min_key(k));
  ASSERT_EQ(10, k);
  ASSERT_EQ(ERROR_CODE_OK, btree.get_max_key(k));
  ASSERT_EQ(10, k);
  INT_BTREE::TScanHandle handle;
  ASSERT_EQ(ERROR_CODE_OK, btree.get_scan_handle(handle));
  ASSERT_EQ(ERROR_CODE_OK, btree.set_key_range(handle, 10, 0, 20, 0));
  ASSERT_EQ(ERROR_CODE_OK, btree.get_next(handle, k, v));
  ASSERT_EQ(10, k);
  ASSERT_EQ(10, v);
  ASSERT_EQ(ERROR_CODE_NOT_FOUND, btree.get_next(handle, k, v));
  handle.reset();
  ASSERT_EQ(1, btree.get_object_count());
  btree.clear();
  ASSERT_EQ(0, btree.get_object_count());
  ASSERT_EQ(ERROR_CODE_OK, btree.reset());
  ASSERT_EQ(ERROR_CODE_OK, btree.reset());
}

bool checkKeys(NUMBER_BTREE & btree, int64_t StartKey, int64_t EndKey,
    int32_t StartExclude, int32_t EndExclude, int64_t MinKey, int64_t MaxKey)
{
  bool ret = true;
  int32_t err = ERROR_CODE_OK;
  Number k(0);
  Number v(0);
  NUMBER_BTREE::TScanHandle handle;
  if (ERROR_CODE_OK != btree.get_scan_handle(handle))
  {
    ret = false;
    CMBTREE_LOG(ERROR, "get_scan_handle error");
  }
  else if (ERROR_CODE_OK != 
      btree.set_key_range(handle, Number(StartKey), StartExclude, Number(EndKey), EndExclude))
  {
    ret = false;
    CMBTREE_LOG(ERROR, "set_key_range error");
  }
  else
  {
    int64_t StartKeyOut, EndKeyOut, step;
    bool loop = true;
    if (StartKey < EndKey)
    {
      step = 1;
      if (StartKey < MinKey) StartKeyOut = MinKey;
      else StartKeyOut = StartKey + StartExclude;
      if (EndKey > MaxKey) EndKeyOut = MaxKey;
      else EndKeyOut = EndKey - EndExclude;
      if (StartKeyOut > EndKeyOut) loop = false;
    }
    else
    {
      step = -1;
      if (StartKey > MaxKey) StartKeyOut = MaxKey;
      else StartKeyOut = StartKey - StartExclude;
      if (EndKey < MinKey) EndKeyOut = MinKey;
      else EndKeyOut = EndKey + EndExclude;
      if (StartKeyOut < EndKeyOut) loop = false;
    }
    int64_t key = StartKeyOut;
    while (loop)
    {
      if (key % 2 == 1)
      {
        if (ERROR_CODE_OK != (err = btree.get_next(handle, k, v)))
        {
          ret = false;
          CMBTREE_LOG(ERROR, "get_next error "
              "StartKey: %ld EndKey: %ld StartExclude: %d EndExclude: %d "
              "MinKey: %ld MaxKey: %ld StartKeyOut: %ld EndKeyOut: %ld "
              "key: %ld k: %ld v: %ld err: %d",
              StartKey, EndKey, StartExclude, EndExclude, MinKey, MaxKey,
              StartKeyOut, EndKeyOut, key, k.value, v.value, err);
        }
        else if (key != k.value)
        {
          ret = false;
          CMBTREE_LOG(ERROR, "key error: key = %ld k = %ld",
              key, k.value);
          CMBTREE_LOG(ERROR, "key error "
              "StartKey: %ld EndKey: %ld StartExclude: %d EndExclude: %d "
              "MinKey: %ld MaxKey: %ld StartKeyOut: %ld EndKeyOut: %ld "
              "key: %ld k: %ld v: %ld err: %d",
              StartKey, EndKey, StartExclude, EndExclude, MinKey, MaxKey,
              StartKeyOut, EndKeyOut, key, k.value, v.value, err);
        }
      }
      if (key == EndKeyOut)
      {
        break;
      }
      key += step;
    }
    if (ERROR_CODE_NOT_FOUND != (err = btree.get_next(handle, k, v)))
    {
      ret = false;
      CMBTREE_LOG(ERROR, "get_next error, should return not_found, "
          "StartKey: %ld EndKey: %ld StartExclude: %d EndExclude: %d "
          "MinKey: %ld MaxKey: %ld StartKeyOut: %ld EndKeyOut: %ld "
          "k: %ld v: %ld err: %d",
          StartKey, EndKey, StartExclude, EndExclude, MinKey, MaxKey,
          StartKeyOut, EndKeyOut, k.value, v.value, err);
    }
    else if (ERROR_CODE_NOT_FOUND != btree.get_next(handle, k, v))
    {
      ret = false;
      CMBTREE_LOG(ERROR, "get_next error, should return not_found, "
          "StartKey: %ld EndKey: %ld StartExclude: %d EndExclude: %d "
          "MinKey: %ld MaxKey: %ld StartKeyOut: %ld EndKeyOut: %ld "
          "k: %ld v: %ld err: %d",
          StartKey, EndKey, StartExclude, EndExclude, MinKey, MaxKey,
          StartKeyOut, EndKeyOut, k.value, v.value, err);
    }
  }
  return ret;
}

bool checkKeys(NUMBER_BTREE & btree, int64_t StartKey, int64_t EndKey,
    int32_t StartExclude, int32_t EndExclude)
{
  int64_t MinKey, MaxKey;
  if (StartKey < EndKey)
  {
    MinKey = StartKey;
    MaxKey = EndKey;
  }
  else
  {
    MinKey = EndKey;
    MaxKey = StartKey;
  }
  return checkKeys(btree, StartKey, EndKey, StartExclude, EndExclude, MinKey, MaxKey);
}

TEST(Btree, scan1)
{
  NUMBER_BTREE btree;
  Number k(0);
  Number v(0);
  ASSERT_EQ(ERROR_CODE_OK, btree.init());
  int64_t MinKey = 11;
  int64_t MaxKey = 181;
  for (k = MinKey; k <= MaxKey; k += 2)
  {
    ASSERT_EQ(ERROR_CODE_OK, btree.put(k, k * k));
  }
  btree.print();

  for (int32_t StartExclude = 0; StartExclude <= 1; StartExclude++)
  {
    for (int32_t EndExclude = 0; EndExclude <= 1; EndExclude++)
    {
      // 顺序扫描，StartKey, EndKey都存在
      ASSERT_TRUE(checkKeys(btree, 13, 17, StartExclude, EndExclude));
      // 顺序扫描，StartKey不存在, EndKey存在
      ASSERT_TRUE(checkKeys(btree, 12, 17, StartExclude, EndExclude));
      // 顺序扫描，StartKey存在, EndKey不存在
      ASSERT_TRUE(checkKeys(btree, 13, 18, StartExclude, EndExclude));
      // 顺序扫描，StartKey, EndKey都不存在
      ASSERT_TRUE(checkKeys(btree, 12, 18, StartExclude, EndExclude));
    }
  }

  for (int32_t StartExclude = 0; StartExclude <= 1; StartExclude++)
  {
    for (int32_t EndExclude = 0; EndExclude <= 1; EndExclude++)
    {
      // 逆序扫描，StartKey, EndKey都存在
      ASSERT_TRUE(checkKeys(btree, 17, 13, StartExclude, EndExclude));
      // 逆序扫描，StartKey不存在, EndKey存在
      ASSERT_TRUE(checkKeys(btree, 18, 13, StartExclude, EndExclude));
      // 逆序扫描，StartKey存在, EndKey不存在
      ASSERT_TRUE(checkKeys(btree, 17, 12, StartExclude, EndExclude));
      // 逆序扫描，StartKey, EndKey都不存在
      ASSERT_TRUE(checkKeys(btree, 18, 12, StartExclude, EndExclude));
    }
  }

  for (int32_t StartExclude = 0; StartExclude <= 1; StartExclude++)
  {
    for (int32_t EndExclude = 0; EndExclude <= 1; EndExclude++)
    {
      // 顺序扫描，StartKey, EndKey都比MinKey小
      ASSERT_TRUE(checkKeys(btree, MinKey - 7, MinKey - 2, StartExclude, EndExclude, MinKey, MaxKey));
      // 顺序扫描，StartKey比MinKey小, EndKey是MinKey
      ASSERT_TRUE(checkKeys(btree, MinKey - 7, MinKey, StartExclude, EndExclude, MinKey, MaxKey));
      // 顺序扫描，StartKey比MinKey小, EndKey比MinKey大
      ASSERT_TRUE(checkKeys(btree, MinKey - 7, MinKey + 2, StartExclude, EndExclude, MinKey, MaxKey));
      // 顺序扫描，StartKey比MinKey小, EndKey是MaxKey
      ASSERT_TRUE(checkKeys(btree, MinKey - 7, MaxKey, StartExclude, EndExclude, MinKey, MaxKey));
      // 顺序扫描，StartKey比MinKey小, EndKey比MaxKey大
      ASSERT_TRUE(checkKeys(btree, MinKey - 7, MaxKey + 11, StartExclude, EndExclude, MinKey, MaxKey));
      // 顺序扫描，StartKey是MinKey, EndKey比MinKey大
      ASSERT_TRUE(checkKeys(btree, MinKey, MinKey + 5, StartExclude, EndExclude, MinKey, MaxKey));
      // 顺序扫描，StartKey是MinKey, EndKey是MaxKey
      ASSERT_TRUE(checkKeys(btree, MinKey, MinKey, StartExclude, EndExclude, MinKey, MaxKey));
      // 顺序扫描，StartKey是MinKey, EndKey比MaxKey大
      ASSERT_TRUE(checkKeys(btree, MinKey, MaxKey + 11, StartExclude, EndExclude, MinKey, MaxKey));
      // 顺序扫描，StartKey比MinKey大, EndKey是MaxKey
      ASSERT_TRUE(checkKeys(btree, MinKey + 3, MaxKey, StartExclude, EndExclude, MinKey, MaxKey));
      // 顺序扫描，StartKey比MinKey大, EndKey比MaxKey大
      ASSERT_TRUE(checkKeys(btree, MinKey + 3, MaxKey + 11, StartExclude, EndExclude, MinKey, MaxKey));
      // 顺序扫描，StartKey是MaxKey, EndKey比MaxKey大
      ASSERT_TRUE(checkKeys(btree, MaxKey, MaxKey + 11, StartExclude, EndExclude, MinKey, MaxKey));
      // 顺序扫描，StartKey, EndKey都比MaxKey大
      ASSERT_TRUE(checkKeys(btree, MaxKey + 5, MaxKey + 11, StartExclude, EndExclude, MinKey, MaxKey));
    }
  }

  for (int32_t StartExclude = 0; StartExclude <= 1; StartExclude++)
  {
    for (int32_t EndExclude = 0; EndExclude <= 1; EndExclude++)
    {
      // 逆序扫描，StartKey, EndKey都比MinKey小
      ASSERT_TRUE(checkKeys(btree, MinKey - 2, MinKey - 7, StartExclude, EndExclude, MinKey, MaxKey));
      // 逆序扫描，StartKey是MinKey, EndKey比MinKey小
      ASSERT_TRUE(checkKeys(btree, MinKey, MinKey - 7, StartExclude, EndExclude, MinKey, MaxKey));
      // 逆序扫描，StartKey比MinKey大, EndKey比MinKey小
      ASSERT_TRUE(checkKeys(btree, MinKey + 2, MinKey - 7, StartExclude, EndExclude, MinKey, MaxKey));
      // 逆序扫描，StartKey是MaxKey, EndKey比MinKey小
      ASSERT_TRUE(checkKeys(btree, MaxKey, MinKey - 7, StartExclude, EndExclude, MinKey, MaxKey));
      // 逆序扫描，StartKey比MaxKey大, EndKey比MinKey小
      ASSERT_TRUE(checkKeys(btree, MaxKey + 11, MinKey - 7, StartExclude, EndExclude, MinKey, MaxKey));
      // 逆序扫描，StartKey比MinKey大, EndKey是MinKey
      ASSERT_TRUE(checkKeys(btree, MinKey + 5, MinKey, StartExclude, EndExclude, MinKey, MaxKey));
      // 逆序扫描，StartKey是MaxKey, EndKey是MinKey
      ASSERT_TRUE(checkKeys(btree, MinKey, MinKey, StartExclude, EndExclude, MinKey, MaxKey));
      // 逆序扫描，StartKey比MaxKey大, EndKey是MinKey
      ASSERT_TRUE(checkKeys(btree, MaxKey + 11, MinKey, StartExclude, EndExclude, MinKey, MaxKey));
      // 逆序扫描，StartKey是MaxKey, EndKey比MinKey大
      ASSERT_TRUE(checkKeys(btree, MaxKey, MinKey + 3, StartExclude, EndExclude, MinKey, MaxKey));
      // 逆序扫描，StartKey比MaxKey大, EndKey比MinKey大
      ASSERT_TRUE(checkKeys(btree, MaxKey + 11, MinKey + 3, StartExclude, EndExclude, MinKey, MaxKey));
      // 逆序扫描，StartKey比MaxKey大, EndKey是MaxKey
      ASSERT_TRUE(checkKeys(btree, MaxKey + 11, MaxKey, StartExclude, EndExclude, MinKey, MaxKey));
      // 逆序扫描，StartKey, EndKey都比MaxKey大
      ASSERT_TRUE(checkKeys(btree, MaxKey + 11, MaxKey + 5, StartExclude, EndExclude, MinKey, MaxKey));
    }
  }

  for (int64_t StartKey = MinKey - 10; StartKey <= MaxKey + 10; StartKey++)
  {
    for (int64_t EndKey = MinKey - 10; EndKey <= MaxKey + 10; EndKey++)
    {
      for (int32_t StartExclude = 0; StartExclude <= 1; StartExclude++)
      {
        for (int32_t EndExclude = 0; EndExclude <= 1; EndExclude++)
        {
          ASSERT_TRUE(checkKeys(btree, StartKey, EndKey, StartExclude, EndExclude, MinKey, MaxKey));
        }
      }
    }
  }
}

class MockAlloc
{
  public:
    MockAlloc() : bOOM_(false)
    {
    }
    void * alloc(int64_t size)
    {
      if (!bOOM_)
      {
        return ::malloc(size);
      }
      else
      {
        return NULL;
      }
    }
    void free(void * p)
    {
      return ::free(p);
    }
    void setOOM()
    {
      bOOM_ = true;
    }
    void setNoOOM()
    {
      bOOM_ = false;
    }
  protected:
    bool bOOM_;
};

typedef BtreeBase<Number, Number, MockAlloc> NUMBER_ALLOC_BTREE;

class DestroyThread : public Thread
{
  public:
    DestroyThread(NUMBER_ALLOC_BTREE & btree)
      : btree_(btree)
    {
    }
    virtual void * run(void * arg)
    {
      UNUSED(arg);
      btree_.destroy();
      return NULL;
    }
  protected:
    NUMBER_ALLOC_BTREE & btree_;
};

TEST(Btree, OOM1)
{
  MockAlloc oMA;
  NUMBER_ALLOC_BTREE btree(oMA);
  ASSERT_EQ(ERROR_CODE_OK, btree.init());
  for (Number k = 1; k <= 100; k += 2)
  {
    ASSERT_EQ(ERROR_CODE_OK, btree.put(k, k * k));
  }
  btree.print();
  oMA.setOOM();
  DestroyThread dt(btree);
  dt.start();
  dt.wait();
}

TEST(Btree, OOM2)
{
  MockAlloc oMA;
  NUMBER_ALLOC_BTREE btree(oMA);
  ASSERT_EQ(ERROR_CODE_OK, btree.init());
  ASSERT_EQ(ERROR_CODE_OK, btree.put(0, 0));
  oMA.setOOM();
  for (Number k = 1; k <= 127088; k+=1)
  {
    int ret = btree.put(k, k * k);
    if (ERROR_CODE_OK != ret && ERROR_CODE_ALLOC_FAIL != ret)
    {
      TBSYS_LOG(ERROR, "ret: %d", ret);
      ASSERT_TRUE(false);
    }
  }
  //btree.print();
  DestroyThread dt(btree);
  dt.start();
  dt.wait();
}

class TraceAllocator
{
  public:
    TraceAllocator()
      : alloced_size(0)
    {
    }
    void * alloc(int64_t size)
    {
      alloced_size += size;
      return ::malloc(size);
    }
    void free(void * p)
    {
      return ::free(p);
    }
  public:
    int64_t alloced_size;
};

typedef BtreeBase<Number, Number, TraceAllocator> NUMBER_TRACE_BTREE;

TEST(Btree, get_alloc_memory)
{
  TraceAllocator alloc;
  NUMBER_TRACE_BTREE btree(alloc);
  ASSERT_EQ(ERROR_CODE_OK, btree.init());
  ASSERT_EQ(ERROR_CODE_OK, btree.put(0, 0));
  ASSERT_EQ(alloc.alloced_size, btree.get_alloc_memory());
  for (Number k = 1; k <= 1000; k++)
  {
    ASSERT_EQ(ERROR_CODE_OK, btree.put(k, k * k));
  }
  ASSERT_EQ(alloc.alloced_size, btree.get_alloc_memory());
  for (Number k = 1; k <= 1000; k++)
  {
    Number v;
    ASSERT_EQ(ERROR_CODE_OK, btree.get(k, v));
    ASSERT_EQ(k * k, v);
  }
  ASSERT_EQ(alloc.alloced_size, btree.get_alloc_memory());
}

TEST(Btree, get_reserved_memory)
{
  TraceAllocator alloc;
  NUMBER_TRACE_BTREE btree(alloc);
  const int64_t RecycleNodeSize = static_cast<int64_t>(
      sizeof(BtreeRecycleNode<Number, Number>));
  const int64_t BtreeNodeSize = static_cast<int64_t>(
      std::max(sizeof(BtreeInterNode<Number, Number>),
               sizeof(BtreeLeafNode<Number, Number>)));
  ASSERT_EQ(ERROR_CODE_OK, btree.init());
  ASSERT_EQ(RecycleNodeSize, btree.get_reserved_memory());

  ASSERT_EQ(ERROR_CODE_OK, btree.put(0, 0));
  // one recycle node in recycle pool, one recycle node in memory pool
  ASSERT_EQ(RecycleNodeSize * 2, btree.get_reserved_memory());

  ASSERT_EQ(ERROR_CODE_OK, btree.put(1, 1));
  // one recycle node in recycle pool, one recycle node in memory pool
  // and one btree node in recycle pool
  ASSERT_EQ(RecycleNodeSize * 2 + BtreeNodeSize, btree.get_reserved_memory());

  ASSERT_EQ(ERROR_CODE_OK, btree.put(2, 2));
  // one recycle node in recycle pool, one recycle node in memory pool
  // and one btree node in recycle pool, one btree node in memory pool
  ASSERT_EQ(RecycleNodeSize * 2 + BtreeNodeSize * 2, btree.get_reserved_memory());

  ASSERT_EQ(ERROR_CODE_OK, btree.put(3, 3));
  // one recycle node in recycle pool, one recycle node in memory pool
  // and one btree node in recycle pool, one btree node in memory pool
  ASSERT_EQ(RecycleNodeSize * 2 + BtreeNodeSize * 2, btree.get_reserved_memory());

}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  ob_init_memory_pool();
  return RUN_ALL_TESTS();
}
