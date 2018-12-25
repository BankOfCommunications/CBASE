#include "gtest/gtest.h"
#include "tbsys.h"
#include "ob_array_lock.h"
#include "ob_malloc.h"

using namespace oceanbase;
using namespace common;

int main(int argc, char **argv)
{
  TBSYS_LOGGER.setLogLevel("ERROR");
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestArrayLock: public ::testing::Test
{
public:
  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
};

TEST_F(TestArrayLock, init)
{
  ObArrayLock lock;
  EXPECT_TRUE(lock.init(10) == OB_SUCCESS);
  EXPECT_TRUE(lock.init(10) == OB_SUCCESS);
  EXPECT_TRUE(lock.size() == 10);
  EXPECT_TRUE(lock.init(20) != OB_SUCCESS);
  EXPECT_TRUE(lock.size() == 10);
  EXPECT_TRUE(lock.init(10) == OB_SUCCESS);
}


TEST_F(TestArrayLock, lock)
{
  ObArrayLock lock;
  EXPECT_TRUE(lock.acquire_lock(0) == NULL);
  uint64_t count = 128;
  EXPECT_TRUE(lock.init(count) == OB_SUCCESS);
  for (uint64_t i = 0; i < count; ++i)
  {
    for (uint64_t j = 0; j < count; ++j)
    {
      EXPECT_TRUE(lock.acquire_lock(j) != NULL);
    }
  }
  EXPECT_TRUE(lock.acquire_lock(count + 1) == NULL);
}


ObArrayLock lock;
static const uint64_t ID_COUNT = 8;
static const int64_t THREAD_PER_ID = 10;
static int64_t ID_ARRAY[ID_COUNT];
static const int64_t MAX_THREAD_COUNT = ID_COUNT * THREAD_PER_ID; 
static const int64_t INC_COUNT = 100000;

void * routine(void * argv)
{
  int64_t idx = *reinterpret_cast<int64_t *>(argv) % ID_COUNT;
  for (int64_t i = 0; i < INC_COUNT; ++i)
  {
    tbsys::CThreadGuard mutex(lock.acquire_lock(idx));
    ID_ARRAY[idx]++;
  }
  return NULL;
}

TEST_F(TestArrayLock, multi_thread_array_lock)
{
  EXPECT_TRUE(lock.acquire_lock(0) == NULL);
  EXPECT_TRUE(lock.init(ID_COUNT) == OB_SUCCESS);
  memset(ID_ARRAY, 0, sizeof(ID_ARRAY));
  pthread_t threads[MAX_THREAD_COUNT];
  int64_t thread_id[MAX_THREAD_COUNT];
  for (int64_t i = 0; i < MAX_THREAD_COUNT; ++i)
  {
    thread_id[i] = i;
    pthread_create(&threads[i], NULL, routine, &thread_id[i]);
  }

  for (int64_t i = 0; i < MAX_THREAD_COUNT; ++i)
  {
    pthread_join(threads[i], NULL);
  }

  for (uint64_t i = 0; i < ID_COUNT; ++i)
  {
    EXPECT_TRUE(ID_ARRAY[i] == INC_COUNT * THREAD_PER_ID);
  }
}

ObSequenceLock seq_lock;
void * seq_routine(void * argv)
{
  UNUSED(argv);
  uint64_t idx = 0;
  for (int64_t i = 0; i < INC_COUNT; ++i)
  {
    // using different lock
    tbsys::CThreadGuard mutex(seq_lock.acquire_lock(idx));
    ID_ARRAY[idx]++;
  }
  return NULL;
}

TEST_F(TestArrayLock, multi_thread_seq_lock)
{
  uint64_t index = 0;
  EXPECT_TRUE(seq_lock.acquire_lock(index) == NULL);
  EXPECT_TRUE(seq_lock.init(ID_COUNT) == OB_SUCCESS);
  pthread_t threads[MAX_THREAD_COUNT];
  memset(ID_ARRAY, 0, sizeof(ID_ARRAY));
  for (int64_t i = 0; i < MAX_THREAD_COUNT; ++i)
  {
    pthread_create(&threads[i], NULL, seq_routine, NULL);
  }

  for (int64_t i = 0; i < MAX_THREAD_COUNT; ++i)
  {
    pthread_join(threads[i], NULL);
  }

  for (uint64_t i = 0; i < ID_COUNT; ++i)
  {
    EXPECT_TRUE(ID_ARRAY[i] == INC_COUNT * THREAD_PER_ID);
  }
  // for one thread
  for (int64_t i = 0; i < INC_COUNT; ++i)
  {
    EXPECT_TRUE(seq_lock.acquire_lock(index) != NULL);
    EXPECT_TRUE(i % ID_COUNT == index);
  }
}


