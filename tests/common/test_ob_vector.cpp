
#include "gtest/gtest.h"
#include "common/ob_malloc.h"
#include "common/ob_vector.h"

using namespace oceanbase;
using namespace common;

struct Nice
{
  int fi;
  Nice(int i) : fi(i) {}
  static void dump(const Nice* n)
  {
    printf("nice value:%d\n", n->fi);
  }
  static void free(Nice* n)
  {
    if (NULL != n) delete n;
  }
};

bool compare(const Nice * a, const Nice * b)
{
  return a->fi < b->fi;
}

bool unique_a(const Nice * a, const Nice * b)
{
  return a->fi == b->fi;
}


int test_add_ob_vector(ObSortedVector<Nice*> & v, int i)
{
  ObSortedVector<Nice*>::iterator iter;
  const Nice *n1  = new Nice(i);
  int ret = v.insert(n1, iter, compare);
  //printf("insert ele:%d,%p, ret pos:%p, ret:%d\n", i, n1, iter, ret);
  return ret;
}

int test_add_ob_vector_unique(ObSortedVector<Nice*> & v, int i)
{
  ObSortedVector<Nice*>::iterator iter;
  const Nice *n1  = new Nice(i);
  int ret = v.insert_unique(n1, iter, compare, unique_a);
  //printf("insert ele:%d,%p, ret pos:%p,%p, ret:%d\n", i, n1, iter, (iter == NULL) ? 0: *iter, ret);
  return ret;
}


TEST(TestObVector, insert)
{
  ObVector<Nice*> v; 
  EXPECT_EQ(0, v.size());
  EXPECT_EQ(0, v.capacity());
  v.push_back(new Nice(1));
  EXPECT_EQ(2, v.capacity());
  v.push_back(new Nice(2));
  EXPECT_EQ(2, v.capacity());
  v.push_back(new Nice(5));
  EXPECT_EQ(6, v.capacity());
  EXPECT_EQ(1, v.at(0)->fi);
  EXPECT_EQ(2, v.at(1)->fi);
  EXPECT_EQ(5, v.at(2)->fi);
  std::for_each(v.begin(), v.end(), Nice::free);
}

TEST(TestObVector, insert2)
{
  ObVector<Nice*> v; 
  v.reserve(20000);
  for (int i = 0; i < 10000; ++i)
  {
    v.push_back(new Nice(i));
  }
  for (int i = 0; i < 10000; ++i)
  {
    EXPECT_EQ(i, v.at(i)->fi);
  }
  std::for_each(v.begin(), v.end(), Nice::free);
}

TEST(TestObSortedVector, insert)
{
  ObSortedVector<Nice*> v; 
  EXPECT_EQ(0, test_add_ob_vector(v, 1));
  EXPECT_EQ(0, test_add_ob_vector(v, 20));
  EXPECT_EQ(0, test_add_ob_vector(v, 10));
  EXPECT_EQ(0, test_add_ob_vector(v, 5));
  EXPECT_EQ(1, v.at(0)->fi);
  EXPECT_EQ(5, v.at(1)->fi);
  EXPECT_EQ(10, v.at(2)->fi);
  EXPECT_EQ(20, v.at(3)->fi);
  std::for_each(v.begin(), v.end(), Nice::free);
}

TEST(TestObSortedVector, insert2)
{
  ObSortedVector<Nice*> v; 
  v.reserve(20000);
  
  for (int i = 10000; i > 0; --i)
  {
    test_add_ob_vector(v, i);
  }
  std::for_each(v.begin(), v.end(), Nice::free);
}

TEST(TestObSortedVector, insert3)
{
  ObSortedVector<Nice*> v; 
  v.reserve(20000);
  
  for (int i = 0; i < 10000; ++i)
  {
    test_add_ob_vector(v, i);
  }
  std::for_each(v.begin(), v.end(), Nice::free);
}

TEST(TestObSortedVector, insert_unique)
{
  ObSortedVector<Nice*> v; 
  EXPECT_EQ(0, test_add_ob_vector_unique(v, 1));
  EXPECT_EQ(0, test_add_ob_vector_unique(v, 20));
  EXPECT_NE(0, test_add_ob_vector_unique(v, 20));
  EXPECT_EQ(0, test_add_ob_vector_unique(v, 5));
  EXPECT_EQ(1, v.at(0)->fi);
  EXPECT_EQ(5, v.at(1)->fi);
  EXPECT_EQ(20, v.at(2)->fi);
  std::for_each(v.begin(), v.end(), Nice::free);
}

TEST(TestObSortedVector, test_find)
{
  ObSortedVector<Nice*> v; 
  v.reserve(20000);
  
  for (int i = 0; i < 10000; ++i)
  {
    EXPECT_EQ(0, test_add_ob_vector_unique(v, i));
  }

  for (int i = 0; i < 10000; ++i)
  {
    ObSortedVector<Nice*>::iterator fiter;
    const Nice* pn = new Nice(i);
    EXPECT_EQ(0, v.find(pn, fiter, compare));
    EXPECT_EQ(i, (*fiter)->fi);
  }

  std::for_each(v.begin(), v.end(), Nice::free);
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}


