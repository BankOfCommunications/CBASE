#define OCEAN_BASE_BTREE_DEBUG
#include <sys/types.h>
#include <pthread.h>
#include <string>
#include <sstream>
#include <vector>
#include "btree_base.h"
#include "thread.h"

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
};

class AddThread : public Thread
{
  public:
    BtreeBase<Number, Number> & b;
    int64_t bn, en, div, mod;
  public:
    AddThread(BtreeBase<Number, Number> & nb) : b(nb)
    {
    }
    AddThread(const AddThread & r)
      : b(r.b), bn(r.bn), en(r.en), div(r.div), mod(r.mod)
    {
    }
    void * run(void * arg)
    {
      for (int64_t i = bn; i < en; i++)
      {
        if (i % div == mod)
        {
          b.put(i, i * i);
        }
      }
      std::cout << "thread end" << std::endl;
      return NULL;
    }
};

int main(int argc, char * argv[])
{
  if (argc < 3)
  {
    fprintf(stderr, "usage: %s <number> <thread number>\n", argv[0]);
    return 1;
  }
  BtreeBase<Number, Number> b;
  b.init();
  int64_t num = atol(argv[1]);
  int64_t thread_num = atol(argv[2]);
  std::vector<AddThread> at(thread_num, AddThread(b));
  for (int64_t i = 0; i < thread_num; i++)
  {
    at[i].bn = 0;
    at[i].en = num;
    at[i].div = thread_num;
    at[i].mod = i;
    at[i].start();
  }
  for (int64_t i = 0; i < thread_num; i++)
  {
    at[i].wait();
  }
  std::cout << "OBJECT COUNTEER: " << b.get_object_count() << std::endl;
  b.dump_mem_info();
  //b.print();
  return 0;
}

