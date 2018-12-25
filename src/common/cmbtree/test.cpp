#define OCEAN_BASE_BTREE_DEBUG
#include <sys/types.h>
#include <pthread.h>
#include <string>
#include <sstream>
#include "btree_base.h"
#include <iostream>

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
    const char * log_str() const
    {
      static char buf[BUFSIZ];
      snprintf(buf, BUFSIZ, "%ld", value);
      return buf;
    }
    const char * to_cstring() const
    {
      return log_str();
    }
};

void scan_btree(BtreeBase<Number, Number> & b, int64_t start, int64_t end)
{
  BtreeBase<Number, Number>::TScanHandle sh;
  b.get_scan_handle(sh);
  b.set_key_range(sh, Number(start), 1, Number(end), 1);
  //b.set_key_range(sh, Number(atoi(argv[1])), 0, Number(1), 0);
  Number key, value;
  int counter = 0;
  while (b.get_next(sh, key, value) == ERROR_CODE_OK)
  {
    std::cout << counter++ << "."
              << "\tK: " << key.to_string()
              << "\tV: " << value.to_string()
              << std::endl;
  }
}

int main(int argc, char * argv[])
{
  if (argc < 2)
  {
    fprintf(stderr, "usage: %s <number>\n", argv[0]);
    return 1;
  }
  BtreeBase<Number, Number> b;
  b.init();
  Number k(10);
  Number v;
  if (ERROR_CODE_NOT_FOUND != b.get(k, v))
  {
    std::cerr << "get from emtpy btree error" << std::endl;
    return 1;
  }
  for (int j = 0; j < 10; j++)
  {
    for (int64_t i = atoi(argv[1]); i >= 1; i--)
    //for (int64_t i = 1; i <= atoi(argv[1]); i++)
    {
      b.put(i * 2, i * i);
    }
    b.print();
    //scan_btree(b, 1, atoi(argv[1]));
    Number min_key, max_key;
    Number min_value, max_value;
    b.get_min_key(min_key);
    b.get_max_key(max_key);
    b.get(min_key, min_value);
    b.get(max_key, max_value);
    std::cout << "MINIMUM KEY: " << min_key.to_string()
              << "\tVALUE: "  << min_value.to_string() << std::endl;
    std::cout << "MAXIMUM KEY: " << max_key.to_string()
              << "\tVALUE: " << max_value.to_string() << std::endl;
    std::cout << "OBJECT COUNTEER: " << b.get_object_count() << std::endl;
    std::cout << "Btree sizeof: " << sizeof(b)
              << " " << sizeof(BtreeBase<Number, Number>) << std::endl;
    b.clear();
  }
  for (int64_t i = atoi(argv[1]); i >= 1; i--)
  {
    b.put(i * 2, i * i);
  }
  for (int64_t i = 0; i< 10;  i++)
  {
    b.put(2, 4, true);
  }
  scan_btree(b, 1, atoi(argv[1]));
  scan_btree(b, 5, atoi(argv[1]) - 5);
  b.destroy();
  return 0;
}

