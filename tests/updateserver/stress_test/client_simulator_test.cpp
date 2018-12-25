
#include "client_simulator.h"

void RandomTest()
{
  Random random;
  random.init();
  printf("%ld\n", random.rand64(10,20));
  printf("%ld\n", random.rand64(2));
  for (int i = 0; i < 10000; i++)
  {
    random.rand64(2);
  }
}

int main()
{
  common::ob_init_memory_pool();
  RandomTest();
  return 0;
}
