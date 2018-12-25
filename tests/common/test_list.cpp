#include "ob_list.h"

using namespace oceanbase;
using namespace common;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ObList<int64_t> list;

  for (int64_t i = 0; i < 1024; i++)
  {
    list.push_back(i);
  }

  ObList<int64_t>::iterator iter;
  int64_t i = 0;
  for (iter = list.begin(); iter != list.end(); iter++, i++)
  {
    assert(i == *iter);
  }
  assert(i == 1024);

  assert(1024 == list.size());

  iter = list.end();
  do
  {
    i--;
    iter--;
    assert(i == *iter);
  }
  while (iter != list.begin());
  assert(i == 0);

  i = 1024;
  assert(0 == list.push_front(i));
  assert(i == *list.begin());
  assert(0 == list.pop_front());
  assert(0 == *list.begin());

  assert(0 == list.push_back(i));
  assert(i == *(--(list.end())));
  assert(0 == list.pop_back());
  assert(1023 == *(--(list.end())));

  assert(false == list.empty());
  assert(1024 == list.size());
  assert(0 == list.clear());
  assert(0 == list.size());
  assert(true == list.empty());
  assert(0 != list.pop_front());
  assert(0 != list.pop_back());

  iter = list.begin();
  assert(0 == list.insert(iter, 1));
  iter = list.begin();
  assert(0 == list.insert(iter, 2));
  assert(2 == *list.begin());
  assert(1 == *(--(list.end())));

  iter = list.begin();
  assert(0 == list.insert(iter, 3));

  ObList<int64_t>::iterator iters[3];
  i = 0;
  for (iter = list.begin(); iter != list.end(); iter++)
  {
    iters[i++] = iter;
  }
  assert(0 == list.erase(iters[1]));
  assert(3 == *iters[0]);
  assert(1 == *iters[2]);
  assert(++iters[0] == iters[2]);

  ObList<int64_t>::const_iterator citers[2];
  ObList<int64_t>::const_iterator citer;
  i = 0;
  for (citer = list.begin(); citer != list.end(); citer++)
  {
    citers[i++] = citer;
  }
  assert(3 == *citers[0]);
  assert(1 == *citers[1]);
  assert(++citers[0] == citers[1]);
}

