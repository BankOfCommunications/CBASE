#include "ob_sql_atomic.h"
#include "tblog.h"
using namespace oceanbase::common;
int64_t atomic_dec_positive(volatile uint32_t *pv)
{
  uint32_t old = 0;
  uint32_t nv = 0;
  int64_t dec = 0;
  old = *pv;
  while(true)
  {
    dec = static_cast<int64_t>(old) - 1;
    if (dec < 0)
    {
      break;
    }
    nv = static_cast<uint32_t>(dec);
    if (old == atomic_compare_exchange(pv, nv, old))
    {
      dec = nv;
      break;
    }
    old = *pv;
  }
  return dec;
}
