#include "ob_hint.h"

using namespace oceanbase;
using namespace oceanbase::common;

namespace oceanbase
{
  namespace common
  {
    const char* get_consistency_level_str(ObConsistencyLevel level)
    {
      return (level == WEAK ? "WEAK" : (level == STRONG ? "STRONG": (level == STATIC ? "STATIC": ((level == FROZEN) ? "FROZEN" : "NO_CONSISTENCY" ))));
    }
  }
}
