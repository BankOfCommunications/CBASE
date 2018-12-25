#include "ob_sql_session_info_traversal.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObSQLSessionInfoTraversal::ObSQLSessionInfoTraversal(hash::ObHashMap<ObSQLSessionKey, ObSQLSessionInfo*> *map)
  :map_(map)
{
  
}

ObSQLSessionInfoTraversal::~ObSQLSessionInfoTraversal()
{
  
}

void ObSQLSessionInfoTraversal::operator()(hash::HashMapPair<ObSQLSessionKey, ObSQLSessionInfo*> entry)
{
  if (NULL != entry.second)
  {
    if (OB_SUCCESS == entry.second->try_read_lock())
    {
      list_.push_back(entry);
    }
  }
}
