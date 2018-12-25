#ifndef OCEANBASE_SQL_MULTIPLAN_H_
#define OCEANBASE_SQL_MULTIPLAN_H_
#include "ob_logical_plan.h"
#include "common/ob_vector.h"

namespace oceanbase
{
  namespace sql
  {
    class ObMultiLogicPlan : public oceanbase::common::ObVector<ObLogicalPlan*>
    {
    public:
      ObMultiLogicPlan();
      ~ObMultiLogicPlan();
      void print(FILE* fp = stderr);
    };
  }
}

#endif //OCEANBASE_SQL_MULTIPLAN_H_

