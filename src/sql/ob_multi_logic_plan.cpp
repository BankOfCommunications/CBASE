#include "ob_multi_logic_plan.h"
#include "parse_malloc.h"
#include "common/ob_string.h"

namespace oceanbase
{
  namespace sql
  {
    using namespace oceanbase::common;
    ObMultiLogicPlan::ObMultiLogicPlan()
    {
    }
    ObMultiLogicPlan::~ObMultiLogicPlan()
    {
      for(int32_t i = 0; i < size(); ++i)
      {
        //delete at(i);
        at(i)->~ObLogicalPlan();
        parse_free(at(i));
      }
    }

    void ObMultiLogicPlan::print(FILE* fp)
    {
      for(int32_t i = 0; i < size(); ++i)
      {
        at(i)->print(fp);
        fprintf(fp, "\n");
      }
    }
    
  }
}

