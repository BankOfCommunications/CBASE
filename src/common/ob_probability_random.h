#ifndef OB_OCEAMBASE_PERCENTAGE_RANDOM_H_
#define OB_OCEANBASE_PERCENTAGE_RANDOM_H_

#include <stdint.h>
#include "tbsys.h"

namespace oceanbase
{
  namespace common
  {
    // thread safe random class
    class ObProbabilityRandom
    {
    public:
      ObProbabilityRandom();
      virtual ~ObProbabilityRandom();
    public:
      // set percent array if failed use old values
      // must make sure that the sum should not overflow int32_t
      int init(const int32_t percent[], const int32_t count);
      // get a random index
      int32_t random(void) const;
    private:
      // build percent index for fast search 
      void build_index(const int32_t percent[], const int32_t count, const int32_t sum);
      // check the percent and return all the percent sum
      int check_sum(const int32_t percent[], const int32_t count, int32_t & sum) const;
    private:
      static const int32_t BASE_NUMBER = 100;
      mutable tbsys::CRWLock lock_;
      bool init_;
      int32_t index_count_;
      int32_t index_[BASE_NUMBER];
    };

    // staleless random class 
    class ObStalessProbabilityRandom
    {
    public:
      // WARN: the sum should be the count's percent number
      // the count and sum must be gt 0
      static int32_t random(const int32_t percent[], const int32_t count, const int32_t sum);
    };
  }
}

#endif //OB_OCEANBASE_PERCENTAGE_RANDOM_H_
