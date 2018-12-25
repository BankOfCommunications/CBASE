#include <string.h>
#include "ob_define.h"
#include "ob_probability_random.h"

using namespace oceanbase::common;

ObProbabilityRandom::ObProbabilityRandom():lock_(tbsys::WRITE_PRIORITY)
{
  init_ = false;
  index_count_ = 0;
  memset(index_, 0, sizeof(index_));
}

ObProbabilityRandom::~ObProbabilityRandom()
{
}

int ObProbabilityRandom::init(const int32_t percent[], const int32_t count)
{
  int32_t sum = 0;
  int ret = check_sum(percent, count, sum);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "check the input percent failed:ret[%d]", ret);
  }
  else if (sum <= 0)
  {
    TBSYS_LOG(WARN, "check input percent sum failed:count[%d], sum[%d]", count, sum);
  }
  else
  {
    // must success
    tbsys::CWLockGuard lock(lock_);
    build_index(percent, count, sum);
    init_ = true;
  }
  return ret;
}

void ObProbabilityRandom::build_index(const int32_t percent[], const int32_t count, const int32_t sum)
{
  int32_t start_pos = 0;
  int32_t end_pos = 0;
  for (int32_t i = 0; i < count; ++i)
  {
    end_pos = start_pos + (int32_t)((double(percent[i]) / sum) * BASE_NUMBER);
    while (start_pos < end_pos)
    {
      index_[start_pos++] = i;
    }
  }
  index_count_ = end_pos;
}

int32_t ObProbabilityRandom::random(void) const
{
  int32_t ret = -1;
  if (true == init_)
  {
    int32_t random_percent = static_cast<int32_t>(::random());
    tbsys::CRLockGuard lock(lock_);
    ret = index_[random_percent % index_count_];
  }
  else
  {
    TBSYS_LOG(WARN, "check status not init");
  }
  return ret;
}

int ObProbabilityRandom::check_sum(const int32_t percent[], const int32_t count,
    int32_t & percent_sum) const
{
  percent_sum = 0;
  int ret = OB_SUCCESS;
  for (int32_t i = 0; i < count; ++i)
  {
    if (percent[i] < 0)
    {
      ret = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "check percent failed:index[%d], count[%d], percent[%d]", i, count, percent[i]);
      break;
    }
    else
    {
      percent_sum += percent[i];
    }
  }
  if (percent_sum <= 0)
  {
    TBSYS_LOG(WARN, "check percent sum failed:count[%d], sum[%d]", count, percent_sum);
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int32_t ObStalessProbabilityRandom::random(const int32_t percent[], const int32_t count, const int32_t sum)
{
  int32_t ret = -1;
  if ((count > 0) && (sum > 0))
  {
    int32_t step_percent = 0;
    int32_t random_percent = static_cast<int32_t>(::random() % sum);
    for (int32_t i = 0; i < count; ++i)
    {
      step_percent += percent[i];
      if ((random_percent < step_percent) && (percent[i] > 0))
      {
        ret = i;
        break;
      }
    }
  }
  return ret;
}


