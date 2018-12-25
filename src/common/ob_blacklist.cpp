#include "ob_define.h"
#include "ob_blacklist.h"

using namespace oceanbase::common;

ObBlackList::ObBlackList():lock_(tbsys::WRITE_PRIORITY)
{
  count_ = 0;
  alpha_ = DEFAULT_DETA;
  threshold_ = DEFAULT_THRESHOLD;
  memset(scores_, 0, sizeof(scores_));
}

ObBlackList::~ObBlackList()
{
}

int ObBlackList::init(const int32_t count, const int64_t alpha,
    const int64_t alpha_deno, const int64_t threshold, const int64_t threshold_deno)
{
  int ret = OB_SUCCESS;
  if ((count <= 0) || (alpha <= 0) || (alpha_deno <= 0) || (threshold <= 0) || (threshold_deno <= 0))
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "check input param failed:count[%d], alpha[%ld], deno[%ld], threshold[%ld], deno[%ld]",
        count, alpha, alpha_deno, threshold, threshold_deno);
  }
  else if (alpha > alpha_deno)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "check input param failed:alpha[%ld], deno[%ld]", alpha, alpha_deno);
  }
  else
  {
    tbsys::CWLockGuard lock(lock_);
    count_ = count;
    memset(scores_, 0, sizeof(scores_));
    alpha_ = double(alpha)/double(alpha_deno);
    threshold_ = double(threshold)/double(threshold_deno);
    TBSYS_LOG(DEBUG, "reinit the scores of the blacklist:count[%d], old[%d], alpha[%2lf], threshold[%2lf]",
        count, count_, alpha_, threshold_);
  }
  return ret;
}

int ObBlackList::update(const bool succ, const int32_t index)
{
  int ret = OB_SUCCESS;
  if (index >= count_)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "check index failed:index[%d], count[%d]", index, count_);
  }
  else
  {
    tbsys::CWLockGuard lock(lock_);
    if (true == succ)
    {
      scores_[index] *= alpha_; 
    }
    else
    {
      ++scores_[index];
    }
  }
  return ret;
}

void ObBlackList::reset(void)
{
  tbsys::CWLockGuard lock(lock_);
  memset(scores_, 0, sizeof(scores_));
}

bool ObBlackList::check(const int32_t index) const
{
  bool ret = false;
  if (index >= count_)
  {
    TBSYS_LOG(WARN, "check index failed:index[%d], count[%d]", index, count_);
  }
  else
  {
    tbsys::CRLockGuard lock(lock_);
    if (scores_[index] < threshold_)
    {
      ret = true;
    }
  }
  return ret;
}

