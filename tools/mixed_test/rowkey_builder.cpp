#include "rowkey_builder.h"
#include "utils.h"

using namespace oceanbase;
using namespace common;

RowkeyBuilder::RowkeyBuilder(const int64_t prefix_start, const int64_t max_suffix_per_prefix, const int64_t suffix_length) :
                prefix_start_(prefix_start), max_suffix_per_prefix_(max_suffix_per_prefix), suffix_length_(suffix_length)
{
  reset();
}

RowkeyBuilder::~RowkeyBuilder()
{
}

void RowkeyBuilder::reset()
{
  cur_prefix_ = prefix_start_;
  cur_prefix_end_ = prefix_start_;
  cur_suffix_num_ = 0;
  cur_max_suffix_num_ = 0;

  srand48_r(prefix_start_, &prefix_rand_seed_);
  srand48_r(prefix_start_, &suffix_rand_seed_);
  srand48_r(time(NULL), &common_rand_seed_);
}

ObString RowkeyBuilder::get_rowkey4checkall(PageArena<char> &allocer, const bool change_prefix, const int64_t *prefix_ptr)
{
  ObString ret;

  // 生成一个新的前缀
  int64_t prefix;
  if (NULL == prefix_ptr)
  {
    prefix = cur_prefix_;
    if (change_prefix)
    {
      prefix = cur_prefix_end_++;
      cur_prefix_ = prefix;

      cur_suffix_num_ = 0;
      srand48_r(prefix, &suffix_rand_seed_);
    }
  }
  else
  {
    prefix = *prefix_ptr;
    if (change_prefix)
    {
      cur_suffix_num_ = 0;
      srand48_r(prefix, &suffix_rand_seed_);
    }
  }

  // 生成后缀
  int64_t suffix = 0;
  lrand48_r(&suffix_rand_seed_, &suffix);
  ++cur_suffix_num_;

  int64_t length = I64_STR_LENGTH + suffix_length_;
  char *ptr = allocer.alloc(length);
  if (NULL != ptr)
  {
    sprintf(ptr, "%020ld", prefix);
    build_string(ptr + I64_STR_LENGTH, suffix_length_, suffix);
  }
  ret.assign_ptr(ptr, static_cast<int32_t>(length));
  return ret;
}

ObString RowkeyBuilder::get_rowkey4apply(PageArena<char> &allocer, const int64_t prefix_end)
{
  ObString ret;

  // 生成一个新的前缀
  int64_t prefix = cur_prefix_;
  if (cur_suffix_num_ >= cur_max_suffix_num_)
  {
    prefix = get_prefix_(prefix_end <= cur_prefix_end_);
    cur_prefix_ = prefix;

    int64_t rand = 0;
    lrand48_r(&prefix_rand_seed_, &rand);
    cur_max_suffix_num_ = range_rand(1, max_suffix_per_prefix_, rand);

    cur_suffix_num_ = 0;
    srand48_r(prefix, &suffix_rand_seed_);
  }

  // 生成后缀
  int64_t suffix = 0;
  lrand48_r(&suffix_rand_seed_, &suffix);
  ++cur_suffix_num_;

  int64_t length = I64_STR_LENGTH + suffix_length_;
  char *ptr = allocer.alloc(length);
  if (NULL != ptr)
  {
    sprintf(ptr, "%020ld", prefix);
    build_string(ptr + I64_STR_LENGTH, suffix_length_, suffix);
  }
  ret.assign_ptr(ptr, static_cast<int32_t>(length));
  return ret;
}

std::pair<ObString,ObString> RowkeyBuilder::get_rowkey4scan(const int64_t prefix_end, PageArena<char> &allocer, int64_t &prefix)
{
  std::pair<ObString, ObString> ret;
  int64_t rand = 0;
  lrand48_r(&common_rand_seed_, &rand);
  prefix = range_rand(prefix_start_, prefix_end, rand);

  int64_t length = I64_STR_LENGTH + suffix_length_;
  char *ptr = allocer.alloc(length);
  if (NULL != ptr)
  {
    sprintf(ptr, "%020ld", prefix);
    memset(ptr + I64_STR_LENGTH, 0, suffix_length_);
  }
  ret.first.assign_ptr(ptr, static_cast<int32_t>(length));
  ptr = allocer.alloc(length);
  if (NULL != ptr)
  {
    sprintf(ptr, "%020ld", prefix);
    memset(ptr + I64_STR_LENGTH, -1, suffix_length_);
  }
  ret.second.assign_ptr(ptr, static_cast<int32_t>(length));

  return ret;
}

std::pair<ObString,ObString> RowkeyBuilder::get_rowkey4scan(const int64_t prefix, const int64_t suffix_length, PageArena<char> &allocer)
{
  std::pair<ObString, ObString> ret;

  int64_t length = I64_STR_LENGTH + suffix_length;
  char *ptr = allocer.alloc(length);
  if (NULL != ptr)
  {
    sprintf(ptr, "%020ld", prefix);
    memset(ptr + I64_STR_LENGTH, 0, suffix_length);
  }
  ret.first.assign_ptr(ptr, static_cast<int32_t>(length));
  ptr = allocer.alloc(length);
  if (NULL != ptr)
  {
    sprintf(ptr, "%020ld", prefix);
    memset(ptr + I64_STR_LENGTH, -1, suffix_length);
  }
  ret.second.assign_ptr(ptr, static_cast<int32_t>(length));

  return ret;
}

std::pair<ObString, ObString> RowkeyBuilder::get_rowkey4total_scan(const int64_t prefix_end, PageArena<char> &allocer)
{
  std::pair<ObString, ObString> ret;

  int64_t length = I64_STR_LENGTH + suffix_length_;
  char *ptr = allocer.alloc(length);
  if (NULL != ptr)
  {
    sprintf(ptr, "%020ld", prefix_start_);
    memset(ptr + I64_STR_LENGTH, 0, suffix_length_);
  }
  ret.first.assign_ptr(ptr, static_cast<int32_t>(length));
  ptr = allocer.alloc(length);
  if (NULL != ptr)
  {
    sprintf(ptr, "%020ld", prefix_end);
    memset(ptr + I64_STR_LENGTH, -1, suffix_length_);
  }
  ret.second.assign_ptr(ptr, static_cast<int32_t>(length));

  return ret;
}

int64_t RowkeyBuilder::get_prefix_(const bool prefix_reach_end)
{
  int64_t ret = 0;
  static int64_t flag = 0;
  if (0 == flag++ % 2
      && !prefix_reach_end)
  {
    ret = ++cur_prefix_end_;
  }
  else
  {
    // 在已有的前缀范围内随机找一个
    // 目的是保证能够有对同一行重复操作的case
    int64_t rand = 0;
    lrand48_r(&common_rand_seed_, &rand);
    ret = range_rand(prefix_start_, cur_prefix_end_, rand);
  }
  return ret;
}

ObString RowkeyBuilder::get_random_rowkey(const int64_t cur_prefix_end, const int64_t max_suffix_num, PageArena<char> &allocer)
{
  ObString ret;
  int64_t prefix = calc_prefix_(cur_prefix_end);
  int64_t suffix = calc_suffix_(prefix, max_suffix_num);
  int64_t length = I64_STR_LENGTH + suffix_length_;
  char *ptr = allocer.alloc(length);
  if (NULL != ptr)
  {
    sprintf(ptr, "%020ld", prefix);
    build_string(ptr + I64_STR_LENGTH, suffix_length_, suffix);
  }
  ret.assign_ptr(ptr, static_cast<int32_t>(length));
  return ret;
}

int64_t RowkeyBuilder::calc_prefix_(const int64_t cur_prefix_end)
{
  int64_t ret = 0;
  
  int64_t rand = 0;
  lrand48_r(&common_rand_seed_, &rand);
  ret = range_rand(prefix_start_, cur_prefix_end, rand);
  
  return ret;
}

int64_t RowkeyBuilder::calc_suffix_(const int64_t prefix, const int64_t max_suffix_num)
{
  int64_t ret = 0;
  
  int64_t rand = 0;
  lrand48_r(&common_rand_seed_, &rand);
  int64_t suffix_pos = range_rand(1, max_suffix_num, rand);
  //int64_t suffix_pos = 0;
  
  struct drand48_data suffix_rand_seed;
  srand48_r(prefix, &suffix_rand_seed);
  
  int64_t suffix_num = 0;
  while (suffix_num <= suffix_pos)
  {
    lrand48_r(&suffix_rand_seed, &rand);
    suffix_num++;
  }
  ret = rand;

  return ret;
}

int64_t RowkeyBuilder::get_prefix(const ObString &row_key)
{
  int64_t ret = 0;
  char buffer[I64_STR_LENGTH + 1] = {'\0'};
  memcpy(buffer, row_key.ptr(), I64_STR_LENGTH);
  ret = strtoll(buffer, NULL, 10);
  return ret;
}

int64_t RowkeyBuilder::get_suffix_length(const ObString &row_key)
{
  return row_key.length() - I64_STR_LENGTH;
}

