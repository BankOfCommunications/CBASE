#ifndef _MIXED_TEST_ROWKEY_BUILDER_
#define _MIXED_TEST_ROWKEY_BUILDER_
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "common/page_arena.h"
#include "common/ob_string.h"
#include "common/hash/ob_hashmap.h"

using namespace oceanbase;
using namespace common;
using namespace common::hash;

class RowkeyBuilder
{
  public:
    static const int64_t I64_STR_LENGTH = 20;
  public:
    RowkeyBuilder(const int64_t prefix_start, const int64_t max_suffix_per_prefix, const int64_t suffix_length);
    ~RowkeyBuilder();
  public:
    void reset();
    ObString get_rowkey4apply(PageArena<char> &allocer, const int64_t prefix_end);
    std::pair<ObString, ObString> get_rowkey4scan(const int64_t prefix_end, PageArena<char> &allocer, int64_t &prefix);
    static std::pair<ObString,ObString> get_rowkey4scan(const int64_t prefix, const int64_t suffix_length, PageArena<char> &allocer);
    std::pair<ObString, ObString> get_rowkey4total_scan(const int64_t prefix_end, PageArena<char> &allocer);
    ObString get_rowkey4checkall(PageArena<char> &allocer, const bool change_prefix, const int64_t *prefix_ptr);
    ObString get_random_rowkey(const int64_t cur_prefix_end, const int64_t max_suffix_num, PageArena<char> &allocer);
    int64_t get_prefix_start() const
    {
      return prefix_start_;
    };
    int64_t get_suffix_length() const
    {
      return suffix_length_;
    };
    int64_t get_max_suffix_per_prefix() const
    {
      return max_suffix_per_prefix_;
    };
    int64_t get_cur_prefix_end() const
    {
      return cur_prefix_end_;
    };
    static int64_t get_prefix(const ObString &row_key);
    static int64_t get_suffix_length(const ObString &row_key);
  private:
    int64_t get_prefix_(const bool prefix_reach_end);
    int64_t calc_prefix_(const int64_t cur_prefix_end);
    int64_t calc_suffix_(const int64_t prefix, const int64_t max_suffix_num);
  private:
    // rowkey前缀的起始值
    const int64_t prefix_start_;
    // 每个rowkey前缀最多可以生成行的数量
    const int64_t max_suffix_per_prefix_;
    // 后缀的长度
    const int64_t suffix_length_;

    // rowkey前缀当前值
    int64_t cur_prefix_;
    // rowkey前缀当前最大值
    int64_t cur_prefix_end_;
    // 当前前缀下 已产生的后缀数量
    int64_t cur_suffix_num_;
    // 当前前缀下 要产生的后缀数量
    int64_t cur_max_suffix_num_;

    // 用于产生前缀相关数据的随机数种子
    struct drand48_data prefix_rand_seed_;
    // 用于产生后缀相关数据的随机数种子
    struct drand48_data suffix_rand_seed_;

    // 通用随机种子
    struct drand48_data common_rand_seed_;
};

#endif //_MIXED_TEST_ROWKEY_BUILDER_
