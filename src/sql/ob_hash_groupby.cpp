/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_hash_groupby.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_hash_groupby.h"
using namespace oceanbase::sql;
/*
int ObHashGroupBy::open()
{
  构造row_desc_;
  // 从子运算符读取所有数据，插入桶中
  child_op_->open();
  const common::ObRow *row = NULL;
  for(child_op_->get_next_row(row))
  {
    根据groupby列计算row归属的hash桶ID;
    add_to_backet(backet_idx, row);
  }
  child_op_->close();
  for(每个桶)
  {
    根据groupby列排序这个桶的内容，如果必要使用外排。
  }
}

int ObHashGroupBy::add_to_backet(const int64_t backet_idx, const common::ObRow* row)
{
  把row写入第backet_idx个桶中;  // 内存存储结构同ObSort，采用common::ObRow和ObCompactRow结合的方式
  if (第backet_idx个桶大小超过backet_mem_size)
  {
    dump这个桶到磁盘;
  }
}

int ObHashGroupBy::close()
{
  释放内存等资源;
}

int ObHashGroupBy::get_next_row(const common::ObRow *&row)
{
  for(当前桶curr_backet_idx_的具有相同groupby列的下k个行)
  {
    计算聚集列，修改curr_row_;
  }
  if (当前桶处理完)
  {
    ++curr_backet_idx_;
  }
  row = &curr_row_;
}

*/

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObHashGroupBy, PHY_HASH_GROUP_BY);
  }
}
