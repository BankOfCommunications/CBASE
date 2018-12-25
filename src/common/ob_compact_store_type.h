/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_compact_store_type.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_COMPACT_STORE_TYPE_H
#define _OB_COMPACT_STORE_TYPE_H 1

namespace oceanbase
{
  namespace common
  {
    /**
     * 用于区分紧凑格式的四种存储类型
     * DENSE_SPARSE和DENSE_DENSE实际实现是rowkey和普通列分属于两行
     */
    enum ObCompactStoreType
    {
      SPARSE, //稀疏格式类型，带column_id
      DENSE, //稠密格式类型，不带column_id
      DENSE_SPARSE, //每一行由两部分组成，前一部分是rowkey，稠密。后一部分普通列稀疏
      DENSE_DENSE, //每一行由两部分组成，前一部分是rowkey，稠密。后一部分普通列也稠密
      INVALID_COMPACT_STORE_TYPE
    };
  }
}

#endif /* _OB_COMPACT_STORE_TYPE_H */



