/**
 * (C) 2013-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_iarray.h
 *
 * Authors:
 *   Fusheng Han <yanran.hfs@taobao.com>
 *
 */

#ifndef _OB_IARRAY_H
#define _OB_IARRAY_H

namespace oceanbase
{
  namespace common
  {
    template <typename T>
    class ObIArray
    {
      public:
        virtual ~ObIArray() {}
        virtual int push_back(const T & obj) = 0;
        virtual void pop_back() = 0;
        virtual int pop_back(T &obj) = 0;

        virtual int at(int64_t idx, T &obj) const = 0;
        virtual T& at(int64_t idx) = 0;
        virtual const T& at(int64_t idx) const = 0;

        virtual int64_t count() const = 0;
        virtual void clear() = 0;
        virtual void reserve(int64_t capacity) = 0;
    };
  } // end namespace common
} // end namespace oceanbase

#endif // _OB_IARRAY_H
