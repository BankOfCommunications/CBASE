/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_strings.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_STRINGS_H
#define _OB_STRINGS_H 1
#include "ob_string.h"
#include "ob_string_buf.h"
#include "ob_array.h"

namespace oceanbase
{
  namespace common
  {
    /**
     * an array of strings
     *
     */
    class ObStrings
    {
      public:
        ObStrings();
        virtual ~ObStrings();
        int add_string(const ObString &str, int64_t *idx = NULL);
        int get_string(int64_t idx, ObString &str) const;
        int64_t count() const;
        void clear();

        int64_t to_string(char* buf, const int64_t buf_len) const;
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        // types and constants
      private:
        // disallow copy
        ObStrings(const ObStrings &other);
        ObStrings& operator=(const ObStrings &other);
        // function members
      private:
        // data members
        ObStringBuf buf_;
        ObArray<ObString> strs_;
    };
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_STRINGS_H */
