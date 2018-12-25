/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_string_buf.h,v 0.1 2010/08/19 16:19:02 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *   Yang Zhifeng <zhuweng.yzf@taobao.com>
 *     - using PageArena to implement
 */
#ifndef OCEANBASE_COMMON_OB_STRING_BUF_H_
#define OCEANBASE_COMMON_OB_STRING_BUF_H_

#include "tblog.h"
#include "ob_string.h"
#include "ob_object.h"
#include "ob_memory_pool.h"
#include "page_arena.h"
#include "ob_rowkey.h"
//add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
#include "common/Ob_Decimal.h"
//add:e

namespace oceanbase
{
  namespace common
  {
    // This class is not thread safe.
    // ObStringBufT is used to store the ObString and ObObj object.
    template <typename PageAllocatorT = ModulePageAllocator, typename PageArenaT = PageArena<char, PageAllocatorT> >
    class ObStringBufT
    {
      public:
        ObStringBufT(const int32_t mod_id = ObModIds::OB_STRING_BUF, const int64_t block_size = DEF_MEM_BLOCK_SIZE);
        explicit ObStringBufT(PageArenaT &arena);
        ~ObStringBufT();
        int clear();
        int reset();
        int reuse();
      public:
        // Writes a string to buf.
        // @param [in] str the string object to be stored.
        // @param [out] stored_str records the stored ptr and length of string.
        // @return OB_SUCCESS if succeed, other error code if error occurs.
        int write_string(const ObString& str, ObString* stored_str);
        // Writes an obj to buf.
        // @param [in] obj the object to be stored.
        // @param [out] stored_obj records the stored obj
        // @return OB_SUCCESS if succeed, other error code if error occurs.
        int write_obj(const ObObj& obj, ObObj* stored_obj);
        // Write a rowkey
        int write_string(const ObRowkey& rowkey, ObRowkey* stored_rowkey);
        //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
        int write_obj_varchar_to_decimal(const ObObj& obj, ObObj* stored_obj);
        //add:e

        inline int64_t used() const
        {
          return arena_.used();
        };

        inline int64_t total() const
        {
          return arena_.total();
        };

        inline PageArenaT& get_arena() const {return arena_;}
        inline void *alloc(const int64_t size) { return arena_.alloc(size);}
        inline void *realloc(void *ptr, const int64_t oldsz, const int64_t newsz)
        {
          return arena_.realloc((char*)ptr, oldsz, newsz);
        }
        inline void free(void *ptr) { return arena_.free((char*)ptr); }
      private:
        DISALLOW_COPY_AND_ASSIGN(ObStringBufT);
        static const int64_t DEF_MEM_BLOCK_SIZE;
        static const int64_t MIN_DEF_MEM_BLOCK_SIZE;
      private:
        PageArenaT local_arena_;
        PageArenaT &arena_;
    };
    typedef ObStringBufT<> ObStringBuf;
  }
}

#include "ob_string_buf.ipp"

#endif //__OB_STRING_BUF_H__
