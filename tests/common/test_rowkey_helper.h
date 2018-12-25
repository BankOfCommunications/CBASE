/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 *         test_rowkey_helper.h is for what ...
 *
 *  Version: $Id: test_rowkey_helper.h 08/09/2012 01:41:14 PM qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#ifndef __OCEANBASE_TEST_ROWKEY_HELPER_H__
#define __OCEANBASE_TEST_ROWKEY_HELPER_H__

#include "ob_rowkey.h"
#include "ob_object.h"
#include "ob_string.h"
#include "page_arena.h"


using namespace oceanbase::common;

class TestRowkeyHelper 
{
  public:
    explicit TestRowkeyHelper(CharArena* a = NULL) 
      : obj_ptr_(NULL), obj_cnt_(0) 
    {
      set_arena(a);
    }
    ~TestRowkeyHelper() 
    {
      if (own_arena_ && NULL != arena_)
      {
        arena_->free();
        arena_ = NULL;
      }
    }

    TestRowkeyHelper(ObObj* ptr, const int64_t cnt, CharArena* a = NULL)
      : obj_ptr_(NULL), obj_cnt_(0) 
    {
      set_arena(a);
      assign(ptr, cnt);
    }

    TestRowkeyHelper(const ObRowkey& rowkey, CharArena* a = NULL)
      : obj_ptr_(NULL), obj_cnt_(0) 
    {
      set_arena(a);
      assign(rowkey.get_obj_ptr(), rowkey.get_obj_cnt());
    }

    TestRowkeyHelper(const ObString & str_value, CharArena* a = NULL)
      : obj_ptr_(NULL), obj_cnt_(0) 
    {
      set_arena(a);
      ObObj obj;
      obj.set_varchar(str_value);
      assign(&obj, 1);
    }

    TestRowkeyHelper(const TestRowkeyHelper& rhs, CharArena* a = NULL)
      : obj_ptr_(NULL), obj_cnt_(0) 
    {
      set_arena(a);
      assign(rhs.obj_ptr_, rhs.obj_cnt_);
    }

    TestRowkeyHelper& operator=(const TestRowkeyHelper& rhs)
    {
      if (this == &rhs)
      {
        return *this;
      }
      else
      {
        if (own_arena_) arena_->reuse();
        assign(rhs.obj_ptr_, rhs.obj_cnt_);
      }
      return *this;
    }

    TestRowkeyHelper& operator=(const ObString& rhs)
    {
      if (own_arena_) arena_->reuse();
      ObObj obj;
      obj.set_varchar(rhs);
      assign(&obj, 1);
      return *this;
    }

    TestRowkeyHelper& operator=(const ObRowkey& rhs)
    {
      if (own_arena_) arena_->reuse();
      assign(rhs.get_obj_ptr(), rhs.get_obj_cnt());
      return *this;
    }

    int compare(const TestRowkeyHelper& rhs) const
    {
      ObRowkey rowkey(obj_ptr_, obj_cnt_);
      return rowkey.compare(rhs);
    }

    operator ObRowkey() const
    {
      ObRowkey rowkey(obj_ptr_, obj_cnt_);
      return rowkey;
    }

    operator ObString() const
    {
      ObString str_value ;
      if (obj_cnt_ == 1 && NULL != obj_ptr_ && obj_ptr_[0].get_type() == ObVarcharType)
      {
        obj_ptr_[0].get_varchar(str_value);
      }
      return str_value;
    }

    ObObj& operator[](const int64_t i) 
    {
      return obj_ptr_[i];
    }

    const ObObj& operator[](const int64_t i) const
    {
      return obj_ptr_[i];
    }

    const ObObj* get_obj_ptr() const { return obj_ptr_; }
    ObObj* get_obj_ptr() { return obj_ptr_; }
    int64_t get_obj_cnt() const { return obj_cnt_; }
    const ObObj* ptr() const { return obj_ptr_; }
    ObObj* ptr() { return obj_ptr_; }
    int64_t length() const { return obj_cnt_; }
    int64_t to_string(char* buf, const int64_t size) const
    {
      ObRowkey rowkey(obj_ptr_, obj_cnt_);
      return rowkey.to_string(buf, size);
    }


    inline bool operator<(const TestRowkeyHelper& rhs) const
    {
      return compare(rhs) < 0;
    }

    inline bool operator<=(const TestRowkeyHelper& rhs) const
    {
      return compare(rhs) <= 0;
    }

    inline bool operator>(const TestRowkeyHelper& rhs) const
    {
      return compare(rhs) > 0;
    }

    inline bool operator>=(const TestRowkeyHelper& rhs) const
    {
      return compare(rhs) >= 0;
    }

    inline bool operator==(const TestRowkeyHelper& rhs) const
    {
      return compare(rhs) == 0;
    }
    inline bool operator!=(const TestRowkeyHelper& rhs) const
    {
      return compare(rhs) != 0;
    }

  private:
    int assign(const ObObj* ptr, const int64_t cnt)
    {
      int ret = OB_SUCCESS;
      if (NULL == ptr || cnt == 0)
      {
        obj_ptr_ = NULL; 
        obj_cnt_ = 0;
      }
      else
      {
        ret = deep_copy(ptr, cnt);
      }
      return ret;
    }

    int deep_copy(const ObObj* ptr, const int64_t cnt) 
    {
      int ret = OB_SUCCESS;
      ObRowkey rowkey(const_cast<ObObj*>(ptr), cnt);
      ObRowkey rhs;
      ret = rowkey.deep_copy(rhs, *arena_);
      if (OB_SUCCESS == ret)
      {
        obj_ptr_ = const_cast<ObObj*>(rhs.get_obj_ptr());
        obj_cnt_ = rhs.get_obj_cnt();
      }
      return ret;
    }

    void set_arena(CharArena* a)
    {
      if (NULL == a)
      {
        arena_ = new CharArena();
        own_arena_ = true;
      }
      else
      {
        arena_ = a;
        own_arena_ = false;
      }
    }


  private:
    CharArena *arena_;
    bool own_arena_;
    ObObj* obj_ptr_;
    int64_t obj_cnt_;
};

inline ObRowkey make_rowkey(const char* cstr, CharArena* arena = NULL)
{
  ObString str;
  str.assign_ptr(const_cast<char*>(cstr), static_cast<int32_t>(strlen(cstr)));
  return TestRowkeyHelper(str, arena);
}

inline ObRowkey make_rowkey(const char* ptr, const int64_t len, CharArena* arena = NULL)
{
  ObString str;
  str.assign_ptr(const_cast<char*>(ptr), static_cast<int32_t>(len));
  return TestRowkeyHelper(str, arena);
}


#endif //__OCEANBASE_TEST_ROWKEY_HELPER_H__

