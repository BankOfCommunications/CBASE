/**
 * (C) 2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 * key.h defines raw key structure. 
 *
  Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */

#ifndef OCEANBASE_SSTABLE_KEY_H_
#define OCEANBASE_SSTABLE_KEY_H_

#include <iostream>
#include "common/ob_endian.h"
#include "common/ob_rowkey.h"
#include "common/ob_object.h"

namespace oceanbase 
{
  namespace sstable 
  {
    class Key 
    {
    public:
      static const int32_t FIXED_KEY_LEN = 17;
      static const int32_t FIXED_ROWKEY_COL_NUM = 3;
 
      /**
       * Constructor (for implicit construction).
       */
      Key() : prefix_(0), flag_(0), suffix_(0)
      {
        memset(ptr_, 0, FIXED_KEY_LEN);
      }
  
      Key(int64_t prefix, char flag, int64_t suffix) 
        : flag_(flag)
      {
        prefix_ = ptr_;
        encode_ts64(prefix_, prefix);
        memcpy(ptr_ + sizeof(int64_t), &flag, sizeof(char));
        suffix_ = ptr_ + sizeof(int64_t) + sizeof(char);
        encode_ts64(suffix_, suffix);
      }

      void assign(int64_t prefix, char flag, int64_t suffix) 
      {
        flag_ = flag_;
        prefix_ = ptr_;
        encode_ts64(prefix_, prefix);
        memcpy(ptr_ + sizeof(int64_t), &flag, sizeof(char));
        suffix_ = ptr_ + sizeof(int64_t) + sizeof(char);
        encode_ts64(suffix_, suffix);
      }

      void trans_to_rowkey(common::ObRowkey & rowkey)
      {
        objs_[0].set_int(decode_ts64(prefix_));
        objs_[1].set_int(flag_);
        objs_[2].set_int(decode_ts64(suffix_));
        rowkey.assign(objs_, FIXED_ROWKEY_COL_NUM);
      }

      inline void encode_ts64(char *bufp, int64_t val) 
      {
#ifdef OB_LITTLE_ENDIAN
        *bufp++ = (char)(val >> 56);
        *bufp++ = (char)(val >> 48);
        *bufp++ = (char)(val >> 40);
        *bufp++ = (char)(val >> 32);
        *bufp++ = (char)(val >> 24);
        *bufp++ = (char)(val >> 16);
        *bufp++ = (char)(val >> 8);
        *bufp++ = (char)val;
#else
        memcpy(bufp, &val, 8);
#endif
      }
  
      inline int64_t decode_ts64(char *bufp) 
      {
        int64_t val = 0;
#ifdef OB_LITTLE_ENDIAN
        val = ((int64_t)((uint8_t)*bufp++) << 56);
        val |= ((int64_t)((uint8_t)*bufp++) << 48);
        val |= ((int64_t)((uint8_t)*bufp++) << 40);
        val |= ((int64_t)((uint8_t)*bufp++) << 32);
        val |= ((int64_t)((uint8_t)*bufp++) << 24);
        val |= ((int64_t)((uint8_t)*bufp++) << 16);
        val |= ((int64_t)((uint8_t)*bufp++) << 8);
        val |= (int64_t)((uint8_t)*bufp++);
#else
        memcpy(&val, bufp, 8);
#endif
        return val;
      }
  
      Key(const char *key) 
      {
        memcpy(ptr_, key, FIXED_KEY_LEN);
        prefix_ = ptr_;
        flag_ = *(ptr_ + sizeof(int64_t));
        suffix_ = ptr_ + sizeof(int64_t) + sizeof(char);
      }
  
      int compare(const Key &key) const 
      {
        return memcmp(ptr_, key.ptr_, FIXED_KEY_LEN);
      }
  
      void write(char *buf) const 
      {
        memcpy(buf, ptr_, FIXED_KEY_LEN);
      }
  
      const char *get_ptr() const
      {
        return reinterpret_cast<const char *> (ptr_);
      }

      char *get_ptr()
      {
        return reinterpret_cast<char *> (ptr_);
      }
  
      int32_t key_len() const 
      { 
        return FIXED_ROWKEY_COL_NUM; 
      }

      void display(std::ostream &os)
      {
        os << " prefix: " << decode_ts64(prefix_);
        os << " flag: " << flag_;
        os << " suffix: " << decode_ts64(suffix_);      
      }
  
    private:
      char            ptr_[FIXED_KEY_LEN];
      char           *prefix_;
      char            flag_;
      char           *suffix_;
      common::ObObj   objs_[FIXED_ROWKEY_COL_NUM];
    };
  
  
    /**
     * Prints a one-line representation of the key to the given ostream.
     *
     * @param os output stream to print key to
     * @param key the key to print
     * @return the output stream
     */
    static inline std::ostream &operator <<(std::ostream &os, Key &key) 
    {
      key.display(os);
      return os;
    }
  
    inline bool operator==(const Key &k1, const Key &k2) 
    {
      return k1.compare(k2) == 0;
    }
  
    inline bool operator!=(const Key &k1, const Key &k2) 
    {
      return k1.compare(k2) != 0;
    }
  
    inline bool operator<(const Key &k1, const Key &k2) 
    {
      return k1.compare(k2) < 0;
    }
  
    inline bool operator<=(const Key &k1, const Key &k2) 
    {
      return k1.compare(k2) <= 0;
    }
  
    inline bool operator>(const Key &k1, const Key &k2) 
    {
      return k1.compare(k2) > 0;
    }
  
    inline bool operator>=(const Key &k1, const Key &k2) 
    {
      return k1.compare(k2) >= 0;
    }

  } // namespace oceanbase::sstable
} // namespace oceanbase

#endif // OCEANBASE_SSTABLE_KEY_H_

