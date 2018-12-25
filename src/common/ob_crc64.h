/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 * ob_crc64.h is for what ...
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *   yubai <yubai.lk@taobao.com>
 *
 */
#ifndef  OCEANBASE_COMMON_CRC64_H_
#define  OCEANBASE_COMMON_CRC64_H_

#include <stdint.h>
#include <algorithm>
#include <string.h>

#define OB_DEFAULT_CRC64_POLYNOM 0xD800000000000000ULL

namespace oceanbase
{
  namespace common
  {
    /**
      * create the crc64_table and optimized_crc64_table for calculate
      * must be called before ob_crc64
      * if not, the return value of ob_crc64 will be undefined
      * @param crc64 polynom
      */
    void ob_init_crc64_table(const uint64_t polynom);

    /** 
      * Processes a multiblock of a CRC64 calculation. 
      * 
      * @returns Intermediate CRC64 value. 
      * @param   uCRC64  Current CRC64 intermediate value. 
      * @param   pv      The data block to process. 
      * @param   cb      The size of the data block in bytes. 
      */ 
    uint64_t ob_crc64(uint64_t uCRC64, const void *pv, int64_t cb) ;
    
    /** 
      * Calculate CRC64 for a memory block. 
      * 
      * @returns CRC64 for the memory block. 
      * @param   pv      Pointer to the memory block. 
      * @param   cb      Size of the memory block in bytes. 
      */ 
    uint64_t ob_crc64(const void *pv, int64_t cb);
    
    /**
      * Get the static CRC64 table. This function is only used for testing purpose.
      *
      */
    const uint64_t * ob_get_crc64_table();

    class ObBatchChecksum
    {
      // ob_crc64函数在计算64个字节整数倍的情况下优势明显
      static const int64_t BUFFER_SIZE = 65536;
      public:
        ObBatchChecksum() : pos_(0), base_(0)
        {
        };
        ~ObBatchChecksum()
        {
        };
      public:
        inline void reset()
        {
          pos_ = 0;
          base_ = 0;
        };
        inline void set_base(const uint64_t base)
        {
          base_ = base;
        };
        inline void fill(const void *pv, const int64_t cb)
        {
          if (NULL != pv
              && 0 < cb)
          {
            char *ptr = (char*)pv;
            int64_t size2fill = cb;
            int64_t size2copy = 0;
            while (size2fill > 0)
            {
              size2copy = std::min(BUFFER_SIZE - pos_, size2fill);
              memcpy(&buffer_[pos_], ptr + cb - size2fill, size2copy);
              size2fill -= size2copy;
              pos_ += size2copy;

              if (pos_ >= BUFFER_SIZE)
              {
                base_ = ob_crc64(base_, buffer_, BUFFER_SIZE);
                pos_ = 0;
              }
            }
          }
        };
        uint64_t calc()
        {
          if (0 < pos_)
          {
            base_ = ob_crc64(base_, buffer_, pos_);
            pos_ = 0;
          }
          return base_;
        };
      private:
        char buffer_[BUFFER_SIZE];
        int64_t pos_;
        uint64_t base_;
    };
  }
}

#endif
