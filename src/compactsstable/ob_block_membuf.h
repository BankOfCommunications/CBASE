/*
* (C) 2007-2011 TaoBao Inc.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
* ob_block_membuf.h is for what ...
*
* Version: $id$
*
* Authors:
*   MaoQi maoqi@taobao.com
*
*/
#ifndef OB_BLOCK_MEMBUF_H
#define OB_BLOCK_MEMBUF_H

#include <stdint.h>
#include "common/ob_define.h"
#include "common/ob_file.h"
#include "common/ob_mod_define.h"

namespace oceanbase
{
  namespace compactsstable
  {
    class ObBlockMembuf
    {
    public:
      /**
       * BlockMemBuf used in the situation that we
       * do not know how many memory will alloc,
       * we can alloc some blocks,and linked them.
       * @param block_size
       */
      ObBlockMembuf(const int64_t block_size,const int32_t mod_id = common::ObModIds::OB_MEM_BUFFER);
      ~ObBlockMembuf();

      /**
       * copy buf to block
       *
       * @param buf  buffer will write to BlockMemBuf
       * @param size the size of buf
       * @param ret_addr the address of buf in BlockMemBuf
       *
       * @return OB_SUCCESS on success,otherwise on failed.
       */
      int write(const char* buf,const int64_t size,const char** ret_addr = NULL);

      /**
       * get buffer according to offset
       * *NOTE* read is a test helper
       *
       * @param offset offset in BlockMemBuf
       * @param size
       *
       * @return the address of buffer on success,NULL on failed.
       */
      const char* read(const int64_t offset,const int64_t size) const;

      /**
       * dump BlockMemBuf to disk
       *
       * @param filesys file appender
       * @param size the number of bytes dumped
       *
       * @return OB_SUCCESS on success,otherwise on failed.
       */
      int dump(common::ObFileAppender& filesys,int64_t& size);

      int64_t get_data_size() const;

      const char* get_current_pos() const;

      int clear();

    private:
      struct BlockMem
      {
        int64_t          data_size_;
        struct BlockMem* next_;
        char             buf_[0];
      };

      static const int64_t BLOCK_META_SIZE = sizeof(BlockMem);

    private:
      int alloc_block();

    private:
      int32_t   mod_id_;
      int64_t   block_size_;      // block size,include BLOCK_META_SIZE
      int64_t   block_data_size_; // block data size,not include BLOCK_META_SIZE
      int64_t   total_data_size_; // the number of bytes written
      int64_t   block_num_;       // the number of blocks
      BlockMem* buf_head_;
      BlockMem* current_block_;
    };
  }
}

#endif
