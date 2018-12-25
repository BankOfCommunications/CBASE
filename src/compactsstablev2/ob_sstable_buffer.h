#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_BUFFER_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_BUFFER_H_

#include "common/ob_list.h"
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include <tbsys.h>

class TestSSTableBuffer_construction_Test;
class TestSSTableBuffer_reset1_Test;
class TestSSTableBuffer_reset2_Test;
class TestSSTableBuffer_clear_Test;
class TestSSTableBuffer_init_Test;
class TestSSTableBuffer_add_item1_Test;
class TestSSTableBuffer_add_item2_Test;
class TestSSTableBuffer_get_data_multi_block_Test;
class TestSSTableBuffer_get_data_single_block_Test;
class TestSSTableBuffer_get_length_Test;
class TestSSTableBuffer_get_size_Test;
class TestSSTableBuffer_get_free_buf_Test;
class TestSSTableBuffer_set_write_size_Test;
class TestSSTableBuffer_alloc_block_Test;

namespace oceanbase
{
  namespace compactsstablev2
  { 
    class ObSSTableBuffer
    {
    public:
      friend class ::TestSSTableBuffer_construction_Test;
      friend class ::TestSSTableBuffer_reset1_Test;
      friend class ::TestSSTableBuffer_reset2_Test;
      friend class ::TestSSTableBuffer_clear_Test;
      friend class ::TestSSTableBuffer_init_Test;
      friend class ::TestSSTableBuffer_add_item1_Test;
      friend class ::TestSSTableBuffer_add_item2_Test;
      friend class ::TestSSTableBuffer_get_data_multi_block_Test;
      friend class ::TestSSTableBuffer_get_data_single_block_Test;
      friend class ::TestSSTableBuffer_get_length_Test;
      friend class ::TestSSTableBuffer_get_size_Test;
      friend class ::TestSSTableBuffer_get_free_buf_Test;
      friend class ::TestSSTableBuffer_set_write_size_Test;
      friend class ::TestSSTableBuffer_alloc_block_Test;

    private:
      //默认block的大小
      const static int64_t DEFAULT_MEM_BLOCK_SIZE = 2 * 1024 * 1024;

    public:
      struct MemBlock
      {
        int64_t cur_pos_;
        int64_t block_size_;
        char data_[0];
      };
  
    public:
      ObSSTableBuffer(const int64_t block_size = DEFAULT_MEM_BLOCK_SIZE);

      ~ObSSTableBuffer();

      //最多只留下一个block
      void reset();

      //清除所有的block
      void clear();

      //初始化，设置block大小
      void init(const int64_t block_size)
      {
        block_size_ = block_size;
      }

      //添加一条记录
      int add_item(const void* buf, const int64_t length);

      //将数据copy到ptr(适用于多个block的情况)
      int get_data(char* const buf, const int64_t buf_size, int64_t& length) const;

      //返回数据的ptr(适用于一个block的情况)
      int get_data(const char*& buf, int64_t& length) const;

      //获得实际数据的大小
      inline int64_t get_length() const
      {
        return length_;
      }

      //获取总的buffer的大小
      inline int64_t get_size() const
      {
        return size_;
      }

      //获取当前block的free大小(用于不能提前知道要写入多少时的情况）
      int get_free_buf(char*& buf, int64_t& size);

      //设置写入的实际数据的大小（用于ObRowkey的写入)
      int set_write_size(const int64_t length);

      //返回block的count
      int64_t get_block_count() const
      {
        return mem_block_list_.size();
      }

      //分配一个新的block
      int alloc_block();
       
    private:
      //分配内存
      int alloc_mem(void*& buf, const int64_t size);

      //释放内存
      inline void free_mem(void* buf)
      {
        common::ob_free(buf);
      }
         
    private:
      //block list
      common::ObList<MemBlock*> mem_block_list_;

      //block size
      int64_t block_size_;
       
      //size
      int64_t size_;     //total size
      int64_t length_;   //real size
    };
  }
}
#endif
