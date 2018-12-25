#ifndef OB_ADAPTER_ALLOCATOR_H_
#define OB_ADAPTER_ALLOCATOR_H_

namespace oceanbase
{
  namespace common
  {
    class AdapterAllocator
    {
      public:
        AdapterAllocator()
          :buffer_(NULL), pos_(0)
        {
        }
        ~AdapterAllocator()
        {
        }
        inline void init(char *buffer)
        {
          buffer_ = buffer;
          pos_ = 0;
        }
        inline void* alloc(int64_t size)
        {
          void *ret = NULL;
          ret = static_cast<void*>(buffer_ + pos_);
          pos_ += size;
          return ret;
        }
        inline void free(void *buf)
        {
          UNUSED(buf);
        }
      private:
        char *buffer_;
        int64_t pos_;
    };
  }
}
#endif
