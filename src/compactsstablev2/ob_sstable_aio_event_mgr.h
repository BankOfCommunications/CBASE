#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_AIO_EVENT_MGR_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_AIO_EVENT_MGR_H_

#include <libaio.h>

namespace oceanbase 
{
  namespace compactsstablev2
  {
    class ObAIOBufferInterface;
    class ObAIOEventMgr
    {
    public:
      ObAIOEventMgr();

      ~ObAIOEventMgr();

      int init(io_context_t ctx);

      int aio_submit(const int fd, const int64_t offset, 
          const int64_t size, ObAIOBufferInterface& aio_buf);

      int aio_wait(int64_t& timeout_us);

    private:
      bool inited_;
      io_context_t ctx_;
      struct iocb iocb_;
    };
  }//end namespace compactsstablev2
}//end namespace oceanbase

#endif //OCEANBASE_SSTABLE_OB_AIO_EVENT_MGR_H_
