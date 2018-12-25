#include "ob_mysql_command_queue.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace obmysql
  {
    ObMySQLCommandQueue::ObMySQLCommandQueue()
    {
      head_ = NULL;
      tail_ = NULL;
      size_ = 0;
      thread_buffer_ = new ThreadSpecificBuffer(THREAD_BUFFER_SIZE);
    }

    ObMySQLCommandQueue::~ObMySQLCommandQueue()
    {
      clear();
      if (NULL != thread_buffer_)
      {
        delete thread_buffer_;
        thread_buffer_ = NULL;
      }
    }

    int ObMySQLCommandQueue::init()
    {
      int ret = OB_SUCCESS;

      ret = ring_buffer_.init();
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "ring buffer init failed, err: %d", ret);
      }
      return ret;
    }

    ObMySQLCommandPacket* ObMySQLCommandQueue::pop()
    {
      ObMySQLCommandPacket* ret = NULL;
      if (NULL != head_)
      {
        ObMySQLCommandPacket* packet = head_;
        head_ = reinterpret_cast<ObMySQLCommandPacket*>(head_->next_);
        if (NULL == head_)
        {
          tail_ = NULL;
        }
        size_ --;

        int err = ring_buffer_.pop_task(packet);
        if (OB_SUCCESS == err)
        {
          ThreadSpecificBuffer::Buffer* tb = thread_buffer_->get_buffer();
          if (NULL == tb)
          {
            TBSYS_LOG(ERROR, "get packet thread buffer failed, return NULL");
          }
          else
          {
            //copy packet from ring buffer
            int64_t total_size = sizeof(ObMySQLCommandPacket) + packet->get_command_length();
            TBSYS_LOG(DEBUG, "pop out packet packet command length is %d total size is %ld",
                      packet->get_command_length(), total_size);
            char* buf = tb->current();
            memcpy(buf, packet, total_size);
            ret = reinterpret_cast<ObMySQLCommandPacket*>(buf);
            ret->set_command(buf + sizeof(ObMySQLCommandPacket), packet->get_command_length());
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "pop tack from ring buffer failed, task: %p, err: %d", packet, err);
        }
      }
      return ret;
    }

    void ObMySQLCommandQueue::free(ObMySQLCommandPacket* packet)
    {
      int err = OB_SUCCESS;
      if (NULL != packet)
      {
        err = ring_buffer_.pop_task(packet);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "pop task from ring buffer failed, task: %p, err: %d", packet, err);
        }
      }
    }

    void ObMySQLCommandQueue::clear()
    {
      if (NULL != head_)
      {
        ObMySQLCommandPacket* packet = NULL;
        while (NULL != head_)
        {
          packet = head_;
          head_ = reinterpret_cast<ObMySQLCommandPacket*>(packet->next_);
          free(packet);
        }
        head_ = tail_ = NULL;
        size_ = 0;
      }
    }

    void ObMySQLCommandQueue::push(ObMySQLCommandPacket* packet)
    {
      if (NULL != packet)
      {
        ObMySQLCommandPacket *ret_ptr = NULL;
        int64_t size = sizeof(ObMySQLCommandPacket) + packet->get_command_length();
        void* return_ptr = NULL;
        int err = ring_buffer_.push_task((const void*)packet, size, return_ptr);

        if (OB_SUCCESS == err)
        {
          ret_ptr = reinterpret_cast<ObMySQLCommandPacket*>(return_ptr);
          ret_ptr->next_ = NULL;
          if (NULL == tail_)
          {
            head_ = ret_ptr;
          }
          else
          {
            tail_->next_ = ret_ptr;
          }
          ret_ptr->set_command((char*)ret_ptr + sizeof(ObMySQLCommandPacket), packet->get_command_length());
          tail_ = ret_ptr;
          size_ ++;
        }
        else
        {
          TBSYS_LOG(WARN, "push packet into ring buffer failed, size: %ld, err: %d", size, err);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "cast packet to ObMySQLCommandPacket failed");
      }
    }

    int ObMySQLCommandQueue::size() const
    {
      return size_;
    }

    bool ObMySQLCommandQueue::empty() const
    {
      return (0 == size_);
    }
  }
}
