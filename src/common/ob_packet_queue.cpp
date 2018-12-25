#include "ob_packet_queue.h"

namespace oceanbase
{
  namespace common
  {
    ObPacketQueue::ObPacketQueue()
    {
      head_ = NULL;
      tail_ = NULL;
      size_ = 0;
      thread_buffer_ = new ThreadSpecificBuffer(THREAD_BUFFER_SIZE);
    }

    ObPacketQueue::~ObPacketQueue()
    {
      clear();
      if (thread_buffer_ != NULL)
      {
        delete thread_buffer_;
        thread_buffer_ = NULL;
      }
    }

    int ObPacketQueue::init()
    {
      int ret = OB_SUCCESS;

      ret = ring_buffer_.init();
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "ring buffer init failed, err: %d", ret);
      }

      return ret;
    }

    int ObPacketQueue::pop_packets(ObPacket** packet_arr, const int64_t ary_size, int64_t& ret_size)
    {
      int err = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer* tb = NULL;

      ret_size = 0;
      if (NULL == packet_arr || ary_size <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, packet_arr=%p, arr_size=%ld", packet_arr, ary_size);
        err = OB_ERROR;
      }
      else
      {
        tb = thread_buffer_->get_buffer();
        if (tb == NULL)
        {
          TBSYS_LOG(ERROR, "get packet thread buffer failed, return NULL");
          err = OB_ERROR;
        }
        else
        {
          tb->reset();
        }
      }

      while (OB_SUCCESS == err && ret_size < ary_size)
      {
        if (head_ == NULL)
        {
          break;
        }
        else
        {
          ObPacket* packet = head_;

          int64_t total_size = sizeof(ObPacket) + packet->get_packet_buffer()->get_capacity();
          if (tb->remain() < total_size)
          {
            if (0 == ret_size)
            {
              TBSYS_LOG(ERROR, "the thread buffer is too small, packet_size=%ld, buffer_size=%d",
                  total_size, tb->remain());
              // discard packet
              ring_buffer_.pop_task(head_);
              head_ = (ObPacket*) head_->_next;
              if (head_ == NULL)
              {
                tail_ = NULL;
              }
              size_ --;
              err = OB_ERROR;
            }
            break;
          }
          else
          {
            char* buf = tb->current();
            memcpy(buf, packet, total_size);
            packet_arr[ret_size] = (ObPacket*) buf;
            buf += sizeof(ObPacket);
            ((ObPacket*) packet_arr[ret_size])->set_packet_buffer(buf, packet->get_packet_buffer()->get_capacity());

            // pop from queue
            head_ = (ObPacket*) head_->_next;
            if (head_ == NULL)
            {
              tail_ = NULL;
            }
            size_ --;
            // pop from ring buffer
            err = ring_buffer_.pop_task(packet);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(ERROR, "failed to pop task, err=%d", err);
            }

            tb->advance(static_cast<int32_t>(total_size));
            ++ret_size;

          }
        }
      }

      return err;
    }

    ObPacket* ObPacketQueue::pop()
    {
      if (head_ == NULL)
      {
        return NULL;
      }

      ObPacket* packet = head_;
      ObPacket* ret_packet = NULL;
      head_ = (ObPacket*)head_->_next;
      if (head_ == NULL)
      {
        tail_ = NULL;
      }
      size_ --;

      int err = ring_buffer_.pop_task(packet);
      if (err == OB_SUCCESS)
      {
        ThreadSpecificBuffer::Buffer* tb = thread_buffer_->get_buffer();
        if (tb == NULL)
        {
          TBSYS_LOG(ERROR, "get packet thread buffer failed, return NULL");
        }
        else
        {
          int64_t total_size = sizeof(ObPacket) + packet->get_packet_buffer()->get_capacity();
          char* buf = tb->current();
          memcpy(buf, packet, total_size);
          ret_packet = (ObPacket*)buf;
          buf += sizeof(ObPacket);
          ret_packet->set_packet_buffer(buf, packet->get_packet_buffer()->get_capacity());
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "pop task from ring buffer failed, task: %p, err: %d", packet, err);
      }

      return ret_packet;
    }

    void ObPacketQueue::free(ObPacket* packet)
    {
      int err = OB_SUCCESS;
      if (packet != NULL)
      {
        err = ring_buffer_.pop_task(packet);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "pop task from ring buffer failed, task: %p, err: %d", packet, err);
        }
      }
    }

    void ObPacketQueue::clear()
    {
      if (head_ != NULL)
      {
        ObPacket* packet = NULL;
        while (head_ != NULL)
        {
          packet = head_;
          head_ = (ObPacket*)packet->_next;
          free(packet);
        }
        head_ = tail_ = NULL;
        size_ = 0;
      }
    }

    void ObPacketQueue::push(ObPacket* packet)
    {
      if (packet != NULL)
      {
        ObPacket* ret_ptr = NULL;
        ObDataBuffer* buf = packet->get_packet_buffer();
        int64_t size = sizeof(ObPacket) + buf->get_position();
        void* return_ptr = NULL;
        int err = ring_buffer_.push_task((const void*)packet, size, return_ptr);

        if (err == OB_SUCCESS)
        {
          ret_ptr = (ObPacket*)return_ptr;
          ret_ptr->_next = NULL;
          if (tail_ == NULL)
          {
            head_ = ret_ptr;
          }
          else
          {
            tail_->_next = ret_ptr;
          }
          ret_ptr->set_packet_buffer((char*)ret_ptr + sizeof(ObPacket), buf->get_position());

          tail_ = ret_ptr;
          size_ ++;
        }
        else
        {
          TBSYS_LOG(WARN, "push packet into ring buffer failed, size:%ld, err: %d", size, err);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "cast packet to ObPacket failed");
      }
    }

    int ObPacketQueue::size() const
    {
      return size_;
    }

    bool ObPacketQueue::empty() const
    {
      return (size_ == 0);
    }

    void ObPacketQueue::move_to(ObPacketQueue* destQueue)
    {
      if (head_ == NULL)
      {
        return;
      }

      if (destQueue->tail_ == NULL)
      {
        destQueue->head_ = head_;
      }
      else
      {
        destQueue->tail_->_next = head_;
      }
      destQueue->tail_ = tail_;
      destQueue->size_ += size_;
      head_ = tail_ = NULL;
      size_ = 0;
    }

    ObPacket* ObPacketQueue::get_timeout_list(const int64_t now)
    {
      ObPacket* list, *tail;
      list = tail = NULL;

      while (head_ != NULL)
      {
        int64_t t = head_->get_expire_time();
        if (t == 0 || t >= now) break;
        if (tail == NULL)
        {
          list = head_;
        }
        else
        {
          tail->_next = head_;
        }
        tail = head_;

        head_ = (ObPacket*)head_->_next;
        if (head_ == NULL)
        {
          tail_ = NULL;
        }

        size_ --;
      }

      if (tail)
      {
        tail->_next = NULL;
      }

      return list;
    }

    ObPacket* ObPacketQueue::get_packet_list()
    {
      return head_;
    }

  } /* common */
} /* oceanbase */
