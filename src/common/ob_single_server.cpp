#include "ob_atomic.h"
#include "ob_single_server.h"
#include "ob_trace_id.h"
#include "ob_profile_log.h"
#include "ob_profile_type.h"
#include "ob_result.h"
#include "utility.h"
#include "tblog.h"

namespace oceanbase
{
  namespace common
  {
    ObSingleServer::ObSingleServer() : thread_count_(0), task_queue_size_(100), min_left_time_(0), drop_packet_count_(0), host_()
    {
    }

    ObSingleServer::~ObSingleServer()
    {
    }

    int ObSingleServer::initialize()
    {
      if (thread_count_ > 0)
      {
        IpPort ip_port;
        ip_port.ip_ = static_cast<uint16_t>(local_ip_ & 0x0000FFFF);
        ip_port.port_ = static_cast<uint16_t>(port_);
        default_task_queue_thread_.set_ip_port(ip_port);
        default_task_queue_thread_.set_host(host_);
        default_task_queue_thread_.setThreadParameter(thread_count_, this, NULL);
        default_task_queue_thread_.start();
      }

      return OB_SUCCESS;
    }

    void ObSingleServer::wait_for_queue()
    {
      if (thread_count_ > 0)
        default_task_queue_thread_.wait();
    }

    void ObSingleServer::destroy()
    {
      if (thread_count_ > 0) {
        default_task_queue_thread_.stop();
        wait_for_queue();
      }
      TBSYS_LOG(WARN, "task threads stoped.");
      ObBaseServer::destroy();
      TBSYS_LOG(WARN, "libeasy timer thread stoped.");
    }

    int ObSingleServer::set_min_left_time(const int64_t left_time)
    {
      int ret = OB_SUCCESS;
      if (left_time < 0)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "left time should positive, you provide: %ld", left_time);
      }
      else
      {
        min_left_time_ = left_time;
      }
      return ret;
    }

    int ObSingleServer::set_thread_count(const int thread_count)
    {
      int ret = OB_SUCCESS;

      if (thread_count < 0)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "thread count should positive, you provide: %d", thread_count);
      } else
      {
        thread_count_ = thread_count;
      }

      return ret;
    }
    void ObSingleServer::set_self_to_thread_queue(const ObServer & host)
    {
      host_ = host;
    }

    int ObSingleServer::set_io_thread_count(const int io_thread_count)
    {
      int ret = OB_SUCCESS;

      if (io_thread_count < 1)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "io thread count should bigger than one, you provide: %d", io_thread_count);
      }
      else
      {
        io_thread_count_ = io_thread_count;
      }
      return ret;
    }

    int ObSingleServer::set_default_queue_size(const int task_queue_size)
    {
      int ret = OB_SUCCESS;

      if (task_queue_size <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "default queue size should be positive, you provide %d", task_queue_size);
      } else
      {
        task_queue_size_ = task_queue_size;
        if (task_queue_size_ > MAX_TASK_QUEUE_SIZE)
        {
          TBSYS_LOG(TRACE, "huge task queue size(%d), make sure this is what you want.", task_queue_size_);
        }
      }
      return ret;
    }

    uint64_t ObSingleServer::get_drop_packet_count(void) const
    {
      return drop_packet_count_;
    }

    size_t ObSingleServer::get_current_queue_size(void) const
    {
      return default_task_queue_thread_.size();
    }

    int ObSingleServer::handlePacket(ObPacket* packet)
    {
      int rc = OB_SUCCESS;
      ObPacket* req = packet;
      TBSYS_LOG(DEBUG, "receive packet, pcode=%d channel=%d", req->get_packet_code(), req->get_channel_id());
      if (thread_count_ == 0)
      {
        handle_request(req);
      }
      else
      {
        bool ps = default_task_queue_thread_.push(req, task_queue_size_, false, false);
        if (!ps)
        {
          if (!handle_overflow_packet(req))
          {
            TBSYS_LOG(WARN, "overflow packet dropped, packet code: %d", req->get_packet_code());
          }
          rc = OB_QUEUE_OVERFLOW;
        }
      }
      return rc;
    }
    int ObSingleServer::handleBatchPacket(ObPacketQueue& packetQueue)
    {
      UNUSED(packetQueue);
      return OB_SUCCESS;
    /*
      int ret = OB_SUCCESS;
      ObPacketQueue temp_queue;

      ObPacket *packet= packetQueue.get_packet_list();

      while (packet != NULL)
      {
        if (thread_count_ == 0)
        {
          handle_request(packet);
        }
        else
        {
          temp_queue.push(packet); // the task queue will free this packet
        }

        packet = (ObPacket*)packet->get_next(); // handle as much as we can
      }

      if (temp_queue.size() > 0)
      {
        default_task_queue_thread_.pushQueue(temp_queue, task_queue_size_); // if queue is full, this will block
      }

      return ret;
    */
    }

    bool ObSingleServer::handlePacketQueue(ObPacket *packet, void *args)
    {
      UNUSED(args);
      ObPacket* req = (ObPacket*) packet;
      int64_t source_timeout = req->get_source_timeout();

      bool ret = false;
      if (source_timeout > 0)
      {
        int64_t receive_time = req->get_receive_ts();
        int64_t current_ts = tbsys::CTimeUtil::getTime();
        PROFILE_LOG(DEBUG, PACKET_RECEIVED_TIME, receive_time);
        PROFILE_LOG(DEBUG, WAIT_TIME_US_IN_QUEUE, current_ts - receive_time);
        if ((current_ts - receive_time) + min_left_time_ > source_timeout)
        {
          atomic_inc(&drop_packet_count_);
          TBSYS_LOG(WARN, "packet block time: %ld(us), plus min left time: %ld(us) "
              "exceed timeout: %ld(us), current queue size:%ld, packet id: %d, dropped",
              (current_ts - receive_time), min_left_time_, source_timeout, default_task_queue_thread_.size(),
              packet->get_packet_code());
          ret = true;
        }
      }

      if (!ret)
      {
        handle_request(req);
      }
      else
      {
        handle_timeout_packet(req);
      }
      return ret;
    }

    void ObSingleServer::handle_request(ObPacket *request)
    {
      if (request == NULL)
      {
        TBSYS_LOG(WARN, "handle a NULL packet");
      }
      else
      {
        easy_addr_t addr = get_easy_addr(request->get_request());
        const int32_t packet_code = request->get_packet_code();
        int ret = do_request(request);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "request process failed, pcode: %d, ret: %d, peer is %s",
              packet_code, ret, inet_ntoa_r(addr));
        }
      }
    }

    bool ObSingleServer::handle_overflow_packet(ObPacket* base_packet)
    {
      //send error packet to client
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      ThreadSpecificBuffer::Buffer* my_buffer = thread_buffer_.get_buffer();
      ObDataBuffer out_buff;
      result_msg.result_code_ = OB_DISCARD_PACKET;
      easy_addr_t addr = get_easy_addr(base_packet->get_request());
      if (MY_VERSION != base_packet->get_api_version())
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (NULL != my_buffer && out_buff.set_data(my_buffer->current(), my_buffer->remain()))
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result message serialize failed, err: %d", ret);
        }
      }
      else
      {
        TBSYS_LOG(WARN, "failed initialize out_buff, buffer=%p", my_buffer);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }

      if (ret == OB_SUCCESS)
      {
        ret = send_response(base_packet->get_packet_code() + 1, MY_VERSION, out_buff,
                            base_packet->get_request(), base_packet->get_channel_id());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to send response to client(%s)", inet_ntoa_r(addr));
        }
      }
      else
      {
        easy_request_wakeup(base_packet->get_request());
      }
      return false;
    }

    void ObSingleServer::handle_timeout_packet(ObPacket* base_packet)
    {
      if (NULL != base_packet)
      {
        easy_request_wakeup(base_packet->get_request());
      }
    }

  } /* common */
} /* oceanbase */
