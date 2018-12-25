/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#include "ob_client_wait_obj.h"
#include "tbsys.h"
#include "easy_io.h"

namespace oceanbase
{
  namespace common
  {
    ObClientWaitObj::ObClientWaitObj(): err_(OB_SUCCESS), done_count_(0), response_(buf_, sizeof(buf_))
    {}

    ObClientWaitObj::~ObClientWaitObj()
    {}

    void ObClientWaitObj::before_post()
    {
      cond_.lock();
      err_ = OB_SUCCESS;
      done_count_ = 0;
    }

    void ObClientWaitObj::after_post(const int post_err)
    {
      if (OB_SUCCESS != post_err)
      {
        err_ = post_err;
        done_count_++;
      }
      cond_.unlock();
    }

    int ObClientWaitObj::wait(ObDataBuffer& response, const int64_t timeout_us)
    {
      int err = OB_SUCCESS;
      int64_t end_time_us = tbsys::CTimeUtil::getTime() + timeout_us;
      int64_t wait_time_us = timeout_us;
      int wait_time_ms = (int)(wait_time_us/1000LL);
      cond_.lock();
      while (done_count_ == 0 && (wait_time_ms = (int)(wait_time_us/1000LL)) > 0)
      {
        cond_.wait(wait_time_ms);
        wait_time_us = end_time_us - tbsys::CTimeUtil::getTime();
      }
      cond_.unlock();
      if (OB_SUCCESS != err_)
      {
        err = err_;
      }
      else if (done_count_ <= 0 && wait_time_ms <= 0)
      {
        err = OB_RESPONSE_TIME_OUT;
        TBSYS_LOG(ERROR, "wait(timeout_us=%ld) TIMEOUT", timeout_us);
      }
      else
      {
        response = response_;
      }
      return err;
    }

    int ObClientWaitObj::receive_packet(ObPacket* packet)
    {
      int err = OB_SUCCESS;
      ObDataBuffer* response_buffer = NULL;
      if (OB_SUCCESS != err_)
      {
        err = err_;
        TBSYS_LOG(ERROR, "post packet error, err=%d", err_);
      }
      else
      {
        cond_.lock();
        if (NULL == packet)
        {
          err_ = OB_RESPONSE_TIME_OUT;
          TBSYS_LOG(WARN, "receive NULL packet, timeout");
        }
        else if (NULL == (response_buffer = packet->get_buffer()))
        {
          err_ = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "response has NULL buffer");
        }
        else if ((int64_t)packet->get_data_length() > response_.get_capacity())
        {
          err = OB_BUF_NOT_ENOUGH;
          TBSYS_LOG(ERROR, "packet_size[%d] > buf_size[%ld]", packet->get_data_length(), response_.get_capacity());
        }
        else
        {
          response_.get_position() = 0;
          memcpy(response_.get_data() + response_.get_position(),
                 response_buffer->get_data() + response_buffer->get_position(),
                 packet->get_data_length());
          response_.get_position() += packet->get_data_length();
        }
        handle_response(packet);
        done_count_++;
        cond_.signal();
        cond_.unlock();
      }
      return err;
    }

    int ObClientWaitObj::on_receive_response(easy_request_t* r)
    {
      int ret = EASY_OK;
      if (NULL == r || NULL == r->ms)
      {
        TBSYS_LOG(WARN, "request is null or r->ms is null");
      }
      else if (NULL == r->user_data)
      {
        TBSYS_LOG(WARN, "request user_data == NULL");
      }
      else
      {
        ObClientWaitObj* self = (typeof(self))r->user_data;
        self->receive_packet((ObPacket*)r->ipacket);
      }
      if (NULL != r && NULL != r->ms)
      {
        easy_session_destroy(r->ms);
      }
      return ret;
    }
  }; // end namespace common
}; // end namespace oceanbase
