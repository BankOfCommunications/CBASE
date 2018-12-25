/*
 * (C) 2007-2010 TaoBao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_client_proxy.cpp is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   MaoQi maoqi@taobao.com
 *
 */

#include "ob_client_proxy.h"
#include "ob_client_manager.h"
#include "ob_result.h"
#include "utility.h"
#include "thread_buffer.h"

namespace oceanbase
{
  namespace common
  {
    ObClientProxy::ObClientProxy() :inited_(false),client_manager_(NULL),thread_buffer_(NULL),timeout_(100*1000L)
    {}

    ObClientProxy::~ObClientProxy()
    {
    }

    void ObClientProxy::init(ObClientManager* client_manager, ThreadSpecificBuffer *thread_buffer,
                              MsList* ms_list, int64_t timeout)
    {
      if (!inited_)
      {
        client_manager_ = client_manager;
        thread_buffer_ = thread_buffer;
        ms_list_ = ms_list;
        timeout_ = timeout;
        inited_ = true;
      }
    }
    
    int ObClientProxy::scan(const ObScanParam& scan_param,ObScanner& scanner)
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        ret = OB_NOT_INIT; 
      }
      
      if(OB_SUCCESS == ret)
      {
        ret = scan(ms_list_->get_one(), scan_param, scanner);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "scan fail:ret[%d]", ret);
        }
      }

      return ret;
    }
    
    int ObClientProxy::get(const ObGetParam& get_param,ObScanner& scanner)
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        ret = OB_NOT_INIT; 
      }
      
      if(OB_SUCCESS == ret)
      {
        ret = get(ms_list_->get_one(), get_param, scanner);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "get fail:ret[%d]", ret);
        }
      }

      return ret;
    }


    int ObClientProxy::scan(const ObServer& server,const ObScanParam& scan_param,ObScanner& scanner)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      
      if (!inited_)
      {
        ret = OB_NOT_INIT; 
      }

      if (OB_SUCCESS == ret)
      {
        ObDataBuffer data_buff;
        get_thread_buffer_(data_buff);
        ret = scan_param.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());

        if (OB_SUCCESS == ret)
        {
          ret = client_manager_->send_request(server, OB_SCAN_REQUEST, MY_VERSION, timeout_, data_buff);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to send request to (%s), ret=%d", to_cstring(server),ret);
          }
        }

        if (OB_SUCCESS == ret)
        {
          // deserialize the response code
          int64_t pos = 0;
          if (OB_SUCCESS == ret)
          {
            ObResultCode result_code;
            ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
            }
            else
            {
              ret = result_code.result_code_;
              if (OB_SUCCESS == ret
                  && OB_SUCCESS != (ret = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
              {
                TBSYS_LOG(ERROR, "deserialize result data failed:pos[%ld], ret[%d]", pos, ret);
              }
            }
          }
        }
      }
      return ret;
    }
    
    int ObClientProxy::get(const ObServer& server,const ObGetParam& get_param,ObScanner& scanner)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      
      if (!inited_)
      {
        ret = OB_NOT_INIT; 
      }

      if (OB_SUCCESS == ret)
      {
        ObDataBuffer data_buff;
        get_thread_buffer_(data_buff);
        ret = get_param.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());

        if (OB_SUCCESS == ret)
        {
          ret = client_manager_->send_request(server, OB_GET_REQUEST, MY_VERSION, timeout_, data_buff);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to send request to (%s), ret=%d", to_cstring(server), ret);
          }
        }

        if (OB_SUCCESS == ret)
        {
          // deserialize the response code
          int64_t pos = 0;
          if (OB_SUCCESS == ret)
          {
            ObResultCode result_code;
            ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
            }
            else
            {
              ret = result_code.result_code_;
              if (OB_SUCCESS == ret
                  && OB_SUCCESS != (ret = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
              {
                TBSYS_LOG(ERROR, "deserialize result data failed:pos[%ld], ret[%d]", pos, ret);
              }
            }
          }
        }
      }
      return ret;
    }
  
    int ObClientProxy::apply(const ObMutator& mutator)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;

      if (!inited_)
      {
        ret = OB_NOT_INIT; 
      }

      if (OB_SUCCESS == ret)
      {
        ObDataBuffer data_buff;
        get_thread_buffer_(data_buff);
        ret = mutator.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());

        if (OB_SUCCESS == ret)
        {
          const ObServer ms = ms_list_->get_one();
          ret = client_manager_->send_request(ms, OB_MS_MUTATE, MY_VERSION, timeout_, data_buff);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to send request to (%s), ret=%d", to_cstring(ms), ret);
          }
        }

        if (OB_SUCCESS == ret)
        {
          // deserialize the response code
          int64_t pos = 0;
          if (OB_SUCCESS == ret)
          {
            ObResultCode result_code;
            ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
            }
            else if (result_code.result_code_ != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR,"apply failed : %d",result_code.result_code_);
            }
          }
        }
      }
      return ret;
    }

    int ObClientProxy::get_thread_buffer_(ObDataBuffer& data_buff)
    {
      int err = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer* buffer = thread_buffer_->get_buffer();
      buffer->reset();
      data_buff.set_data(buffer->current(), buffer->remain());
      return err;
    }
    
  } /* common */
  
} /* oceanbase */
