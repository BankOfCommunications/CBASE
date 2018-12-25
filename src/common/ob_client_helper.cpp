/*
 * (C) 2007-2010 TaoBao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_client_helper.cpp is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   MaoQi maoqi@taobao.com
 *
 */

#include "ob_client_helper.h"
#include "ob_client_manager.h"
#include "ob_result.h"
#include "utility.h"
#include "ob_rowkey.h"
#include "thread_buffer.h"

namespace oceanbase
{
  namespace common
  {
    ObClientHelper::ObClientHelper() :inited_(false),client_manager_(NULL),thread_buffer_(NULL),timeout_(100*1000L)
    {}

    void ObClientHelper::init(ObClientManager* client_manager, ThreadSpecificBuffer *thread_buffer,
                              const ObServer root_server, int64_t timeout)
    {
      if (!inited_)
      {
        client_manager_ = client_manager;
        thread_buffer_ = thread_buffer;
        root_server_ = root_server;
        timeout_ = timeout;
        inited_ = true;
      }
    }
    
    int ObClientHelper::scan(const ObScanParam& scan_param,ObScanner& scanner)
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        ret = OB_NOT_INIT; 
      }

      if ((OB_SUCCESS == ret) && 
          ((ret = get_tablet_info(scan_param)) != OB_SUCCESS) )
      {
        TBSYS_LOG(ERROR,"cann't get mergeserver,ret = %d",ret);
      }

      for(uint32_t i=0; i < sizeof(merge_server_) / sizeof(merge_server_[0]); ++i)
      {
        if ( 0 != merge_server_[i].get_ipv4() && 0 != merge_server_[i].get_port())
        {
          if ((ret = scan(merge_server_[i],scan_param,scanner)) != OB_SUCCESS)
          {
            TBSYS_LOG(INFO,"scan from (%s)", to_cstring(merge_server_[i]));
            if (OB_RESPONSE_TIME_OUT == ret || OB_PACKET_NOT_SENT == ret)
            {
              TBSYS_LOG(WARN,"scan from (%s) error,ret = %d", to_cstring(merge_server_[i]),ret);
              continue; //retry
            }
          }
          break;
        }
      }
      return ret;
    }
    
    int ObClientHelper::get(const ObGetParam& get_param,ObScanner& scanner)
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        ret = OB_NOT_INIT; 
      }

      if ((OB_SUCCESS == ret) && 
          ((ret = get_tablet_info(get_param)) != OB_SUCCESS) )
      {
        TBSYS_LOG(ERROR,"cann't get mergeserver,ret = %d",ret);
      }

      for(uint32_t i=0; i < sizeof(merge_server_) / sizeof(merge_server_[0]); ++i)
      {
        if ( 0 != merge_server_[i].get_ipv4() && 0 != merge_server_[i].get_port())
        {
          TBSYS_LOG(DEBUG,"get from (%s)", to_cstring(merge_server_[i]));
        
          if ((ret = get(merge_server_[i],get_param,scanner)) != OB_SUCCESS)
          {
            if (OB_RESPONSE_TIME_OUT == ret || OB_PACKET_NOT_SENT == ret)
            {
              TBSYS_LOG(WARN,"get from (%s) error,ret = %d", to_cstring(merge_server_[i]), ret);
              continue; //retry
            }
          }
          break;
        }
      }
      return ret;
    }


    int ObClientHelper::scan(const ObServer& server,const ObScanParam& scan_param,ObScanner& scanner)
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
    
    int ObClientHelper::get(const ObServer& server,const ObGetParam& get_param,ObScanner& scanner)
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
  
    int ObClientHelper::apply(const ObServer& update_server, const ObMutator& mutator)
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
          ret = client_manager_->send_request(update_server, OB_WRITE, MY_VERSION, timeout_, data_buff);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to send request to (%s), ret=%d", to_cstring(update_server),ret);
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

    
    int ObClientHelper::get_tablet_info(const ObScanParam& scan_param)
    {
      ObScanner scanner;
      int ret = OB_SUCCESS;

      if ((ret = scan(root_server_,scan_param,scanner)) != OB_SUCCESS) 
      {
        TBSYS_LOG(ERROR,"get tablet from rootserver(%s) failed:[%d]", to_cstring(root_server_),ret);
      }

      if (OB_SUCCESS == ret)
      {
        ret = parse_merge_server(scanner); 
      }
      return  ret;
    }
    
    int ObClientHelper::get_tablet_info(const ObGetParam& param)
    {
      int ret = OB_SUCCESS;
      ObScanner scanner;

      if ((ret = get(root_server_,param,scanner)) != OB_SUCCESS) 
      {
        TBSYS_LOG(ERROR,"get tablet from rootserver(%s) failed:[%d]", to_cstring(root_server_), ret);
      }

      if (OB_SUCCESS == ret)
      {
        ret = parse_merge_server(scanner);
      }

      return ret;
    }

    int ObClientHelper::parse_merge_server(ObScanner& scanner)
    {
      ObServer server;
      ObRowkey start_key;
      ObRowkey end_key; 
      ObCellInfo * cell = NULL;
      ObScannerIterator iter; 
      bool row_change = false;
      int index = 0;
      int ret = OB_SUCCESS;

      int64_t ip = 0;
      int64_t port = 0;
      int64_t version = 0;
      iter = scanner.begin();
      ret = iter.get_cell(&cell, &row_change);
      row_change = false;

      while((OB_SUCCESS == ret))
      {
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "get cell from scanner iterator failed:ret[%d]", ret);
        }
        else if (row_change && index > 0)
        {
          TBSYS_LOG(DEBUG,"row changed,ignore, %s", to_cstring(cell->row_key_)); 
          break; //just get one row        
        } 
        else if (cell != NULL)
        {
          end_key = cell->row_key_;
          if ((cell->column_name_.compare("1_ms_port") == 0) 
              || (cell->column_name_.compare("2_ms_port") == 0) 
              || (cell->column_name_.compare("3_ms_port") == 0)
              //add zhaoqiong[roottable tablet management]20160104:b
              || (cell->column_name_.compare("4_ms_port") == 0)
              || (cell->column_name_.compare("5_ms_port") == 0)
              || (cell->column_name_.compare("6_ms_port") == 0))
            //add:e
          {
            ret = cell->value_.get_int(port);
            TBSYS_LOG(DEBUG,"port is %ld",port);
          }
          else if ((cell->column_name_.compare("1_ipv4") == 0)
                   || (cell->column_name_.compare("2_ipv4") == 0)
                   || (cell->column_name_.compare("3_ipv4") == 0)
                   //add zhaoqiong[roottable tablet management]20160104:b
                   || (cell->column_name_.compare("4_ipv4") == 0)
                   || (cell->column_name_.compare("5_ipv4") == 0)
                   || (cell->column_name_.compare("6_ipv4") == 0))
            //add:e
          {
            ret = cell->value_.get_int(ip);
            TBSYS_LOG(DEBUG,"ip is %ld",ip);
          }
          else if ((cell->column_name_.compare("1_tablet_version") == 0)
                   || (cell->column_name_.compare("2_tablet_version") == 0)
                   || (cell->column_name_.compare("3_tablet_version") == 0)
                   //add zhaoqiong[roottable tablet management]20160104:b
                   || (cell->column_name_.compare("4_tablet_version") == 0)
                   || (cell->column_name_.compare("5_tablet_version") == 0)
                   || (cell->column_name_.compare("6_tablet_version") == 0))
            //add:e
          {
            ret = cell->value_.get_int(version);
            TBSYS_LOG(DEBUG,"tablet_version is %ld, rowkey=%s",version, to_cstring(cell->row_key_));
          }

          if (OB_SUCCESS == ret)
          {
            if (0 != port && 0 != ip && 0 != version)
            {
              TBSYS_LOG(DEBUG,"ip,port,version:%ld,%ld,%ld",ip,port,version);
              merge_server_[index++].set_ipv4_addr(static_cast<int32_t>(ip), static_cast<int32_t>(port));
              ip = port = version = 0;
            }
          }
          else 
          {
            TBSYS_LOG(ERROR, "check get value failed:ret[%d]", ret);
          }

          if (++iter == scanner.end())
            break;
          ret = iter.get_cell(&cell, &row_change);
        }
        else
        {
          //impossible
        }
      }
      return ret;
    }

    int ObClientHelper::get_thread_buffer_(ObDataBuffer& data_buff)
    {
      int err = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer* buffer = thread_buffer_->get_buffer();
      buffer->reset();
      data_buff.set_data(buffer->current(), buffer->remain());
      return err;
    }
    
  } /* common */
  
} /* oceanbase */
