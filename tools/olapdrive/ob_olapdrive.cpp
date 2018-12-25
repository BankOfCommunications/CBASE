/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_olapdrive.cpp for define olapdrive worker. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_olapdrive.h"
#include "common/ob_malloc.h"
#include "common/utility.h"

namespace oceanbase 
{ 
  namespace olapdrive 
  {
    using namespace common;

    ObOlapdrive::ObOlapdrive()
    : client_(servers_mgr_), meta_(olapdrive_schema_, client_), 
      write_worker_(client_, meta_, olapdrive_schema_, param_, stat_), 
      read_worker_(client_, meta_, olapdrive_schema_, param_, stat_)
    {

    }

    ObOlapdrive::~ObOlapdrive()
    {

    }

    int ObOlapdrive::init_servers_manager()
    {
      int ret                       = OB_SUCCESS;
      int64_t merge_server_count    = 0;
      const ObServer* merge_server  = NULL;

      ret = servers_mgr_.init(param_.get_merge_server_count() + 2);
      if (OB_SUCCESS == ret)
      {
        ret = servers_mgr_.set_root_server(param_.get_root_server());
      }

      if (OB_SUCCESS == ret)
      {
        ret = servers_mgr_.set_update_server(param_.get_update_server());
      }

      if (OB_SUCCESS == ret)
      {
        merge_server_count = param_.get_merge_server_count();
        merge_server = param_.get_merge_server();
        for (int64_t i = 0; 
             i < merge_server_count && NULL != merge_server && OB_SUCCESS == ret; 
             ++i)
        {
          ret = servers_mgr_.add_merge_server(merge_server[i]);
        }
      }

      return ret;
    }

    int ObOlapdrive::init()
    {
      int ret = OB_SUCCESS;

      ret = param_.load_from_config();
      if (OB_SUCCESS == ret)
      {
        ret = init_servers_manager();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to init servers manager");
        }
      }

      //for debuging
      if (0)
      {
        param_.dump_param();
      }

      if (OB_SUCCESS == ret)
      {
        ret = client_.init(param_.get_network_time_out());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to init client");
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = client_.fetch_schema(0, olapdrive_schema_.get_schema_manager());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to featch schema from root server");
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = olapdrive_schema_.init();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to init olapdrive schema manager");
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = meta_.init();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to init olapdrive meta");
        }
      }

      return ret;
    }

    int ObOlapdrive::start()
    {
      int ret                     = OB_SUCCESS; 
      int64_t write_thread_count  = 0;
      int64_t read_thread_count   = 0;
      int64_t stat_dump_interval  = 0;

      srandom(static_cast<int32_t>(time(0)));
      ret = ob_init_memory_pool();
      if (OB_SUCCESS == ret)
      {
        ret = init();
      }

      if (OB_SUCCESS == ret)
      {
        write_thread_count = param_.get_write_thread_count();
        read_thread_count = param_.get_read_thread_count();
        stat_dump_interval = param_.get_stat_dump_interval();

        if (write_thread_count > 0)
        {
          write_worker_.setThreadCount(static_cast<int32_t>(write_thread_count));
          write_worker_.start();
        }

        if (read_thread_count > 0)
        {
          read_worker_.setThreadCount(static_cast<int32_t>(read_thread_count));
          read_worker_.start();
        }

        if (stat_dump_interval > 0)
        {
          ret = stat_.init(stat_dump_interval);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to init stat");
          }
        }

        if (OB_SUCCESS == ret)
        {
          ret = wait();
        }
      }

      return ret;
    }

    int ObOlapdrive::stop()
    {
      write_worker_.stop();
      read_worker_.stop();
      
      return OB_SUCCESS;
    }

    int ObOlapdrive::wait()
    {
      write_worker_.wait();
      read_worker_.wait();
//    stat_.dump_stat();
      client_.stop();
      client_.wait();

      return OB_SUCCESS;
    }   
  } // end namespace olapdrive
} // end namespace oceanbase
