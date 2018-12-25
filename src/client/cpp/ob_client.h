/**
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_client.h : C++ client of Oceanbase
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *
 */

#ifndef _OB_CLIENT_H__
#define _OB_CLIENT_H__

#include <stdint.h>

#include <common/ob_define.h>
#include <common/ob_packet.h>
#include <common/ob_server.h>
#include <common/ob_result.h>
#include <common/ob_get_param.h>
#include <common/ob_scan_param.h>
#include <common/ob_mutator.h>
#include <common/ob_scanner.h>
#include <common/ob_packet_factory.h>
#include <common/ob_client_manager.h>
#include <common/ob_list.h>
#include <common/ob_buffer.h>
#include <common/ob_thread_store.h>
#include <common/ob_timer.h>
#include <rootserver/ob_chunk_server_manager.h>

#include "oceanbase.h"

#include <Mutex.h>
#include <tbnet.h>

#include <list>

OB_ERR_CODE err_code_map(int err);
const char* ob_server_to_string(const oceanbase::common::ObServer& server);

struct ObInnerReq
{
  OB_REQ req;
  int64_t pop_counter;
  int64_t submit_time;
};

namespace oceanbase
{
  namespace client
  {
    using namespace common;

    const int32_t OB_DEFAULT_MAX_REQ   = 100;
    const int32_t OB_DEFAULT_TIMEOUT   = 3000000;    // microsecond
    const int32_t OB_DEFAULT_REFRESH_INTERVAL = 5000000; // microsecond
    const int32_t OB_DEFAULT_RETRY     = 5;
    const int32_t OB_DEFAULT_MINIMUM_RETRY_INTERVAL = 1000000; //microsecond

    struct ObApiCntl
    {
      /** 最大并发数 */
      int32_t max_req;
      /** Get操作超时时间 */
      int32_t timeout_get;
      /** Scan操作超时时间 */
      int32_t timeout_scan;
      /** Set操作超时时间 */
      int32_t timeout_set;
      /** 读取模式：同步读(0) or 异步读(1) */
      int32_t read_mode;
      /** 更新MS列表的周期 */
      int32_t refresh_interval;

      ObApiCntl()
      {
        max_req = OB_DEFAULT_MAX_REQ;
        timeout_get = timeout_scan = timeout_set = OB_DEFAULT_TIMEOUT;
        read_mode = ObScanParam::PREREAD;
        refresh_interval = OB_DEFAULT_REFRESH_INTERVAL;
      }
    };

    class ReqList
    {
      public:
        int push_back(ObInnerReq *i_req)
        {
          int ret = OB_SUCCESS;
          mutex_.lock();
          ret = list_.push_back(i_req);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "ObList push_back error, ret=%d, "
                "this should not happened", ret);
          }
          mutex_.unlock();
          return ret;
        }
        int pop_front(ObInnerReq *&i_req)
        {
          int ret = OB_SUCCESS;
          mutex_.lock();
          if (list_.empty())
          {
            i_req = NULL;
          }
          else
          {
            i_req = *(list_.begin());
            ret = list_.pop_front();
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "ObList push_back error, ret=%d, "
                  "this should not happened", ret);
            }
          }
          mutex_.unlock();

          if (NULL != i_req)
          {
            i_req->pop_counter++;
          }

          return ret;
        }
        void clear()
        {
          mutex_.lock();
          list_.clear();
          mutex_.unlock();
        }

      protected:
        tbutil::Mutex mutex_;
        ObList<ObInnerReq *> list_;
    };

    class init_buffer
    {
      public:
        void operator()(void *ptr)
        {
          new (ptr) buffer(OB_MAX_PACKET_LENGTH);
        }
    };

    class ServerCache : public ObTimerTask
    {
      public:
        ServerCache();
        virtual ~ServerCache();
        virtual int periodic_update() = 0;
        virtual int manual_update() = 0;
        int init(const ObServer &rs, ObClientManager *client,
            int32_t refresh_interval);
        virtual void runTimerTask();
      protected:
        ObServer rs_;
        int64_t last_update_time_;
        ObClientManager *client_;
        int32_t refresh_interval_;
        bool initialized_;
    };

    /**
     * MsList记录了所有Mergeserver的地址
     * update函数会从RS处获取新的MS列表，如果列表有变化，则更新自己的
     * get_one函数会从MS列表里选择一个返回
     */
    class MsList : public ServerCache
    {
      public:
        MsList();
        ~MsList();
        void clear();
        const ObServer get_one();
        std::vector<ObServer> get_whole_list();
        virtual int periodic_update();
        virtual int manual_update();
      protected:
        int update_(bool manual);
        bool list_equal_(const std::vector<ObServer> &list);
        void list_copy_(const std::vector<ObServer> &list);
      protected:
        std::vector<ObServer> ms_list_;
        uint64_t ms_iter_;
        uint64_t ms_iter_step_;
        buffer buff_;
        tbsys::CRWSimpleLock rwlock_;
        tbutil::Mutex update_mutex_;
    };

    class UpsMaster : public ServerCache
    {
      public:
        UpsMaster();
        ~UpsMaster();
        void clear();
        const ObServer get_ups();
        virtual int periodic_update();
        virtual int manual_update();
      protected:
        int update_(bool manual);
      protected:
        ObServer ups_;
        buffer buff_;
        tbsys::CRWSimpleLock rwlock_;
        tbutil::Mutex update_mutex_;
    };

    class ObClient : public tbnet::IPacketHandler
    {
    public:
      /**
       * 连接OceanBase
       * @param rs_addr Rootserver地址
       * @param rs_port Rootserver端口号
       */
      int connect(const char* rs_addr, const int rs_port,
                  const char* user, const char* passwd);

      /**
       * 提交异步请求
       * @param reqs_num 异步请求个数
       * @param reqs 异步请求数组
       */
      int submit(int reqs_num, OB_REQ *reqs[]);
      /**
       * 取消异步请求
       * @param reqs_num 异步请求个数
       * @param reqs 异步请求数组
       */
      int cancel(int reqs_num, OB_REQ *reqs[]);
      /**
       * 获取异步结果
       *   当min_num=0或者timeout=0时, 该函数不会阻塞
       *   当timeout=-1时, 表示等待时间是无限长, 直到满足条件
       * @param min_num 本次获取的最少异步完成事件个数
       * @param min_num 本次获取的最多异步完成事件个数
       * @param timeout 超时(us)
       * @param num 输出参数, 异步返回结果个数
       * @param reqs 输出参数, 异步返回结果数组
       */
      int get_results(int min_num, int max_num, int64_t timeout,
                      int64_t &num, OB_REQ* reqs[]);

      OB_RES* scan(const ObScanParam& scan);

      OB_RES* get(const ObGetParam& get);

      OB_ERR_CODE set(const ObMutator& mut);

      /**
       * 获取get请求的req
       */
      OB_REQ* acquire_get_req();
      /**
       * 获取scan请求的req
       */
      OB_REQ* acquire_scan_req();
      /**
       * 获取set请求的req
       */
      OB_REQ* acquire_set_req();
      /**
       * 释放req
       */
      void release_req(OB_REQ* req, bool free_mem = true);

      OB_GET* acquire_get_st();

      OB_SCAN* acquire_scan_st();

      OB_SET* acquire_set_st();

      OB_RES* acquire_res_st();

      void release_get_st(OB_GET* ob_get);

      void release_scan_st(OB_SCAN* ob_scan);

      void release_set_st(OB_SET* ob_set);

      void reset_get_st(OB_GET* ob_get);

      void reset_scan_st(OB_SCAN* ob_scan);

      void reset_set_st(OB_SET* ob_set);

      void release_res_st(OB_RES* res);

      /**
       * 设置事件通知fd
       */
      void set_resfd(int32_t fd, OB_REQ* cb);

      OB_ERR_CODE api_cntl(int32_t cmd, va_list args);

      OB_ERR_CODE ob_debug(int32_t cmd, va_list args);

      /**
       * 将MergeServer列表转换成ObScanner格式
       * 结果格式描述如下：
       *   Row Key         | Port(INT) |
       *   -----------------------------
       *   10.232.36.30    | 2600      |
       *   10.232.36.31    | 2600      |
       *   10.232.36.32    | 2600      |
       * row key是Mergeserver地址的字符串表示
       * Port是Mergeserver的端口号
       */
      int get_ms_list(ObScanner &scanner);

      ObClient();
      virtual ~ObClient();


#ifdef NDEBUG
    protected:
#else
    public:
#endif
//      int init_us_addr();

      int init_ms_addr();

      int init_reqs();

      void close();

      OB_REQ* acquire_req();

      int submit_set(const ObServer &server, OB_REQ *req);

      int submit_scan(const ObServer &server, OB_REQ *req);

      int submit_get(const ObServer &server, OB_REQ *req);

      static void clear_req(OB_REQ *r);

      static void print_req(OB_REQ *r, const char* TARGET);

      static int check_req_integrity(OB_REQ *req);

      tbnet::IPacketHandler::HPRetCode
          handlePacket(tbnet::Packet* packet, void * args);

      int send_request(const ObServer& server,  const int32_t pcode,
                       const int64_t timeout, const ObDataBuffer& in_buffer,
                       OB_REQ *req);

      int dispatch_req(OB_REQ *req);

      int get_us(OB_REQ *req, ObServer &server);

      int get_ms(OB_REQ *req, ObServer &server);

      OB_ERR_CODE serialize_get(const ObGetParam& get,
                                ObDataBuffer& data_buff);

      OB_ERR_CODE serialize_scan(const ObScanParam& scan,
                                 ObDataBuffer& data_buff);

      OB_ERR_CODE serialize_set(const ObMutator& mut,
                                ObDataBuffer& data_buff);

      OB_ERR_CODE deserialize_get(const char* data,
                                  int64_t data_len,
                                  ObResultCode& res_code,
                                  ObScanner& scanner);

      OB_ERR_CODE deserialize_scan(const char* data,
                                   int64_t data_len,
                                   ObResultCode& res_code,
                                   ObScanner& scanner);

      OB_ERR_CODE deserialize_set(const char* data,
                                  int64_t data_len,
                                  ObResultCode& res_code);

      OB_ERR_CODE get_ms(const ObServer& server,
                         ObDataBuffer& data_buff,
                         int32_t timeout,
                         std::list<ObScanner>& scanner);

      OB_ERR_CODE scan_ms(const ObServer& server,
                          ObDataBuffer& data_buff,
                          int32_t timeout,
                          std::list<ObScanner>& scanner);

      OB_ERR_CODE set_us(const ObServer& server,
                         ObDataBuffer& data_buff,
                         int32_t timeout);

      OB_ERR_CODE request(const ObServer& server,
                          int command,
                          ObDataBuffer& data_buff,
                          int32_t timeout);

      OB_ERR_CODE clear_active_memtable();

      OB_ERR_CODE get_thread_data_buffer(common::ObDataBuffer &data_buffer);

#ifdef NDEBUG
    protected:
#else
    public:
#endif
      ObServer rs_;
      ObInnerReq *reqs_;
      ReqList free_list_;
      ReqList ready_list_;
      ReqList retry_list_;

      tbnet::ConnectionManager connmgr_;
      tbnet::DefaultPacketStreamer streamer_;
      tbnet::Transport transport_;
      ObPacketFactory factory_;
      ObClientManager client_;
      thread_store<buffer, init_buffer> tbuff_;
      buffer buff_;
      MsList ms_list_;
      UpsMaster ups_master_;
      ObTimer timer_;

      ObApiCntl cntl_;
    };

  } // end namespace client
} // end namespace oceanbase

#endif // _OB_CLIENT_H__
