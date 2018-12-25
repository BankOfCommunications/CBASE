/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_rpc_event.h,v 0.1 2011/09/26 14:01:30 zhidong Exp $
 *
 * Authors:
 *   zhidong <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OCEANBASE_MERGER_RPC_EVENT_H_
#define OCEANBASE_MERGER_RPC_EVENT_H_

#include "ob_rpc_event.h"
#include "common/ob_packet.h"

namespace oceanbase
{
  namespace common
  {
    class ObPacket;
    class ObScanner;
    class ObReadParam;
  }

  namespace mergeserver
  {
    class ObMergerRequest;
    class ObMergerRpcEvent:public ObCommonRpcEvent
    {
    public:
      enum
      {
        SCAN_RPC,
        GET_RPC
      };
      ObMergerRpcEvent();
      virtual ~ObMergerRpcEvent();

    public:
      // reset stat for reuse
      void reset(void);

      int handle_packet(common::ObPacket* pacekt, void* args);

      /// client for request event check
      uint64_t get_client_id(void) const;
      const ObMergerRequest * get_client_request(void) const;

      // set eventid and client request in the init step
      virtual int init(const uint64_t client_id, ObMergerRequest * request);

      /// print info for debug
      void print_info(FILE * file) const;

      int32_t get_req_type()const
      {
        return req_type_;
      }
      void set_req_type(const int32_t req_type)
      {
        req_type_ = req_type;
      }
      void set_timeout_us(const int64_t timeout_us)
      {
        timeout_us_ = timeout_us;
      }
      int64_t get_timeout_us()const
      {
        return timeout_us_;
      }

    private:
      // check inner stat
      inline bool check_inner_stat(void) const;

      // deserialize the response packet
      int deserialize_packet(common::ObPacket & packet, common::ObScanner & result);

      // parse the packet
      int parse_packet(common::ObPacket * packet, void * args);

    protected:
      int32_t req_type_;
      // the request id
      uint64_t client_request_id_;
      ObMergerRequest * client_request_;
      int64_t timeout_us_;
    };

    bool ObMergerRpcEvent::check_inner_stat(void) const
    {
      return (client_request_ != NULL);
    }

  }
}

#endif // OCEANBASE_MERGER_RPC_EVENT_H_
