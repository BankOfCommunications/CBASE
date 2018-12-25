/** (C) 2007-2010 Taobao Inc.
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

#include "common/ob_result.h"
#include "ob_msync_client.h"

namespace oceanbase
{
  namespace msync
  {
    const char* server_to_str(ObServer svr)
    {
      static char buf[64];
      svr.to_string(buf, sizeof(buf));
      buf[sizeof(buf)-1] = 0;
      return buf;
    }
    
    int ObMsyncClient::start_sync_mutator()
    {
      int err = OB_SUCCESS;
      ObDataBuffer mut_buffer;
      uint64_t seq = 0;
      while(!stop_ && OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = reader_.read(mut_buffer, seq)))
        {
          TBSYS_LOG(ERROR, "read_mutator()=>%d", err);
        }
        else if (OB_SUCCESS != (err = send_mutator_(mut_buffer)))
        {
          TBSYS_LOG(ERROR, "send_mutator()=>%d", err);
        }
        else if (OB_SUCCESS != (err = after_sync(seq)))
        {
          TBSYS_LOG(ERROR, "after_sync(seq=%ld)=>%d", seq, err);
        }
      }
      return err;
    }
    
    int ObMsyncClient::send_mutator_(ObDataBuffer mut)
    {
      int err = OB_NEED_RETRY;
      for(int64_t retry = 0; !stop_ && OB_NEED_RETRY == err && retry < max_retry_times_; retry++)
      {
        err = send_mutator_may_need_retry_(mut);
        TBSYS_LOG(DEBUG, "send_mutator(try=%lu)=>%d", retry, err);
        if (OB_NEED_RETRY == err)
        {
          usleep(static_cast<useconds_t>(wait_time_));
        }
      }
      return err;
    }

    int ObMsyncClient::send_mutator_may_need_retry_(ObDataBuffer mut)
    {
      int err = OB_SUCCESS;
      ObServer server(ObServer::IPV4, host_, port_);
      ObDataBuffer out_buffer(packet_buffer_, sizeof(packet_buffer_));
      ObResultCode result_msg;
      int64_t pos = 0;
      if (OB_SUCCESS != (err = send_request(server, OB_WRITE, MY_VERSION, timeout_, mut, out_buffer)))
      {
        TBSYS_LOG(DEBUG, "send_request(server='%s', cmd=OB_WRITE)=>%d", server_to_str(server), err);
      }
      else if (OB_SUCCESS != (err = result_msg.deserialize(out_buffer.get_data(), out_buffer.get_position(),
                                                           pos)))
      {
        TBSYS_LOG(WARN, "result_msg.deserialize()=>%d", err);
      }
      else if (OB_SUCCESS != (err = result_msg.result_code_))
      {
        TBSYS_LOG(WARN, "result_msg.result_code=%d", err);
      }
      if (OB_RESPONSE_TIME_OUT == err)
      {
        err = OB_NEED_RETRY;
      }
      return err;
    }
  } //end namespace msync
} // end namespace oceanbase
