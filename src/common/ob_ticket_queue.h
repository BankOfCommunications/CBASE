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
#ifndef __OB_COMMON_OB_TICKET_QUEUE_H__
#define __OB_COMMON_OB_TICKET_QUEUE_H__
#include "ob_define.h"

namespace oceanbase
{
  namespace common
  {
    // not thread safe
    class ObTicketQueue
    {
      public:
        struct Item
        {
          int64_t seq_;
          void* data_;
        };
      public:
        ObTicketQueue();
        ~ObTicketQueue();
        int init(int64_t queue_len);
        int push(int64_t seq, void* data);
        int pop();
        int tail(int64_t& seq, void*& data);
        //add chujiajia [Paxos ups_replication] 20160112:b
        void clean_queue();
        //add:e
        int64_t size();
      protected:
        Item* get_item(int64_t idx){ return items_? items_ + (idx % queue_len_): NULL; }
      private:
        int64_t queue_len_;
        Item* items_;
        int64_t push_;
        int64_t pop_;
    };

  }; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_TICKET_QUEUE_H__ */
