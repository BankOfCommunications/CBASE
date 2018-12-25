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
#include "ob_ticket_queue.h"
#include "ob_malloc.h"

namespace oceanbase
{
  namespace common
  {
    ObTicketQueue::ObTicketQueue():queue_len_(0), items_(NULL), push_(0), pop_(0)
    {}

    ObTicketQueue::~ObTicketQueue()
    {
      if (NULL != items_)
      {
        ob_free(items_);
        items_ = NULL;
      }
    }

    int ObTicketQueue::init(int64_t queue_len)
    {
      int err = OB_SUCCESS;
      if (queue_len <= 0)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (NULL != items_)
      {
        err = OB_INIT_TWICE;
      }
      else if (NULL == (items_ = (Item*)ob_malloc(sizeof(Item) * queue_len, ObModIds::TICKET_QUEUE)))
      {
        err = OB_MEM_OVERFLOW;
        TBSYS_LOG(ERROR, "ob_malloc(%ld): fail", sizeof(Item)*queue_len);
      }
      else
      {
        memset(items_, 0, sizeof(Item) * queue_len);
        queue_len_ = queue_len;
      }
      return err;
    }

    int ObTicketQueue::push(int64_t seq, void* data)
    {
      int err = OB_SUCCESS;
      Item* item = NULL;
      if (NULL == items_)
      {
        err = OB_NOT_INIT;
      }
      else if (push_ >= pop_ + queue_len_)
      {
        err = OB_SIZE_OVERFLOW;
      }
      else
      {
        item = get_item(push_);
        item->seq_ = seq;
        item->data_ = data;
        push_++;
      }
      return err;
    }

    int ObTicketQueue::pop()
    {
      int err = OB_SUCCESS;
      if (NULL == items_)
      {
        err = OB_NOT_INIT;
      }
      else if (pop_ >= push_)
      {
        err = OB_ENTRY_NOT_EXIST;
      }
      else
      {
        pop_++;
      }
      return err;
    }

    int ObTicketQueue::tail(int64_t& seq, void*& data)
    {
      int err = OB_SUCCESS;
      Item* item = NULL;
      if (NULL == items_)
      {
        err = OB_NOT_INIT;
      }
      else if (pop_ >= push_)
      {
        err = OB_ENTRY_NOT_EXIST;
      }
      else
      {
        item = get_item(pop_);
        seq = item->seq_;
        data = item->data_;
      }
      return err;
    }

    int64_t ObTicketQueue::size()
    {
      return push_ - pop_;
    }

    //add chujiajia [Paxos ups_replication] 20160112:b
    void ObTicketQueue::clean_queue()
    {
      push_ = 0;
      pop_ = 0;
    }
    //add:e

  }; // end namespace common
}; // end namespace oceanbase
