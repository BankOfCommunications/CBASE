/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_link.h is for what ...
 *
 * Version: $id: ob_link.h,v 0.1 8/19/2010 9:56a wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef OCEANBASE_COMMON_LINK_H_
#define OCEANBASE_COMMON_LINK_H_
#include <stddef.h>
#include "tbsys.h"
namespace oceanbase
{
  namespace common
  {
    /// @class  single link item
    /// @author wushi(wushi.ly@taobao.com)  (8/18/2010)
    class ObSLink
    {
    public:
      /// @fn constructor
      inline ObSLink()
      {
        initialize();
      }

      /// @fn destructor
      virtual ~ObSLink()
      {
        next_ = NULL;
      }

      /// @fn check if single list is empty
      inline bool is_empty()
      {
        return next_ == NULL;
      }

      /// @fn get next item in single list
      inline ObSLink * next()
      {
        return next_;
      }

      /// @fn extract the item after this
      inline ObSLink * extract_next()
      {
        ObSLink *result = next_;
        if (result != NULL)
        {
          next_ = result->next_;
        }
        return result;
      }
    private:
      /// @fn initialize
      inline void initialize()
      {
        next_ = NULL;
      }
      /// @property pointer to next obj in single list
      ObSLink * next_;
    };

    /// @class  double link list item
    /// @author wushi(wushi.ly@taobao.com)  (8/19/2010)
    class ObDLink
    {
    public:
      /// @fn constructor
      inline ObDLink()
      {
        initialize();
      }



      /// @fn check if double list is empty
      inline bool is_empty()
      {
        if (next_ == NULL || prev_ == NULL)
        {
          TBSYS_LOG(ERROR, "dlink item corrupt [link_addr:%p,next:%p,prev:%p]",
                    this, next_, prev_);
          return true;
        }
        return next_ == this;
      }

      /// @fn insert an item after this ObDLink
      inline void insert_next(ObDLink & link)
      {
        /// next_和prev_是私有变量，实现的是循环双向链表，因此只有在内存越界的情况下才为空
        /// 所以这里只打印了fatal日志，没有返回错误，其实应该打印fatal，然后core掉
        if (next_ == NULL || prev_ == NULL)
        {
          TBSYS_LOG(ERROR, "dlink item corrupt [link_addr:%p,next:%p,prev:%p]",
                    this, next_, prev_);
        }
        else
        {
          link.next_ = next_;
          link.prev_ = this;
          next_->prev_ = &link;
          next_ = &link;
        }
      }

      /// @fn insert an item before this ObDLink
      inline void insert_prev(ObDLink & link)
      {
        if (next_ == NULL || prev_ == NULL)
        {
          TBSYS_LOG(ERROR, "dlink item corrupt [link_addr:%p,next:%p,prev:%p]",
                    this, next_, prev_);
        }
        else
        {
          link.prev_ = prev_;
          link.next_ = this;
          prev_->next_ = &link;
          prev_ = &link;
        }
      }

      /// @fn remove self from double list
      inline void remove()
      {
        if (next_ == NULL || prev_ == NULL)
        {
          TBSYS_LOG(ERROR, "dlink item corrupt [link_addr:%p,next:%p,prev:%p]",
                    this, next_, prev_);
        }
        else
        {
          prev_->next_ = next_;
          next_->prev_ = prev_;
          initialize();
        }
      }

      /// @fn remove and return the item after self if exist
      inline ObDLink * extract_next()
      {
        ObDLink *result = NULL;
        if (!is_empty())
        {
          result = next_;
          result->remove();
        }
        return result;
      }

      /// @fn remove and return the item before self if exist
      inline ObDLink * extract_prev()
      {
        ObDLink *result = NULL;
        if (!is_empty())
        {
          result = prev_;
          result->remove();
        }
        return result;
      }

      /// @fn replace self with link
      inline void replace(ObDLink &link)
      {
        if (next_ == NULL || prev_ == NULL)
        {
          TBSYS_LOG(ERROR, "dlink item corrupt [link_addr:%p,next:%p,prev:%p]",
                    this, next_, prev_);
        }
        else
        {
          next_->prev_ = &link;
          prev_->next_ = &link;
          link.prev_ = prev_;
          link.next_ = next_;
          initialize();
        }
      }

      /// @fn get next item
      inline ObDLink *next()
      {
        return next_;
      }

      /// @fn get preivous item
      inline ObDLink *prev()
      {
        return prev_;
      }
    private:
      /// @fn initialize
      inline void initialize()
      {
        next_ = this;
        prev_ = this;
      }
      /// @property point to next item in double list
      ObDLink * next_;
      /// @property point to previous item in double list
      ObDLink * prev_;
    };

/// @def get the object of type "type" that contains the field 
/// "field" stating in address "address"
#ifndef CONTAINING_RECORD
#define CONTAINING_RECORD(address, type, field) ((type *)( 				            \
                                                 (char*)(address) - 			      \
                                                 (long)(&((type *)1)->field) + 1))
#endif
  }
}
#endif
