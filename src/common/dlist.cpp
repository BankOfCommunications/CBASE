#include <assert.h>
#include <stdlib.h>
#include "dlist.h"
#include "tbsys.h"
namespace oceanbase
{
  namespace common
  {
    DLink::DLink()
    {
      prev_ = NULL;
      next_ = NULL;
    }

    // insert one node before this node
    bool DLink::add_before(DLink *e)
    {
      return add(prev_, e, this);
    }

    // insert one node after this node
    bool DLink::add_after(DLink *e)
    {
      return add(this, e, next_);
    }

    // remove node from list
    bool DLink::unlink()
    {
      bool ret = true;
      if (NULL == prev_ || NULL == next_)
      {
        ret = false;
        TBSYS_LOG(ERROR, "prev_=%p and next_=%p must not null", prev_, next_);
      }
      else
      {
        prev_->next_ = next_;
        next_->prev_ = prev_;
        prev_ = NULL;
        next_ = NULL;
      }
      return ret;
    }

    bool DLink::add(DLink *prev, DLink *e, DLink *next)
    {
      bool ret = true;
      if (NULL == prev || NULL == e || NULL == next)
      {
        ret = false;
        TBSYS_LOG(ERROR, "prev=%p, e=%p and next=%p must not null", prev, e, next);
      }
      else
      {
        prev->next_ = e;
        e->prev_ = prev;
        next->prev_ = e;
        e->next_ = next;
      }
      return ret;
    }

    void DLink::add_range_after(DLink *first, DLink *last)
    {
      DLink* next = this->next_;
      this->next_ = first;
      first->prev_ = this;
      next->prev_ = last;
      last->next_ = next;
    }

  //------------dlist define--------------
    DList::DList()
    {
      header_.next_ = &header_;
      header_.prev_ = &header_;
      size_ = 0;
    }

    // insert the node to the tail
    bool DList::add_last(DLink *e)
    {
      bool ret = true;
      if(!e)
      {
        ret = false;
      }
      else if (!header_.add_before(e))
      {
        ret = false;
        TBSYS_LOG(ERROR, "failed to add e before head, e=%p", e);
      }
      else
      {
        size_++;
      }
      return ret;
    }

    // insert the node to the head
    bool DList::add_first(DLink *e)
    {
      bool ret = true;
      if(!e)
      {
        ret = false;
      }
      else if (!header_.add_after(e))
      {
        ret = false;
        TBSYS_LOG(ERROR, "failed to add e after head, e=%p", e);
      }
      else
      {
        size_++;
      }
      return ret;
    }

    // move the node to the head
    bool DList::move_to_first(DLink *e)
    {
      bool ret = true;
      if(e == &header_ || e == NULL)
      {
        ret = false;
      }
      else if (!e->unlink())
      {
        ret = false;
        TBSYS_LOG(ERROR, "failed to move to first, e=%p", e);
      }
      else
      {
        size_--;
        ret = add_first(e);
      }
      return ret;
    }

    // move the node to the tail
    bool DList::move_to_last(DLink *e)
    {
      bool ret = true;
      if(e == &header_ || e == NULL)
      {
        ret = false;
      }
      else if (!e->unlink())
      {
        ret = false;
        TBSYS_LOG(ERROR, "failed to move to last, e=%p", e);
      }
      else
      {
        size_--;
        ret = add_last(e);
      }
      return ret;
    }

   // remove the node at tail
    DLink* DList::remove_last()
    {
      return remove(header_.prev_);
    }

    // remove the node at head
    DLink* DList::remove_first()
    {
      return remove(header_.next_);
    }

    DLink* DList::remove(DLink *e)
    {
      DLink* ret = e;
      if (e == &header_ || e == NULL)
      {
        ret = NULL;
      }
      else if (!e->unlink())
      {
        ret = false;
        TBSYS_LOG(ERROR, "failed to remove e=%p", e);
      }
      else
      {
        size_--;
      }
      return ret;
    }

    DLink* DList::get_first()
    {
      DLink* first = header_.next_;
      if (first == &header_)
      {
        first = NULL;
      }
      return first;
    }

    void DList::push_range(DList &range)
    {
      if (!range.is_empty())
      {
        DLink* first = range.header_.next_;
        DLink* last = range.header_.prev_;
        first->prev_ = NULL;
        last->next_ = NULL;
        this->header_.add_range_after(first, last);
        size_ += range.get_size();
        range.clear();
      }
    }

    void DList::pop_range(int32_t num, DList &range)
    {
      DLink *first = this->header_.next_;
      DLink *last = first;
      int count = 0;
      if (count < num && last != &this->header_)
      {
        ++count;
      }
      while (count < num
             && last->next_ != &this->header_)
      {
        ++count;
        last = last->next_;
      }
      if (0 < count)
      {
        if (last->next_ == &this->header_)
        {
          clear();
        }
        else
        {
          header_.next_ = last->next_;
          last->next_->prev_ = &header_;
          size_ -= count;
        }
        first->prev_ = NULL;
        last->next_ = NULL;
        range.header_.add_range_after(first, last);
        range.size_ += count;
      }
    }

    DLink* DList::get_last()
    {
      return header_.prev_;
    }
  }

}
