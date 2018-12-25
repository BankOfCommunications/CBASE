#ifndef OCEANBASE_COMMON_DOUBLE_LIST_H_
#define OCEANBASE_COMMON_DOUBLE_LIST_H_
#include "ob_define.h"
namespace oceanbase
{
  namespace common
  {

    class DList;

    /** node in list ,it has no value, if you want to use it, you
     *  should inherit it, and then set the value, and you should manage
     *  the memeory by self.
     */
    class DLink
    {
    public:
      // constructor,have no value
      DLink();

      // get the next node
      inline DLink* get_next() const
      {
        return next_;
      }

      // get the prev node
      inline DLink* get_prev() const
      {
        return prev_;
      }

      virtual ~DLink()
      {
      }

      void reset() {prev_ = NULL; next_ = NULL;};
    protected:
      // insert one node before this node
      bool add_before(DLink *e);

      // insert one node after this node
      bool add_after(DLink *e);

      // remove node from list
      bool unlink();

      bool add(DLink *prev, DLink *e, DLink *next);

      void add_range_after(DLink *first, DLink *last);

    protected:
      //DList *list_;
      DLink *prev_;// this will not delete pointer
      DLink *next_;// this will not delete pointer

      friend class DList;
    };

    //double list
    class DList
    {
    public:
      DList();

      // get the header
      DLink* get_header(){return &header_;};
      const DLink* get_header() const {return &header_;};

      //get the first node
      DLink* get_first();
      //get the last node
      DLink* get_last();

      DLink* get_real_first() {return header_.next_;};
      const DLink* get_real_first() const {return header_.next_;};

      // insert the node to the tail
      bool add_last(DLink *e);

      // insert the node to the head
      bool add_first(DLink *e);

      // move the node to the head
      bool move_to_first(DLink *e);

      // move the node to the tail
      bool move_to_last(DLink *e);

      // remove the node at tail
      DLink* remove_last();

      // remove the node at head
      DLink* remove_first();

      void push_range(common::DList &range);
      void pop_range(int32_t num, common::DList &range);

      //the list is empty or not
      inline bool is_empty() const
      {
        return header_.next_ == &header_;
      }

      // get the list size
      inline int get_size() const
      {
        return size_;
      }

      DLink* remove(DLink *e);

      void clear() { header_.next_ = &header_; header_.prev_ = &header_; size_ = 0;};
    private:
      DISALLOW_COPY_AND_ASSIGN(DList);
      DLink header_;
      int  size_;
    };
  }
}

#define dlist_for_each_safe(NodeType, curr, next, dlist)  \
  for (DLink* curr = dlist.get_real_first(),              \
         *next = curr->get_next();              \
       curr != dlist.get_header();              \
       curr = next, next=next->get_next())

#define dlist_for_each(NodeType, curr, dlist) \
      for (NodeType* curr = dynamic_cast<NodeType*>(dlist.get_real_first());\
           curr != dynamic_cast<NodeType*>(dlist.get_header());         \
           curr = dynamic_cast<NodeType*>(curr->get_next()))

#define dlist_for_each_const(NodeType, curr, dlist) \
  for (const NodeType* curr = dynamic_cast<const NodeType*>(dlist.get_real_first());\
       curr != dynamic_cast<const NodeType*>(dlist.get_header());             \
       curr = dynamic_cast<const NodeType*>(curr->get_next()))

#define dlist_for_each_del(p, dlist)\
  for (DLink * p = dlist.remove_first();            \
       NULL != p && p != dlist.get_header();                     \
       p = dlist.remove_first())

#endif
