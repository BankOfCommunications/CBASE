////===================================================================
 //
 // ob_list.cpp / hash / common / Oceanbase
 //
 // Copyright (C) 2010, 2013 Taobao.com, Inc.
 //
 // Created on 2011-03-16 by Yubai (yubai.lk@taobao.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_COMMON_LIST_H_
#define  OCEANBASE_COMMON_LIST_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include "hash/ob_hashutils.h"

namespace oceanbase
{
  namespace common
  {
    template <class T, class Allocator>
    class ObList;

    namespace list
    {
      template <class T>
      struct Node
      {
        typedef Node* ptr_t;
        typedef const Node* const_ptr_t;
        ptr_t next;
        ptr_t prev;
        T data;
      };

      template <class List>
      class ConstIterator
      {
        typedef ConstIterator<List> self_t;
        public:
          typedef std::bidirectional_iterator_tag iterator_category;
          typedef typename List::value_type value_type;
          typedef typename List::const_pointer pointer;
          typedef typename List::const_reference reference;
        private:
          typedef typename List::iterator iterator;
          typedef Node<value_type> node_t;
          typedef typename node_t::ptr_t node_ptr_t;
          typedef typename node_t::const_ptr_t node_const_ptr_t;
          friend class ObList<value_type, typename List::allocator_t>;
        public:
          ConstIterator() : node_(NULL)
          {
          };
          ConstIterator(const self_t &other)
          {
            *this = other;
          };
          self_t &operator =(const self_t &other)
          {
            node_ = other.node_;
            return *this;
          };
          ConstIterator(const iterator &other)
          {
            *this = other;
          };
          self_t &operator =(const iterator &other)
          {
            node_ = other.node_;
            return *this;
          };
          ConstIterator(node_ptr_t node)
          {
            node_ = node;
          };
          ConstIterator(node_const_ptr_t node)
          {
            node_ = const_cast<node_ptr_t>(node);
          };
        public:
          reference operator *() const
          {
            // NOTICE 对end的访问undefined
            return node_->data;
          };
          pointer operator ->() const
          {
            // NOTICE 对end的访问undefined
            return &(node_->data);
          };
          bool operator ==(const self_t &other) const
          {
            return (node_ == other.node_);
          };
          bool operator !=(const self_t &other) const
          {
            return (node_ != other.node_);
          };
          self_t &operator ++()
          {
            // NOTICE 循环链表 不判断end会返回到head
            node_ = node_->next;
            return *this;
          };
          self_t operator ++(int)
          {
            self_t tmp = *this;
            node_ = node_->next;
            return tmp;
          };
          self_t &operator --()
          {
            node_ = node_->prev;
            return *this;
          };
          self_t operator --(int)
          {
            self_t tmp = *this;
            node_ = node_->prev;
            return tmp;
          };
        private:
          node_ptr_t node_;
      };

      template <class List>
      class Iterator
      {
        typedef Iterator<List> self_t;
        public:
          typedef std::bidirectional_iterator_tag iterator_category;
          typedef typename List::value_type value_type;
          typedef typename List::pointer pointer;
          typedef typename List::reference reference;
        private:
          typedef typename List::const_iterator const_iterator;
          typedef Node<value_type> node_t;
          typedef typename node_t::ptr_t node_ptr_t;
          typedef typename node_t::const_ptr_t node_const_ptr_t;
          friend class ConstIterator<List>;
          friend class ObList<value_type, typename List::allocator_t>;
        public:
          Iterator() : node_(NULL)
          {
          };
          Iterator(const self_t &other)
          {
            *this = other;
          };
          self_t &operator =(const self_t &other)
          {
            node_ = other.node_;
            return *this;
          };
          Iterator(node_ptr_t node)
          {
            node_ = node;
          };
          Iterator(node_const_ptr_t node)
          {
            node_ = const_cast<node_ptr_t>(node);
          };
        public:
          reference operator *() const
          {
            // NOTICE 对end的访问undefined
            return node_->data;
          };
          pointer operator ->() const
          {
            // NOTICE 对end的访问undefined
            return &(node_->data);
          };
          bool operator ==(const self_t &other) const
          {
            return (node_ == other.node_);
          };
          bool operator !=(const self_t &other) const
          {
            return (node_ != other.node_);
          };
          self_t &operator ++()
          {
            // NOTICE 循环链表 不判断end会返回到head
            node_ = node_->next;
            return *this;
          };
          self_t operator ++(int)
          {
            self_t tmp = *this;
            node_ = node_->next;
            return tmp;
          };
          self_t &operator --()
          {
            node_ = node_->prev;
            return *this;
          };
          self_t operator --(int)
          {
            self_t tmp = *this;
            node_ = node_->prev;
            return tmp;
          };
        private:
          node_ptr_t node_;
      };
    }

    template <class T>
    struct ListTypes
    {
      typedef list::Node<T> AllocType;
    };

    template <class T, class Allocator = hash::SimpleAllocer<typename ListTypes<T>::AllocType> >
    class ObList
    {
      typedef ObList<T, Allocator> self_t;
      public:
        typedef T value_type;
        typedef value_type* pointer;
        typedef value_type& reference;
        typedef const value_type* const_pointer;
        typedef const value_type& const_reference;
        typedef list::Iterator<self_t> iterator;
        typedef list::ConstIterator<self_t> const_iterator;
        typedef Allocator allocator_t;

      private:
        typedef list::Node<value_type> node_t;
        typedef typename node_t::ptr_t node_ptr_t;
        typedef typename node_t::const_ptr_t node_const_ptr_t;

        typedef struct NodeHolder
        {
          node_ptr_t next;
          node_ptr_t prev;
          operator node_ptr_t()
          {
            return reinterpret_cast<node_ptr_t>(this);
          };
          operator node_const_ptr_t() const
          {
            return reinterpret_cast<node_const_ptr_t>(this);
          };
        } node_holder_t;

      private:
        ObList(const self_t &other);
        self_t &operator =(const self_t &other);

      public:
        ObList() : size_(0)
        {
          root_.next = root_;
          root_.prev = root_;
        };
        ~ObList()
        {
          clear();
        };

      public:
        int push_back(const value_type &value)
        {
          int ret = 0;
          node_ptr_t tmp = allocator_.alloc();
          if (NULL == tmp)
          {
            ret = -1;
          }
          else
          {
            // NOTICE 暂时使用赋值操作 以后可以修改为使用construct函数
            tmp->data = value;
            tmp->prev = root_.prev;
            tmp->next = root_;
            root_.prev->next = tmp;
            root_.prev = tmp;
            size_++;
          }
          return ret;
        };
        int push_front(const value_type &value)
        {
          int ret = 0;
          node_ptr_t tmp = allocator_.alloc();
          if (NULL == tmp)
          {
            ret = -1;
          }
          else
          {
            // NOTICE 暂时使用赋值操作 以后可以修改为使用construct函数
            tmp->data = value;
            tmp->prev = root_;
            tmp->next = root_.next;
            root_.next->prev = tmp;
            root_.next = tmp;
            size_++;
          }
          return ret;
        };
        int pop_back()
        {
          int ret = 0;
          if (0 >= size_)
          {
            ret = -1;
          }
          else
          {
            node_ptr_t tmp = root_.prev;
            root_.prev = tmp->prev;
            tmp->prev->next = root_;
            allocator_.deallocate(tmp);
            size_--;
          }
          return ret;
        };

        int pop_front(value_type & value)
        {
          int ret = 0;
          if (0 >= size_)
          {
            ret = -1;
          }
          else
          {
            node_ptr_t tmp = root_.next;
            root_.next = tmp->next;
            tmp->next->prev = root_;
            value = tmp->data;
            allocator_.free(tmp);
            size_--;
          }
          return ret;
        }

        int pop_front()
        {
          int ret = 0;
          if (0 >= size_)
          {
            ret = -1;
          }
          else
          {
            node_ptr_t tmp = root_.next;
            root_.next = tmp->next;
            tmp->next->prev = root_;
            allocator_.free(tmp);
            size_--;
          }
          return ret;
        };
        int insert(iterator iter, const value_type &value)
        {
          int ret = 0;
          node_ptr_t tmp = allocator_.alloc();
          if (NULL == tmp)
          {
            ret = -1;
          }
          else
          {
            tmp->data = value;
            tmp->next = iter.node_;
            tmp->prev = iter.node_->prev;
            iter.node_->prev->next = tmp;
            iter.node_->prev = tmp;
            size_++;
          }
          return ret;
        };
        int erase(iterator iter)
        {
          int ret = 0;
          if (0 >= size_
              || NULL == iter.node_
              || iter.node_ == (node_ptr_t)&root_)
          {
            ret = -1;
          }
          else
          {
            node_ptr_t tmp = iter.node_;
            tmp->next->prev = iter.node_->prev;
            tmp->prev->next = iter.node_->next;
            allocator_.free(tmp);
            size_--;
          }
          return ret;
        };

        int erase(const value_type &value)
        {
          int ret = -1;
          iterator it = begin();
          for (; it != end(); ++it)
          {
            if (it.node_->data == value)
            {
              ret = erase(it);
              break;
            }
          }
          return ret;
        }

        iterator begin()
        {
          return iterator(root_.next);
        };
        const_iterator begin() const
        {
          return const_iterator(root_.next);
        };
        iterator end()
        {
          return iterator(root_);
        };
        const_iterator end() const
        {
          return const_iterator(root_);
        };
        void clear()
        {
          node_ptr_t iter = root_.next;
          while (iter != root_)
          {
            node_ptr_t tmp = iter->next;
            allocator_.free(iter);
            iter = tmp;
          }
          root_.next = root_;
          root_.prev = root_;
          size_ = 0;
        };
        void destroy()
        {
          clear();
          allocator_.clear();
        };
        bool empty() const
        {
          return (0 == size_);
        };
        int64_t size() const
        {
          return size_;
        };
        void reset()
        {
          clear();
        };

      private:
        node_holder_t root_;
        int64_t size_;
        allocator_t allocator_;
    };
  }
}

#endif //OCEANBASE_COMMON_LIST_H_
