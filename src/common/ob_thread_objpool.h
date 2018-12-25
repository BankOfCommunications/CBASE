////===================================================================
 //
 // ob_thread_objpool.h common / Oceanbase
 //
 // Copyright (C) 2010, 2013 Taobao.com, Inc.
 //
 // Created on 2011-03-08 by Yubai (yubai.lk@taobao.com)
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
#ifndef  OCEANBASE_COMMON_THREAD_OBJPOOL_H_
#define  OCEANBASE_COMMON_THREAD_OBJPOOL_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <typeinfo>
#include <bitset>
#include "common/ob_define.h"
#include "common/page_arena.h"
#include "common/ob_malloc.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_atomic.h"

namespace oceanbase
{
  namespace common
  {
    template <typename T, typename Allocator>
    class ThreadObjPool;

    template <typename T, int ALLOC_MOD = ObModIds::OB_THREAD_OBJPOOL>
    class SingleObjAllocator
    {
      typedef SingleObjAllocator<T, ALLOC_MOD> Allocator;
      typedef typename ThreadObjPool<T, Allocator>::Node BaseNode;
      public:
        BaseNode *construct()
        {
          BaseNode *ret = (BaseNode*)ob_malloc(sizeof(BaseNode), ALLOC_MOD);
          if (NULL != ret)
          {
            ret = new(ret) BaseNode();
          }
          return ret;
        };
        void destroy(BaseNode *ptr)
        {
          if (NULL != ptr)
          {
            ptr->~BaseNode();
            ob_free(ptr, ALLOC_MOD);
          }
        };
        void reset(BaseNode *ptr)
        {
          if (NULL != ptr)
          {
            ptr->~BaseNode();
            ptr = new(ptr) BaseNode();
          }
        };
    };

    template <typename T, int64_t PageSize, int ALLOC_MOD = ObModIds::OB_THREAD_OBJPOOL>
    class MutilObjAllocator
    {
      typedef MutilObjAllocator<T, PageSize, ALLOC_MOD> Allocator;
      typedef typename ThreadObjPool<T, Allocator>::Node BaseNode;
      class NodeArray;
      struct Node : public BaseNode
      {
        NodeArray *parent;
      };
      static const int64_t NODES_BUFFER_SIZE = PageSize / sizeof(Node) * sizeof(Node);
      struct NodeArray
      {
        void reset()
        {
          buffer_pos_ = 0;
          ref_cnt_ = 0;
        };
        BaseNode *alloc()
        {
          BaseNode *ret = NULL;
          if ((buffer_pos_ + (int64_t)sizeof(Node)) < NODES_BUFFER_SIZE)
          {
            Node *node = (Node*)&(nodes_buffer_[buffer_pos_]);
            node->parent = this;
            ret = node;
            buffer_pos_ += sizeof(Node);
            inc_ref_cnt();
          }
          return ret;
        };
        int64_t inc_ref_cnt()
        {
          return atomic_inc((uint64_t*)&ref_cnt_);
        };
        int64_t dec_ref_cnt()
        {
          return atomic_dec((uint64_t*)&ref_cnt_);
        };
        private:
          int64_t buffer_pos_;
          int64_t ref_cnt_;
          char nodes_buffer_[NODES_BUFFER_SIZE];
      };
      public:
        MutilObjAllocator() : cur_node_array_(NULL)
        {
        };
        ~MutilObjAllocator()
        {
          if (NULL != cur_node_array_
              && 0 == cur_node_array_->dec_ref_cnt())
          {
            ob_free(cur_node_array_, ALLOC_MOD);
            cur_node_array_ = NULL;
          }
        };
      public:
        BaseNode *construct()
        {
          BaseNode *ret = NULL;
          while (true)
          {
            if (NULL == cur_node_array_)
            {
              cur_node_array_ = (NodeArray*)ob_malloc(sizeof(NodeArray), ALLOC_MOD);
              if (NULL != cur_node_array_)
              {
                TBSYS_LOG(DEBUG, "alloc node array");
                cur_node_array_->reset();
                cur_node_array_->inc_ref_cnt();
              }
              else
              {
                break;
              }
            }

            ret = cur_node_array_->alloc();
            if (NULL != ret)
            {
              ret = new(ret) BaseNode();
              break;
            }
            else
            {
              if (0 == cur_node_array_->dec_ref_cnt())
              {
                cur_node_array_->reset();
                cur_node_array_->inc_ref_cnt();
              }
              else
              {
                cur_node_array_ = NULL;
              }
              continue;
            }
          }
          return ret;
        };
        void destroy(BaseNode *ptr)
        {
          if (NULL != ptr)
          {
            ptr->~BaseNode();
            Node *node = (Node*)ptr;
            if (NULL != node->parent
                && 0 == node->parent->dec_ref_cnt())
            {
              ob_free(node->parent, ALLOC_MOD);
              TBSYS_LOG(DEBUG, "free node array");
            }
          }
        };
        void reset(BaseNode *ptr)
        {
          if (NULL != ptr)
          {
            ptr->~BaseNode();
            ptr = new(ptr) BaseNode();
          }
        };
      private:
        NodeArray *cur_node_array_;
    };

    template <typename T, typename Allocator>
    class ThreadObjPool
    {
      public:
        struct Node
        {
          T data;
          Node *prev;
          Node *next;
        };
        class GFreeList
        {
          public:
            GFreeList() : size_(0), head_(NULL)
            {
              pthread_spin_init(&spin_, PTHREAD_PROCESS_PRIVATE);
            };
            ~GFreeList()
            {
              Node *iter = head_;
              while (NULL != iter)
              {
                Node *tmp = iter;
                iter = iter->next;
                allocator_.destroy(tmp);
              }
              size_ = 0;
              head_ = NULL;
              pthread_spin_destroy(&spin_);
            };
          public:
            inline void push(Node *ptr, const int64_t max_free_num)
            {
              if (NULL != ptr)
              {
                if (max_free_num <= size_)
                {
                  allocator_.destroy(ptr);
                  if (max_free_num < size_)
                  {
                    Node *head2free = NULL;

                    pthread_spin_lock(&spin_);
                    Node *iter = head_;
                    while (NULL != iter
                          && max_free_num < size_)
                    {
                      Node *tmp = iter;
                      iter = iter->next;
                      tmp->next = head2free;
                      head2free = tmp;
                      size_--;
                    }
                    head_ = iter;
                    pthread_spin_unlock(&spin_);

                    iter = head2free;
                    while (NULL != iter)
                    {
                      Node *tmp = iter;
                      iter = iter->next;
                      allocator_.destroy(tmp);
                    }
                  }
                }
                else
                {
                  pthread_spin_lock(&spin_);
                  ptr->next = head_;
                  head_ = ptr;
                  size_++;
                  pthread_spin_unlock(&spin_);
                }
              }
            };
            inline Node *pop()
            {
              Node *ptr = NULL;
              if (NULL != head_)
              {
                pthread_spin_lock(&spin_);
                ptr = head_;
                if (NULL != head_)
                {
                  head_ = head_->next;
                  size_--;
                }
                pthread_spin_unlock(&spin_);
              }
              return ptr;
            };
          private:
            Allocator allocator_;
            pthread_spinlock_t spin_;
            int64_t size_;
            Node *head_;
        };
      public:
        ThreadObjPool() : head_(NULL), tail_(NULL), alloc_cnt_(0), free_num_(0)
        {
        };
        ~ThreadObjPool()
        {
          Node *iter = head_;
          while (NULL != iter)
          {
            Node *tmp = iter;
            iter = iter->next;
            allocator_.destroy(tmp);
          }
          head_ = NULL;
          tail_ = NULL;
          alloc_cnt_ = 0;
          free_num_ = 0;
        };
      public:
        inline T *alloc(GFreeList &gfreelist)
        {
          Node *ret = NULL;
          if (NULL != tail_)
          {
            ret = tail_;
            tail_ = tail_->prev;
            if (NULL != tail_)
            {
              tail_->next = NULL;
            }
            else
            {
              head_ = NULL;
            }
            allocator_.reset(ret);
            atomic_dec((uint64_t*)&free_num_);
          }
          if (NULL == ret)
          {
            ret = gfreelist.pop();
            allocator_.reset(ret);
          }
          if (NULL == ret)
          {
            ret = allocator_.construct();
          }
          if (NULL != ret)
          {
            atomic_inc((uint64_t*)&alloc_cnt_);
          }
          return &(ret->data);
        };
        inline void free(T *ptr, GFreeList &gfreelist,
                        const int64_t thread_max_free_num,
                        const int64_t global_max_free_num)
        {
          Node *node = (Node*)ptr;
          if (NULL != node)
          {
            node->next = head_;
            node->prev = NULL;
            if (NULL != head_)
            {
              head_->prev = node;
            }
            head_ = node;
            if (NULL == tail_)
            {
              tail_ = head_;
            }
            atomic_inc((uint64_t*)&free_num_);

            Node *iter = tail_;
            while (free_num_ > thread_max_free_num
                  && NULL != iter)
            {
              Node *tmp = iter;
              iter = iter->prev;
              gfreelist.push(tmp, global_max_free_num);
              atomic_dec((uint64_t*)&free_num_);
            }
            tail_ = iter;
            if (NULL != tail_)
            {
              tail_->next = NULL;
            }
            else
            {
              head_ = NULL;
            }
          }
        };
      private:
        Allocator allocator_;
        Node *head_;
        Node *tail_;
        int64_t alloc_cnt_;
        int64_t free_num_;
    };

    template <typename T, typename Allocator>
    class ThreadAllocator
    {
      static const int64_t DEFAULT_THREAD_MAX_FREE_NUM = 16 * 1024 / sizeof(T) + 1;
      static const int64_t DEFAULT_GLOBAL_MAX_FREE_NUM = INT64_MAX;
      typedef ThreadObjPool<T, Allocator> ObjPool;
      typedef typename ThreadObjPool<T, Allocator>::GFreeList GFreeList;
      public:
        ThreadAllocator() : thread_max_free_num_(DEFAULT_THREAD_MAX_FREE_NUM),
                            global_max_free_num_(DEFAULT_GLOBAL_MAX_FREE_NUM)
        {
        };
        ~ThreadAllocator() {};
      public:
        void set_thread_max_free_num(const int64_t thread_max_free_num)
        {
          thread_max_free_num_ = thread_max_free_num;
        };
        void set_global_max_free_num(const int64_t global_max_free_num)
        {
          global_max_free_num_ = global_max_free_num;
        };
        void inc_ref() {};
        void dec_ref() {};
        T *alloc()
        {
          T *ret = NULL;
          ObjPool *pool = GET_TSI_MULT(ObjPool, TSI_COMMON_OBJPOOL_1);
          if (NULL != pool)
          {
            ret = pool->alloc(gfrelist_);
          }
          return ret;
        };
        void free(T *ptr)
        {
          ObjPool *pool = GET_TSI_MULT(ObjPool, TSI_COMMON_OBJPOOL_1);
          if (NULL != pool)
          {
            pool->free(ptr, gfrelist_, thread_max_free_num_, global_max_free_num_);
          }
        };
      public:
        GFreeList gfrelist_;
        int64_t thread_max_free_num_;
        int64_t global_max_free_num_;
    };
  }
}

#endif //OCEANBASE_COMMON_THREAD_OBJPOOL_H_
