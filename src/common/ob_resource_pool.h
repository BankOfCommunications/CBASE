////===================================================================
 //
 // ob_resource_pool.h updateserver / Oceanbase
 //
 // Copyright (C) 2010, 2013 Taobao.com, Inc.
 //
 // Created on 2012-07-19 by Yubai (yubai.lk@taobao.com)
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
#ifndef  OCEANBASE_UPDATESERVER_RESOURCE_POOL_H_
#define  OCEANBASE_UPDATESERVER_RESOURCE_POOL_H_
#include <pthread.h>
#include <typeinfo>
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/ob_fixed_queue.h"
#include "common/ob_spin_lock.h"
#include "common/page_arena.h"
#include "common/utility.h"
#include "common/ob_tsi_factory.h"

DEFINE_HAS_MEMBER(RP_LOCAL_NUM);
DEFINE_HAS_MEMBER(RP_TOTAL_NUM);

namespace oceanbase
{
  namespace common
  {
    template <class T, int64_t LOCAL_NUM = 4, int64_t TOTAL_NUM = 256>
    class ObResourcePool
    {
      static const int64_t ALLOCATOR_PAGE_SIZE = 65536;
      static const int64_t MAX_THREAD_NUM = 1024;
      static const int64_t WARN_INTERVAL = 60000000L; //60s
      static const uint64_t ALLOC_MAGIC_NUM = 0x72737263706f6f6c; // rsrcpool
      static const uint64_t ALLOC_BY_PAGEARENA = 0x0;
      static const uint64_t ALLOC_BY_OBMALLOC = 0x1;
      struct Node
      {
        T data;
        union
        {
          Node *next;
          uint64_t magic;
        };
        uint64_t flag;
        Node() : data(), next(NULL), flag(ALLOC_BY_PAGEARENA) {};
      };
      struct NodeArray
      {
        Node datas[LOCAL_NUM];
        int64_t index;
        NodeArray() : index(0) {};
      };
      typedef common::ObFixedQueue<Node> NodeQueue;
      typedef common::ObFixedQueue<NodeArray> ArrayQueue;
      public:
        class Guard
        {
          public:
            Guard(ObResourcePool &host) : host_(host),
                                          list_(NULL)
            {
            };
            ~Guard()
            {
              reset();
            };
          public:
            void reset()
            {
              Node *iter = list_;
              while (NULL != iter)
              {
                Node *tmp = iter;
                iter = iter->next;
                host_.free_node_(tmp);
              }
              NodeArray *array = host_.get_node_array_();
              if (NULL != array)
              {
                for (int64_t i = array->index - 1; i >= 0; i--)
                {
                  array->datas[i].data.reset();
                }
                array->index = 0;
              }
              list_ = NULL;
            };
          public:
            void add_node(Node *node)
            {
              if (NULL != node)
              {
                node->next = list_;
                list_ = node;
              }
            };
          private:
            ObResourcePool &host_;
            Node *list_;
        };
      public:
        ObResourcePool() : mod_(common::ObModIds::OB_UPS_RESOURCE_POOL_NODE),
                           allocator_(ALLOCATOR_PAGE_SIZE, mod_),
                           allocated_num_(0)
        {
          int ret = pthread_key_create(&thread_key_, NULL);
          if (0 != ret)
          {
            TBSYS_LOG(ERROR, "pthread_key_create fail ret=%d", ret);
          }
          array_list_.init(MAX_THREAD_NUM);
          free_list_.init(TOTAL_NUM);
          //TBSYS_LOG(INFO, "RP init type=%s local_num=%ld total_num=%ld", typeid(typeof(T)).name(), LOCAL_NUM, TOTAL_NUM);
        };
        ~ObResourcePool()
        {
          Node *node = NULL;
          while (common::OB_SUCCESS == free_list_.pop(node))
          {
            node->~Node();
            //allocator_.free((char*)node);
          }
          NodeArray *array = NULL;
          while (common::OB_SUCCESS == array_list_.pop(array))
          {
            free_array_(array);
          }
          pthread_key_delete(thread_key_);
        };
      public:
        T *get(Guard &guard)
        {
          T *ret = NULL;
          NodeArray *array = get_node_array_();
          if (NULL != array
              && LOCAL_NUM > array->index)
          {
            ret = &(array->datas[array->index].data);
            array->index += 1;
          }
          if (NULL == ret)
          {
            Node *node = alloc_node_();
            if (NULL != node)
            {
              ret = &(node->data);
              guard.add_node(node);
            }
          }
          return ret;
        };
        T *alloc()
        {
          T *ret = NULL;
          Node *node = alloc_node_();
          if (NULL != node)
          {
            ret = &(node->data);
            node->magic = ALLOC_MAGIC_NUM;
          }
          return ret;
        };
        void free(T *ptr)
        {
          if (NULL != ptr)
          {
            Node *node = (Node*)ptr;
            if (ALLOC_MAGIC_NUM != node->magic)
            {
              TBSYS_LOG(ERROR, "node=%p magic=%lx not match %lx", node, node->magic, ALLOC_MAGIC_NUM);
            }
            else
            {
              free_node_(node);
            }
          }
        };
        int64_t get_free_num()
        {
          return free_list_.get_total();
        }
      private:
        Node *alloc_node_()
        {
          Node *ret = NULL;
          if (common::OB_SUCCESS != free_list_.pop(ret)
              || NULL == ret)
          {
            void *buffer = NULL;
            uint64_t flag = ALLOC_BY_PAGEARENA;
            int64_t allocated_num = ATOMIC_ADD(&allocated_num_, 1);
            if (TOTAL_NUM >= allocated_num)
            {
              flag = ALLOC_BY_PAGEARENA;
              common::ObSpinLockGuard guard(allocator_lock_);
              buffer = allocator_.alloc(sizeof(Node));
            }
            else
            {
              flag = ALLOC_BY_OBMALLOC;
              buffer = common::ob_malloc(sizeof(Node), common::ObModIds::OB_UPS_RESOURCE_POOL_NODE);
            }
            if (NULL != buffer)
            {
              ret = new(buffer) Node();
              ret->flag = flag;
            }
            else
            {
              ATOMIC_ADD(&allocated_num_, -1);
            }
          }
          return ret;
        };
        void free_node_(Node *ptr)
        {
          if (NULL != ptr)
          {
            ptr->data.reset();
            ptr->next = NULL;
            if (ALLOC_BY_PAGEARENA == ptr->flag)
            {
              if (common::OB_SUCCESS != free_list_.push(ptr))
              {
                TBSYS_LOG(ERROR, "free node to list fail, size=%ld ptr=%p", free_list_.get_total(), ptr);
              }
            }
            else if (ALLOC_BY_OBMALLOC == ptr->flag)
            {
              ptr->~Node();
              common::ob_free(ptr);
              ATOMIC_ADD(&allocated_num_, -1);
            }
            else
            {
              TBSYS_LOG(ERROR, "invalid flag=%lu", ptr->flag);
            }
            //int64_t cnt = 0;
            //int64_t last_warn_time = 0;
            //while (common::OB_SUCCESS != free_list_.push(ptr))
            //{
            //  Node *node = NULL;
            //  if (common::OB_SUCCESS == free_list_.pop(node)
            //      && NULL != node)
            //  {
            //    node->~Node();
            //    common::ob_free(node);
            //  }
            //  if (TOTAL_NUM < cnt++
            //      && tbsys::CTimeUtil::getTime() > (WARN_INTERVAL + last_warn_time))
            //  {
            //    last_warn_time = tbsys::CTimeUtil::getTime();
            //    TBSYS_LOG(ERROR, "free node to list fail count to large %ld, free_list_size=%ld", cnt, free_list_.get_free());
            //  }
            //}
          }
        };

        NodeArray *alloc_array_()
        {
          NodeArray *ret = NULL;
          void *buffer = common::ob_malloc(sizeof(NodeArray), common::ObModIds::OB_UPS_RESOURCE_POOL_ARRAY);
          if (NULL != buffer)
          {
            ret = new(buffer) NodeArray();
          }
          return ret;
        };

        void free_array_(NodeArray *ptr)
        {
          if (NULL != ptr)
          {
            ptr->~NodeArray();
            common::ob_free(ptr);
          }
        };

        NodeArray *get_node_array_()
        {
          NodeArray *ret = (NodeArray*)pthread_getspecific(thread_key_);
          if (NULL == ret)
          {
            if (NULL != (ret = alloc_array_()))
            {
              if (common::OB_SUCCESS != array_list_.push(ret))
              {
                free_array_(ret);
                ret = NULL;
              }
              else
              {
                pthread_setspecific(thread_key_, ret);
                TBSYS_LOG(INFO, "alloc thread specific node_array=%p", ret);
              }
            }
          }
          return ret;
        };
      private:
        common::ObSpinLock allocator_lock_;
        common::ModulePageAllocator mod_;
        common::ModuleArena allocator_;
        volatile int64_t allocated_num_;
        pthread_key_t thread_key_;
        ArrayQueue array_list_;
        NodeQueue free_list_;
    };

    template <class T, int64_t LOCAL_NUM, int64_t TOTAL_NUM>
    ObResourcePool<T, LOCAL_NUM, TOTAL_NUM> &get_resource_pool()
    {
      static ObResourcePool<T, LOCAL_NUM, TOTAL_NUM> resource_pool;
      return resource_pool;
    }

    template <class T, int64_t LOCAL_NUM, int64_t TOTAL_NUM>
    ObResourcePool<T, LOCAL_NUM, TOTAL_NUM> &get_tc_resource_pool()
    {
      typedef ObResourcePool<T, LOCAL_NUM, TOTAL_NUM> RP;
      RP *resource_pool = GET_TSI_MULT(RP, 1);
      return *resource_pool;
    }

    template <class T>
    struct RP
    {
      template <class Type, bool Cond = true>
      struct GetRPLocalNum
      {
        static const int64_t v = Type::RP_LOCAL_NUM;
      };

      template <class Type>
      struct GetRPLocalNum<Type, false>
      {
        static const int64_t v = 1;
      };

      template <class Type, bool Cond = true>
      struct GetRPTotalNum
      {
        static const int64_t v = Type::RP_TOTAL_NUM;
      };

      template <class Type>
      struct GetRPTotalNum<Type, false>
      {
        static const int64_t v = 1024;
      };

      static const int64_t LOCAL_NUM = GetRPLocalNum<T, HAS_MEMBER(T, const int64_t*, RP_LOCAL_NUM)>::v;
      static const int64_t TOTAL_NUM = GetRPTotalNum<T, HAS_MEMBER(T, const int64_t*, RP_TOTAL_NUM)>::v;
    };

#define rp_alloc(type) get_resource_pool<type, RP<type>::LOCAL_NUM, RP<type>::TOTAL_NUM>().alloc()
#define rp_free(ptr) get_resource_pool<typeof(*ptr), RP<typeof(*ptr)>::LOCAL_NUM, RP<typeof(*ptr)>::TOTAL_NUM>().free(ptr)

#define tc_rp_alloc(type) get_tc_resource_pool<type, RP<type>::LOCAL_NUM, RP<type>::TOTAL_NUM>().alloc()
#define tc_rp_free(ptr) get_tc_resource_pool<typeof(*ptr), RP<typeof(*ptr)>::LOCAL_NUM, RP<typeof(*ptr)>::TOTAL_NUM>().free(ptr)
  }
}

#endif //OCEANBASE_UPDATESERVER_RESOURCE_POOL_H_
