////===================================================================
 //
 // ob_tsi_factory.h common / Oceanbase
 //
 // Copyright (C) 2010, 2012 Taobao.com, Inc.
 //
 // Created on 2011-01-14 by Yubai (yubai.lk@taobao.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 // 线程私有单例管理
 // 调用GET_TSI会返回线程内部单例
 // 线程退出时自动析构
 // 除宏外 不要调用其他接口
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================
#ifndef  OCEANBASE_COMMON_TSI_FACTORY_H_
#define  OCEANBASE_COMMON_TSI_FACTORY_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <typeinfo>
#include "common/ob_define.h"
#include "common/page_arena.h"
#include "common/ob_malloc.h"
#include "common/ob_mod_define.h"

namespace oceanbase
{
  namespace common
  {
    enum TSICommonType
    {
      TSI_COMMON_OBJPOOL_1 = 1001,
      TSI_COMMON_SCAN_PARAM_1,
      TSI_COMMON_SCANNER_1,
      TSI_COMMON_MUTATOR_1,
      TSI_COMMON_THE_META_1,
      TSI_COMMON_GET_PARAM_1,
      TSI_COMMON_MULTI_WAKEUP_1,
      TSI_COMMON_PACKET_TRACE_ID_1,
      TSI_COMMON_PACKET_SOURCE_CHID_1,
      TSI_COMMON_PACKET_CHID_1,
      TSI_COMMON_SEQ_ID_1,
      TSI_COMMON_OBSERVER_1,
    };

    enum TSISSTableType
    {
      TSI_SSTABLE_FILE_BUFFER_1 = 2001,
      TSI_SSTABLE_THREAD_AIO_BUFFER_MGR_ARRAY_1,
      TSI_SSTABLE_MODULE_ARENA_1,
    };

    enum TSICompactSSTableType
    {
      TSI_COMPACTSSTABLEV2_BLOCK_INDEX_1 = 9001,
      TSI_COMPACTSSTABLEV2_FILE_BUFFER_1 = 9002,
      TSI_COMPACTSSTABLEV2_FILE_BUFFER_2 = 9003,
      TSI_COMPACTSSTABLEV2_FILE_BUFFER_3 = 9004,
      TSI_COMPACTSSTABLEV2_THREAD_AIO_BUFFER_MGR_ARRAY_1 = 9005,
      TSI_COMPACTSSTABLEV2_MODULE_ARENA_1 = 9006,
      TSI_COMPACTSSTABLEV2_MODULE_ARENA_2 = 9007,
    };

    enum TSIChunkserverType
    {
      TSI_CS_SCANNER_1 = 3001,
      TSI_CS_NEW_SCANNER_1,
      TSI_CS_GET_PARAM_1,
      TSI_CS_SCAN_PARAM_1,
      TSI_CS_SQL_SCAN_PARAM_1,
      TSI_CS_SQL_GET_PARAM_1,
      TSI_CS_TABLET_REPORT_INFO_LIST_1,
      TSI_CS_TABLET_REPORT_INFO_LIST_2,
      TSI_CS_SSTABLE_GETTER_1,
      TSI_CS_GET_THREAD_CONTEXT_1,
      TSI_CS_SSTABLE_SCANNER_1,
      TSI_CS_SCHEMA_DECODER_ASSIS_1,
      TSI_CS_THEEAD_META_WRITER_1,
      TSI_CS_COMPACTSSTABLE_ITERATOR_1,
      TSI_CS_COMPACTSSTABLE_GET_SCANEER_1,
      TSI_CS_COLUMNFILTER_1,
      TSI_CS_QUERY_SERVICE_1,
      TSI_CS_TABLET_SERVICE_1,
      TSI_CS_STATIC_DATA_SERVICE_1,
      TSI_CS_MULTI_TABLET_MERGER_1,
      TSI_CS_TABLE_IMPORT_INFO_1,
      TSI_CS_FETCH_DATA_1,
      TSI_CS_FETCH_DATA_2,
    };

    enum TSIUpdateserverType
    {
      TSI_UPS_SCANNER_1 = 4001,
      TSI_UPS_NEW_SCANNER_1,
      TSI_UPS_GET_PARAM_1,
      TSI_UPS_SCAN_PARAM_1,
      TSI_UPS_MUTATOR_1,
      TSI_UPS_SCANNER_ARRAY_1,
      TSI_UPS_UPS_MUTATOR_1,
      TSI_UPS_TABLE_UTILS_SET_1,
      TSI_UPS_COLUMN_FILTER_1,
      TSI_UPS_COLUMN_MAP_1,
      TSI_UPS_TABLE_LIST_1,
      TSI_UPS_ROW_COMPACTION_1,
      TSI_UPS_ROW_COMPACTION_2,
      TSI_UPS_CLIENT_WRAPPER_TSI_1,
      TSI_UPS_FIXED_SIZE_BUFFER_1,
      TSI_UPS_FIXED_SIZE_BUFFER_2,
      TSI_UPS_SCAN_PARAM_2,
      TSI_UPS_SQL_SCAN_PARAM_1,
      TSI_UPS_SQL_MULTI_SCAN_MERGE_1,
    };

    enum TSIMergeserverType
    {
      TSI_MS_SCANNER_1 = 5001,
      TSI_MS_ORG_GET_PARAM_1,
      TSI_MS_DECODED_GET_PARAM_1,
      TSI_MS_GET_PARAM_WITH_NAME_1,
      TSI_MS_ORG_SCAN_PARAM_1,
      TSI_MS_DECODED_SCAN_PARAM_1,
      TSI_MS_SCHEMA_DECODER_ASSIS_1,
      TSI_MS_GET_EVENT_1,
      TSI_MS_SCAN_EVENT_1,
      TSI_MS_MS_SCAN_PARAM_1,
      TSI_MS_ORG_MUTATOR_1,
      TSI_MS_DECODED_MUTATOR_1,
      TSI_MS_UPS_SCANNER_1,
      TSI_MS_NEW_SCANNER_1,
      TSI_MS_SQL_SCAN_PARAM_1,
    };

    enum TSIOlapDrive
    {
      TSI_OLAP_SCAN_EXTRA_INFO_1 = 6001,
      TSI_OLAP_THREAD_ROW_KEY_1,
      TSI_OLAP_GET_PARAM_1,
      TSI_OLAP_SCAN_PARAM_1,
      TSI_OLAP_SCANNER_1,
      TSI_OLAP_MUTATOR_1,
    };

    //enum TSIRootserverType
    //{
    //  TSI_RS_MS_PROVIDER_1 = 6001,
    //};


    enum TSISqlType
    {
      TSI_SQL_GET_PARAM_1 = 7001,
      TSI_SQL_GET_PARAM_2 = 7002,
      TSI_SQL_EXPR_STACK_1 = 7003,
      TSI_SQL_EXPR_EXTRA_PARAMS_1 = 7005,
      TSI_SQL_TP_ARENA_1 = 7006,
      TSI_SQL_ROW_1 = 7007,
    };

    enum TSIMySQLType
    {
      TSI_MYSQL_CLIENT_WAIT_1 = 8001,
      TSI_MYSQL_RESULT_SET_1,
      TSI_MYSQL_PREPARE_RESULT_1,
      TSI_MYSQL_SESSION_KEY_1,
    };

    enum TSIRootserverType
    {
      TSI_RS_SCANNER_1 = 9001,
      TSI_RS_GET_PARAM_1,
      TSI_RS_MS_PROVIDER_1,
      TSI_RS_NEW_SCANNER_1,
      TSI_RS_SQL_SCAN_PARAM_1,
      TSI_RS_SQL_GET_PARAM_1,
    };

    enum TSIProxyserverType
    {
      TSI_YUNTI_PROXY_READER_1 = 10001,
      TSI_YUNTI_PROXY_READER_2,
    };

    enum TSILiboblogType
    {
      TSI_LIBOBLOG_PARTITION = 11001,
      TSI_LIBOBLOG_DML_STMT,
      TSI_LIBOBLOG_MYSQL_ADAPTOR,
      TSI_LIBOBLOG_META_MANAGER,
      TSI_LIBOBLOG_ROW_VALUE,
      TSI_LIBOBLOG_MYSQL_QUERY_RESULT,
    };

    //#define GET_TSI(type) get_tsi_fatcory().get_instance<type,1>()
    #define GET_TSI_MULT(type, tag) get_tsi_fatcory().get_instance<type,tag>()

    template <class T>
    class Wrapper
    {
      public:
        Wrapper() : instance_(NULL)
        {
        };
        ~Wrapper()
        {
          if (NULL != instance_)
          {
            delete instance_;
            instance_ = NULL;
          }
        };
      public:
        T *&get_instance()
        {
          return instance_;
        };
      private:
        T *instance_;
    };

    #define GET_TSI_ARGS(type, num, args...) \
    ({ \
      type *__type_ret__ = NULL; \
      Wrapper<type> *__type_wrapper__ = GET_TSI_MULT(Wrapper<type>, num); \
      if (NULL != __type_wrapper__) \
      { \
        __type_ret__ = __type_wrapper__->get_instance(); \
        if (NULL == __type_ret__) \
        { \
          __type_wrapper__->get_instance() = new(std::nothrow) type(args); \
          __type_ret__ = __type_wrapper__->get_instance(); \
        } \
      } \
      __type_ret__; \
    })
    //  GET_TSI(Wrapper<type>) ? (GET_TSI(Wrapper<type>)->get_instance() ? (GET_TSI(Wrapper<type>)->get_instance()) :
    //      (GET_TSI(Wrapper<type>)->get_instance() = new(std::nothrow) type(args))) : NULL

    class TSINodeBase
    {
      public:
        TSINodeBase() : next(NULL)
        {
        };
        virtual ~TSINodeBase()
        {
          next = NULL;
        };
        TSINodeBase *next;
    };

    template <class T>
    class TSINode : public TSINodeBase
    {
      public:
        explicit TSINode(T *instance) : instance_(instance)
        {
        };
        virtual ~TSINode()
        {
          if (NULL != instance_)
          {
            TBSYS_LOG(INFO, "delete instance [%s] %p", typeid(T).name(), instance_);
            instance_->~T();
            instance_ = NULL;
          }
        };
      private:
        T *instance_;
    };

    class ThreadSpecInfo
    {
      static const int64_t PAGE_SIZE = 2 * 1024 * 1024;
      public:
        ThreadSpecInfo() : list_(NULL),
                           mod_(ObModIds::OB_TSI_FACTORY),
                           alloc_(PAGE_SIZE, mod_)
        {
        };
        ~ThreadSpecInfo()
        {
          TSINodeBase *iter = list_;
          while (NULL != iter)
          {
            TSINodeBase *tmp = iter;
            iter = iter->next;
            tmp->~TSINodeBase();
          }
          list_ = NULL;
        };
      public:
        template <class T>
        T *get_instance()
        {
          T *instance = NULL;
          TSINode<T> *node = NULL;
          void *instance_buffer = alloc_.alloc(sizeof(T));
          void *node_buffer = alloc_.alloc(sizeof(TSINode<T>));
          if (NULL == (instance = new(instance_buffer) T()))
          {
            TBSYS_LOG(WARN, "new instance [%s] fail", typeid(T).name());
          }
          else
          {
            node = new(node_buffer) TSINode<T>(instance);
            if (NULL == node)
            {
              TBSYS_LOG(WARN, "new tsi_node fail [%s]", typeid(T).name());
              instance->~T();
              instance = NULL;
            }
            else
            {
              TBSYS_LOG(DEBUG, "new instance succ [%s] %p size=%ld, tsi=%p", typeid(T).name(), instance, sizeof(T), this);
              if (NULL == list_)
              {
                list_ = node;
                list_->next = NULL;
              }
              else
              {
                node->next = list_;
                list_ = node;
              }
            }
          }
          return instance;
        };
      private:
        TSINodeBase *list_;
        common::ModulePageAllocator mod_;
        common::ModuleArena alloc_;
    };

    class TSIFactory
    {
      static const pthread_key_t INVALID_THREAD_KEY = INT32_MAX;
      public:
        TSIFactory() : key_(INVALID_THREAD_KEY)
        {
        };
        ~TSIFactory()
        {
        };
      public:
        int init()
        {
          int ret = common::OB_SUCCESS;
          if (0 != pthread_key_create(&key_, destroy_thread_data_))
          {
            TBSYS_LOG(WARN, "pthread_key_create fail errno=%u", errno);
            ret = common::OB_ERROR;
          }
          return ret;
        };
        int destroy()
        {
          int ret = common::OB_SUCCESS;
          if (INVALID_THREAD_KEY != key_)
          {
            void *ptr = pthread_getspecific(key_);
            destroy_thread_data_(ptr);
            pthread_key_delete(key_);
            key_ = INVALID_THREAD_KEY;
          }
          return ret;
        };
        template <class T, size_t num>
        T *get_instance()
        {
          static __thread T *instance = NULL;
          if (NULL == instance && INVALID_THREAD_KEY != key_)
          {
            ThreadSpecInfo *tsi = (ThreadSpecInfo*)pthread_getspecific(key_);
            if (NULL == tsi)
            {
              tsi = new(std::nothrow) ThreadSpecInfo();
              if (NULL != tsi)
              {
                TBSYS_LOG(INFO, "new tsi succ %p key=%d", tsi, key_);
              }
              if (NULL != tsi
                  && 0 != pthread_setspecific(key_, tsi))
              {
                TBSYS_LOG(WARN, "pthread_setspecific fail errno=%u key=%d", errno, key_);
                delete tsi;
                tsi = NULL;
              }
            }
            if (NULL != tsi)
            {
              instance = tsi->get_instance<T>();
            }
          }
          return instance;
        };
      private:
        static void destroy_thread_data_(void *ptr)
        {
          if (NULL != ptr)
          {
            ThreadSpecInfo *tsi = (ThreadSpecInfo*)ptr;
            delete tsi;
          }
        };
      private:
        pthread_key_t key_;
    };

    extern TSIFactory &get_tsi_fatcory();
    extern void tsi_factory_init();
    extern void tsi_factory_destroy();
  }
}

#endif //OCEANBASE_COMMON_TSI_FACTORY_H_
