////===================================================================
 //
 // ob_trans_mgr.h updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2012-03-23 by Yubai (yubai.lk@taobao.com) 
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

#ifndef  OCEANBASE_UPDATESERVER_TRANS_MGR_H_
#define  OCEANBASE_UPDATESERVER_TRANS_MGR_H_
#include "common/ob_define.h"
#include "common/page_arena.h"
#include "common/ob_fixed_queue.h"
#include "common/ob_list.h"
#include "common/ob_id_map.h"
#include "ob_table_engine.h"

namespace oceanbase
{
  namespace updateserver
  {
    class ITableEngine
    {
      public:
        ITableEngine() {};
        virtual ~ITableEngine() {};
      public:
        virtual int rollback(void *data) = 0;
        virtual int commit(void *data) = 0;
    };

    class ITransNode
    {
      public:
        ITransNode() {};
        virtual ~ITransNode() {};
      public:
        virtual int64_t get_trans_id() const = 0;
    };

    class TransMgr;
    class TransNode : public ITransNode
    {
      friend class TransMgr;
      struct RollbackInfo
      {
        ITableEngine *rollbacker;
        void *data;
      };
      struct CommitInfo
      {
        ITableEngine *commiter;
        void *data;
      };
      typedef common::ObList<RollbackInfo> RollbackList;
      typedef common::ObList<CommitInfo> CommitList;
      public:
        explicit TransNode(const TransMgr &trans_mgr);
        virtual ~TransNode();
      public:
        // 开始和结束单个mutator事务的接口
        int start_mutation();
        int end_mutation(const bool rollback);
      public:
        // MemTable读写需要调用的接口
        // 获取当前事务id
        int64_t get_trans_id() const;
        // 注册回滚操作
        int add_rollback_data(ITableEngine *rollbacker, void *data);
        // 注册事务提交操作
        int add_commit_data(ITableEngine *commiter, void *data);
        // 获取当前未结束事务中最早的事务id
        int64_t get_min_flying_trans_id() const;
        // 分配事务局部内存 事务结束后自动释放
        void *stack_alloc(const int64_t size);
      private:
        // TransMgr需要调用的接口
        void reset();
        int rollback();
        int commit();
        // 获取批处理开始时第一个事务id
        int64_t get_start_trans_id() const;
        void set_trans_id(const int64_t trans_id);
        void set_trans_id_const(const bool is_const);
        void set_trans_type(const TETransType trans_type);
        TETransType get_trans_type() const;
        bool mutation_started() const;
      private:
        const TransMgr &trans_mgr_;
        int64_t start_trans_id_;
        int64_t cur_trans_id_;
        bool trans_id_const_;
        TETransType trans_type_;

        bool mutation_started_;
        int64_t rollback_counter_;
        RollbackList rollback_list_;
        int64_t commit_counter_;
        CommitList commit_list_;

        common::CharArena allocator_;
    };

    class TransMgr
    {
      friend class TransNode;
      static const int64_t MAX_TRANS_NODE_NUM = 1024;
      typedef common::ObFixedQueue<TransNode> TransNodeList;
      typedef common::ObIDMap<TransNode> TransNodeMap;
      class MapTraverseCallback
      {
        public:
          MapTraverseCallback(const TransNodeMap &trans_node_map) : trans_node_map_(trans_node_map),
                                                                    min_trans_id_(INT64_MAX)
                                  
          {
          };
          ~MapTraverseCallback()
          {
          };
        public:
          int64_t get_min_trans_id()
          {
            return min_trans_id_;
          };
          void operator()(const uint64_t trans_descriptor)
          {
            TransNode *trans_node = NULL;
            trans_node_map_.get(trans_descriptor, trans_node);
            if (NULL != trans_node
                && min_trans_id_ > trans_node->get_trans_id())
            {
              min_trans_id_ = trans_node->get_trans_id();
            }
          };
        private:
          const TransNodeMap &trans_node_map_;
          int64_t min_trans_id_;
      };
      public:
        static const int64_t CALC_INTERVAL = 10000;
      public:
        TransMgr();
        ~TransMgr();
      public:
        int init(const int64_t max_trans_num);
        void destroy();
      public:
        // 获取一个transaction descriptor
        int start_transaction(const TETransType trans_type, uint64_t &trans_descriptor, const int64_t trans_id);
        int end_transaction(const uint64_t trans_descriptor, const bool rollback);
        TransNode *get_trans_node(const uint64_t trans_descriptor);
      private:
        int64_t get_min_flying_trans_id() const;
      private:
        static int64_t generate_trans_id_(const int64_t commited_trans_id, const int64_t trans_id);
      private:
        bool inited_;

        volatile int64_t flying_trans_counter_;
        volatile int64_t commited_trans_id_;
        volatile int64_t writing_trans_id_;

        mutable int64_t min_flying_trans_id_;
        mutable int64_t calc_timestamp_;

        TransNodeMap trans_node_map_;
        TransNodeList trans_node_list_;
        common::CharArena allocator_;
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_TRANS_MGR_H_

