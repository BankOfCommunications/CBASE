////===================================================================
 //
 // ob_trans_mgr.cpp updateserver / Oceanbase
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

#include <algorithm>
#include "ob_trans_mgr.h"

namespace oceanbase
{
  using namespace common;
  namespace updateserver
  {
    TransNode::TransNode(const TransMgr &trans_mgr) : trans_mgr_(trans_mgr),
                                                      start_trans_id_(-1),
                                                      cur_trans_id_(-1),
                                                      trans_id_const_(false),
                                                      trans_type_(INVALID_TRANSACTION),
                                                      mutation_started_(false),
                                                      rollback_counter_(0),
                                                      rollback_list_(),
                                                      commit_counter_(0),
                                                      commit_list_(),
                                                      allocator_()
    {
    }

    TransNode::~TransNode()
    {
    }

    int TransNode::start_mutation()
    {
      int ret = OB_SUCCESS;
      if (mutation_started_)
      {
        TBSYS_LOG(WARN, "mutation is running, cannot start, start_trans_id=%ld cur_trans_id=%ld "
                  "rollback_list=%ld commit_list=%ld",
                  start_trans_id_, cur_trans_id_, rollback_list_.size(), commit_list_.size());
        ret = OB_UPS_TRANS_RUNNING;
      }
      else
      {
        mutation_started_ = true;
        rollback_counter_ = 0;
        commit_counter_ = 0;
        if (!trans_id_const_)
        {
          cur_trans_id_++;
        }
      }
      return ret;
    }
    
    int TransNode::end_mutation(const bool rollback)
    {
      int ret = OB_SUCCESS;
      if (!mutation_started_)
      {
        TBSYS_LOG(WARN, "mutation not start, start_trans_id=%ld cur_trans_id=%ld "
                  "rollback_list=%ld commit_list=%ld",
                  start_trans_id_, cur_trans_id_, rollback_list_.size(), commit_list_.size());
        ret = OB_NOT_INIT;
      }
      else
      {
        if (rollback)
        {
          RollbackList::iterator iter;
          while (0 < rollback_counter_--)
          {
            iter = rollback_list_.begin();
            if (iter != rollback_list_.end())
            {
              if (NULL != iter->rollbacker)
              {
                int tmp_ret = iter->rollbacker->rollback(iter->data);
                if (OB_SUCCESS != tmp_ret)
                {
                  TBSYS_LOG(WARN, "rollback fail ret=%d", tmp_ret);
                  ret = tmp_ret;
                }
              }
              rollback_list_.pop_front();
            }
          }
        }
        if (rollback)
        {
          CommitList::iterator iter;
          while (0 < commit_counter_--)
          {
            iter = commit_list_.begin();
            if (iter != commit_list_.end())
            {
              commit_list_.pop_front();
            }
          }
        }
        mutation_started_ = false;
        rollback_counter_ = 0;
        commit_counter_ = 0;
      }
      return ret;
    }

    int64_t TransNode::get_trans_id() const
    {
      return cur_trans_id_;
    }

    int64_t TransNode::get_start_trans_id() const
    {
      return start_trans_id_;
    }

    int TransNode::add_rollback_data(ITableEngine *rollbacker, void *data)
    {
      int ret = OB_SUCCESS;
      if (!mutation_started_)
      {
        TBSYS_LOG(WARN, "mutation not start, start_trans_id=%ld cur_trans_id=%ld "
                  "rollback_list=%ld commit_list=%ld",
                  start_trans_id_, cur_trans_id_, rollback_list_.size(), commit_list_.size());
        ret = OB_NOT_INIT;
      }
      else
      {
        RollbackInfo rollback_info;
        rollback_info.rollbacker = rollbacker;
        rollback_info.data = data;
        if (0 != rollback_list_.push_front(rollback_info))
        {
          TBSYS_LOG(WARN, "push rollback info to list rollback list fail");
          ret = OB_ERROR;
        }
        else
        {
          rollback_counter_++;
        }
      }
      return ret;
    }

    int TransNode::add_commit_data(ITableEngine *commiter, void *data)
    {
      int ret = OB_SUCCESS;
      if (!mutation_started_)
      {
        TBSYS_LOG(WARN, "mutation not start, start_trans_id=%ld cur_trans_id=%ld rollback_list=%ld commit_list=%ld",
                  start_trans_id_, cur_trans_id_, rollback_list_.size(), commit_list_.size());
        ret = OB_NOT_INIT;
      }
      else
      {
        CommitInfo commit_info;
        commit_info.commiter = commiter;
        commit_info.data = data;
        if (0 != commit_list_.push_front(commit_info))
        {
          TBSYS_LOG(WARN, "push rollback info to list rollback list fail");
          ret = OB_ERROR;
        }
        else
        {
          commit_counter_++;
        }
      }
      return ret;
    }

    int64_t TransNode::get_min_flying_trans_id() const
    {
      return trans_mgr_.get_min_flying_trans_id();
    }

    void *TransNode::stack_alloc(const int64_t size)
    {
      return allocator_.alloc(size);
    }

    void TransNode::reset()
    {
      trans_id_const_ = false;
      mutation_started_ = false;
      rollback_counter_ = 0;
      rollback_list_.clear();
      commit_counter_ = 0;
      commit_list_.clear();
      allocator_.reuse();
    }

    int TransNode::rollback()
    {
      int ret = OB_SUCCESS;
      RollbackList::iterator iter;
      for (iter = rollback_list_.begin(); iter != rollback_list_.end(); iter++)
      {
        if (NULL != iter->rollbacker)
        {
          int tmp_ret = iter->rollbacker->rollback(iter->data);
          if (OB_SUCCESS != tmp_ret)
          {
            TBSYS_LOG(WARN, "rollback fail ret=%d", tmp_ret);
            ret = tmp_ret;
          }
        }
      }
      return ret;
    }

    int TransNode::commit()
    {
      int ret = OB_SUCCESS;
      CommitList::iterator iter;
      for (iter = commit_list_.begin(); iter != commit_list_.end(); iter++)
      {
        if (NULL != iter->commiter)
        {
          int tmp_ret = iter->commiter->commit(iter->data);
          if (OB_SUCCESS != tmp_ret)
          {
            TBSYS_LOG(WARN, "commit fail ret=%d", tmp_ret);
            ret = tmp_ret;
          }
        }
      }
      return ret;
    }

    void TransNode::set_trans_id_const(const bool is_const)
    {
      trans_id_const_ = is_const;
    }

    void TransNode::set_trans_id(const int64_t trans_id)
    {
      start_trans_id_ = trans_id;
      cur_trans_id_ = trans_id;
    }

    void TransNode::set_trans_type(const TETransType trans_type)
    {
      trans_type_ = trans_type;
    }

    TETransType TransNode::get_trans_type() const
    {
      return trans_type_;
    }
    
    bool TransNode::mutation_started() const
    {
      return mutation_started_;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    TransMgr::TransMgr() : inited_(false),
                           flying_trans_counter_(0),
                           commited_trans_id_(-1),
                           writing_trans_id_(-1),
                           min_flying_trans_id_(-1),
                           calc_timestamp_(0),
                           trans_node_map_(),
                           trans_node_list_(),
                           allocator_()
    {
    }

    TransMgr::~TransMgr()
    {
      if (inited_)
      {
        destroy();
      }
    }

    int TransMgr::init(const int64_t max_trans_num)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (0 >= max_trans_num)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = trans_node_map_.init(max_trans_num)))
      {
        TBSYS_LOG(WARN, "init trans node map fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = trans_node_list_.init(max_trans_num)))
      {
        TBSYS_LOG(WARN, "init trans node list fail ret=%d", ret);
      }
      else
      {
        flying_trans_counter_ = 0;
        commited_trans_id_ = -1;
        writing_trans_id_ = -1;
        min_flying_trans_id_ = -1;
        calc_timestamp_ = 0;
        for (int64_t i = 0; i < max_trans_num; i++)
        {
          char *buffer = allocator_.alloc(sizeof(TransNode));
          TransNode *trans_node = new(buffer) TransNode(*this);
          if (NULL == trans_node
              || OB_SUCCESS != trans_node_list_.push(trans_node))
          {
            ret = OB_ERROR;
            break;
          }
        }
      }

      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      else
      {
        inited_ = true;
      }
      return ret;
    }

    void TransMgr::destroy()
    {
      if (0 != flying_trans_counter_)
      {
        TBSYS_LOG(ERROR, "some transaction have not end, counter=%ld",
                  flying_trans_counter_);
      }

      TransNode *trans_node = NULL;
      while (OB_SUCCESS == trans_node_list_.pop(trans_node))
      {
        if (NULL != trans_node)
        {
          trans_node->~TransNode();
        }
      }

      allocator_.reuse();
      trans_node_list_.destroy();
      trans_node_map_.destroy();

      inited_ = false;
    }

    int TransMgr::start_transaction(const TETransType trans_type, uint64_t &trans_descriptor, const int64_t trans_id)
    {
      int ret = OB_SUCCESS;
      TransNode *trans_node = NULL;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (READ_TRANSACTION != trans_type
              && WRITE_TRANSACTION != trans_type)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (WRITE_TRANSACTION == trans_type
              && -1 != writing_trans_id_)
      {
        ret = OB_UPS_TRANS_RUNNING;
      }
      else if (WRITE_TRANSACTION == trans_type
              && -1 != trans_id
              && trans_id < commited_trans_id_)
      {
        TBSYS_LOG(WARN, "invalid param trans_id=%ld commited_trans_id=%ld", trans_id, commited_trans_id_);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != trans_node_list_.pop(trans_node)
              || NULL == trans_node)
      {
        TBSYS_LOG(WARN, "to many flying transaction counter=%ld", flying_trans_counter_);
        ret = OB_SIZE_OVERFLOW;
      }
      else
      {
        uint64_t td = 0;
        trans_node->reset();
        while (OB_SUCCESS == ret)
        {
          switch (trans_type) 
          {
            case READ_TRANSACTION:
              trans_node->set_trans_id(commited_trans_id_);
              TBSYS_LOG(DEBUG, "read transaction this=%p id=%ld", this, trans_node->get_trans_id());
              break;
            case WRITE_TRANSACTION:
              writing_trans_id_ = generate_trans_id_(commited_trans_id_, trans_id);
              trans_node->set_trans_id(writing_trans_id_);
              trans_node->set_trans_id_const(-1 != trans_id);
              break;
            default:
              ret = OB_INVALID_ARGUMENT;
              break;
          }
          if (OB_SUCCESS == ret)
          {
            trans_node->set_trans_type(trans_type);
            if (OB_SUCCESS != (ret = trans_node_map_.assign(trans_node, td)))
            {
              TBSYS_LOG(WARN, "assign from trans node map fail ret=%d", ret);
            }
          }
          if (OB_SUCCESS == ret)
          {
            if (READ_TRANSACTION == trans_type
                && trans_node->get_trans_id() != commited_trans_id_)
            {
              trans_node_map_.erase(td);
              continue;
            }
            else
            {
              break;
            }
          }
        }
        if (OB_SUCCESS != ret)
        {
          trans_node_list_.push(trans_node);
        }
        else
        {
          ATOMIC_INC((uint64_t*)&flying_trans_counter_);
          trans_descriptor = td;
        }
      }
      return ret;
    }

    int TransMgr::end_transaction(const uint64_t trans_descriptor, const bool rollback)
    {
      int ret = OB_SUCCESS;
      TransNode *trans_node = NULL;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (ret = trans_node_map_.get(trans_descriptor, trans_node))
              || NULL == trans_node)
      {
        TBSYS_LOG(WARN, "invalid trans descriptor, ret=%d trans_descriptor=%lu", ret, trans_descriptor);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (WRITE_TRANSACTION == trans_node->get_trans_type()
              && (trans_node->mutation_started()
                  || writing_trans_id_ != trans_node->get_start_trans_id()))
      {
        TBSYS_LOG(WARN, "mutation is running or trans id not match, cannot end transaction, "
                  "trans_descriptor=%lu writing_trans_id=%ld node_start_trans_id=%ld node_cur_trans_id=%ld",
                  trans_descriptor, writing_trans_id_, trans_node->get_start_trans_id(), trans_node->get_trans_id());
        ret = OB_UPS_TRANS_RUNNING;
      }
      else
      {
        if (WRITE_TRANSACTION == trans_node->get_trans_type())
        {
          if (rollback)
          {
            trans_node->rollback();
          }
          else
          {
            trans_node->commit();
            commited_trans_id_ = trans_node->get_trans_id();
          }
          writing_trans_id_ = -1;
        }
        ATOMIC_DEC((uint64_t*)&flying_trans_counter_);
        trans_node_map_.erase(trans_descriptor);
        trans_node_list_.push(trans_node);
      }
      return ret;
    }

    TransNode *TransMgr::get_trans_node(const uint64_t trans_descriptor)
    {
      TransNode *ret = NULL;
      trans_node_map_.get(trans_descriptor, ret);
      return ret;
    }

    int64_t TransMgr::get_min_flying_trans_id() const
    {
      if (calc_timestamp_ + CALC_INTERVAL < tbsys::CTimeUtil::getTime()
          || 1 >= trans_node_map_.size())
      {
        int64_t commited_trans_id = commited_trans_id_;
        MapTraverseCallback cb(trans_node_map_);
        trans_node_map_.traverse(cb);
        min_flying_trans_id_ = std::min(cb.get_min_trans_id(), (int64_t)commited_trans_id);
        calc_timestamp_ = tbsys::CTimeUtil::getTime();
      }
      return min_flying_trans_id_;
    }

    int64_t TransMgr::generate_trans_id_(const int64_t commited_trans_id, const int64_t trans_id)
    {
      int64_t ret = (-1 == trans_id) ? tbsys::CTimeUtil::getTime() : trans_id;
      if (ret <= commited_trans_id)
      {
        ret = commited_trans_id + 1;
        if (-1 == trans_id)
        {
          TBSYS_LOG(INFO, "time do not increase, will add 1 as next trans_id=%ld", ret);
        }
      }
      return ret;
    }
  }
}

