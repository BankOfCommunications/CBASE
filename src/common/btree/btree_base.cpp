#include "btree_node.h"
#include "btree_read_param.h"
#include "btree_write_param.h"
#include "btree_root_pointer.h"
#include "btree_base.h"

namespace oceanbase
{
  namespace common
  {

    const char *BtreeBase::MIN_KEY = NULL;
    const char *BtreeBase::MAX_KEY = (reinterpret_cast<char*>(~0));
    const int32_t BtreeBase::MAX_RECYCLE_ROOT_COUNT = 10;
    const int64_t BtreeBase::RECYCLE_MASK = 0xffffffff00000000L;
    const int64_t BtreeBase::REFCOUNT_MASK = 0x7fffffffL;

    /**
     * BTree 基础类, 对BTree结构的创建, 搜索, 以及copy on write
     */

    BtreeBase::BtreeBase(BtreeAlloc *allocator)
    {
      root_pointer_ = NULL;
      root_tail_ = NULL;
      root_recycle_ = NULL;
      tree_count_ = 0;
      key_allocator_ = NULL;
      memset(key_compare_, 0, sizeof(key_compare_));

      proot_sequence_ = 0;

      // 锁初始化
      pthread_mutexattr_t mta;
      int32_t rc = pthread_mutexattr_init(&mta);
      rc = pthread_mutexattr_settype(&mta, PTHREAD_MUTEX_RECURSIVE);
      pthread_mutex_init(&mutex_, &mta);
      pthread_mutexattr_destroy(&mta);
      wl_mutex_ = NULL;

      // BtreeRootPointer也从node_allocator_->分配
      assert(sizeof(BtreeRootPointer) <= static_cast<uint32_t>(BtreeArrayList::NODE_SIZE));
      node_allocator_ = (allocator ? allocator : &node_default_allocator_);
      node_allocator_->init(BtreeArrayList::NODE_SIZE);

      // 设成空root_pointer_
      memset(&empty_root_pointer_, 0, sizeof(BtreeRootPointer));
      empty_root_pointer_.init(node_allocator_);
      root_pointer_ = &empty_root_pointer_;
    }

    /**
     * 析构
     */
    BtreeBase::~BtreeBase()
    {
      pthread_mutex_destroy(&mutex_);
    }

    /**
     * 得到读的句柄
     */
    int32_t BtreeBase::get_read_handle(BtreeBaseHandle &handle)
    {
      // BtreeBaseHandle不能重复使用
      OCEAN_BTREE_CHECK_FALSE(handle.root_pointer_, "handle.root_pointer_ is not null.");
      int32_t ret = ERROR_CODE_FAIL;

      if (NULL == handle.root_pointer_)
      {
        int64_t old_value = 0;
        unsigned char res = 0;
        do
        {
          // 获取root指针, root_pointer不会设成null, 而是设成empty_root_pointer
          old_value = root_pointer_->ref_count_;
          handle.root_pointer_ = root_pointer_;
          handle.ref_count_ = &(handle.root_pointer_->ref_count_);
          if ((old_value & RECYCLE_MASK) == ((root_pointer_->sequence_ << 32) & RECYCLE_MASK) &&
              handle.root_pointer_ == root_pointer_)
          {
            res = BtreeRootPointer::refcas(&(root_pointer_->ref_count_), old_value, old_value + 1);
          }
        }
        while(!res);
        ret = ERROR_CODE_OK;
      }

      return ret;
    }

    /**
     * 得到写的句柄
     */
    int32_t BtreeBase::get_write_handle(BtreeWriteHandle &handle)
    {
      OCEAN_BTREE_CHECK_TRUE(node_allocator_, "node_allocator_ is null.");
      if (NULL == node_allocator_)
        return ERROR_CODE_FAIL;

      // 加锁
      handle.start(wl_mutex_, this);

      BtreeRootPointer *proot = NULL;
      if (proot_freelist_.empty())
      {
        proot = reinterpret_cast<BtreeRootPointer*>(node_allocator_->alloc());
      }
      else
      {
        proot = proot_freelist_.pop();
      }
      OCEAN_BTREE_CHECK_TRUE(proot, "proot is null.");
      int32_t ret = ERROR_CODE_FAIL;
      if (NULL == proot)
      {
        handle.end();
      }
      else
      {
        proot->init(node_allocator_);
        proot_sequence_ ++;
        proot->sequence_ = proot_sequence_;
        proot->ref_count_ = ((proot->sequence_ << 32) & RECYCLE_MASK);

        // 取到只读的root
        OCEAN_BTREE_CHECK_TRUE(root_pointer_, "root_pointer is null.");
        if (root_pointer_ != NULL)
        {
          for(int32_t i = 0; i < tree_count_; i++)
          {
            proot->tree_depth_[i] = root_pointer_->tree_depth_[i];
            proot->root_[i] = root_pointer_->root_[i];
          }
          proot->total_object_count_ = root_pointer_->total_object_count_;
          proot->node_count_ = root_pointer_->node_count_;
        }
        handle.root_pointer_ = proot;
        handle.prev_root_pointer_ = root_pointer_;
        handle.key_allocator_ = key_allocator_;
        handle.node_allocator_ = node_allocator_;
        ret = ERROR_CODE_OK;
      }
      return ret;
    }

    /**
     * 得到object数量
     */
    int64_t BtreeBase::get_object_count()
    {
      return (root_pointer_ ? root_pointer_->total_object_count_ : 0);
    }
    /**
     * 得到node数量
     */
    int32_t BtreeBase::get_node_count()
    {
      return (root_pointer_ ? root_pointer_->node_count_ : 0);
    }
    /**
     * 分配器分配出多少块
     */
    int64_t BtreeBase::get_alloc_count()
    {
      return (node_allocator_ ? node_allocator_->get_use_count() : 0);
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * 根据key, 从一棵子树上, 找出对应的BtreeKeyValuePair对
     */
    BtreeKeyValuePair *BtreeBase::get_one_pair(BtreeBaseHandle &handle, const int32_t tree_id, const char *key)
    {
      OCEAN_BTREE_CHECK_FALSE(NULL == handle.root_pointer_ || tree_id < 0
                              || tree_id >= CONST_MAX_BTREE_COUNT,
                              "handle.root_pointer_: %p, tree_id:%d", handle.root_pointer_, tree_id);

      if (NULL == handle.root_pointer_ || tree_id < 0 || tree_id >= CONST_MAX_BTREE_COUNT)
        return NULL;

      BtreeReadParam param;
      BtreeRootPointer *proot = handle.root_pointer_;
      BtreeKeyValuePair *pair = NULL;
      if (ERROR_CODE_OK == get_children(proot, tree_id, key, param))
      {
        // 找到
        int32_t level = param.node_length_ - 1;
        OCEAN_BTREE_CHECK_TRUE(level >= 0 && level < CONST_MAX_DEPTH, "level: %d", level);
        if (0 == param.found_ && level >= 0 && level < CONST_MAX_DEPTH)
        {
          pair = param.node_[level]->get_pair(param.node_pos_[level]);
          OCEAN_BTREE_CHECK_TRUE(pair, "pair is null, level:%d, pos:%d, node:%p, size:%d, tree_id:%d key:%p",
                                 level, param.node_pos_[level], param.node_[level],
                                 param.node_[level]->get_size(), tree_id, key);
        }
      }
      //OCEAN_BTREE_CHECK_TRUE(pair, "pair is null, key=%p, tree_id=%d, proot=%p",
      //  key, tree_id, proot);

      return pair;
    }

    /**
     * 根据key, 从一棵子树上, 找出对应的BtreeKeyValuePair对, 范围搜索
     */
    BtreeKeyValuePair *BtreeBase::get_range_pair(BtreeReadHandle &handle)
    {
      OCEAN_BTREE_CHECK_FALSE(NULL == handle.root_pointer_ || handle.tree_id_ < 0
                              || handle.tree_id_ >= CONST_MAX_BTREE_COUNT,
                              "handle.root_pointer_: %p, tree_id:%d", handle.root_pointer_, handle.tree_id_);

      if (NULL == handle.root_pointer_ || handle.tree_id_ < 0 || handle.tree_id_ >= CONST_MAX_BTREE_COUNT)
        return NULL;

      BtreeRootPointer *proot = handle.root_pointer_;
      BtreeReadParam *param = &handle.param_;
      BtreeKeyValuePair *pair = NULL;
      int32_t tree_id = handle.tree_id_;
      // 没找过, 先找出路径
      BtreeReadParam *to_param = &handle.to_param_;
      int32_t level = 0;

      if (0 == param->node_length_)
      {
        int32_t ret1 = 0;
        int32_t ret2 = 0;
        ret1 = get_children(proot, tree_id, handle.from_key_, *param);
        ret2 = get_children(proot, tree_id, handle.to_key_, *to_param);
        OCEAN_BTREE_CHECK_TRUE(to_param->node_length_ == param->node_length_,
                               "node_length: %d %d", to_param->node_length_, param->node_length_);

        level = param->node_length_ - 1;
        if (ret1 == ERROR_CODE_OK && ret2 == ERROR_CODE_OK
            && to_param->node_length_ == param->node_length_
            && level >= 0 && level < CONST_MAX_DEPTH)
        {
          // 方向, -1 向左, 1 向右, 0 相等
          int32_t direction = 0;
          for(int32_t i = 0; i < param->node_length_; i++)
          {
            direction = to_param->node_pos_[i] - param->node_pos_[i];
            if (direction)
            {
              direction = (direction < 0 ? -1 : 1);
              break;
            }
          }
          handle.direction_ = direction;
          if (handle.direction_ == 0) handle.direction_ = ((param->found_ >= to_param->found_) ? 1 : -1);
          // from
          if (param->node_pos_[level] == -1 ||
              (handle.direction_ > 0 && ((handle.from_exclude_ && param->found_ == 0) || param->found_ < 0)) ||
              (handle.direction_ < 0 && ((handle.from_exclude_ && param->found_ == 0) || param->found_ > 0)))
          {
            if (direction == 0)
              handle.is_eof_ = 1;
            else
              handle.is_eof_ = pos_move_next(param, handle.direction_);
          }
          // to
          if ((handle.direction_ > 0 && ((handle.to_exclude_ && to_param->found_ == 0) || to_param->found_ > 0)) ||
              (handle.direction_ < 0 && ((handle.to_exclude_ && to_param->found_ == 0) || to_param->found_ < 0)))
            handle.to_exclude_ = 1;
          else
            handle.to_exclude_ = 0;
        }
        else
        {
          handle.is_eof_ = 1;
          param->node_length_ = -1;
        }
      }

      // 取当前值
      level = param->node_length_ - 1;
      if (0 == handle.is_eof_ && level >= 0 && level < CONST_MAX_DEPTH)
      {
        // 到to_key了
        if (to_param->node_[level] == param->node_[level] &&
            to_param->node_pos_[level] == param->node_pos_[level])
        {
          // 等于to_key
          if (0 == handle.to_exclude_)
          {
            pair = param->node_[level]->get_pair(param->node_pos_[level]);
            handle.to_exclude_ = 1;
            OCEAN_BTREE_CHECK_TRUE(pair, "level: %d, pos: %d, node: %p, size: %d", level,
                                   param->node_pos_[level], param->node_[level], param->node_[level]->get_size());
          }
          else
          {
            handle.is_eof_ = 1;
          }
        }
        else
        {
          pair = param->node_[level]->get_pair(param->node_pos_[level]);
          OCEAN_BTREE_CHECK_TRUE(pair, "level: %d, pos: %d, node: %p, size: %d", level,
                                 param->node_pos_[level], param->node_[level], param->node_[level]->get_size());

          // 移到下一个
          handle.is_eof_ = pos_move_next(param, handle.direction_);
        }
      }

      return pair;
    }

    /**
     * 移动位置到下一个
     */
    int32_t BtreeBase::pos_move_next(BtreeReadParam *param, int32_t direction)
    {
      int32_t is_eof = 0;
      OCEAN_BTREE_CHECK_TRUE(param, "param is null.");
      if (NULL != param)
      {
        int32_t level = param->node_length_ - 1;
        OCEAN_BTREE_CHECK_TRUE(level >= 0 && level < CONST_MAX_DEPTH, "level: %d", level);
        if (level >= 0 && level < CONST_MAX_DEPTH)
        {
          // 向后移
          if (direction > 0)
          {
            param->node_pos_[level] ++;
            while(param->node_pos_[level] >= param->node_[level]->get_size())
            {
              param->node_pos_[level] = 0;
              if (--level < 0)
              {
                is_eof = 1;
                break;
              }
              param->node_pos_[level] ++;
            }
          }
          else
          {
            param->node_pos_[level] --;
            while(param->node_pos_[level] < 0)
            {
              if (--level < 0)
              {
                is_eof = 1;
                break;
              }
              param->node_pos_[level] --;
            }
          }
          BtreeNode *parent = NULL;
          BtreeKeyValuePair *pair = NULL;

          // 设置node
          while(0 == is_eof && level < param->node_length_ - 1 && level >= 0 && level < CONST_MAX_DEPTH - 1)
          {
            parent = param->node_[level];
            OCEAN_BTREE_CHECK_TRUE(parent, "parent is null.");
            if (NULL != parent)
            {
              pair = parent->get_pair(param->node_pos_[level]);
              OCEAN_BTREE_CHECK_TRUE(pair, "pair is null.");
              if (NULL == pair)
              {
                is_eof = 1;
                break;
              }

              assert(parent->is_leaf() == 0);
              OCEAN_BTREE_CHECK_TRUE(parent->is_leaf() == 0, "parent isn't leaf node.");
              param->node_[level+1] = reinterpret_cast<BtreeNode*>(pair->value_);
              OCEAN_BTREE_CHECK_TRUE(param->node_[level+1],
                                     "param->node_[level+1] is null, level: %d", level);
              if (NULL == param->node_[level+1])
              {
                is_eof = 1;
                break;
              }

              if (param->node_pos_[level+1] < 0)
              {
                param->node_pos_[level+1] = static_cast<int16_t>(param->node_[level+1]->get_size() - 1);
                OCEAN_BTREE_CHECK_TRUE(param->node_pos_[level+1] >= 0,
                                       "param->node_pos: %d, level: %d, size: %d", param->node_pos_[level+1], level + 1,
                                       param->node_[level+1]->get_size());
              }
            }
            level ++;
          }
        }
      }
      return is_eof;
    }

    /**
     * 根据key得到路径
     */
    int32_t BtreeBase::get_children(BtreeRootPointer *proot, const int32_t tree_id, const char *key, BtreeReadParam &param)
    {
      OCEAN_BTREE_CHECK_TRUE(proot, "proot is null.");
      OCEAN_BTREE_CHECK_TRUE(tree_id >= 0 && tree_id < CONST_MAX_BTREE_COUNT, "tree_id: %d", tree_id);

      int32_t ret = ERROR_CODE_FAIL;
      if (NULL != proot && tree_id >= 0 && tree_id < CONST_MAX_BTREE_COUNT)
      {
        BtreeNode *root = proot->root_[tree_id];
        BtreeKeyValuePair *child_pair = NULL;
        int32_t pos = -1;
        int64_t found = -1;

        // search path
        param.node_length_ = 0;
        int32_t depth = proot->tree_depth_[tree_id];
        for(int32_t i = 0; NULL != root && i < depth && i < CONST_MAX_DEPTH; i++)
        {
          if (key_allocator_)
          {
            if (BtreeBase::MIN_KEY == key)
            {
              pos = -1;
              found = 1;
            }
            else if (BtreeBase::MAX_KEY == key)
            {
              pos = root->get_size() - 1;
              found = -1;
            }
            else
            {
              pos = root->find_pos(key, key_compare_[tree_id], found);
            }
          }
          else
          {
            pos = root->find_pos(key, key_compare_[tree_id], found);
          }
          if (CONST_NODE_OBJECT_COUNT == pos) break;

          // set path
          param.node_[i] = root;
          param.node_pos_[i] = static_cast<int16_t>(pos);
          param.node_length_ ++;
          if (i == depth - 1)
          {
            ret = ERROR_CODE_OK;
            break;
          }
          assert(root->is_leaf() == 0);
          OCEAN_BTREE_CHECK_TRUE(root->is_leaf() == 0, "root isn't leaf node.");

          // get child
          if (param.node_pos_[i] < 0)
          {
            param.node_pos_[i] = 0;
          }
          child_pair = root->get_pair(param.node_pos_[i]);
          root = reinterpret_cast<BtreeNode*>(child_pair->value_);
        }
        param.found_ = found;
      }
      return ret;
    }

    /**
     * 在树上(所有子树)插入key,value对
     */
    int32_t BtreeBase::put_pair(BtreeWriteHandle &handle, const char *key, const char *value, const bool overwrite)
    {
      OCEAN_BTREE_CHECK_TRUE(node_allocator_, "node_allocator_ is null.");
      if (NULL == node_allocator_)
        return ERROR_CODE_FAIL;

      int32_t ret = ERROR_CODE_FAIL;
      // 释放空的next_handle
      handle.old_value_ = NULL;
      handle.old_key_ = NULL;
      if (handle.next_handle_ && NULL == handle.next_handle_->owner_)
      {
        char *data = reinterpret_cast<char*>(handle.next_handle_);
        node_allocator_->release(data);
        handle.next_handle_ = NULL;
      }

      OCEAN_BTREE_CHECK_FALSE((NULL == handle.root_pointer_ || NULL != handle.next_handle_),
                              "handle.root_pointer_: %p, handle.next_handle_: %p",
                              handle.root_pointer_, handle.next_handle_);
      if (NULL != handle.root_pointer_ && NULL == handle.next_handle_)
      {
        BtreeRootPointer *proot = handle.root_pointer_;
        OCEAN_BTREE_CHECK_TRUE(proot, "proot is null.");

        // 如果root_pointer_为空创建新root
        if (0 == proot->node_count_)
        {
          // 创建root node
          for(int32_t id = 0; id < tree_count_; id ++)
          {
            proot->root_[id] = proot->new_node(CONST_NODE_OBJECT_COUNT);
            proot->root_[id]->set_leaf(1);
            OCEAN_BTREE_CHECK_TRUE(proot->root_[id], "proot->root_[%d] is null", id);
            if (NULL != proot->root_[id])
            {
              proot->tree_depth_[id] ++;
              proot->node_count_ ++;

              // 第一个key, value的插入
              ret = proot->root_[id]->add_pair(0, key, value, key_compare_[id]);
              OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d, key:%p, value:%p, id:%d",
                                     ret,  key, value, id);
              if (ERROR_CODE_OK != ret) break;
            }
          }
        }
        else
        {
          // 路径操作变量
          BtreeWriteParam params[tree_count_];
#ifdef OCEAN_BTREE_CHECK
          const char *check_key = key;
#endif

          // 对内部每棵树进行插入
          for(int32_t id = 0; id < tree_count_; id ++)
          {
            params[id].tree_id = id;
            ret = get_children(proot, id, key, params[id]);
            OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d, id:%d key:%p",
                                   ret, id, key);
            if (ERROR_CODE_OK != ret) break;

            // key 存在了, 不允许重复
            if (0 == params[id].found_)
            {
              ret = ERROR_CODE_KEY_REPEAT;
              break;
            }
          }

          // key 没重复
          if (ERROR_CODE_OK == ret)
          {

            // 对每棵树
            for(int32_t id = 0; id < tree_count_; id ++)
            {
              ret = copy_node_for_write(handle, params[id]);
              OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d, id=%d", ret, id);
              if (ERROR_CODE_OK != ret) break;

              ret = add_pair_to_node(handle, params[id], params[id].node_length_ - 1, key, value);
              OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d, id:%d len:%d, key:%p, value:%p",
                                     ret, id, params[id].node_length_, key, value);

              if (ret != ERROR_CODE_OK)
                break;

              proot->tree_depth_[id] = static_cast<int8_t>(params[id].node_length_);
              if (params[id].new_root_node_)
                proot->root_[id] = params[id].new_root_node_;
              else
                proot->root_[id] = params[id].node_[0];
            }
          }
          else if ((true == overwrite) && (ERROR_CODE_KEY_REPEAT == ret))
          {
            // 覆盖原来的值
            int32_t index = params[0].node_length_ - 1;
            BtreeKeyValuePair *oldpair = NULL;
            if (index >= 0 && index < CONST_MAX_DEPTH)
            {
              oldpair = params[0].node_[index]->get_pair(params[0].node_pos_[index]);
              OCEAN_BTREE_CHECK_TRUE(oldpair, "oldpair is null, index:%d, pos:%d",
                                     index, params[0].node_pos_[index]);
            }
            if (NULL != oldpair)
            {
              handle.old_value_ = oldpair->value_;
              handle.old_key_ = oldpair->key_;
#ifdef OCEAN_BTREE_CHECK
              check_key = oldpair->key_;
#endif
              for(int32_t id = 0; id < tree_count_; id ++)
              {
                ret = copy_node_for_write(handle, params[id]);
                OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d, id:5d", ret, id);
                if (ERROR_CODE_OK != ret) break;

                ret = params[id].node_[index]->set_pair(params[id].node_pos_[index],
                                                        oldpair->key_, value,
                                                        BtreeNode::UPDATE_KEY | BtreeNode::UPDATE_VALUE,
                                                        key_compare_[id]);
                OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d, id:%d, index:%d key:%p value:%p",
                                       ret, id, index, oldpair->key_, value);

                if (ERROR_CODE_OK != ret) break;
                proot->root_[id] = params[id].node_[0];
              }
              // 新的key释放掉
              if (ERROR_CODE_OK == ret)
              {
                if (key_allocator_)
                {
                  update_key_ref_count(handle, const_cast<char*>(key), NULL);
                }
                proot->total_object_count_ --;
              }
            }
            else
            {
              ret = ERROR_CODE_FAIL;
            }
          }
#ifdef OCEAN_BTREE_CHECK
          BtreeKeyValuePair *pair = NULL;
          BtreeKeyValuePair *fpair = NULL;
          for(int32_t id = 0; ERROR_CODE_OK == ret && id < tree_count_; id ++)
          {
            if (ERROR_CODE_OK == get_children(proot, id, check_key, params[id]))
            {
              for(int32_t i = 0; i < params[id].node_length_ - 1; i++)
              {
                pair = params[id].node_[i]->get_pair(params[id].node_pos_[i]);
                OCEAN_BTREE_CHECK_TRUE(pair && pair->value_ ==
                                       reinterpret_cast<char*>(params[id].node_[i+1]),
                                       "node is error: %p %d", params[id].node_[i], i);
                if (pair && pair->value_)
                {
                  fpair = reinterpret_cast<BtreeNode*>(pair->value_)->get_pair(0);
                  OCEAN_BTREE_CHECK_TRUE(fpair && pair && pair->key_ == fpair->key_,
                                         "fist key is error: %p %d", params[id].node_[i], i);
                }
              }
            }
          }
#endif
        }

        if (ERROR_CODE_OK == ret)
          proot->total_object_count_ ++;
      }
      // 失败
      if (ERROR_CODE_OK != ret)
        handle.error_ = ret;

      return ret;
    }

    /**
     * 根据一个key, 删除树上(所有子树)的pair.
     */
    int32_t BtreeBase::remove_pair(BtreeWriteHandle &handle, const int32_t tree_id, const char *key)
    {
      int32_t ret = ERROR_CODE_FAIL;
      BtreeRootPointer *proot = handle.root_pointer_;
      handle.old_value_ = NULL;
      OCEAN_BTREE_CHECK_TRUE(proot, "proot is null.");
      OCEAN_BTREE_CHECK_TRUE(tree_id >= 0 && tree_id < CONST_MAX_BTREE_COUNT,
                             "tree_id:%d, key:%p", tree_id, key);

      if (NULL != proot && tree_id >= 0 && tree_id < CONST_MAX_BTREE_COUNT)
      {
        ret = ERROR_CODE_NOT_FOUND;

        // root_pointer_不为空
        if (proot->root_[0] != NULL)
        {
          BtreeWriteParam params[tree_count_];

          // 对内部每棵树进行搜索
          params[tree_id].tree_id = tree_id;
          ret = get_children(proot, tree_id, key, params[tree_id]);
          int32_t level = params[tree_id].node_length_ - 1;
          OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret: %d", ret);

          // key存在
          if (ERROR_CODE_OK == ret && 0 == params[tree_id].found_
              && level >= 0 && level < CONST_MAX_DEPTH)
          {
            if (tree_count_ > 1)
            {
              // 完整的key
              BtreeKeyValuePair *pair = NULL;
              pair = params[tree_id].node_[level]->get_pair(params[tree_id].node_pos_[level]);
              // 得到key, 把其他树也路径也找出来
              for(int32_t id = 0; id < tree_count_; id ++)
              {
                params[id].tree_id = id;
                if (id != tree_id)
                {
                  ret = get_children(proot, id, pair->key_, params[id]);
                  OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d", ret);
                  if (ret != ERROR_CODE_OK)
                    break;
                }
              }
            }

            // 对每棵树进行删除
            for(int32_t id = 0; ERROR_CODE_OK == ret && id < tree_count_; id ++)
            {
              ret = copy_node_for_write(handle, params[id]);
              OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d", ret);
              if (ret != ERROR_CODE_OK)
                break;
              ret = remove_pair_from_node(handle, params[id], params[id].node_length_ - 1, 0);
              OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d", ret);
              if (ret != ERROR_CODE_OK)
                break;

              proot->tree_depth_[id] = static_cast<int8_t>(params[id].node_length_);
              if (params[id].new_root_node_)
                proot->root_[id] = params[id].new_root_node_;
              else
                proot->root_[id] = params[id].node_[0];
              if (proot->root_[id] != NULL)
              {
                OCEAN_BTREE_CHECK_TRUE(proot->root_[id]->is_deleted() == 0, "root is deleted");
              }
            }
          }
          else
          {
            ret = ERROR_CODE_NOT_FOUND;
          }
        }
      }

      // 成功
      if (ERROR_CODE_OK == ret)
        proot->total_object_count_ --;

      return ret;

    }

    /**
     * 把搜索到的node,复制出一份来
     */
    int32_t BtreeBase::copy_node_for_write(BtreeWriteHandle &handle, BtreeWriteParam &param)
    {
      int32_t ret = ERROR_CODE_OK;
      BtreeNode *parent = NULL;

      // copy node
      for(int32_t i = 0; i < param.node_length_ && i < CONST_MAX_DEPTH; i++)
      {
        OCEAN_BTREE_CHECK_TRUE(param.node_[i], "param.node_[%d] is null.", i);
        if (NULL == param.node_[i])
        {
          ret = ERROR_CODE_FAIL;
          break;
        }

        // 只读的才复制
        if (0 == param.node_[i]->is_read_only())
          continue;

        // 加入到上一个root_pointer等待relaese
        OCEAN_BTREE_CHECK_TRUE(handle.prev_root_pointer_, "handle.prev_root_pointer_ is null.");
        OCEAN_BTREE_CHECK_TRUE(handle.root_pointer_, "handle.root_pointer_ is null.");
        if (NULL == handle.prev_root_pointer_ || NULL == handle.root_pointer_)
        {
          ret = ERROR_CODE_FAIL;
          break;
        }

        handle.prev_root_pointer_->add_node_list(param.node_[i]);
        param.node_[i] = handle.root_pointer_->copy_node(param.node_[i], key_allocator_);
        OCEAN_BTREE_CHECK_TRUE(param.node_[i], "param.node_[%d] is null.", i);
        if (NULL == param.node_[i])
        {
          ret = ERROR_CODE_FAIL;
          break;
        }

        // 更新parent上的value
        parent = ((i > 0) ? param.node_[i-1] : NULL);
        if (parent)
        {
          ret = parent->set_pair(param.node_pos_[i-1], NULL,
                                 reinterpret_cast<char*>(param.node_[i]),
                                 BtreeNode::UPDATE_VALUE,
                                 key_compare_[param.tree_id]);
          OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d", ret);
          if (ERROR_CODE_OK != ret) break;
        }
        else if (i > 0)
        {
          ret = ERROR_CODE_FAIL;
          break;
        }
      }
      return ret;
    }

    /**
     * 把key,value写到node上
     */
    int32_t BtreeBase::add_pair_to_node(BtreeWriteHandle &handle, BtreeWriteParam& param,
                                        int32_t level, const char *key, const char *value)
    {
      OCEAN_BTREE_CHECK_TRUE(handle.root_pointer_, "handle.root_pointer_ is null.");
      OCEAN_BTREE_CHECK_FALSE(level < 0 || level >= CONST_MAX_DEPTH, "level: %d", level);
      OCEAN_BTREE_CHECK_FALSE(param.node_length_ < 0 || param.node_length_ >= CONST_MAX_DEPTH, "level: %d", level);
      if (level < 0 || level >= CONST_MAX_DEPTH || NULL == handle.root_pointer_ ||
          param.node_length_ < 0 || param.node_length_ >= CONST_MAX_DEPTH)
        return ERROR_CODE_FAIL;

      BtreeNode *node = NULL;
      BtreeNode *next_node = NULL;
      BtreeNode *new_node = NULL;
      BtreeNode *parent = NULL;
      BtreeKeyValuePair *first_key = NULL;
      int32_t size = 0, move_size = 0, pos = 0;
      int32_t ret = ERROR_CODE_OK;
      int32_t new_pos = param.node_pos_[level] + 1;
      int32_t need_update_fk_level = level;
      BtreeRootPointer *proot = handle.root_pointer_;

      // 更新key的引用计数
      if (key_allocator_ && level < param.node_length_ - 1)
      {
        update_key_ref_count(handle, NULL, const_cast<char*>(key));
      }

      node = param.node_[level];
      OCEAN_BTREE_CHECK_TRUE(node, "node is null.");
      // 叶节点没满, 直接插入
      if (node && node->get_size() < CONST_NODE_OBJECT_COUNT)
      {
        ret = node->add_pair(static_cast<int16_t>(new_pos), key, value, key_compare_[param.tree_id]);
        OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d", ret);
      }
      else if (node)
      {
        // copy出来再写
        next_node = copy_next_node_for_write(handle, param, level);

        // 如果下一页面没满, 把当前数据移一部分到下节点
        int32_t is_no_full = (next_node != NULL && next_node->get_size() < CONST_NODE_OBJECT_COUNT);
        if (next_node && CONST_NODE_OBJECT_COUNT == new_pos && is_no_full)
        {
          // 插入到next_node即可
          ret = next_node->add_pair(0, key, value, key_compare_[param.tree_id]);
          OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d", ret);
        }
        else if (next_node && is_no_full)
        {
          // 平均个数
          size = (node->get_size() + next_node->get_size()) / 2;
          move_size = CONST_NODE_OBJECT_COUNT - size;

          // 保证key能插入在当前node上
          if (CONST_NODE_OBJECT_COUNT - new_pos < move_size)
          {
            move_size = CONST_NODE_OBJECT_COUNT - new_pos;
          }
          OCEAN_BTREE_CHECK_TRUE(move_size >= 1 && move_size <= CONST_NODE_OBJECT_COUNT - next_node->get_size(),
                                 "move_size: %d, next_size: %d", move_size, next_node->get_size());

          // 移出move_size个pair
          ret = node->move_pair_out(next_node, move_size);
          OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d", ret);

          if (ERROR_CODE_OK == ret)
          {
            // 插入到当前的node上
            ret = node->add_pair(static_cast<int16_t>(new_pos), key, value, key_compare_[param.tree_id]);
            OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d", ret);
          }
        }
        else
        {
          // 要新增加一个node
          new_node = proot->new_node(CONST_NODE_OBJECT_COUNT);
          if (level == param.node_length_ - 1) new_node->set_leaf(1);
          OCEAN_BTREE_CHECK_TRUE(new_node, "new_node is null.");
          if (NULL != new_node)
          {
            proot->node_count_ ++;

            move_size = CONST_NODE_OBJECT_COUNT / (next_node ? 3 : 2);
            // 移出new_node上
            ret = node->move_pair_out(new_node, move_size);
            OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d", ret);
            if (ERROR_CODE_OK == ret)
            {
              // 从next_node移到new_node上
              if (next_node != NULL)
              {
                new_node->move_pair_in(next_node, move_size);
              }

              // 插入新的key,value
              if (node->get_size() > new_pos)
              {
                ret = node->add_pair(static_cast<int16_t>(new_pos), key, value, key_compare_[param.tree_id]);
                OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d", ret);
              }
              else
              {
                pos = new_pos - node->get_size();
                ret = new_node->add_pair(static_cast<int16_t>(pos), key, value, key_compare_[param.tree_id]);
                OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d", ret);
              }
            }
          }
        }

        // 需要多一层
        if (0 == level)
        {
          // 树太深了
          if (param.node_length_ >= CONST_MAX_DEPTH)
          {
            ret = ERROR_CODE_TOO_DEPTH;
          }
          else
          {
            OCEAN_BTREE_CHECK_TRUE(new_node, "new_node is null.");
            parent = proot->new_node(CONST_NODE_OBJECT_COUNT);
            proot->node_count_ ++;

            // 第一个key
            first_key = node->get_pair(0);
            ret = parent->add_pair(0, first_key->key_, reinterpret_cast<char*>(node),
                                   key_compare_[param.tree_id]);
            OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d", ret);
            if (key_allocator_) update_key_ref_count(handle, NULL, first_key->key_);

            // 第二个key
            first_key = new_node->get_pair(0);
            ret = parent->add_pair(1, first_key->key_, reinterpret_cast<char*>(new_node),
                                   key_compare_[param.tree_id]);
            OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d", ret);
            if (key_allocator_) update_key_ref_count(handle, NULL, first_key->key_);

            param.new_root_node_ = parent;
            param.node_length_ ++;
          }
        }
        else
        {
          // 更新first_key;
          update_parent_first_key(handle, param, level);
          need_update_fk_level = level - 1;

          // 新增
          if (new_node != NULL)
          {
            first_key = new_node->get_pair(0);
            ret = add_pair_to_node(handle, param, level - 1, first_key->key_, reinterpret_cast<char*>(new_node));
            need_update_fk_level = 0;
          }
        }
      }
      else
      {
        ret = ERROR_CODE_FAIL;
        need_update_fk_level = 0;
      }
      // 更新first_key;
      if (need_update_fk_level)
      {
        for(int32_t i = need_update_fk_level; i > 0; i--)
          update_parent_first_key(handle, param, i);
      }
      return ret;
    }

    /**
     * 从node里删除掉key
     */
    int32_t BtreeBase::remove_pair_from_node(BtreeWriteHandle &handle,
        BtreeWriteParam& param, int32_t level, int32_t next_pos)
    {
      OCEAN_BTREE_CHECK_TRUE(handle.root_pointer_, "handle.root_pointer_ is null.");
      OCEAN_BTREE_CHECK_FALSE(level < 0 || level >= CONST_MAX_DEPTH, "level: %d", level);
      OCEAN_BTREE_CHECK_FALSE(next_pos < 0 || next_pos > 1, "next_pos: %d", next_pos);
      OCEAN_BTREE_CHECK_FALSE(param.node_length_ < 0 || param.node_length_ >= CONST_MAX_DEPTH, "level: %d", level);
      if (level < 0 || level >= CONST_MAX_DEPTH || NULL == handle.root_pointer_ ||
          param.node_length_ < 0 || param.node_length_ >= CONST_MAX_DEPTH || next_pos < 0 || next_pos > 1)
        return ERROR_CODE_FAIL;

      BtreeNode *node = NULL;
      BtreeNode *next_node = NULL;
      BtreeKeyValuePair romoved_pair;
      int32_t ret = ERROR_CODE_OK;
      int32_t need_update_fk_level = 0;
      int32_t new_pos = param.node_pos_[level] + next_pos;
      BtreeRootPointer *proot = handle.root_pointer_;
      romoved_pair.key_ = NULL;

      // 要从下一个中去删除掉
      node = param.node_[level];
      OCEAN_BTREE_CHECK_TRUE(node, "node is null.");
      if (NULL != node)
      {

        next_node = param.next_node_[level];
        if (new_pos >= node->get_size())
        {
          new_pos = 0;
          next_node = copy_next_node_for_write(handle, param, level);
          if (NULL != next_node)
          {
            ret = next_node->remove_pair(static_cast<int16_t>(new_pos), romoved_pair);
            OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d", ret);
          }
        }
        else
        {
          ret = node->remove_pair(param.node_pos_[level], romoved_pair);
          OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d", ret);
        }

        // 把删除的值带出去
        if (level == param.node_length_ - 1)
          handle.old_value_ = romoved_pair.value_;

        // 更新key的引用计数
        if (key_allocator_)
        {
          update_key_ref_count(handle, romoved_pair.key_, NULL);
        }
        // 更新first key
        if (new_pos <= 1 && node->get_size() > 0 && level > 0)
        {
          update_parent_first_key(handle, param, level);
          need_update_fk_level = level - 1;
        }

        // root节点上
        if (0 == level)
        {
          // 只有一个节点, 并且root是非叶节点, 去掉一层
          if (1 == node->get_size() && param.node_length_ > 1)
          {
            BtreeKeyValuePair *first_key = node->get_pair(0);
            OCEAN_BTREE_CHECK_TRUE(first_key, "first_key is null.");
            if (key_allocator_ && first_key)
            {
              update_key_ref_count(handle, first_key->key_, NULL);
            }
            node->set_deleted(1);
            proot->node_count_ --;

            // root节点指到下一层
            if (first_key)
            {
              param.new_root_node_ = reinterpret_cast<BtreeNode*>(first_key->value_);
            }
            param.node_length_ --;
          }
          else if (0 == node->get_size()) // 最后一个key也删除掉了
          {
            node->set_deleted(1);
            proot->node_count_ --;
            param.node_length_ = 0;
            param.node_[0] = NULL;
          }
        }
        // 节点是空的
        else if (0 == node->get_size())
        {
          node->set_deleted(1);
          // 从parent中删除掉node
          ret = remove_pair_from_node(handle, param, level - 1, 0);
          OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d", ret);
          need_update_fk_level = 0;
          proot->node_count_ --;
        }
        // 与下一个节点合并
        else if (next_node != NULL && node->get_size() + next_node->get_size() <= CONST_NODE_OBJECT_COUNT)
        {
          if (key_allocator_) next_node->inc_key_refcount();
          ret = node->merge_pair_in(next_node);
          OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d", ret);
          // 把next_node放回等待回收列表中
          if (next_node->is_read_only())
          {
            handle.prev_root_pointer_->add_node_list(next_node);
          }
          else
          {
            next_node->set_deleted(1);
          }
          // 从parent中删除掉next_node
          ret = remove_pair_from_node(handle, param, level - 1, 1);
          OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d", ret);
          need_update_fk_level = 0;
          proot->node_count_ --;
        }
        // 更新first_key;
        if (need_update_fk_level)
        {
          for(int32_t i = need_update_fk_level; i > 0; i--)
            update_parent_first_key(handle, param, i);
        }
      }
      else
      {
        ret = ERROR_CODE_FAIL;
      }

      return ret;
    }

    /**
     * 把next_node复制出来
     */
    BtreeNode* BtreeBase::copy_next_node_for_write(BtreeWriteHandle &handle, BtreeWriteParam& param, int32_t level)
    {
      OCEAN_BTREE_CHECK_FALSE(level < 0 || level >= CONST_MAX_DEPTH, "level: %d", level);
      OCEAN_BTREE_CHECK_FALSE(param.tree_id < 0 || param.tree_id > CONST_MAX_BTREE_COUNT,
                              "param.tree_id: %d", param.tree_id);

      if (level < 0 || level >= CONST_MAX_DEPTH || param.tree_id < 0 || param.tree_id > CONST_MAX_BTREE_COUNT)
        return NULL;

      BtreeNode *node = param.next_node_[level];
      if (NULL == node && level > 0)
      {
        BtreeNode *parent = param.node_[level-1];
        int32_t pos = param.node_pos_[level-1];
        BtreeKeyValuePair *pair = parent->get_next_pair(static_cast<int16_t>(pos));

        // 最后一个pos
        if (NULL == pair)
        {
          parent = copy_next_node_for_write(handle, param, level - 1);
          if (parent != NULL && (pair = parent->get_pair(0)) != NULL)
          {
            node = reinterpret_cast<BtreeNode*>(pair->value_);
            pos = -1;
          }
        }
        else
        {
          node = reinterpret_cast<BtreeNode*>(pair->value_);
        }

        // copy一个出来
        if (NULL != node && node->is_read_only())
        {
          // 加入到上一个root_pointer等待relaese
          if (handle.prev_root_pointer_)
          {
            handle.prev_root_pointer_->add_node_list(node);
          }
          else
          {
            OCEAN_BTREE_CHECK_TRUE(handle.prev_root_pointer_, "handle.prev_root_pointer_ is null");
            node = NULL;
          }
          if (node && handle.root_pointer_)
          {
            param.next_node_[level] = handle.root_pointer_->copy_node(node, key_allocator_);
          }
          else
          {
            OCEAN_BTREE_CHECK_TRUE(handle.root_pointer_, "handle.root_pointer_ is null");
            node = NULL;
          }
          node = param.next_node_[level];
          if (node)
          {
            int32_t ret = parent->set_pair(static_cast<int16_t>(pos + 1), NULL, reinterpret_cast<char*>(node),
                                           BtreeNode::UPDATE_VALUE,
                                           key_compare_[param.tree_id]);
            OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d", ret);
            if(ret) node = NULL;
          }
        }
        else if (node != NULL)
        {
          param.next_node_[level] = node;
        }
      }
      return node;
    }

    /**
     * 更新key上的引用计数
     */
    void BtreeBase::update_key_ref_count(BtreeWriteHandle &handle, char *old_key, char *new_key)
    {
      if (NULL == old_key)
      {
        handle.add_key_to_list(new_key, 1);
      }
      else if (NULL == new_key)
      {
        handle.add_key_to_list(old_key, -1);
      }
      else if (old_key != new_key)
      {
        handle.add_key_to_list(old_key, -1);
        handle.add_key_to_list(new_key, 1);
      }
    }

    /**
     * 更新first key
     */
    void BtreeBase::update_parent_first_key(BtreeWriteHandle &handle, BtreeWriteParam& param, int32_t level)
    {
      OCEAN_BTREE_CHECK_FALSE(level <= 0 || level >= CONST_MAX_DEPTH, "level: %d", level);
      OCEAN_BTREE_CHECK_FALSE(param.tree_id < 0 || param.tree_id > CONST_MAX_BTREE_COUNT,
                              "param.tree_id: %d", param.tree_id);

      if (level <= 0 || level >= CONST_MAX_DEPTH || param.tree_id < 0 || param.tree_id > CONST_MAX_BTREE_COUNT)
        return;

      BtreeNode *node = NULL;
      BtreeNode *parent = NULL;
      BtreeKeyValuePair *first_key = NULL;
      BtreeKeyValuePair *old_key = NULL;
      int32_t ret = 0;

      node = param.node_[level];
      parent = param.node_[level-1];
      OCEAN_BTREE_CHECK_TRUE(node, "node is null.");
      OCEAN_BTREE_CHECK_TRUE(parent, "parent is null.");
      if (NULL != node && NULL != parent)
      {
        first_key = node->get_pair(0);
        OCEAN_BTREE_CHECK_TRUE(first_key, "first_key is null.");

        // key是引用的
        if (key_allocator_ && first_key)
        {
          old_key = parent->get_pair(param.node_pos_[level-1]);
          OCEAN_BTREE_CHECK_TRUE(old_key, "old_key is null.");
          if (old_key) update_key_ref_count(handle, old_key->key_, first_key->key_);
        }
        ret = parent->set_pair(param.node_pos_[level-1], first_key->key_,
                               reinterpret_cast<char*>(node), BtreeNode::UPDATE_KEY | BtreeNode::UPDATE_VALUE,
                               key_compare_[param.tree_id]);
        OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d", ret);
      }

      // next node
      node = param.next_node_[level];
      parent = param.node_[level-1];
      if (NULL != node && NULL != parent && 0 == node->is_read_only())
      {
        first_key = node->get_pair(0);
        int32_t pos = param.node_pos_[level-1] + 1;
        if (pos >= parent->get_size())
        {
          pos = 0;
          parent = param.next_node_[level-1];
        }
        OCEAN_BTREE_CHECK_TRUE(parent, "parent is null.");
        if (parent && first_key)
        {
          // key是引用的
          old_key = parent->get_pair(static_cast<int16_t>(pos));
          if (key_allocator_ && old_key)
          {
            update_key_ref_count(handle, old_key->key_, first_key->key_);
          }
          ret = parent->set_pair(static_cast<int16_t>(pos), first_key->key_, reinterpret_cast<char*>(node),
                                 BtreeNode::UPDATE_KEY | BtreeNode::UPDATE_VALUE,
                                 key_compare_[param.tree_id]);
          OCEAN_BTREE_CHECK_TRUE(ERROR_CODE_OK == ret, "ret:%d", ret);
        }
      }


    }

    /**
     * 切换RootPointer并且回归以前的RootPointer
     */
    int32_t BtreeBase::callback_done(void *data)
    {
      BtreeWriteHandle *handle = reinterpret_cast<BtreeWriteHandle*>(data);
      OCEAN_BTREE_CHECK_TRUE(handle, "handle is null.");
      OCEAN_BTREE_CHECK_TRUE(handle->root_pointer_, "handle->root_pointer is null.");
      OCEAN_BTREE_CHECK_TRUE(node_allocator_, "node_allocator_ is null.");
      if (NULL == handle || NULL == handle->root_pointer_ || NULL == node_allocator_)
        return ERROR_CODE_FAIL;

      if (root_pointer_ && handle->root_pointer_->root_[0] &&
          root_pointer_->root_[0] == handle->root_pointer_->root_[0])
        return ERROR_CODE_FAIL;

      // 把node设成只读
      handle->root_pointer_->set_read_only(key_allocator_, true);
      handle->root_pointer_->clear_list_only();

      // 并且引用计数减1, 并切换
      BtreeRootPointer *old_proot = root_pointer_;
      root_pointer_ = handle->root_pointer_;
      // 把root_pointer_放在new_pointer后面
      if (&empty_root_pointer_ != old_proot)
      {
        handle->root_pointer_->add_tail(old_proot, root_tail_);
      }
      else
      {
        handle->root_pointer_->add_tail(NULL, root_tail_);
      }
      int64_t old_value = 0;
      int64_t new_value = 0;
      do
      {
        old_value = old_proot->ref_count_;
        new_value = ((~old_value) & RECYCLE_MASK) | (old_value & REFCOUNT_MASK);
      }
      while (!BtreeRootPointer::refcas(&(old_proot->ref_count_), old_value, new_value));

      // 回收RootPointer.
      BtreeRootPointer *proot = root_recycle_;
      BtreeRootPointer *proot_prev = NULL;
      BtreeRootPointer *proot_next = NULL;
      int64_t sequence_value = 0;
      if (proot == NULL) proot = root_tail_;

      // 最新节点不要回收, 只回收最后10个rootpointer
      int32_t cnt = MAX_RECYCLE_ROOT_COUNT;
      while(proot && proot != root_pointer_ && --cnt > 0)
      {
        proot_prev = proot->prev_pointer_;

        volatile int64_t f = proot->ref_count_;
        f &= REFCOUNT_MASK;
        // 没被引用了
        if (f == 0)
        {
          // 找refcount>0的RootPointer上seqence值, 只需向后找
          sequence_value = 0;
          proot_next = proot->next_pointer_;
          while(proot_next != NULL)
          {
            // 找到在使用的
            if ((proot_next->ref_count_ & REFCOUNT_MASK) > 0)
            {
              sequence_value = proot_next->sequence_;
              break;
            }
            // next
            proot_next = proot_next->next_pointer_;
          }

          // 全释放光了, 把rootpointer也要释放掉
          if (proot->release_node_list(sequence_value, key_allocator_) == 0)
          {
            // 从list中移出
            proot->clear_list_only();
            proot->remove_from_list(root_tail_);
            proot_freelist_.push(proot);
          }
        }

        // 上一个rootpointer
        proot = proot_prev;
      } // end while
      if (cnt > 0)
      {
        root_recycle_ = NULL;
      }
      return ERROR_CODE_OK;
    }

    /**
     * 把BTree所有的node放入node_list等待删除
     */
    void BtreeBase::remove_allnode(BtreeWriteHandle &handle, BtreeNode *root, int32_t depth, int32_t level)
    {
      OCEAN_BTREE_CHECK_TRUE(root, "root is null.");
      if (NULL == root)
        return;

      BtreeNode *node;
      BtreeKeyValuePair *pair = NULL;
      int32_t *addr = NULL;
      handle.prev_root_pointer_->add_node_list(root);
      if (level < depth)
      {
        int32_t size = root->get_size();
        for(int32_t i = 0; i < size; i++)
        {
          pair = root->get_pair(static_cast<int16_t>(i));
          if (pair)
          {
            if (key_allocator_)
            {
              addr = reinterpret_cast<int32_t*>(pair->key_);
              if (addr) (*addr) --;
            }
            node = reinterpret_cast<BtreeNode*>(pair->value_);
            remove_allnode(handle, node, depth, level + 1);
          }
        }
      }
    }

    /**
     * 把树清空
     */
    void BtreeBase::clear()
    {
      BtreeWriteHandle handle;
      get_write_handle(handle);
      BtreeRootPointer *proot = handle.root_pointer_;
      OCEAN_BTREE_CHECK_TRUE(proot, "root is null.");

      if (proot)
      {
        //fprintf(stderr, "clear:%ld, seq:%ld\n", proot->total_object_count_, proot->sequence_);
        for(int32_t i = 0; i < tree_count_; i++)
        {
          if (proot->root_[i])
            remove_allnode(handle, proot->root_[i], proot->tree_depth_[i], 1);
          proot->tree_depth_[i] = 0;
          proot->root_[i] = NULL;
        }
        proot->total_object_count_ = 0;
        proot->node_count_ = 0;
      }
      handle.end();
    }

    /**
     * 设置写锁
     */
    void BtreeBase::set_write_lock_enable(int32_t enable)
    {
      if (enable)
        wl_mutex_ = &mutex_;
      else
        wl_mutex_ = NULL;
    }

    /**
     * 把树直接销毁
     */
    void BtreeBase::destroy()
    {
      proot_freelist_.clear();
      if (node_allocator_) node_allocator_->destroy();
      if (key_allocator_) key_allocator_->destroy();
      empty_root_pointer_.sequence_ = 0;
      empty_root_pointer_.ref_count_ = 0;
      root_pointer_ = &empty_root_pointer_;
      root_tail_ = NULL;
      root_recycle_ = NULL;
      proot_sequence_ = 0;
    }
    /**
     * 分配出多少M内存
     */
    int32_t BtreeBase::get_alloc_memory()
    {
      int32_t ret = 0;
      if (node_allocator_) ret += node_allocator_->get_malloc_count();
      if (key_allocator_) ret += key_allocator_->get_malloc_count();
      return ret;
    }
    int32_t BtreeBase::get_freelist_size()
    {
      return proot_freelist_.size();
    }

#ifdef OCEAN_BASE_BTREE_DEBUG
    /**
     * 打印出来
     */
    void BtreeBase::print(BtreeBaseHandle *handle)
    {
      BtreeRootPointer *proot = root_pointer_;
      if (handle != NULL) proot = handle->root_pointer_;
      int32_t ret = 0;
      // printbtree
      BtreeNode *root = NULL;
      int32_t depth = 0;
      if (proot)
      {
        root = proot->root_[0];
        depth = proot->tree_depth_[0];
      }
      char buffer[100];
      for(int32_t i = 0; i < 10; i++)
      {
        buffer[i*10] = '0';
        buffer[i*10+1] = '\0';
      }

      if (root) ret = root->print(1, depth, buffer);
      int32_t need_free_node = 0;
      int32_t proot_count = 0;
      while(proot && proot != &empty_root_pointer_)
      {
        proot_count ++;
        need_free_node += proot->get_copy_node_count();
        proot = proot->next_pointer_;
      }

      fprintf(stderr, "ret = %d, root=%p\n", ret, root);
      if (node_allocator_)
      {
        fprintf(stderr, "node_allocator_-> use_count: %ld, malloc: %d\n",
                node_allocator_->get_use_count(), node_allocator_->get_malloc_count());
      }
      fprintf(stderr, "btree: total_object_count_: %ld, node_count_: %d\n",
              get_object_count(), get_node_count());
      fprintf(stderr, "proot_count: %d, need_free_node: %d\n",
              proot_count, need_free_node);
      if (key_allocator_)
      {
        fprintf(stderr, "key_allocator_: use_count: %ld, malloc: %d\n",
                key_allocator_->get_use_count(), key_allocator_->get_malloc_count());
      }
      if (handle != NULL)
      {
        //handle->param_.print();
        //handle->to_param_.print();
      }
    }
#endif

  } // end namespace common
} // end namespace oceanbase
