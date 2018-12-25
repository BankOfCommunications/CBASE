////===================================================================
 //
 // ob_lrucache.cpp / hash / common / Oceanbase
 //
 // Copyright (C) 2010, 2013 Taobao.com, Inc.
 //
 // Created on 2010-08-04 by Yubai (yubai.lk@taobao.com)
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

#ifndef  OCEANBASE_COMMON_LRUCACHE_H_
#define  OCEANBASE_COMMON_LRUCACHE_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "ob_define.h"
#include "dlist.h"
#include "hash/ob_hashmap.h"
#include "hash/ob_hashutils.h"

namespace oceanbase
{
  namespace common
  {
    using namespace hash;

    enum
    {
      LC_RELEASE_IMMED = 0,
      LC_RELEASE_DELAY = 1,
    };

    template <class _key, class _value,
             class DefendMode = MultiWriteDefendMode>
    class ObLruCache
    {
      class LruNode : public DLink
      {
        public:
          LruNode() : need_release_(false), time_stamp_(0), ref_cnt_(0), data_(NULL)
          {
            // do nothing
          };
          ~LruNode()
          {
            // do nothing
          };
        public:
          void init(const _key &key, _value *data)
          {
            time_stamp_ = get_cur_microseconds_time();
            key_ = key;
            data_ = data;
          };
          int64_t get_time_stamp() const
          {
            return time_stamp_;
          };
          const _key &get_key() const
          {
            return key_;
          };
          _value *get_data() const
          {
            return data_;
          };
          int release()
          {
            int ret = OB_SUCCESS;
            if (NULL != data_)
            {
              if (0 != ref_cnt_)
              {
                ret = OB_ERROR;
              }
              else
              {
                data_->release();
                data_ = NULL;
              }
            }
            return ret;
          };
          void set_need_release()
          {
            need_release_ = true;
          };
          bool get_need_release() const
          {
            return need_release_;
          };
          void rdlock()
          {
            ref_cnt_++;
          };
          int unlock()
          {
            int ret = OB_SUCCESS;
            if (0 >= ref_cnt_)
            {
              ret = OB_ERROR;
            }
            else
            {
              ref_cnt_--;
            }
            return ret;
          };
        private:
          bool need_release_;
          int64_t time_stamp_;
          int64_t ref_cnt_;
          _key key_;
          _value *data_;
      };
      typedef LruNode lrunode_t;
      typedef SimpleAllocer<lrunode_t, 1024, NoPthreadDefendMode> allocer_t;
      typedef ObHashMap<_key, lrunode_t*> hashmap_t;
      typedef typename ObHashMap<_key, lrunode_t*>::iterator hashmap_iterator_t;
      typedef typename DefendMode::writelocker mutexlocker;
      typedef typename DefendMode::lock_type lock_type;
      typedef typename DefendMode::lock_initer lock_initer;
      public:
        ObLruCache() : inited_(false), is_traversing_(false), max_cache_num_(0),
          min_gc_timeo_(0), cur_node_(NULL), end_node_(NULL)
        {
          lock_initer initer(lock_);
        };
        ~ObLruCache()
        {
        };
      public:
        int init(int64_t max_cache_num, int64_t min_gc_timeo)
        {
          int ret = OB_SUCCESS;
          mutexlocker locker(lock_);
          if (inited_)
          {
            TBSYS_LOG(WARN, "have already inited");
            ret = OB_ERROR;
          }
          else if (0 >= max_cache_num || 0 >= min_gc_timeo)
          {
            TBSYS_LOG(WARN, "invalid param max_cache_num=%ld min_gc_timeo=%ld", max_cache_num, min_gc_timeo);
            ret = OB_ERROR;
          }
          else if (0 != hashmap_.create(max_cache_num))
          {
            TBSYS_LOG(WARN, "create hashmap fail hash_item_num=%lu", max_cache_num);
            ret = OB_ERROR;
          }
          else
          {
            max_cache_num_ = max_cache_num;
            min_gc_timeo_ = min_gc_timeo;
            cur_node_ = static_cast<lrunode_t*>(lru_list_.get_header());
            end_node_ = static_cast<lrunode_t*>(lru_list_.get_header());
            inited_ = true;
          }
          return ret;
        };
        int destroy()
        {
          int ret = OB_SUCCESS;
          if (!inited_)
          {
            TBSYS_LOG(WARN, "have not inited");
          }
          else if (OB_SUCCESS == (ret = clear()))
          {
            mutexlocker locker(lock_);
            hashmap_.destroy();
            allocer_.gc();  //free memory of node
            inited_ = false;
            is_traversing_ = false;
          }
          return ret;
        };
      public:
        // 根据ID获取缓存的数据
        // @param [in] cache数据的ID
        // @return 非NULL cache的数据指针
        //         NULL 数据不存在
        _value *get(const _key &cache_key)
        {
          _value *ret = NULL;
          mutexlocker locker(lock_);
          lrunode_t *lrunode = NULL;
          if (!inited_)
          {
            HASH_WRITE_LOG(HASH_WARNING, "have not inited");
          }
          else if (NULL == (lrunode = get_lrunode_(cache_key)))
          {
            // do nothing
          }
          else
          {
            lrunode->rdlock();
            if (!is_traversing_)
            {
              //when traversing lrulist, not allow to adjust lrulist
              lru_list_.move_to_last(lrunode);
            }
            ret = lrunode->get_data();
          }
          return ret;
        };

        // get之后归还给cache,表示可以被lru回收或free掉,与get一一对应
        // @param [in] cache数据的ID
        // @return OB_SUCCESS 成功
        //         OB_ERROR 失败
        int revert(const _key &cache_key)
        {
          int ret = OB_SUCCESS;
          mutexlocker locker(lock_);
          lrunode_t *lrunode = NULL;
          if (!inited_)
          {
            HASH_WRITE_LOG(HASH_WARNING, "have not inited");
            ret = OB_ERROR;
          }
          else if (NULL == (lrunode = get_lrunode_(cache_key)))
          {
            ret = OB_ERROR;
          }
          else if (OB_SUCCESS != lrunode->unlock())
          {
            HASH_WRITE_LOG(HASH_WARNING, "unlock lrunode fail lrunode=%p", lrunode);
            ret = OB_ERROR;
          }
          else
          {
            if (lrunode->get_need_release()
                && OB_SUCCESS == lrunode->release())
            {
              erase_lrunode_(lrunode);
            }
          }
          return ret;
        };

        // 释放指定的cache数据
        // 如果cache没有命中直接返回成功
        // @param [in] cache数据的ID
        // @param [in] flag LC_RELEASE_IMMED 如果删除失败则返回错误
        //                  LC_RELEASE_DELAY 如果删除失败则标记等revert后自动删除
        // @return OB_SUCCESS 成功
        int free(const _key &cache_key, int flag)
        {
          int ret = OB_SUCCESS;
          mutexlocker locker(lock_);
          lrunode_t *lrunode = NULL;
          lrunode_t* head = static_cast<lrunode_t*>(lru_list_.get_header());
          if (!inited_)
          {
            HASH_WRITE_LOG(HASH_WARNING, "have not inited");
            ret = OB_ERROR;
          }
          else if (NULL == (lrunode = get_lrunode_(cache_key)))
          {
            // do nothing
          }
          else
          {
            if (lrunode == end_node_ && NULL != end_node_ && end_node_ != head)
            {
              /**
               * when traversing, the end node is not in memcache, and app get
               * the end node to this moment, app will call this function to
               * free the node, so we need unlock the old end node, and find a
               * new end code instead
               */
              end_node_->unlock();
              end_node_ = static_cast<lrunode_t*>(end_node_->get_prev());
              if (NULL != end_node_ && end_node_ != head)
              {
                end_node_->rdlock();
              }
            }
            if (OB_SUCCESS != lrunode->release())
            {
              if (LC_RELEASE_DELAY == flag)
              {
                lrunode->set_need_release();
              }
              /**
               * return OB_SUCCESS, because maybe more than one thread get the
               * same node, for this case, it will fail to release node
               */
            }
            else
            {
              erase_lrunode_(lrunode);
            }
          }
          return ret;
        };

        // 设置cache数据
        // @param [in] cache数据的ID
        // @param [in] cache的数据指针
        // @return OB_SUCCESS 设置成功
        //         OB_ERROR 失败
        int set(const _key &cache_key, _value *cache_value)
        {
          int ret = OB_SUCCESS;
          mutexlocker locker(lock_);
          lrunode_t *lrunode = NULL;
          if (!inited_)
          {
            HASH_WRITE_LOG(HASH_WARNING, "have not inited");
            ret = OB_ERROR;
          }
          else if (NULL == cache_value)
          {
            HASH_WRITE_LOG(HASH_WARNING, "invalid param null pointer");
            ret = OB_ERROR;
          }
          else if (HASH_NOT_EXIST != hashmap_.get(cache_key, lrunode))
          {
            HASH_WRITE_LOG(HASH_NOTICE, "cache_data exist cache_value=%p", cache_value);
            ret = OB_ERROR;
          }
          else
          {
            if (max_cache_num_ <= hashmap_.size())
            {
              int64_t max_gc_num = 1;
              gc_(min_gc_timeo_, max_gc_num, LC_RELEASE_IMMED);
            }
            if (max_cache_num_ <= hashmap_.size())
            {
              HASH_WRITE_LOG(HASH_WARNING, "no more cache item num");
              ret = OB_ERROR;
            }
            else if (NULL == (lrunode = allocer_.alloc()))
            {
              HASH_WRITE_LOG(HASH_WARNING, "allocate lrunode fail cache_value=%p", cache_value);
              ret = OB_ERROR;
            }
            else if (!lru_list_.add_last(lrunode))
            {
              HASH_WRITE_LOG(HASH_WARNING, "add lrunode to lrulist fail cache_value=%p", cache_value);
              allocer_.free(lrunode);
              ret = OB_ERROR;
            }
            else if (HASH_INSERT_SUCC != hashmap_.set(cache_key, lrunode))
            {
              HASH_WRITE_LOG(HASH_WARNING, "add lrunode to hashmap fail cache_value=%p", cache_value);
              lru_list_.remove(lrunode);
              allocer_.free(lrunode);
              ret = OB_ERROR;
            }
            else
            {
              lrunode->init(cache_key, cache_value);
            }
          }
          return ret;
        };

        // 发起一次cache淘汰策略
        // 将调用Obj的reset方法
        void gc(int64_t max_gc_num = 1)
        {
          mutexlocker locker(lock_);
          if (!inited_)
          {
            HASH_WRITE_LOG(HASH_WARNING, "have not inited");
          }
          else
          {
            gc_(min_gc_timeo_, max_gc_num, LC_RELEASE_IMMED);
          }
        };

        // 清空缓存区,即对所有对象执行free
        // 如果有对象不能被free则返回失败
        // @return OB_SUCCESS 成功
        //         OB_ERROR 失败
        int clear()
        {
          int ret = OB_SUCCESS;
          mutexlocker locker(lock_);
          if (!inited_)
          {
            HASH_WRITE_LOG(HASH_WARNING, "have not inited");
            ret = OB_ERROR;
          }
          else
          {
            int64_t min_timeo = 0;
            int64_t max_gc_num = hashmap_.size();
            lrunode_t* head = static_cast<lrunode_t*>(lru_list_.get_header());
            if (NULL != cur_node_ && cur_node_ != head)
            {
              //when doing traversing, and user kill process, unlock current node
              cur_node_->unlock();
            }
            if (NULL != end_node_ && end_node_ != head)
            {
              //when doing traversing, and user kill process, unlock end node
              end_node_->unlock();
            }
            gc_(min_timeo, max_gc_num, LC_RELEASE_DELAY);
            if (0 != hashmap_.size())
            {
              HASH_WRITE_LOG(HASH_WARNING, "have node still be used can not release");
              ret = OB_ERROR;
            }
          }
          return ret;
        };

        int64_t size() const
        {
          return hashmap_.size();
        };

        int set_max_cache_num(const int64_t max_cache_num)
        {
          int ret = OB_SUCCESS;
          mutexlocker locker(lock_);
          if (!inited_)
          {
            HASH_WRITE_LOG(HASH_WARNING, "have not inited");
            ret = OB_ERROR;
          }
          else if (0 >= max_cache_num)
          {
            HASH_WRITE_LOG(HASH_WARNING, "invalid param max_cache_num=%ld", max_cache_num);
            ret = OB_ERROR;
          }
          else
          {
            max_cache_num_ = max_cache_num;
          }
          return ret;
        };

        int set_min_gc_timeo(const int64_t min_gc_timeo)
        {
          int ret = OB_SUCCESS;
          mutexlocker locker(lock_);
          if (!inited_)
          {
            HASH_WRITE_LOG(HASH_WARNING, "have not inited");
            ret = OB_ERROR;
          }
          else if (0 >= min_gc_timeo)
          {
            HASH_WRITE_LOG(HASH_WARNING, "invalid param min_gc_timeo=%ld", min_gc_timeo);
            ret = OB_ERROR;
          }
          else
          {
            min_gc_timeo_ = min_gc_timeo;
          }
          return ret;
        };

        //reverse traverse lru list
        _value *get_next(_key &cache_key)
        {
          _value *ret     = NULL;
          lrunode_t *iter = NULL;
          lrunode_t *head = static_cast<lrunode_t*>(lru_list_.get_header());
          mutexlocker locker(lock_);

          if (!inited_)
          {
            HASH_WRITE_LOG(HASH_WARNING, "have not inited");
          }
          else if (NULL != cur_node_)
          {
            //release the previous node first
            if (NULL != cur_node_ && cur_node_ != head)
            {
              cur_node_->unlock();
            }
            else
            {
              //set the end node to traverse, only set once for traversing once
              end_node_ = static_cast<lrunode_t*>(head->get_prev());
              if (NULL != end_node_ && end_node_ != head)
              {
                /**
                 * rdlock twice, add the ref_cnt twice, avoid erase end node
                 * when doing traversing
                 */
                end_node_->rdlock();
              }
            }
            iter = static_cast<lrunode_t*>(cur_node_->get_next());
            if (NULL == iter || iter == head
                || (end_node_ != head && iter == end_node_->get_next()))
            {
              //reach the tail of lru list or reach the end node
              ret = NULL;
              cur_node_ = head;
              is_traversing_ = false;
              //release the end node
              if (NULL != end_node_ && end_node_ != head)
              {
                end_node_->unlock();
                end_node_ = head;
              }
            }
            else
            {
              cur_node_ = iter;
              /**
               * rdlock twice, add the ref_cnt twice, avoid erase this node
               * when doing traversing
               */
              cur_node_->rdlock();
              cur_node_->rdlock();
              cache_key = cur_node_->get_key();
              ret = cur_node_->get_data();
              is_traversing_ = true;
            }
          }

          return ret;
        }

      private:
        void erase_lrunode_(lrunode_t *lrunode)
        {
          hashmap_.erase(lrunode->get_key());
          lru_list_.remove(lrunode);
          allocer_.free(lrunode);
        };

        lrunode_t *get_lrunode_(const _key &cache_key)
        {
          lrunode_t *lrunode = NULL;
          hashmap_.get(cache_key, lrunode);
          return lrunode;
        };

        void gc_(int64_t min_timeo, int64_t max_gc_num, int flag)
        {
          lrunode_t *head = static_cast<lrunode_t*>(lru_list_.get_header());
          lrunode_t *iter = static_cast<lrunode_t*>(lru_list_.get_first());
          int64_t counter = 0;
          while (iter != head
                && NULL != iter
                && counter < max_gc_num)
          {
            lrunode_t *tmp = (lrunode_t*)iter->get_next();
            if (iter->get_time_stamp() + min_timeo <= get_cur_microseconds_time())
            {
              if (OB_SUCCESS == iter->release())
              {
                erase_lrunode_(iter);
                counter++;
              }
              else
              {
                if (LC_RELEASE_DELAY == flag)
                {
                  iter->set_need_release();
                }
              }
            }
            iter = tmp;
          }
        };

      private:
        bool inited_;
        bool is_traversing_;
        int64_t max_cache_num_;
        int64_t min_gc_timeo_;
        lock_type lock_;
        allocer_t allocer_;
        DList lru_list_;
        hashmap_t hashmap_;
        lrunode_t* cur_node_;
        lrunode_t* end_node_;
    };
  }
}

#endif //OCEANBASE_COMMON_LRUCACHE_H_
