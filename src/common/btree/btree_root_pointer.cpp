#include "btree_define.h"
#include "btree_alloc.h"
#include "btree_node.h"
#include "btree_root_pointer.h"

namespace oceanbase
{
  namespace common
  {
    /**
     * 初始化
     */
    void BtreeRootPointer::init(BtreeAlloc *allocator)
    {
      node_allocator_ = allocator;
      // 用于rootpointer list
      next_pointer_ = NULL;
      prev_pointer_ = NULL;
      // 统计
      total_object_count_ = 0;
      node_count_ = 0;
      // 初始化list
      node_list_.init(sizeof(BtreeRootPointer));
      new_node_create_ = 0;
    }

    /**
     * 增加到copy_node_中
     */
    void BtreeRootPointer::add_node_list(BtreeNode *node)
    {
      node_list_.add_node_list(node_allocator_, 1, node);
    }

    /**
     * 把proot加在此rootpointer后面
     */
    void BtreeRootPointer::add_tail(BtreeRootPointer *proot, BtreeRootPointer* &tail)
    {
      next_pointer_ = proot;
      if (proot)
      {
        proot->prev_pointer_ = this;
      }
      if (tail == NULL)
      {
        tail = this;
      }
    }

    /**
     * 从list中把自己移出来
     */
    void BtreeRootPointer::remove_from_list(BtreeRootPointer* &tail)
    {
      if (prev_pointer_)
      {
        prev_pointer_->next_pointer_ = next_pointer_;
      }
      if (next_pointer_)
      {
        next_pointer_->prev_pointer_ = prev_pointer_;
      }
      else
      {
        tail = prev_pointer_;
      }
      next_pointer_ = NULL;
      prev_pointer_ = NULL;
    }

    /**
     * 把所有node设成只读, 并且把is_deleted的node释放掉
     */
    void BtreeRootPointer::set_read_only(BtreeAlloc *key_allocator, bool remove)
    {
      BtreeNode *node = NULL;
      BtreeNodeList list;

      node_list_.node_list_first(list);

      // 取出每个node
      while((node = reinterpret_cast<BtreeNode*>(node_list_.node_list_next(list))) != NULL)
      {
        if (node->is_deleted())
        {
          if (remove && node_allocator_)
          {
            if (key_allocator) node->dec_key_refcount(key_allocator);
            node_allocator_->release(reinterpret_cast<char*>(node));
          }
        }
        else
        {
          node->set_read_only(1);
        }
      }
      OCEAN_BTREE_CHECK_TRUE(list.pos == list.node_count,
                             "cnt: %d %d", list.pos, list.node_count);
      new_node_create_ = 0;
    }

    /**
     * 清空copy_list, 不对list上node进行释放, CLEAR_LIST_ONLY
     */
    void BtreeRootPointer::clear_list_only()
    {
      OCEAN_BTREE_CHECK_TRUE(node_allocator_, "node_allocator is null, this=%p", this);
      node_list_.clear_list_only(node_allocator_);
    }

    /**
     * 释放掉自己
     */
    void BtreeRootPointer::destroy()
    {
      node_list_.clear_list_only(node_allocator_);
      BtreeAlloc *na = node_allocator_;
      if (na) na->release(reinterpret_cast<char*>(this));
    }

    /**
     * 释放所有copy_node_
     * @param sequence == 0 释放所有的node
     */
    int32_t BtreeRootPointer::release_node_list(int64_t sequence, BtreeAlloc *key_has_ref)
    {
      BtreeNode *node = NULL;
      BtreeNodeList list;
      int32_t ret = 0;

      node_list_.node_list_first(list);
      node_list_.node_list_reset();
      // 取出每个node
      while((node = reinterpret_cast<BtreeNode*>(node_list_.node_list_next(list))) != NULL)
      {
        if (sequence == 0 || node->get_sequence() > sequence)
        {
          if (key_has_ref)
          {
            node->dec_key_refcount(key_has_ref);
          }
          if (node_allocator_)
          {
            node_allocator_->release(reinterpret_cast<char*>(node));
          }
#ifdef OCEAN_BTREE_CHECK
          node_list_.delete_bf(node);
#endif
        }
        else
        {
          node_list_.add_node_list(node_allocator_, 1, node);
          ret ++;
        }
      }
      OCEAN_BTREE_CHECK_TRUE(list.pos == list.node_count,
                             "cnt: %d %d", list.pos, list.node_count);
      OCEAN_BTREE_CHECK_TRUE(ret == node_list_.get_copy_node_count(),
                             "ret: %d %d", ret, node_list_.get_copy_node_count());
      node_list_.shrink_list(node_allocator_);

      return ret;
    }

    /**
     * new出一个新节点
     */
    BtreeNode *BtreeRootPointer::new_node(int32_t count)
    {
      BtreeNode *node = NULL;
      if (node_allocator_)
      {
        node = reinterpret_cast<BtreeNode*>(node_allocator_->alloc());
        OCEAN_BTREE_CHECK_TRUE(node, "node is null.");
      }
      if (node)
      {
        node->init(static_cast<int16_t>(count));
        node->set_sequence(sequence_);
        OCEAN_BTREE_CHECK_TRUE(0 == node->is_deleted(), "deleted %d", node->is_deleted());
        node_list_.add_node_list(node_allocator_, 1, node);
        new_node_create_ = 1;
      }
      return node;
    }

    /**
     * copy出一个节点来
     */
    BtreeNode *BtreeRootPointer::copy_node(BtreeNode *node, BtreeAlloc *key_has_ref)
    {
      OCEAN_BTREE_CHECK_TRUE(node, "node is null.");
      BtreeNode *new_node = NULL;
      if (node)
      {
        if (node_allocator_)
        {
          new_node = reinterpret_cast<BtreeNode*>(node_allocator_->alloc());
          OCEAN_BTREE_CHECK_TRUE(new_node, "node is null.");
        }
        if (new_node)
        {
          memcpy(new_node, node, node_allocator_->get_alloc_size());
          new_node->set_read_only(0);
          if (key_has_ref) new_node->inc_key_refcount();
          new_node->set_sequence(sequence_);
          OCEAN_BTREE_CHECK_TRUE(0 == node->is_deleted(), "deleted %d", node->is_deleted());
          node_list_.add_node_list(node_allocator_, 1, new_node);
          new_node_create_ = 1;
        }
      }
      return new_node;
    }

    unsigned char BtreeRootPointer::refcas(volatile int64_t *ref, int64_t old_value, int64_t new_value)
    {
      unsigned char res = 0;
      __asm__ volatile (
        "    lock; cmpxchgq  %3, %1;   "
        "    sete      %0;       "
        : "=a" (res) : "m" ((*ref)), "a" (old_value), "r" (new_value) : "cc", "memory");
      return res;
    }

    /**
     * copy node数量
     */
    int32_t BtreeRootPointer::get_copy_node_count()
    {
      return node_list_.get_copy_node_count();
    }

    /**
     * 得到分配器
     */
    BtreeAlloc *BtreeRootPointer::get_node_allocator()
    {
      return node_allocator_;
    }

    /**
       * 需要设成only
       */
    int8_t BtreeRootPointer::need_set_only()
    {
      return new_node_create_;
    }

    /**
       * 得到初始值
       */
    void BtreeRootPointer::node_list_first(BtreeNodeList &list)
    {
      node_list_.node_list_first(list);
    }

    /**
     * 得到node_list中的next
     */
    BtreeNode *BtreeRootPointer::node_list_next(BtreeNodeList &list)
    {
      return reinterpret_cast<BtreeNode*>(node_list_.node_list_next(list));
    }

    /**
     * reset copy list
     */
    void BtreeRootPointer::node_list_reset()
    {
      node_list_.node_list_reset();
    }



    /*
     * 构造函数
     */
    BtreeRootPointerList::BtreeRootPointerList()
    {
      _head = NULL;
      _tail = NULL;
      _size = 0;
    }
    /*
     * 析构函数
     */
    BtreeRootPointerList::~BtreeRootPointerList()
    {
      clear();
    }

    /*
     * 出链表
     */
    BtreeRootPointer *BtreeRootPointerList::pop()
    {
      if (_head == NULL)
      {
        return NULL;
      }
      BtreeRootPointer *proot = _head;
      _head = _head->next_pointer_;
      if (_head == NULL)
      {
        _tail = NULL;
      }
      _size --;
      return proot;
    }

    /*
     * 清空
     */
    void BtreeRootPointerList::clear()
    {
      if (_head == NULL)
      {
        return;
      }
      while (_head != NULL)
      {
        BtreeRootPointer *proot = _head;
        _head = proot->next_pointer_;
        proot->destroy();
      }
      _head = _tail = NULL;
      _size = 0;
    }

    /*
     * 入链表
     */
    void BtreeRootPointerList::push(BtreeRootPointer *proot)
    {
      if (proot == NULL)
      {
        return;
      }
      proot->next_pointer_ = NULL;
      if (_tail == NULL)
      {
        _head = proot;
      }
      else
      {
        _tail->next_pointer_ = proot;
      }
      _tail = proot;
      _size++;
    }

    /*
     * 长度
     */
    int BtreeRootPointerList::size()
    {
      return _size;
    }

    /*
     * 是否为空
     */
    bool BtreeRootPointerList::empty()
    {
      return (_size == 0);
    }

  } // end namespace common
} // end namespace oceanbase
