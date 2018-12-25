#include "ob_ps_store.h"

namespace oceanbase
{
  namespace sql
  {
    ObPsStore::ObPsStore()
    {

    }

    ObPsStore::~ObPsStore()
    {
      //free all ObPsStoreItem  && ObPsStoreItemValue && value->plan
    }

    int ObPsStore::init(int64_t hash_bucket)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = id_mgr_.init()))
      {
        TBSYS_LOG(ERROR, "ObSQLIdMgr in ObPsStroe init error, ret=%d", ret);
      }
      else
      {
        ret = ps_store_.create(hash_bucket);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "ObPsStore init error ret = %d", ret);
        }
        else
        {
          TBSYS_LOG(DEBUG, "ObPsStore inited success ret=%d", ret);
        }
      }
      return ret;
    }

    int ObPsStore::get(const ObString &sql, ObPsStoreItem *&item)
    {
      int ret = OB_SUCCESS;
      int64_t sql_id = OB_INVALID_ID;
      ret = id_mgr_.get_sql_id(sql, sql_id);
      if (OB_SUCCESS == ret)
      {
        ret = get(sql_id, item);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "get ObPsStoreItem failed, sql_id=%ld, ret=%d", sql_id, ret);
        }
        else
        {
          TBSYS_LOG(DEBUG, "Get ObPsStoreItem success sql=\"%.*s\", sql_id=%ld", sql.length(), sql.ptr(), sql_id);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "get sql id failed sql=\"%.*s\" ret = %d", sql.length(), sql.ptr(), ret);
      }
      return ret;
    }

    int ObPsStore::get(int64_t sql_id, ObPsStoreItem *&item)
    {
      int ret = OB_SUCCESS;
      int32_t retrycount = 0;
      int innerret = OB_SUCCESS;
      do
      {
        ObPsStoreItemReadAddRef raf;
        ret = get(sql_id, item, &raf);//如果get到了 atomic-inc++
        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(DEBUG, "Get ObPsStoreItem success sql_id=%ld", sql_id);
        }
        else if (OB_ENTRY_NOT_EXIST == ret)
        {
          item = ObPsStoreItem::alloc();
          if (NULL == item)
          {
            TBSYS_LOG(ERROR, "can not alloc mem for ObPsStoreItem");
            ret = OB_MEM_OVERFLOW;
          }
          else
          {
            TBSYS_LOG(DEBUG, "alloc item %p, physical plan is %p", item, &item->get_physical_plan());
            item->inc_ps_count();
            item->set_sql_id(sql_id); // store statement id
            item->rlock();
            ret = ps_store_.set(sql_id, item);
            if (hash::HASH_EXIST == ret)
            {
              item->unlock();
              TBSYS_LOG(DEBUG, "ObPsStoreItem already exist sql_id=%ld free item %p", sql_id, item);
              ObPsStoreItem::free(item);
              ret = OB_EAGAIN;
            }
            else if (hash::HASH_INSERT_SUCC == ret)
            {
              TBSYS_LOG(DEBUG, "insert new item to ps store success sql_id=%ld item is %p", sql_id, item);
              ret = OB_SUCCESS;
            }
            else
            {
              TBSYS_LOG(ERROR, "insert new item to ps store error sql_id=%ld, ret is %d", sql_id, ret);
              item->unlock();
              TBSYS_LOG(DEBUG, "insert new item to ObPsStore failed sql_id=%ld free item %p", sql_id, item);
              ObPsStoreItem::free(item);
            }
          }
        }
        else
        {
          if (retrycount % 100 == 0 || ret != innerret)
          {
            TBSYS_LOG(WARN, "ObPsStore get error sql_id=%ld, ret %d", sql_id, ret);
            innerret = ret;
          }
          retrycount++;
        }
      } while (OB_EAGAIN == ret);
      return ret;
    }

    int ObPsStore::get(int64_t sql_id, ObPsStoreItem *&item, ObPsStoreAtomicOp *op)
    {
      int ret = OB_SUCCESS;
      ret = ps_store_.atomic(sql_id, *op);
      if (hash::HASH_EXIST == ret)
      {
        if (OB_SUCCESS == (ret = op->get_rc())
            || OB_DEC_AND_LOCK == (ret = op->get_rc()))
        {
          item = op->get_item();
        }
        else
        {
          TBSYS_LOG(WARN, "can not do operate on ObPsStoreItem ret=%d, stmt_id=%lu", ret, sql_id);
        }
      }
      else if (hash::HASH_NOT_EXIST == ret)
      {
        ret = OB_ENTRY_NOT_EXIST;
      }
      else
      {
        TBSYS_LOG(WARN, "can not get ObPsStoreItem stmt_id=%lu", sql_id);
        ret = OB_ERR_UNEXPECTED;
      }
      return ret;
    }

    int ObPsStore::get_plan(int64_t sql_id, ObPsStoreItem *&item)
    {
      int ret = OB_SUCCESS;
      ObPsStoreItemRead ireader;
      ret = get(sql_id, item, &ireader);
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG, "Get ObPsStoreItem success sql_id is %ld", sql_id);
      }
      else
      {
        TBSYS_LOG(ERROR, "Get ObPsStoreItem failed sql_id is %ld, ret is %d", sql_id, ret);
        ret = OB_ERROR;
      }
      return ret;
    }

    int ObPsStore::remove_plan(int64_t sql_id)
    {
      int ret = OB_SUCCESS;
      ObPsStoreItem *item = NULL;
      ObPsStoreItemDecRef dr;
      ret = get(sql_id, item, &dr);//从hashmap中拿出来的时候就判断引用计数并且在为零时加上写锁
      if (OB_DEC_AND_LOCK == ret)
      {
        TBSYS_LOG(DEBUG, "ObPsStroreItem sql_id=%ld ref=%ld", sql_id, item->get_ps_count());
        if (0 == item->get_ps_count())
        {
          ret = ps_store_.erase(sql_id);
          if (hash::HASH_EXIST == ret)
          {
            TBSYS_LOG(DEBUG, "Erase ObPsStoreItem success sql_id=%lu", sql_id);
            item->unlock();
            TBSYS_LOG(DEBUG, "clear phy plan %p, free item %p", &item->get_physical_plan(), item);
            item->get_string_buf().reset();
            item->clear(); // free operator alloced by ob_tc_factory when plan assign
            TBSYS_LOG(DEBUG, "free item %p", item);
            ObPsStoreItem::free(item);
          }
          else
          {
            TBSYS_LOG(ERROR, "erase ObPsStoreItem failed sql_id=%lu, ret=%d", sql_id, ret);
            item->unlock();
          }
          ret = OB_SUCCESS;
        }
        else
        {
          TBSYS_LOG(ERROR, "never reach here");
          ret = OB_ERROR;
        }
      }
      else if (OB_SUCCESS == ret)//如果用户在ObPsStoreItem ref变成零 但是还没有从hashmap中删除这段时间 ref会是负数而且item随时可能被删除
      {
        TBSYS_LOG(DEBUG, "Remove Ps Plan succ, sql_id=%ld", sql_id);
      }
      else
      {
        TBSYS_LOG(ERROR, "Get ObPsStoreItem failed sql_id=%lu, ret=%d", sql_id, ret);
      }
      return ret;
    }

    // get query string by sql_id
    int ObPsStore::get_query(int64_t sql_id, ObString &query) const
    {
      return id_mgr_.get_sql(sql_id, query);
    }

  }
}
