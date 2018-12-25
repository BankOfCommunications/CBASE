/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_ps_store_item_callback.h is for what ...
 *
 * Version: ***: ob_ps_store_item_callback.h  Fri Jul  5 10:23:54 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_PS_STORE_ITEM_CALLBACK_H_
#define OB_PS_STORE_ITEM_CALLBACK_H_

#include "common/ob_define.h"
#include "ob_ps_store.h"
#include "common/hash/ob_hashmap.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace sql
  {
    typedef int64_t ObPsStoreKey;
    class ObPsStoreAtomicOp
    {
      public:
        ObPsStoreAtomicOp():rc_(common::OB_SUCCESS), item_(NULL)
        {
        }
        virtual ~ObPsStoreAtomicOp(){}

        virtual void operator()(common::hash::HashMapPair<ObPsStoreKey, ObPsStoreItem*> entry){UNUSED(entry);};

        int get_rc() const
        {
          return rc_;
        }

        ObPsStoreItem* get_item() const
        {
          return item_;
        }
      protected:
        int rc_;
        ObPsStoreItem *item_;
    };

    class ObPsStoreItemRead:public ObPsStoreAtomicOp
    {
      public:
        ObPsStoreItemRead()
        {
        }
        virtual ~ObPsStoreItemRead()
        {
        }

        virtual void operator()(common::hash::HashMapPair<ObPsStoreKey, ObPsStoreItem*> entry)
        {
          do
          {
            rc_ = entry.second->try_rlock();
            item_ = entry.second;
          } while (EBUSY == rc_);
        }
    };

    class ObPsStoreItemReadAddRef : public ObPsStoreAtomicOp
    {
      public:
        ObPsStoreItemReadAddRef()
        {
        }

        virtual ~ObPsStoreItemReadAddRef()
        {

        }

        virtual void operator()(common::hash::HashMapPair<ObPsStoreKey, ObPsStoreItem*> entry)
        {
          rc_ = entry.second->try_rlock();
          //rc_ = entry.second->rlock();
          if (common::OB_SUCCESS == rc_)
          {
            entry.second->inc_ps_count();
            item_ = entry.second;
            TBSYS_LOG(DEBUG, "rlock succ ps ref count is %ld item is %p", item_->get_ps_count(), entry.second);
          }
          else if (EBUSY == rc_)
          {
            rc_ = common::OB_EAGAIN;
          }
          else
          {
            TBSYS_LOG(ERROR, "ps debug rlock failed ret=%d item is %p stmtid=%lu", rc_, entry.second, entry.second->get_sql_id());
            rc_ = common::OB_ERROR;
          }
        }
    };

    class ObPsStoreItemDecRef : public ObPsStoreAtomicOp
    {
      public:
        ObPsStoreItemDecRef()
        {
        }

        virtual ~ObPsStoreItemDecRef()
        {

        }

        virtual void operator()(common::hash::HashMapPair<ObPsStoreKey, ObPsStoreItem*> entry)
        {
          int64_t pscount = entry.second->dec_ps_count();
          TBSYS_LOG(DEBUG, "item is %p stmtid=%ld, ps ref count is %ld", entry.second, entry.second->get_sql_id(),pscount);
          if (0 == pscount)
          {
            if (0 == entry.second->wlock())
            {
              rc_ = OB_DEC_AND_LOCK;
              item_ = entry.second;
            }
          }
          else
          {
            rc_ = common::OB_SUCCESS;
            item_ = entry.second;
          }
          TBSYS_LOG(DEBUG, "ps ref count is %ld", pscount);
        }
    };
  }
}
#endif
