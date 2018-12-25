/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_ps_store.h is for what ...
 *
 * Version: ***: ob_ps_store.h  Tue Jul  2 11:02:35 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_PS_STORE_H_
#define OB_PS_STORE_H_

#include "common/hash/ob_hashmap.h"
#include "common/ob_string.h"
#include "common/ob_tc_factory.h"
#include "ob_ps_store_item.h"
#include "ob_sql_id_mgr.h"
#include "ob_ps_store_item_callback.h"

namespace oceanbase
{
  namespace tests
  {
    namespace sql
    {
      class TestObPsStore;
    }
  }
}
namespace oceanbase
{
  namespace sql
  {
    using namespace oceanbase::common;

    typedef int64_t ObPsStoreKey;


    class ObPsStore
    {
      public:
      typedef hash::ObHashMap<ObPsStoreKey, ObPsStoreItem*> SqlPlanMap;
      friend class oceanbase::tests::sql::TestObPsStore;
      public:
        ObPsStore();
        ~ObPsStore();

        int init(int64_t hash_bucket);

        /**
         * 获取ObPsStoreItem  rlock && inc ref count
         * 如果ObPsStore中不存在 则创建一个item加入到全局store中
         * called by do_com_prepare
         * @param[in]  key   sql
         * @param[out] item  PsStoreItem found
         *
         * @return int       return OB_SUCCESS if item found(or generated)
         *                   else return OB_ERROR
         */
        int get(const ObString &key, ObPsStoreItem *&item);


        //called by do_com_execute && do_com_prepare after plan create
        int get_plan(int64_t sql_id, ObPsStoreItem *&item);


        /**
         * 减引用计数 当引用计数为零时删除执行计划 do_com_close
         * @param sql_id  执行计划ObPsStore的key
         */
        int remove_plan(int64_t sql_id);

        int64_t allocate_new_id()
        {
          return id_mgr_.allocate_new_id();
        }

        ObSQLIdMgr* get_id_mgr()
        {
          return &id_mgr_;
        }

        int64_t count()
        {
          return ps_store_.size();
        }
        // get query string by sql_id
        int get_query(int64_t sql_id, ObString &query) const;
      private:
        DISALLOW_COPY_AND_ASSIGN(ObPsStore);
        int get(int64_t sql_id, ObPsStoreItem *&item);
        int get(int64_t sql_id, ObPsStoreItem *&item, ObPsStoreAtomicOp *op);

      private:
        ObSQLIdMgr id_mgr_;
        SqlPlanMap ps_store_;
    };
  }
}

#endif
