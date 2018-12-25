/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#ifndef __OB_UPDATESERVER_OB_UPS_PHY_OPERATOR_FACTORY_H__
#define __OB_UPDATESERVER_OB_UPS_PHY_OPERATOR_FACTORY_H__
#include "ob_ups_table_mgr.h"
#include "ob_sessionctx_factory.h"
#include "sql/ob_phy_operator_factory.h"

namespace oceanbase
{
  namespace updateserver
  {
    class ObUpsPhyOperatorFactory: public sql::ObPhyOperatorFactory
    {
      public:
        ObUpsPhyOperatorFactory(): session_ctx_(NULL), table_mgr_(NULL)
        {}
        virtual ~ObUpsPhyOperatorFactory(){}
        virtual sql::ObPhyOperator *get_one(sql::ObPhyOperatorType phy_operator_type, common::ModuleArena &allocator);
        virtual void release_one(sql::ObPhyOperator *opt);
        void set_session_ctx(RWSessionCtx* session_ctx) { session_ctx_ = session_ctx; }
        void set_table_mgr(ObIUpsTableMgr* table_mgr) { table_mgr_ = table_mgr; }
      private:
        RWSessionCtx* session_ctx_;
        ObIUpsTableMgr* table_mgr_;
    };
  }; // end namespace updateserver
}; // end namespace oceanbase

#endif /* __OB_UPDATESERVER_OB_UPS_PHY_OPERATOR_FACTORY_H__ */
