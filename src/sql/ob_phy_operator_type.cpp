/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_phy_operator_type.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_phy_operator_type.h"
#include "common/ob_atomic.h"
#include "common/ob_define.h"
#include "tbsys.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

volatile static uint64_t PHY_OP_STAT[PHY_END];
namespace oceanbase
{
  namespace sql
  {

#define DEF_OP(type) \
        case type:\
        ret = # type;        \
        break

    const char* ob_phy_operator_type_str(ObPhyOperatorType type)
    {
      const char* ret = "UNKNOWN";
      switch(type)
      {
        DEF_OP(PHY_INVALID);
        DEF_OP(PHY_PROJECT);
        DEF_OP(PHY_LIMIT);
        DEF_OP(PHY_FILTER);
        DEF_OP(PHY_TABLET_SCAN);
        DEF_OP(PHY_TABLE_RPC_SCAN);
        DEF_OP(PHY_TABLE_MEM_SCAN);
        DEF_OP(PHY_RENAME);
        DEF_OP(PHY_TABLE_RENAME);
        DEF_OP(PHY_SORT);
        DEF_OP(PHY_MEM_SSTABLE_SCAN);
        DEF_OP(PHY_LOCK_FILTER);
        DEF_OP(PHY_INC_SCAN);
        DEF_OP(PHY_UPS_MODIFY);
        DEF_OP(PHY_INSERT_DB_SEM_FILTER);
        DEF_OP(PHY_MULTIPLE_SCAN_MERGE);
        DEF_OP(PHY_MULTIPLE_GET_MERGE);
        DEF_OP(PHY_VALUES);
        DEF_OP(PHY_EMPTY_ROW_FILTER);
        DEF_OP(PHY_EXPR_VALUES);
        DEF_OP(PHY_UPS_EXECUTOR);
        DEF_OP(PHY_TABLET_DIRECT_JOIN);
        DEF_OP(PHY_MERGE_JOIN);
        DEF_OP(PHY_MERGE_EXCEPT);
        DEF_OP(PHY_MERGE_INTERSECT);
        DEF_OP(PHY_MERGE_UNION);
        DEF_OP(PHY_ALTER_SYS_CNF);
        DEF_OP(PHY_ALTER_TABLE);
        DEF_OP(PHY_CREATE_TABLE);
        DEF_OP(PHY_DEALLOCATE);
        DEF_OP(PHY_DROP_TABLE);
        DEF_OP(PHY_TRUNCATE_TABLE); //add zhaoqiong [Truncate Table]:20160318
        DEF_OP(PHY_DUAL_TABLE_SCAN);
        DEF_OP(PHY_END_TRANS);
        DEF_OP(PHY_PRIV_EXECUTOR);
        DEF_OP(PHY_START_TRANS);
        DEF_OP(PHY_VARIABLE_SET);
        DEF_OP(PHY_TABLET_GET);
        DEF_OP(PHY_SSTABLE_GET);
        DEF_OP(PHY_SSTABLE_SCAN);
        DEF_OP(PHY_UPS_MULTI_GET);
        DEF_OP(PHY_UPS_SCAN);
        DEF_OP(PHY_RPC_SCAN);
        DEF_OP(PHY_DELETE);
        DEF_OP(PHY_EXECUTE);
        DEF_OP(PHY_EXPLAIN);
        DEF_OP(PHY_HASH_GROUP_BY);
        DEF_OP(PHY_MERGE_GROUP_BY);
        DEF_OP(PHY_INSERT);
        DEF_OP(PHY_MERGE_DISTINCT);
        DEF_OP(PHY_PREPARE);
        DEF_OP(PHY_SCALAR_AGGREGATE);
        DEF_OP(PHY_UPDATE);
        DEF_OP(PHY_TABLET_GET_FUSE);
        DEF_OP(PHY_TABLET_SCAN_FUSE);
        DEF_OP(PHY_ROW_ITER_ADAPTOR);
        DEF_OP(PHY_INC_GET_ITER);
        DEF_OP(PHY_ADD_PROJECT);
        DEF_OP(PHY_KILL_SESSION);
        DEF_OP(PHY_UPS_MODIFY_WITH_DML_TYPE);
	DEF_OP(PHY_MULTI_BIND);//add peiouya [IN_Subquery_prepare_BUGFIX] 20141013
	DEF_OP(PHY_INDEX_TRIGGER);//add liumz, [bugfix: UNKOWN phy_operator_type]20161124
	DEF_OP(PHY_INDEX_TRIGGER_UPD);//add wenghaixing [secondary index upd.3] 20141128
	DEF_OP(PHY_DELETE_INDEX);//add liumengzhan_delete_index
        DEF_OP(PHY_INDEX_TRIGGER_REP);// add by liyongfeng:20150105 [secondary index replace]
        //add lijianqiang [sequence insert] 20150420 :b
        DEF_OP(PHY_SEQUENCE_INSERT);
        //add 20150420:e
        //add lijianqiang [sequence delete] 20150514 :b
        DEF_OP(PHY_SEQUENCE_DELETE);
        //add 20150514:e
        //add lijianqiang [sequence update] 20150525 :b
        DEF_OP(PHY_SEQUENCE_UPDATE);
        //add 20150525:e
		//add liuzy [sequence select] 20150620 :b
		DEF_OP(PHY_SEQUENCE_SELECT);
		//add 20150620:e
	    DEF_OP(PHY_BIND_VALUES);//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20141027
        DEF_OP(PHY_IUD_LOOP_CONTROL);//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20141207
        DEF_OP(PHY_FILL_VALUES);//add gaojt [Delete_Update_Function] [JHOBv0.1] 20150918
        DEF_OP(PHY_UPDATE_DB_SEM_FILTER); // add lbzhong [Update rowkey] 20160418
        default:
          break;
      }
      return ret;
    }

    void ob_print_phy_operator_stat()
    {
      for (int32_t t = PHY_INVALID; t < PHY_END; ++t)
      {
        if (0 < PHY_OP_STAT[t])
        {
          TBSYS_LOG(INFO, "[PHY_OP_STAT] %s id=%d num=%lu",
                    ob_phy_operator_type_str(static_cast<ObPhyOperatorType>(t)), t, PHY_OP_STAT[t]);
        }
      }
    }

    void ob_inc_phy_operator_stat(ObPhyOperatorType type)
    {
      OB_ASSERT(type >= PHY_INVALID && type < PHY_END);
      TBSYS_LOG(DEBUG, "ob_inc_phy_operator_stat %s num=%lu",
          ob_phy_operator_type_str(type), PHY_OP_STAT[type]);
      atomic_inc(&PHY_OP_STAT[type]);
    }

    void ob_dec_phy_operator_stat(ObPhyOperatorType type)
    {
      OB_ASSERT(type >= PHY_INVALID && type < PHY_END);
      TBSYS_LOG(DEBUG, "ob_dec_phy_operator_stat %s num=%lu",
          ob_phy_operator_type_str(type), PHY_OP_STAT[type]);
      atomic_dec(&PHY_OP_STAT[type]);
    }

  }
}
