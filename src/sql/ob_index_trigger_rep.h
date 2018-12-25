/*
 * =====================================================================================
 *
 *       Filename:  ob_index_trigger_rep.h
 *
 *    Description:  index trigger used for replace 
 *
 *        Version:  1.0
 *        Created:  201501
 *       Revision:  none
 *       Compiler:  gcc/g++
 *
 *        Authors:  Yongfeng LI <minCrazy@gmail.com>
 *   Organization:  bankcomm&ecnu
 *
 * =====================================================================================
 */
#ifndef __OB_INDEX_TRIGGER_REP_H__
#define __OB_INDEX_TRIGGER_REP_H__

#include "ob_single_child_phy_operator.h"
#include "sql/ob_ups_modify.h"
#include "common/ob_iterator.h"
#include "common/ob_iterator_adaptor.h"
#include "updateserver/ob_sessionctx_factory.h"
#include "updateserver/ob_ups_table_mgr.h"
#include "updateserver/ob_ups_utils.h"
#include "ob_expr_values.h"
#include "ob_multiple_get_merge.h"

namespace oceanbase
{
  namespace sql
  {
    class ObIndexTriggerRep : public ObSingleChildPhyOperator
    {
    public:
      ObIndexTriggerRep();
      virtual ~ObIndexTriggerRep();
      virtual void reset();
      virtual void reuse();
      virtual int open();
      virtual int close();

      int handle_trigger(const common::ObSchemaManagerV2 *schema_mgr, updateserver::ObIUpsTableMgr* host,
                         updateserver::RWSessionCtx &session_ctx, ObRowStore* store);
      int handle_trigger_one_index(const int64_t &index_idx, const common::ObSchemaManagerV2 *schema_mgr,
                                   updateserver::ObIUpsTableMgr* host, updateserver::RWSessionCtx &session_ctx
                                   , ObRowStore* store);
      
      virtual int get_next_row(const common::ObRow *&row);
      void reset_iterator();
      virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
      virtual int64_t to_string(char* buf, const int64_t buf_len) const;

      void set_index_num(const int64_t &num);
      void set_main_tid(const uint64_t &tid);
      int add_index_del_row_desc(int64_t idx, common::ObRowDesc desc);
      int add_index_ins_row_desc(int64_t idx, common::ObRowDesc desc);

      void set_input_values(uint64_t subquery) { replace_values_subquery_ = subquery; }
      void set_input_index_values(uint64_t subquery){replace_index_values_subquery_ = subquery; }
      enum ObPhyOperatorType get_type() const { return PHY_INDEX_TRIGGER_REP; }
      bool is_first_replace();
      //add wenghaixing [secondary index replace.fix_replace_bug]20150422
      inline void set_data_max_cid(uint64_t cid){data_max_cid_ = cid;}
      inline uint64_t get_data_max_cid(){return data_max_cid_;}
      //add e
      //add wenghaixing [secondary index static_index_build.consistency]20150424
      inline virtual int64_t get_index_num()const {return index_num_;}
      inline void set_main_desc(ObRowDesc &desc){main_row_desc_ = desc;}
      inline void get_main_desc(ObRowDesc &desc){desc = main_row_desc_;}
      //add e
      DECLARE_PHY_OPERATOR_ASSIGN;
      NEED_SERIALIZE_AND_DESERIALIZE;
    private:
      int64_t index_num_;
      uint64_t main_tid_;// main table id
      //add wenghaixing [secondary index replace.fix_replace_bug]20150422
      uint64_t data_max_cid_;
      //add e
      common::ObRowDesc main_row_desc_;
      common::ObRowDesc child_op_row_desc_; //this desc should not to serialize
      common::ObRowDesc index_row_desc_del_[OB_MAX_INDEX_NUMS];
      common::ObRowDesc index_row_desc_ins_[OB_MAX_INDEX_NUMS];

      uint64_t replace_values_subquery_;// for mergeserver side
      ObExprValues replace_values_;// for updateserver side

      uint64_t replace_index_values_subquery_;
      ObRowStore data_row_;           //this operator need not to serialize
    };

  }//end sql
}//end oceanbase
#endif
