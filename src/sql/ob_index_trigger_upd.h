#ifndef OB_INDEX_TRIGGER_UPD_H
#define OB_INDEX_TRIGGER_UPD_H
#include "ob_single_child_phy_operator.h"
#include "sql/ob_ups_modify.h"
#include "common/ob_iterator.h"
#include "common/ob_iterator_adaptor.h"
#include "updateserver/ob_sessionctx_factory.h"
#include "updateserver/ob_ups_table_mgr.h"
#include "updateserver/ob_ups_utils.h"
#include "common/page_arena.h"
#include "common/ob_se_array.h"
#include "ob_sql_expression.h"
#include "ob_project.h"
namespace oceanbase
{
    namespace sql
    {
        typedef common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> > expr_array;
        typedef common::ObSEArray<ObObj, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObObj> > cast_obj_array;
        class ObIndexTriggerUpd: public ObSingleChildPhyOperator
        {
            public:
            ObIndexTriggerUpd();
            virtual ~ObIndexTriggerUpd();
            virtual void reset();
            virtual void reuse();
            virtual int open();
            //add wenghaixing [secondary index upd.4] 20141129
            virtual int close();
            //add wenghaixing [secondary index static_index_build.consistency]20150424
            inline virtual int64_t get_index_num()const {return index_num_;}
            //add e
            int handle_trigger(const ObSchemaManagerV2 *schema_mgr, updateserver::ObIUpsTableMgr* host
                               , updateserver::RWSessionCtx &session_ctx, ObRowStore *store);
            //int handle_trigger_one_table();
            int handle_trigger_one_table(int64_t index_num,const ObSchemaManagerV2 *schema_mgr,
                                         updateserver::ObIUpsTableMgr* host,updateserver::RWSessionCtx &session_ctx,  ObRowStore* store);
            //add e
            virtual int get_next_row(const ObRow *&row);
            virtual int get_row_desc(const ObRowDesc *&row_desc) const;
            int get_trigger_row_desc(const ObRowDesc *&row_desc) const;
            virtual int64_t to_string(char* buf, const int64_t buf_len) const;
            int get_next_data_row(const common::ObRow *&input_row);
            void set_index_num(int64_t num);
            void set_update_index_num(int64_t num);
            //int add_output_column(uint64_t index_num,const ObSqlExpression& expr);
            int add_set_column(const ObSqlExpression& expr);
            int add_cast_obj(const ObObj &obj);
            int add_row_desc_del(int64_t idx, common::ObRowDesc desc);
            int add_row_desc_upd(int64_t idx, common::ObRowDesc desc);
            int get_row_desc_del(int64_t idx, common::ObRowDesc& desc);
            int get_row_desc_upd(int64_t idx, common::ObRowDesc& desc);
            //add wenghaixing [secondary index upd.bugfix]20150127
            void set_data_row_desc(common::ObRowDesc& desc);
            void set_cond_bool(bool val);
            void get_cond_bool(bool &val);
            void reset_iterator();
            //add e
            //add lijianqiang [sequence update] 20150916:b
            /*Exp:bug fix for sequence usage in update stmt with secodary index */
            void clear_clumns();
            //add 20150916:e
            //add lbzhong [Update rowkey] 20160112:b
            inline bool exist_update_column(const uint64_t column_id) const;
            void set_update_columns(oceanbase::common::ObArray<uint64_t> &update_columns);
            //add:e

            virtual ObPhyOperatorType get_type() const;
            DECLARE_PHY_OPERATOR_ASSIGN;
            NEED_SERIALIZE_AND_DESERIALIZE;
            inline void set_data_max_cid(uint64_t cid){data_max_cid_ = cid;}
            private:
               // expr_array set_columns_;
                //expr_array index_columns_[OB_MAX_INDEX_NUMS];//for construct index row
                expr_array set_index_columns_;//for cacluate
                cast_obj_array cast_obj_;
                int64_t index_num_;
                int64_t update_index_num_;
                common::ObRow row_;
                common::ObRowDesc index_row_desc_del_[OB_MAX_INDEX_NUMS];
                common::ObRowDesc index_row_desc_upd_[OB_MAX_INDEX_NUMS];
                common::ObRowDesc data_row_desc_;
                //if update condition is more than rowkey,we must change the way get next row 20150127
                bool has_other_cond_;
                uint64_t data_max_cid_;
                ModuleArena arena_;
                //add lbzhong [Update rowkey] 20160112:b
                oceanbase::common::ObArray<uint64_t> update_columns_;
                //add:e
        };



    }//end namespace sql
}//end namespace oceanbase

#endif // OB_INDEX_TRIGGER_UPD_H
