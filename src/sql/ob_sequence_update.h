//add lijianqiang [sequence] 20150512:b
/**
 * ob_sequence_update.h
 *
 * used for "UPDATE table_name SET C2 = a, C3 = NEXTVAL/PREVVAL FOR sequence_name... WHERE C1 = PREVVAL FOR sequence_name AND ..."
 *
 * Authors:
 * lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *
 */
#ifndef OB_OCEANBASE_SQL_SEQUENCE_UPDATE_H
#define OB_OCEANBASE_SQL_SEQUENCE_UPDATE_H

#include "ob_update_stmt.h"
#include "ob_sequence.h"
#include "ob_index_trigger_upd.h"

namespace oceanbase
{
  namespace sql
  {
    class ObSequenceUpdate: public ObSequence
    {
      public:
        ObSequenceUpdate();
        virtual ~ObSequenceUpdate();
        virtual int open();
        virtual int close();
        virtual void reset();
        virtual void reuse();
        int64_t to_string(char* buf, const int64_t buf_len) const;
        enum ObPhyOperatorType get_type() const { return PHY_SEQUENCE_UPDATE; }
        int copy_sequence_info_from_update_stmt();
        bool is_sequence_cond_id(uint64_t cond_id);
        int fill_the_sequence_info_to_cond_expr(ObSqlExpression *filter, uint64_t cond_expr_id);
        void gen_sequence_condition_names_idx();
        void reset_sequence_condition_names_idx();
        void set_index_trigger_update(ObIndexTriggerUpd * index_trigger_updat,bool is_column_hint_index)
        {index_trigger_update_ = index_trigger_updat; is_column_hint_index_ = is_column_hint_index;}
        int handle_the_set_clause_of_seuqence(const int64_t& row_num
            //add lbzhong [Update rowkey] 20160107:b
            , const int64_t non_rowkey_count = 0 //total colmun count - rowkey column count, not update rowkey if non_rowkey_count = 0
            , const bool is_update_rowkey = false
            //add:e
        );
        //add lbzhong [Update rowkey] 20160108:b
        int is_update_rowkey_sequence(bool& is_rowkey, const oceanbase::common::ObArray<int32_t> &update_columns_flag) const;
        ObProject* get_project_for_update() { return project_for_update_; }
        //add:e
      private:
        int64_t condition_types_names_idx_;//sequence names 和 types 的下标跟踪标记，没次出理完毕一个含sequence的表达式就自增1
        int64_t const_condition_types_names_idx_;//has the same init value with condition_types_names_idx_,but not increase auto,for rest
        ObIndexTriggerUpd * index_trigger_update_;
        bool is_column_hint_index_;
        common::ObArray<uint64_t> sequecne_expr_ids_;//if current expr has sequence ,push expr_id,else push 0 (copy from logical plan)
    };
    inline bool ObSequenceUpdate::is_sequence_cond_id(uint64_t cond_id)
    {
      bool ret = false;
      ObUpdateStmt *update_stmt = NULL;
      update_stmt = dynamic_cast<ObUpdateStmt *>(sequence_stmt_);
      if (NULL == update_stmt)
      {
        TBSYS_LOG(ERROR, "update_stmt not init!");
      }
      else
      {
        int64_t num = update_stmt->get_sequence_expr_ids_size();
        for (int64_t i = 0; i < num; i++)
        {
          if (cond_id == update_stmt->get_sequence_expr_id(i))
          {
            ret = true;
            break;
          }
        }
      }
      return ret;
    }

    inline void ObSequenceUpdate::gen_sequence_condition_names_idx()
    {
      condition_types_names_idx_++;
    }
    /**
     * if U want to resue the sequence in where conditions in update_stmt
     * please call this fuction before reusing
     */
    inline void ObSequenceUpdate::reset_sequence_condition_names_idx()
    {
      condition_types_names_idx_ = const_condition_types_names_idx_;
    }

    //add lbzhong [Update rowkey] 20160108:b
    inline int ObSequenceUpdate::is_update_rowkey_sequence(bool& is_update, const oceanbase::common::ObArray<int32_t> &update_columns_flag) const
    {
      int ret = OB_SUCCESS;
      int64_t expr_count = sequecne_expr_ids_.count();
      int64_t rowkey_size = out_row_desc_.get_rowkey_cell_count();
      if(0 == update_columns_flag.count())
      {
        is_update = false;
      }
      else if(update_columns_flag.count() > expr_count - rowkey_size)
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "update_columns_flag.count()[=%ld] > expr_count[=%ld] - rowkey_size[%ld]",
                  update_columns_flag.count(), expr_count, rowkey_size);
      }
      else
      {
        for(int64_t i = 0; i < update_columns_flag.count(); i++)
        //for(int64_t i = rowkey_size; i < expr_count; i++)
        {
          if((0 != sequecne_expr_ids_.at(rowkey_size + i)) && (1 == update_columns_flag.at(i)))
          //if((0 != sequecne_expr_ids_.at(i)) && (1 == update_columns_flag.at(i - rowkey_size)))
          {
            is_update = true;
            break;
          }
        }
      }
      return ret;
    }
    //add:e

  } // end namespace sql
}
#endif // OB_SEQUENCE_UPDATE_H
//add  20150512:e
