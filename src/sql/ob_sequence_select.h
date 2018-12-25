//add by liuzy [sequence select] 20150602:b
/**
 * ob_sequence_select.h
 *
 * used for "select nextval/prevval ... for sequence_name from table_name"
 *
 * Authors:
 * liuzy <575553227@qq.com>
 *
 */
#ifndef OB_OCEANBASE_SQL_SEQUENCE_SELECT_H
#define OB_OCEANBASE_SQL_SEQUENCE_SELECT_H

#include "ob_select_stmt.h"
#include "ob_sequence.h"

namespace oceanbase
{
  namespace sql
  {
    class ObSequenceSelect: public ObSequence
    {
    public:
       ObSequenceSelect();
       virtual ~ObSequenceSelect();
       virtual int open();
       virtual int close();
       virtual void reset();
       virtual void reuse();
       int get_next_row(const common::ObRow *&row);
       int get_row_desc(const common::ObRowDesc *&row_desc) const;
       int64_t to_string(char* buf, const int64_t buf_len) const;
       enum ObPhyOperatorType get_type() const { return PHY_SEQUENCE_SELECT; }
       inline void reset_sequence_names_idx(int64_t where_seq_idx = 0);
       void copy_sequence_info_from_select_stmt(ObSelectStmt* select_stmt);
       bool is_sequence_cond_id(uint64_t cond_id);
       int  fill_the_sequence_info_to_cond_expr(ObSqlExpression *filter, uint64_t cond_expr_id);
       bool is_expr_has_sequence(int64_t idx) const;
       int  do_fill(ObSqlExpression &expr, int dml_type);
       inline void gen_sequence_names_idx();
       void set_tmp_table(uint64_t subquery_id) { tmp_table_subquery_ = subquery_id; }
       void set_project_op(uint64_t subquery_id) { project_op_subquery_ = subquery_id; }
       void set_for_update(bool for_update);
       inline bool is_for_update() { return for_update_; }
       //copy from ob_project.cpp
       int add_output_column(const ObSqlExpression& expr);
       inline int64_t get_output_column_size() const;
       inline int64_t get_rowkey_cell_count() const;
       inline void set_rowkey_cell_count(const int64_t count);
       const common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> >  &get_output_columns() const;
    //add peiouya [IN_TYPEBUG_FIX] 20151225:b
    public:
       int get_output_columns_dsttype(common::ObArray<common::ObObjType> &output_columns_dsttype);
       int add_dsttype_for_output_columns(common::ObArray<common::ObObjType>& columns_dsttype);
    //add 20151225:e
    private:
       inline void reset_sequence_pair_idx_for_where();
       void copy_sequence_pair(ObSelectStmt *select_stmt);
       void copy_sequence_select_clause_expr(ObSelectStmt *select_stmt);
       //copy from ob_project.cpp
       int cons_row_desc();
       //disallow copy
       ObSequenceSelect(const ObSequenceSelect &other);
       ObSequenceSelect& operator=(const ObSequenceSelect &other);
    private:
       //do_fill()
       common::ObArray<bool> sequence_select_clause_expr_ids_;//列中有sequence, push "true"; 无sequence,push "false"
       uint64_t tmp_table_subquery_;
       uint64_t project_op_subquery_;
       bool for_update_;
       //copy from ob_project.cpp
       common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> > columns_;
       common::ObRowDesc row_desc_for_project_;
       common::ObRow row_;
       int64_t rowkey_cell_count_;
       //add peiouya [IN_TYPEBUG_FIX] 20151225:b
       common::ObArray<common::ObObjType> output_columns_dsttype_;
       //add 20151225:e
    };
    //copy info from select_stmt
    inline void ObSequenceSelect::copy_sequence_info_from_select_stmt(ObSelectStmt *select_stmt)
    {
      copy_sequence_pair(select_stmt);              //拷贝sequence name_type pair的副本, 在open时使用
      copy_sequence_select_clause_expr(select_stmt);//拷贝语句expr，用于表示某一列时候有sequence
    }
    inline void ObSequenceSelect::gen_sequence_names_idx()
    {
      sequence_names_idx_for_select_++;
    }
    inline void ObSequenceSelect::reset_sequence_names_idx(int64_t where_seq_idx)
    {
      sequence_names_idx_for_select_ = where_seq_idx;
    }
    inline void ObSequenceSelect::reset_sequence_pair_idx_for_where()
    {
      sequence_pair_idx_for_where_ = 0;
    }
    //copy from ob_project.cpp
    inline int64_t ObSequenceSelect::get_output_column_size() const
    {
      return columns_.count();
    }
    inline const common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> >  & ObSequenceSelect::get_output_columns() const
    {
      return columns_;
    }
    inline int64_t ObSequenceSelect::get_rowkey_cell_count() const
    {
      return rowkey_cell_count_;
    }
    inline void ObSequenceSelect::set_rowkey_cell_count(const int64_t count)
    {
      rowkey_cell_count_ = count;
    }
  }
}

#endif // OB_SEQUENCE_VALUES_FOR_SELECT_H
