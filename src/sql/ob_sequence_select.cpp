#include "ob_sequence_select.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObSequenceSelect::ObSequenceSelect()
{
  sequence_pair_idx_for_where_ = 0;
  sequence_names_idx_for_select_ = 0;
  for_update_ = false;
  tmp_table_subquery_ = OB_INVALID_ID;
  project_op_subquery_ = OB_INVALID_ID;
  output_columns_dsttype_.clear ();  //add peiouya [IN_TYPEBUG_FIX] 20151225
}
ObSequenceSelect::~ObSequenceSelect()
{}
void ObSequenceSelect::reset()
{
  sequence_name_type_pairs_.clear();
  sequence_select_clause_expr_ids_.clear();
  row_desc_for_project_.reset();
  rowkey_cell_count_ = 0;
  columns_.clear();
  for_update_ = false;
  tmp_table_subquery_ = OB_INVALID_ID;
  project_op_subquery_ = OB_INVALID_ID;
  ObSequence::reset();
  output_columns_dsttype_.clear ();  //add peiouya [IN_TYPEBUG_FIX] 20151225
}

void ObSequenceSelect::reuse()
{
  sequence_name_type_pairs_.clear();
  sequence_select_clause_expr_ids_.clear();
  columns_.clear();
  row_desc_for_project_.reset();
  rowkey_cell_count_ = 0;
  for_update_ = false;
  tmp_table_subquery_ = OB_INVALID_ID;
  project_op_subquery_ = OB_INVALID_ID;
  ObSequence::reuse();
  output_columns_dsttype_.clear ();  //add peiouya [IN_TYPEBUG_FIX] 20151225
}

int ObSequenceSelect::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  this->reset_nextval_state_map();
  const common::ObRow *input_row = NULL;
  reset_sequence_names_idx();
  if (OB_SUCCESS != (ret = get_child(1)->get_next_row(input_row)))
  {
    if (OB_ITER_END != ret
        && !IS_SQL_ERR(ret))
    {
      TBSYS_LOG(WARN, "failed to get next row, err=%d", ret);
    }
    /*Exp: After handle the last row, we need to update the sequences if with quick path or client transaction */
    if (OB_ITER_END == ret)
    {
      int temp_ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCCESS == temp_ret && i < get_sequence_name_no_dup_size(); i++)
      {
        if (is_quick_path_sequence(get_sequence_name_no_dup(i)) || has_client_trans_)
        {
          SequenceInfo s_value;
          if(-1 == (temp_ret = sequence_info_map_.get(get_sequence_name_no_dup(i), s_value)))
          {
            temp_ret = OB_ERROR;
            TBSYS_LOG(ERROR, "get the sequence values error!");
          }
          else if (hash::HASH_NOT_EXIST == temp_ret)//not find
          {
            temp_ret = OB_ERROR;
            TBSYS_LOG(ERROR, "get the sequence values failed!");
          }
          else if (hash::HASH_EXIST == temp_ret)
          {
            temp_ret = update_by_dml(get_sequence_name_no_dup(i), s_value);
          }
        }//end if
        if (OB_SUCCESS != temp_ret)
        {
          ret = temp_ret;
          TBSYS_LOG(ERROR, "quick path failed, update [%.*s] value failed!", get_sequence_name_no_dup(i).length(), get_sequence_name_no_dup(i).ptr());
        }
      }//end for
      sql_context_->session_info_->set_current_result_set(out_result_set_);
      sql_context_->session_info_->set_session_state(QUERY_ACTIVE);
    }
  }
  else
  {
    TBSYS_LOG(DEBUG, "Sequence Select ret=%d op=%p type=%d %s",
              ret, get_child(1), get_child(1)->get_type(), (NULL == input_row) ? "NULL INPUT ROW" : to_cstring(*input_row));
    const ObObj *result = NULL;
    for (int32_t i = 0; i < columns_.count(); ++i)
    {
      common::ObArray<std::pair<common::ObString,uint64_t> >&names_types_array = sequence_name_type_pairs_.at(sequence_names_idx_for_select_);
      ObSqlExpression &expr = columns_.at(i);
      bool current_expr_has_sequence = is_expr_has_sequence(i);
      if (current_expr_has_sequence)
      {
        /*Exp: "sequence_select_clause_expr_ids_" corresponds "column_". "true":sequence; "false":no sequence*/
        if (OB_SUCCESS != (ret = do_fill(expr, OB_SEQUENCE_SELECT_LIMIT)))
        {
          TBSYS_LOG(WARN,"handle select clause of sequence failed!");
          break;
        }
        if(OB_SUCCESS == ret)//handle one expr succeed,prepare for next!
        {
          gen_sequence_names_idx();
        }
      }
      if (OB_SUCCESS != (ret = expr.calc(*input_row, result)))
      {
        TBSYS_LOG(WARN, "failed to calculate, err=%d", ret);
        break;
      }
      else if (OB_SUCCESS != (ret = row_.set_cell(expr.get_table_id(), expr.get_column_id(), *result)))
      {
        TBSYS_LOG(WARN, "failed to set row cell, err=%d", ret);
        break;
      }
      if (OB_SUCCESS == ret && current_expr_has_sequence)
      {
        recover_the_sequence_expr(expr, names_types_array);
      }
    } // end for
    if (OB_SUCCESS == ret)
    {
      bool update_prevval = false;
      for (int64_t i = 0; OB_SUCCESS == ret && i < single_row_sequence_names_.count(); i++)
      {
        this->need_to_update_prevval(single_row_sequence_names_.at(i), update_prevval);
        if(OB_SUCCESS != (ret = update_sequence_info_map_for_prevval(single_row_sequence_names_.at(i), update_prevval)))
        {
          TBSYS_LOG(WARN, "update sequence info for quick or client transaction failed!");
        }
      }
    }
    if (OB_SUCCESS == ret)
    {
      row = &row_;
    }
  }
  if (OB_SUCCESS == ret)
  {
    this->reset_single_row_sequence_name();
    this->reset_nextval_state_map();
  }
  return ret;
}

int ObSequenceSelect::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 >= row_desc_for_project_.get_column_num()))
  {
    TBSYS_LOG(ERROR, "not init");
    ret = OB_NOT_INIT;
  }
  else
  {
    row_desc = &row_desc_for_project_;
  }
  return ret;
}

//add peiouya [IN_TYPEBUG_FIX] 20151225:b
int ObSequenceSelect::get_output_columns_dsttype(common::ObArray<common::ObObjType> &output_columns_dsttype)
{
  output_columns_dsttype = output_columns_dsttype_;
  return OB_SUCCESS ;
}
int ObSequenceSelect::add_dsttype_for_output_columns(common::ObArray<common::ObObjType>& columns_dsttype)
{
  output_columns_dsttype_ = columns_dsttype;
  return OB_SUCCESS;
}
//add 20151225:e

int ObSequenceSelect::open()
{
  int ret = OB_SUCCESS;
  if (!is_for_update())
  {
    if (OB_SUCCESS != (ret = get_child(1)->open()))//child project
    {
      if (!IS_SQL_ERR(ret))
      {
        TBSYS_LOG(WARN, "failed to open child_op, err=%d", ret);
      }
    }
    else if (OB_SUCCESS != (ret = cons_row_desc()))
    {
      TBSYS_LOG(WARN, "failed to construct row desc, err=%d", ret);
    }
    else
    {
      row_.set_row_desc(row_desc_for_project_);
    }
  }
  else
  {
    ObValues *tmp_table = NULL;
    const ObRow *input_row = NULL;
    ObProject *project_op = NULL;
    bool is_row_empty = false;
    if (NULL == (tmp_table = dynamic_cast<ObValues *>(my_phy_plan_->get_phy_query_by_id(tmp_table_subquery_))))
    {
      TBSYS_LOG(ERROR, "Failed to get get param ObValues!");
    }
    else if (OB_ITER_END == (ret = tmp_table->get_next_row(input_row)))
    {
      TBSYS_LOG(INFO, "Result set is empty.");
    }
    else if (OB_SUCCESS != (ret = input_row->get_is_row_empty(is_row_empty)))
    {
      TBSYS_LOG(ERROR, "Failed to get is_row_empty!");
    }
    else if (is_row_empty)
    {
      TBSYS_LOG(INFO, "Result set is empty.");
    }
    else if (NULL == (project_op = dynamic_cast<ObProject *>(my_phy_plan_->get_phy_query_by_id(project_op_subquery_))))
    {
      TBSYS_LOG(ERROR, "Failed to get get param ObProject!");
    }
    else
    {
      reset_sequence_names_idx();
      const common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> > &column= project_op->get_output_columns();
      for (int32_t i = 0; i < column.count(); ++i)
      {
        const ObSqlExpression &expr = column.at(i);
        ObSqlExpression *tmp_expr = const_cast<ObSqlExpression *>(&expr);
        if (is_expr_has_sequence(i))
        {
          if (OB_SUCCESS != (ret = do_fill(*tmp_expr, OB_SEQUENCE_SELECT_LIMIT)))
          {
            TBSYS_LOG(ERROR,"handle select clause of sequence failed!");
            break;
          }
          if(OB_SUCCESS == ret)//handle one expr succeed,prepare for next!
          {
            gen_sequence_names_idx();
          }
        }
      }//end for
    }//end else
  }//end else
  return ret;
}

int ObSequenceSelect::close()
{
  sequence_select_clause_expr_ids_.clear();
  columns_.clear();
  row_desc_for_project_.reset();
  row_.clear();
  for_update_ = false;
  tmp_table_subquery_ = OB_INVALID_ID;
  project_op_subquery_ = OB_INVALID_ID;
  return ObSequence::close();
}

int64_t ObSequenceSelect::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObSequenceSelect child[0]: begin\n");
  if (NULL != this->get_child(0))
  {
    pos += this->get_child(0)->to_string(buf + pos,buf_len - pos);
  }
  databuff_printf(buf, buf_len, pos, "ObSequenceSelect child[0]: end\n");
  databuff_printf(buf, buf_len, pos, "ObSequenceSelect child[1]: begin\n");
  if (NULL != this->get_child(1))
  {
    pos += this->get_child(1)->to_string(buf + pos,buf_len - pos);
  }
  databuff_printf(buf, buf_len, pos, "ObSequenceSelect child[1]: end\n");
  return pos;
}

int ObSequenceSelect::do_fill(ObSqlExpression &expr, int dml_type)
{
  int ret = OB_SUCCESS;
  bool is_const = false;
  sequence_idx_in_expr_.clear();
  ObPostfixExpression::ExprArray &post_expr_array = expr.get_expr_array();
  if (OB_SUCCESS != (ret = expr.is_const_expr(is_const)))
  {
    TBSYS_LOG(ERROR, "get is const expr failed!");
  }
  if (is_const)//do replace
  {
    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = this->do_simple_fill(post_expr_array, int64_t(1), dml_type, true)))
      {
        TBSYS_LOG(WARN, "fill the sequence value to expr error!");
      }
    }
  }
  else
  {
    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = this->do_complex_fill(post_expr_array, dml_type)))
      {
        TBSYS_LOG(WARN, "fill the sequence value to expr error!");
      }
    }
  }
  sql_context_->session_info_->set_current_result_set(out_result_set_);
  sql_context_->session_info_->set_session_state(QUERY_ACTIVE);
  return ret;
}

int ObSequenceSelect::fill_the_sequence_info_to_cond_expr(ObSqlExpression *filter, uint64_t cond_expr_id)
{
  int ret = OB_SUCCESS;
  UNUSED(cond_expr_id);
  reset_sequence_pair_idx_for_where();
  if (OB_SUCCESS != (ret = do_fill(*filter, OB_SEQUENCE_SELECT_WHERE_LIMIT)))
  {
    TBSYS_LOG(ERROR,"Handle filter is failed. ret = %d", ret);
  }
  else
  {
    reset_sequence_names_idx();
  }
  return ret;
}

int ObSequenceSelect::add_output_column(const ObSqlExpression& expr)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == (ret = columns_.push_back(expr)))
  {
    columns_.at(columns_.count() - 1).set_owner_op(this);
  }
  return ret;
}

int ObSequenceSelect::cons_row_desc()
{
  int ret = OB_SUCCESS;
  if(0 != row_desc_for_project_.get_column_num())
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "row desc should be empty");
  }

  for (int32_t i = 0; OB_SUCCESS == ret && i < columns_.count(); ++i)
  {
    const ObSqlExpression &expr = columns_.at(i);
    if (OB_SUCCESS != (ret = row_desc_for_project_.add_column_desc(expr.get_table_id(), expr.get_column_id())))
    {
      TBSYS_LOG(WARN, "failed to add column desc, err=%d", ret);
      break;
    }
  } // end for
  if (0 >= columns_.count())
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "no column for output");
  }
  else
  {
    row_desc_for_project_.set_rowkey_cell_count(rowkey_cell_count_);
  }
  return ret;
}

void ObSequenceSelect::copy_sequence_pair(ObSelectStmt *select_stmt)
{
  common::ObArray<common::ObArray<std::pair<common::ObString,uint64_t> > > &name_type_pairs = select_stmt->get_sequence_all_name_type_pairs();
  int64_t num = name_type_pairs.count();
  for (int64_t i = 0; i < num; ++i)
  {
    sequence_name_type_pairs_.push_back(name_type_pairs.at(i));
  }
}

bool ObSequenceSelect::is_sequence_cond_id(uint64_t cond_id)
{
  bool is_cond_id = false;
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = NULL;
  if (NULL != sequence_stmt_)
  {
    select_stmt = dynamic_cast<ObSelectStmt *>(sequence_stmt_);
    common::ObArray<uint64_t> &where_expr_ids = select_stmt->get_sequence_where_clause_expr_ids();
    int64_t num = where_expr_ids.count();
    for (int64_t i = 0; i < num; i++)
    {
      if (cond_id == where_expr_ids.at(i))
      {
        is_cond_id = true;
        break;
      }
    }
  }
  else
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "select stmt is not init! ret::[%d]", ret);
  }
  return is_cond_id;
}
void ObSequenceSelect::copy_sequence_select_clause_expr(ObSelectStmt *select_stmt)
{
  common::ObArray<bool> &select_clause_seq_state = select_stmt->get_sequence_select_clause_has_sequence();
  int64_t num = select_clause_seq_state.count();
  for (int64_t i = 0; i < num; ++i)
  {
    sequence_select_clause_expr_ids_.push_back(select_clause_seq_state.at(i));
  }
}

bool ObSequenceSelect::is_expr_has_sequence(int64_t idx) const
{
  OB_ASSERT(idx < sequence_select_clause_expr_ids_.count());
  return sequence_select_clause_expr_ids_.at(idx);
}
void ObSequenceSelect::set_for_update(bool for_update)
{
  for_update_ = for_update;
}
namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObSequenceSelect, PHY_SEQUENCE_SELECT);
  }
}
////add 20150407:e
