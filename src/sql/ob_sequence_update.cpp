//add lijianqiang [sequence update] 20150525:b

#include "ob_sequence_update.h"
#include "ob_lock_v0.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;


ObSequenceUpdate::ObSequenceUpdate()
{
  condition_types_names_idx_ = 0;
  const_condition_types_names_idx_ = 0;
  index_trigger_update_ = NULL;
  is_column_hint_index_ = false;
}

ObSequenceUpdate::~ObSequenceUpdate()
{}

void ObSequenceUpdate::reset()
{
  condition_types_names_idx_ = 0;
//  init_next_prevv_map();
  sequecne_expr_ids_.clear();
  ObSequence::reset();
}

void ObSequenceUpdate::reuse()
{
  condition_types_names_idx_ = 0;
//  init_next_prevv_map();
  sequecne_expr_ids_.clear();
  ObSequence::reuse();
}

int ObSequenceUpdate::open()
{
  return ObSequence::open();
}
int ObSequenceUpdate::copy_sequence_info_from_update_stmt()
{
  int ret = OB_SUCCESS;
  ObUpdateStmt* update_stmt = NULL;
  update_stmt = dynamic_cast<ObUpdateStmt*>(sequence_stmt_);
  if (NULL == update_stmt)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "the update stmt is not init! ret::[%d]",ret);
  }
  if (OB_SUCCESS == ret)
  {
    int64_t size = update_stmt->get_sequence_types_and_names_array_size();
    for (int64_t i = 0; OB_SUCCESS == ret && i < size; i++)
    {
      ret = sequence_name_type_pairs_.push_back(update_stmt->get_sequence_types_and_names_array(i));
    }
  }
  if (OB_SUCCESS == ret)
  {
    int64_t size = update_stmt->get_sequence_expr_ids_size();
    for (int64_t j=0; OB_SUCCESS==ret && j<size; j++)
    {
      ret = sequecne_expr_ids_.push_back(update_stmt->get_sequence_expr_id(j));
    }
  }
  if (OB_SUCCESS == ret)
  {
    condition_types_names_idx_ = update_stmt->get_set_where_boundary();
    const_condition_types_names_idx_ = condition_types_names_idx_;
  }
  return ret;
}
/**
 *
 * @brief fill the sequence_info to condition expr
 * @param filter [in][out] the condition expr which has the sequence
 * @param cond_expr_id [in] UNUSED now.
 *
 */
int ObSequenceUpdate::fill_the_sequence_info_to_cond_expr(ObSqlExpression *filter, uint64_t cond_expr_id)
{
  int ret = OB_SUCCESS;
  UNUSED(cond_expr_id);//can use this info for checking
  ObPostfixExpression::ExprArray &post_expr_array = filter->get_expr_array();
  /// simple expr:COLUMN_IDX | TID | CID | CONST_OBJ |  OBJ      | OP | T_OP_EQ | OP_COUNT | END
  ///                   0       1     2       3          4          5      6         7        8
  ///                 flag                   falg     real_val         op_type
  TBSYS_LOG(DEBUG,"the size of name type is[%ld] the boundary is[%ld]",sequence_name_type_pairs_.count(),condition_types_names_idx_);
  common::ObArray<std::pair<common::ObString,uint64_t> >&names_types_array = sequence_name_type_pairs_.at(condition_types_names_idx_);
  for (int64_t j = 0; OB_SUCCESS == ret && j < names_types_array.count(); j++)//for each and expr
  {
    for (int64_t i = 0; OB_SUCCESS == ret && i < post_expr_array.count(); i++)
    {
      ObObj *expr_array_obj = NULL;
      ObObjType obj_type;
      int64_t expr_type = -1;
      ObString preval_val;
      ObString sequence_name;
      bool can_use_prevval = false;
      if (ObIntType == post_expr_array[i].get_type())
      {
        if (OB_SUCCESS != (ret = post_expr_array[i].get_int(expr_type)))
        {
          TBSYS_LOG(WARN, "fail to get int value.err=%d", ret);
        }
        else if ((int64_t)ObPostfixExpression::CONST_OBJ == expr_type)//const obj
        {
          expr_array_obj = const_cast<ObObj *>(&post_expr_array[++i]);
          if (ObVarcharType != (obj_type = expr_array_obj->get_type()))
          {
            continue;
          }
          else
          {
            if (OB_SUCCESS != (ret = expr_array_obj->get_varchar(sequence_name)))
            {
              TBSYS_LOG(ERROR, "get the expr array obj failed!");
            }
            else if(sequence_name != names_types_array.at(j).first)
            {
              TBSYS_LOG(DEBUG,"not a sequence varchar");
              continue;//not a sequence varchar
            }
            else//find
            {
              if (OB_SUCCESS != (ret = get_prevval_by_name(&sequence_name, preval_val, can_use_prevval)))
              {
                TBSYS_LOG(ERROR, "get the info of sequence::[%.*s] failed",sequence_name.length(),sequence_name.ptr());
              }
              if (OB_SUCCESS == ret && can_use_prevval)
              {
                expr_array_obj->set_decimal(preval_val);
                TBSYS_LOG(DEBUG,"set decimal,the obj is::[%s]",to_cstring(post_expr_array[i]));
              }
              else
              {
                if (!can_use_prevval)
                {
                  ret = OB_ERROR;
                  TBSYS_LOG(USER_ERROR, "THE PREVVAL expression of  sequence [%.*s] can't be used before using NEXTVAL",names_types_array.at(0).first.length(),names_types_array.at(0).first.ptr());
                }
              }
            }
          }
        }
      }
    }//end for(int64_t i = 0
  }//end for for (int64_t j =
  if (OB_SUCCESS == ret)//for next use
  {
    gen_sequence_condition_names_idx();
  }
  return ret;
}

/**
 * @brief Exp:对于update语句，set中既可以使用prevval，也可以使用nextval，并且sequence可以进行混合计算，where中只能使用prevval
 *   1.在一个update语句中，同一行sequence所有的nextval 共享一个值（set clause）；
 *   2.在一个update语句中，同一个sequence所有的prevval共享一个值（set clause 与 where clause）；
 *   3.在一个update语句中，同一个sequence：
 *     如果先使用了prevval，那么之后出现的所有该sequence的prevval值相同
 *     如果先使用了nextval，那么之后出现的所有该sequence的prevval值为该nextval的前一个值，并且值相同
 * this function is used to handle the set clause of batch UPDATE stmt for sequence,
 * if U come to this function, the PROJECT exprs has been filled to posfix expression,
 * but the expressions is with sequence_name yet.
 * step1:we use the sequence_name and the sequence type to generate the sequence_id for each row,
 * then we store the sequence_id in PROJECT OP.
 * step2:the sequence_id will be filled into the PROJECT exprs when the PROJECT OP opened in UPS
 * @param project_op [IN] [OUT]
 * @param row_num [IN] we get the row number of the batch UPDATE stmt in transformer
 */
int ObSequenceUpdate::handle_the_set_clause_of_seuqence(const int64_t& row_num
      //add lbzhong [Update rowkey] 20160107:b
      , const int64_t non_rowkey_count, const bool is_update_rowkey
      //add:e
      )
{
  TBSYS_LOG(INFO, "handle the sequence for update set clause");
  int ret = OB_SUCCESS;
  //add lbzhong [Update rowkey] 20160115:b
  int64_t offset = 0;
  //add:e
  if (NULL == project_for_update_ || (is_column_hint_index_ && NULL == index_trigger_update_))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "not init! ret=[%d]",ret);
  }
  //add lbzhong [Update rowkey] 20160111:b
  else if(is_update_rowkey)
  {
    if(non_rowkey_count < 0)
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "non_rowkey_count < 0");
    }
    else
    {
      offset = non_rowkey_count;
    }
  }
  //add:e
  const common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> >  &out_put_columns = project_for_update_->get_output_columns();
  TBSYS_LOG(DEBUG,"the out put columns count[%ld]   the exprs ids count[%ld]!",out_put_columns.count(), (sequecne_expr_ids_.count()));
  //add lbzhong [Update rowkey bugfix] 20160315:b
  int64_t column_count = out_put_columns.count();
  if(is_update_rowkey)
  {
    column_count = out_put_columns.count() - non_rowkey_count;
  }
  //add:e
  //handle the exprs with sequence info
  if (OB_SUCCESS == ret)
  {
    bool is_const = false;
    sequence_idx_in_expr_.clear();
    this->reset_nextval_state_map();
    TBSYS_LOG(DEBUG,"the update row num is::[%ld]",row_num);
    for (int64_t num = 0; OB_SUCCESS == ret && num < row_num; num++)//for each row
    {
      //mod lbzhong [Update rowkey] 20160107, "i < out_put_columns.count()" to "i < column_count"
      //out_put_columns.count() != sequecne_expr_ids_.count() when update rowkey
      for (int64_t i = 0; OB_SUCCESS == ret && i < column_count; i++)//for eache column
      //mod:e
      {
        //mod lbzhong [Update rowkey] 20160511:b
        //out_put_columns_idx_ = i;//cons idx info of sequnce to project op
        out_put_columns_idx_ = i + offset;
        //mod:e
        if (0 != sequecne_expr_ids_.at(i))
        {
          //mod lbzhong [Update rowkey] 20160511:b
          //const ObSqlExpression &val_expr = out_put_columns.at(i);
          const ObSqlExpression &val_expr = out_put_columns.at(i + offset);
          //mod:e
          ObSqlExpression * sql_expr = const_cast<ObSqlExpression *>(&val_expr);
          ObPostfixExpression::ExprArray &post_expr_array = sql_expr->get_expr_array();
          if (OB_SUCCESS != (ret = val_expr.is_const_expr(is_const)))
          {
            TBSYS_LOG(ERROR, "get is const expr failed!");
          }
          if (is_const)//do replace
          {
            if (OB_SUCCESS == ret)
            {
              if (OB_SUCCESS != (ret = this->do_simple_fill(post_expr_array, int64_t(1), OB_SEQUENCE_UPDATE_LIMIT, true)))
              {
                TBSYS_LOG(WARN, "fill the sequence value to expr error! ret=%d", ret);
              }
            }
          }
          //a complex exprArray here,not const expr,do complex fill
          else
          {
            if (OB_SUCCESS == ret)
            {
              if (OB_SUCCESS != (ret = this->do_complex_fill(post_expr_array, OB_SEQUENCE_UPDATE_LIMIT)))
              {
                TBSYS_LOG(WARN, "fill the sequence value to expr error! ret=%d", ret);
              }
            }
          }
          //check the validity of current column for current sequence use(if obj_cast ok)
          //must be called before the expr was recovered with sequence_name
          if (OB_SUCCESS == ret)
          {
            if (
                //add lbzhong[Update rowkey] 20150108:b
                !is_update_rowkey &&
                //add:e
                OB_SUCCESS != (ret = check_column_cast_validity(*sql_expr, i)))
            {
              TBSYS_LOG(USER_ERROR,"value out of range");
              TBSYS_LOG(WARN,"current column with sequence do obj cast failed, ret=%d",ret);
            }
            //add lbzhong [Update rowkey] 20160510:b
            else if (is_update_rowkey && OB_SUCCESS != (ret = check_column_cast_validity(*sql_expr, -1)))
            {
              TBSYS_LOG(USER_ERROR,"value out of range");
              TBSYS_LOG(WARN,"current column with sequence do obj cast failed, ret=%d",ret);
            }
            //add:e
          }
          //recover the expr with sequence_name for next row
          //the last row won't be recover,we only SERIALIZE and DESERIALIZE the nextval to ups,the prevval is shared to all rows with the same value
          if (OB_SUCCESS == ret && num < row_num - 1)
          {
            common::ObArray<std::pair<common::ObString,uint64_t> >&names_types_array = sequence_name_type_pairs_.at(col_sequence_types_idx_-1);
            recover_the_sequence_expr(*sql_expr, names_types_array);
          }
          TBSYS_LOG(DEBUG, "the column after fill is::[%s]",to_cstring(*sql_expr));
        }//end if

      }//end for
      //after fill one row,we record the count of sequence number in one row used
      if (0 == num && OB_SUCCESS == ret)
      {
        project_for_update_->set_sequence_num_in_one_row();
      }
      //handle the sequence info to secondary index
      //we do not recover the exprs with seq_name when current row is the last row,the prevval is shared for all rows
      if (num == row_num -1 && OB_SUCCESS == ret && is_column_hint_index_)
      {
        index_trigger_update_->clear_clumns();
        for (int64_t j = 0; OB_SUCCESS == ret && j < out_put_columns.count(); j++)
        {
          if (OB_SUCCESS != (ret = index_trigger_update_->add_set_column(out_put_columns.at(j))))
          {
            TBSYS_LOG(ERROR, "add set column to index trigger failed, ret=%d",ret);
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        bool update_prevval = false;//need to judge
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
        this->reset_single_row_sequence_name();
        this->reset_nextval_state_map();
        this->reset_col_sequence_types_idx();
      }
    }//end for
    sql_context_->session_info_->set_current_result_set(out_result_set_);
    sql_context_->session_info_->set_session_state(QUERY_ACTIVE);
    //do update if the sequences with quick path or client transaction
    for (int64_t i = 0; ret == OB_SUCCESS && i < get_sequence_name_no_dup_size(); i++)
    {
      if (is_quick_path_sequence(get_sequence_name_no_dup(i)) || has_client_trans_)
      {
        SequenceInfo s_value;
        if(-1 == (ret = sequence_info_map_.get(get_sequence_name_no_dup(i), s_value)))
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "get the sequence values error!");
        }
        else if (hash::HASH_NOT_EXIST == ret)//not find
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "get the sequence values failed!");
        }
        else if (hash::HASH_EXIST == ret)
        {
          if (OB_SUCCESS != (ret = update_by_dml(get_sequence_name_no_dup(i), s_value)))
          {
            TBSYS_LOG(ERROR,"update quick sequence failed, ret=%d",ret);
          }
        }
      }//end if
    }//end for
  }//end if
  if (OB_SUCCESS == ret && OB_SUCCESS != (ret = project_for_update_->check_the_sequence_validity()))
  {
    TBSYS_LOG(ERROR,"the construct sequence info failed!,ret=[%d]",ret);
  }
  return ret;
}

int ObSequenceUpdate::close()
{
  sequecne_expr_ids_.clear();
  return ObSequence::close();
}

int64_t ObSequenceUpdate::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObSequence Update\n");
  if (NULL != this->get_child(0))
  {
    databuff_printf(buf, buf_len, pos, "child0 of ObSequenceUpdate::\n");
    pos += this->get_child(0)->to_string(buf + pos,buf_len - pos);
  }
  pos += ObSequence::to_string(buf + pos,buf_len - pos);
  return pos;
}
namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObSequenceUpdate, PHY_SEQUENCE_UPDATE);
  }
}
//add 20150525:e
