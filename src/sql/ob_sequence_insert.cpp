//add lijianqiang [sequence] 20150407:b

#include "ob_sequence_insert.h"
#include "ob_table_rpc_scan.h"
#include "ob_insert_stmt.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;


ObSequenceInsert::ObSequenceInsert()
{
  table_id_ = OB_INVALID_ID;
}

ObSequenceInsert::~ObSequenceInsert()
{}

void ObSequenceInsert::reset()
{
  table_id_ = OB_INVALID_ID;
  ObSequence::reset();
}

void ObSequenceInsert::reuse()
{
  table_id_ = OB_INVALID_ID;
  ObSequence::reuse();
}

bool ObSequenceInsert::is_sequence_cond_id(uint64_t cond_id)
{
  UNUSED(cond_id);
  return OB_NOT_SUPPORTED;
}
int ObSequenceInsert::fill_the_sequence_info_to_cond_expr(ObSqlExpression *filter, uint64_t cond_expr_id)
{
  UNUSED(filter);
  UNUSED(cond_expr_id);
  return OB_NOT_SUPPORTED;
}
int ObSequenceInsert::copy_sequence_info_from_insert_stmt(const ObSEArray<int64_t, 64>& row_desc_map)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = NULL;
  insert_stmt = dynamic_cast<ObInsertStmt*>(sequence_stmt_);
  if (NULL == insert_stmt)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "the insert stmt is not init! ret::[%d]",ret);
  }
  if (OB_SUCCESS == ret)
  {
    int64_t row_size = insert_stmt->get_value_row_size();
    int64_t column_size = insert_stmt->get_column_size();
    sequence_columns_mark_.reserve(row_size);
    ObArray<uint64_t> value_row;
    //copy 每个sequence在插入列中的位置
    for (int64_t i = 0; OB_SUCCESS == ret && i < row_size; i++)
    {
      for (int64_t j = 0; OB_SUCCESS == ret && j < column_size; j++)
      {
        uint64_t flag = insert_stmt->get_col_sequence_map_flag(i * column_size + row_desc_map[j]);
        if (OB_SUCCESS != (ret = value_row.push_back(flag)))
        {
          TBSYS_LOG(ERROR, "copy value row col failed!");
          break;
        }
      }//end for
      if (OB_SUCCESS != (ret = sequence_columns_mark_.push_back(value_row)))
      {
        TBSYS_LOG(ERROR, "copy value row failed!");
        break;
      }
      value_row.clear();
    }//end for

    //copy 所有sequence的名字以及其所对应的下标
    row_size = insert_stmt->get_sequence_names_no_dup_size();
    ObArray<uint64_t> tmp_ids;
    for (int64_t i = 0 ;  OB_SUCCESS == ret && i < insert_stmt->get_next_prev_ids_size(); i++)
    {
      ObArray<uint64_t> & ids = insert_stmt->get_next_prev_ids_array(i);
      for (int64_t j = 0; OB_SUCCESS == ret && j < ids.count(); j++)
      {
        if (OB_SUCCESS != (ret = tmp_ids.push_back(ids.at(j))))
        {
          TBSYS_LOG(ERROR,"copy next prev values error!");
        }
      }
      if (OB_SUCCESS != (ret = col_sequence_types_.push_back(tmp_ids)))
      {
        TBSYS_LOG(ERROR, "copy next prev values error!");
      }
      tmp_ids.clear();
    }
    table_id_ = insert_stmt->get_table_id();//copy table id
  }
  return ret;
}

/**
 *
 * @brief this function is used to fill the "sequence_name" in "input exprs" with the "sequence_value" row by row and column by column
 * @param input_values [in][out]
 *
 */
int ObSequenceInsert::do_fill(ObExprValues *input_values)
{
  int ret = OB_SUCCESS;
  int64_t row_num = sequence_columns_mark_.count();
  int64_t col_num = sequence_columns_mark_.at(0).count();
  bool is_const = false;
  OB_ASSERT(0 < row_num);
  OB_ASSERT(0 < col_num);
  col_sequence_types_idx_ = 0;//判断有sequence列的sequence是next型还是prev型时 col_sequence_types_的index
  col_sequence_types_idx_idx_ = 0;
  this->reset_nextval_state_map();
  for (int64_t i = 0; OB_SUCCESS == ret && i < row_num; i++)//each row
  {
    for (int64_t j = 0; OB_SUCCESS == ret && j < col_num; j++)//each column
    {
      if (int64_t(1) == sequence_columns_mark_.at(i).at(j)) //find one,replace
      {
        ObSqlExpression &val_expr = input_values->get_values().at(i * col_num + j);//取得输入列中对应的表达式
//        TBSYS_LOG(ERROR, "====the val expr is::[%s]======",to_cstring(val_expr));
        ObPostfixExpression::ExprArray &post_expr_array = val_expr.get_expr_array();
        /**quick path:generally there is only one sequence in one column*/
        if (OB_SUCCESS != (ret = val_expr.is_const_expr(is_const)))
        {
          TBSYS_LOG(ERROR, "get is const expr failed!");
        }
        if (is_const)//do replace
        {
          if (OB_SUCCESS == ret)
          {
//            TBSYS_LOG(ERROR,"currnet expr [%s] is const expr, do simple fill!",to_cstring(val_expr));
            if (OB_SUCCESS != (ret = this->do_simple_fill(post_expr_array, int64_t(1), OB_SEQUENCE_INSERT_LIMIT, true)))
            {
              TBSYS_LOG(WARN, "fill the sequence value to expr error!");
            }
          }
        }
        //**a complex exprArray here,not const expr,do complex fill
        else
        {
          if (OB_SUCCESS == ret)
          {
//            TBSYS_LOG(ERROR,"currnet expr [%s] is complex expr, do complex fill!",to_cstring(val_expr));
            if (OB_SUCCESS != (ret = this->do_complex_fill(post_expr_array, OB_SEQUENCE_INSERT_LIMIT)))
            {
              TBSYS_LOG(WARN, "fill the sequence value to expr error!");
            }
          }
        }
//        TBSYS_LOG(ERROR, "====the val expr after filling is::[%s]======",to_cstring(val_expr));
      //check if the current the column with sequence value is valid ,we do obj_cast() for check
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = check_column_cast_validity(val_expr, j)))
          {
            TBSYS_LOG(WARN,"current column with sequence do obj cast failed, ret=%d",ret);
          }
        }
      }//end if
    }//end for

    //update sequence info of prevval after each row was filled
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
    }
  }//end for

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
//        ret = OB_SUCCESS;
        ret = update_by_dml(get_sequence_name_no_dup(i), s_value);
      }
    }//end if
  }//end for
//  my_phy_plan_->get_result_set()->get_session()
  sql_context_->session_info_->set_current_result_set(my_phy_plan_->get_result_set());
  return ret;
}


/**
 *
 * @brief this function is used to update the sequence info map,also call the function "do_fill()" fill the exprs with sequence_value
 * @param input_values [in][out]
 * @param sequence_values [in]
 *
 */
int ObSequenceInsert::fill_sequence_info_to_input_values(ObExprValues *input_values, ObValues *sequence_values)
{
  int ret = OB_SUCCESS;
  if (NULL == input_values || NULL == sequence_values)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "input_values and sequence_values are not inited!");
  }
  if (OB_SUCCESS == ret)
  {
    // 1.将获取的sequence信息首次整合出来，存储到本地，方便使用（输入sequence合理性检查）
    if (OB_SUCCESS != (ret = update_sequence_info_map(true,true)))
    {
      TBSYS_LOG(WARN, "set all sequence rows to map failed! [ret:%d]",ret);
    }
    // 2.遍历所有的列，将sequence填充到列中
    else if (OB_SUCCESS != (ret = do_fill(input_values )))
    {
      TBSYS_LOG(WARN, "fill input values with the sequence info failed!");
    }
  }//end else
  return ret;
}

/**
 *
 * @brief this function is used to construct the IN expr(table_key IN(a,b,c,...)) with key value for get the static data from CS,
 * if U used sequence in INSERT,the sequence value almost will be the key for eache row,so the IN expr must be construct after the filling
 * of sequence info
 * @param input_values [in]
 * @param tem_table [in]
 *
 */

int ObSequenceInsert::gen_static_for_duplication_check(ObValues *tem_table, ObExprValues *input_values)
{
  int ret = OB_SUCCESS;
  const ObRowDesc *row_desc = NULL;
//  ObPhyOperator *op = NULL;
  ObTableRpcScan *table_scan = NULL;
  if (NULL == sql_context_
      || OB_INVALID_ID == table_id_
      || NULL == tem_table
      || NULL == input_values)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "the sql_context and table_id are not inited!");
  }
  if (OB_SUCCESS == ret)
  {
    if (NULL == (table_scan = dynamic_cast<ObTableRpcScan *> (tem_table->get_child(0))))
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "get the table scan op failed!");
    }
  }

  const ObTableSchema *table_schema = NULL;
  if (OB_SUCCESS == ret)
  {
    if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(table_id_)))
    {
      ret = OB_ERR_ILLEGAL_ID;
      TBSYS_LOG(ERROR, "fail to get table schema for table[%ld]", table_id_);
    }
  }

  if (OB_SUCCESS == ret)
  {
    const ObRowkeyInfo &rowkey_info = table_schema->get_rowkey_info();
    if (OB_SUCCESS != (ret = input_values->get_row_desc(row_desc)))
    {
      TBSYS_LOG(ERROR, "get input table desc failed!");
    }

    ObSqlExpression *rows_filter = ObSqlExpression::alloc();
    if (NULL == rows_filter)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "no memory");
    }
    ObSqlExpression column_ref;
    // construct left operand of IN operator
    // the same order with row_desc
    ExprItem expr_item;
    expr_item.type_ = T_REF_COLUMN;
    expr_item.value_.cell_.tid = table_id_;
    int64_t rowkey_column_num = rowkey_info.get_size();
    uint64_t tid = OB_INVALID_ID;
    for (int i = 0; OB_SUCCESS == ret && i < row_desc->get_column_num(); ++i)
    {
      if (OB_UNLIKELY(OB_SUCCESS != (ret = row_desc->get_tid_cid(i, tid, expr_item.value_.cell_.cid))))
      {
        break;
      }
      else if (rowkey_info.is_rowkey_column(expr_item.value_.cell_.cid))
      {
        column_ref.reset();
        column_ref.set_tid_cid(table_id_, expr_item.value_.cell_.cid);
        if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
        {
          TBSYS_LOG(WARN, "failed to add expr item, err=%d", ret);
          break;
        }
        else if (OB_SUCCESS != (ret = column_ref.add_expr_item(expr_item)))
        {
          TBSYS_LOG(WARN, "failed to add expr_item, err=%d", ret);
          break;
        }
        else if (OB_SUCCESS != (ret = column_ref.add_expr_item_end()))
        {
          TBSYS_LOG(WARN, "failed to add expr item, err=%d", ret);
          break;
        }
        else if (OB_SUCCESS != (ret = table_scan->add_output_column(column_ref)))
        {
          TBSYS_LOG(WARN, "failed to add output column, err=%d", ret);
          break;
        }
      }
    } // end for
    // add action flag column
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
      column_ref.reset();
      column_ref.set_tid_cid(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
      if (OB_SUCCESS != (ret = ObSqlExpressionUtil::make_column_expr(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, column_ref)))
      {
        TBSYS_LOG(WARN, "fail to make column expr:ret[%d]", ret);
      }
      else if (OB_SUCCESS != (ret = table_scan->add_output_column(column_ref)))
      {
        TBSYS_LOG(WARN, "failed to add output column, err=%d", ret);
      }
    }
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
      expr_item.type_ = T_OP_ROW;
      expr_item.value_.int_ = rowkey_column_num;
      if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
      {
        TBSYS_LOG(ERROR,"Failed to add expr item, err=%d", ret);
      }
    }
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
      expr_item.type_ = T_OP_LEFT_PARAM_END;
      // a in (a,b,c) => 1 Dim;  (a,b) in ((a,b),(c,d)) =>2 Dim; ((a,b),(c,d)) in (...) =>3 Dim
      expr_item.value_.int_ = 2;
      if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
      {
        TBSYS_LOG(WARN, "failed to add expr item, err=%d", ret);
      }
    }


    if (OB_LIKELY(OB_SUCCESS == ret))
    {
      uint64_t column_id = OB_INVALID_ID;
      int64_t row_num = sequence_columns_mark_.count();
      int64_t col_num = row_desc->get_column_num();
      for (int64_t i = 0; ret == OB_SUCCESS && i < row_num; i++) // for each row
      {
        for (int64_t j = 0; ret == OB_SUCCESS && j < col_num; j++)//for each column
        {

          if (OB_SUCCESS != (ret = row_desc->get_tid_cid(j, tid, column_id)))
          {
            TBSYS_LOG(ERROR,"Failed to get tid cid, err=%d", ret);
          }
          // success
          else if (rowkey_info.is_rowkey_column(column_id))
          {
            // add right oprands of the IN operator
            ObSqlExpression &val_expr = input_values->get_values().at(i * col_num + j);
            for (int64_t k = 0; OB_SUCCESS == ret && k < val_expr.get_expr_obj_size() - 1; k++)//do not need the "END" obj
            {
//              TBSYS_LOG(ERROR, "the duplication obj is ::[%s]",to_cstring(val_expr.get_expr_array()[k]));
              if (OB_SUCCESS != (ret = rows_filter->add_expr_obj(val_expr.get_expr_array()[k])))
              {
                TBSYS_LOG(ERROR, "add input key obj to rows filter failed!");
                break;
              }
            }//end for
          }
        } // end for
        if (OB_LIKELY(ret == OB_SUCCESS))
        {
          if (rowkey_column_num > 0)
          {
            expr_item.type_ = T_OP_ROW;
            expr_item.value_.int_ = rowkey_column_num;
            if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
            {
              TBSYS_LOG(ERROR,"Failed to add expr item, err=%d", ret);
            }
          }
        }
      } // end for

      if (OB_LIKELY(OB_SUCCESS == ret))
      {
        expr_item.type_ = T_OP_ROW;
        expr_item.value_.int_ = row_num;
//        TBSYS_LOG(ERROR, "!!!the row num is::%ld",row_num);
        ExprItem expr_in;
        expr_in.type_ = T_OP_IN;
        expr_in.value_.int_ = 2;
        if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
        {
          TBSYS_LOG(ERROR,"Failed to add expr item, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_in)))
        {
          TBSYS_LOG(ERROR,"Failed to add expr item, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = rows_filter->add_expr_item_end()))
        {
          TBSYS_LOG(ERROR,"Failed to add expr item end, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = table_scan->add_filter(rows_filter)))
        {
          TBSYS_LOG(ERROR,"Failed to add filter, err=%d", ret);
        }
      }
      TBSYS_LOG(DEBUG, "the duplicating expr is ::[%s]",to_cstring(*rows_filter));//add lijianqiang [sequecne insert]
    }
    //add liuzy [sequence] [bugfix] 20160614:b
    /*Exp: free ObSqlExpression*/
    if (NULL != rows_filter && OB_SUCCESS != ret)
    {
      ObSqlExpression::free(rows_filter);
    }
    //add 20160614:e
  }
  return ret;
}


/**
 *
 * @brief this open is finishing three mission:
 * 1.get all the sequence info from CS U input from the cilent and  check the legality of all sequecnes
 * 2.fill the input values with "sequence_value"
 * 3.construct the IN expr for the static data for duplication checking
 *
 */
int ObSequenceInsert::open()
{
  int ret = OB_SUCCESS;
  if (this->get_child_num() < 3)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "sub operator(s) is/are NULL");
  }
  else
  {
    ObValues *sequence_values = NULL; //for sequence static data(info) usage
    ObValues *tem_table = NULL; //for duplication check static data usage
    ObExprValues * input_values = NULL; //for fill input values usage
    if (NULL == (sequence_values = dynamic_cast<ObValues*>(this->get_child(0))))
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "get the first child ::ObValues op of sequence failed!");
    }
    // 1. 首次获得静态equence 数据，主要是做sequence存在性检查
    else if (OB_SUCCESS != (ret = sequence_values->open()))//get static data of sequence
    {
      TBSYS_LOG(ERROR, "failed get static data of sequence!");
    }
    else if (OB_SUCCESS != (ret = init_sequence_info_map()))//init map
    {
      TBSYS_LOG(ERROR, "init sequence name column map failed!");
    }
    else if (NULL == (input_values = dynamic_cast<ObExprValues*>(this->get_child(1))))
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "get the second child ::input values op of sequence failed!");
    }
    /// @todo
    // 2.检查输入的insert值中关于sequence的部分是否合法
//    else if (OB_SUCCESS == (ret = check_the_sequence_format(ObExprValues* input_values)))
//    {
//      TBSYS_LOG(ERROR, "check the sequence format failed!");
//    }
    // 3. 进行填充
    else if (OB_SUCCESS != (ret = fill_sequence_info_to_input_values(input_values, sequence_values)))
    {
      TBSYS_LOG(WARN, "fill sequence info to input values op failed!");
    }
    else if (NULL == (tem_table = dynamic_cast<ObValues*>(this->get_child(2))))
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "get the third child ::duplication check op of sequence failed!");
    }
    // 4. 构造正常流程下的查重静态数据
    else if (OB_SUCCESS != (ret = gen_static_for_duplication_check(tem_table, input_values)))
    {
      TBSYS_LOG(ERROR, "gen_static for duplication check failed!");
    }
  }
  return ret;
}

int ObSequenceInsert::close()
{
  return ObSequence::close();
}

int64_t ObSequenceInsert::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObSequenceInsert\n");
  ObValues *sequence_values = NULL;
  ObValues *tem_table = NULL;
  ObExprValues * input_values = NULL;
  if (NULL != (sequence_values = dynamic_cast<ObValues*>(this->get_child(0))))
  {
    databuff_printf(buf, buf_len, pos, "child0 of ObSequenceInsert::\n");
    pos += sequence_values->to_string(buf + pos,buf_len - pos);
  }
  if (NULL != (input_values = dynamic_cast<ObExprValues*>(this->get_child(1))))
  {
    databuff_printf(buf, buf_len, pos, "Child1 of ObSequenceInsert::\n");
    pos += input_values->to_string(buf + pos,buf_len - pos);
  }
  if (NULL != (tem_table = dynamic_cast<ObValues*>(this->get_child(2))))
  {
    databuff_printf(buf, buf_len, pos, "Child2 of ObSequenceInsert::\n");
    pos += tem_table->to_string(buf + pos,buf_len - pos);
  }
  pos += ObSequence::to_string(buf + pos,buf_len - pos);
  return pos;
}
namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObSequenceInsert, PHY_SEQUENCE_INSERT);
  }
}
//add 20150407:e
