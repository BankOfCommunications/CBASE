//add lijianqiang [sequence] 20150407:b

#include "ob_sequence_delete.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;


ObSequenceDelete::ObSequenceDelete()
{
}

ObSequenceDelete::~ObSequenceDelete()
{
}

void ObSequenceDelete::reset()
{
  sequence_expr_ids_.clear();
  input_sequence_names_.clear();
  ObSequence::reset();
}

void ObSequenceDelete::reuse()
{
  sequence_expr_ids_.clear();
  input_sequence_names_.clear();
  ObSequence::reuse();
}

int ObSequenceDelete::copy_sequence_info_from_delete_stmt()
{
  int ret = OB_SUCCESS;
  //copy all sequence names
  ObDeleteStmt *delete_stmt = NULL;
  delete_stmt = dynamic_cast<ObDeleteStmt *>(sequence_stmt_);
  if (NULL != delete_stmt)
  {
    ObArray<ObString> &sequence_names = delete_stmt->get_sequence_name_array();
    ObArray<uint64_t> &sequence_expr_ids = delete_stmt->get_sequence_expr_ids_array();
    input_sequence_names_.reserve(sequence_names.count());
    sequence_expr_ids_.reserve(sequence_expr_ids.count());
    //infact ,the num of sequence nams is equal to sequence expr ids
    for (int32_t i = 0; OB_SUCCESS == ret && i < sequence_names.count(); i++)
    {
      if (OB_SUCCESS != (ret = input_sequence_names_.push_back(sequence_names.at(i))))
      {
        TBSYS_LOG(ERROR, "copy sequence names failed!");
      }
    }
    for (int32_t i = 0; OB_SUCCESS == ret && i < sequence_expr_ids.count(); i++)
    {
      if (OB_SUCCESS != (ret = sequence_expr_ids_.push_back(sequence_expr_ids.at(i))))
      {
        TBSYS_LOG(ERROR, "copy sequence expr ids failed!");
      }
    }
  }
  else
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR,"the delete stmt is not inited! ret::[%d]",ret);
  }
  return ret;
}


int ObSequenceDelete::open()
{
  return ObSequence::open();
}



/**
 *
 * @brief IN DELETE statement,only can use the prevval,this function is fill the sequence info tu the condition expr
 * @param filter [in][out] in with sequence_name(varchar),out with sequence value(decimal)
 * @param cond_expr_id [in] the conditon expr id generated in logical plan
 *
 */
int ObSequenceDelete::fill_the_sequence_info_to_cond_expr(ObSqlExpression *filter, uint64_t cond_expr_id)
{
  int ret = OB_SUCCESS;
  UNUSED(cond_expr_id);
  TBSYS_LOG(DEBUG, "fill sequence info to DELETE condition");
  ObPostfixExpression::ExprArray &post_expr_array = filter->get_expr_array();
  /// simple expr:COLUMN_IDX | TID | CID | CONST_OBJ |  OBJ      | OP | T_OP_EQ | OP_COUNT | END
  ///                   0       1     2       3          4          5      6         7        8
  ///                 flag                   falg     real_val         op_type
  ObObj *expr_array_obj = NULL;
  for (int64_t i = 0; OB_SUCCESS == ret && i < input_sequence_names_.count(); i++)//for each sequence
  {
    for (int64_t j = 0; OB_SUCCESS == ret && j < post_expr_array.count(); j++)//for each obj
    {
      ObString sequence_name;
      ObString preval_val;
      bool can_use_prevval = false;
      ObObjType obj_type;
      int64_t expr_type = -1;
      if (ObIntType == post_expr_array[j].get_type())
      {
        if (OB_SUCCESS != (ret = post_expr_array[j].get_int(expr_type)))
        {
          TBSYS_LOG(WARN, "fail to get int value.err=%d", ret);
        }
        else if ((int64_t)ObPostfixExpression::CONST_OBJ == expr_type)//const obj
        {
          expr_array_obj = const_cast<ObObj *>(&post_expr_array[++j]);
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
            else if(sequence_name != input_sequence_names_.at(i))
            {
              TBSYS_LOG(DEBUG,"not a sequence varchar");
              continue;//not a sequence varchar
            }
            else//find
            {
              TBSYS_LOG(DEBUG, "current sequence is::[%.*s]",sequence_name.length(),sequence_name.ptr());
              if (OB_SUCCESS != (ret = get_prevval_by_name(&sequence_name, preval_val, can_use_prevval)))
              {
                TBSYS_LOG(ERROR, "get the info of sequence::[%.*s] failed",sequence_name.length(),sequence_name.ptr());
              }
              if (OB_SUCCESS == ret && can_use_prevval)
              {
                expr_array_obj->set_decimal(preval_val);
                TBSYS_LOG(DEBUG,"set decimal,the obj is::[%s]",to_cstring(post_expr_array[4]));
              }
              else
              {
                if (OB_SUCCESS == ret && !can_use_prevval)
                {
                  ret = OB_ERROR;
                  TBSYS_LOG(USER_ERROR, "THE PREVVAL expression of  sequence [%.*s] can't be used before using NEXTVAL",sequence_name.length(),sequence_name.ptr());
                }
              }
            }
          }
        }
      }
    }//end for (int64_t j = 0
  }//end for (int64_t i = 0...
  return ret;
}

int ObSequenceDelete::close()
{
  int ret = OB_SUCCESS;
  return ret;
}

int64_t ObSequenceDelete::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObSequence Delete");
  pos += ObSequence::to_string(buf + pos,buf_len - pos);
  for (int64_t i = 0; i < sequence_expr_ids_.count(); i++)
  {

  }
  return pos;
}
namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObSequenceDelete, PHY_SEQUENCE_DELETE);
  }
}
//add 20150407:e

