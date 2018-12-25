#include "ob_sequence_stmt.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;


ObSequenceStmt::ObSequenceStmt()
{
  current_expr_has_sequence_ = false;
  is_next_type_ = true;
 //add liuzy [sequence] 20150623:b
  is_replace_ = false;
 //add 20150623:e
}

ObSequenceStmt::~ObSequenceStmt()
{
}

void ObSequenceStmt::print(FILE* fp, int32_t level, int32_t index)
{
  UNUSED(level);
//  print_indentation(fp, level);
  fprintf(fp, "<ObSequenceStmt %d Begin>\n", index);
//  ObStmt::print(fp, level + 1);
//  print_indentation(fp, level + 1);
  fprintf(fp,"the sequence names are::==");
  for (int64_t i = 0; i < sequence_names_no_dup_.count(); i++)
  {
    if (0 == i)
    {
      fprintf(fp,"%.*s",sequence_names_no_dup_.at(i).length(),sequence_names_no_dup_.at(i).ptr());
    }
    else
    {
       fprintf(fp,", %.*s",sequence_names_no_dup_.at(i).length(),sequence_names_no_dup_.at(i).ptr());
    }
  }
//  print_indentation(fp, level);
  fprintf(fp, "<ObSequenceStmt %d End>\n", index);
}
int ObSequenceStmt::sequence_cons_decimal_value(ObDecimal &res, ParseNode* node,
                                                const char* const node_name, uint8_t datatype, int option)
{
  int ret = OB_SUCCESS;
  ObString name = ObString::make_string(node_name);
  if (node->num_child_ <= 0)
  {
    if (MinValue == option)
    {
      if (OB_SUCCESS != (ret = generate_decimal_value(datatype, res, option)))
      {
        TBSYS_LOG(ERROR, "Generate %.*s deciaml value failed.", name.length(), name.ptr());
      }
    }
    else
    {
      if (OB_SUCCESS != (ret = generate_decimal_value(datatype, res)))
      {
        TBSYS_LOG(ERROR, "Generate %.*s deciaml value failed.", name.length(), name.ptr());
      }
    }
  }
  else
  {
    ParseNode* middle_node = node->children_[0];
    switch (middle_node->type_)
    {
      case T_OP_NEG:
      case T_OP_POS:
        middle_node = node->children_[0]->children_[0];
        break;
      case T_INT:
      case T_DECIMAL:
        break;
      default:
        ret = OB_ERR_PARSE_SQL;
        TBSYS_LOG(USER_ERROR, "An unexpected token was found in '%.*s'.", name.length(), name.ptr());
    }
    if (OB_SUCCESS == ret)
    {
      middle_node->type_ = T_DECIMAL;
      std::string numerices("123456789");
      std::string decimal = middle_node->str_value_;
      std::string::size_type pos = decimal.find_first_of(".");
      if (pos != std::string::npos
          && std::string::npos != decimal.find_first_of(numerices, ++pos))
      {
        ret = OB_ERR_PARSE_SQL;
        TBSYS_LOG(USER_ERROR, "The scale of the decimal number must be zero. In '%.*s'.", name.length(), name.ptr());
      }
      else if (OB_SUCCESS != (ret = res.from(middle_node->str_value_)))
      {
        TBSYS_LOG(ERROR, "Fail to convert %.*s to deciaml.", name.length(), name.ptr());
      }
      if (OB_SUCCESS == ret && T_OP_NEG == node->children_[0]->type_)
      {
        res.negate(res);
      }
    }
  }
  return ret;
}
int ObSequenceStmt::sequence_option_validity(int64_t dml_type)
{
  int ret = OB_SUCCESS;
  if (T_SEQUENCE_CREATE == dml_type)
  {
    for (int8_t i = 0; OB_SUCCESS == ret && i < OptionFlag; ++i)
    {
      if (SequenceOption::Set == seq_option_[i].is_set_)
        continue;
      else
      {
        TBSYS_LOG(ERROR, "Option [%d] of sequence is not set.", i);
        ret = OB_ERR_PARSER_SYNTAX;
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    ObDecimal lower_bound, upper_bound;
    if (0 == data_type_)
    {
      if (OB_SUCCESS != (ret = lower_bound.from(OB_SEQUENCE_DEFAULT_MIN_VALUE))
          || OB_SUCCESS != (ret = upper_bound.from(OB_SEQUENCE_DEFAULT_MAX_VALUE)))
      {
        TBSYS_LOG(ERROR, "Convert boundary value of int32 to decimal failed.");
      }
    }
    else if (64 == data_type_)
    {
      if (OB_SUCCESS != (ret = lower_bound.from(OB_SEQUENCE_DEFAULT_MIN_VALUE_FOR_INT64))
          || OB_SUCCESS != (ret = upper_bound.from(OB_SEQUENCE_DEFAULT_MAX_VALUE_FOR_INT64)))
      {
        TBSYS_LOG(ERROR, "Convert boundary value of int64 to decimal failed.");
      }
    }
    else
    {
      if (OB_SUCCESS != (ret = generate_decimal_value(data_type_, upper_bound))
          || OB_SUCCESS != (ret = upper_bound.negate(lower_bound)))
      {
        TBSYS_LOG(ERROR, "Generate boundary value of decimal failed.");
      }
    }
    if (start_with_ < lower_bound || start_with_ > upper_bound)
    {
      TBSYS_LOG(USER_ERROR, "The value of startwith out of range of data type!");
      ret = OB_ERR_PARSE_SQL;
    }
    else if (increment_by_ < lower_bound || increment_by_ > upper_bound)
    {
      TBSYS_LOG(USER_ERROR, "The value of incrementby out of range of data type!");
      ret = OB_ERR_PARSE_SQL;
    }
    else if (min_value_ < lower_bound || min_value_ > upper_bound)
    {
      TBSYS_LOG(USER_ERROR, "The value of minvalue out of range of data type!");
      ret = OB_ERR_PARSE_SQL;
    }
    else if (max_value_ < lower_bound || max_value_ > upper_bound)
    {
      TBSYS_LOG(USER_ERROR, "The value of maxvalue out of range of data type!");
      ret = OB_ERR_PARSE_SQL;
    }
  }
  if (OB_SUCCESS == ret && min_value_ > max_value_)
  {
    TBSYS_LOG(USER_ERROR, "Minvalue must equal or small than maxvalue.");
    ret = OB_ERR_PARSE_SQL;
  }
  return ret;
}
