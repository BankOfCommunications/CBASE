#include "sql_parser.tab.h"
#include "build_plan.h"
#include "dml_build_plan.h"
#include "priv_build_plan.h"
#include "ob_raw_expr.h"
#include "common/ob_bit_set.h"
#include "ob_select_stmt.h"
#include "ob_multi_logic_plan.h"
#include "ob_insert_stmt.h"
#include "ob_delete_stmt.h"
#include "ob_update_stmt.h"
#include "ob_schema_checker.h"
#include "ob_explain_stmt.h"
#include "ob_create_table_stmt.h"
#include "ob_drop_table_stmt.h"
#include "ob_truncate_table_stmt.h" //add zhaoqiong [Truncate Table]:20160318
#include "ob_show_stmt.h"
#include "ob_create_user_stmt.h"
#include "ob_prepare_stmt.h"
#include "ob_variable_set_stmt.h"
#include "ob_execute_stmt.h"
#include "ob_deallocate_stmt.h"
#include "ob_start_trans_stmt.h"
#include "ob_end_trans_stmt.h"
#include "ob_column_def.h"
#include "ob_alter_table_stmt.h"
#include "ob_alter_sys_cnf_stmt.h"
#include "ob_kill_stmt.h"
#include "parse_malloc.h"
#include "common/ob_define.h"
#include "common/ob_array.h"
#include "common/ob_string_buf.h"
#include "common/utility.h"
#include "common/ob_schema_service.h"
#include "common/ob_obi_role.h"
#include "ob_change_obi_stmt.h"
#include <stdint.h>
//add liumz, [multi_database.sql]:20150613
#include "ob_sql_session_info.h"
#include "parse_node.h"

//add wenghaixing [secondary index] 20141024
#include "ob_create_index_stmt.h"
#include "ob_drop_index_stmt.h"
//add e
using namespace oceanbase::common;
using namespace oceanbase::sql;

int resolve_multi_stmt(ResultPlan* result_plan, ParseNode* node);
int resolve_explain_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
int resolve_const_value(
    ResultPlan * result_plan,
    ParseNode *def_node,
    ObObj& default_value);
int resolve_column_definition(
    ResultPlan * result_plan,
    ObColumnDef& col_def,
    ParseNode* node,
    bool *is_primary_key = NULL);
int resolve_table_elements(
    ResultPlan * result_plan,
    ObCreateTableStmt& create_table_stmt,
    ParseNode* node);
int resolve_create_table_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
//add by wenghaixing[secondary index] 20141024
int resolve_create_index_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
//add e
int resolve_drop_table_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
int resolve_show_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
//add wenghaixing [secondary index drop index]20141222
int resolve_drop_index_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id
        );
//add e
int resolve_prepare_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
int resolve_variable_set_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
int resolve_execute_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
int resolve_deallocate_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
int resolve_alter_sys_cnf_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
int resolve_kill_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
int resolve_change_obi(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);

int resolve_multi_stmt(ResultPlan* result_plan, ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node && node->type_ == T_STMT_LIST);
  if(node->num_child_ == 0)
  {
    ret = OB_ERROR;
  }
  else
  {
    result_plan->plan_tree_ = NULL;
    ObMultiLogicPlan* multi_plan = (ObMultiLogicPlan*)parse_malloc(sizeof(ObMultiLogicPlan), result_plan->name_pool_);
    if (multi_plan != NULL)
    {
      multi_plan = new(multi_plan) ObMultiLogicPlan;
      for(int32_t i = 0; i < node->num_child_; ++i)
      {
        ParseNode* child_node = node->children_[i];
        if (child_node == NULL)
          continue;

        if ((ret = resolve(result_plan, child_node)) != OB_SUCCESS)
        {
          multi_plan->~ObMultiLogicPlan();
          parse_free(multi_plan);
          multi_plan = NULL;
          break;
        }
        if(result_plan->plan_tree_ == NULL)
          continue;

        if ((ret = multi_plan->push_back((ObLogicalPlan*)(result_plan->plan_tree_))) != OB_SUCCESS)
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Can not add logical plan to ObMultiLogicPlan");
          break;
        }
        result_plan->plan_tree_ = NULL;
      }
      result_plan->plan_tree_ = multi_plan;
    }
    else
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc space for ObMultiLogicPlan");
    }
  }
  return ret;
}

int resolve_explain_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node && node->type_ == T_EXPLAIN && node->num_child_ == 1);
  ObLogicalPlan* logical_plan = NULL;
  ObExplainStmt* explain_stmt = NULL;
  query_id = OB_INVALID_ID;


  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc ObLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {
    explain_stmt = (ObExplainStmt*)parse_malloc(sizeof(ObExplainStmt), result_plan->name_pool_);
    if (explain_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc ObExplainStmt");
    }
    else
    {
      explain_stmt = new(explain_stmt) ObExplainStmt();
      query_id = logical_plan->generate_query_id();
      explain_stmt->set_query_id(query_id);
      ret = logical_plan->add_query(explain_stmt);
      if (ret != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not add ObDeleteStmt to logical plan");
      }
      else
      {
        if (node->value_ > 0)
          explain_stmt->set_verbose(true);
        else
          explain_stmt->set_verbose(false);

        uint64_t sub_query_id = OB_INVALID_ID;
        switch (node->children_[0]->type_)
        {
          case T_SELECT:
            ret = resolve_select_stmt(result_plan, node->children_[0], sub_query_id);
            break;
          case T_DELETE:
            ret = resolve_delete_stmt(result_plan, node->children_[0], sub_query_id);
            break;
          case T_INSERT:
            ret = resolve_insert_stmt(result_plan, node->children_[0], sub_query_id);
            break;
          case T_UPDATE:
            ret = resolve_update_stmt(result_plan, node->children_[0], sub_query_id);
            break;
          default:
            ret = OB_ERR_PARSER_SYNTAX;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                "Wrong statement in explain statement");
            break;
        }
        if (ret == OB_SUCCESS)
          explain_stmt->set_explain_query_id(sub_query_id);
      }
    }
  }
  return ret;
}

int resolve_column_definition(
    ResultPlan * result_plan,
    ObColumnDef& col_def,
    ParseNode* node,
    bool *is_primary_key)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node->type_ == T_COLUMN_DEFINITION);
  OB_ASSERT(node->num_child_ >= 3);
  if (is_primary_key)
    *is_primary_key = false;

  col_def.action_ = ADD_ACTION;
  OB_ASSERT(node->children_[0]->type_== T_IDENT);
  col_def.column_name_.assign_ptr(
      (char*)(node->children_[0]->str_value_),
      static_cast<int32_t>(strlen(node->children_[0]->str_value_))
      );

  ParseNode *type_node = node->children_[1];
  OB_ASSERT(type_node != NULL);
  //add liu jun.[fix create table bug] 20150519:b
  int32_t len = static_cast<int32_t>(strlen(node->children_[0]->str_value_));
  ObString column_name(len, len, node->children_[0]->str_value_);
  if(len >= OB_MAX_COLUMN_NAME_LENGTH)
  {
    ret = OB_ERR_COLUMN_NAME_LENGTH;
    PARSER_LOG("Column name is too long, column_name=%.*s", column_name.length(), column_name.ptr());
  }
  else
  {
  //20150519:e
    switch(type_node->type_)
    {
      case T_TYPE_INTEGER:
      //mod lijianqiang [INT_32] 20151008:b
        //col_def.data_type_ = ObIntType;
        col_def.data_type_ = ObInt32Type;
        break;
      case T_TYPE_BIG_INTEGER:
        col_def.data_type_ = ObIntType;
        break;
      //mod 20151008:e
      case T_TYPE_DECIMAL:
        col_def.data_type_ = ObDecimalType;
        if (type_node->num_child_ >= 1 && type_node->children_[0] != NULL)
          col_def.precision_ = type_node->children_[0]->value_;
        if (type_node->num_child_ >= 2 && type_node->children_[1] != NULL)
          col_def.scale_ = type_node->children_[1]->value_;
        //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
        /*
         *建表的时候要对用户输入的decimal的参数做正确性检查
         * report wrong info when input precision<scale
         * */
          if (col_def.precision_ <= col_def.scale_||col_def.precision_>MAX_DECIMAL_DIGIT||col_def.scale_>MAX_DECIMAL_SCALE||col_def.precision_<=0||type_node->num_child_==0)
          {
                    ret = OB_ERR_WRONG_DYNAMIC_PARAM;
                    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                            "You have an error in your SQL syntax; check the param of decimal! precision = %ld,scale=%ld",
                            col_def.precision_, col_def.scale_);
          }
          //add:e
        break;
      case T_TYPE_BOOLEAN:
        col_def.data_type_ = ObBoolType;
        break;
      case T_TYPE_FLOAT:
        col_def.data_type_ = ObFloatType;
        if (type_node->num_child_ >= 1 && type_node->children_[0] != NULL)
          col_def.precision_ = type_node->children_[0]->value_;
        break;
      case T_TYPE_DOUBLE:
        col_def.data_type_ = ObDoubleType;
        break;
      //mod peiouya [DATE_TIME] 20150906:b
      case T_TYPE_DATE:
        //col_def.data_type_ = ObPreciseDateTimeType;
        col_def.data_type_ = ObDateType;
        break;
      case T_TYPE_TIME:
        //col_def.data_type_ = ObPreciseDateTimeType;
        col_def.data_type_ = ObTimeType;
        //del peiouya [DATE_TIME] 20150831:b
        /*
        if (type_node->num_child_ >= 1 && type_node->children_[0] != NULL)
          col_def.precision_ = type_node->children_[0]->value_;
        */
      //del 20150831:e
        break;
      //mod 20150906:e
      case T_TYPE_TIMESTAMP:
        col_def.data_type_ = ObPreciseDateTimeType;
        if (type_node->num_child_ >= 1 && type_node->children_[0] != NULL)
          col_def.precision_ = type_node->children_[0]->value_;
        break;
      case T_TYPE_CHARACTER:
        col_def.data_type_ = ObVarcharType;
        if (type_node->num_child_ >= 1 && type_node->children_[0] != NULL)
          col_def.type_length_= type_node->children_[0]->value_;
        break;
      case T_TYPE_VARCHAR:
        col_def.data_type_ = ObVarcharType;
        if (type_node->num_child_ >= 1 && type_node->children_[0] != NULL)
          //设置varchar的长度
          col_def.type_length_= type_node->children_[0]->value_;
        break;
      case T_TYPE_CREATETIME:
        col_def.data_type_ = ObCreateTimeType;
        break;
      case T_TYPE_MODIFYTIME:
        col_def.data_type_ = ObModifyTimeType;
        break;
      default:
        ret = OB_ERR_ILLEGAL_TYPE;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Unsupport data type of column definiton, column name = %s", node->children_[0]->str_value_);
        break;
    }
  }

  ParseNode *attrs_node = node->children_[2];
  for(int32_t i = 0; ret == OB_SUCCESS && attrs_node && i < attrs_node->num_child_; i++)
  {
    ParseNode* attr_node = attrs_node->children_[i];
    switch(attr_node->type_)
    {
      case T_CONSTR_NOT_NULL:
        col_def.not_null_ = true;
        break;
      case T_CONSTR_NULL:
        col_def.not_null_ = false;
        break;
      case T_CONSTR_AUTO_INCREMENT:
        if (col_def.data_type_ != ObIntType && col_def.data_type_ != ObFloatType
          && col_def.data_type_ != ObDoubleType && col_def.data_type_ != ObDecimalType
          && col_def.data_type_ != ObInt32Type)//add lijianqiang [INT_32] create table bug fix 20151020
        {
          ret = OB_ERR_PARSER_SYNTAX;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "Incorrect column specifier for column '%s'", node->children_[0]->str_value_);
          break;
        }
        col_def.atuo_increment_ = true;
        break;
      case T_CONSTR_PRIMARY_KEY:
        if (is_primary_key != NULL)
        {

          *is_primary_key = true;
        }
        break;
      case T_CONSTR_DEFAULT:
        ret = resolve_const_value(result_plan, attr_node, col_def.default_value_);
        break;
      default:  // won't be here
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Wrong column constraint");
        break;
    }
    //del peiouya [NotNULL_check] [JHOBv0.1] 20131208:b
  /*expr:Remove null check constraints codes associated with default value*/
    //if (ret == OB_SUCCESS && col_def.default_value_.get_type() == ObNullType
    // && (col_def.not_null_ || col_def.primary_key_id_ > 0))
    //{
    //  ret = OB_ERR_ILLEGAL_VALUE;
    //  snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
    //      "Invalid default value for '%s'", node->children_[0]->str_value_);
    //}
  //del 20131208:e
  }
  return ret;
}

int resolve_const_value(
    ResultPlan * result_plan,
    ParseNode *def_node,
    ObObj& default_value)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (def_node != NULL)
  {
    ParseNode *def_val = def_node;
    if (def_node->type_ == T_CONSTR_DEFAULT)
      def_val = def_node->children_[0];
    ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
    ObString str;
    ObObj val;
    switch (def_val->type_)
    {
      case T_INT:
        default_value.set_int(def_val->value_);
        break;
      case T_STRING:
      case T_BINARY:
        if ((ret = ob_write_string(*name_pool,
                                    ObString::make_string(def_val->str_value_),
                                    str)) != OB_SUCCESS)
        {
          PARSER_LOG("Can not malloc space for default value");
          break;
        }
        default_value.set_varchar(str);
        break;
      case T_DATE:
        default_value.set_precise_datetime(def_val->value_);
        break;
      //add peiouya [DATE_TIME] 20150912:b
      case T_DATE_NEW:
        default_value.set_date (def_val->value_);
        break;
      case T_TIME:
        default_value.set_time (def_val->value_);
        break;
      //add 20150912:e
      case T_FLOAT:
        default_value.set_float(static_cast<float>(atof(def_val->str_value_)));
        break;
      case T_DOUBLE:
        default_value.set_double(atof(def_val->str_value_));
        break;
      case T_DECIMAL: // set as string
        if ((ret = ob_write_string(*name_pool,
                                    ObString::make_string(def_val->str_value_),
                                    str)) != OB_SUCCESS)
        {
          PARSER_LOG("Can not malloc space for default value");
          break;
        }
        default_value.set_varchar(str);
        default_value.set_type(ObDecimalType);
        break;
      case T_BOOL:
        default_value.set_bool(def_val->value_ == 1 ? true : false);
        break;
      case T_NULL:
        default_value.set_type(ObNullType);
        break;
      default:
        ret = OB_ERR_ILLEGAL_TYPE;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Illigeal type of default value");
        break;
    }
  }
  return ret;
}

int resolve_table_elements(
    ResultPlan * result_plan,
    ObCreateTableStmt& create_table_stmt,
    ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node->type_ == T_TABLE_ELEMENT_LIST);
  OB_ASSERT(node->num_child_ >= 1);

  ParseNode *primary_node = NULL;
  for(int32_t i = 0; ret == OB_SUCCESS && i < node->num_child_; i++)
  {
    ParseNode* element = node->children_[i];
    if (OB_LIKELY(element->type_ == T_COLUMN_DEFINITION))
    {
      ObColumnDef col_def;
      bool is_primary_key = false;
      col_def.column_id_ = create_table_stmt.gen_column_id();
      if ((ret = resolve_column_definition(result_plan, col_def, element, &is_primary_key)) != OB_SUCCESS)
      {
        break;
      }
      else if (is_primary_key)
      {
        if (create_table_stmt.get_primary_key_size() > 0)
        {
          ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
          PARSER_LOG("Multiple primary key defined");
          break;
        }
        else if ((ret = create_table_stmt.add_primary_key_part(col_def.column_id_)) != OB_SUCCESS)
        {
          PARSER_LOG("Add primary key failed");
          break;
        }
        else
        {
          col_def.primary_key_id_ = create_table_stmt.get_primary_key_size();
        }
      }
      ret = create_table_stmt.add_column_def(*result_plan, col_def);
    }
    else if (element->type_ == T_PRIMARY_KEY)
    {
      if (primary_node == NULL)
      {
        primary_node = element;
      }
      else
      {
        ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
        PARSER_LOG("Multiple primary key defined");
      }
    }
    else
    {
      /* won't be here */
      OB_ASSERT(0);
    }
  }

  if (ret == OB_SUCCESS)
  {
    if (OB_UNLIKELY(create_table_stmt.get_primary_key_size() > 0 && primary_node != NULL))
    {
      ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
      PARSER_LOG("Multiple primary key defined");
    }
    else if (primary_node != NULL)
    {
      ParseNode *key_node = NULL;
      for(int32_t i = 0; ret == OB_SUCCESS && i < primary_node->children_[0]->num_child_; i++)
      {
        key_node = primary_node->children_[0]->children_[i];
        ObString key_name;
        key_name.assign_ptr(
            (char*)(key_node->str_value_),
            static_cast<int32_t>(strlen(key_node->str_value_))
            );
        ret = create_table_stmt.add_primary_key_part(*result_plan, key_name);
      }
    }
  }

  return ret;
}

int resolve_create_table_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node && node->type_ == T_CREATE_TABLE && node->num_child_ == 4);
  ObLogicalPlan* logical_plan = NULL;
  ObCreateTableStmt* create_table_stmt = NULL;
  query_id = OB_INVALID_ID;


  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc ObLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {
    create_table_stmt = (ObCreateTableStmt*)parse_malloc(sizeof(ObCreateTableStmt), result_plan->name_pool_);
    if (create_table_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc ObExplainStmt");
    }
    else
    {
      create_table_stmt = new(create_table_stmt) ObCreateTableStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      create_table_stmt->set_query_id(query_id);
      ret = logical_plan->add_query(create_table_stmt);
      if (ret != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not add ObCreateTableStmt to logical plan");
      }
    }
  }

  if (ret == OB_SUCCESS)
  {
    if (node->children_[0] != NULL)
    {
      OB_ASSERT(node->children_[0]->type_ == T_IF_NOT_EXISTS);
      create_table_stmt->set_if_not_exists(true);
    }
    //add dolphin [database manager]@20150614:b
    ParseNode* relation = node->children_[1];
    OB_ASSERT(relation->type_ == T_RELATION);
    ObString db_name;
    if(relation->children_[0] != NULL)
      db_name.assign_ptr((char*)(relation->children_[0]->str_value_),
                         static_cast<int32_t>(strlen(relation->children_[0]->str_value_)));

    if(db_name.length() <= 0)
    {
      db_name = static_cast<ObSQLSessionInfo*>(result_plan->session_info_)->get_db_name();
    }
    if(db_name.length() <= 0 || db_name.length() >= OB_MAX_DATBASE_NAME_LENGTH)//mod liumz [name_length]
    {
      ret = OB_INVALID_ARGUMENT;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "database name is not valid,you must specify the correct database name when create table.");
    }
    //add:e
    ObString table_name;
    //modify dolphin [database manager]@20150614:b
    /**
    table_name.assign_ptr(
        (char*)(node->children_[1]->str_value_),
        static_cast<int32_t>(strlen(node->children_[1]->str_value_))
        );
    */
    if(OB_SUCCESS == ret) {//add dolphin [database manager]@20150616
      table_name.assign_ptr(
              (char *) (node->children_[1]->children_[1]->str_value_),
              static_cast<int32_t>(strlen(node->children_[1]->children_[1]->str_value_))
      );
      //add dolphin [database manager]@20150616:b
      if (table_name.length() <= 0 || table_name.length()>= OB_MAX_TABLE_NAME_LENGTH) {//mod liumz [name_length]
        ret = OB_INVALID_ARGUMENT;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "table_name is invalid.");
      }
    }
    if(ret == OB_SUCCESS) {
      //add:e
      //add liumz, [database manager.dolphin_bug_fix]20150708:b
      if ((ret = create_table_stmt->set_db_name(*result_plan, db_name)) != OB_SUCCESS) {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Add db name to ObCreateTableStmt failed");
      }
      //add:e
      if ((ret = create_table_stmt->set_table_name(*result_plan, table_name)) != OB_SUCCESS) {
        //snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
        //    "Add table name to ObCreateTableStmt failed");
      }
      /* del liumz, [database manager.dolphin_bug_fix]20150708
      //add dolphin [database manager]@20150625
      if ((ret = create_table_stmt->set_db_name(*result_plan, db_name)) != OB_SUCCESS) {
        //snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
        //    "Add table name to ObCreateTableStmt failed");
      }*/
    }
  }

  if (ret == OB_SUCCESS)
  {
    OB_ASSERT(node->children_[2]->type_ == T_TABLE_ELEMENT_LIST);
    ret = resolve_table_elements(result_plan, *create_table_stmt, node->children_[2]);
  }

  if (ret == OB_SUCCESS && node->children_[3])
  {
    OB_ASSERT(node->children_[3]->type_ == T_TABLE_OPTION_LIST);
    ObString str;
    ParseNode *option_node = NULL;
    int32_t num = node->children_[3]->num_child_;
    for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
    {
      option_node = node->children_[3]->children_[i];
      switch (option_node->type_)
      {
        case T_JOIN_INFO:
          str.assign_ptr(
              const_cast<char*>(option_node->children_[0]->str_value_),
              static_cast<int32_t>(option_node->children_[0]->value_));
          if ((ret = create_table_stmt->set_join_info(str)) != OB_SUCCESS)
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                "Set JOIN_INFO failed");
          break;
        case T_EXPIRE_INFO:
          str.assign_ptr(
              (char*)(option_node->children_[0]->str_value_),
              static_cast<int32_t>(strlen(option_node->children_[0]->str_value_))
              );
          if ((ret = create_table_stmt->set_expire_info(str)) != OB_SUCCESS)
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                "Set EXPIRE_INFO failed");
          break;
        case T_TABLET_MAX_SIZE:
          create_table_stmt->set_tablet_max_size(option_node->children_[0]->value_);
          break;
        case T_TABLET_BLOCK_SIZE:
          create_table_stmt->set_tablet_block_size(option_node->children_[0]->value_);
          break;
        case T_TABLET_ID:
          create_table_stmt->set_table_id(
                                 *result_plan,
                                 static_cast<uint64_t>(option_node->children_[0]->value_)
                                 );
          break;
        case T_REPLICA_NUM:
          create_table_stmt->set_replica_num(static_cast<int32_t>(option_node->children_[0]->value_));
          break;
        case T_COMPRESS_METHOD:
          str.assign_ptr(
              (char*)(option_node->children_[0]->str_value_),
              static_cast<int32_t>(strlen(option_node->children_[0]->str_value_))
              );
          if ((ret = create_table_stmt->set_compress_method(str)) != OB_SUCCESS)
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                "Set COMPRESS_METHOD failed");
          break;
        case T_USE_BLOOM_FILTER:
          create_table_stmt->set_use_bloom_filter(option_node->children_[0]->value_ ? true : false);
          break;
        case T_CONSISTENT_MODE:
          create_table_stmt->set_consistency_level(option_node->value_);
          break;
        case T_COMMENT:
          str.assign_ptr(
              (char*)(option_node->children_[0]->str_value_),
              static_cast<int32_t>(strlen(option_node->children_[0]->str_value_))
              );
          if ((ret = create_table_stmt->set_comment_str(str)) != OB_SUCCESS)
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                "Set COMMENT failed");
          break;
        default:
          /* won't be here */
          OB_ASSERT(0);
          break;
      }
    }
  }
  return ret;
}
//add by wenghaixing[secondary index] 20141024
int resolve_create_index_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{

  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_CREATE_INDEX && node->num_child_ == 4);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObCreateIndexStmt* create_index_stmt = NULL;
  ObLogicalPlan* logical_plan = NULL;
  query_id = OB_INVALID_ID;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  ObString indexed_table;
  //add wenghaixing [secondary index drop index]20141223
  uint64_t src_tid = OB_INVALID_ID;
  //add e
  if (NULL == result_plan->plan_tree_)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (NULL == logical_plan)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
         "Can not malloc ObLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }
  if(OB_SUCCESS == ret)
  {
    create_index_stmt = (ObCreateIndexStmt*)parse_malloc(sizeof(ObCreateIndexStmt),result_plan->name_pool_);
    if(NULL == create_index_stmt)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Can not malloc ObCreateIndexStmt");
    }
    else
    {
      create_index_stmt = new(create_index_stmt)ObCreateIndexStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      create_index_stmt->set_query_id(query_id);
      if(OB_SUCCESS != (ret = logical_plan->add_query(create_index_stmt)))
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Can not add ObCreateIndexStmt to logical plan");
      }
    }
  }
  if(OB_SUCCESS == ret&&node->children_[0])
  {

    //add by zhangcd [multi_database.secondary_index] 20150701:b
    ObString db_name;
    ObString table_name;
    ObString complete_table_name;
    ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
    ParseNode *table_node = node->children_[0];
    if(table_node->num_child_ != 2)
    {
      ret = OB_ERROR;
      PARSER_LOG("Parse failed!");
    }
    else if(table_node->children_[0] == NULL && table_node->children_[1] != NULL)
    {
      ObSQLSessionInfo* session_info = static_cast<ObSQLSessionInfo*>(result_plan->session_info_);
      db_name = session_info->get_db_name();
      table_name = ObString::make_string(table_node->children_[1]->str_value_);
      char *ct_name = NULL;
      ct_name = static_cast<char *>(name_pool->alloc(OB_MAX_COMPLETE_TABLE_NAME_LENGTH));
      if(NULL == ct_name)
      {
        ret = OB_ERROR;
        PARSER_LOG("Memory over flow!");
      }
      else if(db_name.length() >= OB_MAX_DATBASE_NAME_LENGTH)
      {
        ret = OB_ERROR;
        PARSER_LOG("database name is too long!");
      }
      else if(table_name.length() >= OB_MAX_TABLE_NAME_LENGTH)
      {
        ret = OB_ERROR;
        PARSER_LOG("table name is too long!");
      }

      if(OB_SUCCESS == ret)
      {
        complete_table_name.assign_buffer(ct_name, OB_MAX_COMPLETE_TABLE_NAME_LENGTH);
        // mod zhangcd [multi_database.seconary_index] 20150721:b
        //complete_table_name.concat(db_name, table_name, '.');
        complete_table_name.write(db_name.ptr(), db_name.length());
        complete_table_name.write(".", 1);
        complete_table_name.write(table_name.ptr(), table_name.length());
        // mod:e
      }
    }
    else if((table_node->children_[0] != NULL && table_node->children_[1] != NULL))
    {
      db_name = ObString::make_string(table_node->children_[0]->str_value_);
      table_name = ObString::make_string(table_node->children_[1]->str_value_);
      char *ct_name = NULL;
      ct_name = static_cast<char *>(name_pool->alloc(OB_MAX_COMPLETE_TABLE_NAME_LENGTH));
      if(NULL == ct_name)
      {
        ret = OB_ERROR;
        PARSER_LOG("Memory over flow!");
      }
      else if(db_name.length() >= OB_MAX_DATBASE_NAME_LENGTH)
      {
        ret = OB_ERROR;
        PARSER_LOG("database name is too long!");
      }
      else if(table_name.length() >= OB_MAX_TABLE_NAME_LENGTH)
      {
        ret = OB_ERROR;
        PARSER_LOG("table name is too long!");
      }

      if(OB_SUCCESS == ret)
      {
        complete_table_name.assign_buffer(ct_name, OB_MAX_COMPLETE_TABLE_NAME_LENGTH);
        // mod zhangcd [multi_database.seconary_index] 20150721:b
//        complete_table_name.concat(db_name, table_name, '.');
        complete_table_name.write(db_name.ptr(), db_name.length());
        complete_table_name.write(".", 1);
        complete_table_name.write(table_name.ptr(), table_name.length());
        // mod:e
      }
    }
    else
    {
      ret = OB_ERROR;
      PARSER_LOG("Parse failed!");
    }
    //add:e

    //del by zhangcd [multi_database.secondary_index] 20150701:b
/*
    indexed_table.assign_ptr(
                    (char*)(node->children_[0]->str_value_),
                    static_cast<int32_t>(strlen(node->children_[0]->str_value_))
                );
*/
    //del:e

    //add liumz, 20150725:b
    if (OB_SUCCESS == ret)
    {//add:e
      //add by zhangcd [multi_database.secondary_index] 20150701:b
      indexed_table = complete_table_name;
      //add:e

      if(OB_SUCCESS != (ret = create_index_stmt->set_idxed_name(*result_plan,indexed_table)))
      {
      }
      //add wenghaixing[secondary index drop index]20141223
      else
      {
        ObSchemaChecker* schema_checker = NULL;
        schema_checker = static_cast<ObSchemaChecker*>(result_plan->schema_checker_);
        if (NULL == schema_checker)
        {
          ret = OB_ERR_SCHEMA_UNSET;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Schema(s) are not set");
        }
        if (OB_SUCCESS == ret)
        {
          if((src_tid = schema_checker->get_table_id(indexed_table)) == OB_INVALID_ID)
          {
            ret = OB_ENTRY_NOT_EXIST;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "the table to create index '%.*s' is not exist", indexed_table.length(), indexed_table.ptr());
          }
        }

      }
      //add e
    }
  }
  if(OB_SUCCESS == ret)
  {
    OB_ASSERT(node->children_[1]&&node->children_[2]);
    ObString index_lable;
    //add wenghaixing[secondary index drop index]20141223
    char str[OB_MAX_TABLE_NAME_LENGTH];
    memset(str,0,OB_MAX_TABLE_NAME_LENGTH);
    int64_t str_len = 0;
    //add e
    index_lable.assign_ptr(
                (char*)(node->children_[2]->str_value_),
                static_cast<int32_t>(strlen(node->children_[2]->str_value_))
                );
    //add wenghaixing[secondary index drop index]20141223
    if(OB_SUCCESS != (ret = generate_index_table_name(index_lable,src_tid,str,str_len)))
    {
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "index name is invalid!");
    }
    else
    {
      index_lable.reset();
      index_lable.assign_ptr(str,(int32_t)str_len);
    }
    //add e
    if(OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = create_index_stmt->set_index_label(*result_plan,index_lable)))
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                         "invalid index table name,maybe tname[%s] is already exist!",str);
      }
      else
      {
        ParseNode *column_node = NULL;
        ObString index_column;
        /*
        if(0 == node->children_[1]->num_child_)
        {
          column_node=node->children_[1];
          index_column.reset();
          index_column.assign_ptr(
                          (char*)(column_node->str_value_),
                          static_cast<int32_t>(strlen(column_node->str_value_))
                          );
          if(OB_SUCCESS != (ret = create_index_stmt->add_index_colums(*result_plan,indexed_table,index_column)))
          {
             //snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             //"failed to add_index_colums!");
          }
         }*/
         for(int32_t i = 0; i < node->children_[1]->num_child_; i ++)
         {
           column_node = node->children_[1]->children_[i];
           index_column.reset();
           index_column.assign_ptr(
                          (char*)(column_node->str_value_),
                           static_cast<int32_t>(strlen(column_node->str_value_))
                            );
           if(OB_SUCCESS != (ret = create_index_stmt->add_index_colums(*result_plan,indexed_table,index_column)))
           {
             // snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             //"failed to add_index_colums!");
             break;
           }
          }
        }
      }
    }
    if(OB_SUCCESS == ret)
    {
      ObString storing_column;
      ParseNode *column_node = NULL;
      //add wenghaixing[secondary index create fix]20141225
      if(NULL == node->children_[3])
      {
         create_index_stmt->set_has_storing(false);
      }
      else
      //add e
      /*
      if(0 == node->children_[3]->num_child_)
      {
        column_node = node->children_[3];
        storing_column.reset();
        storing_column.assign_ptr(
                        (char*)(column_node->str_value_),
                        static_cast<int32_t>(strlen(column_node->str_value_))
                        );
        if(OB_SUCCESS != (ret = create_index_stmt->set_storing_columns(*result_plan,indexed_table,storing_column)))
        {
               // snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                         //"failed to add_index_colums!");
        }
      }
      else */
      {
        for(int32_t i = 0; i < node->children_[3]->num_child_; i ++)
        {
          column_node = node->children_[3]->children_[i];
          storing_column.reset();
          storing_column.assign_ptr(
                            (char*)(column_node->str_value_),
                            static_cast<int32_t>(strlen(column_node->str_value_))
                            );
          if(OB_SUCCESS != (ret = create_index_stmt->set_storing_columns(*result_plan,indexed_table,storing_column)))
          {
                   // snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                            // "failed to add_index_colums!");
                    break;
          }
        }
      };
    }
    return ret;
}
//add e
int resolve_alter_table_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_ALTER_TABLE && node->num_child_ == 2);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObAlterTableStmt* alter_table_stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, alter_table_stmt)))
  {
  }
  else
  {
    alter_table_stmt->set_name_pool(static_cast<ObStringBuf*>(result_plan->name_pool_));
    OB_ASSERT(node->children_[0]);
    OB_ASSERT(node->children_[1] && node->children_[1]->type_ == T_ALTER_ACTION_LIST);
    //add dolphin [database manager]@20150617:b
    ObString db_name;
    if(node->children_[0]->children_[0] != NULL)
      db_name.assign_ptr((char*)(node->children_[0]->children_[0]->str_value_),
                         static_cast<int32_t>(strlen(node->children_[0]->children_[0]->str_value_)));

    if(db_name.length() <= 0)
    {
      db_name = static_cast<ObSQLSessionInfo*>(result_plan->session_info_)->get_db_name();
    }
    if(db_name.length() <= 0 || db_name.length() >= OB_MAX_DATBASE_NAME_LENGTH)//mod liumz [name_length]
    {
      ret = OB_INVALID_ARGUMENT;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "database name is not valid,you must specify the correct database name when alter table.");
    }
    alter_table_stmt->set_db_name(*result_plan, db_name);
    //add:e
    //modify dolphin [database manager]@20150617:b
/*    int32_t name_len= static_cast<int32_t>(strlen(node->children_[0]->str_value_));
    ObString table_name(name_len, name_len, node->children_[0]->str_value_);*/

    ObString table_name;

    if(OB_SUCCESS == ret)
    {
      if(node->children_[0]->children_[1] != NULL)
       table_name.assign_ptr(
              (char *) (node->children_[0]->children_[1]->str_value_),
              static_cast<int32_t>(strlen(node->children_[0]->children_[1]->str_value_)));
      if (table_name.length() <= 0 || table_name.length() >= OB_MAX_TABLE_NAME_LENGTH)//mod liumz
      {
        ret = OB_INVALID_ARGUMENT;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "table_name is invalid.");
      }
    }
    if(ret == OB_SUCCESS)
    {

      if ((ret = alter_table_stmt->init()) != OB_SUCCESS) {
        PARSER_LOG("Init alter table stmt failed, ret=%d", ret);
      }
      else if ((ret = alter_table_stmt->set_table_name(*result_plan, table_name)) == OB_SUCCESS ) {
        for (int32_t i = 0; ret == OB_SUCCESS && i < node->children_[1]->num_child_; i++) {
          ParseNode *action_node = node->children_[1]->children_[i];
          if (action_node == NULL)
            continue;
          ObColumnDef col_def;
          switch (action_node->type_) {
            case T_TABLE_RENAME:
            {
              //modify dolphin [database manager]@20150618:b
              /*int32_t len = static_cast<int32_t>(strlen(action_node->children_[0]->str_value_));
              if (len >= static_cast<int32_t>(OB_MAX_TABLE_NAME_LENGTH))//add liuj [Alter_Rename] [JHOBv0.1] 20150104
              {
                ret = OB_ERR_TABLE_NAME_LENGTH;//add liuj [Alter_Rename] [JHOBv0.1] 20150104
                PARSER_LOG("New table name is too long");//add liuj [Alter_Rename] [JHOBv0.1] 20150104
              } else//add liuj [Alter_Rename] [JHOBv0.1] 20150104
              {
                ObString new_name(len, len, action_node->children_[0]->str_value_);
                alter_table_stmt->set_has_table_rename();//add liuj [Alter_Rename] [JHOBv0.1] 20150104
                ret = alter_table_stmt->set_new_table_name(*result_plan, new_name);
              }*/
             // int32_t len = static_cast<int32_t>(strlen(action_node->children_[0]->children_[0]->str_value_));
              if(action_node->children_[0]->children_[0]!= NULL)
              {
                if(db_name != ObString(0, static_cast<int32_t>(strlen(action_node->children_[0]->children_[0]->str_value_)),action_node->children_[0]->children_[0]->str_value_))
                {
                  ret = OB_INVALID_ARGUMENT;
                  PARSER_LOG("NEW table name must be in the same database as the original table");
                }
              }
              if(OB_SUCCESS == ret)
              {

                if (strlen(action_node->children_[0]->children_[1]->str_value_) >=
                    static_cast<int32_t>(OB_MAX_TABLE_NAME_LENGTH)
                    || strlen(action_node->children_[0]->children_[1]->str_value_) == 0) {
                  ret = OB_ERR_TABLE_NAME_LENGTH;
                  PARSER_LOG("New table name is too long or empty");
                } else
                {
                  alter_table_stmt->set_has_table_rename();
                  ret = alter_table_stmt->set_new_table_name(*result_plan, ObString(0, static_cast<int32_t>(strlen(
                                                                                            action_node->children_[0]->children_[1]->str_value_)),
                                                                                    action_node->children_[0]->children_[1]->str_value_));
                }
              }
              //modfiy:e
              break;
            }
            case T_COLUMN_DEFINITION: {
              bool is_primary_key = false;
              int32_t len = static_cast<int32_t>(strlen(action_node->children_[0]->str_value_));
              ObString col_name(len, len, action_node->children_[0]->str_value_);
              if (len >= OB_MAX_COLUMN_NAME_LENGTH) {
                ret = OB_ERR_COLUMN_NAME_LENGTH;
                PARSER_LOG("Column name is too long, column_name=%.*s", col_name.length(), col_name.ptr());
              }
                //20150519:e
              else if ((ret = resolve_column_definition(
                      result_plan,
                      col_def,
                      action_node,
                      &is_primary_key)) != OB_SUCCESS) {
              }
              else if (is_primary_key) {
                ret = OB_ERR_MODIFY_PRIMARY_KEY;
                PARSER_LOG("New added column can not be primary key");
              }
              else {
                ret = alter_table_stmt->add_column(*result_plan, col_def);
              }
              break;
            }
            case T_COLUMN_DROP: {
              int32_t len = static_cast<int32_t>(strlen(action_node->children_[0]->str_value_));
              ObString table_name(len, len, action_node->children_[0]->str_value_);
              col_def.action_ = DROP_ACTION;
              col_def.column_name_ = table_name;
              switch (action_node->value_) {
                case 0:
                  col_def.drop_behavior_ = NONE_BEHAVIOR;
                  break;
                case 1:
                  col_def.drop_behavior_ = RESTRICT_BEHAVIOR;
                  break;
                case 2:
                  col_def.drop_behavior_ = CASCADE_BEHAVIOR;
                  break;
                default:
                  break;
              }
              ret = alter_table_stmt->drop_column(*result_plan, col_def);
              break;
            }
            case T_COLUMN_ALTER: {
              int32_t table_len = static_cast<int32_t>(strlen(action_node->children_[0]->str_value_));
              ObString table_name(table_len, table_len, action_node->children_[0]->str_value_);
              col_def.action_ = ALTER_ACTION;
              col_def.column_name_ = table_name;

              OB_ASSERT(action_node->children_[1]);

              switch (action_node->children_[1]->type_) {
                case T_CONSTR_NOT_NULL:
                  col_def.not_null_ = true;
                  //add fyd [NotNULL_check] [JHOBv0.1] 20140108:b
                  col_def.action_ = ALTER_ACTION_NULL;
                  //add 20140108:e
                  break;
                case T_CONSTR_NULL:
                  col_def.not_null_ = false;
                  //add fyd [NotNULL_check] [JHOBv0.1] 20140108:b
                  col_def.action_ = ALTER_ACTION_NULL;
                  //add 20140108:e
                  break;
                case T_CONSTR_DEFAULT:
                  ret = resolve_const_value(result_plan, action_node->children_[1], col_def.default_value_);
                  break;
                default:
                  /* won't be here */
                  ret = OB_ERR_RESOLVE_SQL;
                  PARSER_LOG("Unkown alter table alter column action type, type=%d",
                             action_node->children_[1]->type_);
                  break;
              }
              ret = alter_table_stmt->alter_column(*result_plan, col_def);
              break;
            }
            case T_COLUMN_RENAME: {
              int32_t table_len = static_cast<int32_t>(strlen(action_node->children_[0]->str_value_));
              ObString table_name(table_len, table_len, action_node->children_[0]->str_value_);
              int32_t new_len = static_cast<int32_t>(strlen(action_node->children_[1]->str_value_));
              ObString new_name(new_len, new_len, action_node->children_[1]->str_value_);
              col_def.action_ = RENAME_ACTION;
              col_def.column_name_ = table_name;
              col_def.new_column_name_ = new_name;
              //add liu jun. fix rename bug. 20150519:b
              if (new_len >= OB_MAX_COLUMN_NAME_LENGTH) {
                ret = OB_ERR_COLUMN_NAME_LENGTH;
                PARSER_LOG("New column name is too long, column_name=%.*s", new_name.length(), new_name.ptr());
              }
              else
                ret = alter_table_stmt->rename_column(*result_plan, col_def);
              //20150519:e
              break;
            }
            default:
              /* won't be here */
              ret = OB_ERR_RESOLVE_SQL;
              PARSER_LOG("Unkown alter table action type, type=%d", action_node->type_);
              break;
          }
        }
      }
    }
  }
  return ret;
}

int resolve_drop_table_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node && node->type_ == T_DROP_TABLE && node->num_child_ == 2);
  ObLogicalPlan* logical_plan = NULL;
  ObDropTableStmt* drp_tab_stmt = NULL;
  query_id = OB_INVALID_ID;


  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc ObLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {
    drp_tab_stmt = (ObDropTableStmt*)parse_malloc(sizeof(ObDropTableStmt), result_plan->name_pool_);
    if (drp_tab_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc ObDropTableStmt");
    }
    else
    {
      drp_tab_stmt = new(drp_tab_stmt) ObDropTableStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      drp_tab_stmt->set_query_id(query_id);
      if ((ret = logical_plan->add_query(drp_tab_stmt)) != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not add ObDropTableStmt to logical plan");
      }
    }
  }

  if (ret == OB_SUCCESS && node->children_[0])
  {
    drp_tab_stmt->set_if_exists(true);
  }
  if (ret == OB_SUCCESS)
  {
    OB_ASSERT(node->children_[1] && node->children_[1]->num_child_ > 0);
    //add dolphin [database manager]@20150614:b
    OB_ASSERT(node->children_[1]->type_ == T_TABLE_LIST);
    ObString db_name;
    char buf[OB_MAX_DATBASE_NAME_LENGTH + OB_MAX_TABLE_NAME_LENGTH + 1];
    ObString dt;
    dt.assign_buffer(buf,OB_MAX_DATBASE_NAME_LENGTH + OB_MAX_TABLE_NAME_LENGTH + 1);
    //add:e
    ParseNode *table_node = NULL;
    ObString table_name;
    for (int32_t i = 0; i < node->children_[1]->num_child_ && OB_SUCCESS == ret; i ++)
    {
      table_node = node->children_[1]->children_[i];
      //add liumz, bugfix: drop table t1 t2; 20150626:b
      if (T_RELATION != table_node->type_)
      {
        ret = OB_ERR_TABLE_UNKNOWN;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Unkown table name!");
      }
      else
      {//add:e
        //add dolphin [database manager]@20150617:b
        if(table_node->children_[0] != NULL)
          db_name.assign_ptr((char*)(table_node->children_[0]->str_value_),
              static_cast<int32_t>(strlen(table_node->children_[0]->str_value_)));
        TBSYS_LOG(DEBUG,"resolve create stmt dbname is %.*s",db_name.length(),db_name.ptr());

        if(db_name.length() <= 0)
        {
          db_name = static_cast<ObSQLSessionInfo*>(result_plan->session_info_)->get_db_name();
        }
        if(db_name.length() <= 0)
        {
          ret = OB_INVALID_ARGUMENT;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "database name is null,you must specify the database when create table.");
        }
        //add:e
        //modify dolphin [database manager]@20150617:b

        /**table_name.assign_ptr(
          (char*)(table_node->str_value_),
          static_cast<int32_t>(strlen(table_node->str_value_))
          );
     */
        if(table_node->children_[1] != NULL)
          table_name.assign_ptr(
                (char*)(table_node->children_[1]->str_value_),
              static_cast<int32_t>(strlen(table_node->children_[1]->str_value_)));
        if (table_name.length() <= 0)
        {
          ret = OB_INVALID_ARGUMENT;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "table_name is null,you must specify the table_name when drop table.");
        }
        //add liumz, [check table name length]20151130:b
        else if (table_name.length() >= OB_MAX_TABLE_NAME_LENGTH)
        {
          ret = OB_INVALID_ARGUMENT;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "table name is too long");
        }
        else
        {//add:e
          dt.concat(db_name, table_name);
          //modify:e
          if (OB_SUCCESS != (ret = drp_tab_stmt->add_table_name_id(
                  *result_plan, /**modify dolphin [database manager]@20150617 table_name */dt))) {
            break;
          }
        }
      }
    }
  }
    return ret;
  }

//add zhaoqiong [Truncate Table]:20160318:b
int resolve_truncate_table_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node && node->type_ == T_TRUNCATE_TABLE && node->num_child_ == 3);
  ObLogicalPlan* logical_plan = NULL;
  ObTruncateTableStmt* trun_tab_stmt = NULL;
  query_id = OB_INVALID_ID;


  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc ObLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {
    trun_tab_stmt = (ObTruncateTableStmt*)parse_malloc(sizeof(ObTruncateTableStmt), result_plan->name_pool_);
    if (trun_tab_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc ObTruncateTableStmt");
    }
    else
    {
      trun_tab_stmt = new(trun_tab_stmt) ObTruncateTableStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      trun_tab_stmt->set_query_id(query_id);
      if ((ret = logical_plan->add_query(trun_tab_stmt)) != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not add ObDropTableStmt to logical plan");
      }
    }
  }

  if (ret == OB_SUCCESS && node->children_[0])
  {
    trun_tab_stmt->set_if_exists(true);
  }
  if (ret == OB_SUCCESS)
  {
    OB_ASSERT(node->children_[1] && node->children_[1]->num_child_ > 0);
    //add dolphin [database manager]@20150614:b
    OB_ASSERT(node->children_[1]->type_ == T_TABLE_LIST);
    ObString db_name;
    char buf[OB_MAX_DATBASE_NAME_LENGTH + OB_MAX_TABLE_NAME_LENGTH + 1];
    ObString dt;
    dt.assign_buffer(buf,OB_MAX_DATBASE_NAME_LENGTH + OB_MAX_TABLE_NAME_LENGTH + 1);
    //add:e
    ParseNode *table_node = NULL;
    ObString table_name;
    for (int32_t i = 0; i < node->children_[1]->num_child_ && OB_SUCCESS == ret; i ++)
    {
      table_node = node->children_[1]->children_[i];
      if (T_RELATION != table_node->type_)
      {
        ret = OB_ERR_TABLE_UNKNOWN;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Unkown table name!");
      }
      else
      {
        //add dolphin [database manager]@20150617:b
        if(table_node->children_[0] != NULL)
          db_name.assign_ptr((char*)(table_node->children_[0]->str_value_),
              static_cast<int32_t>(strlen(table_node->children_[0]->str_value_)));
        TBSYS_LOG(DEBUG,"resolve create stmt dbname is %.*s",db_name.length(),db_name.ptr());

        if(db_name.length() <= 0)
        {
          db_name = static_cast<ObSQLSessionInfo*>(result_plan->session_info_)->get_db_name();
        }
        if(db_name.length() <= 0)
        {
          ret = OB_INVALID_ARGUMENT;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "database name is null,you must specify the database when create table.");
        }
        //add:e
        //modify dolphin [database manager]@20150617:b

        /**table_name.assign_ptr(
          (char*)(table_node->str_value_),
          static_cast<int32_t>(strlen(table_node->str_value_))
          );
     */
        if(table_node->children_[1] != NULL)
          table_name.assign_ptr(
                (char*)(table_node->children_[1]->str_value_),
              static_cast<int32_t>(strlen(table_node->children_[1]->str_value_)));
        if (table_name.length() <= 0)
        {
          ret = OB_INVALID_ARGUMENT;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "table_name is null,you must specify the table_name when truncate table.");
        }
        //add liumz, [check table name length]20151130:b
        else if (table_name.length() >= OB_MAX_TABLE_NAME_LENGTH)
        {
          ret = OB_INVALID_ARGUMENT;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "table name is too long");
        }
        else
        {//add:e
          dt.concat(db_name, table_name);
          //modify:e
          if (OB_SUCCESS != (ret = trun_tab_stmt->add_table_name_id(
                  *result_plan, /**modify dolphin [database manager]@20150617 table_name */dt)))
          {
            TBSYS_LOG(WARN,"add table name id failed ret=[%d]",ret);
            break;
          }
        }
      }
    }
  }
  if (ret == OB_SUCCESS && node->children_[2])
  {

    ObString comment;
    if (static_cast<int32_t>(strlen(node->children_[2]->str_value_)) <= OB_MAX_TABLE_COMMENT_LENGTH)
    {
      if (OB_SUCCESS != (ret = ob_write_string(
                          *name_pool,
                          ObString::make_string(node->children_[2]->str_value_),
                          comment)))
      {
        PARSER_LOG("Can not malloc space for comment");
      }
    }
    else
    {
      if (OB_SUCCESS != (ret = ob_write_string(
                          *name_pool,
                          ObString::make_string(node->children_[2]->str_value_),
                          comment,0,static_cast<int32_t>(OB_MAX_TABLE_COMMENT_LENGTH))))
      {
        PARSER_LOG("Can not malloc space for comment");
      }
    }
    if (ret == OB_SUCCESS)
    {
      trun_tab_stmt->set_comment(comment);
    }

  }
  return ret;
  }
//add:e


//add wenghaixing[secondary index drop index]20141222
int resolve_drop_index_stmt(ResultPlan *result_plan, ParseNode *node, uint64_t &query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node && node->type_ == T_DROP_INDEX && node->num_child_ == 2);
  ObLogicalPlan* logical_plan = NULL;
  ObDropIndexStmt* drp_idx_stmt = NULL;
  query_id = OB_INVALID_ID;
  uint64_t data_table_id = OB_INVALID_ID;
  ObSchemaChecker* schema_checker=NULL;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  //modify zhangcd [multi_datbase.secondary_index] 20150703:b
  //char tname_str[OB_MAX_DATBASE_NAME_LENGTH];
  char tname_str[OB_MAX_COMPLETE_TABLE_NAME_LENGTH];
  ObString db_name;
  //modify:e
  int64_t str_len=0;
  if (NULL == result_plan->plan_tree_)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (NULL == logical_plan)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Can not malloc ObLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }
  if (OB_SUCCESS == ret)
  {
    drp_idx_stmt = (ObDropIndexStmt*)parse_malloc(sizeof(ObDropIndexStmt), result_plan->name_pool_);
    if (NULL == drp_idx_stmt)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Can not malloc ObDropIndexStmt");
    }
    else
    {
      drp_idx_stmt = new(drp_idx_stmt) ObDropIndexStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      drp_idx_stmt->set_query_id(query_id);
      if (OB_SUCCESS != (ret = logical_plan->add_query(drp_idx_stmt)))
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Can not add ObDropIndexStmt to logical plan");
      }
    }
  }
  if(OB_SUCCESS == ret)
  {
    OB_ASSERT(node->children_[0]);
    ParseNode *table_node = NULL;
    ObString table_name;
    table_node =node->children_[0];
    // add by zhangcd [multi_database.secondary_index] 20150703
    if(NULL == table_node || table_node->num_child_ != 2)
    {
      ret = OB_ERROR;
      PARSER_LOG("Drop Index failed!");
    }
    // add:e
    if(OB_SUCCESS == ret)
    {
      //add by zhangcd [multi_database.secondary_index] 20150703:b
      ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
      ObString short_table_name;
      if(table_node->children_[0] == NULL && table_node->children_[1] != NULL)
      {
        ObSQLSessionInfo* session_info = static_cast<ObSQLSessionInfo*>(result_plan->session_info_);
        db_name = session_info->get_db_name();
        short_table_name = ObString::make_string(table_node->children_[1]->str_value_);
        char *ct_name = NULL;
        ct_name = static_cast<char *>(name_pool->alloc(OB_MAX_COMPLETE_TABLE_NAME_LENGTH));
        if(NULL == ct_name)
        {
          ret = OB_ERROR;
          PARSER_LOG("Memory over flow!");
        }
        else if(db_name.length() >= OB_MAX_DATBASE_NAME_LENGTH)
        {
          ret = OB_ERROR;
          PARSER_LOG("db name is too long!");
        }
        else if(short_table_name.length() >= OB_MAX_TABLE_NAME_LENGTH)
        {
          ret = OB_ERROR;
          PARSER_LOG("table name is too long!");
        }
        else
        {
          table_name.assign_buffer(ct_name, OB_MAX_COMPLETE_TABLE_NAME_LENGTH);
          // mod zhangcd [multi_database.seconary_index] 20150721:b
//          table_name.concat(db_name, short_table_name, '.');
          table_name.write(db_name.ptr(), db_name.length());
          table_name.write(".", 1);
          table_name.write(short_table_name.ptr(), short_table_name.length());
          // mod:e
        }
      }
      else if(table_node->children_[0] != NULL && table_node->children_[1] != NULL)
      {
        db_name = ObString::make_string(table_node->children_[0]->str_value_);
        short_table_name = ObString::make_string(table_node->children_[1]->str_value_);
        char *ct_name = NULL;
        ct_name = static_cast<char *>(name_pool->alloc(OB_MAX_COMPLETE_TABLE_NAME_LENGTH));
        if(NULL == ct_name)
        {
          ret = OB_ERROR;
          PARSER_LOG("Memory over flow!");
        }
        else if(db_name.length() >= OB_MAX_DATBASE_NAME_LENGTH)
        {
          ret = OB_ERROR;
          PARSER_LOG("db name is too long!");
        }
        else if(short_table_name.length()  >= OB_MAX_TABLE_NAME_LENGTH)
        {
          ret = OB_ERROR;
          PARSER_LOG("table name is too long!");
        }
        else
        {
          table_name.assign_buffer(ct_name, OB_MAX_COMPLETE_TABLE_NAME_LENGTH);
          // mod zhangcd [multi_database.seconary_index] 20150721:b
//          table_name.concat(db_name, short_table_name, '.');
          table_name.write(db_name.ptr(), db_name.length());
          table_name.write(".", 1);
          table_name.write(short_table_name.ptr(), short_table_name.length());
          // mod:e
        }
      }
      else
      {
        ret = OB_ERROR;
        PARSER_LOG("Parse failed!");
      }
      //add:e

      //del by zhangcd [multi_database.secondary_index] 20150703:b
      /*
      table_name.assign_ptr(
                (char*)(table_node->str_value_),
                static_cast<int32_t>(strlen(table_node->str_value_))
                );
      */
      //del:e

    }
    //add liumz, 20150725:b
    if (OB_SUCCESS == ret)
    {//add:e
      schema_checker = static_cast<ObSchemaChecker*>(result_plan->schema_checker_);
      if (schema_checker == NULL)
      {
        ret = OB_ERR_SCHEMA_UNSET;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                  "Schema(s) are not set");
      }
      else if((data_table_id=schema_checker->get_table_id(table_name))==OB_INVALID_ID)
      {
        ret = OB_ERR_TABLE_UNKNOWN;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                    "table '%.*s' does not exist", table_name.length(), table_name.ptr());
      }
    }
   }
   if (OB_SUCCESS == ret)
   {
     OB_ASSERT(node->children_[1] && node->children_[1]->num_child_ > 0);
     ParseNode *table_node = NULL;
     ObString table_name;
     for (int32_t i = 0; i < node->children_[1]->num_child_; i ++)
     {
       uint64_t index_tid = OB_INVALID_ID;
       table_node = node->children_[1]->children_[i];
       table_name.assign_ptr(
              (char*)(table_node->str_value_),
              static_cast<int32_t>(strlen(table_node->str_value_))
              );
        //generate index name here

       //modify by zhangcd [multi_database.secondary_index] 20150703:b
       char *dt_name_buffer_ptr = NULL;
       if(NULL == (dt_name_buffer_ptr = (char *)name_pool->alloc(OB_MAX_COMPLETE_TABLE_NAME_LENGTH)))
       {
         ret = OB_ERROR;
       }
       else
       {
         memset(dt_name_buffer_ptr, 0, OB_MAX_COMPLETE_TABLE_NAME_LENGTH);
       }
       //memset(tname_str,0,OB_MAX_TABLE_NAME_LENGTH);
       //modify:e

       if(OB_SUCCESS == ret && OB_SUCCESS != (ret = generate_index_table_name(table_name,data_table_id,tname_str,str_len)))
       {
         snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                  "generate index name failed '%.*s'", table_name.length(), table_name.ptr());
       }
       else if(db_name.length() >= OB_MAX_DATBASE_NAME_LENGTH)
       {
         ret = OB_ERROR;
         PARSER_LOG("db name is too long!");
       }
       else if(str_len >= OB_MAX_TABLE_NAME_LENGTH)
       {
         ret = OB_ERROR;
         PARSER_LOG("table name is too long!");
       }
       else
       {
         // add by zhangcd [multi_database.secondary_index] 20150703:b
         strncpy(dt_name_buffer_ptr, (const char*)db_name.ptr(), db_name.length());
         strncpy(dt_name_buffer_ptr + db_name.length(), ".", 1);
         strncpy(dt_name_buffer_ptr + db_name.length() + 1, tname_str, str_len);
         // add zhangcd [multi_database.seconary_index] 20150721:b
         //dt_name_buffer_ptr[db_name.length() + 1 + str_len] = 0;
         // add:e
         // add:e
         table_name.reset();
         table_name.assign_ptr(dt_name_buffer_ptr,(int32_t)str_len + db_name.length() + 1);
       }
       if(OB_SUCCESS == ret)
       {
         if(OB_INVALID_ID == (index_tid = schema_checker->get_table_id(table_name)))
         {
           snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                      "failed to get index id'%.*s'", table_name.length(), table_name.ptr());
         }
         else if (OB_SUCCESS != (ret = drp_idx_stmt->add_table_name_id(*result_plan, table_name)))
         {
           break;
         }
       }
     }
   }
   return ret;
}
//add e

int resolve_show_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  uint64_t  sys_table_id = OB_INVALID_ID;
  ParseNode *show_table_node = NULL;
  ParseNode *condition_node = NULL;
  OB_ASSERT(node && node->type_ >= T_SHOW_TABLES && node->type_ <= T_SHOW_PROCESSLIST);
  query_id = OB_INVALID_ID;

  ObLogicalPlan* logical_plan = NULL;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc ObLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {
    ObShowStmt* show_stmt = (ObShowStmt*)parse_malloc(sizeof(ObShowStmt), result_plan->name_pool_);
    if (show_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc ObShowStmt");
    }
    else
    {
      ParseNode sys_table_name;
      sys_table_name.type_ = T_IDENT;
      switch (node->type_)
      {
        case T_SHOW_TABLES:
          OB_ASSERT(node->num_child_ == 1);
          condition_node = node->children_[0];
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_TABLES);
          sys_table_name.str_value_ = OB_TABLES_SHOW_TABLE_NAME;
          break;
        // add by zhangcd [multi_database.show_tables] 20150616:b
        case T_SHOW_SYSTEM_TABLES:
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_SYSTEM_TABLES);
          sys_table_name.str_value_ = OB_TABLES_SHOW_TABLE_NAME;
          break;
        // add by zhangcd [multi_database.show_tables] 20150616:e
        // add by zhangcd [multi_database.show_databases] 20150616:b
        case T_SHOW_DATABASES:
          show_stmt = new(show_stmt) ObShowStmt(name_pool,ObBasicStmt::T_SHOW_DATABASES);
          sys_table_name.str_value_ = OB_DATABASES_SHOW_TABLE_NAME;
          break;
        // add by zhangcd [multi_database.show_databases] 20150616:e
        // add by zhangcd [multi_database.show_databases] 20150617:b
        case T_SHOW_CURRENT_DATABASE:
          show_stmt = new(show_stmt) ObShowStmt(name_pool,ObBasicStmt::T_SHOW_CURRENT_DATABASE);
          sys_table_name.str_value_ = OB_DATABASES_SHOW_TABLE_NAME;
          break;
        // add by zhangcd [multi_database.show_databases] 20150617:e
        //add liumengzhan_show_index [20141208]
        case T_SHOW_INDEX:
          OB_ASSERT(node->num_child_ == 2);
          show_table_node = node->children_[0];
          condition_node = node->children_[1];
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_INDEX);
          sys_table_name.str_value_ = OB_INDEX_SHOW_TABLE_NAME;
          break;
        //add:e
        case T_SHOW_VARIABLES:
          OB_ASSERT(node->num_child_ == 1);
          condition_node = node->children_[0];
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_VARIABLES);
          show_stmt->set_global_scope(node->value_ == 1 ? true : false);
          sys_table_name.str_value_ = OB_VARIABLES_SHOW_TABLE_NAME;
          break;
        case T_SHOW_COLUMNS:
          OB_ASSERT(node->num_child_ == 2);
          show_table_node = node->children_[0];
          condition_node = node->children_[1];
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_COLUMNS);
          sys_table_name.str_value_ = OB_COLUMNS_SHOW_TABLE_NAME;
          break;
        case T_SHOW_SCHEMA:
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_SCHEMA);
          sys_table_name.str_value_ = OB_SCHEMA_SHOW_TABLE_NAME;
          break;
        case T_SHOW_CREATE_TABLE:
          OB_ASSERT(node->num_child_ == 1);
          show_table_node = node->children_[0];
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_CREATE_TABLE);
          sys_table_name.str_value_ = OB_CREATE_TABLE_SHOW_TABLE_NAME;
          break;
        case T_SHOW_TABLE_STATUS:
          OB_ASSERT(node->num_child_ == 1);
          condition_node = node->children_[0];
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_TABLE_STATUS);
          sys_table_name.str_value_ = OB_TABLE_STATUS_SHOW_TABLE_NAME;
          break;
        case T_SHOW_SERVER_STATUS:
          OB_ASSERT(node->num_child_ == 1);
          condition_node = node->children_[0];
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_SERVER_STATUS);
          sys_table_name.str_value_ = OB_SERVER_STATUS_SHOW_TABLE_NAME;
          break;
        case T_SHOW_WARNINGS:
          OB_ASSERT(node->num_child_ == 0 || node->num_child_ == 1);
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_WARNINGS);
          break;
        case T_SHOW_GRANTS:
          OB_ASSERT(node->num_child_ == 1);
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_GRANTS);
          break;
        case T_SHOW_PARAMETERS:
          OB_ASSERT(node->num_child_ == 1);
          condition_node = node->children_[0];
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_PARAMETERS);
          sys_table_name.str_value_ = OB_PARAMETERS_SHOW_TABLE_NAME;
          break;
        case T_SHOW_PROCESSLIST:
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_PROCESSLIST);
          show_stmt->set_full_process(node->value_ == 1? true: false);
          show_stmt->set_show_table(OB_ALL_SERVER_SESSION_TID);
          break;
        default:
          /* won't be here */
          break;
      }
      if (node->type_ >= T_SHOW_TABLES && node->type_ <= T_SHOW_SERVER_STATUS
        && (ret = resolve_table(result_plan, show_stmt, &sys_table_name, sys_table_id)) == OB_SUCCESS)
      {
        show_stmt->set_sys_table(sys_table_id);
        query_id = logical_plan->generate_query_id();
        show_stmt->set_query_id(query_id);
      }
      if (ret == OB_SUCCESS && (ret = logical_plan->add_query(show_stmt)) != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not add ObShowStmt to logical plan");
      }
      if (ret != OB_SUCCESS && show_stmt != NULL)
      {
        show_stmt->~ObShowStmt();
      }
    }

    if (ret == OB_SUCCESS && sys_table_id != OB_INVALID_ID)
    {
      TableItem *table_item = show_stmt->get_table_item_by_id(sys_table_id);
      ret = resolve_table_columns(result_plan, show_stmt, *table_item);
    }
    /* modify liumengzhan_show_index [20141208],
     * add node->type_ == T_SHOW_INDEX, set show_stmt's table_id
     */
    if (ret == OB_SUCCESS && (node->type_ == T_SHOW_INDEX || node->type_ == T_SHOW_COLUMNS || node->type_ == T_SHOW_CREATE_TABLE))
    {
      OB_ASSERT(show_table_node);
      //add liumz, [multi_database.sql]:20150613
      OB_ASSERT(show_table_node->children_[1]);
      ObSQLSessionInfo *session_info = static_cast<ObSQLSessionInfo*>(result_plan->session_info_);
      if (session_info == NULL)
      {
        ret = OB_ERR_SESSION_UNSET;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Session not set");
      }
      //add:e
      ObSchemaChecker *schema_checker = static_cast<ObSchemaChecker*>(result_plan->schema_checker_);
      if (schema_checker == NULL)
      {
        ret = OB_ERR_SCHEMA_UNSET;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Schema(s) are not set");
      }
      //del liumz, [multi_database.sql]:20150613
      /*
      int32_t len = static_cast<int32_t>(strlen(show_table_node->str_value_));
      ObString table_name(len, len, show_table_node->str_value_);
      uint64_t show_table_id = schema_checker->get_table_id(table_name);
      if (show_table_id == OB_INVALID_ID)
      {
        ret = OB_ERR_TABLE_UNKNOWN;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Unknown table \"%s\"", show_table_node->str_value_);
      }
      else
      {
        show_stmt->set_show_table(show_table_id);
      }*/
      //del:e
        //add liumz, [multi_database.sql]:20150613
        //add liumz, [multi_database.bugfix]20150713
        if (OB_SUCCESS == ret)
        {//add:e
          ParseNode *db_node = show_table_node->children_[0];
          ParseNode *table_node = show_table_node->children_[1];
          ObString db_name;
          ObString table_name = ObString::make_string(table_node->str_value_);
          uint64_t show_table_id = schema_checker->get_table_id(table_name);
          if (NULL == db_node) {
            //mod liumz, [multi_database.bugfix]20150713:b
            db_name = session_info->get_db_name();
            /*if ((ret = ob_write_string(*name_pool, session_info->get_db_name(), db_name)) != OB_SUCCESS) {
              PARSER_LOG("Can not malloc space for db name");
            }*/
            //mod:e
          }
          else {
            //mod liumz, [multi_database.bugfix]20150713:b
            db_name = ObString::make_string(db_node->str_value_);
            /*if ((ret = ob_write_string(*name_pool, ObString::make_string(db_node->str_value_), db_name)) != OB_SUCCESS) {
              PARSER_LOG("Can not malloc space for db name");
            }*/
            //mod:e
          }
          //if (OB_SUCCESS == ret) {
            if (!IS_SYS_TABLE_ID(show_table_id)) {
              ObString db_table_name;
              int64_t buf_len = OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1;
              char name_buf[buf_len];
              db_table_name.assign_buffer(name_buf, static_cast<ObString::obstr_size_t>(buf_len));
              if (OB_SUCCESS != (ret = write_db_table_name(db_name, table_name, db_table_name))) {
                snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Table name too long");
              }
              else {
                show_table_id = schema_checker->get_table_id(db_table_name);
              }
            }
            if (OB_SUCCESS == ret) {
              if (show_table_id == OB_INVALID_ID) {
                ret = OB_ERR_TABLE_UNKNOWN;
                snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                         "Unknown table \"%.*s.%.*s\"", db_name.length(), db_name.ptr(), table_name.length(), table_name.ptr());
              }
              else {
                show_stmt->set_db_name(db_name);
                show_stmt->set_show_table(show_table_id);
              }
            }
          //}
        }
        //add:e
    }

    /* modify liumengzhan_show_index [20141210]
     * support like expr: show index on table_name [ like | where ]
     *
     */
    if (ret == OB_SUCCESS && condition_node
      && (node->type_ == T_SHOW_TABLES || node->type_ == T_SHOW_INDEX || node->type_ == T_SHOW_VARIABLES || node->type_ == T_SHOW_COLUMNS
      || node->type_ == T_SHOW_TABLE_STATUS || node->type_ == T_SHOW_SERVER_STATUS
      || node->type_ == T_SHOW_PARAMETERS))
    {
      if (condition_node->type_ == T_OP_LIKE && condition_node->num_child_ == 1)
      {
        OB_ASSERT(condition_node->children_[0]->type_ == T_STRING);
        ObString  like_pattern;
        like_pattern.assign_ptr(
            (char*)(condition_node->children_[0]->str_value_),
            static_cast<int32_t>(strlen(condition_node->children_[0]->str_value_))
            );
        ret = show_stmt->set_like_pattern(like_pattern);
      }
      else
      {
        ret = resolve_and_exprs(
                  result_plan,
                  show_stmt,
                  condition_node->children_[0],
                  show_stmt->get_where_exprs(),
                  T_WHERE_LIMIT
                  );
      }
    }

    if (ret == OB_SUCCESS && node->type_ == T_SHOW_WARNINGS)
    {
      show_stmt->set_count_warnings(node->value_ == 1 ? true : false);
      if (node->num_child_ == 1 && node->children_[0] != NULL)
      {
        ParseNode *limit = node->children_[0];
        OB_ASSERT(limit->num_child_ == 2);
        int64_t offset = limit->children_[0] == NULL ? 0 : limit->children_[0]->value_;
        int64_t count = limit->children_[1] == NULL ? -1 : limit->children_[1]->value_;
        show_stmt->set_warnings_limit(offset, count);
      }
    }

    if (ret == OB_SUCCESS && node->type_ == T_SHOW_GRANTS)
    {
      if (node->children_[0] != NULL)
      {
        ObString name;
        if ((ret = ob_write_string(*name_pool, ObString::make_string(node->children_[0]->str_value_), name)) != OB_SUCCESS)
        {
          PARSER_LOG("Can not malloc space for user name");
        }
        else
        {
          show_stmt->set_user_name(name);
        }
      }
    }
  }
  return ret;
}

int resolve_prepare_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_PREPARE && node->num_child_ == 2);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObPrepareStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
    if (ret == OB_SUCCESS)
    {
      OB_ASSERT(node->children_[0]);
      ObString name;
      if ((ret = ob_write_string(*name_pool, ObString::make_string(node->children_[0]->str_value_), name)) != OB_SUCCESS)
      {
        PARSER_LOG("Can not malloc space for stmt name");
      }
      else
      {
        stmt->set_stmt_name(name);
      }
    }
    if (ret == OB_SUCCESS)
    {
      uint64_t sub_query_id = OB_INVALID_ID;
      switch (node->children_[1]->type_)
      {
        case T_SELECT:
          ret = resolve_select_stmt(result_plan, node->children_[1], sub_query_id);
          break;
        case T_DELETE:
          ret = resolve_delete_stmt(result_plan, node->children_[1], sub_query_id);
          break;
        case T_INSERT:
          ret = resolve_insert_stmt(result_plan, node->children_[1], sub_query_id);
          break;
        case T_UPDATE:
          ret = resolve_update_stmt(result_plan, node->children_[1], sub_query_id);
          break;
        default:
          ret = OB_ERR_PARSER_SYNTAX;
          PARSER_LOG("Wrong statement type in prepare statement");
          break;
      }
      if (ret == OB_SUCCESS)
        stmt->set_prepare_query_id(sub_query_id);
    }
  }
  return ret;
}

int resolve_variable_set_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_VARIABLE_SET);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObVariableSetStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
    ParseNode* set_node = NULL;
    ObVariableSetStmt::VariableSetNode var_node;
    for (int32_t i = 0; ret == OB_SUCCESS && i < node->num_child_; i++)
    {
      set_node = node->children_[i];
      OB_ASSERT(set_node->type_ == T_VAR_VAL);
      switch (set_node->value_)
      {
        case 1:
          var_node.scope_type_ = ObVariableSetStmt::GLOBAL;
          break;
        case 2:
          var_node.scope_type_ = ObVariableSetStmt::SESSION;
          break;
        case 3:
          var_node.scope_type_ = ObVariableSetStmt::LOCAL;
          break;
        default:
          var_node.scope_type_ = ObVariableSetStmt::NONE_SCOPE;
          break;
      }

      ParseNode* var = set_node->children_[0];
      OB_ASSERT(var);
      var_node.is_system_variable_ = (var->type_ == T_SYSTEM_VARIABLE) ? true : false;
      if ((ret = ob_write_string(*name_pool, ObString::make_string(var->str_value_),
                                  var_node.variable_name_)) != OB_SUCCESS)
      {
        PARSER_LOG("Can not malloc space for variable name");
        break;
      }

      OB_ASSERT(node->children_[1]);
      if ((ret = resolve_independ_expr(result_plan, NULL, set_node->children_[1], var_node.value_expr_id_,
                                        T_VARIABLE_VALUE_LIMIT)) != OB_SUCCESS)
      {
        //PARSER_LOG("Resolve set value error");
        break;
      }

      if ((ret = stmt->add_variable_node(var_node)) != OB_SUCCESS)
      {
        PARSER_LOG("Add set entry failed");
        break;
      }
    }
  }
  return ret;
}

int resolve_execute_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_EXECUTE && node->num_child_ == 2);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObExecuteStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
    if (ret == OB_SUCCESS)
    {
      OB_ASSERT(node->children_[0]);
      ObString name;
      if ((ret = ob_write_string(*name_pool, ObString::make_string(node->children_[0]->str_value_), name)) != OB_SUCCESS)
      {
        PARSER_LOG("Can not malloc space for stmt name");
      }
      else
      {
        stmt->set_stmt_name(name);
      }
    }
    if (ret == OB_SUCCESS && NULL != node->children_[1])
    {
      OB_ASSERT(node->children_[1]->type_ == T_ARGUMENT_LIST);
      ParseNode *arguments = node->children_[1];
      for (int32_t i = 0; ret == OB_SUCCESS && i < arguments->num_child_; i++)
      {
        OB_ASSERT(arguments->children_[i] && arguments->children_[i]->type_ == T_TEMP_VARIABLE);
        ObString name;
        if ((ret = ob_write_string(*name_pool, ObString::make_string(arguments->children_[i]->str_value_), name)) != OB_SUCCESS)
        {
          PARSER_LOG("Resolve variable %s error", arguments->children_[i]->str_value_);
        }
        else if ((ret = stmt->add_variable_name(name)) != OB_SUCCESS)
        {
          PARSER_LOG("Add Using variable failed");
        }
      }
    }
  }
  return ret;
}

int resolve_deallocate_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_DEALLOCATE && node->num_child_ == 1);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObDeallocateStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
    TBSYS_LOG(WARN, "fail to prepare resolve stmt. ret=%d", ret);
  }
  else
  {
    ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
    OB_ASSERT(node->children_[0]);
    ObString name;
    if ((ret = ob_write_string(*name_pool, ObString::make_string(node->children_[0]->str_value_), name)) != OB_SUCCESS)
    {
      PARSER_LOG("Can not malloc space for stmt name");
    }
    else
    {
      stmt->set_stmt_name(name);
    }
  }
  return ret;
}

int resolve_start_trans_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_BEGIN && node->num_child_ == 0);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObStartTransStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    stmt->set_with_consistent_snapshot(0 != node->value_);
  }
  return ret;
}

int resolve_commit_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_COMMIT && node->num_child_ == 0);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObEndTransStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    stmt->set_is_rollback(false);
  }
  return ret;
}

int resolve_rollback_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_ROLLBACK && node->num_child_ == 0);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObEndTransStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    stmt->set_is_rollback(true);
  }
  return ret;
}

int resolve_alter_sys_cnf_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_ALTER_SYSTEM && node->num_child_ == 1);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObAlterSysCnfStmt* alter_sys_cnf_stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, alter_sys_cnf_stmt)))
  {
  }
  else if ((ret = alter_sys_cnf_stmt->init()) != OB_SUCCESS)
  {
    PARSER_LOG("Init alter system stmt failed, ret=%d", ret);
  }
  else
  {
    OB_ASSERT(node->children_[0] && node->children_[0]->type_ == T_SYTEM_ACTION_LIST);
    ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
    for (int32_t i = 0; ret == OB_SUCCESS && i < node->children_[0]->num_child_; i++)
    {
      ParseNode *action_node = node->children_[0]->children_[i];
      if (action_node == NULL)
        continue;
      OB_ASSERT(action_node->type_ == T_SYSTEM_ACTION && action_node->num_child_ == 5);
      ObSysCnfItem sys_cnf_item;
      ObString param_name;
      ObString comment;
      ObString server_ip;
      sys_cnf_item.config_type_ = static_cast<ObConfigType>(action_node->value_);
      if ((ret = ob_write_string(
                     *name_pool,
                     ObString::make_string(action_node->children_[0]->str_value_),
                     sys_cnf_item.param_name_)) != OB_SUCCESS)
      {
        PARSER_LOG("Can not malloc space for param name");
        break;
      }
      else if (action_node->children_[2] != NULL
        && (ret = ob_write_string(
                      *name_pool,
                      ObString::make_string(action_node->children_[2]->str_value_),
                      sys_cnf_item.comment_)) != OB_SUCCESS)
      {
        PARSER_LOG("Can not malloc space for comment");
        break;
      }
      else if ((ret = resolve_const_value(
                          result_plan,
                          action_node->children_[1],
                          sys_cnf_item.param_value_)) != OB_SUCCESS)
      {
        break;
      }
      else if (action_node->children_[4] != NULL)
      {
        if (action_node->children_[4]->type_ == T_CLUSTER)
        {
          sys_cnf_item.cluster_id_ = action_node->children_[4]->children_[0]->value_;
        }
        else if (action_node->children_[4]->type_ == T_SERVER_ADDRESS)
        {
          if ((ret = ob_write_string(
                         *name_pool,
                         ObString::make_string(action_node->children_[4]->children_[0]->str_value_),
                         sys_cnf_item.server_ip_)) != OB_SUCCESS)
          {
            PARSER_LOG("Can not malloc space for IP");
            break;
          }
          else
          {
            sys_cnf_item.server_port_ = action_node->children_[4]->children_[1]->value_;
          }
        }
      }
      OB_ASSERT(action_node->children_[3]);
      switch (action_node->children_[3]->value_)
      {
        case 1:
          sys_cnf_item.server_type_ = OB_ROOTSERVER;
          break;
        case 2:
          sys_cnf_item.server_type_ = OB_CHUNKSERVER;
          break;
        case 3:
          sys_cnf_item.server_type_ = OB_MERGESERVER;
          break;
        case 4:
          sys_cnf_item.server_type_ = OB_UPDATESERVER;
          break;
        default:
          /* won't be here */
          ret = OB_ERR_RESOLVE_SQL;
          PARSER_LOG("Unkown server type");
          break;
      }
      if ((ret = alter_sys_cnf_stmt->add_sys_cnf_item(*result_plan, sys_cnf_item)) != OB_SUCCESS)
      {
        // PARSER_LOG("Add alter system config item failed");
        break;
      }
    }
  }
  return ret;
}

int resolve_change_obi(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  UNUSED(query_id);
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_CHANGE_OBI && node->num_child_ >= 2);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObChangeObiStmt* change_obi_stmt = NULL;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  ObLogicalPlan *logical_plan = NULL;
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc ObLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {
    change_obi_stmt = (ObChangeObiStmt*)parse_malloc(sizeof(ObChangeObiStmt), result_plan->name_pool_);
    if (change_obi_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc ObChangeObiStmt");
    }
    else
    {
      change_obi_stmt = new(change_obi_stmt) ObChangeObiStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      change_obi_stmt->set_query_id(query_id);
      if ((ret = logical_plan->add_query(change_obi_stmt)) != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not add ObChangeObiStmt to logical plan");
      }
      else
      {
        OB_ASSERT(node->children_[0]->type_ == T_SET_MASTER
            || node->children_[0]->type_ == T_SET_SLAVE
            || node->children_[0]->type_ == T_SET_MASTER_SLAVE);
        OB_ASSERT(node->children_[1]&& node->children_[1]->type_ == T_STRING);
        change_obi_stmt->set_target_server_addr(node->children_[1]->str_value_);
        if (node->children_[0]->type_ == T_SET_MASTER)
        {
          change_obi_stmt->set_target_role(ObiRole::MASTER);
        }
        else if (node->children_[0]->type_ == T_SET_SLAVE)
        {
          change_obi_stmt->set_target_role(ObiRole::SLAVE);
        }
        else // T_SET_MASTER_SLAVE
        {
          if (node->children_[2] != NULL)
          {
            OB_ASSERT(node->children_[2]->type_ == T_FORCE);
            change_obi_stmt->set_force(true);
          }
        }
      }
    }
  }
  return ret;
}
int resolve_kill_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_KILL && node->num_child_ == 3);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObKillStmt* kill_stmt = NULL;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  ObLogicalPlan *logical_plan = NULL;
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc ObLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {
    kill_stmt = (ObKillStmt*)parse_malloc(sizeof(ObKillStmt), result_plan->name_pool_);
    if (kill_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc ObKillStmt");
    }
    else
    {
      kill_stmt = new(kill_stmt) ObKillStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      kill_stmt->set_query_id(query_id);
      if ((ret = logical_plan->add_query(kill_stmt)) != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not add ObKillStmt to logical plan");
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    OB_ASSERT(node->children_[0]&& node->children_[0]->type_ == T_BOOL);
    OB_ASSERT(node->children_[1]&& node->children_[1]->type_ == T_BOOL);
    OB_ASSERT(node->children_[2]);
    kill_stmt->set_is_global(node->children_[0]->value_ == 1? true: false);
    kill_stmt->set_thread_id(node->children_[2]->value_);
    kill_stmt->set_is_query(node->children_[1]->value_ == 1? true: false);
  }
  return ret;
}

////////////////////////////////////////////////////////////////
int resolve(ResultPlan* result_plan, ParseNode* node)
{
  if (!result_plan)
  {
    TBSYS_LOG(ERROR, "null result_plan");
    return OB_ERR_RESOLVE_SQL;
  }
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (ret == OB_SUCCESS && result_plan->name_pool_ == NULL)
  {
    ret = OB_ERR_RESOLVE_SQL;
    PARSER_LOG("name_pool_ nust be set");
  }
  if (ret == OB_SUCCESS && result_plan->schema_checker_ == NULL)
  {
    ret = OB_ERR_RESOLVE_SQL;
    PARSER_LOG("schema_checker_ must be set");
  }

  if (OB_LIKELY(OB_SUCCESS == ret))
  {
    bool is_preparable = false;
    switch (node->type_)
    {
      case T_STMT_LIST:
      case T_SELECT:
      case T_DELETE:
      case T_INSERT:
      case T_UPDATE:
      case T_BEGIN:
      case T_COMMIT:
      case T_ROLLBACK:
        is_preparable = true;
        break;
      default:
        break;
    }
    if (result_plan->is_prepare_ && !is_preparable)
    {
      ret = OB_ERR_RESOLVE_SQL;
      PARSER_LOG("the statement can not be prepared");
    }
  }

  uint64_t query_id = OB_INVALID_ID;
  if (ret == OB_SUCCESS && node != NULL)
  {
    switch (node->type_)
    {
      case T_STMT_LIST:
      {
        ret = resolve_multi_stmt(result_plan, node);
        break;
      }
      case T_SELECT:
      {
        ret = resolve_select_stmt(result_plan, node, query_id);
        break;
      }
      case T_DELETE:
      {
        ret = resolve_delete_stmt(result_plan, node, query_id);
        break;
      }
      case T_INSERT:
      {
        ret = resolve_insert_stmt(result_plan, node, query_id);
        break;
      }
      case T_UPDATE:
      {
        ret = resolve_update_stmt(result_plan, node, query_id);
        break;
      }
      case T_EXPLAIN:
      {
        ret = resolve_explain_stmt(result_plan, node, query_id);
        break;
      }
      case T_CREATE_TABLE:
      {
        ret = resolve_create_table_stmt(result_plan, node, query_id);
        break;
      }
      //add by wenghaixing [secondary index] 20141024
      case T_CREATE_INDEX:
      {
            ret=resolve_create_index_stmt(result_plan, node, query_id);
            break;
      }
      //add e
      //add wenghaixing [secondary index drop index]20141223
      case T_DROP_INDEX:
      {
        ret=resolve_drop_index_stmt(result_plan, node, query_id);
        break;
      }
      //add e
      case T_DROP_TABLE:
      {
        ret = resolve_drop_table_stmt(result_plan, node, query_id);
        break;
      }
      case T_ALTER_TABLE:
      {
        ret = resolve_alter_table_stmt(result_plan, node, query_id);
        break;
      }
      //add zhaoqiong [truncate stmt] 20151204:b
      case T_TRUNCATE_TABLE:
      {
        ret = resolve_truncate_table_stmt(result_plan, node, query_id);
        break;
      }
      //add:e
      case T_SHOW_TABLES:
      //add liumengzhan_show_index [20141208]
      case T_SHOW_INDEX:
      //add:e
      case T_SHOW_VARIABLES:
      case T_SHOW_COLUMNS:
      case T_SHOW_SCHEMA:
      case T_SHOW_SYSTEM_TABLES:// add by zhangcd [multi_database.show_tables] 20150616
      case T_SHOW_CREATE_TABLE:
      case T_SHOW_TABLE_STATUS:
      case T_SHOW_SERVER_STATUS:
      case T_SHOW_WARNINGS:
      case T_SHOW_GRANTS:
      case T_SHOW_PARAMETERS:
      case T_SHOW_PROCESSLIST :
      case T_SHOW_DATABASES://add dolphin [show database]@20150604
      case T_SHOW_CURRENT_DATABASE:// add by zhangcd [multi_database.show_databases] 20150617
      {
        ret = resolve_show_stmt(result_plan, node, query_id);
        break;
      }
      case T_CREATE_USER:
      {
        ret = resolve_create_user_stmt(result_plan, node, query_id);
        break;
      }
      case T_DROP_USER:
      {
        ret = resolve_drop_user_stmt(result_plan, node, query_id);
        break;
      }
      //add wenghaixing [database manage]20150608
      case T_CREATE_DATABASE:
      {
        ret =  resolve_create_db_stmt(result_plan, node, query_id);
        break;
      }
      case T_USE_DATABASE:
      {
        ret =  resolve_use_db_stmt(result_plan, node, query_id);
        break;
      }
      case T_DROP_DATABASE:
      {
        ret = resolve_drop_db_stmt(result_plan, node, query_id);
        break;
      }
      //add e
      case T_SET_PASSWORD:
      {
        ret = resolve_set_password_stmt(result_plan, node, query_id);
        break;
      }
      case T_RENAME_USER:
      {
        ret = resolve_rename_user_stmt(result_plan, node, query_id);
        break;
      }
      case T_LOCK_USER:
      {
        ret = resolve_lock_user_stmt(result_plan, node, query_id);
        break;
      }
      case T_GRANT:
      {
        ret = resolve_grant_stmt(result_plan, node, query_id);
        break;
      }
      case T_REVOKE:
      {
        ret = resolve_revoke_stmt(result_plan, node, query_id);
        break;
      }
      case T_PREPARE:
      {
        ret = resolve_prepare_stmt(result_plan, node, query_id);
        break;
      }
      case T_VARIABLE_SET:
      {
        ret = resolve_variable_set_stmt(result_plan, node, query_id);
        break;
      }
      case T_EXECUTE:
      {
        ret = resolve_execute_stmt(result_plan, node, query_id);
        break;
      }
      case T_DEALLOCATE:
      {
        ret = resolve_deallocate_stmt(result_plan, node, query_id);
        break;
      }
      case T_BEGIN:
        ret = resolve_start_trans_stmt(result_plan, node, query_id);
        break;
      case T_COMMIT:
        ret = resolve_commit_stmt(result_plan, node, query_id);
        break;
      case T_ROLLBACK:
        ret = resolve_rollback_stmt(result_plan, node, query_id);
        break;
      case T_ALTER_SYSTEM:
        ret = resolve_alter_sys_cnf_stmt(result_plan, node, query_id);
        break;
      case T_KILL:
        ret = resolve_kill_stmt(result_plan, node, query_id);
        break;
      case T_CHANGE_OBI:
        ret = resolve_change_obi(result_plan, node, query_id);
        break;
      //add liuzy [sequence] [JHOBv0.1] 20150327:b
      case T_SEQUENCE_CREATE:
        ret = resolve_sequence_create_stmt(result_plan, node, query_id);
        break;
      case T_SEQUENCE_DROP:
        ret = resolve_sequence_drop_stmt(result_plan, node, query_id);
        break;
      //add 20150327:e
      default:
        TBSYS_LOG(ERROR, "unknown top node type=%d", node->type_);
        ret = OB_ERR_UNEXPECTED;
        break;
    };
  }
  if (ret == OB_SUCCESS && result_plan->is_prepare_ != 1
    && node->type_ != T_STMT_LIST && node->type_ != T_PREPARE)
  {
    ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
    if (logical_plan != NULL && logical_plan->get_question_mark_size() > 0)
    {
      ret = OB_ERR_PARSE_SQL;
      PARSER_LOG("Uknown column '?'");
    }
  }
  if (ret != OB_SUCCESS && result_plan->plan_tree_ != NULL)
  {
    ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
    logical_plan->~ObLogicalPlan();
    parse_free(result_plan->plan_tree_);
    result_plan->plan_tree_ = NULL;
  }
  return ret;
}

extern void destroy_plan(ResultPlan* result_plan)
{
  if (result_plan->plan_tree_ == NULL)
    return;

  //delete (static_cast<multi_plan*>(result_plan->plan_tree_));
  parse_free(result_plan->plan_tree_);

  result_plan->plan_tree_ = NULL;
  result_plan->name_pool_ = NULL;
  result_plan->schema_checker_ = NULL;
  result_plan->session_info_ = NULL;//add liumz, [multi_database.sql]:20150613
}
