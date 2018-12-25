/**
 * (C) 2010-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_tablet_merge_filter.cpp for expire row of tablet when do
 * daily merge.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <time.h>
#include <tblog.h>
#include "common/utility.h"
#include "sql/ob_item_type.h"
#include "ob_tablet_merge_filter.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace common;
    using namespace common::hash;
    using namespace sstable;

    const char* ObTabletMergerFilter::EXPIRE_CONDITION_CNAME_PREFIX = "__EXPIRE_COND_";

    ObTabletMergerFilter::ObTabletMergerFilter()
    : inited_(false), hash_map_inited_(false),
      expire_row_filter_(DEFAULT_SSTABLE_ROW_COUNT, &expire_filter_allocator_),
      schema_(NULL), table_id_(OB_INVALID_ID), frozen_version_(0), frozen_time_(0),
      column_group_num_(0), extra_column_cnt_(0), need_filter_(false)
    {

    }

    ObTabletMergerFilter::~ObTabletMergerFilter()
    {
      if (hash_map_inited_)
      {
        cname_to_idx_map_.destroy();
      }
      expire_row_filter_.destroy();
    }

    int ObTabletMergerFilter::init(const ObSchemaManagerV2& schema, const int64_t column_group_num,
      ObTablet* tablet, const int64_t frozen_version, const int64_t frozen_time)
    {
      int ret           = OB_SUCCESS;
      uint64_t table_id = OB_INVALID_ID;

      if (NULL == tablet || frozen_version <= 1 || frozen_time < 0)
      {
        TBSYS_LOG(WARN, "invalid param, tablet=%p, frozen_version=%ld, "
                        "frozen_time=%ld",
          tablet, frozen_version, frozen_time);
        ret = OB_ERROR;
      }
      else if (OB_INVALID_ID == (table_id = tablet->get_range().table_id_))
      {
        TBSYS_LOG(WARN, "invalid table_id=%lu", table_id);
        ret = OB_ERROR;
      }
      else if (!hash_map_inited_)
      {
        ret = cname_to_idx_map_.create(HASH_MAP_BUCKET_NUM);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to create hash map for select column "
                          "name to column index map ret=%d", ret);
        }
        else
        {
          hash_map_inited_ = true;
        }
      }

      if (OB_SUCCESS == ret)
      {
        reset();
        schema_ = &schema;
        table_id_ = table_id;
        frozen_version_ = frozen_version;
        frozen_time_ = frozen_time;
        need_filter_ = need_filter(frozen_version, tablet->get_last_do_expire_version());
        column_group_num_ = column_group_num;
      }

      if (OB_SUCCESS == ret)
      {
        inited_ = true;
      }

      return ret;
    }

    void ObTabletMergerFilter::reset()
    {
      inited_ = false;
      cname_to_idx_map_.clear();
      expire_filter_.reset();
      expire_row_filter_.clear();
      schema_ = NULL;
      table_id_ = OB_INVALID_ID;
      frozen_version_ = 0;
      frozen_time_ = 0;
      column_group_num_ = 0;
      extra_column_cnt_ = 0;
      need_filter_ = false;
    }

    void ObTabletMergerFilter::clear_expire_bitmap()
    {
      expire_row_filter_.clear();
    }

    bool ObTabletMergerFilter::need_filter(const int64_t frozen_version,
      const int64_t last_do_expire_version)
    {
      bool ret            = false;
      uint64_t column_id  = 0;
      int64_t duration    = 0;
      const ObTableSchema *table_schema = NULL;
      const char* expire_condition      = NULL;

      if (frozen_version <= 1 || last_do_expire_version < 0)
      {
        TBSYS_LOG(WARN, "invalid param, frozen_version=%ld, last_do_expire_version=%ld",
          frozen_version, last_do_expire_version);
      }
      else if (NULL == (table_schema = schema_->get_table_schema(table_id_)))
      {
        TBSYS_LOG(ERROR, "table=%lu not exist.", table_id_);
      }
      else if (frozen_version >= table_schema->get_expire_frequency() + last_do_expire_version)
      {
        expire_condition = table_schema->get_expire_condition();
        ret = check_expire_condition_need_filter(*table_schema);
        TBSYS_LOG(INFO, "expire info, frozen_version=%ld, expire_frequency=%ld, "
                        "last_do_expire_version=%ld, table_id=%lu, "
                        "expire column id=%ld, expire_duration=%ld, "
                        "expire_condition=%s, need_filter=%d, ignore_filter=%d",
          frozen_version, table_schema->get_expire_frequency(),
          last_do_expire_version, table_id_, column_id, duration,
          (NULL == expire_condition) ? "" : expire_condition, ret,
          (NULL != expire_condition && expire_condition[0] != '\0' && !ret));
      }

      return ret;
    }

    bool ObTabletMergerFilter::check_expire_condition_need_filter(
       const ObTableSchema& table_schema) const
    {
      int ret   = OB_SUCCESS;
      bool bret = true;
      const char* expire_condition = NULL;
      char infix_condition_expr[OB_MAX_EXPIRE_CONDITION_LENGTH];
      ObString cond_expr;
      ObExpressionParser* parser = new (std::nothrow) ObExpressionParser();

      if (NULL == parser)
      {
        TBSYS_LOG(WARN, "failed to new expression parser");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (NULL != (expire_condition = table_schema.get_expire_condition()) 
               && expire_condition[0] != '\0')
      {
        if (static_cast<int64_t>(strlen(expire_condition)) >= OB_MAX_EXPIRE_CONDITION_LENGTH)
        {
          TBSYS_LOG(WARN, "expire condition too large, expire_condition_len=%zu, "
              "max_condition_len=%ld, table_name=%s",
              strlen(expire_condition), OB_MAX_EXPIRE_CONDITION_LENGTH,
              table_schema.get_table_name());
          ret = OB_ERROR;
        }
        else
        {
          strncpy(infix_condition_expr, expire_condition, OB_MAX_EXPIRE_CONDITION_LENGTH);
          ret = replace_system_variable(infix_condition_expr,
              OB_MAX_EXPIRE_CONDITION_LENGTH);
          if (OB_SUCCESS == ret)
          {
            cond_expr.assign_ptr(infix_condition_expr,
                static_cast<int32_t>(strlen(infix_condition_expr)));
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
            //ret = check_expire_dependent_columns(cond_expr, table_schema, *parser);
            ret = check_expire_dependent_columns(cond_expr, table_schema, *parser, true);
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to check expire dependent columns, infix_expr=%s",
                  infix_condition_expr);
            }
          }
        }
      }
      else 
      {
        bret = false;
      }

      if (OB_SUCCESS != ret)
      {
        bret = false;
      }

      if (NULL != parser)
      {
        delete parser;
        parser = NULL;
      }

      return bret;
    }

    //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
    //int ObTabletMergerFilter::check_expire_dependent_columns(const ObString& expr,
    //  const ObTableSchema& table_schema, ObExpressionParser& parser) const
    int ObTabletMergerFilter::check_expire_dependent_columns(const ObString& expr,
                                                                                                               const ObTableSchema& table_schema,
                                                                                                               ObExpressionParser& parser,
                                                                                                               bool is_expire_info) const
     //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
    {
      int ret               = OB_SUCCESS;
      int i                 = 0;
      int64_t type          = 0;
      int64_t postfix_size  = 0;
      ObString key_col_name;
      ObArrayHelper<ObObj> expr_array;
      ObObj post_expr[OB_MAX_COMPOSITE_SYMBOL_COUNT];
      const ObColumnSchemaV2* column_schema = NULL;
      ObString table_name;

      expr_array.init(OB_MAX_COMPOSITE_SYMBOL_COUNT, post_expr);
      //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
      //ret = parser.parse(expr, expr_array);
      ret = parser.parse(expr, expr_array, is_expire_info);
      //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
      if (OB_SUCCESS == ret)
      {
        postfix_size = expr_array.get_array_index();
      }
      else
      {
        TBSYS_LOG(WARN, "parse infix expression to postfix expression "
            "error, ret=%d", ret);
      }

      if (OB_SUCCESS == ret)
      {
        i = 0;
        while(i < postfix_size - 1)
        {
          if (OB_SUCCESS != expr_array.at(i)->get_int(type))
          {
            TBSYS_LOG(WARN, "unexpected data type. int expected, but actual type is %d",
                expr_array.at(i)->get_type());
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          else
          {
            if (ObExpression::COLUMN_IDX == type)
            {
              if (OB_SUCCESS != expr_array.at(i+1)->get_varchar(key_col_name))
              {
                TBSYS_LOG(WARN, "unexpected data type. varchar expected, "
                    "but actual type is %d",
                    expr_array.at(i+1)->get_type());
                ret = OB_ERR_UNEXPECTED;
                break;
              }
              else
              {
                table_name.assign_ptr(const_cast<char*>(table_schema.get_table_name()),
                    static_cast<int32_t>(strlen(table_schema.get_table_name())));
                column_schema = schema_->get_column_schema(table_name, key_col_name);
                if (NULL == column_schema)
                {
                  TBSYS_LOG(ERROR, "expire condition includes invalid column name, "
                      "table_name=%s, column_name=%.*s, expire_condition=%.*s",
                      table_schema.get_table_name(), key_col_name.length(), key_col_name.ptr(),
                      expr.length(), expr.ptr());
                  ret = OB_ERROR;
                  break;
                }
              }
            }/* only column name needs to decode. other type is ignored */
            i += 2; // skip <type, data> (2 objects as an element)
          }
        }
      }

      return ret;
    }

    int ObTabletMergerFilter::replace_system_variable(char* expire_condition,
      const int64_t buf_size) const
    {
      int ret = OB_SUCCESS;
      struct tm frozen_tm;
      struct tm* tm = NULL;
      time_t frozen_second = 0;
      char replace_str_buf[32];

      if (NULL == expire_condition || buf_size <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, expire_condition=%p, buf_size=%ld",
          expire_condition, buf_size);
        ret = OB_ERROR;
      }
      else
      {
        frozen_second = static_cast<time_t>(frozen_time_ / 1000 / 1000);
        tm = localtime_r(&frozen_second, &frozen_tm);
        if (NULL == tm)
        {
          TBSYS_LOG(WARN, "failed to transfer frozen time to tm, "
                          "frozen_time_=%ld, frozen_second=%ld",
            frozen_time_, frozen_second);
          ret = OB_ERROR;
        }
        else
        {
          //modify peiouya [Expire_condition_modify] 20140909:b
          if ((NULL != strstr(expire_condition,SYS_DATE)) && (NULL != strstr(expire_condition,SYS_DAY)))
          {
            ret = OB_ERROR;
            TBSYS_LOG(WARN, "$SYS_DATE and $SYS_DAY, the two can only and must choose one!");
          }
          else if (NULL != strstr(expire_condition,SYS_DATE))
          {
            strftime(replace_str_buf, sizeof(replace_str_buf),
              "#%Y-%m-%d %H:%M:%S#", tm);
            ret = replace_str(expire_condition, buf_size, SYS_DATE, replace_str_buf);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to replace $SYS_DATE in expire condition");
            }
          }
          else
          {
            strftime(replace_str_buf, sizeof(replace_str_buf),
              "#%Y-%m-%d#", tm);
            ret = replace_str(expire_condition, buf_size, SYS_DAY, replace_str_buf);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to replace $SYS_DAY in expire condition");
            }
          }
          //modify 20140909:b
        }
      }

      return ret;
    }

    int ObTabletMergerFilter::trans_op_func_obj(const int64_t op, ObObj &obj_value1, ObObj &obj_value2)
    {
      struct OpMap
      {
        int32_t old_op;
        int32_t new_op;
        int32_t op_count;
      } op_map[] =
      {
        {ObExpression::ADD_FUNC, T_OP_ADD, 2} ,
        {ObExpression::SUB_FUNC, T_OP_MINUS, 2},
        {ObExpression::MUL_FUNC, T_OP_MUL, 2},
        {ObExpression::DIV_FUNC, T_OP_DIV, 2},
        {ObExpression::MOD_FUNC, T_OP_MOD, 2},

        {ObExpression::AND_FUNC, T_OP_AND, 2},
        {ObExpression::OR_FUNC,  T_OP_OR, 2},
        {ObExpression::NOT_FUNC, T_OP_NOT, 1},

        {ObExpression::LT_FUNC, T_OP_LT, 2},
        {ObExpression::LE_FUNC, T_OP_LE, 2},
        {ObExpression::EQ_FUNC, T_OP_EQ, 2},
        {ObExpression::NE_FUNC, T_OP_NE, 2},
        {ObExpression::GE_FUNC, T_OP_GE, 2},
        {ObExpression::GT_FUNC, T_OP_GE, 2},
        {ObExpression::LIKE_FUNC, T_OP_LIKE, 2},
        {ObExpression::IS_FUNC, T_OP_IS, 2},
        {ObExpression::IS_NOT_FUNC, T_OP_IS_NOT, 2},

        {ObExpression::MINUS_FUNC, T_OP_NEG, 1},
        {ObExpression::PLUS_FUNC, T_OP_POS, 1},
        {ObExpression::CAST_THIN_FUNC,T_OP_CAST_THIN,2}  //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608
      };

      int ret = OB_ENTRY_NOT_EXIST;

      for (uint32_t i = 0; i < sizeof(op_map); ++i)
      {
        OpMap & m = op_map[i];
        if (op == m.old_op)
        {
          obj_value1.set_int(m.new_op);
          obj_value2.set_int(m.op_count);
          break;
        }
      }


      return ret;
    }

    //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
    /*int ObTabletMergerFilter::infix_str_to_sql_expression(
        const common::ObString& table_name,
        const common::ObString& cond_expr, sql::ObSqlExpression& sql_expr)
    */
    int ObTabletMergerFilter::infix_str_to_sql_expression(const common::ObString&       table_name, 
                                                          const common::ObString&       cond_expr, 
                                                          sql::ObSqlExpression&         sql_expr,
                                                          const bool                    is_expire_info)
    {
      int ret = OB_SUCCESS;
      int64_t obj_count = OB_MAX_EXPIRE_CONDITION_LENGTH;
      int64_t expr_obj_count = 0;
      int64_t i = 0;
      int64_t op = 0;
      int64_t type = 0;

      const ObColumnSchemaV2 *column_schema = NULL;

      ObObj objs[obj_count];
      ObArrayHelper<ObObj> expr_array;
      expr_array.init(obj_count, objs);
      ObString column_name;
      ObObj obj_value1;
      ObObj obj_value2;

      //if (OB_SUCCESS != (ret =  post_expression_parser_.parse(cond_expr, expr_array)))
      if (OB_SUCCESS != (ret =  post_expression_parser_.parse(cond_expr, expr_array, is_expire_info)))
      {
        TBSYS_LOG(WARN, "parse cond_expr:%.*s error, ret=%d",
            cond_expr.length(), cond_expr.ptr(), ret);
      }
      else if ((expr_obj_count = expr_array.get_array_index()) <= 0)
      {
        TBSYS_LOG(WARN, "no expr obj:expr_obj_count=%ld", expr_obj_count);
      }
      else
      {
        while (i < expr_obj_count && OB_SUCCESS == ret)
        {
          ObObj *obj = expr_array.at(i);
          column_name.assign_ptr(NULL, 0);
          if (OB_SUCCESS != (ret = obj->get_int(type)))
          {
            TBSYS_LOG(WARN, "unexpected data type. int expected, but actual type is %d",
                expr_array.at(i)->get_type());
            ret = OB_ERR_UNEXPECTED;
          }
          else if (type == ObExpression::COLUMN_IDX)
          {
            if (expr_array.at(i+1)->get_type() != ObVarcharType
                || OB_SUCCESS != expr_array.at(i+1)->get_varchar(column_name)
                || NULL == column_name.ptr() || column_name.length() <= 0)
            {
              TBSYS_LOG(WARN, "illegal postfix expr, type=%d, colname:%.*s",
                  expr_array.at(i+1)->get_type(), column_name.length(), column_name.ptr());
              ret = OB_INVALID_ARGUMENT;
            }
            else if (NULL == (column_schema = schema_->get_column_schema(table_name, column_name)))
            {
              TBSYS_LOG(WARN, "invalid column :%.*s", column_name.length(), column_name.ptr());
              ret = OB_INVALID_ARGUMENT;
            }
            else
            {
              obj_value1.set_int(table_id_);
              obj_value2.set_int(column_schema->get_id());
              i += 2;
            }
          }
          else if (type == ObExpression::OP)
          {
            expr_array.at(i+1)->get_int(op);
            trans_op_func_obj(op, obj_value1, obj_value2);
            i += 2;
          }
          else if (type == ObExpression::CONST_OBJ)
          {
            // CONST_OBJ only need one operand (const value)
            obj_value1 = *expr_array.at(i+1);
            i += 2;
          }
          else if (type == ObExpression::END)
          {
            ++i;
          }

          if (OB_SUCCESS == ret && type != ObExpression::END)
          {
            // item type same.
            // don't add END type obj
            // COLUMN_IDX, OP, CONST_OBJ
            if (OB_SUCCESS != (ret = sql_expr.add_expr_obj(*obj)))
            {
              TBSYS_LOG(WARN, "add expr obj :%s error, ret=%d", to_cstring(*obj), ret);
            }
            else  if (OB_SUCCESS != (ret = sql_expr.add_expr_obj(obj_value1)))
            {
              TBSYS_LOG(WARN, "add expr obj :%s error, ret=%d", to_cstring(obj_value1), ret);
            }
            else if (type == ObExpression::COLUMN_IDX || type == ObExpression::OP)
            {
              if (OB_SUCCESS != (ret = sql_expr.add_expr_obj(obj_value2)))
              {
                TBSYS_LOG(WARN, "add expr obj :%s error, ret=%d", to_cstring(obj_value2), ret);
              }
            }
          }

        }
      }
      return ret;
    }

    /*int ObTabletMergerFilter::infix_to_postfix(const ObString& expr,
        ObScanParam& scan_param, ObObj* objs, const int64_t obj_count)
    */
    int ObTabletMergerFilter::infix_to_postfix(const common::ObString&       expr, 
                                               common::ObScanParam&          scan_param, 
                                               common::ObObj*                objs, 
                                               const int64_t                 obj_count,
                                               const bool                    is_expire_info)
    {
      int ret               = OB_SUCCESS;
      int hash_ret          = 0;
      int i                 = 0;
      int64_t type          = 0;
      int64_t val_index     = 0;
      int64_t postfix_size  = 0;
      ObString key_col_name;
      ObArrayHelper<ObObj> expr_array;

      if (NULL == objs || obj_count <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, objs=%p, obj_count=%ld",
          objs, obj_count);
        ret = OB_ERROR;
      }
      else
      {
        expr_array.init(obj_count, objs);
        //ret = post_expression_parser_.parse(expr, expr_array);
        ret = post_expression_parser_.parse(expr, expr_array, is_expire_info);
        TBSYS_LOG(DEBUG, "parse done. expr_array len=%ld",
           expr_array.get_array_index());
        if (OB_SUCCESS == ret)
        {
          postfix_size = expr_array.get_array_index();
        }
        else
        {
          TBSYS_LOG(WARN, "parse infix expression to postfix expression "
                          "error, ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        // 基本算法如下：
        //  1. 遍历expr_array数组，将其中类型为COLUMN_IDX的Obj读出
        //  2. 将obj中表示列名的值key_col_name读出，到hashmap中查找对应index
        //      scan_param.some_hash_map(key_col_name, val_index)
        //  3. 如果找到，则将obj的内容填充为val_index
        //     如果找不到，则报错返回

        i = 0;
        while(i < postfix_size - 1)
        {
          if (OB_SUCCESS != expr_array.at(i)->get_int(type))
          {
            TBSYS_LOG(WARN, "unexpected data type. int expected, but actual type is %d",
                expr_array.at(i)->get_type());
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          else
          {
            if (ObExpression::COLUMN_IDX == type)
            {
              if (OB_SUCCESS != expr_array.at(i+1)->get_varchar(key_col_name))
              {
                TBSYS_LOG(WARN, "unexpected data type. varchar expected, "
                                "but actual type is %d",
                    expr_array.at(i+1)->get_type());
                ret = OB_ERR_UNEXPECTED;
                break;
              }
              else
              {
                hash_ret = cname_to_idx_map_.get(key_col_name, val_index);
                if (HASH_EXIST != hash_ret)
                {
                  ret = add_extra_basic_column(scan_param, key_col_name, val_index);
                  if (OB_SUCCESS != ret || val_index < 0)
                  {
                    TBSYS_LOG(WARN, "failed add extra column into scan param, "
                                    "column_name=%.*s, table_id=%lu, val_index=%ld",
                      key_col_name.length(), key_col_name.ptr(), table_id_, val_index);
                    break;
                  }
                }
                if (OB_SUCCESS == ret)
                {
                  /* decode. quite simple, isn't it. */
                  expr_array.at(i+1)->set_int(val_index);
                  TBSYS_LOG(DEBUG, "succ decode.  key(%.*s) -> %ld",
                      key_col_name.length(), key_col_name.ptr(), val_index);
                }
              }
            }/* only column name needs to decode. other type is ignored */
            i += 2; // skip <type, data> (2 objects as an element)
          }
        }
      }

      return ret;
    }
    //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e

    int ObTabletMergerFilter::add_extra_basic_column(ObScanParam& scan_param,
      const ObString& column_name, int64_t& column_index)
    {
      int ret             = OB_SUCCESS;
      int hash_err        = 0;
      int64_t column_size = 0;
      const ObTableSchema* table_schema     = NULL;
      const ObColumnSchemaV2* column_schema = NULL;
      ObString table_name;

      column_index = -1;

      if (NULL == column_name.ptr() || column_name.length() <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, column_name_ptr=%p, column_name_len=%d",
          column_name.ptr(), column_name.length());
        ret = OB_ERROR;
      }
      else if (NULL == (table_schema = schema_->get_table_schema(table_id_)))
      {
        TBSYS_LOG(ERROR, "table=%lu not exist", table_id_);
        ret = OB_ERROR;
      }
      else
      {
        table_name.assign_ptr(const_cast<char*>(table_schema->get_table_name()),
          static_cast<int32_t>(strlen(table_schema->get_table_name())));
        column_schema = schema_->get_column_schema(table_name, column_name);
        if (NULL == column_schema)
        {
          TBSYS_LOG(WARN, "failed to get column schema, table_name=%s, "
                          "column_name=%.*s",
            table_schema->get_table_name(), column_name.length(), column_name.ptr());
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        column_size = scan_param.get_column_id_size();
        if (HASH_INSERT_SUCC != (hash_err = cname_to_idx_map_.set(
          column_name, column_size)))
        {
          TBSYS_LOG(WARN, "fail to add expire column <column_name, idx> pair "
                          "hash_err=%d, column_name=%.*s, expire_column_idx=%ld",
            hash_err, column_name.length(), column_name.ptr(), column_size);
          ret = OB_ERROR;
        }
        else if (OB_SUCCESS != (ret = scan_param.add_column(column_schema->get_id())))
        {
          TBSYS_LOG(WARN, "failed to add expire column into scan_param, "
                          "column_id=%lu, ret=%d",
            column_schema->get_id(), ret);
        }
        else
        {
          column_index = column_size;
          extra_column_cnt_ += 1;
        }
      }

      return ret;
    }

    int ObTabletMergerFilter::add_extra_composite_column(ObScanParam& scan_param,
      const ObObj* post_expr)
    {
      int ret = OB_SUCCESS;
      int hash_ret = 0;
      int64_t column_size = 0;
      int64_t comp_cname_len = 0;
      char comp_cname[MAX_COMPOSITE_CNAME_LEN];
      ObString comp_cname_str;
      ObObj true_obj;

      if (NULL == post_expr)
      {
        TBSYS_LOG(WARN, "invalid param, post_expr=%p", post_expr);
        ret = OB_ERROR;
      }
      else
      {
        column_size = scan_param.get_column_id_size();
        comp_cname_len = snprintf(comp_cname, sizeof(comp_cname),
          "%s%ld", EXPIRE_CONDITION_CNAME_PREFIX, column_size);
        comp_cname_str.assign_ptr(comp_cname, static_cast<int32_t>(comp_cname_len));
        if (HASH_INSERT_SUCC != (hash_ret = cname_to_idx_map_.set(
          comp_cname_str, column_size)))
        {
          TBSYS_LOG(WARN, "fail to add expire composite column <column_name, idx> pair "
                          "hash_err=%d, column_name=%s, expire_column_idx=%ld",
            hash_ret, comp_cname, column_size);
          ret = OB_ERROR;
        }
        else if (OB_SUCCESS != (ret = scan_param.add_column(post_expr)))
        {
          TBSYS_LOG(WARN, "failed to add expire composite column into scan_param, "
                          "column_name=%s, ret=%d",
            comp_cname, ret);
        }
        else
        {
          true_obj.set_bool(true);
          ret = expire_filter_.add_cond(scan_param.get_column_id_size(),
            EQ, true_obj);
          if (OB_SUCCESS == ret)
          {
            extra_column_cnt_ += 1;
          }
        }
      }

      return ret;
    }

    int ObTabletMergerFilter::build_expire_condition_filter(ObScanParam& scan_param)
    {
      int ret                           = OB_SUCCESS;
      const ObTableSchema *table_schema = NULL;
      const char* expire_condition      = NULL;
      char infix_condition_expr[OB_MAX_EXPIRE_CONDITION_LENGTH];
      ObObj post_expr[OB_MAX_COMPOSITE_SYMBOL_COUNT];
      ObString cond_expr;
      ObObj true_obj;

      if (NULL == (table_schema = schema_->get_table_schema(table_id_)))
      {
        TBSYS_LOG(ERROR, "table=%lu not exist", table_id_);
        ret = OB_ERROR;
      }
      else if (NULL != (expire_condition = table_schema->get_expire_condition())
               && expire_condition[0] != '\0')
      {
        if (static_cast<int64_t>(strlen(expire_condition)) >= OB_MAX_EXPIRE_CONDITION_LENGTH)
        {
          TBSYS_LOG(WARN, "expire condition too large, expire_condition_len=%zu, "
                          "max_condition_len=%ld",
            strlen(expire_condition), OB_MAX_EXPIRE_CONDITION_LENGTH);
          ret = OB_ERROR;
        }
        else
        {
          strcpy(infix_condition_expr, expire_condition);
          ret = replace_system_variable(infix_condition_expr,
            OB_MAX_EXPIRE_CONDITION_LENGTH);
          if (OB_SUCCESS == ret)
          {
            cond_expr.assign_ptr(infix_condition_expr,
              static_cast<int32_t>(strlen(infix_condition_expr)));
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
            //ret = infix_to_postfix(cond_expr, scan_param, post_expr,
            //  OB_MAX_COMPOSITE_SYMBOL_COUNT);
            ret = infix_to_postfix(cond_expr, scan_param, post_expr, OB_MAX_COMPOSITE_SYMBOL_COUNT, true);
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to transfer infix expression to postfix "
                              "expression, infix_expr=%s",
                infix_condition_expr);
            }
            else
            {
              TBSYS_LOG(INFO, "success to transfer infix expression to postfix "
                              "expression, infix_expr=%s",
                infix_condition_expr);
            }
          }

          if (OB_SUCCESS == ret)
          {
            ret = add_extra_composite_column(scan_param, post_expr);
          }
        }
      }

      return ret;
    }

    int ObTabletMergerFilter::fill_cname_to_idx_map(const uint64_t column_group_id)
    {
      int ret                     = OB_SUCCESS;
      int hash_err                = 0;
      int32_t size                = 0;
      int64_t column_index        = 0;
      const ObColumnSchemaV2* col = NULL;
      const ObTableSchema* ts     = NULL;
      ObString column_name;

      col = schema_->get_group_schema(table_id_, column_group_id, size);
      ts  = schema_->get_table_schema(table_id_);
      if (NULL == col || NULL == ts || size <= 0)
      {
        TBSYS_LOG(WARN, "cann't find this column group=%lu, table_id=%ld", column_group_id, table_id_);
        ret = OB_ERROR;
      }
      else
      {
        for(int64_t i = 0; i < size; ++i)
        {
          if (ts->get_rowkey_info().is_rowkey_column(col[i].get_id()))
          {
            TBSYS_LOG(INFO, "id=%ld is rowkey column of table:%ld", col[i].get_id(), table_id_);
            // erase rowkey columns;
            continue;
          }
          column_name.assign_ptr(const_cast<char*>((col + i)->get_name()),
            static_cast<int32_t>(strlen((col + i)->get_name())));
          if (HASH_INSERT_SUCC != (hash_err = cname_to_idx_map_.set(column_name, column_index)))
          {
            TBSYS_LOG(WARN, "fail to add column <column_name, idx> pair hash_err=%d",
              hash_err);
            ret = OB_ERROR;
            break;
          }
          column_index++;
        }
      }

      return ret;
    }

    int ObTabletMergerFilter::adjust_scan_param(const int64_t column_group_idx,
        const uint64_t column_group_id, ObScanParam& scan_param)
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "tablet merge filter doesn't initialize");
        ret = OB_ERROR;
      }
      else if (column_group_idx < 0 || column_group_idx >= column_group_num_
          || OB_INVALID_ID == column_group_id)
      {
        TBSYS_LOG(WARN, "invalid param, column_group_idx=%ld, column_group_num_=%ld, "
            "column_group_id=%lu",
            column_group_idx, column_group_num_, column_group_id);
        ret = OB_ERROR;
      }
      else if (need_filter_ && 0 == column_group_idx)
      {
        extra_column_cnt_ = 0;
        cname_to_idx_map_.clear();
        expire_filter_.reset();
        ret = fill_cname_to_idx_map(column_group_id);
        if (OB_SUCCESS == ret
            && cname_to_idx_map_.size() != scan_param.get_column_id_size())
        {
          TBSYS_LOG(WARN, "column count in scan param is unexpected, "
              "column_count_in_param=%ld, scan_column_count=%ld",
              scan_param.get_column_id_size(), cname_to_idx_map_.size());
          ret = OB_ERROR;
        }
        if (OB_SUCCESS == ret)
        {
          ret = build_expire_condition_filter(scan_param);
        }
      }

      return ret;
    }

    int ObTabletMergerFilter::check_and_trim_row(const int64_t column_group_idx,
        const int64_t row_num, ObSSTableRow& sstable_row, bool& is_expired)
    {
      int ret           = OB_SUCCESS;
      const ObObj* objs = NULL;
      int64_t obj_count = 0;
      int64_t rowkey_column_count = 0;
      is_expired        = false;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "tablet merge filter doesn't initialize");
        ret = OB_ERROR;
      }
      else if (column_group_idx < 0 || column_group_idx >= column_group_num_
          || row_num < 0)
      {
        TBSYS_LOG(WARN, "invalid param, column_group_idx=%ld, column_group_num_=%ld, "
            "row_num=%ld",
            column_group_idx, column_group_num_, row_num);
        ret = OB_ERROR;
      }
      else if (need_filter_ && 0 == column_group_idx)
      {
        rowkey_column_count = sstable_row.get_rowkey_obj_count();
        objs = sstable_row.get_row_objs(obj_count);
        if (obj_count - rowkey_column_count != cname_to_idx_map_.size())
        {
          TBSYS_LOG(WARN, "column count in sstable row is unexpected, "
                          "column_count_in_row=%ld, scan_column_count=%ld",
            obj_count - rowkey_column_count, cname_to_idx_map_.size());
          ret = OB_ERROR;
        }
        else
        {
          /**
           * because check() default return true, if there is not
           * condition in filter, it return true, we call
           * default_false_check() to check whether the current is
           * expired, if no filter, it return false. it only support
           * 'AND', only if all the expire condition is satisfied, the
           * function default_false_check() return true.
           */
          ret = expire_filter_.default_false_check(objs + rowkey_column_count,
                obj_count - rowkey_column_count, is_expired);
          //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
          if(OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to filter expire sstable row, objs=%p, "
                            "obj_count=%ld, ret=%d",
              objs + rowkey_column_count, obj_count - rowkey_column_count, ret);
            is_expired = false;
            ret = OB_SUCCESS;
          }
          //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
        }
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to filter expire sstable row, objs=%p, "
                          "obj_count=%ld, ret=%d",
            objs + rowkey_column_count, obj_count - rowkey_column_count, ret);
        }
        else
        {
          /**
           * in order to do expire, we add some extra columns after the
           * basic columns to scan, after do expire, we trim the extra
           * columns here.
           */
          if (extra_column_cnt_ > 0)
          {
            ret = sstable_row.set_obj_count(obj_count - extra_column_cnt_);
          }

          if (OB_SUCCESS == ret && is_expired && column_group_num_ > 1)
          {
            ret = expire_row_filter_.set(row_num, true);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to set expire bitmap filter, column_group_idx=%ld, "
                              "row_num=%ld, is_expired=%d, ret=%d",
                column_group_idx, row_num, is_expired, ret);
            }
          }
        }
      }
      else if (need_filter_ && column_group_idx > 0 && column_group_num_ > 1)
      {
        is_expired = expire_row_filter_.test(row_num);
      }

      return ret;
    }

    int ObTabletMergerFilter::build_expire_condition_filter(sql::ObSqlExpression& sql_expr, bool& has_filter)
    {
      int ret = OB_SUCCESS;
      const ObTableSchema *table_schema = NULL;
      const char* expire_condition      = NULL;

      char infix_condition_expr[OB_MAX_EXPIRE_CONDITION_LENGTH];
      ObString cond_expr;
      ObString table_name;
      has_filter = false;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "tablet merge filter doesn't initialize");
        ret = OB_ERROR;
      }
      else if (!need_filter_)
      {
        ret = OB_SUCCESS;
        // do nothing
      }
      else if (NULL == (table_schema = schema_->get_table_schema(table_id_)))
      {
        TBSYS_LOG(ERROR, "table=%lu not exist", table_id_);
        ret = OB_ERROR;
      }
      else if (NULL == (expire_condition = table_schema->get_expire_condition())
          || expire_condition[0] == '\0')
      {
        TBSYS_LOG(INFO, "table %ld has no expire condition expression.", table_id_);
        ret = OB_SUCCESS;
      }
      else if (static_cast<int64_t>(strlen(expire_condition)) >= OB_MAX_EXPIRE_CONDITION_LENGTH)
      {
        TBSYS_LOG(WARN, "table %ld expire condition too large,"
            "expire_condition_len=%zu, max_condition_len=%ld",
            table_id_, strlen(expire_condition), OB_MAX_EXPIRE_CONDITION_LENGTH);
        ret = OB_ERROR;
      }
      else
      {
        strcpy(infix_condition_expr, expire_condition);
        ret = replace_system_variable(infix_condition_expr, OB_MAX_EXPIRE_CONDITION_LENGTH);
        if (OB_SUCCESS == ret)
        {
          cond_expr.assign_ptr(infix_condition_expr,
              static_cast<int32_t>(strlen(infix_condition_expr)));
          table_name.assign(const_cast<char*>(table_schema->get_table_name()),
              static_cast<ObString::obstr_size_t>(strlen(table_schema->get_table_name())));
          //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
          //if (OB_SUCCESS != (ret = infix_str_to_sql_expression(table_name, cond_expr, sql_expr)))
          if (OB_SUCCESS != (ret = infix_str_to_sql_expression(table_name, cond_expr, sql_expr, true)))
          //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
          {
            TBSYS_LOG(WARN, "failed to transfer infix expression to postfix "
                "expression, infix_expr=%s",
                infix_condition_expr);
          }
          else
          {
            has_filter = true;
            TBSYS_LOG(INFO, "success to transfer infix expression to postfix "
                "expression, infix_expr=%s", infix_condition_expr);
          }
        }
      }
      return ret;
    }

    int ObTabletMergerFilter::finish_expire_filter(sql::ObSqlExpression& sql_expr,
        const bool has_column_filter, const bool has_condition_filter)
    {
      int ret = OB_SUCCESS;
      sql::ExprItem item;

      // add and
      if (has_column_filter && has_condition_filter)
      {
        item.type_ = T_OP_AND;
        item.value_.int_ = 2;
        if (OB_SUCCESS != (ret = sql_expr.add_expr_item(item)))
        {
          TBSYS_LOG(WARN, "add AND op item error, ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        item.type_ = T_OP_NOT;
        item.value_.int_ = 1;
        if (OB_SUCCESS != (ret = sql_expr.add_expr_item(item)))
        {
          TBSYS_LOG(WARN, "add NE op item error, ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = sql_expr.add_expr_item_end()))
        {
          TBSYS_LOG(WARN, "add_expr_item_end ret = %d", ret);
        }
      }

      return ret;
    }

    // add ObFilter to scan_param;
    int ObTabletMergerFilter::adjust_scan_param(sql::ObSqlScanParam& scan_param)
    {
      int ret = OB_SUCCESS;
      ObSqlExpression *sql_expr = ObSqlExpression::alloc();
      bool has_condition_filter = false;
      if (NULL == sql_expr)
      {
        TBSYS_LOG(WARN, "no memory");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS != (ret = build_expire_condition_filter(*sql_expr, has_condition_filter)))
      {
        ObSqlExpression::free(sql_expr);
        TBSYS_LOG(WARN, "build_expire_condition_filter error, ret =%d", ret);
      }
      else if (OB_SUCCESS != (ret = finish_expire_filter(*sql_expr, false, has_condition_filter)))
      {
        ObSqlExpression::free(sql_expr);
        TBSYS_LOG(WARN, "finish_expire_filter ret = %d", ret);
      }
      else if (OB_SUCCESS != (ret = scan_param.add_filter(sql_expr)))
      {
        TBSYS_LOG(WARN, "add_filter to scan_param ret = %d, sql_expr=%s", ret, to_cstring(*sql_expr));
      }
      else
      {
        TBSYS_LOG(INFO, "add_filter to scan_param sql_expr=%s", to_cstring(*sql_expr));
      }

      return ret;
    }


  } // namespace oceanbase::chunkserver
} // namespace Oceanbase
