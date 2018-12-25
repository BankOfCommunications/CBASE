#include <stdio.h>
#include "stmt.h"
#include "common_func.h"
#include "common/ob_malloc.h"
#include "common/ob_define.h"

namespace oceanbase
{ 
  namespace obsql
  {
    using namespace std;
    using namespace common;
    using namespace oceanbase::common;

    Stmt::Stmt(char* buf, int32_t size, 
                ObClientServerStub& rpc_stub,
                map<string, vector<RowKeySubType> >& rowkey_map)
      :query_string_(size, strlen(buf), buf), rpc_stub_(rpc_stub), rowkey_map_(rowkey_map)
    { }

    Stmt::~Stmt()
    {
      vector<ConditionNode>::iterator cit = condition_list_.begin();
      for (; cit != condition_list_.end(); cit++)
      {
        if (cit->row_key_.ptr())
        {
          ob_free(cit->row_key_.ptr());
          cit->row_key_.assign_ptr(NULL, 0);
        }
        if (cit->end_row_key_.ptr())
        {
          ob_free(cit->end_row_key_.ptr());
          cit->end_row_key_.assign_ptr(NULL, 0);
        }

        for (uint32_t i = 0; i < cit->raw_keynode_list_.size(); i++)
        {
          KeyNode& key_node = cit->raw_keynode_list_.at(i);
          for (uint32_t j = 0; j < key_node.sub_key_list_.size(); j++)
          {
            char *rowkey_part = key_node.sub_key_list_.at(j);
            if (!rowkey_part)
            {
              ob_free(rowkey_part);
              key_node.sub_key_list_.at(j) = NULL;
            }
          }
        }
      }
    }
    
    int32_t Stmt::query()
    {
      int32_t ret = OB_SUCCESS;
      
      ret = parse();
      if (ret != OB_SUCCESS) { fprintf(stderr, "parse error, ret = %d\n", ret); return ret; }
      ret = resolve();
      if (ret != OB_SUCCESS) { fprintf(stderr, "resolve error, ret = %d\n", ret); return ret; }
      ret = execute();
      if (ret != OB_SUCCESS) {  fprintf(stderr, "execute query error, ret = %d\n", ret); return ret; }
      ret = output();
      if (ret != OB_SUCCESS) {  fprintf(stderr, "output results error, ret = %d\n", ret); return ret; }
      return ret;
    }

    int32_t Stmt::init(ObClientServerStub& rpc_stub)
    {
      rpc_stub_ = rpc_stub;

      return OB_SUCCESS;
    }

    int32_t Stmt::parse_from(char *from_str)
    {
      int32_t ret = OB_SUCCESS;
      char *tbl_token = NULL;

      if (!from_str)
        return OB_ERROR;

      while ((tbl_token = string_split_by_ch(from_str, ',')) != NULL)
      {
        ret = add_table(tbl_token);
        if (ret != OB_SUCCESS)
        {
          fprintf(stderr, "Syntax error around \"%s\"\n", tbl_token);
          return ret;
        }
      }
      add_table(from_str);
      if (ret != OB_SUCCESS)
      {
        fprintf(stderr, "Syntax error around \"%s\"\n", tbl_token);
        return ret;
      }

      return ret;
    }

    int32_t Stmt::add_table(char *tbl_str)
    {
      int32_t ret = OB_SUCCESS;
      TableNode tbl_node;
      char *token = NULL;

      memset(&tbl_node, 0, sizeof(tbl_node));

      if (!(token = get_token(tbl_str)))
        return OB_ERROR;
      memcpy(tbl_node.table_name_, token, strlen(token));

      if ((token = get_token(tbl_str)) != NULL)
      {
        char *next_token = NULL;

        if (!(next_token = get_token(tbl_str)))
        {
          memcpy(tbl_node.table_alias_, token, strlen(token));
        }
        else
        {
          char *tail_token = NULL;
          if (strcasecmp(token, "as") == 0 && !(tail_token = get_token(tbl_str)))
            memcpy(tbl_node.table_alias_, next_token, strlen(next_token));
          else
            /* There is more words after alias */
            ret = OB_ERROR;
        }
      }

      table_list_.push_back(tbl_node);

      return ret;
    }

    int32_t Stmt::add_table(TableNode& tbl_node)
    {
      table_list_.push_back(tbl_node);

      return OB_SUCCESS;
    }

    int32_t Stmt::parse_where(char *where_str)
    {
      int32_t  ret = OB_SUCCESS;
      char     *front_str;
      bool     do_flag = true;

      while (do_flag) 
      {
        front_str = string_split_by_str(where_str, "or", false, true);
        if (!front_str)
        {
          do_flag = false;
          front_str = where_str;
        }

        ret = parse_and(front_str);
      }

      return ret;
    }

    int32_t Stmt::parse_and(char *and_str)
    {
      int32_t       ret = OB_SUCCESS;
      char          *front_str;
      ConditionNode cnd_node;
      int32_t       op = OB_ER_OP;
      bool          do_flag = true;

      memset(&cnd_node, 0, sizeof(cnd_node));
      cnd_node.is_equal_condition_ = false;
      cnd_node.border_flag_.set_min_value();
      cnd_node.border_flag_.set_max_value();

      while (do_flag)
      {
        char *variable_str = NULL;

        front_str = string_split_by_str(and_str, "and", false, true);
        if (!front_str)
        {
          do_flag = false;
          front_str = and_str;
        }
        
        if ((variable_str = string_split_by_str(front_str, ">=", true, false)) != NULL)
        {
          op = OB_GE_OP;
        }
        else if((variable_str = string_split_by_str(front_str, "<=", true, false)) != NULL)
        {
          op = OB_LE_OP;
        }
        else if((variable_str = string_split_by_ch(front_str, '=')) != NULL)
        {
          op = OB_EQ_OP;
        }
        else if((variable_str = string_split_by_ch(front_str, '<')) != NULL)
        {
          op = OB_LT_OP;
        }
        else if((variable_str = string_split_by_ch(front_str, '>')) != NULL)
        {
          op = OB_GT_OP;
        }
        else
        {
          /* Error operator */
          fprintf(stderr, "Syntax error around \"%s\"\n", front_str);
          fprintf(stderr, "Operators out of ('=', '<', '>', '<=', '>=') are not supported!\n");

          return OB_ERROR;
        }

        char *tbl_token = NULL;
        if ((tbl_token = string_split_by_ch(front_str, '.')) != NULL ||
            strcasecmp(front_str, "RowKey") == 0)
        {
          if (op > OB_EQ_OP)
            op = (op % 2 == 0) ? op + 1 : op - 1;

          strcpy(cnd_node.column_name_, front_str);
          ret = pase_rowkey_node(op, variable_str, cnd_node);

          if (ret != OB_SUCCESS)
          {
            fprintf(stderr, "Error value %s\n", variable_str);
            return OB_ERROR;
          }
        }
        else if ((tbl_token = string_split_by_ch(variable_str, '.')) != NULL ||
                  strcasecmp(variable_str, "RowKey") == 0)
        {
          strcpy(cnd_node.column_name_, variable_str);
          ret = pase_rowkey_node(op, front_str, cnd_node);

          if (ret != OB_SUCCESS)
          {
            fprintf(stderr, "Error value %s\n", front_str);
            return OB_ERROR;
          }
        }
        else
        {
          fprintf(stderr, "Error pair <%s, %s>\n", variable_str, front_str);
          return OB_ERROR;
        }
      }

      condition_list_.push_back(cnd_node);

      return ret;
    }

    int32_t Stmt::pase_rowkey_node(int32_t op, char *value, ConditionNode &cnd_node)
    {
      int32_t ret = OB_SUCCESS;
    
      if (strcasecmp(cnd_node.column_name_, "RowKey"))
      {
        fprintf(stderr, "Syntax error around \"%s\"\n", cnd_node.column_name_);
        fprintf(stderr, "Only \"Rowkey\" is supported in where clause now!\n ");

        return OB_ERROR;
      }

      KeyNode key_node;
      memset(&key_node, 0, sizeof(key_node));
      key_node.op_type_ = op;

      value = trim(value);
      int32_t   len = strlen(value);
      /* kick off the braces */
      while (value[0] == '(' && value[len - 1] == ')')
      {
        value[len - 1] = '\0';
        len -=2;
        value++;
      }

      vector<char *> sub_values;
      tbsys::CStringUtil::split(value, ",", sub_values);
      for (uint32_t i = 0; i < sub_values.size(); i++)
      {
        char *sub_val = duplicate_str(trim(sub_values.at(i)));
        key_node.sub_key_list_.push_back(sub_val);
      }

      cnd_node.raw_keynode_list_.push_back(key_node);

      return ret;
    }

    int32_t Stmt::resolve()
    {
      int32_t ret = OB_SUCCESS;

      if (ret == OB_SUCCESS)
        ret = resolve_tables();
      
      if (ret == OB_SUCCESS)
        ret = resolve_conditions();

      return ret;
    }
    
    int32_t Stmt::resolve_tables()
    {
      int32_t ret = OB_SUCCESS;
      
      if (strlen(schema_mgr_.get_app_name()) == 0)
        rpc_stub_.fetch_schema(schema_mgr_);

      for (vector<TableNode>::iterator tit = table_list_.begin(); tit != table_list_.end(); tit++)
      {
        const ObTableSchema *table_schema = schema_mgr_.get_table_schema(tit->table_name_);
        if (table_schema == NULL)
        {
          fprintf(stderr, "Unknown table \"%s\"\n", tit->table_name_);
          return OB_ERROR;
        }
        tit->table_id_ = table_schema->get_table_id();
      }

      return ret;
    }

    int32_t Stmt::resolve_conditions()
    {
      int32_t ret = OB_SUCCESS;
      int32_t max_rowey_len = 0;
      
      if (strlen(schema_mgr_.get_app_name()) == 0)
        rpc_stub_.fetch_schema(schema_mgr_);

      const ObTableSchema* ob_schema = schema_mgr_.table_begin();
      for (; ob_schema != schema_mgr_.table_end(); ob_schema++)
      {
        if (ob_schema->get_rowkey_max_length() > max_rowey_len)
          max_rowey_len = ob_schema->get_rowkey_max_length();
      }

      const vector<RowKeySubType> *sub_type_list = NULL;
      char *table_name = table_list_.begin()->table_name_;
      map<string, vector<RowKeySubType> >::const_iterator it = rowkey_map_.find(table_name);
      if (it != rowkey_map_.end()) 
      {
          sub_type_list = &(it->second);
      }
      /* resolve where clause */
      /* Only RowKey is allowed in where conditions now
            * We do not support other columns
            * if we want to do that, we need resolve these column names too
            */
      for (vector<ConditionNode>::iterator cit = condition_list_.begin(); cit != condition_list_.end(); cit++)
      {
        if (strcasecmp(cit->column_name_, "RowKey") != 0)
        {
          fprintf(stderr, "Only column RowKey is allowed now\n");
          return OB_ERROR;
        }

        for (uint32_t i = 0; i < cit->raw_keynode_list_.size(); i++)
        {
          KeyNode& key_node = cit->raw_keynode_list_.at(i);
          char *buf = (char *)ob_malloc(max_rowey_len);
          int64_t pos = 0;

          if (sub_type_list != NULL &&
              sub_type_list->size() != 0 &&
              key_node.sub_key_list_.size() != sub_type_list->size())
          {
            fprintf(stderr, "Syntax error: wrong number of sub-values in RowKey condition!\n");
            ob_free(buf);
            return OB_ERROR;
          }
          else if ((sub_type_list == NULL || sub_type_list->size() == 0) && 
                    key_node.sub_key_list_.size() != 1)
          {
            fprintf(stderr, "Syntax error: wrong number of sub-values in RowKey condition!\n");
            ob_free(buf);
            return OB_ERROR;
          }

          for (uint32_t j = 0; j < key_node.sub_key_list_.size(); j++)
          {
            char *sub_val = key_node.sub_key_list_.at(j);

            RowKeySubType key_type = ROWKEY_MIN;
            if (sub_type_list ==NULL || sub_type_list->size() == 0)
            {
              pos = strlen(sub_val);
              strcpy(buf, sub_val);
              break;
            }
            key_type = sub_type_list->at(j);

            switch (key_type)
            {
              case ROWKEY_INT8_T:
              {
                int8_t val = static_cast<int8_t>(atoi(sub_val));
                serialization::encode_i8(buf, max_rowey_len, pos, val);
                break;
              }
              case ROWKEY_INT16_T:
              {
                int16_t val = static_cast<int16_t>(atoi(sub_val));
                serialization::encode_i16(buf, max_rowey_len, pos, val);
                break;
              }
              case ROWKEY_INT32_T:
              {
                int32_t val = static_cast<int32_t>(atoi(sub_val));
                serialization::encode_i32(buf, max_rowey_len, pos, val);
                break;
              }
              case ROWKEY_INT64_T:
              {
                int64_t val = strtoll(sub_val, NULL, 10);
                serialization::encode_i64(buf, max_rowey_len, pos, val);
                break;
              }
              case ROWKEY_FLOAT:
              {
                float val = atof(sub_val);
                serialization::encode_float(buf, max_rowey_len, pos, val);
                break;
              }
              case ROWKEY_DOUBLE:
              {
                double val = atof(sub_val);
                serialization::encode_double(buf, max_rowey_len, pos, val);
                break;
              }
              case ROWKEY_VARCHAR:
              {
                serialization::encode_str(buf, max_rowey_len, pos, sub_val, strlen(sub_val));
                break;
              }
              case ROWKEY_DATETIME:
              {
                ObDateTime val = strtoll(sub_val, NULL, 10);
                serialization::encode_datetime_type(buf, max_rowey_len, pos, val, false/* right? */);
                break;
              }
              case ROWKEY_PRECISEDATETIME:
              {
                ObPreciseDateTime val = strtoll(sub_val, NULL, 10);
                serialization::encode_precise_datetime_type(buf, max_rowey_len, pos, val, false/* right? */);
                break;
              }
              case ROWKEY_CEATETIME:
              {
                ObCreateTime val = strtoll(sub_val, NULL, 10);
                serialization::encode_createtime_type(buf, max_rowey_len, pos, val);
                break;
              }
              case ROWKEY_MODIFYTIME:
              {
                ObModifyTime val = strtoll(sub_val, NULL, 10);
                serialization::encode_modifytime_type(buf, max_rowey_len, pos, val);
                break;
              }
              default:
                fprintf(stderr, "Syntax error: wrong RowKey type!\n");
                ob_free(buf);
                return OB_ERROR;
                break;
            }
          }

          ObString new_val(max_rowey_len, pos, buf);
          switch (key_node.op_type_)
          {
            case OB_EQ_OP:
              if ((cit->border_flag_.is_min_value() ||
                   cit->border_flag_.inclusive_start() && cit->row_key_ <= new_val ||
                   !cit->border_flag_.inclusive_start() && cit->end_row_key_ < new_val) &&
                   (cit->border_flag_.is_max_value() ||
                   cit->border_flag_.inclusive_end() && cit->row_key_ >= new_val ||
                   !cit->border_flag_.inclusive_end() && cit->end_row_key_ > new_val))
              {
                if (cit->row_key_.ptr())
                  ob_free(cit->row_key_.ptr());
                cit->row_key_ = new_val;
                cit->is_equal_condition_= true;
                cit->border_flag_.set_inclusive_start();
                cit->border_flag_.set_inclusive_end();
                cit->border_flag_.unset_min_value();
                cit->border_flag_.unset_max_value();
              }
              else
              {
                ob_free(buf);
                ret = OB_ERROR;
              }
              break;
              case OB_LT_OP:
                if ((cit->border_flag_.is_min_value() || cit->row_key_ < new_val) &&
                    (cit->border_flag_.is_max_value() || cit->end_row_key_ >= new_val))
                {
                  if (cit->end_row_key_.ptr())
                    ob_free(cit->end_row_key_.ptr());
                  cit->end_row_key_ = new_val;
                  cit->border_flag_.unset_inclusive_end();
                  cit->border_flag_.unset_max_value();
                }
                else if(!cit->border_flag_.is_max_value() && cit->end_row_key_ < new_val)
                {
                  /* do nothing */
                }
                else
                {
                  ob_free(buf);
                  ret = OB_ERROR;
                }
                break;
              case OB_GT_OP:
                if ((cit->border_flag_.is_min_value() || cit->row_key_ <= new_val) &&
                    (cit->border_flag_.is_max_value() || cit->end_row_key_ > new_val))
                {
                  if (cit->row_key_.ptr())
                    ob_free(cit->row_key_.ptr());
                  cit->row_key_= new_val;
                  cit->border_flag_.unset_inclusive_start();
                  cit->border_flag_.unset_min_value();
                }
                else if (!cit->border_flag_.is_min_value() && cit->row_key_ > new_val)
                {
                  /* do nothing */
                }
                else
                {
                  ob_free(buf);
                  ret = OB_ERROR;
                }
                break;
              case OB_LE_OP:
                if ((cit->border_flag_.is_min_value() || 
                     (cit->border_flag_.inclusive_start() && cit->row_key_ <= new_val) ||
                     (!cit->border_flag_.inclusive_start() && cit->row_key_ < new_val)) &&
                    (cit->border_flag_.is_max_value() || cit->end_row_key_ > new_val))
                {
                  if (cit->end_row_key_.ptr())
                    ob_free(cit->end_row_key_.ptr());
                  cit->end_row_key_ = new_val;
                  cit->border_flag_.inclusive_end();
                  cit->border_flag_.set_inclusive_end();
                  cit->border_flag_.unset_max_value();
                }
                else if(!cit->border_flag_.is_max_value() && cit->end_row_key_ <= new_val)
                {
                  /* do nothing */
                }
                else
                {
                  ob_free(buf);
                  ret = OB_ERROR;
                }
                break;
              case OB_GE_OP:
                if ((cit->border_flag_.is_min_value() || cit->row_key_ < new_val) &&
                    (cit->border_flag_.is_max_value() || 
                    (cit->border_flag_.inclusive_end() && cit->end_row_key_ >= new_val) ||
                    (!cit->border_flag_.inclusive_end() && cit->end_row_key_ > new_val)))
                {
                  if (cit->row_key_.ptr())
                    ob_free(cit->row_key_.ptr());
                  cit->row_key_= new_val;
                  cit->border_flag_.inclusive_start();
                  cit->border_flag_.unset_inclusive_start();
                  cit->border_flag_.unset_min_value();
                }
                else if (!cit->border_flag_.is_min_value() && cit->row_key_ > new_val)
                {
                  /* do nothing */
                }
                else
                {
                  ob_free(buf);
                  ret = OB_ERROR;
                }
                break;

            default:
              ob_free(buf);
              ret = OB_ERROR;
              break;
          }
        }

/*
        if (!cit->is_equal_condition_ && 
            !(cit->border_flag_.is_min_value()) &&
            !(cit->border_flag_.is_max_value()) &&
            cit->row_key_.compare(cit->end_row_key_) > 0)
        {
          fprintf(stderr, "Wrong range for %s, from %s to %s\n", 
                  cit->column_name_, cit->row_key_.ptr(), cit->end_row_key_.ptr());
          return OB_ERROR;
        }
*/
      }

      return ret;
    }

    int32_t Stmt::get_decode_rowkey_string(const char *buf, const int32_t data_len, int64_t& pos, 
                                           const char *table_name, char *& outbuf, const int64_t out_len)
    {
      int32_t ret = OB_SUCCESS;
      memset(outbuf, 0, out_len);

      const vector<RowKeySubType> *sub_type_list = NULL;
      map<string, vector<RowKeySubType> >::const_iterator it = rowkey_map_.find(table_name);
      if (it != rowkey_map_.end()) 
      {
          sub_type_list = &(it->second);
      }

      if (sub_type_list == NULL || sub_type_list->size() == 0)
      {
        int64_t len = data_len + 1 < out_len ? data_len + 1 : out_len;
        snprintf(outbuf, len, "%s", buf);
        return OB_SUCCESS;
      }

      int64_t out_pos = 0;
      for (uint32_t i = 0; i < sub_type_list->size(); i++)
      {
        if (out_pos >= out_len)
        {
          fprintf(stderr, "Syntax error: RowKey is too long to output!\n");
        }
        
        if (i != 0)
          outbuf[out_pos++] = ',';
        
        RowKeySubType sub_type = sub_type_list->at(i);
        switch (sub_type)
        {
          case   ROWKEY_INT8_T:
          {
            int8_t val;
            ret = serialization::decode_i8(buf, data_len, pos, &val);
            snprintf(outbuf + out_pos, out_len - out_pos, "%d", val);
            out_pos = strlen(outbuf);
            break;
          }
          case ROWKEY_INT16_T:
          {
            int16_t val;
            ret = serialization::decode_i16(buf, data_len, pos, &val);
            snprintf(outbuf + out_pos, out_len - out_pos, "%d", val);
            out_pos = strlen(outbuf);
            break;
          }
          case ROWKEY_INT32_T:
          {
            int32_t val;
            ret = serialization::decode_i32(buf, data_len, pos, &val);
            snprintf(outbuf + out_pos, out_len - out_pos, "%d", val);
            out_pos = strlen(outbuf);
            break;
          }
          case ROWKEY_INT64_T:
          {
            int64_t val;
            ret = serialization::decode_i64(buf, data_len, pos, &val);
            snprintf(outbuf + out_pos, out_len - out_pos, "%ld", val);
            out_pos = strlen(outbuf);
            break;
          }
          case ROWKEY_FLOAT:
          {
            float val;
            ret = serialization::decode_float(buf, data_len, pos, &val);
            snprintf(outbuf + out_pos, out_len - out_pos, "%f", val);
            out_pos = strlen(outbuf);
            break;
          }
          case ROWKEY_DOUBLE:
          {
            double val;
            ret = serialization::decode_double(buf, data_len, pos, &val);
            snprintf(outbuf + out_pos, out_len - out_pos, "%f", val);
            out_pos = strlen(outbuf);
            break;
          }
          case ROWKEY_VARCHAR:
          {
            int32_t lenp = 0;
            snprintf(outbuf + out_pos, out_len - out_pos, "%s", serialization::decode_str(buf, data_len, 0, pos, lenp));
            out_pos = strlen(outbuf);
            break;
          }
          case ROWKEY_DATETIME:
          {
            ObDateTime val;
            bool  is_add = false;
            /* FIX ME: first_type */
            ret = serialization::decode_datetime_type(buf, data_len, 0, pos, val, is_add);
            snprintf(outbuf + out_pos, out_len - out_pos, "%ld", val);
            out_pos = strlen(outbuf);
            break;
          }
          case ROWKEY_PRECISEDATETIME:
          {
            ObPreciseDateTime val;
            bool  is_add = false;
            /* FIX ME: first_type */
            ret = serialization::decode_precise_datetime_type(buf, data_len, 0, pos, val, is_add);
            snprintf(outbuf + out_pos, out_len - out_pos, "%ld", val);
            out_pos = strlen(outbuf);
            break;
          }
          case ROWKEY_CEATETIME:
          {
            ObPreciseDateTime val;
            /* FIX ME: first_type */
            ret = serialization::decode_createtime_type(buf, data_len, 0, pos, val);
            snprintf(outbuf + out_pos, out_len - out_pos, "%ld", val);
            out_pos = strlen(outbuf);
            break;
          }
          case ROWKEY_MODIFYTIME:
          {
            ObPreciseDateTime val;
            /* FIX ME: first_type */
            ret = serialization::decode_modifytime_type(buf, data_len, 0, pos, val);
            snprintf(outbuf + out_pos, out_len - out_pos, "%ld", val);
            out_pos = strlen(outbuf);
            break;
          }
          default:
            fprintf(stderr, "Wrong RowKey sub-type!\n");
            break;
        }

        if (ret != OB_SUCCESS)
        {
          fprintf(stderr, "decode RowKey error!\n");
          return ret;
        }
      }
      return ret;
    }

  }
}

