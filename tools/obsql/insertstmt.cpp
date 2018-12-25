#include <stdio.h>
#include "stmt.h"
#include "insertstmt.h"
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

    InsertStmt::~InsertStmt()
    {
      vector<ValueNode>::iterator vit;
      for (vit = value_list_.begin(); vit != value_list_.end(); vit++)
      {
        if (vit->raw_value_)
        {
          ob_free(vit->raw_value_);
          vit->raw_value_ = NULL;
        }
      }
    }

    int32_t InsertStmt::query()
    {
      Stmt::query();

      return OB_SUCCESS;
    }
    
    int32_t InsertStmt::init(ObClientServerStub& rpc_stub)
    {
      Stmt::init(rpc_stub);

      return OB_SUCCESS;
    }
    
    int32_t InsertStmt::parse()
    {
      int32_t ret = OB_SUCCESS;
      char    *table_str = NULL;
      char    *into_str = NULL;
      char    *value_str = NULL;
      char    *token = NULL;
      char    *query_str = duplicate_str(get_raw_query_string().ptr());
      char    *query_str_bak = query_str;
    
      token = get_token(query_str);
      if (!token || strcasecmp(token, "insert") != 0)
      {
        fprintf(stderr, "Not INSERT statement!\n");
        return OB_ERROR;
      }
      token = get_token(query_str);
      if (!token || strcasecmp(token, "into") != 0)
      {
        fprintf(stderr, "Expect INTO key word!\n");
        return OB_ERROR;
      }

      into_str = string_split_by_str(query_str, "values", false, false);
      table_str = string_split_by_ch(into_str, '(');
      if (!table_str)
      {
        table_str = into_str;
        into_str = NULL;
      }
      else
      {
        int32_t len = strlen(into_str);
        if ( len == 0 || into_str[len - 1] != ')')
        {
          fprintf(stderr, "Expect an ')' after %s!\n", into_str);
          return OB_ERROR;
        }
        else
        {
          into_str[len - 1] = '\0';
        }
      }

      value_str = string_split_by_str(query_str, "where", false, true);
      if (!value_str)
      {
        fprintf(stderr, "Syntax error: can not find WHERE clause!\n");
        return OB_ERROR;
      }

      ret = parse_from(table_str);
      if (ret != OB_SUCCESS)
      {
        fprintf(stderr, "Parse Table error!\n");
        return OB_ERROR;
      }

      ret = parse_into(into_str);
      if (ret != OB_SUCCESS)
      {
        fprintf(stderr, "Parse into clause error!\n");
        return OB_ERROR;
      }

      ret = parse_values(value_str);
      if (ret != OB_SUCCESS)
      {
        fprintf(stderr, "Parse FROM clause error!\n");
        return OB_ERROR;
      }

      ret = parse_where(query_str);
      if (ret != OB_SUCCESS)
      {
        fprintf(stderr, "Parse WHERE clause error!\n");
        return OB_ERROR;
      }

      ob_free(query_str_bak);
      
      return ret;
    }
    
    int32_t InsertStmt::resolve()
    {
      int32_t ret = OB_SUCCESS;
      
      ret = Stmt::resolve();
      if (ret != OB_SUCCESS)
      {
        fprintf(stderr, "Resolve FROM clause or WHERE clause error!\n");
        return OB_ERROR;
      }

      vector<TableNode>& table_list = get_table_list();
      if (table_list.size() != 1)
      {
        fprintf(stderr, "DELETE statement only supports single table in FROM clause!\n");
        return OB_ERROR;
      }

      vector<ConditionNode>& cnd_list = get_condition_list();
      if (cnd_list.size() != 1)
      {
        fprintf(stderr, "DELETE statement only supports single equal RowKey condition in WHERE clause!\n");
        return OB_ERROR;
      }
      if (!(cnd_list.begin()->is_equal_condition_))
      {
        fprintf(stderr, "INSERT statement only supports equal RowKey condition in WHERE clause!\n");
        return OB_ERROR;
      }

      /* resolve columns */
      vector<ColumnNode>& column_list = get_column_list();
      vector<ColumnNode>::iterator cit;
      ObSchemaManagerV2& obschema_mgr = get_schema_manager();
      uint64_t table_id = table_list.begin()->table_id_;
      const ObTableSchema *obschema = obschema_mgr.get_table_schema(table_id);
      if (column_list.size() == 0)
      {
        ColumnNode      col_node;
        int32_t         column_size;
        const ObColumnSchemaV2  *columns = obschema_mgr.get_table_schema(table_id,column_size);

        for (int32_t i = 0; i < column_size; i++)
        {
          memset(&col_node, 0, sizeof(col_node));
          strcpy(col_node.column_name_, columns[i].get_name());
          col_node.column_id_ = columns[i].get_id();
          col_node.table_id_ = table_id;
          strcpy(col_node.table_name_, obschema->get_table_name());
          col_node.position_ = column_list.size() + 1;
          column_list.push_back(col_node);

        }
      }
      else
      {
        for (cit = column_list.begin(); cit != column_list.end(); cit++)
        {
          const char *table_name = obschema->get_table_name();
          const ObColumnSchemaV2  *col_schema = obschema_mgr.get_column_schema(table_name, cit->column_name_);
          if (col_schema == NULL)
          {
            fprintf(stderr, "Unknown column %s!\n", cit->column_name_);
            return OB_ERROR;
          }
          else
          {
            cit->table_id_ = table_id;
            strcpy(cit->table_name_, table_name);
            cit->column_id_ = col_schema->get_id();
          }
        }
      }

      /* resolve values */
      if (column_list.size() != value_list_.size())
      {
        fprintf(stderr, "Syntax error: numbers of columns and values are not match!\n");
        return OB_ERROR;
      }
      cit = column_list.begin();
      vector<ValueNode>::iterator vit = value_list_.begin();
      for (; cit != column_list.end() && vit != value_list_.end(); cit++, vit++)
      {
        ObString varchar;
        switch (obschema_mgr.get_column_schema(table_id, cit->column_id_)->get_type())
        {
          case ObNullType:
            vit->object_.set_null();
            break;
          case ObIntType:
            vit->object_.set_int(atoi(vit->raw_value_));
            break;
          case ObFloatType:
            vit->object_.set_float(atof(vit->raw_value_));
            break;
          case ObDoubleType:
            vit->object_.set_double(atof(vit->raw_value_));
            break;
          case ObDateTimeType:
            vit->object_.set_datetime(atoi64(vit->raw_value_));
            break;
          case ObPreciseDateTimeType:
            vit->object_.set_precise_datetime(atoi64(vit->raw_value_));
            break;
          case ObVarcharType:
            varchar.assign_ptr(vit->raw_value_, strlen(vit->raw_value_)); 
            vit->object_.set_varchar(varchar);
            break;
          case ObCreateTimeType:
            vit->object_.set_createtime(atoi64(vit->raw_value_));
            break;
          case ObModifyTimeType:
            vit->object_.set_modifytime(atoi64(vit->raw_value_));
            break;
          case ObExtendType:
            vit->object_.set_ext(atoi(vit->raw_value_));
            break;
          case ObSeqType:
          case ObMaxType:
          case ObMinType:
            break;
          default:
            fprintf(stderr, "unknown object type!\n");
          break;
        }
      }
      
      return ret;
    }
    
    int32_t InsertStmt::execute()
    {
      int32_t   ret = OB_SUCCESS;
      int64_t   start;
      int64_t   end;
      ObMutator mutator;

      char      *table_name_ptr = get_table_list().begin()->table_name_;
      ObString  table_name;
      table_name.assign_ptr(table_name_ptr, strlen(table_name_ptr));

      ObString& row_key = get_condition_list().begin()->row_key_;
      vector<ColumnNode>::iterator cit = get_column_list().begin();
      vector<ValueNode>::iterator vit = value_list_.begin();
      for (; cit != get_column_list().end() && vit != value_list_.end(); cit++, vit++)
      {
        ObString  column_name;
        column_name.assign_ptr(cit->column_name_, strlen(cit->column_name_));

/*
int64_t rowkey_num;
int64_t rowkey_len = 0;
rowkey_num = atoi64(row_key.ptr());
char key_buf[256];
serialization::encode_i64(key_buf, 256, rowkey_len, rowkey_num);
ObString encode_row_key(0, rowkey_len, key_buf);
ret = mutator.insert(table_name, encode_row_key, column_name, vit->object_);
*/
  
        ret = mutator.insert(table_name, row_key, column_name, vit->object_);
        if (ret != OB_SUCCESS)
        {
          fprintf(stderr, "fail to add mutator!\n");
          return OB_ERROR;
        }
      }
      start = tbsys::CTimeUtil::getTime();
      ret = get_rpc_stub().ups_apply(mutator);
      end = tbsys::CTimeUtil::getTime();
      elapsed_time_ = end - start;

      return ret;
    }

    int32_t InsertStmt::output()
    {
      int32_t ret = OB_SUCCESS;

      fprintf(stderr, "INSERT runs successfully\n");
      fprintf(stderr, "***%ldns elapsed***\n", elapsed_time_);

      return ret;
    }

    int32_t InsertStmt::parse_into(char *into_str)
    {
      int32_t  ret = OB_SUCCESS;
      char     *front_str;
      bool     do_flag = true;

      if (!into_str)
        return ret;

      vector<ColumnNode>& column_list = get_column_list();
      while (do_flag)
      {
        ColumnNode col_node;
        memset(&col_node, 0, sizeof(col_node));
        
        front_str = string_split_by_ch(into_str, ',');
        if (!front_str)
        {
          do_flag = false;
          front_str = into_str;
        }
        memcpy(col_node.column_name_, front_str, strlen(front_str));
        col_node.position_ = column_list.size() + 1;
        column_list.push_back(col_node);
      }

      return ret;
    }

    int32_t InsertStmt::parse_values(char *value_str)
    {
      int32_t   ret = OB_SUCCESS;
      char      *front_str;
      bool      do_flag = true;
      int32_t   len = strlen(value_str);
      ValueNode value_node;

      if (len == 0)
      {
        fprintf(stderr, "Syntax error: empty value!\n");
        return OB_ERROR;
      }

      /* kick off the braces */
      while (value_str[0] == '(' && value_str[len - 1] == ')')
      {
        value_str[len - 1] = '\0';
        len -=2;
        value_str++;
      }

      while (do_flag)
      {
        front_str = string_split_by_ch(value_str, ',');
        if (!front_str)
        {
          do_flag = false;
          front_str = value_str;
        }

        value_node.raw_value_ = duplicate_str(front_str);
        value_list_.push_back(value_node);
      }

      return ret;
    }

    int32_t InsertStmt::parse_from(char *from_str)
    {
      return Stmt::parse_from(from_str);
    }
    
    int32_t InsertStmt::parse_where(char *where_str)
    {
      return Stmt::parse_where(where_str);
    }
    

   

  }
}






