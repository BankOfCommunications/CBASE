#include <stdio.h>
#include "updatestmt.h"
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

    UpdateStmt::~UpdateStmt()
    {
      vector<SetNode>::iterator sit;
      for (sit = set_list_.begin(); sit != set_list_.end(); sit++)
      {
        if (sit->raw_value_)
        {
          ob_free(sit->raw_value_);
          sit->raw_value_ = NULL;
        }
      }
    }

    int32_t UpdateStmt::query()
    {
      Stmt::query();

      return OB_SUCCESS;
    }
    
    int32_t UpdateStmt::init(ObClientServerStub& rpc_stub)
    {
      Stmt::init(rpc_stub);

      return OB_SUCCESS;
    }
    
    int32_t UpdateStmt::parse()
    {
      int32_t ret = OB_SUCCESS;
      char    *table_str = NULL;
      char    *set_str = NULL;
      char    *where_str = NULL;
      char    *token = NULL;
      char    *query_str = duplicate_str(get_raw_query_string().ptr());
      char    *query_str_bak = query_str;
    
      token = get_token(query_str);
      if (!token || strcasecmp(token, "update") != 0)
      {
        fprintf(stderr, "Not UPDATE statement!\n");
        return OB_ERROR;
      }

      table_str = string_split_by_str(query_str, "set", false, true);
      if (!token)
      {
        fprintf(stderr, "Syntax error: can not find SET clause!\n");
        return OB_ERROR;
      }

      set_str = string_split_by_str(query_str, "where", false, true);
      if (!set_str)
      {
        fprintf(stderr, "Syntax error: can not find WHERE clause!\n");
        return OB_ERROR;
      }
      where_str = query_str;

      ret = parse_from(table_str);
      if (ret != OB_SUCCESS)
      {
        fprintf(stderr, "Parse FROM clause erro!\n");
        return OB_ERROR;
      }

      ret = parse_set(set_str);
      if (ret != OB_SUCCESS)
      {
        fprintf(stderr, "Parse FROM clause erro!\n");
        return OB_ERROR;
      }

      ret = parse_where(where_str);
      if (ret != OB_SUCCESS)
      {
        fprintf(stderr, "Parse WHERE clause erro!\n");
        return OB_ERROR;
      }

      ob_free(query_str_bak);
      
      return ret;
    }
    
    int32_t UpdateStmt::resolve()
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
        fprintf(stderr, "DELETE statement only supports equal RowKey condition in WHERE clause!\n");
        return OB_ERROR;
      }

      vector<SetNode>::iterator sit;
      ObSchemaManagerV2& obschema_mgr = get_schema_manager();
      uint64_t table_id = table_list.begin()->table_id_;
      const ObTableSchema *obschema = obschema_mgr.get_table_schema(table_id);
      for (sit = set_list_.begin(); sit != set_list_.end(); sit++)
      {
        const ObColumnSchemaV2 *obcolschema = obschema_mgr.get_column_schema(obschema->get_table_name(), sit->column_name_);
        if (!obcolschema)
        {
          fprintf(stderr, "Syntax error: unknown column %s\n", sit->column_name_);
          return OB_ERROR;
        }
        sit->column_id_ = obcolschema->get_id();

        ObString varchar;
        switch (obcolschema->get_type())
        {
          case ObNullType:
            sit->object_.set_null();
            break;
          case ObIntType:
            sit->object_.set_int(atoi(sit->raw_value_));
            break;
          case ObFloatType:
            sit->object_.set_float(atof(sit->raw_value_));
            break;
          case ObDoubleType:
            sit->object_.set_double(atof(sit->raw_value_));
            break;
          case ObDateTimeType:
            sit->object_.set_datetime(atoi64(sit->raw_value_));
            break;
          case ObPreciseDateTimeType:
            sit->object_.set_precise_datetime(atoi64(sit->raw_value_));
            break;
          case ObVarcharType:
            varchar.assign_ptr(sit->raw_value_, strlen(sit->raw_value_)); 
            sit->object_.set_varchar(varchar);
            break;
          case ObCreateTimeType:
            sit->object_.set_createtime(atoi64(sit->raw_value_));
            break;
          case ObModifyTimeType:
            sit->object_.set_modifytime(atoi64(sit->raw_value_));
            break;
          case ObExtendType:
            sit->object_.set_ext(atoi(sit->raw_value_));
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
    
    int32_t UpdateStmt::execute()
    {
      int32_t   ret = OB_SUCCESS;
      int64_t   start;
      int64_t   end;
      ObMutator mutator;

      char      *table_name_ptr = get_table_list().begin()->table_name_;
      ObString  table_name;
      table_name.assign_ptr(table_name_ptr, strlen(table_name_ptr));

      ObString& row_key = get_condition_list().begin()->row_key_;
/*
int64_t rowkey_num;
int64_t rowkey_len = 0;
rowkey_num = atoi64(row_key.ptr());
char key_buf[256];
serialization::encode_i64(key_buf, 256, rowkey_len, rowkey_num);
ObString encode_row_key(0, rowkey_len, key_buf);
*/
      vector<SetNode>::iterator sit;
      for (sit = set_list_.begin(); sit != set_list_.end(); sit++)
      {
        ObString  column_name;
        column_name.assign_ptr(sit->column_name_, strlen(sit->column_name_));
        // ret = mutator.update(table_name, encode_row_key, column_name, sit->object_);
        ret = mutator.update(table_name, row_key, column_name, sit->object_);
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

    int32_t UpdateStmt::output()
    {
      int32_t ret = OB_SUCCESS;

      fprintf(stderr, "UPDATE runs successfully\n");
      fprintf(stderr, "***%ldns elapsed***\n", elapsed_time_);

      return ret;
    }

    int32_t UpdateStmt::parse_set(char *set_str)
    {
      int32_t  ret = OB_SUCCESS;
      char     *front_str;
      bool     do_flag = true;
      SetNode  setNode;

      while (do_flag) 
      {
        char *column_str;
        
        front_str = string_split_by_ch(set_str, ',');
        if (!front_str)
        {
          do_flag = false;
          front_str = set_str;
        }

        column_str = string_split_by_ch(front_str, '=');
        if(!column_str || strlen(front_str) == 0)
        {
          fprintf(stderr, "Syntax error around %s = %s\n", column_str, front_str);
          return OB_ERROR;
        }

        memset(&setNode, 0, sizeof(setNode));
        memcpy(setNode.column_name_, column_str, strlen(column_str));
        setNode.raw_value_ = duplicate_str(front_str);
        set_list_.push_back(setNode);
      }

      return ret;
    }

    int32_t UpdateStmt::parse_from(char *from_str)
    {
      return Stmt::parse_from(from_str);
    }
    
    int32_t UpdateStmt::parse_where(char *where_str)
    {
      return Stmt::parse_where(where_str);
    }
    

   

  }
}





