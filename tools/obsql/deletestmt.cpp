#include <stdio.h>
#include "deletestmt.h"
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

    int32_t DeleteStmt::query()
    {
      Stmt::query();

      return OB_SUCCESS;
    }
    
    int32_t DeleteStmt::init(ObClientServerStub& rpc_stub)
    {
      Stmt::init(rpc_stub);

      return OB_SUCCESS;
    }
    
    int32_t DeleteStmt::parse()
    {
      int32_t ret = OB_SUCCESS;
      char    *from_str = NULL;
      char    *where_str = NULL;
      char    *token = NULL;
      char    *query_str = NULL;
      char    *query_str_bak = NULL;

      query_str = duplicate_str(get_raw_query_string().ptr());
      query_str_bak = query_str;

      token = get_token(query_str);
      if (!token || strcasecmp(token, "delete") != 0)
      {
        fprintf(stderr, "Not DELETE statement!\n");
        ob_free(query_str_bak);
        return OB_ERROR;
      }

      token = get_token(query_str);
      if (token && strcmp(token, "*") == 0)
        token = get_token(query_str);
      if (!token || strcasecmp(token, "from") != 0)
      {
        fprintf(stderr, "Syntax error after DELETE clause!\n");
        ob_free(query_str_bak);
        return OB_ERROR;
      }

      from_str = string_split_by_str(query_str, "where", false, true);
      if (!from_str)
      {
        fprintf(stderr, "Can not find WHERE clause!\n");
        ob_free(query_str_bak);
        return OB_ERROR;
      }
      where_str = query_str;

      ret = parse_from(from_str);
      if (ret != OB_SUCCESS)
      {
        fprintf(stderr, "Parse FROM clause erro!\n");
        ob_free(query_str_bak);
        return OB_ERROR;
      }

      ret = parse_where(where_str);
      if (ret != OB_SUCCESS)
      {
        fprintf(stderr, "Parse WHERE clause erro!\n");
        ob_free(query_str_bak);
        return OB_ERROR;
      }
      
      ob_free(query_str_bak);
      return ret;
    }
    
    int32_t DeleteStmt::resolve()
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
      
      return ret;
    }
    int32_t DeleteStmt::execute()
    {
      int32_t   ret = OB_SUCCESS;
      int64_t   start;
      int64_t   end;
      ObMutator mutator;

      char      *table_name_ptr = get_table_list().begin()->table_name_;
      ObString  table_name(OB_MAX_COLUMN_NAME_LENGTH, strlen(table_name_ptr), table_name_ptr);

/*
int64_t rowkey_num;
int64_t rowkey_len = 0;
rowkey_num = atoi64(get_condition_list().begin()->row_key_.ptr());
char key_buf[256];
serialization::encode_i64(key_buf, 256, rowkey_len, rowkey_num);
ObString encode_row_key(0, rowkey_len, key_buf);
ret = mutator.del_row(table_name, encode_row_key);
*/

      ret = mutator.del_row(table_name, get_condition_list().begin()->row_key_);
      start = tbsys::CTimeUtil::getTime();
      ret = get_rpc_stub().ups_apply(mutator);
      end = tbsys::CTimeUtil::getTime();
      elapsed_time_ = end - start;

      return ret;
    }

    int32_t DeleteStmt::output()
    {
      int32_t ret = OB_SUCCESS;

      fprintf(stderr, "DELETE runs successfully\n");
      fprintf(stderr, "***%ldns elapsed***\n", elapsed_time_);

      return ret;
    }
       
    int32_t DeleteStmt::parse_from(char *from_str)
    {
      return Stmt::parse_from(from_str);
    }
    
    int32_t DeleteStmt::parse_where(char *where_str)
    {
      return Stmt::parse_where(where_str);
    }
    

   

  }
}




