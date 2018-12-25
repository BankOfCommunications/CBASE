#include <stdio.h>
#include "stmt.h"
#include "selectstmt.h"
#include "common_func.h"
#include "common/ob_malloc.h"
#include "common/ob_define.h"
#include "common/ob_scan_param.h"
    
namespace oceanbase
{ 
  namespace obsql
  {
    
    using namespace std;
    using namespace oceanbase::common;

    int32_t SelectStmt::query()
    {
      Stmt::query();

      return OB_SUCCESS;
    }
    
    int32_t SelectStmt::init(ObClientServerStub& rpc_stub)
    {
      Stmt::init(rpc_stub);

      return OB_SUCCESS;
    }
    
    int32_t SelectStmt::parse()
    {
      int32_t ret = OB_SUCCESS;
      char    *select_str = NULL;
      char    *from_str = NULL;
      char    *where_str = NULL;
      char    *order_str = NULL;
      char    *token = NULL;
      char    *query_str = NULL;
      char    *query_str_bak = NULL;

      query_str = duplicate_str(get_raw_query_string().ptr());
      query_str_bak = query_str;

      token = get_token(query_str);
      if (!token || strcasecmp(token, "select") != 0)
      {
        fprintf(stderr, "Not SELECT statement!\n");
        ob_free(query_str_bak);
        return OB_ERROR;
      }

      select_str = string_split_by_str(query_str, "from", false, true);
      if (!select_str)
      {
        fprintf(stderr, "Can not find key word FROM!\n");
        ob_free(query_str_bak);
        return OB_ERROR;
      }

      from_str = string_split_by_str(query_str, "where", false, true);
      if (!from_str)
      {
        from_str = string_split_by_str(query_str, "order", false, true);
        if (!from_str)
          from_str = query_str;
        else
          order_str = query_str;
      }
      else
      {
        where_str = string_split_by_str(query_str, "order", false, true);
        if (!where_str)
          where_str = query_str;
        else
          order_str = query_str;
      }

      ret = parse_from(from_str);
      if (ret != OB_SUCCESS)
      {
        fprintf(stderr, "Parse FROM clause erro!\n");
        ob_free(query_str_bak);
        return OB_ERROR;
      }

      ret = parse_select(select_str);
      if (ret != OB_SUCCESS)
      {
        fprintf(stderr, "Parse SELECT clause erro!\n");
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

      ret = parse_order(order_str);
      if (ret != OB_SUCCESS)
      {
        fprintf(stderr, "Parse ORDER clause erro!\n");
        ob_free(query_str_bak);
        return OB_ERROR;
      }
      
      ob_free(query_str_bak);
      return ret;
    }
    
    int32_t SelectStmt::resolve()
    {
      int32_t ret = OB_SUCCESS;
      
      ret = Stmt::resolve();
      if (ret != OB_SUCCESS)
      {
        fprintf(stderr, "Resolve FROM clause or WHERE clause error!\n");
        return OB_ERROR;
      }
      ret = resolve_select();
      if (ret != OB_SUCCESS)
      {
        fprintf(stderr, "Resolve SELECT clause error!\n");
        return OB_ERROR;
      }
      ret = resolve_order();
      if (ret != OB_SUCCESS)
      {
        fprintf(stderr, "Resolve ORDER clause error!\n");
        return OB_ERROR;
      }

      return ret;
    }

    int32_t SelectStmt::execute()
    {
      int32_t ret = OB_SUCCESS;
      int64_t start;
      int64_t end;

      elapsed_time_ = 0;

      vector<ConditionNode>& cnd_list = get_condition_list();
      vector<ConditionNode>::iterator cit = cnd_list.begin();
      for (; cit != cnd_list.end(); cit++)
      {
        if (cit->is_equal_condition_)
        {
          CharArena  allocator;
          ObGetParam input;
          char       *cell_info_buf = allocator.alloc(sizeof(ObCellInfo));
          ObCellInfo *cell_info = new (cell_info_buf) ObCellInfo();

          cell_info->row_key_ = cit->row_key_;
          ObVersionRange version_range;
          version_range.border_flag_.set_min_value();
          version_range.border_flag_.set_max_value();
          input.set_version_range(version_range);

          vector<ColumnNode>& col_list = get_column_list();
          vector<ColumnNode>::iterator colit = col_list.begin();
          for (; colit != col_list.end(); colit++)
          {
            ObString table_name(0, strlen(colit->table_name_), colit->table_name_);
            ObString column_name(0, strlen(colit->column_name_), colit->column_name_);
            //cell_info->table_id_ = colit->table_id_;
            cell_info->table_name_ = table_name;
            //cell_info->column_id_ = colit->column_id_;
            cell_info->column_name_ = column_name;
            input.add_cell(*cell_info);
          }
	  input.set_is_result_cached(true);

          start = tbsys::CTimeUtil::getTime();
          ret = get_rpc_stub().cs_get(input, scanner_);
	  //printf("version[%ld:%ld]\n", input.get_version_range().start_version_, input.get_version_range().end_version_);
          //printf("min[%s], max[%s]\n", input.get_version_range().border_flag_.is_min_value() ? "true": "false", 
	 //	input.get_version_range().border_flag_.is_max_value() ? "true" : "false");

	  end = tbsys::CTimeUtil::getTime();
        }
        else
        {
          ObScanParam input;
          ObRange     range;

          range.table_id_ = get_column_list().begin()->table_id_;

          range.start_key_.assign_ptr(cit->row_key_.ptr(), cit->row_key_.length());
          range.end_key_.assign_ptr(cit->end_row_key_.ptr(), cit->end_row_key_.length());
          range.border_flag_ = cit->border_flag_;

          vector<ColumnNode>& col_list = get_column_list();
          vector<ColumnNode>::iterator colit = col_list.begin();
          
          ObString table_name(0, strlen(colit->table_name_), colit->table_name_);
          input.set(OB_INVALID_ID, table_name, range);
          input.set_scan_size(2*1024*1024);
          input.set_is_result_cached(true);
          ObVersionRange version_range;
          version_range.border_flag_.set_min_value();
          version_range.border_flag_.set_max_value();
          input.set_version_range(version_range);

          //input.set_scan_direction(ObScanParam::FORWARD);
          //input.set_read_mode(ObScanParam::PREREAD);
          for (; colit != col_list.end(); colit++)
          {
            ObString column_name(0, strlen(colit->column_name_), colit->column_name_);
            //input.add_column(colit->column_id_);
            input.add_column(column_name);
          }

          start = tbsys::CTimeUtil::getTime();
          ret = get_rpc_stub().cs_scan(input, scanner_);
          end = tbsys::CTimeUtil::getTime();
        }

        elapsed_time_ += end - start;

        /* show results */
        if (cit == cnd_list.begin())
          output_head();
        output_body();
        fprintf(stderr, "***%ldns elapsed***\n", elapsed_time_);
      }

      return ret;
    }

    int32_t SelectStmt::output_head()
    {
      int32_t ret = OB_SUCCESS;

      fprintf(stderr, "\n");
      vector<ColumnNode>& col_list = get_column_list();
      vector<ColumnNode>::iterator cit = col_list.begin();

      fprintf(stderr, "RowKey          ");
      for(; cit != col_list.end(); cit++)
      {
        fprintf(stderr, "%-15s ", 
          ((cit->column_alias_)[0] != '\0') ? cit->column_alias_ : cit->column_name_);
      }
      fprintf(stderr, "\n");
      for(uint32_t i = 0; i <= col_list.size(); i++)
        fprintf(stderr, "---------------");

      return ret;
    }

    int32_t SelectStmt::output_body()
    {
      int32_t           ret = OB_SUCCESS;
      int32_t           row_count = 0;
      bool              is_row_changed = false;
      ObCellInfo        *cell_info = NULL;
      ObScannerIterator iter;

      for (iter = scanner_.begin(); iter != scanner_.end(); iter++)
      {
        iter.get_cell(&cell_info, &is_row_changed);
        /* zero record */
        if (cell_info->column_name_.length() == 0)
          break;  
        if (is_row_changed || iter == scanner_.begin())
        {
          fprintf(stderr, "\n");
/*
int64_t rowkey_num;
int64_t pos = 0;
serialization::decode_i64(cell_info->row_key_.ptr(), cell_info->row_key_.length(), pos, &rowkey_num);
*/
          //fprintf(stderr, "%.*s", cell_info->row_key_.length(), cell_info->row_key_.ptr());
          char rowkey_str[OB_MAX_ROW_NUMBER_PER_QUERY];
          char *rowkey_ptr = rowkey_str;
          int64_t pos = 0;
          char table_name[OB_MAX_TABLE_NAME_LENGTH];
          int64_t len = cell_info->table_name_.length();
          strncpy(table_name, cell_info->table_name_.ptr(), len);
          if (len == OB_MAX_TABLE_NAME_LENGTH)
            table_name[len - 1] = '\0';
          else
            table_name[len] = '\0';
          get_decode_rowkey_string(cell_info->row_key_.ptr(), cell_info->row_key_.length(), pos, 
                                   table_name, rowkey_ptr, OB_MAX_ROW_NUMBER_PER_QUERY);
          //get_decode_rowkey_string(cell_info->row_key_.ptr(), cell_info->row_key_.length(), pos, 
          //                         cell_info->table_name_.ptr(), rowkey_ptr, OB_MAX_ROW_NUMBER_PER_QUERY);
          fprintf(stderr, "%-15s ", rowkey_ptr);
          row_count++;
        }
//fprintf(stderr, "is_row_changed=%d, column_name=%.*s\n", is_row_changed, 
//          cell_info->column_name_.length(), cell_info->column_name_.ptr());
        switch (cell_info->value_.get_type())
        {
          case ObNullType:
          {
            fprintf(stderr, "???            ");
            break;
          }
          case ObIntType:
          {
            int64_t value;
            cell_info->value_.get_int(value);
            fprintf(stderr, "%-15ld ", value);
            break;
          }
          case ObExtendType:
          {
            fprintf(stderr, "%-15ld ", cell_info->value_.get_ext());
            break;
          }
          case ObFloatType:
          {
            float value;
            cell_info->value_.get_float(value);
            fprintf(stderr, "%-15f ", value);
            break;
          }
          case ObDoubleType:
          {
            double value;
            cell_info->value_.get_double(value);
            fprintf(stderr, "%-15f ", value);
            break;
          }
          case ObDateTimeType:
          {
            ObDateTime value;
            cell_info->value_.get_datetime(value);
            fprintf(stderr, "%-15ld ", value);
            break;
          }
          case ObPreciseDateTimeType:
          {
            ObPreciseDateTime value;
            cell_info->value_.get_precise_datetime(value);
            fprintf(stderr, "%-15ld ", value);
            break;
          }
          case ObCreateTimeType:
          {
            ObCreateTime value;
            cell_info->value_.get_createtime(value);
            fprintf(stderr, "%-15ld ", value);
            break;
          }
          case ObModifyTimeType:
          {
            ObModifyTime value;
            cell_info->value_.get_modifytime(value);
            fprintf(stderr, "%-15ld ", value);
            break;
          }
          case ObVarcharType:
          {
            ObString value;
            cell_info->value_.get_varchar(value);
            fprintf(stderr, "%-15.*s ", value.length(), value.ptr());
            break;
          }
          default:
            break;
        }
      }

      fprintf(stderr, "\n\n***%d rows returned***\n", row_count);

      return ret;
    }

    int32_t SelectStmt::output()
    {
      int32_t ret = OB_SUCCESS;

      return ret;
    }

    int32_t SelectStmt::parse_select(char *select_str)
    {
      int32_t             ret = OB_SUCCESS;
      char                *ptr = select_str;
      char                *column_token = NULL;
      bool                do_flag = true;

      ptr = trim(ptr);
      if (strlen(ptr) == 0)
      {
        fprintf(stderr, "Syntax error: there is no target columns in select statement!\n");
        return OB_ERROR;
      }

      while (do_flag)
      {
        column_token = string_split_by_ch(ptr, ',');
        if (!column_token)
        {
          do_flag = false;
          column_token = ptr;
        }

        ret = add_column(column_token);
        if (ret != OB_SUCCESS)
        {
          fprintf(stderr, "Syntax error around \"%s\"\n", column_token);
          return ret;
        }
      }

      return ret;
    }
    
    int32_t SelectStmt::add_column(char *column_str)
    {
      int32_t    ret = OB_SUCCESS;
      ColumnNode column_node;
      char       *token = NULL;
      char       *next_token = NULL;
      char       *tail_token = NULL;

      memset(&column_node, 0, sizeof(column_node));
      if (!(column_str = trim(column_str)))
        return OB_ERROR;

      if ((token = string_split_by_ch(column_str, '.')) != NULL)
      {
        strcpy(column_node.table_name_, token);
      }

      token = get_token(column_str);
      next_token = get_token(column_str);
      tail_token = get_token(column_str);
      if (!token)
      {
        fprintf(stderr, "Syntax error after \"%s\"\n", column_node.table_name_);
        return OB_ERROR;
      }
      else
      {
        strcpy(column_node.column_name_, token);
      }

      if (!next_token)
      {
        get_column_list().push_back(column_node);
        return ret;
      }
      if (strcasecmp(next_token, "as") == 0)
        next_token = tail_token;
      else if (tail_token)
      {
        fprintf(stderr, "Syntax error around \"%s\"\n", tail_token);
        return OB_ERROR;
      }
        
      if (!next_token)
      {
        fprintf(stderr, "Syntax error: expect alias name after \"as\"\n");
        return OB_ERROR;
      }
      else
        strcpy(column_node.column_alias_, next_token);

      if (strlen(trim(column_str)) != 0)
      {
        fprintf(stderr, "Syntax error around \"%s\"\n", column_str);
        return OB_ERROR;
      }
      
      column_node.position_ = get_column_list().size() + 1;
      get_column_list().push_back(column_node);

      return ret;
    }

    int32_t SelectStmt::add_column(ColumnNode& column_node)
    {
      vector<ColumnNode>& column_list = get_column_list();
      column_list.push_back(column_node);

      return OB_SUCCESS;
    }
    
    int32_t SelectStmt::parse_from(char *from_str)
    {
      return Stmt::parse_from(from_str);
    }
    
    int32_t SelectStmt::parse_where(char *where_str)
    {
      return Stmt::parse_where(where_str);
    }
    
    int32_t SelectStmt::parse_order(char *order_str)
    {
      int32_t ret =  OB_SUCCESS;
      char    *front_str = NULL;
      bool    do_flag = true;

      order_str = trim(order_str);
      if (!order_str || strlen(order_str) == 0)
        return ret;

      while(do_flag)
      {
        OrderNode order_node;
        int32_t   pos;
        char      *token;

        memset(&order_node, 0, sizeof(order_node));
        front_str = string_split_by_ch(order_str, ',');
        if (!front_str)
        {
          do_flag = false;
          front_str = order_str;
        }

        /* order column */
        token = get_token(front_str);
        if (token == NULL)
        {
          fprintf(stderr, "Syntax error before \",\"\n");
          return OB_ERROR;
        }
        if ((pos = atoi(token)) == 0)
          memcpy(order_node.column_name_, token, sizeof(token));
        else
          order_node.rel_position_ = pos;

        /* ASC or DESC */
        token = get_token(front_str);
        if (!token || strcasecmp(token, "asc") == 0)
          order_node.order = ObScanParam::ASC;
        else if(strcasecmp(token, "desc") == 0)
          order_node.order = ObScanParam::DESC;
        else
        {
          fprintf(stderr, "Unknow token \"%s\"\n", token);
          return OB_ERROR;
        }

        if (!(token = get_token(front_str)))
        {
          fprintf(stderr, "Error token \"%s\"\n", token);
          return OB_ERROR;
        }
        
        order_list_.push_back(order_node);
      }

      return ret;
    }

    int32_t SelectStmt::resolve_select()
    {
      int32_t ret = OB_SUCCESS;
      
      vector<ColumnNode>& column_list = get_column_list();
      ObSchemaManagerV2& schema_mgr = get_schema_manager();
      
      vector<ColumnNode>::iterator cit = column_list.begin();
      for (; cit != column_list.end(); cit++)
      {
        if (strlen(cit->table_name_) != 0)
        {
          const ObTableSchema *table_schema = schema_mgr.get_table_schema(cit->table_name_);
          if (table_schema == NULL)
          {
            const char *name = NULL;
            name = get_original_tblname(cit->table_name_);
            table_schema = schema_mgr.get_table_schema(name);
            if (table_schema == NULL)
            {
              fprintf(stderr, "Unknown table \"%s\"\n", cit->table_name_);
              return OB_ERROR;
            }
            else
              strcpy(cit->table_name_, name);
          }
          cit->table_id_ = table_schema->get_table_id();

          if (strcmp(cit->column_name_, "*") == 0)
            expand_star(cit, cit->table_id_);
          else
          {
            const ObColumnSchemaV2 *col_schema = schema_mgr.get_column_schema(cit->table_name_, cit->column_name_);
            if (col_schema == NULL)
            {
              fprintf(stderr, "Unknown column \"%s\" of table \"%s\"\n", cit->column_name_, cit->table_name_);
              return OB_ERROR;
            }
            else
            {
              cit->column_id_ = col_schema->get_id();
            }
          }
        }
        else
        {
          vector<TableNode>& table_list = get_table_list();
          vector<TableNode>::iterator tit = table_list.begin();

          if (strcmp(cit->column_name_, "*") == 0)
          {
            if (table_list.size() != 1)
            {
              fprintf(stderr, "Syntax error: Ambiguous definition of \"*\"");
              return OB_ERROR;
            }
            expand_star(cit, tit->table_id_);
          }
          else
          {
            for(; tit != table_list.end(); tit++)
            {
              const ObColumnSchemaV2 *col_schema = schema_mgr.get_column_schema(tit->table_name_, cit->column_name_);
              if (col_schema != NULL)
              {
                cit->table_id_ = tit->table_id_;
                strcpy(cit->table_name_, tit->table_name_);
                cit->column_id_ = col_schema->get_id();
                break;
              }
            }
            if (tit == table_list.end())
            {
              fprintf(stderr, "Syntax error: cannot find table with column \"%s\"\n", cit->column_name_);
              return OB_ERROR;
            }
          }
        }
      }

      return ret;
    }

    int32_t SelectStmt::expand_star(vector<ColumnNode>::iterator& it, uint64_t table_id)
    {
      ObSchemaManagerV2&    schema_mgr = get_schema_manager();
      vector<ColumnNode>& col_list = get_column_list();
      vector<ColumnNode>  temp_list;
      int32_t             num = col_list.end() - it;

      while (num > 1)
      {
        temp_list.push_back(col_list.back());
        col_list.pop_back();
      }
      col_list.pop_back();
      
      ColumnNode column;
      const ObTableSchema   *schema_ptr = schema_mgr.get_table_schema(table_id);

      memset(&column, 0, sizeof(column));
      column.table_id_ = table_id;
      strcpy(column.table_name_, schema_ptr->get_table_name());

      int32_t         column_size;
      const ObColumnSchemaV2  *columns = schema_mgr.get_table_schema(table_id, column_size);
      for (int32_t i = 0; i < column_size; i++)
      {
        column.column_id_ = columns[i].get_id();
        strcpy(column.column_name_, columns[i].get_name());
        column.position_ = col_list.size() + 1;
        col_list.push_back(column);
      }
      it = col_list.end() - 1;
      
      while (temp_list.size() > 0)
      {
        ColumnNode node = temp_list.back();
        node.position_ = col_list.size() + 1;
        col_list.push_back(node);
        temp_list.pop_back();
      }

      return OB_SUCCESS;
    }

    int32_t SelectStmt::resolve_order()
    {
      int32_t ret = OB_SUCCESS;

      vector<ColumnNode>& column_list = get_column_list();
      vector<OrderNode>::iterator oit = order_list_.begin();
      for(; oit != order_list_.end(); oit++)
      {
        if (oit->rel_position_ > 0)
        {
          if (oit->rel_position_ > static_cast<int32_t>(column_list.size()))
          {
            fprintf(stderr, "Syntax error: wrong column position \"%d\"\n", oit->rel_position_);
            return OB_ERROR;
          }

          ColumnNode& col = column_list.at(oit->rel_position_);
          memcpy(oit->column_name_, col.column_name_, sizeof(col.column_name_));
          oit->column_id_ = col.column_id_;
          oit->table_id_ = col.table_id_;
          memcpy(oit->table_name_, col.table_name_, sizeof(col.table_name_));
        }
        else
        {
          /* order by columns must appear in select column list */
          vector<ColumnNode>::iterator cit = column_list.begin();
          for (; cit != column_list.end(); cit++)
          {
            if (strcmp(oit->column_name_, cit->column_name_) == 0 ||
                strcmp(oit->column_name_, cit->column_alias_) == 0)
            {
              oit->column_id_ = cit->column_id_;
              oit->table_id_ = cit->table_id_;
              memcpy(oit->table_name_, cit->table_name_, sizeof(cit->table_name_));
              break;
            }
          }

          if (oit->column_id_ <= 0 || oit->column_id_ != OB_INVALID_ID)
          {
            fprintf(stderr, "Syntax error: Unkown column \"%s\"\n", oit->table_name_);
            return OB_ERROR;
          }
        }
      }

      return ret;
    }

    char* SelectStmt::get_original_tblname(const char *alias)
    {
      char* ptr = NULL;
      vector<TableNode>& table_list = get_table_list();
      vector<TableNode>::iterator tit = table_list.begin();

      for (; tit != table_list.end(); tit++)
      {
        if (strcasecmp(alias, tit->table_alias_) == 0)
        {
          if (ptr)
          {
            fprintf(stderr, "Syntax error: duplicate alias name \"%s\"\n", alias);
            return NULL;
          }
          else
            ptr = tit->table_name_;
        }
      }

      return ptr;      
    }

  }
}



