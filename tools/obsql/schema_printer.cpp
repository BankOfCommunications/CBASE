#include "schema_printer.h"
#include "common/ob_define.h"

namespace oceanbase
{
  namespace obsql
  {
    using namespace common;

    int ObSchemaPrinter::output()
    {
      int ret = OB_SUCCESS;
      uint64_t max_table_id = 0;
      
      fprintf(stderr, "--------------------Schema Start--------------------\n");
      fprintf(stderr, "[app_name]\n");
      fprintf(stderr, "name=%s\n", schema_mgr_.get_app_name());

      /* There is no interface of ObSchemaManager.max_table_id_, so we have to scan them one by one */
      for (const oceanbase::common::ObTableSchema* it = schema_mgr_.table_begin(); it != schema_mgr_.table_end(); ++ it)
      {
        if (max_table_id < it->get_table_id())
          max_table_id = it->get_table_id();
      }
      
      fprintf(stderr, "max_table_id=%ld\n", max_table_id);

      for (const oceanbase::common::ObTableSchema* it = schema_mgr_.table_begin(); it != schema_mgr_.table_end(); ++it)
      {
        fprintf(stderr, "\n[%s]\n", it->get_table_name());
        fprintf(stderr, "table_id=%ld\n", it->get_table_id());
        fprintf(stderr, "table_type=%d\n", it->get_table_type());
        fprintf(stderr, "rowkey_split=%d\n", it->get_split_pos());
        fprintf(stderr, "rowkey_max_length=%d\n", it->get_rowkey_max_length());

        ret = column_output(it);
        if (ret)
        {
          fprintf(stderr, "column output Error!\n");
          return ret;
        }
        ret = join_output(it);
        if (ret)
        {
          fprintf(stderr, "join info output Error!\n");
          return ret;
        }

        fprintf(stderr, "compress_func_name=%s\n", it->get_compress_func_name());
        fprintf(stderr, "block_size=%d\n", it->get_block_size());
        fprintf(stderr, "use_bloomfilter=%d\n", it->is_use_bloomfilter());
        fprintf(stderr, "rowkey_is_fixed_length=%d\n", it->is_row_key_fixed_len());
        fprintf(stderr, "max_column_id=%ld\n", it->get_max_column_id());
      }

      fprintf(stderr, "--------------------Schema End--------------------\n");

      return ret;
    }

    int ObSchemaPrinter::column_output(const oceanbase::common::ObTableSchema* table)
    {
      int ret = OB_SUCCESS;
      uint64_t table_id = table->get_table_id();
      int32_t  column_size;
      const ObColumnSchemaV2  *columns = schema_mgr_.get_table_schema(table_id, column_size);
      for (int32_t i = 0; i < column_size; i++)
      {
        fprintf(stderr, "column_info=%d,%ld,%s,%s", 
                (columns[i].is_maintained() ? 1 : 0),
                columns[i].get_id(),
                columns[i].get_name(),
                get_type_string(columns + i));
        (columns[i].get_type() == oceanbase::common::ObVarcharType)?
          fprintf(stderr, ",%ld\n", columns[i].get_size()) : fprintf(stderr, "\n");
      }
      
      return ret;
    }

    /* This ugly implementation is because that there is no interface to get join column size */
    int ObSchemaPrinter::join_output(const oceanbase::common::ObTableSchema* table)
    {
      int       ret = OB_SUCCESS;
      bool      is_first = true;
      uint64_t  table_id = table->get_table_id();
      int32_t   column_size;
      const ObColumnSchemaV2  *columns = schema_mgr_.get_table_schema(table_id, column_size);

      for (int32_t i = 0; i < column_size; i++)
      {
        const ObColumnSchemaV2::ObJoinInfo *join_info = columns[i].get_join_info();
        
        if (join_info != NULL)
        {
          const ObColumnSchemaV2 *join_column = schema_mgr_.get_column_schema(table_id, join_info->correlated_column_);
          if (is_first)
          {
            const ObTableSchema *join_table = schema_mgr_.get_table_schema(join_info->join_table_);
            is_first = false;
            fprintf(stderr, "join=rowkey[%d,%d]%%%s:%s$%s", join_info->start_pos_, join_info->end_pos_,
                    join_table->get_table_name(), columns[i].get_name(), join_column->get_name());
          }
          else
          {
            fprintf(stderr, ",%s$%s", columns[i].get_name(), join_column->get_name());
          }
        }
      }
      if (!is_first)
        fprintf(stderr, "\n");

/*
      while ((joininfo = schema->get_join_info(i)) != NULL)
      {
        int isfirst = 1;
        
        joininfo->get_rowkey_join_range(start_pos, end_pos);
        fprintf(stderr, "join=rowkey[%d,%d]%%%s:", start_pos, end_pos,
                        schema_mgr_.get_table_name(joininfo->get_table_id_joined()));

        const oceanbase::common::ObSchema* joinschema = 
          schema_mgr_.get_table_schema(joininfo->get_table_id_joined());

        for (const oceanbase::common::ObColumnSchema* it = joinschema->column_begin(); 
             it != joinschema->column_end(); ++it)
        {
          uint64_t lcid = oceanbase::common::OB_INVALID_ID;

          lcid = joininfo->find_left_column_id(it->get_id());
          if (lcid != oceanbase::common::OB_INVALID_ID)
          {
            if (isfirst)
            {
              fprintf(stderr, "%s$%s", schema->find_column_info(lcid)->get_name(), it->get_name());
              isfirst = 0;
            }
            else
              fprintf(stderr, ",%s$%s", schema->find_column_info(lcid)->get_name(), it->get_name());
          }
        }
        fprintf(stderr, "\n");

        i++;        
      }
*/

      return ret;
    }

    const char* ObSchemaPrinter::get_type_string(const oceanbase::common::ObColumnSchemaV2 * column)
    {
      switch(column->get_type())
      {
        case oceanbase::common::ObNullType:
          return "";
          break;
        case oceanbase::common::ObIntType:
          return "int";
          break;
        case oceanbase::common::ObFloatType:
          return "float";
          break;
        case oceanbase::common::ObDoubleType:
          return "double";
          break;
        case oceanbase::common::ObDateTimeType:
          return "datetime";
          break;
        case oceanbase::common::ObPreciseDateTimeType:
          return "precise_datetime";
          break;
        case oceanbase::common::ObVarcharType:
          return "varchar";
          break;
        case oceanbase::common::ObSeqType:
          return "sequence";
          break;
        case oceanbase::common::ObCreateTimeType:
          return "create_time";
          break;
        case oceanbase::common::ObModifyTimeType:
          return "modify_time";
          break;
        default:
          return "";
          break;
      }
    }
  }
}
