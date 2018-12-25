/*
 *add by wenghaixing[secondary index] 2014/10/24
 *This is head file for secondary index statement for generate logical plan
 *
 **/
#ifndef OB_CREATE_INDEX_STMT_H
#define OB_CREATE_INDEX_STMT_H

#include "common/ob_array.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "sql/ob_basic_stmt.h"
#include "parse_node.h"
#include "ob_schema_checker.h"
namespace oceanbase{
    namespace sql{

        class ObCreateIndexStmt :public ObBasicStmt{

        public:
           explicit ObCreateIndexStmt(common::ObStringBuf* name_pool);
           virtual ~ObCreateIndexStmt();
           int add_index_colums(ResultPlan& result_plan,const common::ObString tname,const common::ObString colums_name);
           int64_t get_index_colums_count() const;
           int64_t get_storing_columns_count() const;
           int set_index_label(ResultPlan& result_plan,const common::ObString index_name);
           const common::ObString& get_index_label() const;
           int set_storing_columns(ResultPlan& result_plan,const common::ObString tname,const common::ObString storing_name);
           const common::ObString& get_storing_columns(int64_t idx) const;
           int set_idxed_name(ResultPlan& result_plan,const common::ObString tname);
           const common::ObString& get_idxed_name() const;
           const common::ObString& get_index_columns(int64_t idx)const;
           void print(FILE* fp, int32_t level, int32_t index = 0);
           //add wenghaixing[secondary index create fix]20141225
           bool get_has_storing();
           void set_has_storing(bool val);
           int push_hit_rowkey(uint64_t cid);
           bool is_rowkey_hit(uint64_t cid);
           bool is_col_expire_in_storing(common::ObString col);
           int set_storing_columns_simple(const common::ObString storing_name);
           //add e
        protected:
          common::ObStringBuf*        name_pool_;

        private:
           // common::ObArray<common::ObString>   indexs_;
           common::ObString index_label_;
           common::ObArray<common::ObString> index_colums_;
           common::ObArray<common::ObString> storing_column_;
           common::ObString idxed_tname_;
           //add wenghaixing[secondary index create fix]20141225
           bool has_storing_col_;
           common::ObArray<uint64_t> hit_rowkey_;
           //add e
        };
         //add wenghaixing [secondary index create fix]20141225
        inline bool ObCreateIndexStmt::get_has_storing()
        {
          return has_storing_col_;
        }

        inline void ObCreateIndexStmt::set_has_storing(bool val)
        {
          has_storing_col_ = val;
        }

        inline int ObCreateIndexStmt::push_hit_rowkey(uint64_t cid)
        {
          int ret = common::OB_SUCCESS;
          if(common::OB_SUCCESS != (ret = hit_rowkey_.push_back(cid)))
          {
            TBSYS_LOG(WARN,"put cid[%ld] rowkey in hit arrary error",cid);
            //return ret;
          }
          return ret;
        }

        inline bool ObCreateIndexStmt::is_rowkey_hit(uint64_t cid)
        {
          bool ret = false;
          //uint64_t rk=common::OB_INVALID_ID;
          for(int64_t i = 0;i<hit_rowkey_.count();i++)
          {
            if(cid == hit_rowkey_.at(i))
            {
              ret=true;
              break;
            }
          }
          return ret;
        }
        inline bool ObCreateIndexStmt::is_col_expire_in_storing(common::ObString col)
        {
          bool ret = false;
          for(int64_t i = 0;i<storing_column_.count();i++)
          {
            if(col == storing_column_.at(i))
            {
              ret = true;
              break;
            }
          }
          for(int64_t i=0;i<index_colums_.count();i++)
          {
            if(col==index_colums_.at(i))
            {
              ret=true;
              break;
            }
          }
          return ret;
        }

        inline int ObCreateIndexStmt::set_storing_columns_simple(const common::ObString storing_name)
        {
          return storing_column_.push_back(storing_name);
        }
        //add e
        inline const common::ObString& ObCreateIndexStmt::get_index_columns(int64_t idx)const
        {
          return index_colums_.at(idx);
        }

        inline int64_t ObCreateIndexStmt::get_index_colums_count() const
        {
          return index_colums_.count();
        }

        inline int64_t ObCreateIndexStmt::get_storing_columns_count() const
        {
          return storing_column_.count();
        }
        inline const common::ObString& ObCreateIndexStmt::get_index_label() const
        {
          return index_label_;
        }

        inline const common::ObString& ObCreateIndexStmt::get_storing_columns(int64_t idx) const
        {
          return storing_column_.at(idx);
        }

        inline int ObCreateIndexStmt::set_index_label(ResultPlan& result_plan,const common::ObString index_name)
        {
          int ret = common::OB_SUCCESS;
          ObSchemaChecker* schema_checker = NULL;
          common::ObString str;
          uint64_t table_id = common::OB_INVALID_ID;
          schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_);
          if (NULL == schema_checker)
          {
            ret = common::OB_ERR_SCHEMA_UNSET;
            snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                    "Schema(s) are not set");
          }
          if (common::OB_SUCCESS == ret)
          {
            if (common::OB_INVALID_ID != (table_id = schema_checker->get_table_id(index_name)))
            {
              ret = common::OB_ERR_ALREADY_EXISTS;
              snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                             "table/index '%.*s' is already exist", index_name.length(), index_name .ptr());
            }
          }
          if(common::OB_SUCCESS == ret)
          {
            if (common::OB_SUCCESS != (ret = ob_write_string(*name_pool_, index_name, str)))
            {
              TBSYS_LOG(ERROR,
                  "Make space for %.*s failed", index_name.length(), index_name.ptr());
            }
            else
            {
              index_label_=str;
            }
          }
          return ret;
        }

        inline int ObCreateIndexStmt::set_idxed_name(ResultPlan& result_plan,const common::ObString tname)
        {
          int ret = common::OB_SUCCESS;
          common::ObString str;
          ObSchemaChecker* schema_checker = NULL;
          uint64_t table_id = common::OB_INVALID_ID;
          bool is_index_full = false;
          schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_);
          if (NULL == schema_checker)
          {
            ret = common::OB_ERR_SCHEMA_UNSET;
            snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                  "Schema(s) are not set");
          }
          if (common::OB_SUCCESS == ret)
          {
            if (common::OB_INVALID_ID == (table_id = schema_checker->get_table_id(tname)))
            {
              ret = common::OB_ERR_ALREADY_EXISTS;
              snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                           "the table to create index '%.*s' is not exist", tname.length(), tname.ptr());
            }
            if(common::OB_SUCCESS == ret)
            {
              if(common::OB_SUCCESS != (ret = schema_checker->is_index_full(table_id,is_index_full)))
              {
              }
              else if(is_index_full)
              {
                ret = common::OB_ERROR;
                snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                           "the table  '%.*s' index num is full!", tname.length(), tname.ptr());
              }
            }
          }
          if ((common::OB_SUCCESS == ret)&&!is_index_full&&common:: OB_SUCCESS != (ret=ob_write_string(*name_pool_, tname, str)))
          {
            TBSYS_LOG(ERROR,
                  "Make space for %.*s failed", tname.length(), tname.ptr());
          }
          else
          {
            idxed_tname_=str;
          }
          return ret;
        }

        inline const common::ObString& ObCreateIndexStmt::get_idxed_name() const
        {
          return idxed_tname_;
        }

        inline int ObCreateIndexStmt::set_storing_columns(ResultPlan& result_plan,const common::ObString table_name,const common::ObString storing_column)
        {

          int ret = common::OB_SUCCESS;
          common::ObString str;
          ObSchemaChecker* schema_checker = NULL;
          uint64_t col_id = common::OB_INVALID_ID;
          schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_);
          if (NULL == schema_checker)
          {
            ret = common::OB_ERR_SCHEMA_UNSET;
            snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                  "Schema(s) are not set");
          }
          if(common::OB_SUCCESS == ret)
          {
            col_id = schema_checker->get_column_id(table_name,storing_column);
            if(common::OB_INVALID_ID == col_id)
            {
              ret = common::OB_ERR_COLUMN_NOT_FOUND;
                  snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                        "Storing Columns %.*s are not found",storing_column.length(), storing_column.ptr());
            }
          }
          if(common::OB_SUCCESS == ret)
          {
            if(schema_checker->is_rowkey_column(table_name,storing_column))
            {
              ret = common::OB_ERR_COLUMN_NOT_FOUND;
              snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                        "Storing Columns %.*s can not be rowkey!",storing_column.length(), storing_column.ptr());
            }
          }
          if(common::OB_SUCCESS == ret)
          {
            if(storing_column_.count()>0)
            {
              for (int32_t i = 0; i < storing_column_.count(); i++)
              {
                if (storing_column_.at(i) == storing_column)
                {
                  ret = common::OB_ERROR;
                  snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                                     "Not unique index_colums_: '%.*s'", storing_column.length(), storing_column.ptr());
                                 break;
                }
              }
            }
          }
          if(common::OB_SUCCESS == ret)
          {
            for(int32_t i=0;i<index_colums_.count();i++)
            {
              if (index_colums_.at(i) == storing_column)
              {
                ret = common::OB_ERROR;
                snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                           "There is same column in storing columns and index_columns: '%.*s'", storing_column.length(), storing_column.ptr());
                break;
              }
            }
          }
          if(common::OB_SUCCESS == ret)
          {
            if (common::OB_SUCCESS != (ret = ob_write_string(*name_pool_, storing_column, str)))
            {
              TBSYS_LOG(ERROR,
                            "Make space for %.*s failed", storing_column.length(), storing_column.ptr());
            }
            else
            {
              storing_column_.push_back(str);
            }
          }
          return ret;
        }

    }
}
#endif // OB_CREATE_INDEX_STMT_H
