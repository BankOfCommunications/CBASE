#ifndef OCEANBASE_SQL_STMT_H_
#define OCEANBASE_SQL_STMT_H_
#include "common/ob_row_desc.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "common/ob_vector.h"
#include "common/ob_hint.h"
#include "common/ob_define.h"
#include "sql/ob_basic_stmt.h"
#include "parse_node.h"
#include "common/ob_array.h" //add lbzhong [Update rowkey] 20160108:b:e

namespace oceanbase
{
  // add by zcd 20141216 :b
  // add hushuang [bloomfilter_join]20150422 :b
  namespace sql
  {
    using namespace common;
    struct ObIndexTableNamePair
    {
      common::ObString src_table_name_;
      common::ObString index_table_name_;
      uint64_t src_table_id_;
      uint64_t index_table_id_;
      ObIndexTableNamePair()
      {
        src_table_id_ = common::OB_INVALID_ID;
        index_table_id_ = common::OB_INVALID_ID;
        src_table_name_.assign_buffer(src_tb_buf, OB_MAX_TABLE_NAME_LENGTH);
        index_table_name_.assign_buffer(index_tb_buf, OB_MAX_TABLE_NAME_LENGTH);
      }
    private:
      char src_tb_buf[OB_MAX_TABLE_NAME_LENGTH];
      char index_tb_buf[OB_MAX_TABLE_NAME_LENGTH];
    };
  struct ObJoinTypeArray
    {
      ObItemType join_type_;
      int32_t index_;
      ObJoinTypeArray()
      {
        join_type_ = T_INVALID;
        index_ = 0;
      }
    };
  }

  namespace common
  {
    template <>
    struct ob_vector_traits<oceanbase::sql::ObIndexTableNamePair>
    {
      typedef oceanbase::sql::ObIndexTableNamePair* pointee_type;
      typedef oceanbase::sql::ObIndexTableNamePair value_type;
      typedef const oceanbase::sql::ObIndexTableNamePair const_value_type;
      typedef value_type* iterator;
      typedef const value_type* const_iterator;
      typedef int32_t difference_type;
    };
  template <>
    struct ob_vector_traits<oceanbase::sql::ObJoinTypeArray>
    {
      typedef oceanbase::sql::ObJoinTypeArray* pointee_type;
      typedef oceanbase::sql::ObJoinTypeArray value_type;
      typedef const oceanbase::sql::ObJoinTypeArray const_value_type;
      typedef value_type* iterator;
      typedef const value_type* const_iterator;
      typedef int32_t difference_type;
    };
  }
  // add :e

  namespace sql
  {
    struct ObQueryHint
    {
      ObQueryHint()
      {
        hotspot_ = false;
        not_use_index_ = false;
        read_consistency_ = common::NO_CONSISTENCY;
        is_insert_multi_batch_ = false;//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20151213
        is_delete_update_multi_batch_ = false;//add gaojt [Delete_Update_Function] [JHOBv0.1] 20160302
        is_all_rowkey_ = false;//add gaojt [Delete_Update_Function] [JHOBv0.1] 20160418
        is_parallal_ = true;//add gaojt [Delete_Update_Function] [JHOBv0.1] 20160612
        change_value_size_= false;//add maosy [Delete_Update_Function_isolation] [JHOBv0.1] 20161103
      }

      // add by zcd 20141218 :b
      bool has_index_hint() const
      {
        return use_index_array_.size() > 0 ? true : false;
      }
      // end :e

      bool    hotspot_;
      bool    not_use_index_;//add zhuyanchao secondary index
      common::ObConsistencyLevel    read_consistency_;
      common::ObVector<ObIndexTableNamePair> use_index_array_; // add by zcd 20141216
    common::ObVector<ObJoinTypeArray> join_array_; //add hushuang [bloomfilter_join] 20150422
      bool is_insert_multi_batch_;//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20151213
      bool is_delete_update_multi_batch_;//add gaojt [Delete_Update_Function] [JHOBv0.1] 20160302
      bool is_all_rowkey_;//add gaojt [Delete_Update_Function] [JHOBv0.1] 20160418
      bool is_parallal_;//add gaojt [Delete_Update_Function_isolation] [JHOBv0.1] 20160612
      bool change_value_size_;//add maosy [Delete_Update_Function_isolation] [JHOBv0.1] 20161103
    };

    struct TableItem
    {
      TableItem()
      {
        table_id_ = common::OB_INVALID_ID;
        ref_id_ = common::OB_INVALID_ID;
        has_scan_columns_ = false;
      }

      enum TableType
      {
        BASE_TABLE,
        ALIAS_TABLE,
        GENERATED_TABLE,
      };

      // if real table id, it is valid for all threads,
      // else if generated id, it is unique just during the thread session
      uint64_t    table_id_;
      common::ObString    db_name_;//add liumz, [multi_database.sql]:20150613
      common::ObString    table_name_;
      common::ObString    alias_name_;
      TableType   type_;
      // type == BASE_TABLE? ref_id_ is the real Id of the schema
      // type == ALIAS_TABLE? ref_id_ is the real Id of the schema, while table_id_ new generated
      // type == GENERATED_TABLE? ref_id_ is the reference of the sub-query.
      uint64_t    ref_id_;
      bool        has_scan_columns_;

      //add dragon [common] 2016-8-24 09:16:31
      int64_t to_string(char* buf, const int64_t len) const;
      //add e
    };

    struct ColumnItem
    {
      uint64_t    column_id_;
      common::ObString    column_name_;
      uint64_t    table_id_;
      uint64_t    query_id_;
      // This attribute is used by resolver, to mark if the column name is unique of all from tables
      bool        is_name_unique_;
      bool        is_group_based_;
      // TBD, we can not calculate resulte type now.
      common::ObObjType     data_type_;
    };

    struct ObColumnInfo
    {
      uint64_t table_id_;
      uint64_t column_id_;
      ObColumnInfo()
      {
        table_id_ = common::OB_INVALID_ID;
        column_id_ = common::OB_INVALID_ID;
      }
      ObColumnInfo(uint64_t tid, uint64_t cid)
      {
        table_id_ = tid;
        column_id_ = cid;
      }
    };
  }

  namespace common
  {
    template <>
      struct ob_vector_traits<oceanbase::sql::TableItem>
      {
        typedef oceanbase::sql::TableItem* pointee_type;
        typedef oceanbase::sql::TableItem value_type;
        typedef const oceanbase::sql::TableItem const_value_type;
        typedef value_type* iterator;
        typedef const value_type* const_iterator;
        typedef int32_t difference_type;
      };

    template <>
      struct ob_vector_traits<oceanbase::sql::ColumnItem>
      {
        typedef oceanbase::sql::ColumnItem* pointee_type;
        typedef oceanbase::sql::ColumnItem value_type;
        typedef const oceanbase::sql::ColumnItem const_value_type;
        typedef value_type* iterator;
        typedef const value_type* const_iterator;
        typedef int32_t difference_type;
      };

    template <>
      struct ob_vector_traits<uint64_t>
      {
        typedef uint64_t* pointee_type;
        typedef uint64_t value_type;
        typedef const uint64_t const_value_type;
        typedef value_type* iterator;
        typedef const value_type* const_iterator;
        typedef int32_t difference_type;
      };
  }

  namespace sql
  {
    class ObLogicalPlan;
    class ObStmt : public ObBasicStmt
    {
    public:
      ObStmt(common::ObStringBuf* name_pool, StmtType type);
      virtual ~ObStmt();

      //add lbzhong[Update rowkey] 20151221:b
      virtual bool exist_update_column(const uint64_t column_id) const
      {
        UNUSED(column_id);
        return false;
      }
      virtual int get_update_expr_id(const uint64_t column_id, uint64_t &expr_id) const
      {
        UNUSED(column_id);
        UNUSED(expr_id);
        return OB_SUCCESS;
      }
      virtual void get_update_columns(oceanbase::common::ObArray<uint64_t>& update_columns) const
      {
        UNUSED(update_columns);
      }
      //add:e

      int32_t get_table_size() const { return table_items_.size(); }
      int32_t get_column_size() const { return column_items_.size(); }
      int32_t get_condition_size() const { return where_expr_ids_.size(); }
      int32_t get_when_fun_size() const { return when_func_ids_.size(); }
      int32_t get_when_expr_size() const { return when_expr_ids_.size(); }
      //add liumz, [multi_database.sql]:20150613
      int write_db_table_name(const common::ObString &db_name, const common::ObString &table_name, common::ObString &db_table_name);
      //add:e
      //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160425:b
      ParseNode* get_ud_where_parse_tree(){return ud_where_parse_tree_;};
      void set_ud_where_parse_tree(ParseNode* ud_where_parse_tree){ud_where_parse_tree_ = ud_where_parse_tree;};
      //add gaojt 20160425:e
      int check_table_column(
          ResultPlan& result_plan,
          const common::ObString& column_name,
          const TableItem& table_item,
          uint64_t& column_id,
          common::ObObjType& column_type);
      int add_table_item(
          ResultPlan& result_plan,
          const common::ObString& db_name,//add liumz, [multi_database.sql]:20150613
          common::ObString& table_name,
          //const common::ObString& table_name,
          const common::ObString& alias_name,
          uint64_t& table_id,
          const TableItem::TableType type,
          const uint64_t ref_id = common::OB_INVALID_ID);
      //add liumz, [multi_database.sql]:20150615
      /**
       * @brief get_ref_table_name
       * @param db_table_name: db_name.table_name | db_name.alias_name
       * @param table_name: table_name | alias_name
       * @return table_name: real db_name.table_name
       */
      uint64_t get_ref_table_name(
          const common::ObString& db_table_name,
          common::ObString& table_name);
      //add:e
      int change_table_item_tid(uint64_t& id)
      {
        int ret = common::OB_SUCCESS;
        TableItem* table_item = get_table_item_by_id(id);
        if (NULL == table_item)
        {
          ret = common::OB_ERROR;
        }
        else
        {
          table_item->table_id_ = table_item->ref_id_;
          table_item->type_ = TableItem::BASE_TABLE;
          id = table_item->table_id_;
          // for tables bitset usage
          if ((ret = tables_hash_.add_column_desc(table_item->table_id_, OB_INVALID_ID)) != OB_SUCCESS)
            TBSYS_LOG(ERROR, "Can not add table_id to hash table");
        }
        return ret;
      }
      int add_column_item(
          ResultPlan& result_plan,
          const oceanbase::common::ObString& column_name,
          const oceanbase::common::ObString* table_name = NULL,
          ColumnItem** col_item = NULL);
      int add_column_item(const ColumnItem& column_item);
      ColumnItem* get_column_item(
        const common::ObString* table_name,
        const common::ObString& column_name);
      ColumnItem* get_column_item_by_id(uint64_t table_id, uint64_t column_id) const ;
      const ColumnItem* get_column_item(int32_t index) const
      {
        const ColumnItem *column_item = NULL;
        if (0 <= index && index < column_items_.size())
        {
          column_item = &column_items_[index];
        }
        return column_item;
      }
      TableItem* get_table_item_by_id(uint64_t table_id) const;
      TableItem& get_table_item(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < table_items_.size());
        return table_items_[index];
      }
      uint64_t get_condition_id(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < where_expr_ids_.size());
        return where_expr_ids_[index];
      }
      uint64_t get_table_item(
        const common::ObString& table_name,
        TableItem** table_item = NULL) const ;
      int32_t get_table_bit_index(uint64_t table_id) const ;

      common::ObVector<uint64_t>& get_where_exprs()
      {
        return where_expr_ids_;
      }

      common::ObVector<uint64_t>& get_when_exprs()
      {
        return when_expr_ids_;
      }

      common::ObStringBuf* get_name_pool() const
      {
        return name_pool_;
      }

      ObQueryHint& get_query_hint()
      {
        return query_hint_;
      }

      int add_when_func(uint64_t expr_id)
      {
        return when_func_ids_.push_back(expr_id);
      }

      uint64_t get_when_func_id(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < when_func_ids_.size());
        return when_func_ids_[index];
      }

      uint64_t get_when_expr_id(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < when_expr_ids_.size());
        return when_expr_ids_[index];
      }

      void set_when_number(const int64_t when_num)
      {
        when_number_ = when_num;
      }

      int64_t get_when_number() const
      {
        return when_number_;
      }

      virtual void print(FILE* fp, int32_t level, int32_t index = 0);

    //add dragon 2016-8-24 11:01:23
    public:
      void print_table_items() const;
    //add e

    protected:
      common::ObStringBuf* name_pool_;
      common::ObVector<TableItem>    table_items_;
      common::ObVector<ColumnItem>   column_items_;

    private:
      //uint64_t  where_expr_id_;
      common::ObVector<uint64_t>     where_expr_ids_;
      common::ObVector<uint64_t>     when_expr_ids_;
      common::ObVector<uint64_t>     when_func_ids_;
      ObQueryHint                    query_hint_;

      int64_t  when_number_;

      // it is only used to record the table_id--bit_index map
      // although it is a little weird, but it is high-performance than ObHashMap
      common::ObRowDesc tables_hash_;
      //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160425:b
      ParseNode* ud_where_parse_tree_;
      //add gaojt 20160425:e
    };
  }
}

#endif //OCEANBASE_SQL_STMT_H_
