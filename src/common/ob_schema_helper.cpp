/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 * First Create_time: 2011-8-5
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 */

#include "ob_schema_helper.h"
#include "ob_hint.h"

namespace oceanbase
{
  namespace common
  {
    ObSchemaHelper::ObSchemaHelper()
    {
    }
    ObSchemaHelper::~ObSchemaHelper()
    {
    }

    int ObSchemaHelper::get_left_offset_array(
        const ObArray<TableSchema>& table_schema_array, const int64_t left_table_index,
        const uint64_t right_table_id,
        uint64_t *left_column_offset_array, int64_t &size)
    {
      int ret = OB_SUCCESS;
      size = 0;
      int64_t right_table_index = 0;
      if ((NULL == left_column_offset_array)
          || (0 == right_table_id)
          || (0 == table_schema_array.at(left_table_index).join_info_.count())
          || (left_table_index >= table_schema_array.count()))
      {
        TBSYS_LOG(WARN, "invalid argument, table_id[%lu] not legal, or left_table is not a wide table,join_count=%ld",
            right_table_id, table_schema_array.at(left_table_index).join_info_.count());
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = get_table_by_id(table_schema_array, right_table_id, right_table_index)))
        {
          TBSYS_LOG(WARN, "fail to get table by id. table_id=%ld", right_table_id);
        }
        else if (right_table_index == left_table_index)
        {
          ret = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "something wrong, right_table_index=left_table_index");
        }
      }
      TableSchema *left_table_schema = NULL;
      TableSchema *right_table_schema = NULL;
      if (OB_SUCCESS == ret)
      {
        left_table_schema = (TableSchema*)(&table_schema_array.at(left_table_index));
        right_table_schema = (TableSchema*)(&table_schema_array.at(right_table_index));
        for (int64_t i = 0; i < left_table_schema->join_info_.count(); i++)
        {
          uint64_t right_column_id = left_table_schema->join_info_.at(i).right_column_id_;
          uint64_t left_column_id = left_table_schema->join_info_.at(i).left_column_id_;
          int64_t rowkey_index = 0;
          int64_t offset = 0;
          ret = right_table_schema->get_column_rowkey_index(right_column_id, rowkey_index);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fail to get column rowkey index. column_id=%lu", right_column_id);
            break;
          }
          else if (OB_SUCCESS != (ret = left_table_schema->get_column_rowkey_index(left_column_id, offset)))
          {
            TBSYS_LOG(WARN, "fail to get column rowkey index. column_id=%lu", left_column_id);
            break;
          }
          else if (offset > 0 && rowkey_index > 0)
          {
            *(left_column_offset_array + (rowkey_index - 1)) = offset - 1;
            size ++;
          }
          else
          {
            TBSYS_LOG(WARN, "rowkey column have rowkey_id less than 0. left_column_id=%lu," \
                "rokey_index=%lu, right_column_id = %lu, rowkey_index=%lu",
                left_column_id, offset, right_column_id, rowkey_index);
          }
        }
      }
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to get left_column_offset_array");
      }
      return ret;
    }
    int ObSchemaHelper::get_table_by_id(const ObArray<TableSchema>& table_schema_array,
        const uint64_t table_id, int64_t &index)
    {
      int ret = OB_SUCCESS;
      index = -1;
      for (int i = 0; i < table_schema_array.count(); i++)
      {
        if (table_schema_array.at(i).table_id_ == table_id)
        {
          index = i;
          break;
        }
      }
      if (-1 == index)
      {
        TBSYS_LOG(WARN, "not find table, table_id=%lu", table_id);
        ret = OB_ENTRY_NOT_EXIST;
      }
      return ret;
    }

    int ObSchemaHelper::transfer_manager_to_table_schema(const ObSchemaManagerV2 &schema_manager, ObArray<TableSchema> &table_schema_array)
    {
      int ret = OB_SUCCESS;
      if (0 == schema_manager.get_table_count())
      {
        TBSYS_LOG(INFO, "schema_manager has 0 table.");
      }
      else
      {
        ObTableSchema *table = (ObTableSchema*)schema_manager.table_begin();
        while (OB_SUCCESS == ret && table != schema_manager.table_end())
        {
          TableSchema table_schema;
          bool has_join_info = false;
          ObColumnSchemaV2::ObJoinInfo *tmp_join = NULL;
          ObColumnSchemaV2::ObJoinInfo *join = NULL;
          //mod zhaoqiong [database manager]@20150610:b
          // memcpy(table_schema.table_name_, table->get_table_name(), strlen(table->get_table_name()) + 1);
          char const * ptr = table->get_table_name();
          while ( *ptr != '\0')
          {
            if (*ptr == '.')
              break;
            else
              ptr++;
          }
          if(( *ptr != '\0') && (ptr != table->get_table_name()))
          {
            int64_t db_len = ptr - table->get_table_name() +1;
            snprintf(table_schema.dbname_,db_len,"%s",table->get_table_name());
            memcpy(table_schema.table_name_, ptr+1, strlen(table->get_table_name()) - db_len);
          }
          else
          {
            memcpy(table_schema.table_name_, table->get_table_name(), strlen(table->get_table_name()) + 1);
          }
          //mod:e
          table_schema.table_id_ = table->get_table_id();
          if (table->get_table_type() == ObTableSchema::SSTABLE_IN_DISK)
          {
            table_schema.load_type_ = TableSchema::DISK;
          }
          else if (table->get_table_type() == ObTableSchema::SSTABLE_IN_RAM)
          {
            table_schema.load_type_ = TableSchema::MEMORY;
          }
          else
          {
            table_schema.load_type_ = TableSchema::INVALID;
          }
          table_schema.table_type_ = TableSchema::NORMAL; //@todo
          table_schema.table_def_type_ = TableSchema::USER_DEFINED; //@todo
          table_schema.rowkey_column_num_ = static_cast<int32_t>(table->get_rowkey_info().get_size());
		  //add zhaoqiong[roottable tablet management]20150409:b
          //table_schema.replica_num_ = 3; //@todo
		  table_schema.replica_num_ = static_cast<int32_t>(table->get_replica_count());
		  //add e
          table_schema.max_used_column_id_ = table->get_max_column_id();
          table_schema.create_mem_version_ = table->get_version();
          table_schema.tablet_max_size_ = table->get_max_sstable_size();
          table_schema.tablet_block_size_ = table->get_block_size();
          table_schema.max_rowkey_length_ = table->get_rowkey_max_length();
          memcpy(table_schema.compress_func_name_,
              table->get_compress_func_name(), strlen(table->get_compress_func_name()) + 1);
          memcpy(table_schema.expire_condition_,
              table->get_expire_condition(), strlen(table->get_expire_condition()) + 1);
          memcpy(table_schema.comment_str_,
              table->get_comment_str(), strlen(table->get_comment_str()) + 1);
          table_schema.rowkey_split_ = table->get_split_pos();
          table_schema.create_time_column_id_ = table->get_create_time_column_id();
          table_schema.modify_time_column_id_ = table->get_modify_time_column_id();
          table_schema.is_use_bloomfilter_ = table->is_use_bloomfilter();
          table_schema.is_pure_update_table_ = table->is_pure_update_table();
          table_schema.merge_write_sstable_version_ = table->get_merge_write_sstable_version();
          table_schema.consistency_level_ = table->get_consistency_level();
          table_schema.schema_version_ = table->get_schema_version();
          int32_t columns_size = 0;
          ObColumnSchemaV2* column = (ObColumnSchemaV2*)schema_manager.get_table_schema(table->get_table_id(), columns_size);
          ObRowkeyInfo rowkey_info = table->get_rowkey_info();
          for (int64_t i = 0; i < columns_size; i++)
          {
            ColumnSchema column_schema;
            tmp_join = (ObColumnSchemaV2::ObJoinInfo*)(column->get_join_info());
            if (NULL != tmp_join)
            {
              TBSYS_LOG(DEBUG, "column have join_info, join_table_id=%lu, join_column_id_=%lu",
                  column->get_join_info()->join_table_, column->get_join_info()->correlated_column_);
              has_join_info = true;
              join = tmp_join;
              column_schema.join_table_id_ = column->get_join_info()->join_table_;
              column_schema.join_column_id_ = column->get_join_info()->correlated_column_;
            }
            else
            {
              column_schema.join_table_id_ = OB_INVALID_ID;
              column_schema.join_column_id_ = OB_INVALID_ID;
            }
            memcpy(column_schema.column_name_, column->get_name(), strlen(column->get_name()) + 1);
            TBSYS_LOG(DEBUG, "column_name=%s", column->get_name());
            column_schema.column_id_ = column->get_id();
            column_schema.column_group_id_ = column->get_column_group_id();
            column_schema.data_type_ = column->get_type();
            column_schema.data_length_ = column->get_size();
            //@todo
            //column_schema.data_precision_ = column->
            //column_schema.data_scale_ = column->
            //column_schema.nullable_ = column->
            int64_t index = 0;
            if (rowkey_info.is_rowkey_column(column->get_id()))
            {
              ObRowkeyColumn rowkey_column;
              if (OB_SUCCESS != (ret = rowkey_info.get_index(column->get_id(), index, rowkey_column)))
              {
                ret = OB_ERROR;
                TBSYS_LOG(WARN, "fail to get column rowkey index. ret=%d", ret);
                break;
              }
              else
              {
                column_schema.length_in_rowkey_ = rowkey_column.length_;
                column_schema.rowkey_id_ = index + 1;
              }
            }
            else
            {
              column_schema.length_in_rowkey_ = 0;
              column_schema.rowkey_id_ = index;
            }
            table_schema.add_column(column_schema);
            column++;
          }
          if (has_join_info)
          {
            ObArray<JoinInfo> join_info;
            ret = get_join_info_by_offset_array(*join, rowkey_info, table_schema.table_name_, strlen(table_schema.table_name_),
                table_schema.columns_, table_schema.table_id_, &schema_manager, join_info);
            for(int64_t i = 0; i < join_info.count(); i++)
            {
              table_schema.add_join_info(join_info.at(i));
            }
          }
          table_schema_array.push_back(table_schema);
          table++;
        }
      }
      return ret;
    }
    int ObSchemaHelper::get_join_info_by_offset_array(const ObColumnSchemaV2::ObJoinInfo ob_join_info,
        const ObRowkeyInfo &rowkey_info,
        const char* left_table_name, const int64_t left_table_name_length,
        const ObArray<ColumnSchema>& left_column_array,
        const uint64_t left_table_id, const ObSchemaManagerV2 *schema_manager,
        ObArray<JoinInfo>& join_array)
    {
      int ret = OB_SUCCESS;
      for (uint64_t i = 0; i < ob_join_info.left_column_count_; i++)
      {
        JoinInfo join_info;
        memcpy(join_info.left_table_name_, left_table_name, left_table_name_length);
        join_info.left_table_name_[left_table_name_length] = '\0';
        join_info.left_table_id_ = left_table_id;
        int64_t rowkey_index = ob_join_info.left_column_offset_array_[i];
        join_info.left_column_id_ = rowkey_info.get_column(rowkey_index)->column_id_;
        ColumnSchema *column = NULL;
        for (int64_t j = 0; j < left_column_array.count(); j++)
        {
          if (left_column_array.at(j).column_id_ == join_info.left_column_id_)
          {
            column = (ColumnSchema*)(&left_column_array.at(j));
            break;
          }
        }
        if (NULL != column)
        {
          memcpy(join_info.left_column_name_, column->column_name_, strlen(column->column_name_) + 1);
        }
        else
        {
          TBSYS_LOG(WARN, "fail to find column_schema. column_id=%lu", join_info.left_column_id_);
          break;
        }
        join_info.right_table_id_ = ob_join_info.join_table_;
        ObTableSchema* table_schema = (ObTableSchema*)schema_manager->get_table_schema(join_info.right_table_id_);
        memcpy(join_info.right_table_name_, table_schema->get_table_name(), strlen(table_schema->get_table_name()) + 1);
        ObRowkeyInfo rowkey = table_schema->get_rowkey_info();
        uint64_t right_column_id = 0;
        rowkey.get_column_id(i, right_column_id);
        join_info.right_column_id_ = right_column_id;
        ObColumnSchemaV2 *column_schema = (ObColumnSchemaV2*)schema_manager->get_column_schema(join_info.right_table_id_,
            join_info.right_column_id_);
        memcpy(join_info.right_column_name_, column_schema->get_name(), strlen(column_schema->get_name()) + 1);
        join_array.push_back(join_info);
      }

      return ret;
    }
  }  //end_updateserver
}//end_oceanbase
