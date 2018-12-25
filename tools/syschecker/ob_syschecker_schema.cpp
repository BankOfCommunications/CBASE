/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_syschecker_schema.cpp for define syschecker schema
 * manager. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "tblog.h"
#include "common/ob_define.h"
#include "ob_syschecker_schema.h"

namespace oceanbase 
{ 
  namespace syschecker 
  {
    using namespace common;

    const char* ObSyscheckerSchema::AUX_NAME_PREFIX = "aux_";

    ObSyscheckerSchema::ObSyscheckerSchema()
    : inited_(false), wt_schema_(NULL), wt_column_count_(0),  
      wt_writable_column_count_(0), wt_unwritable_column_count_(0), 
      wt_addable_column_count_(0), jt_schema_(NULL), 
      jt_column_count_(0), jt_writable_column_count_(0), 
      jt_unwritable_column_count_(0), jt_addable_column_count_(0)
    {
      memset(wt_writable_column_, 0, OB_MAX_COLUMN_NUMBER * sizeof(ObColumnPair));
      memset(wt_unwritable_column_, 0, OB_MAX_COLUMN_NUMBER * sizeof(ObColumnPair));
      memset(wt_addable_column_, 0, OB_MAX_COLUMN_NUMBER * sizeof(ObColumnPair));
      memset(jt_writable_column_, 0, OB_MAX_COLUMN_NUMBER * sizeof(ObColumnPair));
      memset(jt_unwritable_column_, 0, OB_MAX_COLUMN_NUMBER * sizeof(ObColumnPair));
      memset(jt_addable_column_, 0, OB_MAX_COLUMN_NUMBER * sizeof(ObColumnPair));
    }

    ObSyscheckerSchema::~ObSyscheckerSchema()
    {

    }

    const ObColumnSchemaV2* ObSyscheckerSchema::find_aux_column_schema(
      const ObTableSchema* schema, const char* org_column_name)
    {
      int ret = OB_SUCCESS;
      ObColumnSchemaV2* column[OB_MAX_COLUMN_GROUP_NUMBER] = {NULL};
      int32_t column_size = OB_MAX_COLUMN_GROUP_NUMBER;
      char aux_column_name[OB_MAX_COLUMN_NAME_LENGTH];

      if (NULL != schema && NULL != org_column_name)
      {
        sprintf(aux_column_name, "%s%s", AUX_NAME_PREFIX, org_column_name);
        ret = schema_manager_.get_column_schema(
          schema->get_table_name(), aux_column_name, column, column_size);
        if (OB_SUCCESS != ret || column_size <= 0)
        {
          TBSYS_LOG(WARN, "failed to get column schema, ret=%d", ret);
          column[0] = NULL;
        }
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, schema=%p, org_column_name=%p",
                  schema, org_column_name);
      }

      return column[0];
    }

    int ObSyscheckerSchema::init_table_schema()
    {
      int ret                         = OB_SUCCESS;
      const ObTableSchema* schema     = NULL;
      const ObColumnSchemaV2* column  = NULL;
      int32_t column_size             = 0; 
      const ObColumnSchemaV2::ObJoinInfo* join_info = NULL;

      for (schema = schema_manager_.table_begin(); 
           schema != schema_manager_.table_end() && schema != NULL; 
           schema++)
      {
        int64_t i = 0;
        column = schema_manager_.get_table_schema(schema->get_table_id(), column_size);
        for (i = 0; i < column_size; ++i)
        {
          join_info = column[i].get_join_info();
          if (NULL != join_info)
          {
            //table which has join info is wide table
            wt_schema_ = schema;
            break;
          }
        }

        if (i == column_size)
        {
          jt_schema_ = schema;
        }
      }

      if (NULL == wt_schema_)
      {
        wt_schema_ = schema_manager_.get_table_schema(1001);
      }
      if (NULL == jt_schema_)
      {
        jt_schema_ = schema_manager_.get_table_schema(1002);
      }

      return ret;
    }

    int ObSyscheckerSchema::init_wt_column()
    {
      int ret                             = OB_SUCCESS;
      const ObColumnSchemaV2* column      = NULL;
      const ObColumnSchemaV2* aux_column  = NULL;
      const char* column_name             = NULL;
      ObObjType type                      = ObNullType;
      int32_t column_size                 = 0 ;

      if (NULL == wt_schema_ || NULL == jt_schema_)
      {
        TBSYS_LOG(WARN, "wide table or join table schema is NULL, "
                        "wt_schema=%p, jt_schema=%p", 
                  wt_schema_, jt_schema_);
        ret = OB_ERROR;
      }
      else
      {
        column = schema_manager_.get_table_schema(wt_schema_->get_table_id(), column_size);
        const ObRowkeyInfo& rowkey_info = schema_manager_.get_table_schema(wt_schema_->get_table_id())->get_rowkey_info();
        for (int64_t i = 0; i < column_size; ++i)
        {
          // donot add rowkey columns;
          if (rowkey_info.is_rowkey_column(column[i].get_id()))
          {
            continue;
          }

          column_name = column[i].get_name();
          type = column[i].get_type();

          //original cloumn name doesn't start with 'aux'
          if (memcmp(column_name, AUX_NAME_PREFIX, AUX_NAME_PREFIX_SIZE) != 0)
          {
            aux_column = find_aux_column_schema(wt_schema_, column_name);
            wt_column_[wt_column_count_].org_ = &column[i];
            wt_column_[wt_column_count_].aux_ = aux_column;
            TBSYS_LOG(INFO, "wide table: column=%s, aux_column=%s",
                column[i].get_name(), aux_column == NULL ? NULL : aux_column->get_name());
            wt_column_count_++;

            if (NULL == column[i].get_join_info())
            {
              //create time and modify time can't be written
              if (ObCreateTimeType != type && ObModifyTimeType != type)
              {
                wt_writable_column_[wt_writable_column_count_].org_ = &column[i];
                wt_writable_column_[wt_writable_column_count_].aux_ = aux_column;
                wt_writable_column_count_++;
              }

              //only int, float, double can be added
              if (ObIntType == type || /*ObFloatType == type 
                  || ObDoubleType == type ||*/ ObDateTimeType == type
                  || ObPreciseDateTimeType == type)
              {
                wt_addable_column_[wt_addable_column_count_].org_ = &column[i];
                wt_addable_column_[wt_addable_column_count_].aux_ = aux_column;
                wt_addable_column_count_++;
              }

              //exclude create time and modify time
              if (ObCreateTimeType == type || ObModifyTimeType == type)
              {
                wt_unwritable_column_[wt_unwritable_column_count_].org_ = &column[i];
                wt_unwritable_column_[wt_unwritable_column_count_].aux_ = aux_column;
                wt_unwritable_column_count_++;
              }
            }
            else
            {
              //column in join table, can't write
              wt_unwritable_column_[wt_unwritable_column_count_].org_ = &column[i];
              wt_unwritable_column_[wt_unwritable_column_count_].aux_ = aux_column;
              wt_unwritable_column_count_++;
            }
          }
        }
      }

      if (0 == wt_column_count_)
      {
        TBSYS_LOG(WARN, "no column in wide table, wt_column_count_=%ld", 
                  wt_column_count_);
        ret = OB_ERROR;
      }

      return ret;
    }

    int ObSyscheckerSchema::init_jt_column()
    {
      int ret                             = OB_SUCCESS;
      const ObColumnSchemaV2* column      = NULL;
      const ObColumnSchemaV2* aux_column  = NULL;
      const char* column_name             = NULL;
      ObObjType type                      = ObNullType;
      int32_t column_size                 = 0 ;

      if (NULL == jt_schema_)
      {
        TBSYS_LOG(WARN, "wide table or join table schema is NULL, "
                        "wt_schema=%p, jt_schema=%p", 
                  wt_schema_, jt_schema_);
        ret = OB_ERROR;
      }
      else
      {
        column = schema_manager_.get_table_schema(jt_schema_->get_table_id(), column_size);
        const ObRowkeyInfo& rowkey_info = schema_manager_.get_table_schema(jt_schema_->get_table_id())->get_rowkey_info();
        for (int64_t i = 0; i < column_size; ++i)
        {
          // donot add rowkey columns;
          if (rowkey_info.is_rowkey_column(column[i].get_id()))
          {
            continue;
          }

          column_name = column[i].get_name();
          type = column[i].get_type();

          //original cloumn name doesn't start with 'aux'
          if (memcmp(column_name, AUX_NAME_PREFIX, AUX_NAME_PREFIX_SIZE) != 0)
          {
            aux_column = find_aux_column_schema(jt_schema_, column_name);
            jt_column_[jt_column_count_].org_ = &column[i];
            jt_column_[jt_column_count_].aux_ = aux_column;
            TBSYS_LOG(INFO, "join table: column=%s, aux_column=%s",
                column[i].get_name(), aux_column == NULL ? NULL : aux_column->get_name());
            jt_column_count_++;

            //create time and modify time can't be written
            if (ObCreateTimeType != type && ObModifyTimeType != type)
            {
              jt_writable_column_[jt_writable_column_count_].org_ = &column[i];
              jt_writable_column_[jt_writable_column_count_].aux_ = aux_column;
              jt_writable_column_count_++;
            }

            //only int, float, double can be written
            if (ObIntType == type || /*ObFloatType == type 
                || ObDoubleType == type ||*/ ObDateTimeType == type
                || ObPreciseDateTimeType == type) 
            {
              jt_addable_column_[jt_addable_column_count_].org_ = &column[i];
              jt_addable_column_[jt_addable_column_count_].aux_ = aux_column;
              jt_addable_column_count_++;
            }

            //exclude create time and modify time
            if (ObCreateTimeType == type || ObModifyTimeType == type)
            {
              jt_unwritable_column_[jt_unwritable_column_count_].org_ = &column[i];
              jt_unwritable_column_[jt_unwritable_column_count_].aux_ = aux_column;
              jt_unwritable_column_count_++;
            }
          }
        }
      }

      if (0 == jt_column_count_)
      {
        TBSYS_LOG(WARN, "no column in join table, jt_column_count_=%ld", 
                  jt_column_count_);
        ret = OB_ERROR;
      }

      return ret;
    }

    int ObSyscheckerSchema::init()
    {
      int ret             = OB_SUCCESS;
      int64_t schema_size = 0;

      if (inited_)
      {
        TBSYS_LOG(WARN, "syschecker schema has inited before");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret 
          && (schema_size = schema_manager_.get_table_count()) != TABLE_COUNT)
      {
        TBSYS_LOG(WARN, "expected 2 schema in schema manager, but get %ld schema",
                  schema_size);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ret = init_table_schema();
      }

      if (OB_SUCCESS == ret)
      {
        ret = init_wt_column();
      }

      if (OB_SUCCESS == ret)
      {
        ret = init_jt_column();
      }

      if (OB_SUCCESS == ret)
      {
        inited_ = true;
      }

      return ret;
    }

    const int64_t ObSyscheckerSchema::get_wt_column_count() const
    {
      return wt_column_count_;
    }

    const int64_t ObSyscheckerSchema::get_wt_writable_column_count() const
    {
      return wt_writable_column_count_;
    }

    const int64_t ObSyscheckerSchema::get_wt_unwritable_column_count() const
    {
      return wt_unwritable_column_count_;
    }

    const int64_t ObSyscheckerSchema::get_wt_addable_column_count() const
    {
      return wt_addable_column_count_;
    }

    const int64_t ObSyscheckerSchema::get_jt_column_count() const 
    {
      return jt_column_count_;
    }

    const int64_t ObSyscheckerSchema::get_jt_writable_column_count() const
    {
      return jt_writable_column_count_;
    }

    const int64_t ObSyscheckerSchema::get_jt_unwritable_column_count() const
    {
      return jt_unwritable_column_count_;
    }

    const int64_t ObSyscheckerSchema::get_jt_addable_column_count() const
    {
      return jt_addable_column_count_;
    }

    const ObSchemaManagerV2& ObSyscheckerSchema::get_schema_manager() const
    {
      return schema_manager_;
    }

    ObSchemaManagerV2& ObSyscheckerSchema::get_schema_manager()
    {
      return schema_manager_;
    }

    const ObTableSchema* ObSyscheckerSchema::get_wt_schema() const
    {
      return wt_schema_;
    }

    const ObTableSchema* ObSyscheckerSchema::get_jt_schema() const
    {
      return jt_schema_;
    }

    const ObString ObSyscheckerSchema::get_wt_name() const 
    {
      ObString table_name;
      const char* name_str = NULL;

      if (inited_ && NULL != wt_schema_)
      {
        name_str = wt_schema_->get_table_name();
        table_name.assign(const_cast<char*>(name_str), static_cast<int32_t>(strlen(name_str)));
      }

      return table_name;
    }

    const ObString ObSyscheckerSchema::get_jt_name() const 
    {
      ObString table_name;
      const char* name_str = NULL;

      if (inited_ && NULL != jt_schema_)
      {
        name_str = jt_schema_->get_table_name();
        table_name.assign(const_cast<char*>(name_str), static_cast<int32_t>(strlen(name_str)));
      }

      return table_name;
    }

    const ObColumnPair* ObSyscheckerSchema::get_wt_column(const int64_t index) const
    {
      const ObColumnPair* column_pair = NULL;

      if (inited_ && index < wt_column_count_)
      {
        column_pair = &wt_column_[index];
      }

      return column_pair;
    }

    const ObColumnPair* ObSyscheckerSchema::get_wt_writable_column(const int64_t index) const
    {
      const ObColumnPair* column_pair = NULL;

      if (inited_ && index < wt_writable_column_count_)
      {
        column_pair = &wt_writable_column_[index];
      }

      return column_pair;
    }

    const ObColumnPair* ObSyscheckerSchema::get_wt_unwritable_column(const int64_t index) const
    {
      const ObColumnPair* column_pair = NULL;

      if (inited_ && index < wt_unwritable_column_count_)
      {
        column_pair = &wt_unwritable_column_[index];
      }

      return column_pair;
    }

    const ObColumnPair* ObSyscheckerSchema::get_wt_addable_column(const int64_t index) const
    {
      const ObColumnPair* column_pair = NULL;

      if (inited_ && index < wt_addable_column_count_)
      {
        column_pair = &wt_addable_column_[index];
      }

      return column_pair;
    }

    const ObColumnPair* ObSyscheckerSchema::get_jt_column(const int64_t index) const
    {
      const ObColumnPair* column_pair = NULL;

      if (inited_ && index < jt_column_count_)
      {
        column_pair = &jt_column_[index];
      }

      return column_pair;
    }

    const ObColumnPair* ObSyscheckerSchema::get_jt_writable_column(const int64_t index) const
    {
      const ObColumnPair* column_pair = NULL;

      if (inited_ && index < jt_writable_column_count_)
      {
        column_pair = &jt_writable_column_[index];
      }

      return column_pair;
    }

    const ObColumnPair* ObSyscheckerSchema::get_jt_unwritable_column(const int64_t index) const
    {
      const ObColumnPair* column_pair = NULL;

      if (inited_ && index < jt_unwritable_column_count_)
      {
        column_pair = &jt_unwritable_column_[index];
      }

      return column_pair;
    }

    const ObColumnPair* ObSyscheckerSchema::get_jt_addable_column(const int64_t index) const
    {
      const ObColumnPair* column_pair = NULL;

      if (inited_ && index < jt_addable_column_count_)
      {
        column_pair = &jt_addable_column_[index];
      }

      return column_pair;
    }

    const ObString ObSyscheckerSchema::get_column_name(const ObColumnSchemaV2& column) const
    {
      const char* name_str = NULL;
      ObString column_name;

      name_str = column.get_name();
      column_name.assign(const_cast<char*>(name_str), static_cast<int32_t>(strlen(name_str)));

      return column_name;
    }

    const common::ObRowkeyInfo& ObSyscheckerSchema::get_rowkey_info(const uint64_t table_id) const
    {
      return schema_manager_.get_table_schema(table_id)->get_rowkey_info();
    }

    bool ObSyscheckerSchema::is_rowkey_column(const uint64_t table_id, const uint64_t column_id) const
    {
      return get_rowkey_info(table_id).is_rowkey_column(column_id);
    }

    bool ObSyscheckerSchema::is_prefix_column(const uint64_t table_id, const uint64_t column_id) const
    {
      const common::ObRowkeyInfo& rowkey_info = get_rowkey_info(table_id);
      ObRowkeyColumn column;
      int64_t index = -1;
      return 0 == rowkey_info.get_index(column_id, index, column) && index == 0;
    }

  } // end namespace syschecker
} // end namespace oceanbase
