////===================================================================
 //
 // ob_schema_mgr.cpp updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2010-10-08 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#include "ob_schema_mgr.h"

namespace oceanbase
{
  namespace updateserver
  {
    using namespace common;
    using namespace common::hash;

    SchemaMgrImp::SchemaMgrImp() : ref_cnt_(0), name_map_(), id_map_()
    {
      TBSYS_LOG(INFO, "construct SchemaMgrImp %p", this);
      if (0 != name_map_.create(SCHEMA_HASH_SIZE))
      {
        TBSYS_LOG(ERROR, "create schema map fail");
      }
      if (0 != id_map_.create(SCHEMA_HASH_SIZE))
      {
        TBSYS_LOG(ERROR, "create schema map fail");
      }
    }

    SchemaMgrImp::~SchemaMgrImp()
    {
      TBSYS_LOG(INFO, "deconstruct SchemaMgrImp %p", this);
      clear_();
    }

    void SchemaMgrImp::analyse_column_(const ObSchema &schema, SchemaInfo &schema_info)
    {
      const ObColumnSchema *column_schema_iter = NULL;
      for (column_schema_iter = schema.column_begin();
          column_schema_iter != schema.column_end();
          column_schema_iter++)
      {
        if (NULL == column_schema_iter)
        {
          break;
        }
        if (ObCreateTimeType == column_schema_iter->get_type())
        {
          if (OB_INVALID_ID != schema_info.create_time_column_id)
          {
            TBSYS_LOG(WARN, "there is already a create_time type column id=%lu, cur=%ld",
                      schema_info.create_time_column_id, column_schema_iter->get_id());
          }
          else
          {
            schema_info.create_time_column_id = column_schema_iter->get_id();
          }
        }
        else if (ObModifyTimeType == column_schema_iter->get_type())
        {
          if (OB_INVALID_ID != schema_info.modify_time_column_id)
          {
            TBSYS_LOG(WARN, "there is already a modify_time type column id=%lu, cur=%ld",
                      schema_info.modify_time_column_id, column_schema_iter->get_id());
          }
          else
          {
            schema_info.modify_time_column_id = column_schema_iter->get_id();
          }
        }
      }
    }

    int SchemaMgrImp::set_schema_(const ObSchema &schema)
    {
      int ret = OB_SUCCESS;
      void *memory = NULL;
      if (NULL == (memory = ob_malloc(sizeof(SchemaInfo), ObModIds::OB_UPS_SCHEMA)))
      {
        TBSYS_LOG(WARN, "ob_malloc schema memory fail");
        ret = OB_ERROR;
      }
      else
      {
        SchemaInfo *pschema_info = NULL;
        if (NULL == (pschema_info = new(memory) SchemaInfo()))
        {
          TBSYS_LOG(WARN, "placement new SchemaInfo fail memory=%p", memory);
          ret = OB_ERROR;
        }
        else
        {
          ObSchema *pschema = &(pschema_info->schema);
          *pschema = schema;
          analyse_column_(schema, *pschema_info);
          const ObString table_name(0, strlen(pschema->get_table_name()),
                                    const_cast<char*>(pschema->get_table_name()));
          int hash_ret = name_map_.set(table_name, pschema_info);
          if (HASH_INSERT_SUCC != hash_ret)
          {
            TBSYS_LOG(WARN, "update name map fail hash_ret=%d", hash_ret);
            ret = OB_ERROR;
          }
          else
          {
            hash_ret = id_map_.set(pschema->get_table_id(), pschema_info);
            if (HASH_INSERT_SUCC != hash_ret)
            {
              TBSYS_LOG(WARN, "update id map fail hash_ret=%d", hash_ret);
              name_map_.erase(table_name);
              ret = OB_ERROR;
            }
          }
        }
        if (OB_SUCCESS != ret)
        {
          pschema_info->~SchemaInfo();
          ob_free(memory);
        }
      }
      return ret;
    }

    uint64_t SchemaMgrImp::get_create_time_column_id(const uint64_t table_id) const
    {
      uint64_t ret = OB_INVALID_ID;
      SchemaInfo *pschema_info = NULL;
      if (HASH_EXIST == id_map_.get(table_id, pschema_info)
          && NULL != pschema_info)
      {
        ret = pschema_info->create_time_column_id;
      }
      return ret;
    }

    uint64_t SchemaMgrImp::get_modify_time_column_id(const uint64_t table_id) const
    {
      uint64_t ret = OB_INVALID_ID;
      SchemaInfo *pschema_info = NULL;
      if (HASH_EXIST == id_map_.get(table_id, pschema_info)
          && NULL != pschema_info)
      {
        ret = pschema_info->modify_time_column_id;
      }
      return ret;
    }

    ObSchema *SchemaMgrImp::get_schema(const ObString &table_name) const
    {
      ObSchema *ret = NULL;
      SchemaInfo *pschema_info = NULL;
      if (HASH_EXIST == name_map_.get(table_name, pschema_info)
          && NULL != pschema_info)
      {
        ret = &(pschema_info->schema);
      }
      return ret;
    }

    ObSchema *SchemaMgrImp::get_schema(const uint64_t table_id) const
    {
      ObSchema *ret = NULL;
      SchemaInfo *pschema_info = NULL;
      if (HASH_EXIST == id_map_.get(table_id, pschema_info)
          && NULL != pschema_info)
      {
        ret = &(pschema_info->schema);
      }
      return ret;
    }

    void SchemaMgrImp::clear_()
    {
      if (0 != ref_cnt_)
      {
        TBSYS_LOG(WARN, "ref_cnt=%ld do not equal to 0", ref_cnt_);
      }
      else
      {
        id_map_t::iterator iter;
        for (iter = id_map_.begin(); iter != id_map_.end(); ++iter)
        {
          if (NULL != iter->second)
          {
            iter->second->~SchemaInfo();
            ob_free(iter->second);
          }
        }
        id_map_.clear();
        name_map_.clear();
      }
    }

    int SchemaMgrImp::set_schemas(const ObSchemaManagerWrapper &schema_manager)
    {
      int ret = OB_SUCCESS;
      const ObSchemaManager *com_schema_ptr = schema_manager.get_impl();
      if (NULL != com_schema_ptr)
      {
        com_schema_mgr_ = *com_schema_ptr;
      }
      const ObSchema *schema_iter = NULL;
      for (schema_iter = schema_manager.begin(); schema_iter != schema_manager.end(); schema_iter++)
      {
        if (NULL == schema_iter
            || OB_SUCCESS != (ret = set_schema_(*schema_iter)))
        {
          ret = (OB_SUCCESS == ret) ? OB_ERROR : ret;
          break;
        }
      }
      return ret;
    }

    const common::ObSchemaManager &SchemaMgrImp::get_com_schema_mgr() const
    {
      return com_schema_mgr_;
    }

    int SchemaMgrImp::build_sstable_schema(sstable::ObSSTableSchema &sstable_schema) const
    {
      int ret = OB_SUCCESS;
      sstable::ObSSTableSchemaColumnDef column_info;
      id_map_t::const_iterator iter;
      for (iter = id_map_.begin(); iter != id_map_.end(); iter++)
      {
        SchemaInfo *schema_info = iter->second;
        if (NULL == schema_info)
        {
          TBSYS_LOG(ERROR, "invalid schema_info table_id=%lu", iter->first);
          ret = OB_ERROR;
          break;
        }
        else
        {
          const ObColumnSchema *column_schema = schema_info->schema.column_begin();
          for (; column_schema != schema_info->schema.column_end(); column_schema++)
          {
            if (NULL == column_schema)
            {
              TBSYS_LOG(ERROR, "invalid column_schema table_id=%lu", iter->first);
              ret = OB_ERROR;
              break;
            }
            else
            {
              column_info.reserved_ = 0;
              column_info.column_group_id_ = DEFAULT_COLUMN_GROUP_ID;
              column_info.column_name_id_ = column_schema->get_id();
              column_info.column_value_type_ = column_schema->get_type();
              column_info.table_id_ = iter->first;
              if (OB_SUCCESS != (ret = sstable_schema.add_column_def(column_info)))
              {
                TBSYS_LOG(WARN, "add_column_def fail ret=%d group_id=%lu column_id=%lu value_type=%d table_id=%lu",
                          ret, column_info.column_group_id_, column_info.column_name_id_, 
                          column_info.column_value_type_, column_info.table_id_);
                break;
              }
            }
          }
        }
      }
      return ret;
    }

    void SchemaMgrImp::dump2text() const
    {
      const int64_t BUFFER_SIZE = 1024;
      char buffer[BUFFER_SIZE];
      snprintf(buffer, BUFFER_SIZE, "/tmp/ups_schemas.pid_%d.tim_%ld", getpid(), tbsys::CTimeUtil::getTime());
      FILE *fd = fopen(buffer, "w");
      if (NULL != fd)
      {
        id_map_t::const_iterator iter;
        for (iter = id_map_.begin(); iter != id_map_.end(); ++iter)
        {
          if (NULL != iter->second)
          {
            const ObSchema &s = iter->second->schema;
            fprintf(fd, "[TABLE_SCHEMA] table_id=%lu table_type=%d table_name=%s split_pos=%d rowkey_max_length=%d\n",
                    s.get_table_id(), s.get_table_type(), s.get_table_name(), s.get_split_pos(), s.get_rowkey_max_length());
            s.print_info();
            const ObColumnSchema *c = NULL;
            for (c = s.column_begin(); c != s.column_end(); c++)
            {
              if (NULL != c)
              {
                c->print_info();
                fprintf(fd, "              [COLUMN_SCHEMA] column_id=%lu column_name=%s column_type=%d size=%ld\n",
                        c->get_id(), c->get_name(), c->get_type(), c->get_size());
              }
            }
          }
        }
      }
      fclose(fd);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    const SchemaMgr::SchemaHandle SchemaMgr::INVALID_SCHEMA_HANDLE = NULL;

    SchemaMgr::SchemaMgr() : cur_schema_mgr_imp_(NULL), has_schema_(false)
    {
      if (NULL == (cur_schema_mgr_imp_ = new(std::nothrow) SchemaMgrImp()))
      {
        TBSYS_LOG(ERROR, "new schema mgr imp fail");
      }
    }

    SchemaMgr::~SchemaMgr()
    {
      if (NULL != cur_schema_mgr_imp_)
      {
        delete cur_schema_mgr_imp_;
        cur_schema_mgr_imp_ = NULL;
      }
    }

    int SchemaMgr::set_schemas(const ObSchemaManagerWrapper &schema_manager)
    {
      int ret = OB_SUCCESS;
      SchemaMgrImp *tmp_schema_mgr_imp = NULL;
      if (NULL == (tmp_schema_mgr_imp = new(std::nothrow) SchemaMgrImp()))
      {
        TBSYS_LOG(WARN, "new tmp schema mgr imp fail");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = tmp_schema_mgr_imp->set_schemas(schema_manager)))
      {
        TBSYS_LOG(WARN, "set schemas to tmp schema mgr imp fail ret=%d", ret);
        delete tmp_schema_mgr_imp;
        tmp_schema_mgr_imp = NULL;
      }
      else
      {
        rwlock_.wrlock();
        SchemaMgrImp *prev_schema_mgr_imp = cur_schema_mgr_imp_;
        cur_schema_mgr_imp_ = tmp_schema_mgr_imp;
        if (NULL != prev_schema_mgr_imp
            && 0 == prev_schema_mgr_imp->get_ref_cnt())
        {
          delete prev_schema_mgr_imp;
          prev_schema_mgr_imp = NULL;
        }
        rwlock_.unlock();
        has_schema_ = true;
      }
      return ret;
    }

    int SchemaMgr::get_schemas(ObSchemaManagerWrapper &schema_manager)
    {
      int ret = OB_SUCCESS;
      rwlock_.rdlock();
      if (NULL == cur_schema_mgr_imp_)
      {
        ret = OB_ERROR;
      }
      else
      {
        ret = schema_manager.set_impl(cur_schema_mgr_imp_->get_com_schema_mgr());
      }
      rwlock_.unlock();
      return ret;
    }

    uint64_t SchemaMgr::get_create_time_column_id(const uint64_t table_id) const
    {
      uint64_t ret = OB_INVALID_ID;
      rwlock_.rdlock();
      if (NULL == cur_schema_mgr_imp_)
      {
        TBSYS_LOG(WARN, "schema mgr imp null pointer");
      }
      else
      {
        ret = cur_schema_mgr_imp_->get_create_time_column_id(table_id);
      }
      rwlock_.unlock();
      return ret;
    }

    uint64_t SchemaMgr::get_modify_time_column_id(const uint64_t table_id) const
    {
      uint64_t ret = OB_INVALID_ID;
      rwlock_.rdlock();
      if (NULL == cur_schema_mgr_imp_)
      {
        TBSYS_LOG(WARN, "schema mgr imp null pointer");
      }
      else
      {
        ret = cur_schema_mgr_imp_->get_modify_time_column_id(table_id);
      }
      rwlock_.unlock();
      return ret;
    }

    ObSchema *SchemaMgr::get_schema(const ObString &table_name, SchemaHandle &schema_handle) const
    {
      ObSchema *ret = NULL;
      rwlock_.rdlock();
      if (NULL == cur_schema_mgr_imp_)
      {
        TBSYS_LOG(WARN, "schema mgr imp null pointer");
      }
      else
      {
        cur_schema_mgr_imp_->inc_ref_cnt();
        if (NULL != (ret = cur_schema_mgr_imp_->get_schema(table_name)))
        {
          schema_handle = cur_schema_mgr_imp_;
        }
        else
        {
          cur_schema_mgr_imp_->dec_ref_cnt();
        }
      }
      rwlock_.unlock();
      return ret;
    }

    ObSchema *SchemaMgr::get_schema(const uint64_t table_id, SchemaHandle &schema_handle) const
    {
      ObSchema *ret = NULL;
      rwlock_.rdlock();
      if (NULL == cur_schema_mgr_imp_)
      {
        TBSYS_LOG(WARN, "schema mgr imp null pointer");
      }
      else
      {
        cur_schema_mgr_imp_->inc_ref_cnt();
        if (NULL != (ret = cur_schema_mgr_imp_->get_schema(table_id)))
        {
          schema_handle = cur_schema_mgr_imp_;
        }
        else
        {
          cur_schema_mgr_imp_->dec_ref_cnt();
        }
      }
      rwlock_.unlock();
      return ret;
    }

    void SchemaMgr::revert_schema_handle(SchemaHandle &schema_handle)
    {
      rwlock_.rdlock();
      if (NULL != schema_handle)
      {
        int64_t ref_cnt = schema_handle->dec_ref_cnt();
        if (0 > ref_cnt)
        {
          TBSYS_LOG(WARN, "schema_mgr=%p have a unexpected ref_cnt=%d", schema_handle, ref_cnt);
        }
        if (0 == ref_cnt
            && schema_handle != cur_schema_mgr_imp_)
        {
          delete schema_handle;
          schema_handle = NULL;
        }
      }
      rwlock_.unlock();
    }

    SchemaMgr::SchemaHandle SchemaMgr::get_schema_handle()
    {
      SchemaHandle ret = NULL;
      rwlock_.rdlock();
      if (NULL == cur_schema_mgr_imp_)
      {
        TBSYS_LOG(WARN, "schema mgr imp null pointer");
      }
      else
      {
        cur_schema_mgr_imp_->inc_ref_cnt();
        ret = cur_schema_mgr_imp_;
      }
      rwlock_.unlock();
      return ret;
    }

    int SchemaMgr::build_sstable_schema(const SchemaHandle schema_handle,
                                        sstable::ObSSTableSchema &sstable_schema) const
    {
      int ret = OB_SUCCESS;
      if (NULL == schema_handle)
      {
        TBSYS_LOG(WARN, "invalid schema_handle=%p", schema_handle);
        ret = OB_ERROR;
      }
      else
      {
        schema_handle->build_sstable_schema(sstable_schema);
      }
      return ret;
    }

    uint64_t SchemaMgr::get_create_time_column_id(const SchemaHandle schema_handle, const uint64_t table_id) const
    {
      uint64_t ret = OB_INVALID_ID;
      if (NULL == schema_handle)
      {
        TBSYS_LOG(WARN, "invalid schema_handle=%p", schema_handle);
      }
      else
      {
        ret = schema_handle->get_create_time_column_id(table_id);
      }
      return ret;
    }

    uint64_t SchemaMgr::get_modify_time_column_id(const SchemaHandle schema_handle, const uint64_t table_id) const
    {
      uint64_t ret = OB_INVALID_ID;
      if (NULL == schema_handle)
      {
        TBSYS_LOG(WARN, "invalid schema_handle=%p", schema_handle);
      }
      else
      {
        ret = schema_handle->get_modify_time_column_id(table_id);
      }
      return ret;
    }

    void SchemaMgr::dump2text() const
    {
      rwlock_.rdlock();
      if (NULL == cur_schema_mgr_imp_)
      {
        TBSYS_LOG(WARN, "schema mgr imp null pointer");
      }
      else
      {
        cur_schema_mgr_imp_->dump2text();
      }
      rwlock_.unlock();
    }
  }
}

