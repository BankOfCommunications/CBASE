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

#include "ob_ups_utils.h"
#include "ob_schema_mgrv2.h"

namespace oceanbase
{
  namespace updateserver
  {
    using namespace common;

    CommonSchemaManagerWrapper::CommonSchemaManagerWrapper() : schema_mgr_buffer_(NULL), schema_mgr_impl_(NULL)
    {
      schema_mgr_buffer_ = ob_malloc(sizeof(CommonSchemaManager), ObModIds::OB_UPS_SCHEMA);
      schema_mgr_impl_ = new(schema_mgr_buffer_) CommonSchemaManager();
      if (NULL == schema_mgr_impl_)
      {
        TBSYS_LOG(WARN, "new schema mgr fail");
      }
    }

    CommonSchemaManagerWrapper::CommonSchemaManagerWrapper(const CommonSchemaManager &other) : schema_mgr_buffer_(NULL),
                                                                                               schema_mgr_impl_(NULL)
    {
      schema_mgr_buffer_ = ob_malloc(sizeof(CommonSchemaManager), ObModIds::OB_UPS_SCHEMA);
      schema_mgr_impl_ = new(schema_mgr_buffer_) CommonSchemaManager(other);
      if (NULL == schema_mgr_impl_)
      {
        TBSYS_LOG(WARN, "new schema mgr fail");
      }
    }

    CommonSchemaManagerWrapper::~CommonSchemaManagerWrapper()
    {
      if (NULL != schema_mgr_buffer_)
      {
        schema_mgr_impl_->~CommonSchemaManager();
        ob_free(schema_mgr_buffer_);
        schema_mgr_buffer_ = NULL;
        schema_mgr_impl_ = NULL;
      }
    }

    CommonSchemaManagerWrapper &CommonSchemaManagerWrapper::operator= (const CommonSchemaManager &other)
    {
      if (NULL != schema_mgr_impl_)
      {
        *schema_mgr_impl_ = other;
      }
      return *this;
    }

    DEFINE_SERIALIZE(CommonSchemaManagerWrapper)
    //int CommonSchemaManagerWrapper::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int ret = OB_SUCCESS;
      if (NULL == schema_mgr_impl_)
      {
        ret = OB_ERROR;
      }
      //add zhaoqiong [Schema Manager] 20150327:b
      else if (OB_SUCCESS != (ret = schema_mgr_impl_->determine_serialize_pos()))
      {
        TBSYS_LOG(WARN, "determine_serialize_pos fail, ret = %d",ret);
      }
      //add:e
      else
      {
        ret = schema_mgr_impl_->serialize(buf, buf_len, pos);
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(CommonSchemaManagerWrapper)
    //int64_t CommonSchemaManagerWrapper::get_serialize_size(void) const
    {
      int64_t ret = 0;
      if (NULL != schema_mgr_impl_)
      {
        ret = schema_mgr_impl_->get_serialize_size();
      }
      return ret;
    }

    DEFINE_DESERIALIZE(CommonSchemaManagerWrapper)
    //int CommonSchemaManagerWrapper::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
    {
      int ret = OB_SUCCESS;
      if (NULL == schema_mgr_impl_)
      {
        ret = OB_ERROR;
      }
      else
      {
        schema_mgr_impl_->set_drop_column_group();
        ret = schema_mgr_impl_->deserialize(buf, data_len, pos);
      }
      return ret;
    }

    int64_t CommonSchemaManagerWrapper::get_version() const
    {
      int64_t ret = 0;
      if (NULL != schema_mgr_impl_)
      {
        ret = schema_mgr_impl_->get_version();
      }
      return ret;
    }

    int64_t CommonSchemaManagerWrapper::get_code_version() const
    {
      int64_t ret = 0;
      if (NULL != schema_mgr_impl_)
      {
        ret = schema_mgr_impl_->get_code_version();
      }
      return ret;
    }

    bool CommonSchemaManagerWrapper::parse_from_file(const char* file_name, tbsys::CConfig& config)
    {
      bool bret = false;
      if (NULL != schema_mgr_impl_)
      {
        bret = schema_mgr_impl_->parse_from_file(file_name, config);
      }
      return bret;
    }

    void CommonSchemaManagerWrapper::print_info() const
    {
      if (NULL != schema_mgr_impl_)
      {
        schema_mgr_impl_->print_info();
      }
    }
    //add zhaoqiong [Schema Manager] 20150327:b
    void CommonSchemaManagerWrapper::print_debug_info() const
    {
      if (NULL != schema_mgr_impl_)
      {
        schema_mgr_impl_->print_debug_info();
      }
    }
	
    void CommonSchemaManagerWrapper::reset()
    {
      if (NULL != schema_mgr_impl_)
      {
        schema_mgr_impl_->reset();
      }
    }

    CommonSchemaManager &CommonSchemaManagerWrapper::get_impl_ref()
    {
      return *schema_mgr_impl_;
    }

    //add:e

    const CommonSchemaManager *CommonSchemaManagerWrapper::get_impl() const
    {
      return schema_mgr_impl_;
    }

    int CommonSchemaManagerWrapper::set_impl(const CommonSchemaManager &schema_impl) const
    {
      int ret = OB_SUCCESS;
      if (NULL == schema_mgr_impl_)
      {
        ret = OB_ERROR;
      }
      else
      {
        *schema_mgr_impl_ = schema_impl;
      }
      return ret;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    const UpsSchemaMgr::SchemaHandle UpsSchemaMgr::INVALID_SCHEMA_HANDLE = NULL;

    UpsSchemaMgr::UpsSchemaMgr() : cur_schema_mgr_imp_(NULL), has_schema_(false)
    {
      if (NULL == (cur_schema_mgr_imp_ = new(std::nothrow) UpsSchemaMgrImp()))
      {
        TBSYS_LOG(ERROR, "new schema mgr imp fail");
      }
      else
      {
        cur_schema_mgr_imp_->born();
        //cur_schema_mgr_imp_->inc_ref_cnt();
      }
    }

    UpsSchemaMgr::~UpsSchemaMgr()
    {
      if (NULL != cur_schema_mgr_imp_)
      {
        delete cur_schema_mgr_imp_;
        cur_schema_mgr_imp_ = NULL;
      }
    }

    int UpsSchemaMgr::set_schema_mgr(const CommonSchemaManagerWrapper &schema_manager)
    {
      int ret = OB_SUCCESS;
      UpsSchemaMgrImp *tmp_schema_mgr_imp = NULL;
      const CommonSchemaManager *schema_mgr = schema_manager.get_impl();
      if (NULL == schema_mgr)
      {
        TBSYS_LOG(WARN, "get schema mgr from schema_mgr_wrapper fail");
        ret = OB_ERROR;
      }
      else if (NULL == (tmp_schema_mgr_imp = new(std::nothrow) UpsSchemaMgrImp()))
      {
        TBSYS_LOG(WARN, "new tmp schema mgr imp fail");
        ret = OB_ERROR;
      }
      else
      {
        tmp_schema_mgr_imp->get_schema_mgr() = *schema_mgr;
        rwlock_.wrlock();
        UpsSchemaMgrImp *prev_schema_mgr_imp = cur_schema_mgr_imp_;
        tmp_schema_mgr_imp->born();
        //tmp_schema_mgr_imp->inc_ref_cnt();
        cur_schema_mgr_imp_ = tmp_schema_mgr_imp;
        g_conf.global_schema_version = schema_mgr->get_version();
        if (NULL != prev_schema_mgr_imp
            && prev_schema_mgr_imp->end())
            //&& 0 == prev_schema_mgr_imp->dec_ref_cnt())
        {
          delete prev_schema_mgr_imp;
          prev_schema_mgr_imp = NULL;
        }
        rwlock_.unlock();
        has_schema_ = true;
      }
      //add zhaoqiong [Schema Manager] 20150327:b
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(TRACE, "==========print schema version[%ld] start==========",schema_manager.get_version());
        schema_manager.print_debug_info();
        TBSYS_LOG(TRACE, "==========print schema version[%ld] end==========",schema_manager.get_version());
      }
      //add:e
      return ret;
    }

    //add zhaoqiong [Schema Manager] 20150327:b
    int UpsSchemaMgr::apply_schema_mutator(const ObSchemaMutator &schema_mutator)
    {
      int ret = OB_SUCCESS;
      UpsSchemaMgrImp *tmp_schema_mgr_imp = NULL;
      if (NULL == (tmp_schema_mgr_imp = new(std::nothrow) UpsSchemaMgrImp()))
      {
        TBSYS_LOG(WARN, "new tmp schema mgr imp fail");
        ret = OB_ERROR;
      }
      else if (schema_mutator.get_refresh_schema())
      {
        ret = OB_INNER_STAT_ERROR;
        TBSYS_LOG(ERROR, "mutator contain refresh schema, need fetch full schema");
      }
      else
      {        
        tmp_schema_mgr_imp->get_schema_mgr().copy_without_sort(cur_schema_mgr_imp_->get_schema_mgr());

        if (OB_SUCCESS != (ret = tmp_schema_mgr_imp->get_schema_mgr().apply_schema_mutator(schema_mutator)))
        {
          TBSYS_LOG(WARN, "apply_schema_mutator fail(mutator version[%ld->%ld])",
                    schema_mutator.get_start_version(), schema_mutator.get_end_version());
        }
        else
        {
          rwlock_.wrlock();
          UpsSchemaMgrImp *prev_schema_mgr_imp = cur_schema_mgr_imp_;
          tmp_schema_mgr_imp->born();
          cur_schema_mgr_imp_ = tmp_schema_mgr_imp;
          g_conf.global_schema_version = schema_mutator.get_end_version();
          if (NULL != prev_schema_mgr_imp
              && prev_schema_mgr_imp->end())
          {
            delete prev_schema_mgr_imp;
            prev_schema_mgr_imp = NULL;
          }
          rwlock_.unlock();
          has_schema_ = true;
        }
      }
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(TRACE, "==========print schema version[%ld] start==========",tmp_schema_mgr_imp->get_schema_mgr().get_version());
        tmp_schema_mgr_imp->get_schema_mgr().print_debug_info();
        TBSYS_LOG(TRACE, "==========print schema version[%ld] end==========",tmp_schema_mgr_imp->get_schema_mgr().get_version());
      }
      return ret;
    }
    //add:e

    int UpsSchemaMgr::get_schema_mgr(CommonSchemaManagerWrapper &schema_manager) const
    {
      int ret = OB_SUCCESS;
      rwlock_.rdlock();
      if (NULL == cur_schema_mgr_imp_)
      {
        ret = OB_ERROR;
      }
      else
      {
        ret = schema_manager.set_impl(cur_schema_mgr_imp_->get_schema_mgr());
      }
      rwlock_.unlock();
      return ret;
    }

    int UpsSchemaMgr::get_schema_handle(SchemaHandle &schema_handle) const
    {
      int ret = OB_SUCCESS;
      rwlock_.rdlock();
      if (NULL == cur_schema_mgr_imp_)
      {
        ret = OB_ERROR;
      }
      else
      {
        cur_schema_mgr_imp_->ref();
        //cur_schema_mgr_imp_->inc_ref_cnt();
        schema_handle = cur_schema_mgr_imp_;
      }
      rwlock_.unlock();
      return ret;
    }

    void UpsSchemaMgr::revert_schema_handle(SchemaHandle &schema_handle) const
    {
      rwlock_.rdlock();
      if (NULL != schema_handle
          && schema_handle->deref())
          //&& 0 == schema_handle->dec_ref_cnt())
      {
        delete schema_handle;
        schema_handle = NULL;
      }
      rwlock_.unlock();
    }

    int64_t UpsSchemaMgr::get_version() const
    {
      int64_t ret = 0;
      SchemaHandle schema_handle = INVALID_SCHEMA_HANDLE;
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS == (tmp_ret = get_schema_handle(schema_handle)))
      {
        ret = schema_handle->get_schema_mgr().get_version();
        revert_schema_handle(schema_handle);
      }
      return ret;
    }

    uint64_t UpsSchemaMgr::get_create_time_column_id(const SchemaHandle &schema_handle, const uint64_t table_id) const
    {
      uint64_t ret = OB_INVALID_ID;
      if (INVALID_SCHEMA_HANDLE != schema_handle)
      {
        ret = schema_handle->get_schema_mgr().get_create_time_column_id(table_id);
      }
      return ret;
    }

    uint64_t UpsSchemaMgr::get_modify_time_column_id(const SchemaHandle &schema_handle, const uint64_t table_id) const
    {
      uint64_t ret = OB_INVALID_ID;
      if (INVALID_SCHEMA_HANDLE != schema_handle)
      {
        ret = schema_handle->get_schema_mgr().get_modify_time_column_id(table_id);
      }
      return ret;
    }

    uint64_t UpsSchemaMgr::get_create_time_column_id(const uint64_t table_id) const
    {
      uint64_t ret = OB_INVALID_ID;
      SchemaHandle schema_handle = INVALID_SCHEMA_HANDLE;
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS == (tmp_ret = get_schema_handle(schema_handle)))
      {
        ret = schema_handle->get_schema_mgr().get_create_time_column_id(table_id);
        revert_schema_handle(schema_handle);
      }
      return ret;
    }

    uint64_t UpsSchemaMgr::get_modify_time_column_id(const uint64_t table_id) const
    {
      uint64_t ret = OB_INVALID_ID;
      SchemaHandle schema_handle = INVALID_SCHEMA_HANDLE;
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS == (tmp_ret = get_schema_handle(schema_handle)))
      {
        ret = schema_handle->get_schema_mgr().get_modify_time_column_id(table_id);
        revert_schema_handle(schema_handle);
      }
      return ret;
    }

    const CommonTableSchema *UpsSchemaMgr::get_table_schema(const SchemaHandle &schema_handle, const uint64_t table_id) const
    {
      const CommonTableSchema *ret = NULL;
      if (INVALID_SCHEMA_HANDLE != schema_handle)
      {
        ret = schema_handle->get_schema_mgr().get_table_schema(table_id);
      }
      return ret;
    }

    const CommonTableSchema *UpsSchemaMgr::get_table_schema(const SchemaHandle &schema_handle, const ObString &table_name) const
    {
      const CommonTableSchema *ret = NULL;
      if (INVALID_SCHEMA_HANDLE != schema_handle)
      {
        ret = schema_handle->get_schema_mgr().get_table_schema(table_name);
      }
      return ret;
    }

    const CommonColumnSchema *UpsSchemaMgr::get_column_schema(const SchemaHandle &schema_handle,
                                                           const ObString &table_name,
                                                           const ObString &column_name) const
    {
      const CommonColumnSchema *ret = NULL;
      if (INVALID_SCHEMA_HANDLE != schema_handle)
      {
        ret = schema_handle->get_schema_mgr().get_column_schema(table_name, column_name);
      }
      return ret;
    }

    const CommonSchemaManager *UpsSchemaMgr::get_schema_mgr(UpsSchemaMgrGuard &guard) const
    {
      const CommonSchemaManager *ret = NULL;
      SchemaHandle schema_handle = INVALID_SCHEMA_HANDLE;
      if (OB_SUCCESS == get_schema_handle(schema_handle)
          && INVALID_SCHEMA_HANDLE != schema_handle)
      {
        ret = &(schema_handle->get_schema_mgr());
        guard.set_host(this, schema_handle);
      }
      return ret;
    }

    int UpsSchemaMgr::build_sstable_schema(const SchemaHandle schema_handle, sstable::ObSSTableSchema &sstable_schema) const
    {
      int ret = OB_SUCCESS;
      if (INVALID_SCHEMA_HANDLE == schema_handle)
      {
        ret = OB_ERROR;
      }
      else
      {
        const CommonSchemaManager &schema_mgr = schema_handle->get_schema_mgr();
        sstable_schema.reset();
        ret = oceanbase::sstable::build_sstable_schema(schema_mgr, sstable_schema);
      }
      return ret;
    }

    void UpsSchemaMgr::dump2text() const
    {
      const int64_t BUFFER_SIZE = 1024;
      char buffer[BUFFER_SIZE];
      snprintf(buffer, BUFFER_SIZE, "/tmp/ups_schemas.pid_%d.tim_%ld", getpid(), tbsys::CTimeUtil::getTime());
      FILE *fd = fopen(buffer, "w");
      if (NULL != fd)
      {
        SchemaHandle schema_handle = INVALID_SCHEMA_HANDLE;
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS == (tmp_ret = get_schema_handle(schema_handle)))
        {
          const CommonSchemaManager &schema_mgr = schema_handle->get_schema_mgr();
          uint64_t cur_table_id = OB_INVALID_ID;
          const CommonColumnSchema *iter = NULL;
          for (iter = schema_mgr.column_begin(); iter != schema_mgr.column_end(); iter++)
          {
            if (NULL != iter)
            {
              if (iter->get_table_id() != cur_table_id)
              {
                const CommonTableSchema *table_schema = schema_mgr.get_table_schema(iter->get_table_id());
                if (NULL != table_schema)
                {
                  fprintf(fd, "[TABLE_SCHEMA] table_id=%lu table_type=%d table_name=%s split_pos=%d rowkey_max_length=%d\n",
                          iter->get_table_id(), table_schema->get_table_type(), table_schema->get_table_name(),
                          table_schema->get_split_pos(), table_schema->get_rowkey_max_length());
                }
                else
                {
                  fprintf(fd, "[TABLE_SCHEMA] table_id=%lu\n", iter->get_table_id());
                }
                cur_table_id = iter->get_table_id();
              }
              fprintf(fd, "              [COLUMN_SCHEMA] column_id=%lu column_name=%s column_type=%d size=%ld\n",
                      iter->get_id(), iter->get_name(), iter->get_type(), iter->get_size());
              iter->print_info();
            }
          }
          revert_schema_handle(schema_handle);
        }
        fclose(fd);
      }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    UpsSchemaMgrGuard::UpsSchemaMgrGuard() : host_(NULL),
                                         handle_(UpsSchemaMgr::INVALID_SCHEMA_HANDLE)
    {
    }

    UpsSchemaMgrGuard::~UpsSchemaMgrGuard()
    {
      deref_();
    }

    void UpsSchemaMgrGuard::set_host(const UpsSchemaMgr *host, const UpsSchemaMgr::SchemaHandle &handle)
    {
      if (NULL != host
          && UpsSchemaMgr::INVALID_SCHEMA_HANDLE != handle)
      {
        deref_();
        host_ = host;
        handle_ = handle;
      }
    }

    void UpsSchemaMgrGuard::deref_()
    {
      if (NULL != host_
          && UpsSchemaMgr::INVALID_SCHEMA_HANDLE != handle_)
      {
        host_->revert_schema_handle(handle_);
        host_ = NULL;
        handle_ = UpsSchemaMgr::INVALID_SCHEMA_HANDLE;
      }
    }
  }
}

