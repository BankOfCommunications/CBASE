////===================================================================
 //
 // ob_log_meta_manager.cpp liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-05-23 by Yubai (yubai.lk@alipay.com) 
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

#include "ob_log_meta_manager.h"
#include "ob_log_schema_getter.h"  // ObLogSchema IObLogSchemaGetter
#include "ob_log_config.h"          // ObLogConfig
#include "obmysql/ob_mysql_util.h"  // get_mysql_type
#include "common/ob_tsi_factory.h"  // GET_TSI_MULT
#include "common/ob_define.h"
#include "common/ob_obj_cast.h"     // obj_cast
#include <MD.h>                     // ITableMeta IDBMeta IColMeta IMetaDataCollections
#include <DRCMessageFactory.h>      // DRCMessageFactory

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
    ObLogDBNameBuilder::ObLogDBNameBuilder() : inited_(false),
                                               allocator_(),
                                               name_map_()
    {
    }

    ObLogDBNameBuilder::~ObLogDBNameBuilder()
    {
      destroy();
    }

    int ObLogDBNameBuilder::init(const ObLogConfig &config)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (0 != name_map_.create(NAME_MAP_SIZE))
      {
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        ret = ObLogConfig::parse_tb_select(config.get_tb_select(), *this, &config);
        if (OB_SUCCESS == ret)
        {
          inited_ = true;
        }
      }
      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    int ObLogDBNameBuilder::operator ()(const char *tb_name, const ObLogConfig *config)
    {
      int ret = OB_SUCCESS;

      std::string dbn_format = config->get_dbn_format(tb_name);
      char *tb_name_cstr = NULL;
      char *dbn_format_cstr = NULL;
      if (NULL == (tb_name_cstr = allocator_.alloc(strlen(tb_name) + 1))
          || NULL == (dbn_format_cstr = allocator_.alloc(dbn_format.size() + 1)))
      {
        TBSYS_LOG(WARN, "allocate tb_name_cstr or dbn_format_cstr fail");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }

      if (OB_SUCCESS == ret)
      {
        sprintf(tb_name_cstr, "%s", tb_name);
        sprintf(dbn_format_cstr, "%s", dbn_format.c_str());
        int hash_ret = name_map_.set(tb_name_cstr, dbn_format_cstr);
        if (hash::HASH_INSERT_SUCC != hash_ret
            && hash::HASH_EXIST != hash_ret)
        {
          TBSYS_LOG(WARN, "insert into name map fail, hash_ret=%d key=%s value=%s",
                    hash_ret, tb_name_cstr, dbn_format_cstr);
          ret = OB_ERR_UNEXPECTED;
        }
      }

      return ret;
    }

    void ObLogDBNameBuilder::destroy()
    {
      inited_ = false;
      name_map_.destroy();
      allocator_.reuse();
    }

    int ObLogDBNameBuilder::get_db_name(const char *src_name,
        const uint64_t db_partition,
        char *dest_name,
        const int64_t dest_buffer_size)
    {
      int ret = OB_SUCCESS;
      const char *format = NULL;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (hash::HASH_EXIST != name_map_.get(src_name, format)
              || NULL == format)
      {
        TBSYS_LOG(WARN, "get from name map fail, tb_name=%s", src_name);
        ret = OB_ENTRY_NOT_EXIST;
      }
      else if (dest_buffer_size <= snprintf(dest_name, dest_buffer_size, format, db_partition))
      {
        TBSYS_LOG(WARN, "buffer size not enough, partition=%lu", db_partition);
        ret = OB_BUF_NOT_ENOUGH;
      }
      else
      {
        // do nothing
      }
      return ret;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    ObLogTBNameBuilder::ObLogTBNameBuilder() : inited_(false),
                                                     allocator_(),
                                                     name_map_()
    {
    }

    ObLogTBNameBuilder::~ObLogTBNameBuilder()
    {
      destroy();
    }

    int ObLogTBNameBuilder::init(const ObLogConfig &config)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (0 != name_map_.create(NAME_MAP_SIZE))
      {
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        ret = ObLogConfig::parse_tb_select(config.get_tb_select(), *this, &config);
        if (OB_SUCCESS == ret)
        {
          inited_ = true;
        }
      }
      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    int ObLogTBNameBuilder::operator ()(const char *tb_name, const ObLogConfig *config)
    {
      int ret = OB_SUCCESS;

      std::string tbn_format = config->get_tbn_format(tb_name);
      char *tb_name_cstr = NULL;
      char *tbn_format_cstr = NULL;
      if (NULL == (tb_name_cstr = allocator_.alloc(strlen(tb_name) + 1))
          || NULL == (tbn_format_cstr = allocator_.alloc(tbn_format.size() + 1)))
      {
        TBSYS_LOG(WARN, "allocate tb_name_cstr or tbn_format_cstr fail");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }

      if (OB_SUCCESS == ret)
      {
        sprintf(tb_name_cstr, "%s", tb_name);
        sprintf(tbn_format_cstr, "%s", tbn_format.c_str());
        int hash_ret = name_map_.set(tb_name_cstr, tbn_format_cstr);
        if (hash::HASH_INSERT_SUCC != hash_ret
            && hash::HASH_EXIST != hash_ret)
        {
          TBSYS_LOG(WARN, "insert into name map fail, hash_ret=%d key=%s value=%s",
                    hash_ret, tb_name_cstr, tbn_format_cstr);
          ret = OB_ERR_UNEXPECTED;
        }
      }

      return ret;
    }

    void ObLogTBNameBuilder::destroy()
    {
      inited_ = false;
      name_map_.destroy();
      allocator_.reuse();
    }

    int ObLogTBNameBuilder::get_tb_name(const char *src_name,
        const uint64_t tb_partition,
        char *dest_name,
        const int64_t dest_buffer_size)
    {
      int ret = OB_SUCCESS;
      const char *format = NULL;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (hash::HASH_EXIST != name_map_.get(src_name, format)
              || NULL == format)
      {
        TBSYS_LOG(WARN, "get from name map fail, tb_name=%s", src_name);
        ret = OB_ENTRY_NOT_EXIST;
      }
      else if (dest_buffer_size <= snprintf(dest_name, dest_buffer_size, format, tb_partition))
      {
        TBSYS_LOG(WARN, "buffer size not enough, partition=%lu", tb_partition);
        ret = OB_BUF_NOT_ENOUGH;
      }
      else
      {
        // do nothing
      }
      return ret;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    const std::string &ObLogMetaManager::META_DATA_COLLECTION_TYPE = DRCMessageFactory::DFT_METAS;
    const std::string &ObLogMetaManager::DB_META_TYPE = DRCMessageFactory::DFT_DBMeta;
    const std::string &ObLogMetaManager::TABLE_META_TYPE = DRCMessageFactory::DFT_TableMeta;
    const std::string &ObLogMetaManager::COL_META_TYPE = DRCMessageFactory::DFT_ColMeta;

    ObLogMetaManager::ObLogMetaManager() : inited_(false),
                                           version_(OB_INVALID_VERSION),
                                           reserved_version_count_(0),
                                           table_meta_map_(),
                                           meta_collect_(NULL),
                                           meta_collect_lock_(),
                                           db_name_builder_(NULL),
                                           tb_name_builder_(NULL)
    {
    }

    ObLogMetaManager::~ObLogMetaManager()
    {
      destroy();
    }

    int ObLogMetaManager::get_meta_manager(IObLogMetaManager *&meta_manager,
        IObLogSchemaGetter *schema_getter,
        IObLogDBNameBuilder *db_name_builder,
        IObLogTBNameBuilder *tb_name_builder)
    {
      int ret = OB_SUCCESS;

      IObLogMetaManager *tmp_meta_manager = GET_TSI_MULT(ObLogMetaManager, TSI_LIBOBLOG_META_MANAGER);
      if (NULL == tmp_meta_manager)
      {
        TBSYS_LOG(ERROR, "GET_TSI_MULT get IObLogMetaManager fail, return NULL");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (! tmp_meta_manager->is_inited())
      {
        ret = tmp_meta_manager->init(schema_getter, db_name_builder, tb_name_builder);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "initialize IObLogMetaManager fail, ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        meta_manager = tmp_meta_manager;
      }

      return ret;
    }

    int ObLogMetaManager::init(IObLogSchemaGetter *schema_getter,
        IObLogDBNameBuilder *db_name_builder,
        IObLogTBNameBuilder *tb_name_builder)
    {
      int ret = OB_SUCCESS;
      const ObLogSchema *total_schema = NULL;

      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == schema_getter
          || NULL == (db_name_builder_ = db_name_builder)
          || NULL == (tb_name_builder_ = tb_name_builder))
      {
        TBSYS_LOG(WARN, "invalid param, schema_getter=%p, db_name_builder=%p tb_name_builder=%p",
                  schema_getter, db_name_builder, tb_name_builder);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (total_schema = schema_getter->get_schema()))
      {
        TBSYS_LOG(ERROR, "schema_getter get invalid schema");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (OB_INVALID_VERSION == (version_ = total_schema->get_version()))
      {
        TBSYS_LOG(ERROR, "actual version of schema is invalid");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (0 != table_meta_map_.create(MAX_TABLE_META_ENTRY_NUM))
      {
        TBSYS_LOG(ERROR, "create table_meta_map fail, size=%ld", MAX_TABLE_META_ENTRY_NUM);
        ret = OB_ERR_UNEXPECTED;
      }
      else if (OB_SUCCESS != (ret = create_meta_collect_(meta_collect_)))
      {
        TBSYS_LOG(WARN, "create meta collection fails, ret=%d", ret);
      }
      else
      {
        reserved_version_count_ = 0;
        inited_ = true;
      }

      if (NULL != total_schema)
      {
        schema_getter->revert_schema(total_schema);
        total_schema = NULL;
      }

      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    void ObLogMetaManager::destroy()
    {
      inited_ = false;

      tb_name_builder_ = NULL;
      db_name_builder_ = NULL;
      table_meta_map_.destroy();
      reserved_version_count_ = 0;
      version_ = OB_INVALID_VERSION;

      destroy_meta_data_of_all_versions_();
    }

    int ObLogMetaManager::get_table_meta(ITableMeta *&table_meta,
        const uint64_t table_id,
        const uint64_t db_partition,
        const uint64_t tb_partition,
        const ObLogSchema *total_schema)
    {
      int hash_ret = 0;
      int ret = OB_SUCCESS;
      ITableMeta *tmp_table_meta = NULL;
      int64_t asked_schema_version = OB_INVALID_VERSION;
      TableMetaKey table_meta_key(table_id, db_partition, tb_partition);

      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (OB_INVALID_ID == table_id || NULL == total_schema
          || OB_INVALID_VERSION == (asked_schema_version = total_schema->get_version()))
      {
        TBSYS_LOG(ERROR, "invalid argument: table_id=%lu, total_schema=%p, asked_schema_version=%ld",
            table_id, total_schema, asked_schema_version);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (version_ > asked_schema_version)
      {
        TBSYS_LOG(ERROR, "invalid schema: current meta version=%ld, asked schema version=%ld",
            asked_schema_version, version_);
        ret = OB_ERR_INVALID_SCHEMA;
      }
      else
      {
        // Update meta data version
        if (version_ < asked_schema_version)
        {
          TBSYS_LOG(INFO, "Meta version updates from %ld to %ld", version_, asked_schema_version);

          if (OB_SUCCESS != (ret = update_meta_version_(asked_schema_version)))
          {
            TBSYS_LOG(ERROR, "update_meta_version_ fail, asked schema version=%ld, ret=%d",
                asked_schema_version, ret);
          }
        }

        if (OB_SUCCESS == ret)
        {
          if (hash::HASH_EXIST == (hash_ret = table_meta_map_.get(table_meta_key, tmp_table_meta)))
          {
            if (NULL == tmp_table_meta)
            {
              TBSYS_LOG(WARN, "unexpected error, get from table_meta_map_ fail, %s",
                  table_meta_key.to_cstring());
              ret = OB_ERR_UNEXPECTED;
            }
          }
          else if (hash::HASH_NOT_EXIST != hash_ret)
          {
            TBSYS_LOG(WARN, "unexpected error, get from table_meta_map_ fail, hash_ret=%d", hash_ret);
            ret = OB_ERR_UNEXPECTED;
          }
          else if (OB_SUCCESS != (ret = build_table_meta_(tmp_table_meta, table_meta_key, total_schema)))
          {
            TBSYS_LOG(ERROR, "build table_meta fail, %s, ret=%d", table_meta_key.to_cstring(), ret);
          }
          else if (hash::HASH_INSERT_SUCC != (hash_ret = table_meta_map_.set(table_meta_key, tmp_table_meta)))
          {
            TBSYS_LOG(WARN, "add table_meta fail, hash_ret=%d %s", hash_ret, table_meta_key.to_cstring());
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            TBSYS_LOG(INFO, "build and add table_meta succ, %s table_meta=%p",
                table_meta_key.to_cstring(), tmp_table_meta);
          }
        }
      }

      if (OB_SUCCESS == ret && NULL == tmp_table_meta)
      {
        TBSYS_LOG(ERROR, "get_table_meta fail, return NULL");
        ret = OB_ERR_UNEXPECTED;
      }

      if (OB_SUCCESS == ret)
      {
        table_meta = tmp_table_meta;

        // As table meta must belong to meta_collect_, so increase reference of meta_collect_ directly.
        inc_ref_(meta_collect_);

        TBSYS_LOG(DEBUG, "get_table_meta: table_meta=%p, meta_collect_=%p, ref=%ld",
            table_meta, meta_collect_,
            reinterpret_cast<int64_t>(meta_collect_->getUserData()));
      }

      return ret;
    }

    void ObLogMetaManager::revert_table_meta(ITableMeta *table_meta)
    {
      if (NULL != table_meta)
      {
        IMetaDataCollections *meta_collect = NULL;
        IDBMeta *db_meta = NULL;

        if (NULL == (db_meta = table_meta->getDBMeta()))
        {
          TBSYS_LOG(WARN, "can not revert ITableMeta %p, IDBMeta is NULL",
              table_meta);
        }
        else if (NULL == (meta_collect = db_meta->getMetaDataCollections()))
        {
          TBSYS_LOG(WARN, "can not revert ITableMeta %p, IMetaDataCollections is NULL",
              table_meta);
        }
        else
        {
          dec_ref_(meta_collect);

          TBSYS_LOG(DEBUG, "revert table meta=%p, meta_collect=%p, ref=%ld",
              table_meta, meta_collect,
              reinterpret_cast<int64_t>(meta_collect->getUserData()));
        }
      }
    }

    int ObLogMetaManager::create_meta_collect_(IMetaDataCollections *&meta_collect) const
    {
      OB_ASSERT(NULL == meta_collect);

      int ret = OB_SUCCESS;

      if (NULL == (meta_collect = DRCMessageFactory::createMetaDataCollections(META_DATA_COLLECTION_TYPE)))
      {
        TBSYS_LOG(WARN, "createMetaDataCollections fails, return NULL");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        // Initialize reference
        int64_t ref = 1;
        meta_collect->setUserData(reinterpret_cast<void *>(ref));
      }

      return ret;
    }

    void ObLogMetaManager::destroy_meta_data_of_all_versions_()
    {
      IMetaDataCollections *mc = meta_collect_;

      while (NULL != mc)
      {
        IMetaDataCollections *tmp_mc = mc->getPrev();
        destroy_meta_data_of_one_version_(mc);
        mc = tmp_mc;
      }

      meta_collect_ = NULL;
    }

    void ObLogMetaManager::destroy_meta_data_of_one_version_(IMetaDataCollections *meta_collect) const
    {
      if (NULL != meta_collect)
      {
        TBSYS_LOG(INFO, "Destroy meta data: addr=%p, ref=%ld",
            meta_collect, reinterpret_cast<int64_t>(meta_collect->getUserData()));

        // NOTE: All metas in IMetaDataCollections will be destroyed: IDBMeta, ITableMeta, IColMeta
        // So here we need not destroy metas one by one manually.
        // If you do not trust DRC, you can destroy them by yourself, but you should first erase
        // IDBMeta from IMetaDataCollections and erase ITableMeta from IDBMeta and erase IColMeta
        // from ITableMeta, then destroy one by one manually.
        DRCMessageFactory::destroy(meta_collect);
        meta_collect = NULL;
      }
    }

    int ObLogMetaManager::update_meta_version_(int64_t target_version)
    {
      OB_ASSERT(version_ < target_version);

      int ret = OB_SUCCESS;
      IMetaDataCollections *new_meta_collect = NULL;

      // Create a new meta data collection
      if (OB_SUCCESS != (ret = create_meta_collect_(new_meta_collect)))
      {
        TBSYS_LOG(WARN, "create meta collection fails, ret=%d", ret);
      }
      else
      {
        // Clear all meta info in table meta map
        table_meta_map_.clear();

        if (NULL != meta_collect_)
        {
          // Decrease refrence of old meta data collection
          dec_ref_(meta_collect_);
        }

        new_meta_collect->setPrev(meta_collect_);
        meta_collect_ = new_meta_collect;

        reserved_version_count_++;
        if (reserved_version_count_ > MAX_RESERVED_VERSION_COUNT)
        {
          recycle_meta_data_();
        }

        version_ = target_version;
      }

      return ret;
    }

    void ObLogMetaManager::recycle_meta_data_()
    {
      TBSYS_LOG(INFO, "Recycle meta data begin: current version=%ld, reserved_version_count=%d",
          version_, reserved_version_count_);

      if (NULL != meta_collect_)
      {
        int index = reserved_version_count_;
        IMetaDataCollections *mc = meta_collect_->getPrev();
        IMetaDataCollections *last = meta_collect_;

        TBSYS_LOG(INFO, "Recycle meta data %d: addr=%p, ref=%ld",
            index--, meta_collect_,
            reinterpret_cast<int64_t>(meta_collect_->getUserData()));

        while (NULL != mc)
        {
          IMetaDataCollections *prev = mc->getPrev();
          int64_t ref = reinterpret_cast<int64_t>(mc->getUserData());

          TBSYS_LOG(INFO, "Recycle meta data %d: addr=%p, ref=%ld", index--, mc, ref);

          if (0 == ref)
          {
            last->setPrev(prev);
            destroy_meta_data_of_one_version_(mc);
            reserved_version_count_--;
          }
          else
          {
            last = mc;
          }

          mc = prev;
        }
      }

      TBSYS_LOG(INFO, "Recycle meta data end: current version=%ld, reserved_version_count=%d",
          version_, reserved_version_count_);
    }

    void ObLogMetaManager::dec_ref_(IMetaDataCollections *meta_collect)
    {
      OB_ASSERT(NULL != meta_collect);

      ObSpinLockGuard guard(meta_collect_lock_);
      meta_collect->setUserData(reinterpret_cast<void *>(reinterpret_cast<int64_t>(meta_collect->getUserData()) - 1));
    }

    void ObLogMetaManager::inc_ref_(IMetaDataCollections *meta_collect)
    {
      OB_ASSERT(NULL != meta_collect);

      ObSpinLockGuard guard(meta_collect_lock_);
      meta_collect->setUserData(reinterpret_cast<void *>(reinterpret_cast<int64_t>(meta_collect->getUserData()) + 1));
    }

    int ObLogMetaManager::build_table_meta_(ITableMeta *&table_meta,
        const TableMetaKey &table_meta_key,
        const ObLogSchema *total_schema)
    {
      OB_ASSERT(NULL != total_schema);

      int ret = OB_SUCCESS;
      const int64_t BUFFER_SIZE = 1024;
      char db_partition_name_buffer[BUFFER_SIZE] = {'\0'};
      char tb_partition_name_buffer[BUFFER_SIZE] = {'\0'};
      IDBMeta *tmp_db_meta = NULL;
      ITableMeta *tmp_table_meta = NULL;
      bool db_exist = false;
      bool table_exist = false;

      const ObTableSchema *table_schema = NULL;

      if (NULL == (table_schema = (total_schema->get_table_schema(table_meta_key.table_id))))
      {
        TBSYS_LOG(ERROR, "get_table_schema fail, table_id=%lu", table_meta_key.table_id);
        ret = OB_ERR_UNEXPECTED;
      }
      else if (OB_SUCCESS != (ret = get_db_and_tb_partition_name_(table_meta_key,
              table_schema->get_table_name(),
              db_partition_name_buffer, BUFFER_SIZE,
              tb_partition_name_buffer, BUFFER_SIZE)))
      {
        TBSYS_LOG(ERROR, "get db and table name fail, ret=%d, %s", ret, table_meta_key.to_cstring());
      }
      else if (OB_SUCCESS != (ret = get_db_meta_(tmp_db_meta, db_partition_name_buffer, db_exist)))
      {
        TBSYS_LOG(WARN, "get db_meta fail, db_name=%s, ret=%d", db_partition_name_buffer, ret);
      }
      else if (OB_SUCCESS != (ret = get_table_meta_(tmp_table_meta,
              db_partition_name_buffer,
              tb_partition_name_buffer,
              table_exist)))
      {
        TBSYS_LOG(WARN, "get table_meta fail, db_partition_name=%s table_partition_name=%s, ret=%d",
            db_partition_name_buffer, tb_partition_name_buffer, ret);
      }
      else if (!table_exist
              && OB_SUCCESS != (ret = prepare_table_column_schema_(*total_schema, *table_schema, *tmp_table_meta)))
      {
        TBSYS_LOG(WARN, "set table schema fail, table_partition_name=%s, ret=%d",
            tb_partition_name_buffer, ret);
      }
      else
      {
        tmp_db_meta->setName(db_partition_name_buffer);
        tmp_db_meta->setEncoding(DEFAULT_ENCODING);
        tmp_db_meta->setMetaDataCollections(meta_collect_);

        tmp_table_meta->setName(tb_partition_name_buffer);
        tmp_table_meta->setDBMeta(tmp_db_meta);
        tmp_table_meta->setHasPK(true);
        tmp_table_meta->setEncoding(DEFAULT_ENCODING);
        tmp_table_meta->setUserData(static_cast<void *>(this));

        if (!db_exist
            && 0 != meta_collect_->put(tmp_db_meta))
        {
          TBSYS_LOG(ERROR, "put db_meta to meta_collect fail, db_partition_name=%s", db_partition_name_buffer);
          ret = OB_ERR_UNEXPECTED;
        }
        if (!table_exist
            && 0 != tmp_db_meta->put(tmp_table_meta))
        {
          TBSYS_LOG(ERROR, "put table_meta to db_meta fail, table_partition_name=%s db_partition_name=%s",
              tb_partition_name_buffer, db_partition_name_buffer);
          ret = OB_ERR_UNEXPECTED;
        }
      }

      if (OB_SUCCESS == ret)
      {
        table_meta = tmp_table_meta;
      }

      return ret;
    }

    int ObLogMetaManager::get_db_meta_(IDBMeta *&db_meta, const char *db_partition_name, bool &exist)
    {
      int ret = OB_SUCCESS;

      if (NULL == (db_meta = meta_collect_->get(db_partition_name)))
      {
        if (NULL == (db_meta = DRCMessageFactory::createDBMeta(DB_META_TYPE)))
        {
          TBSYS_LOG(WARN, "createDBMeta fails, return NULL");
          ret = OB_ERR_UNEXPECTED;
        }

        exist = false;
      }
      else
      {
        exist = true;
      }

      return ret;
    }

    int ObLogMetaManager::get_table_meta_(ITableMeta *&table_meta,
        const char *db_partition_name,
        const char *table_partition_name,
        bool &exist)
    {
      int ret = OB_SUCCESS;

      if (NULL == (table_meta = meta_collect_->get(db_partition_name, table_partition_name)))
      {
        if (NULL == (table_meta = DRCMessageFactory::createTableMeta(TABLE_META_TYPE)))
        {
          TBSYS_LOG(WARN, "createTableMeta fail, return NULL");
          ret = OB_ERR_UNEXPECTED;
        }

        exist = false;
      }
      else
      {
        exist = true;
      }

      return ret;
    }

    int ObLogMetaManager::get_db_and_tb_partition_name_(const TableMetaKey &table_meta_key,
        const char *table_name,
        char *db_partition_name_buffer, const int64_t db_partition_name_buffer_size,
        char *tb_partition_name_buffer, const int64_t tb_partition_name_buffer_size)
    {
      OB_ASSERT(NULL != db_partition_name_buffer && NULL != tb_partition_name_buffer);

      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = db_name_builder_->get_db_name(table_name,
              table_meta_key.db_partition,
              db_partition_name_buffer,
              db_partition_name_buffer_size)))
      {
        TBSYS_LOG(WARN, "build db name fail, ret=%d table_name=%s table_id=%lu db_partition=%lu",
            ret, table_name, table_meta_key.table_id, table_meta_key.db_partition);
      }
      else if (OB_SUCCESS != (ret = tb_name_builder_->get_tb_name(table_name,
              table_meta_key.tb_partition,
              tb_partition_name_buffer,
              tb_partition_name_buffer_size)))
      {
        TBSYS_LOG(WARN, "build table name fail, ret=%d table_name=%s table_id=%lu tb_partition=%lu",
            ret, table_name, table_meta_key.table_id, table_meta_key.tb_partition);
      }

      return ret;
    }

    int ObLogMetaManager::prepare_table_column_schema_(const ObLogSchema &total_schema,
        const ObTableSchema &table_schema,
        ITableMeta &table_meta)
    {
      int ret = OB_SUCCESS;
      int32_t column_num = 0;
      const ObRowkeyInfo &rowkey_info = table_schema.get_rowkey_info();

      const ObColumnSchemaV2 *column_schemas = total_schema.get_table_schema(
          table_schema.get_table_id(),
          column_num);
      if (NULL == column_schemas)
      {
        TBSYS_LOG(WARN, "get column schemas fail table_id=%lu", table_schema.get_table_id());
        ret = OB_ERR_UNEXPECTED;
      }

      for (int32_t i = 0; OB_SUCCESS == ret && i < column_num; i++)
      {
        const ObColumnSchemaV2 &column_schema = column_schemas[i];
        IColMeta *col_meta = NULL;
        const char *default_value = NULL;
        int mysql_type = DRCMSG_TYPES;
        if (NULL != (col_meta = table_meta.getCol(column_schema.get_name())))
        {
          TBSYS_LOG(INFO, "col_meta has been already add to table_meta succ, table_name=%s column_name=%s col_num=%d col_meta=%p",
                    table_schema.get_table_name(), column_schema.get_name(), table_meta.getColNum(column_schema.get_name()), col_meta);
        }
        else if (NULL == (col_meta = DRCMessageFactory::createColMeta(COL_META_TYPE)))
        {
          TBSYS_LOG(WARN, "createColMeta fails, col_name=%s", column_schema.get_name());
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (OB_SUCCESS != (ret = type_trans_mysql_(column_schema.get_type(), mysql_type)))
        {
          TBSYS_LOG(WARN, "type trans to mysql fail, ret=%d ob_type=%d, mysql_type=%d", ret, column_schema.get_type(), mysql_type);
        }
        else
        {
          col_meta->setName(column_schema.get_name());
          col_meta->setType(mysql_type);
          col_meta->setLength(column_schema.get_size());
          col_meta->setSigned(true);
          col_meta->setIsPK(rowkey_info.is_rowkey_column(column_schema.get_id()));
          col_meta->setNotNull(column_schema.is_nullable());
          // FIXME: 0.4 does not support default value. All default values are set to null type
          col_meta->setDefault(default_value);
          col_meta->setEncoding(DEFAULT_ENCODING);
          // Do not need
          //col_meta->setDecimals(int decimals);
          //col_meta->setRequired(int required);
          //col_meta->setValuesOfEnumSet(std::vector<std::string> &v);
          //col_meta->setValuesOfEnumSet(std::vector<const char*> &v);
          //col_meta->setValuesOfEnumSet(const char** v, size_t size);
          if (0 != table_meta.append(column_schema.get_name(), col_meta))
          {
            TBSYS_LOG(ERROR, "append col_meta to table_meta fail, table_name=%s column_name=%s", table_schema.get_table_name(), column_schema.get_name());
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            TBSYS_LOG(DEBUG, "append col_meta to table_meta succ, table_name=%s column_name=%s", table_schema.get_table_name(), column_schema.get_name());
          }
        }
      }

      // Set Primary Keys
      std::string pks;
      std::vector<int> pk_index;
      int index = 0;
      for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey_info.get_size(); i++)
      {
        const ObRowkeyColumn *rowkey_column = NULL;
        const ObColumnSchemaV2 *column_schema = NULL;
        if (NULL == (rowkey_column = rowkey_info.get_column(i)))
        {
          TBSYS_LOG(WARN, "get rowkey column fail index=%ld", i);
          ret = OB_ERR_UNEXPECTED;
        }
        else if (NULL == (column_schema = total_schema.get_column_schema(
                table_schema.get_table_id(),
                rowkey_column->column_id_, &index)))
        {
          TBSYS_LOG(WARN, "get column schema fail table_id=%lu column_id=%lu",
              table_schema.get_table_id(), rowkey_column->column_id_);
          ret = OB_ERR_UNEXPECTED;
        }
        else
        {
          pk_index.push_back(index);
          pks.append(column_schema->get_name());
          if (i < (rowkey_info.get_size() - 1))
          {
            pks.append(",");
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        table_meta.setPKIndice(pk_index);
        table_meta.setPKs(pks.c_str());
      }

      return ret;
    }

    int ObLogMetaManager::type_trans_mysql_(const ObObjType ob_type, int &mysql_type)
    {
      uint8_t num_decimals = 0;
      uint32_t length = 0;
      return obmysql::ObMySQLUtil::get_mysql_type(ob_type, (obmysql::EMySQLFieldType&)mysql_type, num_decimals, length);
    }
  }
}

