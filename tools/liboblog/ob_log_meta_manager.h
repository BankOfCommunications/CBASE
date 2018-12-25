////===================================================================
 //
 // ob_log_meta_manager.h liboblog / Oceanbase
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

#ifndef  OCEANBASE_LIBOBLOG_META_MANAGER_H_
#define  OCEANBASE_LIBOBLOG_META_MANAGER_H_

#include "common/ob_define.h"
#include "common/page_arena.h"        // CharArena
#include "common/ob_schema.h"         // ObTableSchema
#include "common/ob_obj_type.h"       // ObObjType
#include "common/ob_spin_lock.h"
#include "common/hash/ob_hashmap.h"   // ObHashMap
#include "ob_log_common.h"            // MAX_TB_PARTITION_NUM

#define DEFAULT_ENCODING "UTF-8"

class IDBMeta;
class IMetaDataCollections;
class ITableMeta;

namespace oceanbase
{
  namespace liboblog
  {
    class IObLogSchemaGetter;
    class ObLogSchema;
    class ObLogConfig;

    class IObLogDBNameBuilder
    {
      public:
        virtual ~IObLogDBNameBuilder() {};
      public:
        virtual int init(const ObLogConfig &config) = 0;

        virtual void destroy() = 0;

        virtual int get_db_name(const char *src_name,
            const uint64_t db_partition,
            char *dest_name,
            const int64_t dest_buffer_size) = 0;
    };

    class IObLogTBNameBuilder
    {
      public:
        virtual ~IObLogTBNameBuilder() {};
      public:
        virtual int init(const ObLogConfig &config) = 0;

        virtual void destroy() = 0;

        virtual int get_tb_name(const char *src_name,
            const uint64_t tb_partition,
            char *dest_name,
            const int64_t dest_buffer_size) = 0;
    };

    class IObLogMetaManager
    {
      public:
        virtual ~IObLogMetaManager() {};
      public:
        virtual bool is_inited() const = 0;

        virtual int init(IObLogSchemaGetter *schema_getter,
            IObLogDBNameBuilder *db_name_builder,
            IObLogTBNameBuilder *tb_name_builder) = 0;

        virtual void destroy() = 0;

        virtual int get_table_meta(ITableMeta *&table_meta,
            const uint64_t table_id,
            const uint64_t db_partition,
            const uint64_t tb_partition,
            const ObLogSchema *total_schema) = 0;

        virtual void revert_table_meta(ITableMeta *table_meta) = 0;
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    class ObLogDBNameBuilder : public IObLogDBNameBuilder
    {
      typedef ObLogStringMap<const char*> NameMap;
      static const int64_t NAME_MAP_SIZE = 128;
      public:
        ObLogDBNameBuilder();
        ~ObLogDBNameBuilder();
      public:
        int init(const ObLogConfig &config);
        void destroy();
        int get_db_name(const char *src_name,
            const uint64_t db_partition,
            char *dest_name,
            const int64_t dest_buffer_size);
      public:
        int operator ()(const char *tb_name, const ObLogConfig *config);
      private:
        bool inited_;
        common::CharArena allocator_;
        NameMap name_map_;
    };

    class ObLogTBNameBuilder : public IObLogTBNameBuilder
    {
      typedef ObLogStringMap<const char*> NameMap;
      static const int64_t NAME_MAP_SIZE = 128;
      public:
        ObLogTBNameBuilder();
        ~ObLogTBNameBuilder();
      public:
        int init(const ObLogConfig &config);
        void destroy();
        int get_tb_name(const char *src_name,
            const uint64_t tb_partition,
            char *dest_name,
            const int64_t dest_buffer_size);
      public:
        int operator ()(const char *tb_name, const ObLogConfig *config);
      private:
        bool inited_;
        common::CharArena allocator_;
        NameMap name_map_;
    };

    class ObLogMetaManager : public IObLogMetaManager
    {
      struct TableMetaKey
      {
        uint64_t table_id;
        uint64_t db_partition;
        uint64_t tb_partition;
        TableMetaKey() : table_id(common::OB_INVALID_ID), db_partition(0), tb_partition(0)
        {
        };
        TableMetaKey(const uint64_t t, const uint64_t dp, const uint64_t tp) : table_id(t), db_partition(dp), tb_partition(tp)
        {
        };
        int64_t hash() const
        {
          return common::murmurhash2(this, sizeof(*this), 0);
        };
        bool operator== (const TableMetaKey &other) const
        {
          return (other.table_id == table_id && other.db_partition == db_partition && other.tb_partition == tb_partition);
        };
        const char *to_cstring() const
        {
          static const int64_t BUFFER_SIZE = 64;
          static __thread char buffers[2][BUFFER_SIZE];
          static __thread uint64_t i = 0;
          char *buffer = buffers[i++ % 2];
          buffer[0] = '\0';
          snprintf(buffer, BUFFER_SIZE, "table_id=%lu db_partition=%lu tb_partition=%lu", table_id, db_partition, tb_partition);
          return buffer;
        };
      };

      public:
        typedef common::hash::ObHashMap<TableMetaKey, ITableMeta*> TableMetaMap;

        static const std::string &META_DATA_COLLECTION_TYPE;
        static const std::string &DB_META_TYPE;
        static const std::string &TABLE_META_TYPE;
        static const std::string &COL_META_TYPE;
        static const int64_t MAX_TABLE_META_ENTRY_NUM = static_cast<int64_t>(MAX_TB_PARTITION_NUM * DEFAULT_MAX_TABLE_NUM);
        static const int32_t MAX_RESERVED_VERSION_COUNT = 5;

      public:
        static int get_meta_manager(IObLogMetaManager *&meta_manager,
            IObLogSchemaGetter *schema_getter,
            IObLogDBNameBuilder *db_name_builder,
            IObLogTBNameBuilder *tb_name_builder);

      public:
        ObLogMetaManager();
        ~ObLogMetaManager();
      public:
        bool is_inited() const { return inited_; }

        int init(IObLogSchemaGetter *schema_getter,
            IObLogDBNameBuilder *db_name_builder,
            IObLogTBNameBuilder *tb_name_builder);
        void destroy();

        /// @brief Get table meta based on specific schema
        /// @param[out] table_meta returned table meta
        /// @param[in] table_id target table ID
        /// @param[in] db_partition database partition number
        /// @param[in] tb_partition table partition number
        /// @param[in] total_schema specific schema
        /// @retval OB_SUCCESS on success
        /// @retval OB_ERR_INVALID_SCHEMA when specific schema's version is less than meta version
        /// @retval ! OB_SUCCESS and ! OB_ERR_INVALID_SCHEMA on other errors
        int get_table_meta(ITableMeta *&table_meta,
            const uint64_t table_id,
            const uint64_t db_partition,
            const uint64_t tb_partition,
            const ObLogSchema *total_schema);

        /// @brief Revert table meta
        /// @param table_meta target table meta
        void revert_table_meta(ITableMeta *table_meta);
      private:
        /// @brief Create IMetaDataCollections
        /// @param[out] meta_collect returned IMetaDataCollections object pointer
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int create_meta_collect_(IMetaDataCollections *&meta_collect) const;

        /// @brief Destroy meta data of all versions
        void destroy_meta_data_of_all_versions_();

        /// @brief Destroy meta data of one specific version
        /// @param meta_collect meta data of target version
        /// @note all meta data in IMetaDataCollections will be destroyed:
        ///       1. IDBMeta 2. ITableMeta 3. IColMeta 4. IMetaDataCollections
        void destroy_meta_data_of_one_version_(IMetaDataCollections *meta_collect) const;

        /// @brief Update meta data version to target version
        /// @param target_version target version
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int update_meta_version_(int64_t target_version);

        /// @brief Recycle meta data to destroy unreferenced versions of meta data
        void recycle_meta_data_();

        /// @brief Decrease reference of meta data of specific version
        /// @param meta_collect meta data of specific version
        void dec_ref_(IMetaDataCollections *meta_collect);

        /// @brief Increase refrence of meta data of specific version
        /// @param meta_collect meta data of specific version
        void inc_ref_(IMetaDataCollections *meta_collect);

        /// @brief Build ITableMeta of specific version
        /// @param[out] table_meta returned ITableMeta object pointer
        /// @param[in] table_meta_key the key to build table meta
        /// @param[in] total_schema schema of specific version
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int build_table_meta_(ITableMeta *&table_meta,
            const TableMetaKey &table_meta_key,
            const ObLogSchema *total_schema);

        /// @brief Get IDBMeta
        /// @param[out] db_meta returned IDBMeta object pointer
        /// @param[in] db_partition_name database partition name used retrieve IDBMeta
        /// @param[out] exist returned bool value to represent whether IDBMeta is already existed
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int get_db_meta_(IDBMeta *&db_meta, const char *db_partition_name, bool &exist);

        /// @brief Get ITableMeta
        /// @param[out] table_meta returned ITableMeta object pointer
        /// @param[in] db_partitioin_name database partition name used to retrieve ITableMeta
        /// @param[in] table_partition_name table partition name used to retrieve ITableMeta
        /// @param[out] exist returned bool value to represent whether ITableMeta is already existed
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int get_table_meta_(ITableMeta *&table_meta,
            const char *db_partition_name,
            const char *table_partition_name,
            bool &exist);

        /// @brief Get database and table partition name
        /// @param table_meta_key the key used to build database and table name
        /// @param table_name original table name
        /// @param db_partition_name_buffer database partition name buffer
        /// @param db_partition_name_buffer_size database partition name buffer size
        /// @param tb_partition_name_buffer table partition name buffer
        /// @param tb_partition_name_buffer_size table partition name buffer size
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int get_db_and_tb_partition_name_(const TableMetaKey &table_meta_key,
            const char *table_name,
            char *db_partition_name_buffer, const int64_t db_partition_name_buffer_size,
            char *tb_partition_name_buffer, const int64_t tb_partition_name_buffer_size);

        /// @brief Prepare column meta data for table meta based on specific schema
        /// @param total_schema specific schema
        /// @param table_schema specific table schema
        /// @param table_meta target table meta
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int prepare_table_column_schema_(const ObLogSchema &total_schema,
            const common::ObTableSchema &table_schema,
            ITableMeta &table_meta);

        int type_trans_mysql_(const common::ObObjType ob_type, int &mysql_type);
      private:
        bool inited_;
        int64_t version_;
        int32_t reserved_version_count_;

        TableMetaMap table_meta_map_;
        IMetaDataCollections *meta_collect_;
        common::ObSpinLock meta_collect_lock_;
        IObLogDBNameBuilder *db_name_builder_;
        IObLogTBNameBuilder *tb_name_builder_;
    };
  }
}

#endif //OCEANBASE_LIBOBLOG_META_MANAGER_H_

