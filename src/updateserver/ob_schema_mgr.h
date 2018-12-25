////===================================================================
 //
 // ob_schema_mgr.h updateserver / Oceanbase
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

#ifndef  OCEANBASE_UPDATESERVER_SCHEMA_MGR_H_
#define  OCEANBASE_UPDATESERVER_SCHEMA_MGR_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "ob_atomic.h"
#include "common/ob_define.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_schema.h"
#include "common/page_arena.h"
#include "common/ob_spin_rwlock.h"
#include "sstable/ob_sstable_schema.h"
#include "ob_ups_utils.h"

#define DEFAULT_COLUMN_GROUP_ID 0 

namespace oceanbase
{
  namespace updateserver
  {
    class SchemaMgrImp
    {
      struct SchemaInfo
      {
        common::ObSchema schema;
        uint64_t create_time_column_id;
        uint64_t modify_time_column_id;
        SchemaInfo()
          : schema(),
          create_time_column_id(common::OB_INVALID_ID),
          modify_time_column_id(common::OB_INVALID_ID)
        {
        };
      };
      static const int64_t SCHEMA_HASH_SIZE = 1024;
      typedef common::hash::ObHashMap<common::ObString, SchemaInfo*, common::hash::NoPthreadDefendMode> name_map_t;
      typedef common::hash::ObHashMap<uint64_t, SchemaInfo*, common::hash::NoPthreadDefendMode> id_map_t;
      public:
        SchemaMgrImp();
        ~SchemaMgrImp();
      public:
        int set_schemas(const ObSchemaManagerWrapper &schema_manager);
        const common::ObSchemaManager &get_com_schema_mgr() const;
        common::ObSchema *get_schema(const common::ObString &table_name) const;
        common::ObSchema *get_schema(const uint64_t table_id) const;
        uint64_t get_create_time_column_id(const uint64_t table_id) const;
        uint64_t get_modify_time_column_id(const uint64_t table_id) const;
        void dump2text() const;
        int build_sstable_schema(sstable::ObSSTableSchema &sstable_schema) const;
        int64_t inc_ref_cnt()
        {
          return atomic_inc((uint64_t*)&ref_cnt_);
        };
        int64_t dec_ref_cnt()
        {
          return atomic_dec((uint64_t*)&ref_cnt_);
        };
        int64_t get_ref_cnt()
        {
          return ref_cnt_;
        };
      private:
        static void analyse_column_(const common::ObSchema &schema, SchemaInfo &schema_info);
        int set_schema_(const common::ObSchema &schema);
        void clear_();
      private:
        int64_t ref_cnt_;
        name_map_t name_map_;
        id_map_t id_map_;
        common::ObSchemaManager com_schema_mgr_;
    };

    class SchemaMgr
    {
      public:
        typedef SchemaMgrImp *SchemaHandle;
        static const SchemaHandle INVALID_SCHEMA_HANDLE;
      public:
        SchemaMgr();
        ~SchemaMgr();
        DISALLOW_COPY_AND_ASSIGN(SchemaMgr);
      public:
        int set_schemas(const ObSchemaManagerWrapper &schema_manager);
        int get_schemas(ObSchemaManagerWrapper &schema_manager);
        common::ObSchema *get_schema(const common::ObString &table_name, SchemaHandle &schema_handle) const;
        common::ObSchema *get_schema(const uint64_t table_id, SchemaHandle &schema_handle) const;
        uint64_t get_create_time_column_id(const uint64_t table_id) const;
        uint64_t get_modify_time_column_id(const uint64_t table_id) const;
        void revert_schema_handle(SchemaHandle &schema_handle);
        SchemaHandle get_schema_handle();
        int build_sstable_schema(const SchemaHandle schema_handle, sstable::ObSSTableSchema &sstable_schema) const;
        uint64_t get_create_time_column_id(const SchemaHandle schema_handle, const uint64_t table_id) const;
        uint64_t get_modify_time_column_id(const SchemaHandle schema_handle, const uint64_t table_id) const;
        void dump2text() const;
        bool has_schema() const {return has_schema_;}
      private:
        SchemaMgrImp *cur_schema_mgr_imp_;
        mutable common::SpinRWLock rwlock_;
        bool has_schema_;
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_SCHEMA_MGR_H_

