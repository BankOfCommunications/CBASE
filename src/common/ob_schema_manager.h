/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_schema_manager.h,v 0.1 2010/09/17 14:01:30 zhidong Exp $
 *
 * Authors:
 *   zhidong <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */


#ifndef OB_COMMON_SCHEMA_MANAGER_H_
#define OB_COMMON_SCHEMA_MANAGER_H_

#include <pthread.h>
#include "tbsys.h"
#include "common/ob_string.h"
#include "common/ob_schema.h"

namespace oceanbase
{
  namespace common
  {
    // warning: the schema manager contain MAX_VERSION_COUNT schema instance, if add a new schema
    // and fullfilled, then we have remove the oldest and not using by other thread, which read
    // schema through get_schema() interface, if not find available position, add_schema will failed.
    // In some case, if the new version is in-error state, then we maybe weed out the right old version.
    class ObMergerSchemaManager
    {
    public:
      ObMergerSchemaManager();
      virtual ~ObMergerSchemaManager();

    public:
      // max version
      static const uint64_t MAX_VERSION_COUNT = 4;

      // init the newest system or user table schema
      int init(const bool system, const common::ObSchemaManagerV2 & schema);

      // add a new schema, del the oldest-version and not used scheam if fullfilled
      int add_schema(const common::ObSchemaManagerV2 & schema,
          const common::ObSchemaManagerV2 ** manager = NULL);

      // get sys or user table schema of local newest version if not exist return null
      const common::ObSchemaManagerV2 * get_schema(const common::ObString & table_name);
      const common::ObSchemaManagerV2 * get_schema(const uint64_t table_id);

      // get user table schema of pointed version if not exist return null
      const common::ObSchemaManagerV2 * get_user_schema(const int64_t version);

      int release_schema(const common::ObSchemaManagerV2 * schema);

    public:
      // MAX INT64 VALUE
      static const int64_t MAX_INT64_VALUE = ((((uint64_t)1) << 63) - 1);

      // get the latest schema version
      inline int64_t get_latest_version(void) const;

      // get the oldest scheam version
      int64_t get_oldest_version(void) const;

      // get last update timestamp
      inline int64_t get_update_timestamp(void) const;

      // dump all schema info
      int print_info(void) const;

    private:
      // check inner stat
      inline bool check_inner_stat(void) const;

      // find a new pos for the new version schema
      int64_t find_replace_pos(void) const;

    private:
      bool init_;
      // last update timestamp
      int64_t update_timestamp_;
      int64_t latest_version_;

      // schema item
      typedef struct MergeServerSchemaItem
      {
        int64_t ref_count;
        common::ObSchemaManagerV2 schema;

        MergeServerSchemaItem():ref_count(0) {}
      } MergerSchemaItem;

      // reserve some version schema
      MergerSchemaItem user_schemas_[MAX_VERSION_COUNT];
      // root table schema only one version
      MergerSchemaItem sys_schema_;
      mutable tbsys::CThreadMutex lock_;
    };

    inline bool ObMergerSchemaManager::check_inner_stat(void) const
    {
      return (true == init_);
    }

    inline int64_t ObMergerSchemaManager::get_update_timestamp(void) const
    {
      tbsys::CThreadGuard lock(&lock_);
      return update_timestamp_;
    }

    inline int64_t ObMergerSchemaManager::get_latest_version(void) const
    {
      tbsys::CThreadGuard lock(&lock_);
      return latest_version_;
    }
  };
};




#endif //OB_COMMON_SCHEMA_MANAGER_H_
