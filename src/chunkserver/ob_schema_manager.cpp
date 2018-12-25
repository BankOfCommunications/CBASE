/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_schema_manager.cpp for manage multi-version schemas got 
 * form rootserver. 
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "common/ob_define.h"
#include "common/ob_schema.h"
#include "ob_schema_manager.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;

    ObMergerSchemaManager::ObMergerSchemaManager()
    {
      init_ = false;
      update_timestamp_ = 0;
      latest_version_ = -1;
    }
    
    ObMergerSchemaManager::~ObMergerSchemaManager()
    {
    
    }
    
    // init in main thread
    int ObMergerSchemaManager::init(const ObSchemaManagerV2 & schema)
    {
      int ret = OB_SUCCESS;
      if (true == init_)
      {
        TBSYS_LOG(WARN, "init schema manager twice");
        ret = OB_INIT_TWICE;
      }
      else
      {
        _schemas[0].ref_count = 1; 
        _schemas[0].schema = schema;
        update_timestamp_ = tbsys::CTimeUtil::getTime();
        latest_version_ = schema.get_version();
        TBSYS_LOG(INFO, "init schema manager succ:timestamp[%ld]", latest_version_);
        init_ = true;
      }
      return ret;
    }
    
    // schema version must incremental 
    int ObMergerSchemaManager::add_schema(const ObSchemaManagerV2 & schema, 
        const ObSchemaManagerV2 ** manager)
    {
      int ret = OB_SUCCESS;
      if (false == check_inner_stat())
      {
        TBSYS_LOG(WARN, "check inner stat or schema version failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        tbsys::CThreadGuard lock(&lock_);
        if (latest_version_ >= schema.get_version())
        {
          TBSYS_LOG(WARN, "check schema version failed:latest[%ld], schema[%ld]",
              latest_version_, schema.get_version());
          ret = OB_OLD_SCHEMA_VERSION;
        }
        else
        {
          // find replace position for the new schema
          // at first not inited, then not used and the oldest version pos
          int64_t pos = find_replace_pos();
          if (pos >= 0)
          {
            _schemas[pos].schema = schema;
            _schemas[pos].ref_count = 1;
            update_timestamp_ = tbsys::CTimeUtil::getTime();
            latest_version_ = schema.get_version();
            //
            if (NULL != manager)
            {
              _schemas[pos].ref_count = 2;
              *manager = &_schemas[pos].schema;
            }
          }
          else
          {
            ret = OB_NO_EMPTY_ENTRY;
          }
        }
      }
      return ret;
    }
    
    int64_t ObMergerSchemaManager::find_replace_pos(void) const
    {
      int64_t pos = -1;
      int64_t old_version = MAX_INT64_VALUE;
      for (uint64_t i = 0; i < MAX_VERSION_COUNT; ++i) 
      {
        // empty pos
        if (0 == _schemas[i].ref_count)
        {
            pos = i;
            break;
        }
        // not empty but not using right now
        else if (1 == _schemas[i].ref_count)
        {
          if (old_version > _schemas[i].schema.get_version())
          {
            old_version = _schemas[i].schema.get_version();
            pos = i;
          }
          continue;
        }
      }
      return pos;
    }
    
    const ObSchemaManagerV2 * ObMergerSchemaManager::get_schema(const int64_t version)
    {
      const ObSchemaManagerV2 * schema = NULL;
      if (false == check_inner_stat())
      {
        TBSYS_LOG(WARN, "check inner stat failed");
      }
      else
      {
        int64_t new_version = version;
        // get the latest version
        tbsys::CThreadGuard lock(&lock_);
        if (0 == version)
        {
          new_version = latest_version_; 
        }
        // 
        for (uint64_t i = 0; i < MAX_VERSION_COUNT; ++i)
        {
          if (0 == _schemas[i].ref_count)
          {
            continue;
          }
          else
          {
            if (_schemas[i].schema.get_version() == new_version)
            {
              ++_schemas[i].ref_count;
              schema = &_schemas[i].schema;
            }
          }
        }
      }
      return schema;
    }
    
    int ObMergerSchemaManager::release_schema(const int64_t version)
    {
      int ret = OB_RELEASE_SCHEMA_ERROR;
      if (false == check_inner_stat())
      {
        TBSYS_LOG(WARN, "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        tbsys::CThreadGuard lock(&lock_);
        for (uint64_t i = 0; i < MAX_VERSION_COUNT; ++i)
        {
          if (0 == _schemas[i].ref_count)
          {
            continue;
          }
          else
          {
            if (_schemas[i].schema.get_version() == version)
            {
              --_schemas[i].ref_count;
              ret = OB_SUCCESS;
            }
          }
        }
      }
      return ret;
    }
    
    int64_t ObMergerSchemaManager::get_oldest_version(void) const
    {
      int64_t version = MAX_INT64_VALUE;
      tbsys::CThreadGuard lock(&lock_);
      for (uint64_t i = 0; i < MAX_VERSION_COUNT; ++i)
      {
        if (0 == _schemas[i].ref_count)
        {
          continue;
        }
        else
        {
          if (_schemas[i].schema.get_version() < version)
          {
            version = _schemas[i].schema.get_version();
          }
        }
      }
      return version;
    }
    
    int ObMergerSchemaManager::print_info(void) const
    {
      int ret = OB_SUCCESS;
      if (false == check_inner_stat())
      {
        TBSYS_LOG(WARN, "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        tbsys::CThreadGuard lock(&lock_);
        TBSYS_LOG(INFO, "update[%ld], latest[%ld]", update_timestamp_, latest_version_);
        for (uint64_t i = 0; i < MAX_VERSION_COUNT; ++i)
        {
          if (0 == _schemas[i].ref_count)
          {
            continue;
          }
          else
          {
            TBSYS_LOG(INFO, "schema ref_count[%lu]", _schemas[i].ref_count);
            _schemas[i].schema.print_info();
          }
        }
      }
      return ret;
    }
  } // end namespace chunkserver
} // end namespace oceanbase
