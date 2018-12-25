/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_sstable_schema_cache.h for sstable schema cache. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *  
 * because sstable with the same version has the same sstable 
 * schema, so we can use this sstable schema cache to make 
 * sstable with the same version share the same sstable schema. 
 */
#ifndef OCEANBASE_SSTABLE_OB_SSTABLE_SCHEMA_CACHE_H_
#define OCEANBASE_SSTABLE_OB_SSTABLE_SCHEMA_CACHE_H_

#include "common/ob_atomic.h"
#include "common/ob_spin_rwlock.h"
#include "ob_sstable_schema.h"

namespace oceanbase 
{
  namespace sstable 
  {
    struct ObSchemaNode
    {
      ObSSTableSchema* schema_;
      uint64_t table_id_;
      int64_t schema_ver_;
      int64_t ref_cnt_;

      ObSchemaNode() : schema_(NULL), schema_ver_(0), ref_cnt_(0)
      {

      }

      inline void reset()
      {
        schema_ = NULL;
        table_id_ = common::OB_INVALID_ID;
        schema_ver_ = 0;
        ref_cnt_ = 0;
      }

      inline int compare(const uint64_t table_id, const int64_t version) const
      {
        int ret = 0;

        if (schema_ver_ > version)
        {
          ret = 1;
        }
        else if (schema_ver_ < version)
        {
          ret = -1;
        }
        else if (schema_ver_ == version)
        {
          if (table_id_ > table_id)
          {
            ret = 1;
          }
          else if (table_id_ < table_id)
          {
            ret = -1;
          }
          else
          {
            ret = 0;
          }
        }

        return ret;
      }

      inline int64_t inc_ref_cnt()
      {
        return common::atomic_inc(reinterpret_cast<uint64_t*>(&ref_cnt_));
      }

      inline int64_t dec_ref_cnt()
      {
        return common::atomic_dec(reinterpret_cast<uint64_t*>(&ref_cnt_));
      }
    };

    class ObSSTableSchemaCache
    {
    public:
      ObSSTableSchemaCache();
      ~ObSSTableSchemaCache();

      /**
       * get sstable schema by sstable version, this function will 
       * increase the reference of schema, when the application don't 
       * use the schema any more, would call revert_schema() to 
       * decrease the reference of schema. 
       *  
       * @param table_id table id of schema to get 
       * @param version sstable version
       * 
       * @return ObSSTableSchema* if success, return the existent 
       *         sstable schema, else return NULL
       */
      ObSSTableSchema* get_schema(const uint64_t table_id, const int64_t version);

      /**
       * add a new sstable schema into schema cache, for interanl 
       * implementation, the schema array is ordered by sstable 
       * version. 
       *  
       * @param table_id table id of schema to add 
       * @param schema sstable schema to add
       * @param version sstable version
       * 
       * @return int if success, return OB_SUCCESS, if this version 
       *         schema is existent, return OB_ENTRY_EXIST, else
       *         return OB_ERROR
       */
      int add_schema(ObSSTableSchema* schema, const uint64_t table_id, 
                     const int64_t version);

      /**
       * revert schema gotten by get_schema(), it will decrease the 
       * reference of schema 
       *  
       * @param table_id table id of schema to revert 
       * @param version sstable version
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int revert_schema(const uint64_t table_id, const int64_t version);

      int clear();
      int destroy();

    private:
      int ensure_schema_buf_space(const int64_t size);
      int64_t find_schema_node_index(const uint64_t table_id, const int64_t version) const;
      int64_t upper_bound_index(const uint64_t table_id, const int64_t version) const;

    private:
      static const int64_t SCHEMA_NODE_SIZE = sizeof(ObSchemaNode);
      static const int64_t DEFAULT_SCHEMA_BUF_SIZE = 
        common::OB_MAX_TABLE_NUMBER * SCHEMA_NODE_SIZE;

      DISALLOW_COPY_AND_ASSIGN(ObSSTableSchemaCache);

      ObSchemaNode* schema_array_;
      int64_t schema_buf_size_;
      int64_t schema_cnt_;
      common::SpinRWLock rwlock_;
    };
  } // namespace oceanbase::sstable
} // namespace Oceanbase

#endif //OCEANBASE_SSTABLE_OB_SSTABLE_SCHEMA_CACHE_H_
