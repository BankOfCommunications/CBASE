#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_SCHEMA_CACHE_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_SCHEMA_CACHE_H_

#include "common/ob_atomic.h"
#include "common/ob_spin_rwlock.h"
#include "ob_sstable_schema.h"

class TestSSTableSchemaCache_construct_Test;

namespace oceanbase 
{
  namespace compactsstablev2
  {
    struct ObSSTableSchemaNode
    {
      ObSSTableSchema* schema_;
      uint64_t table_id_;
      int64_t schema_ver_;
      int64_t ref_cnt_;

      ObSSTableSchemaNode() 
        : schema_(NULL), 
          schema_ver_(0), 
          ref_cnt_(0)
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
      friend class ::TestSSTableSchemaCache_construct_Test;

    public:
      ObSSTableSchemaCache()
        : schema_cnt_(0)
      {
        memset(schema_array_, 0, 
            sizeof(ObSSTableSchemaNode) * MAX_SCHEMA_VER_COUNT);
      }

      ~ObSSTableSchemaCache()
      {
        destroy();
      }

      int clear();

      int destroy()
      {
        return clear();
      }

      ObSSTableSchema* get_schema(const uint64_t table_id, 
          const int64_t version);

      int add_schema(ObSSTableSchema* schema, const uint64_t table_id, 
                     const int64_t version);

      int revert_schema(const uint64_t table_id, const int64_t version);

    private:
      int64_t find_schema_node_index(const uint64_t table_id, 
          const int64_t version) const;

      int64_t upper_bound_index(const uint64_t table_id, 
          const int64_t version) const;

    private:
      static const int64_t MAX_SCHEMA_VER_COUNT = 1024;
      ObSSTableSchemaNode schema_array_[MAX_SCHEMA_VER_COUNT];
      int64_t schema_cnt_;
      common::SpinRWLock rwlock_;
    };
  } // namespace oceanbase::sstable
} // namespace Oceanbase

#endif
