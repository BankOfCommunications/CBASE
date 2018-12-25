#include "ob_sstable_schema_cache.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    int ObSSTableSchemaCache::clear()
    {
      int ret = OB_SUCCESS;

      rwlock_.wrlock();
      if (0 < schema_cnt_)
      {
        for (int64_t i = 0; i < schema_cnt_; ++i)
        {
          if (0 != schema_array_[i].ref_cnt_)
          {
            TBSYS_LOG(WARN, "schema array ref cnt !=0");
            ret = OB_ERROR;
          }

          if (NULL != schema_array_[i].schema_)
          {
            schema_array_[i].schema_->~ObSSTableSchema();
            ob_free(reinterpret_cast<void*>(schema_array_[i].schema_));
          }
        }
      }
      schema_cnt_ = 0;
      rwlock_.unlock();

      return ret;
    }

    ObSSTableSchema* ObSSTableSchemaCache::get_schema(
        const uint64_t table_id, const int64_t version)
    {
      ObSSTableSchema* schema = NULL;
      int64_t index           = 0;
      
      if (OB_INVALID_ID == table_id || 0 == table_id || version < 0)
      {
        TBSYS_LOG(WARN, "table_id error"); 
      }
      else
      {
        rwlock_.rdlock();
        index = find_schema_node_index(table_id, version);
        if (0 <= index)
        {
          schema = schema_array_[index].schema_;
          if (NULL != schema)
          {
            schema_array_[index].inc_ref_cnt();
          }
          else
          {
            TBSYS_LOG(WARN, "schem==NULL");
          }
        }
        rwlock_.unlock();
      }

      return schema;
    }

    int ObSSTableSchemaCache::add_schema(ObSSTableSchema* schema, 
        const uint64_t table_id, const int64_t version)
    {
      int ret       = OB_SUCCESS;
      int64_t index = 0;
      int64_t i     = 0;
      
      if (NULL == schema || OB_INVALID_ID == table_id 
          || 0 == table_id || version < 0)
      {
        TBSYS_LOG(WARN, "invalid argument");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (schema_cnt_ >= MAX_SCHEMA_VER_COUNT)
      {
        TBSYS_LOG(WARN, "schema nct >=MAX");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        rwlock_.wrlock();
        index = find_schema_node_index(table_id, version);
        if (index >= 0)
        {
          TBSYS_LOG(INFO, "find schema node index error");
          ret = OB_ENTRY_EXIST;
        }

        if (OB_SUCCESS == ret)
        {
          i = upper_bound_index(table_id, version);
          if (i < schema_cnt_)
          {
            memmove(&schema_array_[i + 1], &schema_array_[i], 
                    sizeof(ObSSTableSchemaNode) * (schema_cnt_ - i));
          }
  
          schema_array_[i].reset();
          schema_array_[i].table_id_ = table_id;
          schema_array_[i].schema_ = schema;
          schema_array_[i].schema_ver_ = version;
          schema_array_[i].inc_ref_cnt();
          ++schema_cnt_;
        }
        rwlock_.unlock();
      }

      return ret;
    }
    
    int ObSSTableSchemaCache::revert_schema(const uint64_t table_id, 
        const int64_t version)
    {
      int ret                     = OB_SUCCESS;
      int64_t index               = 0;
      int64_t ref_cnt             = 0;
      ObSSTableSchema* free_schema = NULL;
      
      if (OB_INVALID_ID == table_id || 0 == table_id || version < 0)
      {
        TBSYS_LOG(WARN, "table id error:table_id=%lu", table_id);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        rwlock_.wrlock();
        index = find_schema_node_index(table_id, version);
        if (index < 0)
        {
          TBSYS_LOG(WARN, "find schema node index error");
          ret = OB_ERROR;
        }
        else
        {
          ref_cnt = schema_array_[index].dec_ref_cnt();
          if (0 == ref_cnt && NULL != schema_array_[index].schema_)
          {
            free_schema = schema_array_[index].schema_;
            if (index < schema_cnt_ - 1)
            {
              memmove(&schema_array_[index], &schema_array_[index + 1], 
                      sizeof(ObSSTableSchemaNode) * (schema_cnt_ - index));
            }
            --schema_cnt_;
          }
          else if (ref_cnt < 0 || NULL == schema_array_[index].schema_)
          {
            TBSYS_LOG(WARN, "ref cnt <0");
            ret = OB_ERROR;
          }
        }
        rwlock_.unlock();
      }

      if (NULL != free_schema)
      {
        free_schema->~ObSSTableSchema();
        ob_free(reinterpret_cast<void*>(free_schema));
      }

      return ret;
    }

    int64_t ObSSTableSchemaCache::find_schema_node_index(
        const uint64_t table_id, const int64_t version) const
    {
      int64_t ret     = -1;
      int64_t left    = 0;
      int64_t middle  = 0;
      int64_t right   = schema_cnt_ - 1;

      while (left <= right)
      {
        middle = (left + right) / 2;
        if (schema_array_[middle].compare(table_id, version) > 0)
        {
          right = middle - 1;
        }
        else if (schema_array_[middle].compare(table_id, version) < 0)
        {
          left = middle + 1;
        }
        else
        {
          ret = middle;
          break;
        }
      }

      return ret;
    }

    int64_t ObSSTableSchemaCache::upper_bound_index(
        const uint64_t table_id, const int64_t version) const
    {
      int64_t left    = 0;
      int64_t middle  = 0;
      int64_t right   = schema_cnt_;

      while (left < right)
      {
        middle = (left + right) / 2;
        if (schema_array_[middle].compare(table_id, version) > 0)
        {
          right = middle;
        }
        else
        {
          left = middle + 1;
        }
      }

      return left;
    }
  }//end namespace compactsstablev2
}//end namespace oceanbase
