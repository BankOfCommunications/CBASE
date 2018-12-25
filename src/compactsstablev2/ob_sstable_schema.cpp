#include "ob_sstable_schema.h"
#include "common/ob_schema.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    ObSSTableSchema::ObSSTableSchema()
      : column_def_(column_def_array_),
        cur_head_(0),
        hash_init_(false)
    {
      memset(&schema_header_, 0, OB_SSTABLE_SCHEMA_HEADER_SIZE);
      memset(column_def_array_, 0,
          OB_MAX_COLUMN_NUMBER * OB_SSTABLE_SCHEMA_COLUMN_DEF_SIZE);
    }

    ObSSTableSchema::~ObSSTableSchema()
    {
      if (NULL != column_def_ && column_def_array_ != column_def_)
      {
        ob_free(column_def_);
        column_def_ = NULL;
      }

      if (hash_init_)
      {
        table_schema_hashmap_.destroy();
        table_column_schema_hashmap_.destroy();
      }
    }

    void ObSSTableSchema::reset()
    {
      memset(&schema_header_, 0, OB_SSTABLE_SCHEMA_HEADER_SIZE);
      memset(column_def_array_, 0,
          OB_MAX_COLUMN_NUMBER * OB_SSTABLE_SCHEMA_COLUMN_DEF_SIZE);

      cur_head_ = 0;

      if (NULL != column_def_ && column_def_array_ != column_def_)
      {
        ob_free(column_def_);
        column_def_ = column_def_array_;
      }

      if (hash_init_)
      {
        table_schema_hashmap_.clear();
        table_column_schema_hashmap_.clear();
      }
    }

    const ObSSTableSchemaColumnDef* ObSSTableSchema::get_table_schema(
        const uint64_t table_id, const bool is_rowkey_column,
        int64_t& size) const
    {
      ObSSTableSchemaColumnDef* ret = NULL;

      if (OB_INVALID_ID == table_id || 0 == table_id)
      {
        TBSYS_LOG(WARN, "table id error:table_id =%lu", table_id);
      }
      else if (0 == get_column_count())
      {
        TBSYS_LOG(WARN, "column count = 0");
      }
      else if (hash_init_)
      {
        TableKey key;
        key.table_id_ = table_id;
        key.is_rowkey_ = is_rowkey_column;
        TableValue value;
        int hash_get = 0;
        int64_t offset = 0;
        hash_get = table_schema_hashmap_.get(key, value);
        if (-1 == hash_get)
        {
          TBSYS_LOG(ERROR, "hash_get = -1");
        }
        else if (hash::HASH_NOT_EXIST == hash_get)
        {
          size = 0;
        }
        else if (hash::HASH_EXIST == hash_get)
        {
          size = value.size_;
          offset = value.offset_;

          if (0 > offset || get_column_count() <= offset)
          {
            TBSYS_LOG(ERROR, "get offset error");
          }

          ret = column_def_ + value.offset_;
        }
        else
        {
          TBSYS_LOG(ERROR, "can not go there");
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "hash is not init");
      }

      return ret;
    }

    const ObSSTableSchemaColumnDef* ObSSTableSchema::get_column_def(
        const uint64_t table_id, const uint64_t column_id) const
    {
      ObSSTableSchemaColumnDef* def = NULL;

      int64_t offset = find_column_offset(table_id, column_id);
      if (0 <= offset)
      {
        def = column_def_ + offset;
      }

      return def;
    }

    bool ObSSTableSchema::is_table_exist(const uint64_t table_id) const
    {
      bool ret = false;

      if (OB_INVALID_ID == table_id || 0 == table_id)
      {
        TBSYS_LOG(WARN, "table id error");
      }
      else if (0 == get_column_count())
      {
        TBSYS_LOG(WARN, "column count = 0");
      }
      else if (!hash_init_)
      {
        TBSYS_LOG(WARN, "hash init = 0");
      }
      else
      {
        TableKey key;
        key.table_id_ = table_id;
        key.is_rowkey_ = true;
        TableValue value;
        int hash_get = table_schema_hashmap_.get(key, value);
        if (-1 == hash_get)
        {
          TBSYS_LOG(ERROR, "hash get == -1");
        }
        else if (hash::HASH_NOT_EXIST == hash_get)
        {
        }
        else if (hash::HASH_EXIST == hash_get)
        {
          ret = true;
        }
      }

      return ret;
    }

    bool ObSSTableSchema::is_column_exist(const uint64_t table_id,
        const uint64_t column_id) const
    {
      bool ret = false;

      int64_t offset = find_column_offset(table_id, column_id);
      if (0 <= offset)
      {
        ret = true;
      }

      return ret;
    }

    int ObSSTableSchema::add_column_def(const ObSSTableSchemaColumnDef& column_def)
    {
      int ret = OB_SUCCESS;

      int64_t size = get_column_count();
      ObSSTableSchemaColumnDef def = column_def;

      if (DEFAULT_COLUMN_DEF_ARRAY_CNT <= size)
      {
        TBSYS_LOG(WARN, "is largert than the column count");
        ret = OB_ERROR;
      }
      else
      {
        ObSSTableSchemaColumnDefCompare compare;
        if (0 < size && false == compare(column_def_[size - 1], def))
        {
          TBSYS_LOG(WARN, "compare error");
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        int64_t offset = find_column_offset(def.table_id_, def.column_id_);
        if (offset >= 0)
        {
          TBSYS_LOG(WARN, "find column offset error");
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret && !hash_init_)
      {
        if (OB_SUCCESS !=(ret = create_hashmaps()))
        {
          TBSYS_LOG(ERROR, "create hashmap error");
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (common::OB_MAX_COLUMN_NUMBER <= size
            && column_def_ == column_def_array_)
        {
          if (NULL == (column_def_ =
                reinterpret_cast<ObSSTableSchemaColumnDef*>(
                  ob_malloc(OB_SSTABLE_SCHEMA_COLUMN_DEF_SIZE
                            * DEFAULT_COLUMN_DEF_ARRAY_CNT, ObModIds::OB_SSTABLE_SCHEMA))))
          {
            TBSYS_LOG(WARN, "malloc memory error");
            ret = OB_ERROR;
          }
          else
          {
            memcpy(column_def_, column_def_array_,
                OB_SSTABLE_SCHEMA_COLUMN_DEF_SIZE * OB_MAX_COLUMN_NUMBER);
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (0 == size)
        {
          cur_head_ = size;
        }
        else if (column_def_[size - 1].table_id_ != def.table_id_
            || column_def_[size - 1].is_rowkey_column() != def.is_rowkey_column())
        {
          cur_head_ = size;
        }
      }

      if (OB_SUCCESS == ret)
      {
        def.offset_ = size - cur_head_;
      }

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = update_hashmaps(def)))
        {
          TBSYS_LOG(WARN, "update hashmaps error:ret=%d"
              "def.table_id_=%lu,def.column_id_=%lu,def.column_value_type_=%d"
              "def.offset_=%ld,def.rowkey_seq_=%ld", ret,
              def.table_id_, def.column_id_, def.column_value_type_,
              def.offset_, def.rowkey_seq_);
        }
      }

      if (OB_SUCCESS == ret)
      {
        column_def_[schema_header_.column_count_] = def;
        schema_header_.column_count_ ++;
      }

      return ret;
    }

    const ObSSTableSchemaColumnDef* ObSSTableSchema::get_rowkey_column(
        const uint64_t table_id, const int64_t rowkey_seq) const
    {
      const ObSSTableSchemaColumnDef* ret = NULL;
      const ObSSTableSchemaColumnDef* def = NULL;

      if (OB_INVALID_ID == table_id || 0 == table_id)
      {
        TBSYS_LOG(ERROR, "table id error");
      }
      else
      {
        bool is_rowkey_column = true;
        int64_t size = 0;
        def = get_table_schema(table_id, is_rowkey_column, size);
        if (NULL == def)
        {
          TBSYS_LOG(ERROR, "get table schema error");
        }
        else
        {
          for (int64_t i = 0; i < size; i ++)
          {
            if (rowkey_seq == def[i].rowkey_seq_)
            {
              ret = def + i;
              break;
            }
          }
        }
      }

      return ret;
    }

    const ObSSTableSchemaColumnDef* ObSSTableSchema::get_rowvalue_column(
        const uint64_t table_id, const int64_t rowvalue_seq) const
    {
      const ObSSTableSchemaColumnDef* ret = NULL;
      const ObSSTableSchemaColumnDef* def = NULL;

      if (OB_INVALID_ID == table_id || 0 == table_id)
      {
        TBSYS_LOG(ERROR, "table id error");
      }
      else
      {
        bool is_rowkey_column = false;
        int64_t size = 0;
        def = get_table_schema(table_id, is_rowkey_column, size);
        if (NULL == def)
        {
          TBSYS_LOG(ERROR, "def == NULL");
        }
        else if (rowvalue_seq > size)
        {
          TBSYS_LOG(WARN, "the rowvalue_seq error:rowvalue_seq=%ld,size=%ld",
              rowvalue_seq, size);
        }
        else
        {//从1开始算
          ret = def + rowvalue_seq - 1;
        }
      }

      return ret;
    }

    const int ObSSTableSchema::get_rowkey_column_count(
        const uint64_t table_id, int64_t& column_count) const
    {
        int ret = OB_SUCCESS;
        const ObSSTableSchemaColumnDef* def = NULL;
        bool is_rowkey_column = true;
        def = get_table_schema(table_id, is_rowkey_column, column_count);
        return ret;
    }

    int ObSSTableSchema::operator=(const ObSSTableSchema& schema)
    {
      int ret = common::OB_SUCCESS;

      const ObSSTableSchemaColumnDef* def = NULL;
      int64_t column_num = schema.get_column_count();

      for (int64_t i = 0; i < column_num; i ++)
      {
        if(NULL == (def = schema.get_column_def(i)))
        {
          TBSYS_LOG(WARN, "get oclumn def error");
          break;
        }
        else if (OB_SUCCESS != (ret = add_column_def(*def)))
        {
          TBSYS_LOG(WARN, "add column def error");
          break;
        }
      }

      return ret;
    }

    int ObSSTableSchema::check_row(const uint64_t table_id,
        const ObRowkey& rowkey, const ObRow& rowvalue)
    {
      int ret = OB_SUCCESS;
      const int64_t rowkey_obj_cnt = rowkey.length();
      const int64_t rowvalue_obj_cnt = rowvalue.get_column_num();
      ObSSTableSchemaColumnDef* def = NULL;
      int64_t size = 0;

      if (NULL == (def = const_cast<ObSSTableSchemaColumnDef*>(
              get_table_schema(table_id, true, size))))
      {
        TBSYS_LOG(WARN, "get table schema error:table_id=%lu,size=%ld",
            table_id, size);
        ret = OB_ERROR;
      }
      else if (size != rowkey_obj_cnt)
      {
        TBSYS_LOG(WARN, "rowkey obj cnt is not equal:size=%ld,"
            "rowkey_obj_cnt=%ld", size, rowkey_obj_cnt);
        ret = OB_ERROR;
      }
      else
      {
        const ObObj* rowkey_obj_ptr = rowkey.ptr();
        for (int64_t i = 0; i < rowkey_obj_cnt; i ++)
        {
          if (def[i].column_value_type_ != rowkey_obj_ptr[i].get_type())
          {
            TBSYS_LOG(WARN, "value type is not match:i=%ld,"
                "def[i].column_value_type_=%d,rowkey_obj_ptr[i].get_type()=%d",
                i, def[i].column_value_type_, rowkey_obj_ptr[i].get_type());
            ret = OB_ERROR;
            break;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (NULL == (def = const_cast<ObSSTableSchemaColumnDef*>
              (get_table_schema(table_id, false, size))))
        {
          TBSYS_LOG(WARN, "get table schema error:table_id=%lu,size=%ld",
              table_id, size);
          ret = OB_ERROR;
        }
        else if (size != rowvalue_obj_cnt)
        {
          TBSYS_LOG(WARN, "rowvalue obj cnt not match:size=%ld,"
              "rowvalue_obj_cnt=%ld", size, rowvalue_obj_cnt);
          ret = OB_ERROR;
        }
        else
        {
          const ObObj* obj = NULL;
          uint64_t t_table_id = 0;
          uint64_t column_id = 0;
          for (int64_t i = 0; i < rowvalue_obj_cnt; i ++)
          {
            if (OB_SUCCESS != (ret = rowvalue.raw_get_cell(
                    i, obj, t_table_id, column_id)))
            {
              TBSYS_LOG(WARN, "rowvalue raw get cell error:ret=%d,i=%ld",
                  ret, i);
              break;
            }
            else
            {
              if (def[i].column_value_type_ != obj->get_type())
              {
                TBSYS_LOG(WARN, "rowvalue column value type not match:"
                    "def[i].column_value_type_=%d,obj->get_type()=%d",
                    def[i].column_value_type_, obj->get_type());
                ret = OB_ERROR;
                break;
              }
            }
          }//end for
        }
      }

      return ret;
    }

    int ObSSTableSchema::check_row(const uint64_t table_id,
        const ObRow& row)
    {
      int ret = OB_SUCCESS;
      const int64_t rowkey_obj_cnt
        = row.get_row_desc()->get_rowkey_cell_count();
      const int64_t row_obj_cnt = row.get_column_num();
      const int64_t rowvalue_obj_cnt = row_obj_cnt - rowkey_obj_cnt;
      ObSSTableSchemaColumnDef* def = NULL;
      int64_t size = 0;
      int64_t i = 0;
      const ObObj* obj = NULL;
      uint64_t t_table_id = 0;
      uint64_t column_id = 0;

      if (NULL == (def = const_cast<ObSSTableSchemaColumnDef*>(
              get_table_schema(table_id, true, size))))
      {
        TBSYS_LOG(WARN, "get table schema error:table_id=%lu,size=%ld",
            table_id, size);
        ret = OB_ERROR;
      }
      else if (size != rowkey_obj_cnt)
      {
        TBSYS_LOG(WARN, "rowkey obj cnt is not equal:size=%ld,"
            "rowkey_obj_cnt=%ld", size, rowkey_obj_cnt);
        ret = OB_ERROR;
      }
      else
      {
        for (i = 0; i < rowkey_obj_cnt; i ++)
        {
          if (OB_SUCCESS != (ret = row.raw_get_cell(i, obj,
                  t_table_id, column_id)))
          {
            char buf[1024];
            (*obj).to_string(buf, 1024);
            TBSYS_LOG(WARN, "row raw get cell error:ret=%d,i=%ld,obj=%s"
                "table_id=%lu,column_id=%lu",
                ret, i, buf, table_id, column_id);
            break;
          }
          else
          {
            if (def[i].column_value_type_ != obj->get_type())
            {
              TBSYS_LOG(WARN, "rowkey type is not match:i=%ld,"
                  "def[i].column_value_type_=%d,"
                  "obj->get_type()=%d",
                  i, def[i].column_value_type_, obj->get_type());
              ret = OB_ERROR;
              break;
            }
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (NULL == (def = const_cast<ObSSTableSchemaColumnDef*>
              (get_table_schema(table_id, false, size))))
        {
          TBSYS_LOG(WARN, "get table schema error:table_id=%lu,size=%ld",
              table_id, size);
          ret = OB_ERROR;
        }
        else if (size != rowvalue_obj_cnt)
        {
          TBSYS_LOG(WARN, "rowvalue obj cnt not match:size=%ld,"
              "rowvalue_obj_cnt=%ld", size, rowvalue_obj_cnt);
          ret = OB_ERROR;
        }
        else
        {
          for (i = 0; i < rowvalue_obj_cnt; i ++)
          {
            if (OB_SUCCESS != (ret = row.raw_get_cell(
                    i + rowkey_obj_cnt, obj, t_table_id, column_id)))
            {
              TBSYS_LOG(WARN, "rowvalue raw get cell error:ret=%d,i=%ld",
                  ret, i);
              break;
            }
            else
            {
              if (obj->get_type() != ObNullType && def[i].column_value_type_ != obj->get_type())
              {
                TBSYS_LOG(WARN, "rowvalue column value type not match:"
                    "obj[%ld].obj_type =%d, tid=%ld, cid=%ld,"
                    "def.cid=%ld, def.cvt=%d, def.offset=%ld, def.rs=%ld",
                    i+rowkey_obj_cnt,  obj->get_type(), t_table_id, column_id,
                    def[i].column_id_, def[i].column_value_type_, def[i].offset_, def[i].rowkey_seq_);
                ret = OB_ERROR;
                break;
              }
            }
          }//end for
        }
      }

      return ret;
    }

    int64_t ObSSTableSchema::to_string(char* buf,
        const int64_t buf_len) const
    {
      int64_t pos = 0;
      const int64_t column_count = schema_header_.column_count_;

      if (pos < buf_len)
      {
        databuff_printf(buf, buf_len, pos, "column_count=%ld:",
            column_count);
      }

      if (pos < buf_len)
      {
        databuff_printf(buf, buf_len, pos, "<i,table_id,column_id,"
            "column_value_type,offset,rowkey_seq>,");
      }

      if (pos < buf_len)
      {
        for (int64_t i = 0; i < column_count; i ++)
        {
          databuff_printf(buf, buf_len, pos, "<%ld,%lu,%lu,%d,%ld,%ld>",
              i, column_def_[i].table_id_, column_def_[i].column_id_,
              column_def_[i].column_value_type_, column_def_[i].offset_,
              column_def_[i].rowkey_seq_);
        }
      }

      return pos;
    }

    int64_t ObSSTableSchema::find_column_offset(const uint64_t table_id,
        const uint64_t column_id) const
    {
      int64_t ret = -1;

      if (OB_INVALID_ID == table_id || 0 == table_id)
      {
        TBSYS_LOG(WARN, "table id error");
      }
      else if (OB_INVALID_ID == column_id)
      {
        TBSYS_LOG(WARN, "column id error");
      }
      else
      {
        int hash_get = 0;
        TableColumnKey key;
        key.table_id_ = table_id;
        key.column_id_ = column_id;
        TableColumnValue value;
        hash_get = table_column_schema_hashmap_.get(key, value);
        if (-1 == hash_get)
        {
         // TBSYS_LOG(INFO, "hash get failed:hash_get=%d, key.table_id_=%lu,"
         //     "key.column_=%lu", hash_get, key.table_id_, key.column_id_);
        }
        else if (hash::HASH_NOT_EXIST == hash_get)
        {
        }
        else if(hash::HASH_EXIST == hash_get)
        {
          ret = value.offset_;
        }
      }

      return ret;
    }

    int ObSSTableSchema::create_hashmaps()
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = table_schema_hashmap_.create(
              hash::cal_next_prime(OB_MAX_TABLE_NUMBER))))
      {
        TBSYS_LOG(WARN, "create schema hashmap error");
      }
      else if (OB_SUCCESS != (ret = table_column_schema_hashmap_.create(
              hash::cal_next_prime(512))))
      {
        TBSYS_LOG(WARN, "create table column schema hashmap error");
      }
      else
      {
        hash_init_ = true;
      }

      return ret;
    }

    int ObSSTableSchema::update_hashmaps(const ObSSTableSchemaColumnDef& column_def)
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = update_table_schema_hashmap(column_def)))
      {
        TBSYS_LOG(WARN, "update table schema hashmap error");
      }
      else if (OB_SUCCESS != (ret = update_table_column_schema_hashmap(column_def)))
      {
        TBSYS_LOG(WARN, "update table column schema hashmpa error");
      }

      return ret;
    }

    int ObSSTableSchema::update_table_schema_hashmap(const ObSSTableSchemaColumnDef& column_def)
    {
      int ret = OB_SUCCESS;

      if (!hash_init_)
      {
        TBSYS_LOG(WARN, "hash is not init");
        ret = OB_NOT_INIT;
      }
      else
      {
        TableKey key;
        key.table_id_ = column_def.table_id_;
        key.is_rowkey_ = column_def.is_rowkey_column();
        TableValue value;
        int64_t size = get_column_count();
        const ObSSTableSchemaColumnDef* last = NULL;
        if (0 < size)
        {
          last = &column_def_[size - 1];
        }

        int ret_hash = 0;
        if (0 == size || (NULL != last
              && (column_def.table_id_ != last->table_id_
                || column_def.is_rowkey_column() != last->is_rowkey_column())))
        {
          value.offset_ = size;
          value.size_ = 1;
          ret_hash = table_schema_hashmap_.set(key, value);

          if (-1 == ret_hash)
          {
            TBSYS_LOG(WARN, "table schema hash map set error:ret_hash=%d,"
                "key.table_id_=%lu,key.is_rowkey_=%d, value.offset_=%ld",
                ret_hash, key.table_id_, key.is_rowkey_, value.offset_);
            ret = OB_ERROR;
          }
        }
        else if (NULL != last && (column_def.table_id_ == last->table_id_
              && column_def.is_rowkey_column() == last->is_rowkey_column()))
        {
          ret_hash = table_schema_hashmap_.get(key, value);
          if (-1 == ret_hash || hash::HASH_NOT_EXIST == ret_hash)
          {
            TBSYS_LOG(ERROR, "table schema hashmap get errror:ret_hash=%d,"
                "key.table_id_=%lu,key.is_rowkey_=%d", ret_hash,
                key.table_id_, key.is_rowkey_);
            ret = OB_ERROR;
          }
          else if (hash::HASH_EXIST == ret_hash)
          {
            value.size_ ++;
            ret_hash = table_schema_hashmap_.set(key, value, 1);
            if (-1 == ret_hash)
            {
              TBSYS_LOG(WARN, "table schema hashmap set error:ret_hash=%d,"
                  "key.table_id_=%lu,key.is_rowkey_=%d,value.offset_=%ld",
                  ret_hash, key.table_id_, key.is_rowkey_, value.offset_);
              ret = OB_ERROR;
            }
          }
        }
        else
        {
          TBSYS_LOG(WARN, "ERROR");
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    int ObSSTableSchema::update_table_column_schema_hashmap(const ObSSTableSchemaColumnDef& column_def)
    {
      int ret = OB_SUCCESS;

      if (!hash_init_)
      {
        TBSYS_LOG(WARN, "hash not init");
        ret = OB_ERROR;
      }
      else
      {
        TableColumnKey key;
        key.table_id_ = column_def.table_id_;
        key.column_id_ = column_def.column_id_;
        TableColumnValue value;
        int64_t size = get_column_count();
        int ret_hash = 0;

        ret_hash = table_column_schema_hashmap_.get(key, value);
        if (-1 == ret_hash || hash::HASH_EXIST == ret_hash)
        {
          TBSYS_LOG(ERROR, "not exist");
          ret = OB_ERROR;
        }
        else if (hash::HASH_NOT_EXIST == ret_hash)
        {
          value.offset_ = size;

          ret_hash = table_column_schema_hashmap_.set(key, value);
          if (-1 == ret_hash)
          {
            TBSYS_LOG(ERROR, "hash not exist");
            ret = OB_ERROR;
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "can not go there");
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    int build_sstable_schema(const uint64_t new_table_id, const uint64_t schema_table_id,
        const common::ObSchemaManagerV2 & schema, ObSSTableSchema& sstable_schema)
    {
      int ret = OB_SUCCESS;
      int32_t size = 0;
      int64_t col_index = 0;
      ObSSTableSchemaColumnDef column_def;
      const ObColumnSchemaV2 *col = schema.get_table_schema(schema_table_id, size);
      const ObTableSchema* table_schema = schema.get_table_schema(schema_table_id);
      const ObRowkeyInfo & rowkey_info = table_schema->get_rowkey_info();

      memset(&column_def, 0, sizeof(column_def));

      if (NULL == col || size <= 0)
      {
        TBSYS_LOG(ERROR,"cann't find this table:%lu in schema", schema_table_id);
        ret = OB_ERROR;
      }
      else
      {
        const ObRowkeyColumn* column = NULL;
        // add rowkey column first;
        for (col_index = 0; col_index < rowkey_info.get_size() && OB_SUCCESS == ret; ++col_index)
        {
          column = rowkey_info.get_column(col_index);
          if (NULL == column)
          {
            TBSYS_LOG(ERROR, "schema internal error, rowkey column %ld not exist.", col_index);
            ret = OB_ERROR;
            break;
          }

          column_def.table_id_ = static_cast<uint32_t>(new_table_id);
          column_def.column_id_ = static_cast<uint16_t>(column->column_id_);
          column_def.column_value_type_ = column->type_;
          column_def.rowkey_seq_ = static_cast<uint16_t>(col_index + 1);

          if ( OB_SUCCESS != (ret = sstable_schema.add_column_def(column_def)) )
          {
            TBSYS_LOG(ERROR,"add column_def(%lu,%lu,%ld) failed col_index : %ld",column_def.table_id_,
                column_def.column_id_,column_def.rowkey_seq_, col_index);
          }
        }

        for (col_index = 0; col_index < size && OB_SUCCESS == ret; ++col_index)
        {
          if (rowkey_info.is_rowkey_column(col[col_index].get_id()))
          {
            // ignore rowkey columns;
            continue;
          }

          column_def.table_id_ = static_cast<uint32_t>(new_table_id);
          column_def.column_id_ = static_cast<uint16_t>(col[col_index].get_id());
          column_def.column_value_type_ = col[col_index].get_type();
          column_def.rowkey_seq_ = 0;

          if ( OB_SUCCESS != (ret = sstable_schema.add_column_def(column_def)) )
          {
            TBSYS_LOG(ERROR,"add column_def(%lu,%lu,%ld) failed col_index : %ld",column_def.table_id_,
                column_def.column_id_,column_def.rowkey_seq_, col_index);
          }
        }
      }

      return ret;
    }

    int build_sstable_schema(const uint64_t table_id,
        const common::ObSchemaManagerV2 & schema, ObSSTableSchema& sstable_schema)
    {
      return build_sstable_schema(table_id, table_id, schema, sstable_schema);
    }

    int build_sstable_schema(const common::ObSchemaManagerV2 & schema, ObSSTableSchema& sstable_schema)
    {
      int ret = OB_SUCCESS;
      ObSchemaSortByIdHelper helper(&schema);
      const ObSchemaSortByIdHelper::Item* item = helper.begin();
      const ObTableSchema* table_schema = NULL;
      for (; item != helper.end(); ++item)
      {
        if (NULL != item && NULL != (table_schema = helper.get_table_schema(item)))
        {
          ret = build_sstable_schema(table_schema->get_table_id(), schema, sstable_schema);
        }
        else
        {
          TBSYS_LOG(DEBUG, "build_sstable_schema error item=%p, table_schema=%p", item, table_schema);
          break;
        }
      }
      return ret;
    }

  }//end namesapce compactsstablev2
}
