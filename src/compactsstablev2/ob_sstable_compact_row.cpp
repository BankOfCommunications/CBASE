namespace oceanbase
{
  namespace compactsstablev2
  {
    ObCompactRow::ObCompactRow()
      : buf_(NULL),
        key_pos_(0),
        value_pos_(0),
        end_pos_(0),
        key_iter_pos_(0),
        value_iter_pos_(0),
        self_alloc_(false),
        store_type_(SPARSE),
        step_(false)
    {
    }

    ObCompactRow::~ObCompactRow()
    {
      if (self_alloc_)
      {
        ob_free(buf_);
      }
      buf_ = NULL;
    }

    int ObCompactRow::get_obj(common::ObObj& obj)
    {
      int ret = OB_SUCCESS;

      return ret;
    }

    int ObCompactRow::get_rowkey(common::ObRowkey& rowkey, commom::ObObj* rowkey_buf)
    {
      int ret = OB_SUCCESS;
      common::ObObj* obj_ptr = rowkey_buf;
      
      if (!key_setted)
      {
        TBSYS_LOG(ERROR, "");
        ret = OB_ERROR;
      }
      else if (value_pos_ - key_post_ <= 0)
      {
        TBSYS_LOG(ERROR, "");
        ret = OB_ERROR;
      }
      else
      {
        int rowkey_obj_count = 0;
        while (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = next_cell()))
          {
            TBSYS_LOG(WARN, "");
          }
          else if (OB_SUCCESS != (ret = get_cell(obj_ptr, &is_rowkey_end)))
          {
            TBSYS_LOG(WARN, "");
          }
          else if (!is_rowkey_end)
          {
            rowkey_obj_count ++;
            obj_ptr ++;
          }
          else
          {//rowkey finish
            rowkey->assign(rowkey_buf, rowkey_obj_count);
            break;
          }
        }//end while(OB_SUCCESS == ret)
      }

      return ret;
    }

    int ObCompactRow::set_rowkey(const common::ObRowKey& rowkey)
    {
      int ret = OB_SUCCESS;
      const ObObj* obj_ptr = rowkey.get_obj_ptr();
      int64_t obj_cnt = rowkey.get_obj_cnt();
      uint64_t table_id = OB_INVALID_ID;
      uint64_t column_id = OB_INVALID_ID;

      if (rowkey_setted_)
      {
        TBSYS_LOG(WARN, "");
        ret = OB_ERROR;
      }
      else if (rowvalue_setted_)
      {
        TBSYS_LOG(WARN, "");
        ret = OB_ERROR;
      }
      else if (NULL == buf_)
      {
        if (NULL == (buf_ = (char*)ob_malloc(MAX_BUF_SIZE)))
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          self_alloc_ = true;
        }
      }
      else
      {
        for (int64_t i = 0; i < obj_cnt; i ++)
        {
          if (OB_SUCCESS != (ret = append(*obj_ptr)))
          {
            TBSYS_LOG(WARN, "");
            break;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = set_rowkey_end()))
        {
          TBSYS_LOG(WARN, "");
        }
        else
        {
          rowkey_setted_ = true;
        }
      }

      return ret;
    }
    

    int ObCompactRow::set_rowvalue(const common::ObRow& rowvalue)
    {
      int ret = OB_SUCCESS;
      const ObObj* cell = NULL;
      int64_t obj_num = row.get_column_num();
      uint64_t table_id = OB_INVALID_ID;
      uint64_t column_id = OB_INVALID_ID;

      if (value_setted)
      {
        TBSYS_LOG(ERROR, "");
        ret = OB_ERROR;
      }
      else if (NULL == buf_)
      {
        if (NULL == (buf_ = (char*)ob_malloc(MAX_BUF_SIZE)))
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          self_alloc_ = true;
        }
      }
      else
      {
        for (int64_t i = 0; i < obj_num; i ++)
        {
          if (OB_SUCCESS != (ret = row.raw_get_cell(i, cell, table_id, column_id)))
          {
            TBSYS_LOG(WARN, "");
            break;
          }
          else if (OB_SUCCESS != (ret = append(column_id, *cell)))
          {
            TBSYS_LOG(WARN, "");
            break;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = set_rowvalue_end()))
        {
          TBSYS_LOG(WARN, "");
        }
        else
        {
          rowvalue_setted_ = true;
        }
      }

      return ret;
    }


    int ObCompactRow::assign_row(const char* buf, const int64_t key_size, const int64_t value_size)
    {
      int ret = OB_SUCCESS;

      if (rowkey_setted_)
      {
        TBSYS_LOG(WARN, "");
        ret = OB_SUCCESS;
      }
      else if (rowvalue_setted_)
      {
        TBSYS_LOG(WARN, "");
        ret = OB_ERROR;
      }
      else if (self_alloc_)
      {
        TBSYS_LOG(WARN, "");
        ret = OB_ERROR;
      }
      else
      {
        buf_ = buf;
        key_pos_ = buf_;
        value_pos_ = key_pos_ + key_size;
        end_pos_ = value_pos_ + value_size;

        if (0 < key_size)
        {
          rowkey_setted_ = true;
        }

        if (0 < value_size)
        {
          rowvalue_setted_ = true;
        }
      }

      return ret;
    }

    int ObCompactRow::next_cell()
    {
      int ret = OB_SUCCESS;

      if (!rowkey_inited && !rowvalue_inited)
      {
        TBSYS_LOG(WARN, "");
        ret = OB_ERROR;
      }
      else
      {
        ObCompactObjMeta* col_meta = reinterpret_cast<ObCompactObjMeta*>(buf_ + iter_pos_);
        int32_t follow_size = 0;
        if (OB_SUCCESS != (ret = get_follow_size(col_meta, follow_size)))
        {
          TBSYS_LOG(WARN, "");
        }
      }

      return ret;
    }
    

    int ObCompactRow::get_follow_size(const ObCompactObjMeta* meta, int32_t& follow_size)
    {
      int ret = OB_SUCCESS;
      int32_t size = 0;

      if (reinterpret_cast<const char*>(meta) > (buf_ + end_pos_))
      {
        TBSYS_LOG(WARN, "");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (is_end_mark(*meta))
      {
        ret = OB_ITER_END;
        follow_size = 0;
      }
      else
      {
        switch(meta->type_)
        {
        case COMPACT_OBJ_TYPE_NULL:
          size = 0;
          break;
        case COMPACT_OBJ_TYPE_INT8:
          size = 1;
          break;
        case COMPACT_OBJ_TYPE_INT16:
          size = 2;
          break;
        case COMPACT_OBJ_TYPE_INT32:
          size = 4;
          break;
        case COMPACT_OBJ_TYPE_INT64:
          size = 8;
          break;
        case COMPACT_OBJ_TYPE_PRECISE:
        case COMPACT_OBJ_TYPE_TIME:
        case COMPACT_OBJ_TYPE_MODIFY:
        case COMPACT_OBJ_TYPE_CREATE:
          size = 8;
          break;
        case COMPACT_OBJ_TYPE_VARCHAR:
          char* varchar_len = (char*)(meta + 1);
          if ((varchar_len + VARCHAR_LEN_TYPE_LEN) < buf_end)
          {
            size = *((int16_t*)varchar_len);
            size += VARCHAR_LEN_TYPE_LEN;
          }
          else
          {
            ret = OB_SIZE_OVERFLOW;
          }
          break;
        case COMPACT_OBJ_TYPE_ESCAPE:
          size = 0;
          break;
        default:
          TBSYS_LOG(WARN, "");
          ret = OB_INVALID_ARGUMENT;
          break;
        }

        if (OB_SUCCESS == ret)
        {
          if ((COMPACT_OBJ_TYPE_ESCAPE == meta->type_) && (COMPACT_OBJ_ESCAPE_NOP != meta->attr_))
          {
            //have no column id
          }
          else
          {
            switch(row_store_type_)
            {
            case DENSE_SPARSE:
              if (iter_pos_ < rowvalue_pos_)
              {
              }
              else
              {
                size += static_cast<int32_t>(COL_ID_SIZE);
              }
              break;
            case DENSE_DENSE:
              break;
            }
          }

          follow_size = size;
        }
      }

      return ret;
    }
  }//end namespace sstable
}//end namespace oceanbase
