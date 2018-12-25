#include "ob_file_table.h"
using namespace oceanbase::sql;
using namespace oceanbase::sql::test;
using namespace oceanbase::common;

#define GET_LINE do { \
  fgets(line_, sizeof(line_), fp_); \
  line_[strlen(line_) - 1] = '\0'; \
  strcpy(line_copy_, line_); \
} \
while(0)

ObFileTable::ObFileTable(const char *file_name)
  :table_id_(0),
  row_count_(0),
  column_count_(0),
  file_name_(file_name),
  fp_(NULL),
  curr_count_(0),
  count_(0)
{
}

ObFileTable::~ObFileTable()
{
}

int ObFileTable::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  UNUSED(child_idx);
  UNUSED(child_operator);
  return OB_NOT_SUPPORTED;
}


int ObFileTable::parse_line(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj value;
  int64_t int_value;
  ObString str_value;

  if (column_count_ == count_)
  {
    for (int32_t i=0;(OB_SUCCESS == ret) && i<count_;i++)
    {
      if(0 == strcmp(tokens_[i], "NULL"))
      {
        value.set_null();
      }
      else
      {
        if(ObIntType == column_type_[i])
        {
          bool is_add = false;
          if( tokens_[i][strlen(tokens_[i]) - 1] == '+' )
          {
            tokens_[i][strlen(tokens_[i]) - 1] = '\0';
            is_add = true;
          }
          int_value = atoi(tokens_[i]);
          value.set_int(int_value, is_add);
        }
        else if(ObVarcharType == column_type_[i])
        {
          str_value.assign_ptr(tokens_[i], (int32_t)strlen(tokens_[i]));
          value.set_varchar(str_value);
        }
        else if(ObExtendType == column_type_[i])
        {
          value.set_ext(ObActionFlag::OP_DEL_ROW);
        }
        else
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "unsupport type [%d]", column_type_[i]);
        }
      }

      if(OB_SUCCESS == ret)
      {
        int64_t index = 0;
        ObRowkeyColumn rk_col;
        if(OB_SUCCESS == rowkey_info_.get_index(column_ids_[i], index , rk_col))
        {
          rowkey_obj[index] = value;
        }
        get_curr_row()->set_cell(table_id_, column_ids_[i], value);
      }
    }
  }
  else
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "column_count_[%d], count_[%d]", column_count_, count_);
  }

  if (OB_SUCCESS == ret)
  {
    row = get_curr_row();
  }

  return ret;
}

int ObFileTable::get_next_row(const ObRow *&row)
{
  const common::ObRowkey *rowkey = NULL;
  return get_next_row(rowkey, row);
}

int ObFileTable::get_next_row(const common::ObRowkey *&rowkey, const ObRow *&row)
{
  int ret = OB_SUCCESS;

  if(curr_count_ >= row_count_)
  {
    ret = OB_ITER_END;
  }

  if(OB_SUCCESS == ret)
  {
    GET_LINE;
    split(line_, " ", tokens_, count_);
    ret = parse_line(row);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "parse line fail");
    }
  }

  if(OB_SUCCESS == ret)
  {
    cur_rowkey_.assign(rowkey_obj, rowkey_info_.get_size());
    rowkey = &cur_rowkey_;
    curr_count_ ++;
  }

  return ret;
}

int ObFileTable::open()
{
  int ret = OB_SUCCESS;

  memset(&rowkey_info_, 0, sizeof(rowkey_info_));
  row_desc_.reset();
  row_count_ = 0;
  column_count_ = 0;
  curr_count_ = 0;
  count_ = 0;
  fp_ = NULL;
  table_id_ = 0;

  if (NULL == file_name_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "file_name_ is null");
  }

  if(OB_SUCCESS == ret)
  {
    fp_ = fopen(file_name_, "r");
    if (NULL == fp_)
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "open file [%s] fail", file_name_);
    }
    else
    {
      TBSYS_LOG(INFO, "open file [%s]", file_name_);
    }
  }

  if(OB_SUCCESS == ret)
  {
    GET_LINE;
    ret = convert(line_, table_id_);
  }

  //if(OB_SUCCESS == ret)
  //{
  //  GET_LINE;
  //  ret = convert(line_, rowkey_item_count_);
  //}

  //if(rowkey_item_count_ >= OB_MAX_ROWKEY_COLUMN_NUMBER)
  //{
  //  ret = OB_SIZE_OVERFLOW;
  //  TBSYS_LOG(WARN, "rowkey item count too large[%d]", rowkey_item_count_);
  //}

  if(OB_SUCCESS == ret)
  {
    GET_LINE;
    split(line_, " ", tokens_, count_);
  }

  if(OB_SUCCESS == ret)
  {
    if ( 2 == count_ )
    {
      if(OB_SUCCESS != (ret = convert(tokens_[0], row_count_)))
      {
        TBSYS_LOG(WARN, "convert int fail:[%s]", tokens_[0]);
      }
      else if(OB_SUCCESS != (ret = convert(tokens_[1], column_count_)))
      {
        TBSYS_LOG(WARN, "convert int fail:[%s]", tokens_[1]);
      }
    }
    else
    {
      TBSYS_LOG(WARN, "should be two num [%d]", count_);
      ret = OB_ERROR;
    }
  }

  if(OB_SUCCESS == ret)
  {
    GET_LINE;
    split(line_, " ", tokens_, count_);
  }

  if(OB_SUCCESS == ret)
  {
    if( column_count_ == count_ )
    {
      for(int32_t i=0; (OB_SUCCESS == ret) && i<count_;i++)
      {
        column_ids_[i] = atoi(tokens_[i]);
      }
    }
    else
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "column_count_[%d], count_[%d], line_[%s]", column_count_, count_, line_copy_);
    }
  }

  if(OB_SUCCESS == ret)
  {
    GET_LINE;
    split(line_, " ", tokens_, count_);
  }

  if(OB_SUCCESS == ret)
  {
    if( column_count_ == count_ )
    {
      int type = 0;
      for(int32_t i=0; (OB_SUCCESS == ret) && i<count_;i++)
      {
        if(OB_SUCCESS != (ret = convert(tokens_[i], type)))
        {
          TBSYS_LOG(WARN, "convert [%s] fail", tokens_[i]);
        }
        else
        {
          switch( type )
          {
          case 0:
            column_type_[i] = ObIntType;
            break;
          case 1:
            column_type_[i] = ObVarcharType;
            break;
          case 2:
            column_type_[i] = ObExtendType;
            break;
          default:
            ret = OB_ERROR;
            TBSYS_LOG(WARN, "unsupport type[%s]", tokens_[i]);
          }
        }
      }
    }
    else
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "column_count_[%d], count_[%d]", column_count_, count_);
    }
  }

  if(OB_SUCCESS == ret)
  {
    GET_LINE;
    split(line_, " ", tokens_, count_);
  }

  int32_t int_value = 0;
  ObRowkeyColumn rk_col;

  if(OB_SUCCESS == ret)
  {
    for(int i=0;i<count_;i++)
    {
      convert(tokens_[i], int_value);
      rk_col.column_id_ = (uint64_t)int_value;
      rk_col.type_ = get_column_type(rk_col.column_id_); 
      rowkey_info_.add_column(rk_col);
    }
  }

  if(OB_SUCCESS == ret)
  {
    GET_LINE;
    split(line_, " ", tokens_, count_);
  }

  if(OB_SUCCESS == ret)
  {
    get_curr_row()->set_row_desc(row_desc_);

    for(int i=0;i<count_;i++)
    {
      convert(tokens_[i], int_value);
      row_desc_.add_column_desc(table_id_, (uint64_t)int_value);
    }
    
  }

  return ret;
}

int ObFileTable::close()
{
  int ret = OB_SUCCESS;
  if (NULL != fp_)
  {
    fclose(fp_);
    fp_ = NULL;
  }
  return ret;
}

ObRow *ObFileTable::get_curr_row()
{
  return &curr_row_;
}

int64_t ObFileTable::to_string(char* buf, const int64_t buf_len) const
{
  UNUSED(buf);
  UNUSED(buf_len);
  return 0;
}

int ObFileTable::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  row_desc = &row_desc_;
  return ret;
}

