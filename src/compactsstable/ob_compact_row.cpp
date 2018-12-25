/*
* (C) 2007-2011 TaoBao Inc.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
* ob_compact_row.cpp is for what ...
*
* Version: $id$
*
* Authors:
*   MaoQi maoqi@taobao.com
*
*/
#include "ob_compact_row.h"
#include "common/ob_string.h"
#include "common/ob_malloc.h"
#include "common/utility.h"

namespace oceanbase
{
  using namespace common;
  namespace compactsstable
  {
    namespace
    {
      uint8_t get_int_type(const int64_t v)
      {
        /*
         * from yubai.lk
         * 注意 为了减少计算代价 与常规的int类型表示范围不同
         * int8_t 表示范围为 [-127, 127] 而非 [-128, 127]
         * int16_t 表示范围为 [-32767, 32767] 而非 [-32768, 127]
         * int32_t 表示范围为 [-2147483647, 2147483647] 而非 [-2147483648, 2147483647]
         */

        static const int64_t INT8_MASK  = 0xffffffffffffff80;
        static const int64_t INT16_MASK = 0xffffffffffff8000;
        static const int64_t INT32_MASK = 0xffffffff80000000;

        int64_t abs_v = v;
        if (0 > abs_v)
        {
          abs_v = llabs(v);
        }
        uint8_t ret = COMPACT_OBJ_TYPE_INT64;
        if (!(INT8_MASK & abs_v))
        {
          ret = COMPACT_OBJ_TYPE_INT8;
        }
        else if (!(INT16_MASK & abs_v))
        {
          ret = COMPACT_OBJ_TYPE_INT16;
        }
        else if (!(INT32_MASK & abs_v))
        {
          ret = COMPACT_OBJ_TYPE_INT32;
        }
        else
        {
          //int64
        }
        return ret;
      }

      uint8_t get_time_type(int32_t vtype)
      {
        uint8_t type = 0;
        if (ObDateTimeType == vtype)
        {
          type = COMPACT_OBJ_TYPE_TIME;
        }
        //add peiouya [DATE_TIME] 20150906:b
        else if (ObDateType == vtype)
        {
          type = COMPACT_OBJ_TYPE_DATE;
        }
        else if (ObTimeType == vtype)
        {
          type = COMPACT_OBJ_TYPE_TIME2;
        }
        //add 20150906:e
        else if (ObPreciseDateTimeType == vtype)
        {
          type = COMPACT_OBJ_TYPE_PRECISE;
        }
        else if (ObCreateTimeType == vtype)
        {
          type = COMPACT_OBJ_TYPE_CREATE;
        }
        else if (ObModifyTimeType == vtype)
        {
          type = COMPACT_OBJ_TYPE_MODIFY;
        }
        else
        {
          TBSYS_LOG(WARN,"have no this type of time:%d",vtype);
        }
        return type;
      }

      void set_time_type(ObObj& obj,uint8_t vtype,int64_t v,bool is_add)
      {
        if (COMPACT_OBJ_TYPE_TIME == vtype)
        {
          obj.set_datetime(v,is_add);
        }
        else if (COMPACT_OBJ_TYPE_PRECISE == vtype)
        {
          obj.set_precise_datetime(v,is_add);
        }
        else if (COMPACT_OBJ_TYPE_CREATE == vtype)
        {
          obj.set_createtime(v);
        }
        else if (COMPACT_OBJ_TYPE_MODIFY == vtype)
        {
          obj.set_modifytime(v);
        }
        //add peiouya [DATE_TIME] 20150906:b
        else if (COMPACT_OBJ_TYPE_DATE == vtype)
        {
          obj.set_date(v, is_add);
        }
        else if (COMPACT_OBJ_TYPE_TIME2 == vtype)
        {
          obj.set_time(v, is_add);
        }
        //add 20150906:e
        else
        {
          TBSYS_LOG(WARN,"have no this type of time:%d",vtype);
        }
      }

      int set_value(char* buffer, const int64_t size, int32_t& pos, const int64_t v, int& itype)
      {
        int ret = OB_SUCCESS;
        itype = get_int_type(v);
        if ((pos + INT_DATA_SIZE[itype]) > size)
        {
          TBSYS_LOG(WARN, "size overflow pos=%d size=%ld int_size=%d",
                    pos, size, INT_DATA_SIZE[itype]);
          ret = OB_SIZE_OVERFLOW;
        }
        else
        {
          switch (itype)
          {
          case COMPACT_OBJ_TYPE_INT8:
            *((int8_t*)(buffer + pos)) = static_cast<int8_t>(v);
            break;
          case COMPACT_OBJ_TYPE_INT16:
            *((int16_t*)(buffer + pos)) = static_cast<int16_t>(v);
            break;
          case COMPACT_OBJ_TYPE_INT32:
            *((int32_t*)(buffer + pos)) = static_cast<int32_t>(v);
            break;
          case COMPACT_OBJ_TYPE_INT64:
          default:
            *((int64_t*)(buffer + pos)) = static_cast<int64_t>(v);
            break;
          }
          pos += INT_DATA_SIZE[itype];
        }
        return ret;
      }

      inline int set_value(char* buffer, const int64_t size, int32_t& pos, const ObString &v)
      {
        int ret = OB_SUCCESS;
        if ( (v.length() > COMPACT_OBJ_MAX_VARCHAR_LEN) || ((pos + VARCHAR_LEN_TYPE_LEN + v.length()) > size))
        {
          TBSYS_LOG(WARN, "size overflow pos=%d size=%ld varchar_size=%d",
                    pos, size, v.length());
          ret = OB_SIZE_OVERFLOW;
        }
        else
        {
          *((int16_t*)(buffer + pos)) = static_cast<int16_t>(v.length());
          pos += 2;
          memcpy(buffer+pos, v.ptr(), v.length());
          pos += v.length();
        }
        return ret;
      }

      inline int set_value(char* buffer, const int64_t size,int32_t& pos,const int64_t v)
      {
        int ret = OB_SUCCESS;
        if (static_cast<int64_t>((pos + sizeof(int64_t))) > size)
        {
          TBSYS_LOG(WARN,"size overflow,pos=%d size=%ld",pos,size);
          ret = OB_SIZE_OVERFLOW;
        }
        else
        {
          *((int64_t *)(buffer + pos)) = v;
          pos += static_cast<int32_t>(sizeof(int64_t));
        }
        return ret;
      }

      inline bool is_end_mark(const ObCompactObjMeta& meta)
      {
        bool ret = false;
        if ((COMPACT_OBJ_TYPE_ESCAPE == meta.type_) && (COMPACT_OBJ_ESCAPE_END_ROW == meta.attr_))
        {
          ret = true;
        }
        return ret;
      }

      /*
       * accroding column meta to get the follow data size
       * if this data have a column id, then include the column id size (2bytes)
       */

      inline int get_follow_size(const ObCompactObjMeta* meta,const char* buf_end,int32_t& follow_size)
      {
        int32_t size = -1;
        int ret = OB_SUCCESS;

        if (reinterpret_cast<const char*>(meta) > buf_end)
        {
          TBSYS_LOG(WARN,"the address of meta invalid,meta:%p,buf_end:%p",meta,buf_end);
          ret = OB_INVALID_ARGUMENT;
        }
        else if (is_end_mark(*meta))
        {
          TBSYS_LOG(DEBUG,"end mark");
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
          {
            char* varchar_len_pos = (char *)(meta + 1);
            if ( (varchar_len_pos + VARCHAR_LEN_TYPE_LEN) < buf_end )
            {
              size = *((int16_t*)varchar_len_pos);
              size += VARCHAR_LEN_TYPE_LEN;
            }
            else
            {
              ret = OB_SIZE_OVERFLOW;
            }
          }
          break;
          case COMPACT_OBJ_TYPE_ESCAPE:
            size = 0;
            break;
          default:
            TBSYS_LOG(WARN,"unknow type:%hhu,%hhu,%hhu",meta->meta_,meta->type_,meta->attr_);
            ret = OB_INVALID_ARGUMENT;
            break;
          }

          if ((COMPACT_OBJ_TYPE_ESCAPE == meta->type_) && (COMPACT_OBJ_ESCAPE_NOP != meta->attr_) )
          {
            //have no column id
          }
          else
          {
            size += static_cast<int32_t>(ObCompactRow::COL_ID_SIZE);
          }
          if (OB_SUCCESS == ret)
          {
            follow_size = size;
          }
        }
        return ret;
      }
    }

    ObCompactRow::ObCompactRow() : buf_pos_(0),
                                   iter_pos_(0),
                                   iter_start_(0),
                                   iter_end_(0),
                                   self_alloc_(false),
                                   col_num_(0),
                                   key_col_num_(0),
                                   key_col_len_(0),
                                   buf_(NULL)
    {}

    ObCompactRow::~ObCompactRow()
    {
      reset();
      if (self_alloc_)
      {
        ob_free(buf_);
        self_alloc_ = false;
      }
      buf_ = NULL;
    }

    int ObCompactRow::add_col(const uint64_t column_id, const ObObj &value)
    {
      int ret = OB_SUCCESS;
      if ((MAX_COL_ID < column_id))
      {
        if ((value.get_type() != ObExtendType) && (value.get_ext() != ObActionFlag::OP_DEL_ROW))
        {
          TBSYS_LOG(WARN,"invalid column_id:%lu",column_id);
          value.dump(TBSYS_LOG_LEVEL_WARN);
          ret = OB_SIZE_OVERFLOW;
        }
      }
      else if (NULL == buf_)
      {
        if (NULL == (buf_ = (char*)ob_malloc(MAX_BUF_SIZE, ObModIds::COMPACT_ROW)))
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          self_alloc_ = true;
        }
      }
      else if (!self_alloc_)
      {
        TBSYS_LOG(WARN,"not self alloc buffer,do not support add column");
        ret = OB_NOT_SUPPORTED;
      }
      else if (MAX_BUF_SIZE < static_cast<int64_t>((buf_pos_ + sizeof(ObCompactObjMeta) + COL_ID_SIZE))
              || MAX_COL_NUM <= col_num_)
      {
        TBSYS_LOG(WARN,"size overflow,buf_pos_:%d,col_num_:%d",buf_pos_,col_num_);
        ret = OB_SIZE_OVERFLOW;
      }

      if (OB_SUCCESS == ret)
      {
        int32_t buf_pos = buf_pos_;
        ObCompactObjMeta *col_meta = (ObCompactObjMeta*)(buf_ + buf_pos);
        col_meta->type_ = COMPACT_OBJ_TYPE_NULL;
        col_meta->attr_ = COMPACT_OBJ_ATTR_NORMAL;
        buf_pos += static_cast<int32_t>(sizeof(ObCompactObjMeta));

        int64_t v = 0;
        //add lijianqiang [INT_32] 20151008:b
        int32_t v32 = 0;
        //add 20151008:e
        bool is_add = false;
        ObString str;

        int vtype = value.get_type();
        int itype = 0;

        if ( ObNullType == vtype )
        {
          //do nothing
        }
        else if ( ObIntType == vtype )
        {
          value.get_int(v, is_add);
          ret = set_value(buf_, MAX_BUF_SIZE, buf_pos, v, itype);
          col_meta->type_ = itype & 0x1f;
        }
        //add lijianqiang [INT_32] 20151008:b
        else if ( ObInt32Type == vtype )
        {
          value.get_int32(v32, is_add);
          ret = set_value(buf_, MAX_BUF_SIZE, buf_pos, v32, itype);
          col_meta->type_ = itype & 0x1f;
        }
        //add 20151008:e
        else if ( ObVarcharType == vtype )
        {
          value.get_varchar(str);
          ret = set_value(buf_, MAX_BUF_SIZE, buf_pos, str);
          col_meta->type_ = COMPACT_OBJ_TYPE_VARCHAR;
        }
        else if (ObDateTimeType == vtype)
        {
          value.get_datetime(v,is_add);
          ret = set_value(buf_, MAX_BUF_SIZE, buf_pos, v);
          col_meta->type_ = COMPACT_OBJ_TYPE_TIME;
        }
        //add peiouya [DATE_TIME] 20150906:b
        else if (ObDateType == vtype)
        {
          value.get_date(v,is_add);
          ret = set_value(buf_, MAX_BUF_SIZE, buf_pos, v);
          col_meta->type_ = COMPACT_OBJ_TYPE_DATE;
        }
        else if (ObTimeType == vtype)
        {
          value.get_time(v,is_add);
          ret = set_value(buf_, MAX_BUF_SIZE, buf_pos, v);
          col_meta->type_ = COMPACT_OBJ_TYPE_TIME2;
        }
        //add 20150906:e
        else if (ObPreciseDateTimeType == vtype)
        {
          value.get_precise_datetime(v,is_add);
          ret = set_value(buf_, MAX_BUF_SIZE, buf_pos, v);
          col_meta->type_ = COMPACT_OBJ_TYPE_PRECISE;
        }
        else if (ObCreateTimeType == vtype)
        {
          value.get_createtime(v);
          ret = set_value(buf_, MAX_BUF_SIZE, buf_pos, v);
          col_meta->type_ = COMPACT_OBJ_TYPE_CREATE;
        }
        else if (ObModifyTimeType == vtype)
        {
          value.get_modifytime(v);
          ret = set_value(buf_, MAX_BUF_SIZE, buf_pos, v);
          col_meta->type_ = COMPACT_OBJ_TYPE_MODIFY;
        }
        else if (ObExtendType == vtype)
        {
          value.get_ext(v);
          col_meta->type_ = COMPACT_OBJ_TYPE_ESCAPE;
          if (ObActionFlag::OP_DEL_ROW == v)
          {
            col_meta->attr_ = COMPACT_OBJ_ESCAPE_DEL_ROW;
          }
          else if (ObActionFlag::OP_NOP == v)
          {
            col_meta->attr_ = COMPACT_OBJ_ESCAPE_NOP;
          }
          else
          {
            TBSYS_LOG(WARN,"not supported exten:%ld,column id is %lu,buf_pos_ is %d",v,column_id,buf_pos_);
            hex_dump(buf_, buf_pos_,true,TBSYS_LOG_LEVEL_WARN);
            ret = OB_NOT_SUPPORTED;
          }
        }
        else
        {
          TBSYS_LOG(WARN,"not supported type:%d",vtype);
          ret = OB_NOT_SUPPORTED;
        }

        if (OB_SUCCESS == ret)
        {
          if (is_add)
          {
            col_meta->attr_ = COMPACT_OBJ_ATTR_ADD;
          }

          if ((COMPACT_OBJ_TYPE_ESCAPE == col_meta->type_) && (COMPACT_OBJ_ESCAPE_NOP != col_meta->attr_) )
          {
            //have no column id
          }
          else
          {
            *((uint16_t*)(buf_ + buf_pos)) = static_cast<uint16_t>(column_id);
            buf_pos += static_cast<int32_t>(sizeof(uint16_t));
          }

          col_num_ += 1;
          buf_pos_ = buf_pos;

          if (key_col_num_ >= col_num_)
          {
            key_col_len_ = buf_pos_;
          }
        }
      }
      return ret;
    }

    int ObCompactRow::set_end()
    {
      int ret = OB_SUCCESS;
      if (!self_alloc_)
      {
        ret = OB_NOT_SUPPORTED;
      }
      else if (!is_end_)
      {
        if (0 == buf_pos_)
        {
          TBSYS_LOG(WARN,"not data in this row");
          ret = OB_ERROR;
        }
        else if (buf_pos_ >= MAX_BUF_SIZE)
        {
          ret = OB_SIZE_OVERFLOW;
        }
        else
        {
          ObCompactObjMeta* col_meta = reinterpret_cast<ObCompactObjMeta*>(buf_+buf_pos_);
          col_meta->type_ = COMPACT_OBJ_TYPE_ESCAPE;
          col_meta->attr_ = COMPACT_OBJ_ESCAPE_END_ROW;
          is_end_         = true;
          ++buf_pos_;
        }
      }
      else
      {
        //do nothing
      }
      return ret;
    }

    void ObCompactRow::reset()
    {
      reset_iter();
      buf_pos_ = 0;
      col_num_ = 0;
      is_end_  = false;
    }

    int ObCompactRow::set_row(const char* row,const int64_t row_len)
    {
      int ret = OB_SUCCESS;
      if (self_alloc_ && buf_ != NULL)
      {
        ob_free(buf_);
        self_alloc_ = false;
      }

      buf_ = const_cast<char*>(row);
      is_end_ = true;
      buf_pos_ = static_cast<int32_t>(row_len);

      if ( 0 == buf_pos_)
      {
        buf_pos_ = MAX_BUF_SIZE; //will calc row len later
      }

      if ((col_num_ = calc_col_num()) < 0)
      {
        TBSYS_LOG(WARN,"invalid row,row:%p,row_len:%ld",row,row_len);
        hex_dump(row,static_cast<int32_t>(row_len),true,TBSYS_LOG_LEVEL_WARN);
        ret = OB_ERROR;
      }

      return ret;
    }

    /**
     * if rowkey not split,rowkey is the value of column 1
     *
     * @param[out] rowkey
     *
     * @return OB_SUCCESS on success
     */
    int ObCompactRow::get_row_key(ObRowkey& rowkey)
    {
      int ret = OB_SUCCESS;
      if (buf_pos_ <= 0 || NULL == buf_)
      {
        ret = OB_ERROR;
      }
      else
      {
        reset_iter();
        ObCellInfo* cell = NULL;
        if ((ret = next_cell()) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"iterator row failed,ret=%d",ret);
        }
        else if ((ret = get_cell(&cell)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"get cell failed,ret=%d",ret);
        }
        else if ((NULL == cell) || (cell->value_.get_type() != ObVarcharType) ||
                 (cell->column_id_ != ROWKEY_COLUMN_ID))
        {
          TBSYS_LOG(WARN,"invalid row");
        }
        /* TODO
        else if ((ret = cell->value_.get_varchar(rowkey)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"get varchar failed,ret=%d",ret);
        }
        */
        else if (rowkey.length() <= 0 || NULL == rowkey.ptr())
        {
          TBSYS_LOG(WARN,"rowkey is invalid");
          ret = OB_ERROR;
        }
        else
        {
          //success
        }
      }
      return ret;
    }

    void ObCompactRow::reset_iter()
    {
      iter_pos_ = 0;
      iter_end_ = 0;
      iter_start_ = 0;
    }

    int ObCompactRow::next_cell()
    {
      int ret = OB_SUCCESS;
      if (iter_end_ || static_cast<int32_t>((iter_pos_ + sizeof(ObCompactObjMeta))) > buf_pos_)
      {
        iter_end_ = 1;
        ret = OB_ITER_END;
      }
      else if (!iter_start_)
      {
        iter_start_ = 1;
      }
      else
      {
        ObCompactObjMeta *col_meta = reinterpret_cast<ObCompactObjMeta*>(buf_ + iter_pos_);
        int32_t follow_size = 0;
        if (get_follow_size(col_meta,buf_ + buf_pos_,follow_size) != OB_SUCCESS)
        {
          iter_end_ = 1;
          ret = OB_ITER_END;
        }
        else
        {
          iter_pos_ += static_cast<int32_t>(sizeof(ObCompactObjMeta) + follow_size);
        }

        if (static_cast<int32_t>(iter_pos_ + sizeof(ObCompactObjMeta)) > buf_pos_)
        {
          iter_end_ = 1;
          ret = OB_ITER_END;
        }
        else
        {
          col_meta = reinterpret_cast<ObCompactObjMeta*>(buf_ + iter_pos_);
          if (is_end_mark(*col_meta))
          {
            iter_end_ = 1;
            ret = OB_ITER_END;
          }
          else
          {
            if (get_follow_size(col_meta,buf_ + buf_pos_,follow_size) != OB_SUCCESS)
            {
              iter_end_ = 1;
              ret = OB_ITER_END;
            }
            else if ((iter_pos_ + follow_size) > buf_pos_)
            {
              iter_end_ = 1;
              ret = OB_ITER_END;
            }
          }
        }
      }
      return ret;
    }

    int ObCompactRow::get_cell(ObCellInfo** cell)
    {
      return get_cell(cell,NULL);
    }

    int ObCompactRow::get_cell(ObCellInfo** cell,bool* is_row_changed)
    {
      int  ret         = OB_SUCCESS;
      bool has_colmnid = true;
      if (NULL == cell)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!iter_start_)
      {
        ret = OB_NOT_INIT;
      }
      else if (iter_end_)
      {
        ret = OB_ITER_END;
      }
      else
      {
        if (is_row_changed != NULL)
        {
          *is_row_changed = (0 == iter_pos_);
        }
        ObCompactObjMeta* col_meta = (ObCompactObjMeta*)(buf_ + iter_pos_);
        bool is_add = ( COMPACT_OBJ_ATTR_ADD == col_meta->attr_ );
        int64_t v = 0;
        //add lijianqiang [INT_32] 20151008:b
        int32_t v32 = 0;
        //add 20151008:e
        int32_t follow_size = 0;
        switch(col_meta->type_)
        {
        case COMPACT_OBJ_TYPE_NULL:
          cur_cell_.value_.set_null();
          follow_size = 0;
          break;
        case COMPACT_OBJ_TYPE_INT8:
          v = *((int8_t *)(col_meta + 1));
          cur_cell_.value_.set_int(v,is_add);
          follow_size = 1;
          break;
        case COMPACT_OBJ_TYPE_INT16:
          v = *((int16_t *)(col_meta + 1));
          cur_cell_.value_.set_int(v,is_add);
          follow_size = 2;
          break;
        case COMPACT_OBJ_TYPE_INT32:
        //mod lijianqiang [INT_32] 20151008:b
          //v = *((int32_t *)(col_meta + 1));
          //cur_cell_.value_.set_int(v,is_add);
          v32 = *((int32_t *)(col_meta + 1));
          cur_cell_.value_.set_int32(v32,is_add);
        //mod 20151008:e
          follow_size = 4;
          break;
        case COMPACT_OBJ_TYPE_INT64:
          v = *((int64_t *)(col_meta + 1));
          cur_cell_.value_.set_int(v,is_add);
          follow_size = 8;
          break;
        case COMPACT_OBJ_TYPE_PRECISE:
        case COMPACT_OBJ_TYPE_TIME:
        case COMPACT_OBJ_TYPE_MODIFY:
        case COMPACT_OBJ_TYPE_CREATE:
          v = *((int64_t *)(col_meta + 1));
          set_time_type(cur_cell_.value_,col_meta->type_,v,is_add);
          follow_size = 8;
          break;
        case COMPACT_OBJ_TYPE_VARCHAR:
        {
          char* varchar_len_pos = (char *)(col_meta + 1);
          v = *((int16_t*)varchar_len_pos);
          ObString vchar;
          vchar.assign_ptr(varchar_len_pos + VARCHAR_LEN_TYPE_LEN,static_cast<int32_t>(v));
          cur_cell_.value_.set_varchar(vchar);
          follow_size = static_cast<int32_t>(v + VARCHAR_LEN_TYPE_LEN);
        }
          break;
        case COMPACT_OBJ_TYPE_ESCAPE:
          if (COMPACT_OBJ_ESCAPE_DEL_ROW == col_meta->attr_)
          {
            cur_cell_.value_.set_ext(ObActionFlag::OP_DEL_ROW);
            has_colmnid = false;
          }
          else if (COMPACT_OBJ_ESCAPE_NOP == col_meta->attr_)
          {
            cur_cell_.value_.set_ext(ObActionFlag::OP_NOP);
          }
          follow_size = 0;
          break;
        default:
          ret = OB_NOT_SUPPORTED;
          break;
        }

        if (OB_SUCCESS == ret)
        {
          if (has_colmnid)
          {
            cur_cell_.column_id_ = *((uint16_t *)((char *)(col_meta + 1) + follow_size));
          }
          else
          {
            cur_cell_.column_id_ = OB_INVALID_ID;
          }
          *cell = &cur_cell_;
        }
      }
      return ret;
    }

    int ObCompactRow::copy(const ObCompactRow& row)
    {
      int ret = OB_SUCCESS;
      reset();

      if (NULL == buf_)
      {
        if (NULL == (buf_ = reinterpret_cast<char*>(ob_malloc(MAX_BUF_SIZE, ObModIds::COMPACT_ROW))))
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(WARN,"alloc mem failed");
        }
        else
        {
          self_alloc_ = 1;
        }
      }

      if (row.buf_pos_ > MAX_BUF_SIZE)
      {
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        memcpy(buf_,row.buf_,row.buf_pos_);
        buf_pos_     = row.buf_pos_;
        col_num_     = row.col_num_;
        key_col_num_ = row.key_col_num_;
        key_col_len_ = row.key_col_len_;
        is_end_      = row.is_end_;
      }
      return ret;
    }

    int32_t ObCompactRow::compare(ObCompactRow& left,ObCompactRow& right,int32_t key_col_nums /*=1*/)
    {
      left.reset_iter();
      right.reset_iter();
      int32_t result = 0;
      int ret = OB_SUCCESS;

      for(int32_t i=0; (i < key_col_nums) && (OB_SUCCESS == ret); ++i)
      {
        if ((OB_SUCCESS == left.next_cell()) && (OB_SUCCESS == right.next_cell()))
        {
          ObCellInfo *left_cell = NULL;
          ObCellInfo *right_cell = NULL;

          if ((OB_SUCCESS == left.get_cell(&left_cell)) && (OB_SUCCESS == right.get_cell(&right_cell)))
          {
            if (left_cell->column_id_ != right_cell->column_id_)
            {
              TBSYS_LOG(WARN,"can not compare");
            }
            else
            {
              if (left_cell->value_ < right_cell->value_)
              {
                result = -1;
              }
              else if (left_cell->value_ > right_cell->value_)
              {
                result = 1;
              }
              else
              {
                result = 0;
              }
            }
          }
        }
      }
      return result;
    }


    int ObCompactRow::copy_key(char* buf,const int64_t buf_len,int64_t& pos,int32_t key_col_nums)
    {
      int ret = OB_SUCCESS;
      if (NULL == buf || buf_len <= 0 || pos <= 0 || key_col_nums <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        int32_t key_size = get_key_columns_len(key_col_nums);
        if (key_size < 0 || (key_size + pos) > buf_len)
        {
          ret = OB_SIZE_OVERFLOW;
        }
        else
        {
          memcpy(buf + pos,buf_,key_size);
          pos += key_size;
        }
      }
      return ret;
    }

    const char* ObCompactRow::get_key_buffer() const
    {
      return buf_;
    }

    /**
     * get value buffer,include END_MARK
     *
     * @return
     */
    const char* ObCompactRow::get_value_buffer(int32_t key_col_nums) const
    {
      return buf_ + get_key_columns_len(key_col_nums);
    }

    /**
     * 这里可以做个优化，提供一个函数来获取key_size,通过key_size来做offset,
     * 这样拷贝key和value可以只计算一次key_size
     */
    int ObCompactRow::copy_value(char* buf,const int64_t buf_len,int64_t& pos,int32_t key_col_nums)
    {
      int ret = OB_SUCCESS;
      if (NULL == buf || buf_len <= 0 || pos < 0 || key_col_nums <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        int32_t key_size = get_key_columns_len(key_col_nums);
        int32_t data_size = buf_pos_ - key_size;
        if (key_size < 0 || data_size <= 0 || (pos + data_size) > buf_len)
        {
          ret = OB_SIZE_OVERFLOW;
        }
        else
        {
          memcpy(buf + pos,buf_ + key_size,data_size);
          pos += data_size;
        }
      }
      return ret;
    }

    void ObCompactRow::set_key_col_num(int32_t key_col_num)
    {
      if (key_col_num > 0)
      {
        key_col_num_ = key_col_num;
      }
    }

    int64_t ObCompactRow::get_value_columns_len(const int32_t key_col_nums) const
    {
      int64_t value_data_size = buf_pos_ - get_key_columns_len(key_col_nums);
      if (value_data_size < 0)
      {
        TBSYS_LOG(WARN,"data corrupt");
        value_data_size = -1;
      }
      return value_data_size;
    }

    int64_t ObCompactRow::get_key_columns_len(const int32_t key_col_nums) const
    {
      int64_t size = -1;
      int ret = OB_SUCCESS;
      if (key_col_nums < 0 || key_col_nums > col_num_)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (col_num_ == key_col_nums)
      {
        size = buf_pos_;
      }
      else if ((key_col_nums == key_col_num_) && (key_col_len_ > 0))
      {
        size = key_col_len_;
      }
      else
      {
        ObCompactObjMeta *col_meta = NULL;
        int64_t data_size = 0;
        int32_t tmp = 0;
        for(int32_t i=0; (i < key_col_nums) && (OB_SUCCESS == ret); ++i)
        {
          col_meta = reinterpret_cast<ObCompactObjMeta *>(buf_ + data_size);
          if ((ret = get_follow_size(col_meta,buf_ + buf_pos_,tmp)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN,"buf corrupt");
            size = -1;
          }
          else
          {
            data_size += (static_cast<int32_t>(sizeof(*col_meta)) + tmp);
          }
        }
        if (OB_SUCCESS == ret)
        {
          size = data_size;
        }
      }
      return size;
    }

    int32_t ObCompactRow::calc_col_num()
    {
      int     ret     = OB_SUCCESS;
      int32_t col_num = 0;
      if ((buf_ != NULL) && (buf_pos_ > 0))
      {
        int32_t tmp = 0;

        int32_t total_len = 0;
        ObCompactObjMeta* col_meta = reinterpret_cast<ObCompactObjMeta*>(buf_);
        while(OB_SUCCESS == (ret = get_follow_size(col_meta,buf_ + buf_pos_,tmp)))
        {
          ++col_num;
          total_len += (tmp + static_cast<int32_t>(sizeof(*col_meta)));
          col_meta = reinterpret_cast<ObCompactObjMeta*>(buf_ + total_len);
        }

        if (OB_ITER_END != ret)
        {
          TBSYS_LOG(WARN,"col_num:%d,col_pos:%p",col_num,col_meta);
          col_num = -1;
        }
        else
        {
          total_len += static_cast<int32_t>(sizeof(*col_meta)); //END MARK
          if ( buf_pos_ == total_len)
          {
            //normal
          }
          else if ( (buf_pos_ != total_len) && (MAX_BUF_SIZE == buf_pos_) )
          {
            buf_pos_ = total_len;
          }
          else
          {
            TBSYS_LOG(WARN,"invalid row len:%d,actual len:%d",buf_pos_,total_len);
            col_num = -1;
          }
        }
      }
      return col_num;
    }
  } //end namespace compactsstable
} //end namespace oceanbase
