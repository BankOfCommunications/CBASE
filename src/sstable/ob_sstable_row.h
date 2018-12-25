/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_sstable_row.h for persistent ssatable single row. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *   fangji  <fangji.hcm@taobao.com>
 *
 */
#ifndef OCEANBASE_SSTABLE_OB_SSTABLE_ROW_H_
#define OCEANBASE_SSTABLE_OB_SSTABLE_ROW_H_

#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_object.h"
#include "common/ob_malloc.h"
#include "common/ob_string_buf.h"
#include "common/ob_rowkey_helper.h"
#include "common/ob_schema.h"
#include "ob_sstable_schema.h"

namespace oceanbase 
{
  namespace common
  {
    class ObRowkey;
  }
  namespace sstable 
  {
    /**
     * this class is used to serialize or deserialize one row.
     * 
     * WARNING: an ObSSTableRow instance only can do serialization
     * or deserialization, don't use one ObSSTableRow instance to do
     * both serialization and desrialization at the same time.
     */
    class ObSSTableRow
    {
      static const int64_t DEFAULT_KEY_BUF_SIZE = 64;
      static const int64_t DEFAULT_MAX_COLUMN_NUMBER = 
        common::OB_MAX_COLUMN_NUMBER * 2; 

    public:
      ObSSTableRow();
      ObSSTableRow(const common::ObRowkeyInfo* rowkey_info);
      ~ObSSTableRow();



      //add zhaoqiong [bugfix::trim rowkey in varchar type in case of memtable checksum mismatch] 20160612:b
      int set_internal_rowkey(const uint64_t table_id,
              const common::ObRowkey& rowkey);
      //add:e
      inline bool is_binary_rowkey() const { return NULL != binary_rowkey_info_; }

      /**
       * return how many objects(columns) in the row, maximum count is 
       * common::MAX_COLUMN_NUMBER, if no object, return 0
       * 
       * @return int64_t object count
       */
      inline const int64_t get_obj_count() const
      {
        return obj_count_;
      }

      /**
       * If user uses this class to deserialize one row, first you 
       * need set the count of objects in the row based on the count 
       * of columns in schema 
       * 
       * @param obj_count columns count in schema 
       * @param sparse_format whether row is in sparse format 
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int set_obj_count(const int64_t obj_count, 
                        const bool sparse_format = false);


      /**
       * return ObRowkeyInfo
       *
       * @return Pointer of ObRowkeyInfo
       */
      const common::ObRowkeyInfo* get_rowkey_info() const;

      /**
       * set ObRowkeyInfo
       *
       * @return int  if success return OB_SUCCESS, else return OB_ERROR
       */
      int set_rowkey_info(const common::ObRowkeyInfo* rowkey_info);
      
      /**
       * return table id of this row
       *
       * @return uint64_t table id of this row
       */
      inline const uint64_t get_table_id() const
      {
        return table_id_;
      }

      /**
       * set table id of this row
       *
       * @return int if success, return OB_SUCCESS, else return OB_ERROR
       */
      int set_table_id(const uint64_t table_id);

      /**
       * return column group id of this row
       *
       * @return uint64_t column group id of this row
       */
      inline const uint64_t get_column_group_id() const
      {
        return column_group_id_;
      }

      /**
       * set column group id of this row
       *
       * @return int if success, return OB_SUCCESS, else return OB_ERROR
       */
      int set_column_group_id(const uint64_t column_group_id);

      /**
       * 
       */
      inline int get_rowkey(common::ObRowkey& rowkey) const
      {
        int ret = common::OB_SUCCESS;

        int64_t rowkey_obj_cnt = rowkey.get_obj_cnt();

        if (rowkey_obj_cnt > obj_count_ || rowkey_obj_cnt != rowkey_obj_count_)
        {
          TBSYS_LOG(ERROR, "invalid argument, rowkey_obj_count_ = %ld, obj_count_ =%d, "
                           "expected_rowkey_obj_cnt= %ld",
              rowkey_obj_count_, obj_count_, rowkey_obj_cnt);
          ret = common::OB_INVALID_ARGUMENT;
        }
        else
        {
          rowkey.assign(const_cast<common::ObObj*>(objs_), rowkey_obj_cnt);
        }

        return ret;
      }

      /**
       * get one object by index
       * 
       * @param index the index of the object in the inner object 
       *              array
       * 
       * @return const ObObj* if object exists, return it, else return 
       *         NULL.
       */
      const common::ObObj* get_obj(const int32_t index) const;
      //add zhuyanchao[secondary index static_data_build]20150326
      common::ObObj* get_obj_v1(const int32_t index);
      common::ObObj get_row_obj(int64_t index);
      //add e

      /**
       * get objs array of row 
       * 
       * @param obj_count obj count in row
       * 
       * @return const common::ObObj* obj array in row
       */
      const common::ObObj* get_row_objs(int64_t& obj_count);

      /**
       * clear all the members 
       *  
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_INVALID_ARGUMENT 
       */
      int clear();

      /**
       * set row key of the row
       * 
       * @param key row key
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int set_rowkey(const common::ObRowkey& rowkey);

      //add wenghaixing [secondary index static_index_build]20150305
      void set_rowkey_obj_count(const int64_t count);
      //add e

      inline const int64_t get_rowkey_obj_count() const 
      {
        return rowkey_obj_count_;
      }


      //del zhaoqiong [bugfix::trim rowkey in varchar type in case of memtable checksum mismatch] 20160612:b
      //only used by updateserver to fill rowkey obj into 
      //sstable row with shallow copy
//      inline int set_internal_rowkey(const uint64_t table_id,
//        const common::ObRowkey& rowkey)
//      {
//        int ret = common::OB_SUCCESS;
//        int64_t length = rowkey.get_obj_cnt();
//        const common::ObObj* ptr = rowkey.get_obj_ptr();

//        if (common::OB_INVALID_ID == table_id
//            || length <= 0 || NULL == ptr
//            || 0 != obj_count_
//            || obj_count_ >= DEFAULT_MAX_COLUMN_NUMBER - 1)
//        {
//          TBSYS_LOG(WARN, "invalid argument, table_id=%lu, "
//                          "rowkey_length=%ld, rowkey_ptr=%p, obj_count=%d",
//                    table_id, length, ptr, obj_count_);
//          ret = common::OB_ERROR;
//        }
//        else
//        {
//          table_id_ = table_id;
//          column_group_id_ = common::OB_DEFAULT_COLUMN_GROUP_ID;
//          memcpy(&objs_[obj_count_], ptr, length * sizeof(common::ObObj));
//          rowkey_obj_count_ = length;
//          obj_count_ += static_cast<int32_t>(length);
//        }

//        return ret;
//      }
      //del:e

      /**
       * for Compatible with binary rowkey.
       * parse binary rowkey into ObObj array,
       * then copy into row objs_ array.
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int set_binary_rowkey(const common::ObString& binary_rowkey);

      int get_binary_rowkey(common::ObString& binary_rowkey) const;

      /**
       * add one object into the row, increase the object count. user 
       * must add the objects as the order of columns in schema. it 
       * will deep copy obj. 
       * 
       * @param obj the object to add
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      inline int add_obj(const common::ObObj& obj)
      {
        int ret = common::OB_SUCCESS;

        if (obj_count_ < common::OB_MAX_COLUMN_NUMBER)
        {
          ret = string_buf_.write_obj(obj, &objs_[obj_count_++]);
        }
        else
        {
          TBSYS_LOG(WARN, "can't add obj anymore, max_count=%ld, obj_count=%d", 
                    common::OB_MAX_COLUMN_NUMBER, obj_count_);
          ret = common::OB_ERROR;
        }

        return ret;
      }

      /**
       * add one object into the row, increase the object count, it is 
       * used for sstable writer to store sparse format with shadow 
       * copy 
       * 
       * @param obj the object to add
       * @param column_id column id of this obj
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      inline int shallow_add_obj(const common::ObObj& obj, const uint64_t column_id)
      {
        int ret = common::OB_SUCCESS;

        if (column_id == common::OB_INVALID_ID
            || obj_count_ >= DEFAULT_MAX_COLUMN_NUMBER - 1)
        {
          TBSYS_LOG(WARN, "invalid column id or obj array full, "
                          "column_id=%lu, max_count=%ld, obj_count=%d", 
                    column_id, DEFAULT_MAX_COLUMN_NUMBER, obj_count_);
          ret = common::OB_ERROR;
        }
        else
        {
          objs_[obj_count_++].set_int(column_id);
          objs_[obj_count_++] = obj;
        }

        return ret;
      }




      //add zhaoqiong [bugfix::trim rowkey in varchar type in case of memtable checksum mismatch] 20160612:b
      /**
       * add one object into the row, increase the object count, it is
       * used for sstable writer to store sparse format with shadow
       * copy
       *
       * @param obj the object to add
       * @param column_id column id of this obj
       *
       * @return int if success,return OB_SUCCESS, else return
       *         OB_ERROR
       */
      inline int shallow_add_rowkey_obj(const int64_t index, const uint64_t column_id)
      {
        int ret = common::OB_SUCCESS;

        if (column_id == common::OB_INVALID_ID
            || obj_count_ >= DEFAULT_MAX_COLUMN_NUMBER - 1)
        {
          TBSYS_LOG(WARN, "invalid column id or obj array full, "
                          "column_id=%lu, max_count=%ld, obj_count=%d",
                    column_id, DEFAULT_MAX_COLUMN_NUMBER, obj_count_);
          ret = common::OB_ERROR;
        }
        else if (index < 0 || index > rowkey_obj_count_ -1 )
        {
          TBSYS_LOG(WARN, "invalid index argument, "
                          "index=%lu, rowkey_count=%ld, obj_count=%d",
                    index, rowkey_obj_count_, obj_count_);
          ret = common::OB_ERROR;
        }
        else
        {
          objs_[obj_count_++].set_int(column_id);
          objs_[obj_count_++] = objs_[index];
        }

        return ret;
      }
      //add:e


      /**
       * check if the order and type of object are consistent with 
       * schema. 
       * 
       * @param schema the schema stores the order of objects and 
       *               column type information
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int check_schema(const ObSSTableSchema& schema) const;



      //add zhaoqiong [bugfix::trim rowkey in varchar type in case of memtable checksum mismatch] 20160612
      int obj_varchar_trim(common::ObObj & obj);
      //add:e

      NEED_SERIALIZE_AND_DESERIALIZE;

      int serialize(char* buf, const int64_t buf_len, int64_t& pos, 
          const int64_t row_serialize_size) const;

    private:
      /**
       * allocate memory and ensure the space is enough to store the 
       * row key 
       * 
       * @param size the row key size to need
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int ensure_key_buf_space(const int64_t size);

      //add zhaoqiong [bugfix::trim rowkey in varchar type in case of memtable checksum mismatch] 20160612:b
      int ensure_key_string_buf_space(const int64_t size);
      //add:e

    private:
      DISALLOW_COPY_AND_ASSIGN(ObSSTableRow);
      
      uint64_t table_id_;         //table id of this row
      uint64_t column_group_id_;  //group id of this row
      int64_t rowkey_obj_count_;  //how many obj count of rowkey
      char* row_key_buf_;         //buffer to store row key char stream
      int32_t row_key_buf_size_;  //size of row key buffer


      //add zhaoqiong [bugfix::trim rowkey in varchar type in case of memtable checksum mismatch] 20160612:b
      char* row_key_string_buf_;  //buffer to store varchar obj of rowkey
      int32_t row_key_string_buf_length_; //data length of row key varchar buffer
      int32_t row_key_string_buf_size_; //size of row key varchar buffer
      int32_t row_key_string_buf_used_;
      //add:e


      /* the order of object must be the same as schema */
      int32_t obj_count_;         //count of objects in the objs array
      common::ObObj objs_[DEFAULT_MAX_COLUMN_NUMBER];
      common::ObStringBuf string_buf_;  //store the varchar of ObObj
      
      /* Parse Rowkey split */
      const common::ObRowkeyInfo* binary_rowkey_info_;
    };
  } // namespace oceanbase::sstable
} // namespace Oceanbase

#endif // OCEANBASE_SSTABLE_OB_SSTABLE_ROW_H_
