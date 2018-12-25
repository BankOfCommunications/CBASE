/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 *         ob_rowkey.h is for what ...
 *
 *  Version: $Id: ob_rowkey.h 11/28/2011 04:22:18 PM qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */

#ifndef OCEANBASE_COMMON_OB_ROWKEY_H_
#define OCEANBASE_COMMON_OB_ROWKEY_H_

#include <stdint.h>
#include "tblog.h"
#include "ob_define.h"
#include "ob_object.h"
#include "common/ob_array.h"  //add peiouya [IN_TYPEBUG_FIX] 20151225
namespace oceanbase
{
  namespace common
  {

    class ObBatchChecksum;
    class ObRowkeyInfo;
    class ObSchemaManagerV2;

    class ObRowkey
    {
      public:
        ObRowkey() : obj_ptr_(NULL), obj_cnt_(0) {}
        ObRowkey(ObObj* ptr, const int64_t cnt) : obj_ptr_(ptr), obj_cnt_(cnt) {}
        ~ObRowkey() {}
      public:
        inline int64_t get_obj_cnt() const { return obj_cnt_; }
        inline const ObObj* get_obj_ptr() const { return obj_ptr_; }
        // for convenience compactible with ObString
        inline int64_t length()  const { return obj_cnt_; }
        inline const ObObj* ptr() const { return obj_ptr_; }
        int64_t get_binary_key_length() const ;
        inline bool is_empty_row() const { return NULL == obj_ptr_ && 0 == obj_cnt_; }
        // is min rowkey or max rowkey
        inline bool is_min_row(void) const { return (*this == ObRowkey::MIN_ROWKEY); }
        inline bool is_max_row(void) const { return (*this == ObRowkey::MAX_ROWKEY); }
        inline void set_min_row(void) { *this = ObRowkey::MIN_ROWKEY; }
        inline void set_max_row(void) { *this = ObRowkey::MAX_ROWKEY; }
        //
        inline void assign(ObObj* ptr, const int64_t cnt)
        {
          obj_ptr_ = ptr;
          obj_cnt_ = cnt;
        }

        //add zhaoqiong [bugfix::trim rowkey in varchar type in case of memtable checksum mismatch] 20160612:b
        int64_t get_varchar_length() const;
        //add:e
        int64_t get_deep_copy_size() const;

        template <typename Allocator>
          int deep_copy(ObRowkey& rhs, Allocator& allocator) const;

        int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
        int64_t get_serialize_size(void) const;
        int deserialize(const char* buf, const int64_t buf_len, int64_t& pos);

        int serialize_objs(char* buf, const int64_t buf_len, int64_t& pos) const;
        int64_t get_serialize_objs_size(void) const;
        int deserialize_objs(const char* buf, const int64_t buf_len, int64_t& pos);

        /**
         * suppose stream %buf[%buf_len] is serialize objects array stream.
         * deserialize all objects until reach the end of %buf.
         */
        int deserialize_from_stream(const char* buf, const int64_t buf_len);
        int64_t to_string(char* buffer, const int64_t length) const;

        int64_t checksum(const int64_t current) const;
        void checksum(ObBatchChecksum& bc) const;
        uint32_t murmurhash2(const uint32_t hash) const;
        uint64_t murmurhash64A(const uint64_t hash) const;
        inline int64_t hash() const
        {
          return murmurhash2(0);
        }

      public:
        int32_t compare(const ObRowkey& rhs) const;
        int32_t compare(const ObRowkey& rhs, const ObRowkeyInfo * rowkey_info) const;

        inline bool operator<(const ObRowkey& rhs) const
        {
          return compare(rhs) < 0;
        }

        inline bool operator<=(const ObRowkey& rhs) const
        {
          return compare(rhs) <= 0;
        }

        inline bool operator>(const ObRowkey& rhs) const
        {
          return compare(rhs) > 0;
        }

        inline bool operator>=(const ObRowkey& rhs) const
        {
          return compare(rhs) >= 0;
        }

        inline bool operator==(const ObRowkey& rhs) const
        {
          return compare(rhs) == 0;
        }
        inline bool operator!=(const ObRowkey& rhs) const
        {
          return compare(rhs) != 0;
        }

        void dump(const int32_t log_level = TBSYS_LOG_LEVEL_DEBUG) const;

      private:
        ObObj* obj_ptr_;
        int64_t obj_cnt_;
      public:
        static ObObj MIN_OBJECT;
        static ObObj MAX_OBJECT;
        static ObRowkey MIN_ROWKEY;
        static ObRowkey MAX_ROWKEY;
    };

    class RowkeyInfoHolder
    {
      public:
        RowkeyInfoHolder() : rowkey_info_(NULL) {}
        RowkeyInfoHolder(const ObRowkeyInfo * ri);
        RowkeyInfoHolder(const ObSchemaManagerV2* schema, const uint64_t table_id);
        virtual ~RowkeyInfoHolder();

        void set_rowkey_info_by_schema(
            const ObSchemaManagerV2* schema, const uint64_t table_id);
        void set_rowkey_info(const ObRowkeyInfo* ri);
        inline const ObRowkeyInfo* get_rowkey_info() const { return rowkey_info_; }
      protected:
        // only for compare with rowkey_info;
        const ObRowkeyInfo * rowkey_info_;
    };

    class ObRowkeyLess : public RowkeyInfoHolder
    {
      public:
        ObRowkeyLess(const ObRowkeyInfo * ri)
          : RowkeyInfoHolder(ri) {}
        ObRowkeyLess(const ObSchemaManagerV2* schema, const uint64_t table_id)
          : RowkeyInfoHolder(schema, table_id) {}
        ~ObRowkeyLess() {}
        bool operator()(const ObRowkey& lhs, const ObRowkey &rhs) const;
        int compare(const ObRowkey& lhs, const ObRowkey &rhs) const;
    };


    template <typename Allocator>
      int ObRowkey::deep_copy(ObRowkey& rhs, Allocator& allocator) const
      {
        int ret = OB_SUCCESS;

        if (obj_cnt_ > 0 && NULL != obj_ptr_)
        {
          int64_t obj_arr_len = obj_cnt_ * sizeof(ObObj);
          int64_t total_len = get_deep_copy_size();

          char* ptr = NULL;
          ObObj* obj_ptr = NULL;
          char* varchar_ptr = NULL;
          ObString varchar_val;
          //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
          ObString decimal_val;
          //add:e
          int32_t varchar_len = 0;

          if (NULL == (ptr = (char*)allocator.alloc(total_len)))
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            TBSYS_LOG(WARN, "allocate mem for obj array fail:total_len[%ld]", total_len);
          }
          else
          {
            obj_ptr = new(ptr) ObObj[obj_cnt_];
            varchar_ptr = ptr + obj_arr_len;
          }

          for (int64_t i = 0; i < obj_cnt_ && OB_SUCCESS == ret; ++i)
          {
            obj_ptr[i] = obj_ptr_[i];  // copy object
            if (obj_ptr_[i].get_type() == ObVarcharType)
            {
              obj_ptr_[i].get_varchar(varchar_val);
              varchar_len = obj_ptr_[i].get_val_len();

              memcpy(varchar_ptr, varchar_val.ptr(), varchar_len);
              varchar_val.assign_ptr(varchar_ptr, varchar_len);

              obj_ptr[i].set_varchar(varchar_val);
              varchar_ptr += varchar_len;
            }
            //add wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_14:b
            else if(obj_ptr_[i].get_type() == ObDecimalType){
                obj_ptr_[i].get_decimal(decimal_val);
                varchar_len = obj_ptr_[i].get_val_len();

                memcpy(varchar_ptr, decimal_val.ptr(), varchar_len);
                decimal_val.assign_ptr(varchar_ptr,varchar_len);

                obj_ptr[i].set_decimal(decimal_val,obj_ptr_[i].get_precision(),obj_ptr_[i].get_scale(),obj_ptr_[i].get_vscale());
                varchar_ptr += varchar_len;
            }
            //add e
          }

          if (OB_SUCCESS == ret)
          {
            rhs.assign(obj_ptr, obj_cnt_);
          }
        }
        else
        {
          rhs.assign(NULL, 0);
        }

        return ret;

      }

    inline std::ostream & operator<<(std::ostream &os, const ObRowkey& key) // for google test
    {
      os << " len=" << key.get_obj_cnt();
      return os;
    }

    template <typename AllocatorT>
    int ob_write_rowkey(AllocatorT &allocator, const ObRowkey &src, ObRowkey &dst)
    {
      return src.deep_copy(dst, allocator);
    }

    class ObRowkeyInfo;
    int ob_cast_rowkey(const ObRowkeyInfo &rowkey_info, ObRowkey &rowkey,
                       char* buf, int64_t buf_size, int64_t &used_buf_len);
    int ob_cast_rowkey_need_buf(const ObRowkeyInfo &rowkey_info, ObRowkey &rowkey, bool &need);
    //add peiouya [IN_TYPEBUG_FIX] 20151225:b
    int ob_cast_rowkey(const common::ObArray<common::ObObjType>* expect_rowkey_type_desc, ObRowkey &rowkey_src,
                                       ObObj *casted_objs, ObObj *rowkery_dst);
    bool is_arr_contain_null_value(ObObj* obj_ptr, int64_t obj_cnt);  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
#define bind_memptr_for_casted_cells(casted_cells, cells_count, varchar_mem)  \
     for (int64_t idx = 0;  idx < cells_count; idx++)  \
     {  \
       ObString varchar;  \
       varchar.assign_ptr(varchar_mem + OB_MAX_VARCHAR_LENGTH * idx, OB_MAX_VARCHAR_LENGTH);  \
       casted_cells[idx].set_varchar(varchar);  \
     }
#define alloc_small_mem_return_ptr(varchar_mem, rowkey_count, ret) \
     if (NULL == (varchar_mem = (char*)ob_malloc(OB_MAX_VARCHAR_LENGTH*(rowkey_count), ObModIds::OB_SQL_EXECUTER)))  \
     {  \
       TBSYS_LOG(ERROR, "failed to malloc memory");  \
       ret = OB_ALLOCATE_MEMORY_FAILED;  \
     }
//mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
//add new prameter is_subquery_result_contain_null
#define cast_and_set_hashmap(expect_type_arr,  src_columns, rowkey_count, casted_cells, dst_value, hashmap, arena, is_subquery_result_contain_null,ret)  \
     if (OB_SUCCESS ==ret && OB_SUCCESS != (ret =  \
     ob_cast_rowkey(expect_type_arr, src_columns, casted_cells, dst_value)))  \
     {  \
        break;  \
     }  \
     /*add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b*/  \
     else if(is_arr_contain_null_value(dst_value, rowkey_count))  \
     {  \
       is_subquery_result_contain_null = true;  \
     }  \
     /*add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e*/  \
     else  \
     {  \
       ObRowkey tmp_columns;  \
       tmp_columns.assign(dst_value,rowkey_count);  \
       ObRowkey rst_columns;  \
       if(OB_SUCCESS != (ret = tmp_columns.deep_copy(rst_columns, arena)))  \
       {  \
         TBSYS_LOG(WARN, "fail to deep copy column");  \
         break;  \
       }  \
       hashmap.set(rst_columns, rst_columns);  \
     }
#define cast_and_set_hashmap_and_bloomfilter(expect_type_arr,  src_columns, rowkey_count, casted_cells, dst_value, hashmap, bloom_filter1, bloom_filter2, arena, is_subquery_result_contain_null,ret)  \
       if (OB_SUCCESS ==ret && OB_SUCCESS != (ret =  \
       ob_cast_rowkey(expect_type_arr, src_columns, casted_cells, dst_value)))  \
       {  \
          break;  \
       }  \
       /*add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b*/  \
       else if(is_arr_contain_null_value(dst_value, rowkey_count))\
       {  \
         is_subquery_result_contain_null = true;   \
       }  \
       /*add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e*/  \
       else  \
       {  \
          ObRowkey tmp_columns;  \
          tmp_columns.assign(dst_value,rowkey_count);  \
          ObRowkey rst_columns;  \
          if(OB_SUCCESS != (ret = tmp_columns.deep_copy(rst_columns, arena)))  \
          {  \
             TBSYS_LOG(WARN, "fail to deep copy column");  \
            break;  \
          }  \
          hashmap.set(rst_columns, rst_columns);  \
          bloom_filter1->insert(rst_columns);  \
          bloom_filter2->insert(rst_columns);  \
       }
    //add 20151225:e
//mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
  }
}


#endif //OCEANBASE_COMMON_OB_ROWKEY_H_
