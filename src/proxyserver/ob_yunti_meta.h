/**
 * (C) 2010-2011 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  version 2 as published by the Free Software Foundation.
 *
 *  ob_yunti_meta.h
 *
 *  Authors:
 *    zian <yunliang.shi@alipay.com>
 *
 */


#ifndef OCEANBASE_YUNTIMETA_H_
#define OCEANBASE_YUNTIMETA_H_
#include "common/ob_define.h"
#include "common/ob_obj_type.h"
#include "common/ob_vector.h"
#include "common/ob_range2.h"
#include "common/ob_array.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_atomic.h"

namespace oceanbase
{
  using namespace common;
  namespace proxyserver
  {
    class YuntiProxy;

    struct columnInfo
    {
      common::ObObjType type;
      int32_t limit_len;
    };

    struct MetaInfo
    {
      MetaInfo();
      void reset();
      int16_t sstable_version_;
      char sstable_dir_[OB_MAX_FILE_NAME_LENGTH];
      common::ObSortedVector<common::ObNewRange*> range_list_;
      common::ObArray<common::ObRowkey*> endkey_list_;
      char delimeter_;
      columnInfo column_info_[common::OB_MAX_COLUMN_NUMBER];
      int column_num_;
      uint64_t table_id_;
      char table_name_[common::OB_MAX_TABLE_NAME_LENGTH];
      volatile uint32_t ref_count_ CACHE_ALIGNED;
      common::ModulePageAllocator mod_;
      common::ModuleArena allocator_;
      int64_t to_string(char* buffer, const int64_t length) const;
      int inc_ref(){return atomic_inc(&ref_count_);}
      int dec_ref(){return atomic_dec(&ref_count_);}
      int get_ref(){return ref_count_;}
    };

    bool unique_range(const ObNewRange* lhs, const common::ObNewRange* rhs);
    bool equal_range(const ObNewRange* lhs, const ObNewRange* rhs);
    bool compare_range(const ObNewRange* lhs, const ObNewRange* rhs);
    int transform_date_to_time(const char *str, ObDateTime& val);
    int drop_esc_char(char *buf,int32_t& len);
    typedef int (*parse_one_line)(char* data, int32_t len, MetaInfo &meta_info);

    class YuntiMeta
    {
      public:
        static const int64_t DEFAULT_META_NUM = 100;
        static const char* TABLE_INFO_FILE_NAME;
        static const int64_t DOUBLE_MULTIPLE_VALUE = 100;
        static const char* PARTITION_FILE_NAME;
        typedef hash::ObHashMap<common::ObString, MetaInfo*> hashmap;

        YuntiMeta(){}
        ~YuntiMeta();
        int initialize(YuntiProxy* yunti_proxy, const int32_t mem_limit);
        int fetch_range_list(const char* table_name, const char* sstable_dir, common::ObArray<common::ObNewRange*> &range_list);
        int get_sstable_version(const char* sstable_dir, int16_t &sstable_version);
        int get_sstable_file_path(const char* sstable_dir, const ObNewRange &range, char* sstable_file_path, int len);
      private:
        int get_meta_info(const char* sstable_dir, MetaInfo* &meta_info);
        int create_meta_info(const char* sstable_dir, MetaInfo* &meta_info);
        static int parse_table_info_one_line(char* line, const int32_t len, MetaInfo &meta_info);
        static int parse_partition_one_line(char* line, const int32_t len, MetaInfo &meta_info);
        int parse_file(const char* dir, const char* file_name, parse_one_line parser, MetaInfo &meta_info);
        MetaInfo* evict();
      private:
        YuntiProxy* yunti_proxy_;
        int64_t used_mem_size_;
        int64_t mem_limit_;
        mutable tbsys::CRWLock lock_;

        //use for evict
        common::ObArray<MetaInfo*> meta_infos_;
        hashmap meta_map_;
    };
  }
}

#endif
