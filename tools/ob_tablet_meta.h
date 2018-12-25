/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_tablet_meta.h is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   maoqi <maoqi@taobao.com>
 *
 */

#ifndef OB_CHUNKSERVER_TABLET_META_H
#define OB_CHUNKSERVER_TABLET_META_H

#include <stdint.h>
#include <tbsys.h>
#include "common/ob_define.h"
#include "common/ob_range.h"
#include "common/page_arena.h"
#include "common/hash/ob_hashmap.h"


namespace oceanbase 
{
  namespace obsolete
  {
    namespace chunkserver 
    {
      struct ObSSTableId 
      {
        ObSSTableId():sstable_file_id_(0),sstable_file_offset_(0) {}
        ObSSTableId(uint64_t id):sstable_file_id_(id),sstable_file_offset_(0) {}
        uint64_t sstable_file_id_;
        int64_t sstable_file_offset_;

        bool operator < (const ObSSTableId& rhs) const
        {
          return ((sstable_file_id_ < rhs.sstable_file_id_) || \
              ((sstable_file_id_ == rhs.sstable_file_id_) && \
               (sstable_file_offset_ < rhs.sstable_file_offset_)));
        }

        bool operator == (const ObSSTableId& rhs) const
        {
          return (sstable_file_id_ == rhs.sstable_file_id_) &&
            (sstable_file_offset_ == rhs.sstable_file_offset_);
        }
        NEED_SERIALIZE_AND_DESERIALIZE;
      };

      struct ObSSTableId_hash_func
      {
        int64_t operator () (const ObSSTableId& id) const
        {
          common::hash::hash_func<int64_t> func;
          return func(id.sstable_file_id_ + id.sstable_file_offset_);
        };
      };

      class ObTabletMeta
      {
        public:
          static const int32_t MAX_PATH = 256;
          struct IndexEntry 
          {
            IndexEntry():sstable_id_(),sstable_file_size_(0) {}

            ObSSTableId sstable_id_;
            int64_t sstable_file_size_;
            common::ObString path_;
            common::ObRange range_;

            NEED_SERIALIZE_AND_DESERIALIZE;
          };

          struct ObMetaHead
          {
            ObMetaHead():timestamp_(0),entries_(0) {}
            //char magic_[];
            int64_t timestamp_;
            int64_t entries_;
            NEED_SERIALIZE_AND_DESERIALIZE;
          };


          ObTabletMeta();
          ~ObTabletMeta();

          typedef common::hash::ObHashMap<ObSSTableId,IndexEntry,\
            common::hash::NoPthreadDefendMode,ObSSTableId_hash_func>  META_MAP;

          typedef META_MAP::iterator iterator;

          iterator begin() {return meta_map_.begin();}
          iterator end() {return meta_map_.end();}

          int init(const char *idx_path);
          void set_timestamp(const int64_t timestamp);
          int64_t get_timestamp() const;
          int load_meta_info();
          int add_entry(IndexEntry & entry);
          int write_meta_info();
          void clear();

        private:
          NEED_SERIALIZE_AND_DESERIALIZE;

          friend bool operator==(const ObTabletMeta &lhs,const ObTabletMeta &rhs);
          int add_entry_nocopy(IndexEntry & entry);
          int clone_index_entry(IndexEntry& o,IndexEntry& d,oceanbase::common::CharArena *arena);
        private:
          bool inited;
          bool meta_loaded;
          static const int32_t DEFAULT_TABLET_NUM = 1000;

          META_MAP meta_map_;
          common::CharArena arena_;
          char idx_file_path_[MAX_PATH];
          ObMetaHead meta_head_;
          tbsys::CRWLock lock_;
#ifdef ORDER_SERIALIZE
          char *serialize_buf_;
          int64_t serialize_pos_;
          int32_t serialized_;
          static const int64_t serialize_buf_size_ =  1024 * 1024L;
#endif
      };

    } /* chunkserver */
  }
} /* oceanbase */

#endif /*OB_CHUNKSERVER_TABLET_META_H*/

