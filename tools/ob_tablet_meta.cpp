/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_tablet_meta.cpp is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   maoqi <maoqi@taobao.com>
 *
 */

#include "ob_tablet_meta.h"
#include "common/serialization.h"
#include <errno.h>
#include <map>

namespace oceanbase {
  namespace obsolete {
    namespace chunkserver {
      using namespace oceanbase::common;
      using namespace oceanbase::common::hash;

      const uint64_t DISK_NO_MASK = ((1UL << 8) - 1);

      int ObSSTableId::serialize(char *buf,int64_t buf_len,int64_t& pos) const
      {
        int64_t p = pos;
        int ret = OB_ERROR;
        if ((OB_SUCCESS == serialization::encode_vi64(buf,buf_len,p,static_cast<int64_t>(sstable_file_id_))) &&
            (OB_SUCCESS == serialization::encode_vi64(buf,buf_len,p,sstable_file_offset_)))
        {
          pos = p;
          ret = OB_SUCCESS;
        }
        return ret;
      }

      int ObSSTableId::deserialize(const char *buf,int64_t data_len,int64_t& pos)
      {
        int64_t p = pos;
        int ret = OB_ERROR;
        int64_t id = 0;
        if ((OB_SUCCESS == serialization::decode_vi64(buf,data_len,p,&id)) &&
            (OB_SUCCESS == serialization::decode_vi64(buf,data_len,p,&sstable_file_offset_)))
        {
          sstable_file_id_ = static_cast<uint64_t>(id);
          pos = p;
          ret = OB_SUCCESS;
        }
        return ret;
      }

      int64_t ObSSTableId::get_serialize_size() const
      {
        return serialization::encoded_length_vi64(static_cast<int64_t>(sstable_file_id_))\
          + serialization::encoded_length_vi64(sstable_file_offset_);
      }

      ObTabletMeta::ObTabletMeta():inited(false),meta_loaded(false)
      {
      }

      ObTabletMeta::~ObTabletMeta()
      {
      }

      int ObTabletMeta::add_entry(IndexEntry& e)
      {
        int ret = OB_ERROR;
        if (!inited)
        {
          ret = OB_ERROR;
        }
        else
        {
          IndexEntry n;
          IndexEntry tmp;
          tbsys::CWLockGuard guard(lock_);
          if (HASH_EXIST == meta_map_.get(e.sstable_id_,tmp))
          {
            ret = OB_SUCCESS;
          }
          else
          {
            clone_index_entry(e,n,&arena_);
            if ( (meta_map_.set(n.sstable_id_,n)) != -1)
            {
              ret = OB_SUCCESS;
              meta_head_.entries_ += 1;
            }
#ifdef ORDER_SERIALIZE

            if ( 0 == serialized_)
            {
              if ( OB_ERROR == (meta_head_.serialize(serialize_buf_,serialize_buf_size_,serialize_pos_)))
              {
                ret = OB_ERROR;
              }
              ++serialized_;
            }
            if ( OB_ERROR == n.serialize(serialize_buf_,serialize_buf_size_,serialize_pos_))
            {
              ret = OB_ERROR;
            }
#endif
          }
        }
        return ret;
      }

      int ObTabletMeta::add_entry_nocopy(IndexEntry& e)
      {
        int ret = OB_ERROR;
        if (!inited)
        {
          ret = OB_ERROR;
        }
        else
        {
          tbsys::CWLockGuard guard(lock_);
          if ( meta_map_.set(e.sstable_id_,e) != -1)
          {
            ret = OB_SUCCESS;
          }
        }
        return ret;
      }

      int ObTabletMeta::clone_index_entry(IndexEntry& o,IndexEntry& d,CharArena *arena)
      {
        d.sstable_id_ = o.sstable_id_;
        d.sstable_file_size_ = o.sstable_file_size_;

        ObString path_str;

        char *path = arena->alloc(o.path_.length());
        memcpy(path,o.path_.ptr(),o.path_.length());
        path_str.assign(path,o.path_.length());

        d.path_ = path_str;

        ObString start_key;
        ObString end_key;

        char *skey = arena->alloc(o.range_.start_key_.length());
        memcpy(skey,o.range_.start_key_.ptr(),o.range_.start_key_.length());
        start_key.assign(skey,o.range_.start_key_.length());

        char *ekey = arena->alloc(o.range_.end_key_.length());
        memcpy(ekey,o.range_.end_key_.ptr(),o.range_.end_key_.length());
        end_key.assign(ekey,o.range_.end_key_.length());

        d.range_.table_id_ = o.range_.table_id_;
        d.range_.border_flag_ = o.range_.border_flag_;
        d.range_.start_key_ = start_key;
        d.range_.end_key_ = end_key;

        return 0;
      }

      int ObTabletMeta::init(const char* path)
      {
        if (!inited)
        {
          tbsys::CWLockGuard guard(lock_);
          if (!inited)
          {
            meta_map_.create(DEFAULT_TABLET_NUM);
            strncpy(idx_file_path_,path,strlen(path));
            idx_file_path_[strlen(path)] = '\0';

#ifdef ORDER_SERIALIZE
            serialize_buf_ = arena_.alloc(serialize_buf_size_);
            serialize_pos_ = 0;
            serialized_ = 0;
#endif
            inited = true;
          }
        }
        return OB_SUCCESS;
      }

      int ObTabletMeta::load_meta_info()
      {
        int ret = OB_ERROR;
        int fd = -1;

        if (!inited)
        {
          ret = OB_ERROR;
        }
        else if (meta_loaded)
        {
          ret = OB_ERROR;
        }
        else if ( ('\0' == idx_file_path_[0]) || ((fd = open(idx_file_path_,O_RDONLY)) < 0))
        {
          printf("open index file fail\n");
          ret = OB_ERROR;
        }
        else
        {
          tbsys::CWLockGuard guard(lock_);
          struct stat st;
          if (fstat(fd,&st) != 0)
          {
            ret = OB_ERROR;
          }
          if (st.st_size <= 0)
          {
            ret = OB_ERROR;
          }
          else
          {
            char *buf = arena_.alloc(st.st_size);
            if (read(fd,buf,st.st_size) != st.st_size)
            {
              ret = OB_ERROR;
              arena_.free();
            }
            else
            {
              int64_t pos = 0;
              if (OB_SUCCESS == deserialize(buf,st.st_size,pos))
              {
                printf("deserialize success\n");
                ret = OB_SUCCESS;
                meta_loaded = true;
              }
            }
          }
          close(fd);
        }
        return ret;
      }

      int ObTabletMeta::write_meta_info()
      {
        int ret = OB_ERROR;

        if (!inited)
        {
          ret = OB_ERROR;
        }
        else if ('\0' == idx_file_path_[0])
        {
          ret = OB_ERROR;
        }
        else 
        {

          printf("write_meta_info\n");
          tbsys::CRLockGuard guard(lock_);
          char old_name[MAX_PATH];
          snprintf(old_name,sizeof(old_name),"%s.old",idx_file_path_);

          if (rename(idx_file_path_,old_name) != 0)
          {
            //just warn
          }

          int fd = -1;
          if ( (fd = open(idx_file_path_,O_RDWR|O_CREAT|O_TRUNC,00644)) < 0)
          {

            printf("open meta file error,%s\n",idx_file_path_);
            ret = OB_ERROR;
          }
          else
          {
#ifdef ORDER_SERIALIZE
            printf("meta_len:%ld\n",serialize_pos_);

            if (write(fd,serialize_buf_,serialize_pos_) != serialize_pos_)
            {
              ret = OB_ERROR;
            }
            else
            {
              fsync(fd);
              ret = OB_SUCCESS;
            }
#else
            int64_t meta_len = get_serialize_size();
            printf("meta_len:%ld\n",meta_len);
            char buf[meta_len];
            int64_t pos = 0;
            if (OB_SUCCESS != serialize(buf,sizeof(buf),pos))
            {
              ret = OB_ERROR;
            }
            else
            {
              if (write(fd,buf,meta_len) != meta_len)
              {
                ret = OB_ERROR;
              }
              else
              {
                fsync(fd);
                ret = OB_SUCCESS;
              }
            }
#endif
          }
          close(fd);
        }
        return ret;
      }

      void ObTabletMeta::clear()
      {
        tbsys::CWLockGuard guard(lock_);
        meta_map_.clear();
        arena_.free();
#ifdef ORDER_SERIALIZE
        serialize_buf_ = NULL;
        serialize_pos_ = 0;
        serialized_ = 0;
#endif
      }

      void ObTabletMeta::set_timestamp(const int64_t timestamp)
      {
        meta_head_.timestamp_ = timestamp;
      }

      int64_t ObTabletMeta::get_timestamp() const
      {
        return meta_head_.timestamp_;
      }

      int ObTabletMeta::serialize(char *buf,int64_t buf_len,int64_t& pos) const
      {
        int ret = OB_ERROR;
        int64_t tmp_pos = pos;
        META_MAP::const_iterator it = meta_map_.begin();

        if ( OB_ERROR ==  (meta_head_.serialize(buf,buf_len,tmp_pos)))
        {
          ret = OB_ERROR;
        }
        else
        {
          for(;it != meta_map_.end();++it)
          {
            if ( OB_ERROR == (ret = it->second.serialize(buf,buf_len,tmp_pos)))
            {
              break;
            }
          }
          if (OB_SUCCESS == ret)
          {
            pos = tmp_pos;
          }
        }
        return ret;
      }

      int ObTabletMeta::deserialize(const char *buf,int64_t data_len,int64_t& pos)
      {
        int ret = OB_ERROR;

        if (OB_ERROR == meta_head_.deserialize(buf,data_len,pos))
        {
          ret = OB_ERROR;
        }
        else
        {
          IndexEntry e;
          while( OB_SUCCESS == e.deserialize(buf,data_len,pos))
          {
            ret = add_entry_nocopy(e);
            memset(&e,0,sizeof(e));
          }
        }
        return ret;
      }

      int64_t ObTabletMeta::get_serialize_size() const
      {
        int64_t total_size = 0;
        total_size += meta_head_.get_serialize_size();
        META_MAP::const_iterator it = meta_map_.begin();
        for(;it != meta_map_.end();++it)
        {
          total_size += it->second.get_serialize_size();
        }
        return total_size;
      }


      int64_t ObTabletMeta::IndexEntry::get_serialize_size() const
      {
        return sstable_id_.get_serialize_size() + 
          serialization::encoded_length_vi64(sstable_file_size_) + \
          path_.get_serialize_size() + \
          range_.get_serialize_size();
      }

      int ObTabletMeta::IndexEntry::serialize(char *buf,int64_t buf_len,int64_t& pos) const
      {
        int ret = OB_ERROR;
        int64_t tmp_pos = pos;
        if ((OB_SUCCESS == sstable_id_.serialize(buf,buf_len,tmp_pos))
            && OB_SUCCESS == serialization::encode_vi64(buf,buf_len,tmp_pos,sstable_file_size_)
            && OB_SUCCESS == path_.serialize(buf,buf_len,tmp_pos)
            && OB_SUCCESS == range_.serialize(buf,buf_len,tmp_pos))
        {
          pos = tmp_pos;
          ret = OB_SUCCESS;
        }
        return ret;
      }

      int ObTabletMeta::IndexEntry::deserialize(const char *buf,int64_t data_len,int64_t& pos)
      {
        int64_t tmp_pos = pos;
        int ret = OB_ERROR;

        if ((OB_SUCCESS == sstable_id_.deserialize(buf,data_len,tmp_pos)) && 
            (OB_SUCCESS == serialization::decode_vi64(buf,data_len,tmp_pos,&sstable_file_size_)) &&
            (OB_SUCCESS == path_.deserialize(buf,data_len,tmp_pos)) &&
            (OB_SUCCESS == range_.deserialize(buf,data_len,tmp_pos)))
        {
          pos = tmp_pos;
          ret = OB_SUCCESS;
        }
        return ret;
      }


      int ObTabletMeta::ObMetaHead::serialize(char *buf,int64_t buf_len,int64_t& pos) const
      {
        int64_t p = pos;
        int ret = OB_ERROR;
        if ((OB_SUCCESS == serialization::encode_vi64(buf,buf_len,p,timestamp_)) &&
            (OB_SUCCESS == serialization::encode_vi64(buf,buf_len,p,entries_)))
        {
          pos = p;
          ret = OB_SUCCESS;
        }
        return ret;
      }

      int ObTabletMeta::ObMetaHead::deserialize(const char *buf,int64_t data_len,int64_t& pos)
      {
        int64_t p = pos;
        int ret = OB_ERROR;
        if ((OB_SUCCESS == serialization::decode_vi64(buf,data_len,p,&timestamp_)) &&
            (OB_SUCCESS == serialization::decode_vi64(buf,data_len,p,&entries_)))
        {
          pos = p;
          ret = OB_SUCCESS;
        }
        return ret;
      }

      int64_t ObTabletMeta::ObMetaHead::get_serialize_size() const
      {
        return serialization::encoded_length_vi64(timestamp_) \
          + serialization::encoded_length_vi64(entries_);
      }

    } /* chunkserver */
  }
} /* oceanbase */

