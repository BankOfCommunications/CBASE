/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_fileinfo_cache.h for file info cache. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef  OCEANBASE_CHUNKSERVER_FILECACHE_H_
#define  OCEANBASE_CHUNKSERVER_FILECACHE_H_

#include "common/ob_define.h"
#include "common/ob_kv_storecache.h"
#include "common/ob_fileinfo_manager.h"
#include "sstable/ob_disk_path.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class FileInfoBase : public common::IFileInfo
    {
      protected:
        int fd_;
    };

    class FileInfo;
    class FileInfoCache : public common::IFileInfoMgr
    {
      public:
        static const int64_t KVCACHE_BLOCK_SIZE = 
          sizeof(uint64_t) + sizeof(FileInfo*) +
          sizeof(FileInfoBase) + sizeof(common::CacheHandle);
        static const int64_t KVCACHE_ITEM_SIZE = KVCACHE_BLOCK_SIZE;
        typedef common::KeyValueCache<uint64_t, FileInfo*, KVCACHE_ITEM_SIZE, KVCACHE_BLOCK_SIZE,
                                      common::KVStoreCacheComponent::MultiObjFreeList> KVCache;
        typedef common::CacheHandle Handle;

      public:
        FileInfoCache();
        virtual ~FileInfoCache();

      public:
        int init(const int64_t max_cache_num);
        int enlarg_cache_num(const int64_t max_cache_num);
        int destroy();

      public:
        const FileInfo *get_cache_fileinfo(const uint64_t sstable_id);
        virtual const common::IFileInfo *get_fileinfo(const uint64_t sstable_id);
        virtual int revert_fileinfo(const common::IFileInfo *file_info);
        int clear();

      private:
        KVCache cache_;
    };

    class FileInfo : public FileInfoBase
    {
      static const int FILE_OPEN_NORMAL_RFLAG = O_RDONLY;
      static const int FILE_OPEN_DIRECT_RFLAG = O_RDONLY | O_DIRECT;
      typedef common::CacheHandle Handle;

      public:
        FileInfo();
        virtual ~FileInfo();
        FileInfo(const FileInfo &other);

      public:
        int init(const uint64_t sstable_id, const bool use_dio = true);
        void destroy();
        void set_cache_handle(const Handle &handle);
        const Handle &get_cache_handle() const;

      public:
        virtual int get_fd() const;

      private:
        static char *get_sstable_fname_(uint64_t sstable_id);

      private:
        Handle handle_;
    };
  }

  namespace common
  {
    namespace KVStoreCacheComponent
    {
      struct CSFileInfoPointerDeepCopyTag {};

      template <>
      struct traits<chunkserver::FileInfo*>
      {
        typedef CSFileInfoPointerDeepCopyTag Tag;
      };

      inline chunkserver::FileInfo **do_copy(chunkserver::FileInfo * const &other,
                                            char *buffer, CSFileInfoPointerDeepCopyTag)
      {
        chunkserver::FileInfo **ret = (chunkserver::FileInfo**)buffer;
        if (NULL != ret)
        {
          *ret = new(buffer + sizeof(chunkserver::FileInfo*)) chunkserver::FileInfo(*other);
        }
        return ret;
      };

      inline int32_t do_size(chunkserver::FileInfo* const &data,
                            CSFileInfoPointerDeepCopyTag)
      {
        return (sizeof(data) + sizeof(chunkserver::FileInfo));
      }

      inline void do_destroy(chunkserver::FileInfo **data,
                            CSFileInfoPointerDeepCopyTag)
      {
        using namespace chunkserver;
        (*data)->~FileInfo();
      }
    }
  }
}

#endif //OCEANBASE_CHUNKSERVER_FILECACHE_H_
