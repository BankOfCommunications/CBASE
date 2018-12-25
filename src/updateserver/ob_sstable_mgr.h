////===================================================================
 //
 // ob_sstable_mgr.h updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2011-03-23 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 // sstable文件管理器
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_UPDATESERVER_SSTABLE_MGR_H_
#define  OCEANBASE_UPDATESERVER_SSTABLE_MGR_H_
#include <sys/types.h>
#include <dirent.h>
#include <sys/vfs.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_atomic.h"
#include "common/ob_define.h"
#include "common/ob_vector.h"
#include "common/page_arena.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_list.h"
#include "common/ob_regex.h"
#include "common/ob_fileinfo_manager.h"
#include "common/ob_fetch_runnable.h"
#include "common/ob_spin_rwlock.h"
#include "sstable/ob_sstable_row.h"
#include "sstable/ob_sstable_schema.h"
#include "ob_ups_utils.h"
#include "ob_store_mgr.h"
#include "ob_schema_mgrv2.h"

#define SSTABLE_SUFFIX ".sst"
#define SCHEMA_SUFFIX ".schema"
#define SSTABLE_FNAME_REGEX "^[0-9]+_[0-9]+-[0-9]+_[0-9]+.sst$"

namespace oceanbase
{
  namespace updateserver
  {
    struct SSTFileInfo
    {
      common::ObString path;
      common::ObString name;
    };
  } // end namespace updateserver

  namespace common
  {
    template <>
    struct ob_vector_traits<updateserver::SSTFileInfo>
    {
    public:
      typedef updateserver::SSTFileInfo& pointee_type;
      typedef updateserver::SSTFileInfo value_type;
      typedef const updateserver::SSTFileInfo const_value_type;
      typedef value_type* iterator;
      typedef const value_type* const_iterator;
      typedef int32_t difference_type;
    };
  } // end namespace common

  namespace updateserver
  {
    // 监控sstable文件变化的观察者接口
    class ISSTableObserver
    {
      public:
        virtual ~ISSTableObserver() {};
      public:
        // 扫描磁盘有新加入的的sstable时 会回调
        virtual int add_sstable(const uint64_t sstable_id) = 0;
        // sstable所有副本都失效时 会回调
        virtual int erase_sstable(const uint64_t sstable_id) = 0;
    };

    // 可用于dump单个memtable或做多个sstable的compaction
    class IRowIterator
    {
      public:
        virtual ~IRowIterator() {};
      public:
        virtual int next_row() = 0;
        virtual int get_row(sstable::ObSSTableRow &sstable_row) = 0;
        virtual int reset_iter() = 0;
        virtual bool get_compressor_name(common::ObString &compressor_str) = 0;
        virtual bool get_sstable_schema(sstable::ObSSTableSchema &sstable_schema) = 0;
        virtual const common::ObRowkeyInfo *get_rowkey_info(const uint64_t table_id) const = 0;
        virtual bool get_store_type(int &store_type) = 0;
        virtual bool get_block_size(int64_t &block_size) = 0;
    };

    typedef common::ObVector<SSTFileInfo> SSTList;

    /// Fetch线程需要获取的日志号范围, checkpoint号, SSTable文件列表
    /// 当一次checkpoint有多个文件时, ckpt_ext_数组描述这多个文件的后缀名
    /// Fetch线程会将ckpt_id_下的多个不同后缀名的checkpoint文件都获取到
    /// SSTable文件会自动RAID到多个目录
    struct ObUpsFetchParam : public common::ObFetchParam
    {
      bool fetch_sstable_; // whether to fetch sstables

      SSTList sst_list_;
      common::ObStringBuf string_buf_;

      int add_sst_file_info(const SSTFileInfo &sst_file_info);

      int clone(const ObUpsFetchParam& rp);

      ObUpsFetchParam() : fetch_sstable_(false), sst_list_() {}

      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    // 管理单个文件句柄的类 实现IFileInfo的虚函数
    class SSTableInfo;
    class StoreInfo : public common::IFileInfo
    {
      static const int FILE_OPEN_DIRECT_RFLAG = O_RDONLY | O_DIRECT;
      static const int FILE_OPEN_NORMAL_RFLAG = O_RDONLY;
      public:
        StoreInfo();
        virtual ~StoreInfo();
      public:
        virtual int get_fd() const;
      public:
        void init(SSTableInfo *sstable_info, const StoreMgr::Handle store_handle);
        void destroy();
        const char *get_dir() const;
        bool store_handle_match(const StoreMgr::Handle other) const;
        int64_t inc_ref_cnt();
        int64_t dec_ref_cnt();
        void remove_sstable_file();
        SSTableInfo *get_sstable_info() const;
        StoreMgr::Handle get_store_handle() const;
      private:
        int get_fd_(int &fd, int mode) const;
        void remove_schema_file_(const char *path, const char *fname_substr);
      private:
        SSTableInfo *sstable_info_;
        StoreMgr::Handle store_handle_;
        mutable int fd_;
        volatile int64_t ref_cnt_;
    };

    // 管理一个sstable的类 内含多个store_info
    class SSTableMgr;
    class SSTableInfo
    {
      public:
        static const int64_t MAX_SUBSTR_SIZE = 64;
        static const int64_t MAX_STORE_NUM = 20;
      public:
        SSTableInfo();
        ~SSTableInfo();
      public:
        int init(SSTableMgr *sstable_mgr, const uint64_t sstable_id, const uint64_t clog_id);
        void destroy();
        int add_store(const StoreMgr::Handle store_handle);
        int erase_store(const StoreMgr::Handle store_handle);
        int64_t inc_ref_cnt();
        int64_t dec_ref_cnt();
        int64_t get_store_num() const;
        uint64_t get_sstable_id() const;
        uint64_t get_clog_id() const;
        void remove_sstable_file();
        StoreInfo *get_store_info();
        SSTableMgr *get_sstable_mgr() const;
        const char *get_fname_substr() const;
        void log_sstable_info() const;
      private:
        volatile int64_t ref_cnt_;
        SSTableMgr *sstable_mgr_;
        uint64_t sstable_id_;
        uint64_t clog_id_;
        int64_t loop_pos_;
        int64_t store_num_;
        StoreInfo *store_infos_[MAX_STORE_NUM];
    };

    struct SSTableID
    {
      static const uint64_t MINOR_VERSION_BIT = 32;
      static const uint64_t MAX_MAJOR_VERSION = (1UL<<32) - 1;
      static const uint64_t MAX_MINOR_VERSION = (1UL<<16) - 1;
      static const uint64_t MAX_CLOG_ID = INT64_MAX;
      static const uint64_t START_MAJOR_VERSION = 2;
      static const uint64_t START_MINOR_VERSION = 1;
      union
      {
        uint64_t id;
        struct
        {
          uint64_t minor_version_end:16;
          uint64_t minor_version_start:16;
          uint64_t major_version:32;
        };
      };
      SSTableID() : id(0)
      {
      };
      SSTableID(const uint64_t other_id)
      {
        id = other_id;
      };
      static inline uint64_t get_major_version(const uint64_t id)
      {
        SSTableID sst_id = id;
        return sst_id.major_version;
      };
      static inline uint64_t get_minor_version_start(const uint64_t id)
      {
        SSTableID sst_id = id;
        return sst_id.minor_version_start;
      };
      static inline uint64_t get_minor_version_end(const uint64_t id)
      {
        SSTableID sst_id = id;
        return sst_id.minor_version_end;
      };
      static inline uint64_t get_id(const uint64_t major_version,
                                    const uint64_t minor_version_start,
                                    const uint64_t minor_version_end)
      {
        SSTableID sst_id;
        //sst_id.major_version = major_version;
        sst_id.id = major_version << MINOR_VERSION_BIT;
        sst_id.minor_version_start = static_cast<uint16_t>(minor_version_start);
        sst_id.minor_version_end = static_cast<uint16_t>(minor_version_end);
        return sst_id.id;
      };
      static inline const char *log_str(const uint64_t id)
      {
        SSTableID sst_id = id;
        return sst_id.log_str();
      };
      inline bool continous(const SSTableID &other) const
      {
        bool bret = false;
        if (major_version == other.major_version)
        {
          if ((minor_version_end + 1) == other.minor_version_start)
          {
            bret = true;
          }
        }
        else
        {
          if (START_MINOR_VERSION == other.minor_version_start)
          {
            bret = true;
          }
        }
        return bret;
      };
      inline const char *log_str() const
      {
        static const int64_t BUFFER_SIZE = 128;
        static __thread char buffers[2][BUFFER_SIZE];
        static __thread uint64_t i = 0;
        char *buffer = buffers[i++ % 2];
        buffer[0] = '\0';
        snprintf(buffer, BUFFER_SIZE, "sstable_id=%lu name=[%lu_%lu-%lu]",
                id, major_version, minor_version_start, minor_version_end);
        return buffer;
      };
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const
      {
        return common::serialization::encode_i64(buf, buf_len, pos, (int64_t)id);
      };
      int deserialize(const char* buf, const int64_t data_len, int64_t& pos)
      {
        return common::serialization::decode_i64(buf, data_len, pos, (int64_t*)&id);
      };
      int64_t get_serialize_size(void) const
      {
        return common::serialization::encoded_length_i64((int64_t)id);
      };
      static inline int compare(const SSTableID &a, const SSTableID &b)
      {
        int ret = 0;
        if (a.major_version > b.major_version)
        {
          ret = 1;
        }
        else if (a.major_version < b.major_version)
        {
          ret = -1;
        }
        else if (a.minor_version_start > b.minor_version_start)
        {
          ret = 1;
        }
        else if (a.minor_version_start < b.minor_version_start)
        {
          ret = -1;
        }
        else if (a.minor_version_end > b.minor_version_end)
        {
          ret = 1;
        }
        else if (a.minor_version_end < b.minor_version_end)
        {
          ret = -1;
        }
        else
        {
          ret = 0;
        }
        return ret;
      };
      static uint64_t trans_format_v1(const uint64_t id)
      {
        union SSTableIDV1
        {
          uint64_t id;
          struct
          {
            uint64_t minor_version_end:8;
            uint64_t minor_version_start:8;
            uint64_t major_version:48;
          };
        };
        SSTableIDV1 v1;
        SSTableID sst_id;
        v1.id = id;
        //sst_id.major_version = v1.major_version;
        uint64_t major_version = v1.major_version;
        sst_id.id = (major_version << MINOR_VERSION_BIT);
        sst_id.minor_version_start = v1.minor_version_start;
        sst_id.minor_version_end = v1.minor_version_end;
        return sst_id.id;
      };
    };

    struct LoadBypassInfo
    {
      char fname[common::OB_MAX_FILE_NAME_LENGTH];
      StoreMgr::Handle store_handle;
      static bool cmp(const LoadBypassInfo *a, const LoadBypassInfo *b)
      {
        return (NULL != a) && (NULL != b) && (strcmp(a->fname, b->fname) < 0);
      };
      bool operator ==(const LoadBypassInfo &other) const
      {
        return 0 == strcmp(fname, other.fname);
      };
    };

    // 管理多个sstable的类
    class SSTableMgr : public common::IFileInfoMgr
    {
      static const int64_t STORE_NUM = 10;
      static const int64_t SSTABLE_NUM = 1024;
      typedef common::hash::ObHashMap<StoreMgr::Handle, int64_t> StoreRefMap;
      typedef common::hash::ObHashMap<uint64_t, SSTableInfo*> SSTableInfoMap;
      typedef common::ObList<ISSTableObserver*> ObserverList;
      typedef common::hash::SimpleAllocer<SSTableInfo> SSTableInfoAllocator;
      typedef common::hash::SimpleAllocer<StoreInfo> StoreInfoAllocator;
      public:
        SSTableMgr();
        virtual ~SSTableMgr();
      public:
        int init(const char *store_root, const char *raid_regex, const char *dir_regex);
        void destroy();
      public:
        virtual const common::IFileInfo *get_fileinfo(const uint64_t sstable_id);
        virtual int revert_fileinfo(const common::IFileInfo *file_info);
        int get_schema(const uint64_t sstable_id, CommonSchemaManagerWrapper &sm);
      public:
        // 需要dump新的sstable时调用 阻塞
        int add_sstable(const uint64_t sstable_id, const uint64_t clog_id, const int64_t time_stamp,
                        IRowIterator &iter, const CommonSchemaManager *sm);
        // TableMgr卸载sstable的时候调用 将sstable改名到trash目录
        // 会被TableMgr和StoreMgr调用
        int erase_sstable(const uint64_t sstable_id, const bool remove_sstable_file);
        // 扫描新加入的磁盘加载sstable 工作线程调用
        bool load_new();
        // 重新加载所有的存储目录
        void reload_all();
        // 重新加载指定的存储目录
        void reload(const StoreMgr::Handle store_handle);
        // 检查磁盘损坏 并卸载损坏的磁盘 工作线程调用
        void check_broken();
        // 手动卸载磁盘 异步 (调用report broken)
        void umount_store(const char *path);
        // 注册sstable发生变化的观察者
        int reg_observer(ISSTableObserver *observer);

        // master调用 根据传入的min_sstable_id和max_sstable_id决定需要拷贝的sstable文件
        int fill_fetch_param(const uint64_t min_sstable_id, const uint64_t max_sstable_id,
                             const uint64_t max_fill_major_num, ObUpsFetchParam &fetch_param);

        // slave调用
        uint64_t get_min_sstable_id();
        uint64_t get_max_sstable_id();

        // 如果不存在上次的major frozen点，返回0
        uint64_t get_last_major_frozen_clog_file_id();
        // master调用 用来决定自己的回放点
        // slave调用来传给master
        uint64_t get_max_clog_id();

        int load_sstable_bypass(const uint64_t major_version,
                                const uint64_t minor_version_start,
                                const uint64_t minor_version_end,
                                const uint64_t clog_id,
                                common::ObList<uint64_t> &table_list,
                                uint64_t &checksum);

        static const char *build_str(const char *fmt, ...);

        static const char *sstable_id2str(const uint64_t sstable_id, const uint64_t clog_id);

        static bool sstable_str2id(const char *sstable_str, uint64_t &sstable_id, uint64_t &clog_id);

        inline StoreMgr &get_store_mgr()
        {
          return store_mgr_;
        };

        inline SSTableInfoAllocator &get_sstable_allocator()
        {
          return sstable_allocator_;
        };

        inline StoreInfoAllocator &get_store_allocator()
        {
          return store_allocator_;
        };

        void log_sstable_info();
      private:
        bool copy_sstable_file_(const uint64_t sstable_id, const uint64_t clog_id,
                                const StoreMgr::Handle src_handle, const StoreMgr::Handle dest_handle);
        bool build_sstable_file_(const uint64_t sstable_id, const common::ObString &fpaths,
                                const int64_t time_stamp, IRowIterator &iter);
        bool build_multi_sstable_file_(const common::ObList<StoreMgr::Handle> &store_list,
                                      SSTableInfo &sstable_info, const int64_t time_stamp,
                                      IRowIterator &iter, const CommonSchemaManager *sm);
        bool build_schema_file_(const char *path, const char *fname_substr, const CommonSchemaManager *sm);
        void add_sstable_file_(const uint64_t sstable_id,
                              const uint64_t clog_id,
                              const StoreMgr::Handle store_handle,
                              const bool invoke_callback);
        bool sstable_exist_(const uint64_t sstable_id);
        void add_sstable_callback_(const uint64_t sstable_id);
        void erase_sstable_callback_(const uint64_t sstable_id);
        void load_dir_(const StoreMgr::Handle store_handle);
        bool check_sstable_(const char *fpath, uint64_t *sstable_checksum);
        int prepare_load_info_(const common::ObList<StoreMgr::Handle> &store_list,
                              common::CharArena &allocator,
                              common::ObList<LoadBypassInfo*> &info_list);
        int info_list_uniq_(common::CharArena &allocator,
                            common::ObList<LoadBypassInfo*> &info_list);
        void load_list_bypass_(const common::ObList<LoadBypassInfo*> &info_list,
                              const uint64_t major_version,
                              const uint64_t minor_version_start,
                              const uint64_t minor_version_end,
                              const uint64_t clog_id,
                              common::ObList<uint64_t> &sstable_list,
                              uint64_t &checksum);
      private:
        bool inited_;

        SSTableInfoMap sstable_info_map_;   // sstable_id到SSTableInfo的map
        common::SpinRWLock map_lock_;               // 保护多线程操作sstable_info_map的读写锁

        StoreMgr store_mgr_;                // 管理多磁盘的管理器

        common::ObRegex sstable_fname_regex_;       // sstable文件名正则表达式

        ObserverList observer_list_;                // 注册的观察者链表
        SSTableInfoAllocator sstable_allocator_;    // 用于分配SSTableInfo的分配器
        StoreInfoAllocator store_allocator_;        // 用于分配StoreInfo的分配器
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_SSTABLE_MGR_H_

