////===================================================================
 //
 // ob_store_mgr.h updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2011-03-16 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 // 多磁盘管理器
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_UPDATESERVER_STORE_MGR_H_
#define  OCEANBASE_UPDATESERVER_STORE_MGR_H_
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
#include "ob_ups_utils.h"

#define TRASH_DIR "trash"
#define BYPASS_DIR "bypass"

namespace oceanbase
{
  namespace updateserver
  {
    class StoreMgr
    {
      public:
        typedef uint64_t Handle;
        static const uint64_t INVALID_HANDLE = UINT64_MAX;
        static const int64_t MAX_DIR_NAME_LENGTH = 2048;
        static const mode_t TRASH_DIR_MODE = S_IRWXU | S_IRWXG | S_IRWXO;
      private:
        static const int64_t DEVICE_SET_SIZE = 10;
        static const double DEFAULT_DISK_RESERVED_RATIO = 0.1;
        static const int64_t DEFAULT_DISK_RESERVED_SPACE = 10L * 1024L * 1024L * 1024L;
        static const int64_t DEFAULT_DISK_ENFILE_INTERVAL_TIME = 10 * 60 * 1000 * 1000;
        struct StoreNode;
        struct RaidNode;
        typedef common::hash::ObHashMap<dev_t, Handle> DeviceMap;
        typedef common::ObVector<StoreNode*> StoreVector;
        typedef common::ObList<StoreNode*> StoreList;
        typedef common::ObList<RaidNode*> RaidList;
        typedef common::PageArena<char> Allocator;
        enum StoreStat
        {
          USING = 0,
          BROKEN = 1,
          UMOUNTED = 2,
        };
        struct StoreNode
        {
          char path[MAX_DIR_NAME_LENGTH];
          StoreStat stat;
          Handle store_handle;
          dev_t dev_no;
          int64_t last_enfile_time;
        };
        struct RaidNode
        {
          char path[MAX_DIR_NAME_LENGTH];
          StoreList node_list;
          RaidNode() : node_list()
          {
          };
        };
        class StoreNodeBuilder
        {
          public:
            StoreNodeBuilder(StoreMgr &store_mgr, RaidNode &raid_node);
            void handle(const char *path);
            bool match(const char *path);
          private:
            StoreMgr &store_mgr_;
            RaidNode &raid_node_;
        };
        class RaidNodeBuilder
        {
          public:
            RaidNodeBuilder(StoreMgr &store_mgr);
            void handle(const char *path);
            bool match(const char *path);
          private:
            StoreMgr &store_mgr_;
        };
        friend class StoreNodeBuilder;
        friend class RaidNodeBuilder;
      public:
        StoreMgr();
        ~StoreMgr();
      public:
        int init(const char *store_root, const char *raid_regex, const char *dir_regex);
        void destroy();
      public:
        // 磁盘预留空间的百分比
        int set_disk_reserved_ratio(const int64_t disk_reserved_ratio);
        int set_disk_enfile_interval_time(const int64_t disk_enfile_interval_time);
      public:
        // 分配一个磁盘目录用于写sstable 不可重入
        int assign_stores(common::ObList<Handle> &store_list);
        // 有磁盘损坏时调用
        void report_broken(const Handle store_handle);
        void report_broken(const char *path);
        // 文件系统打开文件过多时调用
        void report_enfile(const Handle store_handle);
        // 有文件关闭时调用
        void report_fclose(const Handle store_handle);
        // 已汇报磁盘损坏 并且这个磁盘上的文件都关闭时调用
        void umount(const Handle store_handle);
        // 获取一个已损坏的磁盘目录 不可重入
        const char *get_broken_dir(Handle &store_handle);
        // 获取一个待加载的磁盘目录 不可重入
        const char *get_new_dir(Handle &store_handle);
        // 根据storehandle获取磁盘目录名
        const char *get_dir(const Handle store_handle) const;
        // 从root目录加载新的磁盘目录
        bool load();
        // 打印所有磁盘目录信息
        void log_store_info();
        // 获取当前所有有效的磁盘
        int get_all_valid_stores(common::ObList<Handle> &store_list);
      public:
        static inline bool disk_broken(const uint32_t errno);
        static inline bool is_dir(const char *name);
        static inline bool is_access(const char *name);
        static inline bool is_symlink(const char *name);
        static inline bool is_rfile(const char *name);
        static inline dev_t get_dev(const char *name);
        static inline int64_t get_free(const char *name);
        static inline int64_t get_total(const char *name);
        static inline int64_t get_mtime(const int fd);
        static inline bool build_dir(const char *path, const char *dir);
      private:
        template <class Func>
        static void lookup_dir_(const char *root, Func &func);
        void *alloc_(const int64_t nbyte)
        {
          return allocator_.alloc(nbyte);
        };
        void free_(void *ptr)
        {
          UNUSED(ptr);
        };
        StoreNode *get_store_node_(const Handle store_handle) const;
        RaidNode *get_raid_node_(const char *path) const;
        bool filt_store_(const RaidNode &raid_node, common::ObList<Handle> &store_list);
        void erase_store_(const StoreNode *store_node);
      private:
        bool inited_;
        Allocator allocator_;
        mutable pthread_mutex_t mutex_;
        double disk_reserved_ratio_; // 磁盘剩余空间比例
        int64_t disk_reserved_space_;
        int64_t disk_enfile_interval_time_; // 磁盘出现打开文件过多后 重试的时间间隔
        char store_root_str_[MAX_DIR_NAME_LENGTH];
        char raid_regex_str_[MAX_DIR_NAME_LENGTH];
        char dir_regex_str_[MAX_DIR_NAME_LENGTH];
        common::ObRegex raid_regex_;
        common::ObRegex dir_regex_;
        DeviceMap store_dev_;
        StoreVector all_store_;
        StoreList new_store_;
        StoreList broken_store_;
        RaidList valid_raid_;
    };

    template <class Func>
    void StoreMgr::lookup_dir_(const char *root, Func &func)
    {
      DIR *dd = NULL;
      if (NULL == (dd = opendir(root)))
      {
        TBSYS_LOG(WARN, "open rootdir [%s] fail errno=%u", root, errno);
      }
      else
      {
        struct dirent* dir_iter = NULL;
        char tmp_buffer[MAX_DIR_NAME_LENGTH];
        while (NULL != (dir_iter = readdir(dd))
              && NULL != dir_iter->d_name)
        {
          if (0 == strcmp(dir_iter->d_name, ".")
              || 0 == strcmp(dir_iter->d_name, ".."))
          {
            TBSYS_LOG(INFO, "skip [%s/%s]", root, dir_iter->d_name);
          }
          else if (!func.match(dir_iter->d_name))
          {
            TBSYS_LOG(INFO, "dname [%s/%s] do not match regex", root, dir_iter->d_name);
          }
          else if (MAX_DIR_NAME_LENGTH <= snprintf(tmp_buffer, MAX_DIR_NAME_LENGTH, "%s/%s", root, dir_iter->d_name))
          {
            TBSYS_LOG(WARN, "build entire path [%s/%s] fail", root, dir_iter->d_name);
          }
          else if (!is_access(tmp_buffer)
                  || !is_dir(tmp_buffer))
          {
            TBSYS_LOG(INFO, "dname [%s/%s] cannot access or not dir", root, tmp_buffer);
          }
          else
          {
            func.handle(tmp_buffer);
          }
        }
        closedir(dd);
      }
    }

    bool StoreMgr::is_dir(const char *name)
    {
      bool bret = false;
      struct stat st;
      if (NULL != name
          && 0 == stat(name, &st)
          && S_ISDIR(st.st_mode))
      {
        bret = true;
      }
      return bret;
    }
    
    bool StoreMgr::is_access(const char *name)
    {
      bool bret = false;
      if (NULL != name)
      {
        if (is_dir(name)
            && 0 == access(name, R_OK | X_OK))
        {
          bret = true;
        }
        else if (0 == access(name, R_OK))
        {
          bret = true;
        }
      }
      return bret;
    }
    
    bool StoreMgr::is_symlink(const char *name)
    {
      bool bret = false;
      struct stat st;
      if (NULL != name
          && 0 == lstat(name, &st)
          && S_ISLNK(st.st_mode))
      {
        bret = true;
      }
      return bret;
    }

    bool StoreMgr::is_rfile(const char *name)
    {
      bool bret = false;
      struct stat st;
      if (NULL != name
          && 0 == lstat(name, &st)
          && S_ISREG(st.st_mode))
      {
        bret = true;
      }
      return bret;
    }
    
    dev_t StoreMgr::get_dev(const char *name)
    {
      dev_t ret = 0;
      struct stat st;
      if (NULL != name
          && 0 == stat(name, &st))
      {
        ret = st.st_dev;
      }
      return ret;
    }

    int64_t StoreMgr::get_free(const char *name)
    {
      int64_t ret = 0;
      struct statfs fsst;
      if (NULL != name
          && 0 == statfs(name, &fsst))
      {
        ret = fsst.f_bsize * fsst.f_bavail;
      }
      return ret;
    }

    int64_t StoreMgr::get_total(const char *name)
    {
      int64_t ret = 0;
      struct statfs fsst;
      if (NULL != name
          && 0 == statfs(name, &fsst))
      {
        ret = fsst.f_bsize * fsst.f_blocks;
      }
      return ret;
    }

    inline int64_t StoreMgr::get_mtime(const int fd)
    {
      int64_t ret = 0;
      struct stat st;
      if (-1 != fd
          && 0 == fstat(fd, &st))
      {
        ret = st.st_mtime * 1000000L;
      }
      return ret;
    }

    bool StoreMgr::build_dir(const char *path, const char *dir)
    {
      bool bret = false;
      char dpath[MAX_DIR_NAME_LENGTH];
      if (NULL != path
          && NULL != dir
          && MAX_DIR_NAME_LENGTH > snprintf(dpath, MAX_DIR_NAME_LENGTH, "%s/%s", path, dir))
      {
        if (0 != mkdir(dpath, TRASH_DIR_MODE)
            && EEXIST != errno)
        {
          TBSYS_LOG(WARN, "mkdir fail dpath=[%s] errno=%u", dpath, errno);
        }
        else
        {
          bret = true;
        }
      }
      return bret;
    }
  }
}

#endif //OCEANBASE_UPDATESERVER_STORE_MGR_H_

