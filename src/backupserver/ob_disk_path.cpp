/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ob_disk_path.cpp is for what ...
 *
 *  Version: $Id: ob_disk_path.cpp 2011年03月22日 09时54分22秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#include "sstable/ob_disk_path.h"
#include "chunkserver/ob_tablet.h"
#include "ob_backup_server_main.h"

using namespace oceanbase::common;
using namespace oceanbase::backupserver;

namespace oceanbase
{
  namespace sstable
  {
    uint64_t get_sstable_disk_no(const uint64_t sstable_file_id)
    {
      return (sstable_file_id & DISK_NO_MASK);
    }


    int get_config_item(const char *&data_dir, const char *&app_name)
    {
      int ret = OB_SUCCESS;
      data_dir = ObBackupServerMain::get_instance()->get_backup_server().get_config().datadir;
      if (NULL == data_dir || '\0' == *data_dir)
      {
        TBSYS_LOG(ERROR, "data dir has not been set.");
        ret = OB_ERROR;
      }
      else
      {
        // read from configure file
        app_name = ObBackupServerMain::get_instance()->get_backup_server().get_config().appname;
        if (NULL == app_name || '\0' == *app_name)
        {
          TBSYS_LOG(ERROR, "app name has not been set.");
          ret = OB_ERROR;
        }
      }
      return ret;
    }

    int get_sstable_directory(const char* disk_path, char *path, const int64_t path_len)
    {
      int ret = OB_SUCCESS;
      if (NULL == disk_path || NULL == path || path_len < 0)
      {
        TBSYS_LOG(WARN, "get_sstable_directory invalid arguments, "
            "disk_path=%p,path=%p,path_len=%ld", disk_path, path, path_len);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        // read from configure file
        const char *data_dir = NULL;
        const char *app_name = NULL;
        ret = get_config_item(data_dir, app_name);
        if (OB_SUCCESS == ret)
        {
          int bufsiz = snprintf(path, path_len, "%s/%s/sstable", disk_path, app_name);
          if (bufsiz + 1 > path_len)
          {
            TBSYS_LOG(WARN, "get_sstable_directory, path_len=%ld <= bufsiz=%d", path_len, bufsiz);
            ret = OB_SIZE_OVERFLOW;
          }
        }
      }


      return ret;
    }


    int get_sstable_directory(const int32_t disk_no, char *path, const int64_t path_len)
    {
      int ret = OB_SUCCESS;
      if (disk_no < 0 || NULL == path || path_len < 0)
      {
        TBSYS_LOG(WARN, "get_sstable_directory invalid arguments, "
            "disk_no=%d,path=%p,path_len=%ld", disk_no, path, path_len);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        // read from configure file
        const char *data_dir = NULL;
        const char *app_name = NULL;
        ret = get_config_item(data_dir, app_name);
        if (OB_SUCCESS == ret)
        {
          int bufsiz = snprintf(path, path_len, "%s/%d/%s/sstable", data_dir, disk_no, app_name);
          if (bufsiz + 1 > path_len)
          {
            TBSYS_LOG(WARN, "get_sstable_directory, path_len=%ld <= bufsiz=%d", path_len, bufsiz);
            ret = OB_SIZE_OVERFLOW;
          }
        }
      }


      return ret;
    }

    int get_recycle_directory(const char* disk_path, char *path, const int64_t path_len)
    {
      int ret = OB_SUCCESS;
      if (NULL == disk_path || NULL == path || path_len < 0)
      {
        TBSYS_LOG(WARN, "get_recycle_directory invalid arguments, "
            "disk_path=%p,path=%p,path_len=%ld", disk_path, path, path_len);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        // read from configure file
        const char *data_dir = NULL;
        const char *app_name = NULL;
        ret = get_config_item(data_dir, app_name);
        if (OB_SUCCESS == ret)
        {
          int bufsiz = snprintf(path, path_len, "%s/Recycle", disk_path);
          if (bufsiz + 1 > path_len)
          {
            TBSYS_LOG(WARN, "get_recycle_directory , path_len=%ld <= bufsiz=%d", path_len, bufsiz);
            ret = OB_SIZE_OVERFLOW;
          }
        }
      }


      return ret;
    }

    int get_recycle_directory(const int32_t disk_no, char *path, const int64_t path_len)
    {
      int ret = OB_SUCCESS;
      if (disk_no < 0 || NULL == path || path_len < 0)
      {
        TBSYS_LOG(WARN, "get_recycle_directory invalid arguments, "
            "disk_no=%d,path=%p,path_len=%ld", disk_no, path, path_len);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        // read from configure file
        const char *data_dir = NULL;
        const char *app_name = NULL;
        ret = get_config_item(data_dir, app_name);
        if (OB_SUCCESS == ret)
        {
          int bufsiz = snprintf(path, path_len, "%s/%d/Recycle", data_dir, disk_no);
          if (bufsiz + 1 > path_len)
          {
            TBSYS_LOG(WARN, "get_recycle_directory , path_len=%ld <= bufsiz=%d", path_len, bufsiz);
            ret = OB_SIZE_OVERFLOW;
          }
        }
      }


      return ret;
    }

    int get_recycle_path(const ObSSTableId& sstable_id, char *path, const int64_t path_len)
    {
      int ret = OB_SUCCESS;
      if (sstable_id.sstable_file_offset_ < 0 || NULL == path || path_len < 0)
      {
        TBSYS_LOG(WARN, "get_recycle_path invalid arguments, "
            "offset=%ld, path=%p,path_len=%ld",
            sstable_id.sstable_file_offset_, path, path_len);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        // read from configure file
        const char *data_dir = NULL;
        const char *app_name = NULL;
        ret = get_config_item(data_dir, app_name);
        if (OB_SUCCESS == ret)
        {
          int32_t disk_no =  (sstable_id.sstable_file_id_ & DISK_NO_MASK);
          if (disk_no < 0)
          {
            TBSYS_LOG(WARN, "get_recycle_path, sstable file id = %ld invalid",
                sstable_id.sstable_file_id_);
            ret = OB_ERROR;
          }
          else
          {
            int bufsiz = snprintf(path, path_len, "%s/%d/Recycle/%ld",
                data_dir, disk_no, sstable_id.sstable_file_id_);
            if (bufsiz + 1 > path_len)
            {
              TBSYS_LOG(WARN, "get_recycle_path, path_len=%ld <= bufsiz=%d",
                  path_len, bufsiz);
              ret = OB_SIZE_OVERFLOW;
            }
          }
        }
      }

      return ret;
    }

    int get_sstable_path(const ObSSTableId& sstable_id, char *path, const int64_t path_len)
    {
      int ret = OB_SUCCESS;
      if (sstable_id.sstable_file_offset_ < 0 || NULL == path || path_len < 0)
      {
        TBSYS_LOG(WARN, "get_sstable_path invalid arguments, "
            "offset=%ld, path=%p,path_len=%ld",
            sstable_id.sstable_file_offset_, path, path_len);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        // read from configure file
        const char *data_dir = NULL;
        const char *app_name = NULL;
        ret = get_config_item(data_dir, app_name);
        if (OB_SUCCESS == ret)
        {
          int32_t disk_no =  (sstable_id.sstable_file_id_ & DISK_NO_MASK);
          if (disk_no < 0)
          {
            TBSYS_LOG(WARN, "get_sstable_path, sstable file id = %ld invalid",
                sstable_id.sstable_file_id_);
            ret = OB_ERROR;
          }
          else
          {
            int bufsiz = snprintf(path, path_len, "%s/%d/%s/sstable/%ld",
                data_dir, disk_no, app_name, sstable_id.sstable_file_id_);
            if (bufsiz + 1 > path_len)
            {
              TBSYS_LOG(WARN, "get_sstable_path, path_len=%ld <= bufsiz=%d",
                  path_len, bufsiz);
              ret = OB_SIZE_OVERFLOW;
            }
          }
        }
      }

      return ret;
    }

    int get_tmp_meta_path(const int32_t disk_no, char *path, const int32_t path_len)
    {
      /**
       * sequence number of tablet index temporary file name,
       * it will be increment atomicity to be unique .
       * only used in get_tmp_meta_path.
       */
      static volatile uint64_t s_tmp_idx_file_name_seq_no = 1;

      int ret = OB_SUCCESS;
      if (disk_no <= 0 || NULL == path ||  path_len <= 0)
      {
        TBSYS_LOG(WARN, "get_tmp_meta_path invalid arguments, "
            "disk_no=%d,path=%p,path_len=%d", disk_no, path, path_len);
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        const char *data_dir = NULL;
        const char *app_name = NULL;
        uint64_t curr_tmp_meta_no = atomic_inc(&s_tmp_idx_file_name_seq_no);
        ret = get_config_item(data_dir, app_name);
        if (OB_SUCCESS == ret)
        {
          int bufsiz = snprintf(path, path_len, "%s/%d/%s/sstable/tmp_idx_%d_%ld",
              data_dir, disk_no, app_name, disk_no, curr_tmp_meta_no);

          if (bufsiz + 1 > path_len)
          {
            TBSYS_LOG(WARN, "get_meta_path, path_len=%d <= bufsiz=%d", path_len, bufsiz);
            ret = OB_SIZE_OVERFLOW;
          }
        }
      }

      return ret;
    }

    int get_meta_path(const int32_t disk_no, const bool current, char *path, const int32_t path_len)
    {
      int ret = OB_SUCCESS;
      if (disk_no <= 0 || NULL == path ||  path_len <= 0)
      {
        TBSYS_LOG(WARN, "get_meta_path invalid arguments, "
            "disk_no=%d,path=%p,path_len=%d", disk_no, path, path_len);
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        const char *data_dir = NULL;
        const char *app_name = NULL;
        ret = get_config_item(data_dir, app_name);
        if (OB_SUCCESS == ret)
        {
          int bufsiz = 0;
          if (current)
          {
            bufsiz = snprintf(path, path_len, "%s/%d/%s/sstable/idx_%d",
                data_dir, disk_no, app_name, disk_no);
          }
          else
          {
            time_t t;
            ::time(&t);
            struct tm* tm = ::localtime(&t);
            bufsiz = snprintf(path, path_len, "%s/%d/%s/sstable/idx_%d.%04d%02d%02d%02d%02d%02d",
                data_dir, disk_no, app_name, disk_no,
                tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
                tm->tm_hour, tm->tm_min, tm->tm_sec);
          }
          if (bufsiz + 1 > path_len)
          {
            TBSYS_LOG(WARN, "get_meta_path, path_len=%d <= bufsiz=%d", path_len, bufsiz);
            ret = OB_SIZE_OVERFLOW;
          }
        }
      }

      return ret;
    }

    int get_meta_path(const int64_t version, const int32_t disk_no,
        const bool current, char *path, const int32_t path_len)
    {
      int ret = OB_SUCCESS;
      if (version <=0 || disk_no <= 0 || NULL == path ||  path_len <= 0)
      {
        TBSYS_LOG(WARN, "get_sstable_directory invalid arguments, "
            "version=%ld, disk_no=%d,path=%p,path_len=%d",
            version, disk_no, path, path_len);
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        // read from configure file
        const char *data_dir = NULL;
        const char *app_name = NULL;
        ret = get_config_item(data_dir, app_name);
        if (OB_SUCCESS == ret)
        {
          int bufsiz = 0;
          if (current)
          {
            bufsiz = snprintf(path, path_len, "%s/%d/%s/sstable/idx_%ld_%d",
                data_dir, disk_no, app_name, version, disk_no);
          }
          else
          {
            bufsiz = snprintf(path, path_len, "%s/%d/%s/sstable/bak_idx_%ld_%d",
                data_dir, disk_no, app_name, version, disk_no);
          }
          if (bufsiz + 1 > path_len)
          {
            TBSYS_LOG(WARN, "get_sstable_directory, path_len=%d <= bufsiz=%d", path_len, bufsiz);
            ret = OB_SIZE_OVERFLOW;
          }
        }
      }

      return ret;
    }

    static int get_sstable_directory_helper(const int32_t disk_no, char *path, 
      const int64_t path_len, const char* import_dir_name)
    {
      int ret = OB_SUCCESS;
      if (disk_no < 0 || NULL == path || path_len < 0 || NULL == import_dir_name)
      {
        TBSYS_LOG(WARN, "get_sstable_directory_helper invalid arguments, "
            "disk_no=%d, path=%p, path_len=%ld, import_dir_name=%p", disk_no, path, path_len, import_dir_name);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        // read from configure file
        const char *data_dir = NULL;
        const char *app_name = NULL;
        ret = get_config_item(data_dir, app_name);
        if (OB_SUCCESS == ret)
        {
          int bufsiz = snprintf(path, path_len, "%s/%d/%s/%s", data_dir, disk_no, app_name, import_dir_name);
          if (bufsiz + 1 > path_len)
          {
            TBSYS_LOG(WARN, "get_sstable_directory_helper, path_len=%ld <= bufsiz=%d, "
                            "import_dir_name=%s", path_len, bufsiz, import_dir_name);
            ret = OB_SIZE_OVERFLOW;
          }
        }
      }

      return ret;
    }

    static int get_sstable_path_helper(const int32_t disk_no, 
      const char* sstable_name, char *path, const int64_t path_len, 
      const char* import_dir_name)
    {
      int ret = OB_SUCCESS;
      if (disk_no < 0 || NULL == sstable_name || NULL == path || path_len < 0 || NULL == import_dir_name)
      {
        TBSYS_LOG(WARN, "get_import_sstable_path invalid arguments, "
            "disk_no=%d, sstable_name=%p, path=%p, path_len=%ld, import_dir_name=%p", 
            disk_no, sstable_name, path, path_len, import_dir_name);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        // read from configure file
        const char *data_dir = NULL;
        const char *app_name = NULL;
        ret = get_config_item(data_dir, app_name);
        if (OB_SUCCESS == ret)
        {
          int bufsiz = snprintf(path, path_len, "%s/%d/%s/%s/%s",
              data_dir, disk_no, app_name, import_dir_name, sstable_name);
          if (bufsiz + 1 > path_len)
          {
            TBSYS_LOG(WARN, "get_import_sstable_path, path_len=%ld <= bufsiz=%d, "
                            "import_dir_name=%s", 
                path_len, bufsiz, import_dir_name);
            ret = OB_SIZE_OVERFLOW;
          }
        }
      }

      return ret;
    }

    int get_bypass_sstable_directory(const int32_t disk_no, char *path, 
      const int64_t path_len)
    {
      return get_sstable_directory_helper(disk_no, path, path_len, "bypass");
    }

    int get_bypass_sstable_directory(const char* disk_path, char *path, const int64_t path_len)
    {
      int ret = OB_SUCCESS;
      if (NULL == disk_path || NULL == path || path_len < 0)
      {
        TBSYS_LOG(WARN, "get_sstable_directory invalid arguments, "
            "disk_path=%p,path=%p,path_len=%ld", disk_path, path, path_len);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        // read from configure file
        const char *data_dir = NULL;
        const char *app_name = NULL;
        ret = get_config_item(data_dir, app_name);
        if (OB_SUCCESS == ret)
        {
          int bufsiz = snprintf(path, path_len, "%s/%s/bypass", disk_path, app_name);
          if (bufsiz + 1 > path_len)
          {
            TBSYS_LOG(WARN, "get_sstable_directory, path_len=%ld <= bufsiz=%d", path_len, bufsiz);
            ret = OB_SIZE_OVERFLOW;
          }
        }
      }


      return ret;
    }

    int get_bypass_sstable_path(const int32_t disk_no, 
      const char* sstable_name, char *path, const int64_t path_len)
    {
      return get_sstable_path_helper(disk_no, sstable_name, path, path_len, "bypass");
    }

    int idx_file_name_filter(const struct dirent *d)
    {
      int ret = 0;
      if (NULL != d)
      {
        ret = (0 == strncmp(d->d_name,"idx_",4)) ? 1 : 0;
      }
      return ret;
    }

    int bak_idx_file_name_filter(const struct dirent *d)
    {
      int ret = 0;
      if (NULL != d)
      {
        ret = (0 == strncmp(d->d_name,"bak_idx_",8)) ? 1 : 0;
      }
      return ret;
    }

  }
}
