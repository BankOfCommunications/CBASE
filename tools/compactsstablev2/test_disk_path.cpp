#include <tbsys.h>
#include "common/ob_define.h"
#include "sstable/ob_disk_path.h"

using namespace oceanbase::common;
extern const char* g_sstable_directory;

namespace oceanbase 
{
  namespace compactsstablev2
  {
    uint64_t get_sstable_disk_no(const uint64_t sstable_file_id)
    {
      return (sstable_file_id & DISK_NO_MASK);
    }

    int get_sstable_directory(const int32_t disk_no, char *path, 
        const int64_t path_len)
    {
      int ret = OB_SUCCESS;
      if (disk_no < 0 || NULL == path || path_len < 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        snprintf(path, path_len, "%s", g_sstable_directory);
      }
      return ret;
    }

    int get_sstable_path(uint64_t sstable_id, char *path, 
        const int64_t path_len)
    {
      int ret = OB_SUCCESS;
      if (NULL == path || path_len < 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        snprintf(path, path_len, "%s/%ld", g_sstable_directory,
            sstable_id);
      }
      return ret;
    }

    int get_meta_path(const int32_t disk_no, const bool current, 
        char *path, const int32_t path_len)
    {
      int ret = OB_SUCCESS;
      UNUSED(current);
      if (disk_no <= 0 || NULL == path ||  path_len <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == ret)
      {
        snprintf(path, path_len, "idx_%d", disk_no);
      }
      return ret;
    }

    int get_meta_path(const int64_t version, const int32_t disk_no, 
        const bool current, char *path, const int32_t path_len)
    {
      int ret = OB_SUCCESS;
      UNUSED(current);
      if (disk_no <= 0 || NULL == path ||  path_len <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == ret)
      {
        snprintf(path, path_len, "./tmp/%d/idx_%ld_%d", 
            disk_no, version, disk_no);
      }
      return ret;
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

    int get_recycle_directory(const int32_t disk_no, char *path, 
        const int64_t path_len)
    {
      int ret = OB_SUCCESS;
      if (disk_no < 0 || NULL == path || path_len < 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        int bufsiz = snprintf(path, path_len, "./tmp/%d/Recycle",  disk_no);
        if (bufsiz + 1 > path_len)
        {
          ret = OB_SIZE_OVERFLOW;
        }
      }


      return ret;
    }

    int get_tmp_meta_path(const int32_t disk_no, char *path, 
        const int32_t path_len)
    {
      int ret = OB_SUCCESS;
      if (disk_no <= 0 || NULL == path ||  path_len <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        int64_t current_time = tbsys::CTimeUtil::getTime();
        int bufsiz = snprintf(path, path_len, "./tmp/tmp_idx_%d_%ld", 
            disk_no, current_time);

        if (bufsiz + 1 > path_len)
        {
          ret = OB_SIZE_OVERFLOW;
        }
      }

      return ret;
    }
  }//end namespace compactsstablev2
}//end namespace oceanbase
