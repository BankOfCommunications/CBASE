#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_DISK_PATH_H__
#define OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_DISK_PATH_H__

#include <stdint.h>
#include <dirent.h>


namespace oceanbase
{
  namespace compactsstablev2 
  {
    uint64_t get_sstable_disk_no(const uint64_t sstable_file_id);

    int get_sstable_path(const uint64_t sstable_id, 
        char *path, const int64_t path_len);

    int get_sstable_directory(const int32_t disk_no, 
        char *path, const int64_t path_len);

    int get_recycle_directory(const int32_t disk_no, char *path, 
        const int64_t path_len);

    int get_tmp_meta_path(const int32_t disk_no, char *path, 
        const int32_t path_len);

    int get_meta_path(const int32_t disk_no, const bool current, 
        char *path, const int32_t path_len);

    int get_meta_path(const int64_t version, const int32_t disk_no, 
        const bool current, char *path, const int32_t path_len);

    int get_import_sstable_directory(const int32_t disk_no, 
        char *path, const int64_t path_len);

    int get_import_sstable_path(const int32_t disk_no, 
      const char* sstable_name, char *path, const int64_t path_len);

    int idx_file_name_filter(const struct dirent *d);

    int bak_idx_file_name_filter(const struct dirent *d);
  }//end namespace compactsstablev2
}//end namespace oceanbase

#endif
