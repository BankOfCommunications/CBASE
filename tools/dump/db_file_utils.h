/*
 * =====================================================================================
 *
 *       Filename:  db_file_utils.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/11/2011 01:18:55 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#ifndef __DB_FILE_UTILS_H__
#define  __DB_FILE_UTILS_H__

#include <string>
#include <vector>
#include <unistd.h>
#include <dirent.h>

class DbFileUtils {
  public:
    DbFileUtils(std::string path) : 
      path_(path) { }

    ~DbFileUtils() {
    }

    bool exists() {
      int ret = access(path_.c_str(), R_OK);
      return (ret == 0);
    }

    template <class Processor>
      int search_files(Processor &processor) {
        int ret = EXIT_SUCCESS;
        DIR *dir = NULL;

        if (exists()) {
          dir = opendir(path_.c_str());
          if (dir == NULL)
            ret = EXIT_FAILURE;
          else {
            dirent entry;
            dirent *entryp = NULL;
            while (((ret = readdir_r(dir, &entry, &entryp)) == 0) && entryp) {
              const char *name = entry.d_name;
              if (name[0] == '.' && (name[1] == '\0' || (name[1] == '.' && name[2] == '\0')))
                continue;

              processor(path_.c_str(), name);
//              if (filter(path_.c_str(), name))
//                files.push_back(name);
            }
          }
        } else {
          ret = EXIT_FAILURE;
        }

        if (dir != NULL) {
          closedir(dir);
        }

        return ret;
      }

    int system(const char *cmd) {
      //using c system function to exec user cmd
      return ::system(cmd);
    }

  private:
    std::string path_;
};

inline bool is_number(const char *s) {
  size_t i = 0;

  for(i=0; i<strlen(s); i++) {
    if(s[i]<'0' or s[i]>'9')
      return false;
  }

  return true;
}

#endif
