/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#include "ob_log_dir_scanner.h"

#include <dirent.h>

#include "ob_log_entry.h"

using namespace oceanbase::common;

int ObLogFile::assign(const char* filename, FileType &type)
{
  int ret = OB_SUCCESS;
  if (NULL == filename)
  {
    TBSYS_LOG(ERROR, "parameter filename=NULL. SHOULD NOT REACH HERE");
    ret = OB_ERROR;
  }
  else
  {
    char basename[OB_MAX_FILE_NAME_LENGTH];
    int f_len = static_cast<int32_t>(strlen(filename));
    if (f_len >= OB_MAX_FILE_NAME_LENGTH)
    {
      type = UNKNOWN;
    }
    else
    {
      int ckpt_len = static_cast<int32_t>(strlen(DEFAULT_CKPT_EXTENSION));
      if (f_len > ckpt_len + 1
          && 0 == strcmp(filename + f_len - ckpt_len, DEFAULT_CKPT_EXTENSION)
          && '.' == *(filename + f_len - ckpt_len - 1))
      {
        f_len -= ckpt_len + 1;
        strncpy(basename, filename, f_len);
        basename[f_len] = '\0';
        type = CKPT;
      }
      else
      {
        strncpy(basename, filename, f_len);
        basename[f_len] = '\0';
        type = LOG;
      }

      if (!isLogId(basename))
      {
        type = UNKNOWN;
      }
      else
      {
        strcpy(name, filename);
        id = strToUint64(basename);
      }
    }
  }

  return ret;
}

bool ObLogFile::isLogId(const char* str) const
{
  bool ret = true;
  if (str == NULL || (*str) == '\0' || (*str) == '0')
  {
    ret = false;
  }
  while((*str) != '\0')
  {
    if ((*str) < '0' || (*str) > '9')
    {
      ret = false;
      break;
    }
    str ++;
  }
  return ret;

}

uint64_t ObLogFile::strToUint64(const char* str) const
{
  if (NULL == str)
  {
    return 0U;
  }
  else
  {
    return atoll(str);
  }
}

bool ObLogFile::operator< (const ObLogFile& r) const
{
  return id < r.id;
}

ObLogDirScanner::ObLogDirScanner()
{
  min_log_id_ = 0;
  max_log_id_ = 0;
  max_ckpt_id_ = 0;
  has_log_ = false;
  has_ckpt_ = false;
  is_initialized_ = false;
}

ObLogDirScanner::~ObLogDirScanner()
{
}

int ObLogDirScanner::init(const char* log_dir)
{
  int ret = OB_SUCCESS;

  if (is_initialized_)
  {
    TBSYS_LOG(ERROR, "ObLogDirScanner has been initialized.");
    ret = OB_INIT_TWICE;
  }
  else
  {
    ret = search_log_dir_(log_dir);
    if (OB_SUCCESS == ret)
    {
      is_initialized_ = true;
      TBSYS_LOG(INFO, "ObLogDirScanner initialize successfully[min_log_id_=%lu max_log_id_=%lu max_ckpt_id_=%lu]",
          min_log_id_, max_log_id_, max_ckpt_id_);
    }
    else if (OB_DISCONTINUOUS_LOG == ret)
    {
      is_initialized_ = true;
      TBSYS_LOG(INFO, "ObLogDirScanner initialize successfully[min_log_id_=%lu max_log_id_=%lu max_ckpt_id_=%lu], "
          "but log files in \"%s\" directory is not continuous",
          min_log_id_, max_log_id_, max_ckpt_id_, log_dir);
    }
    else
    {
      TBSYS_LOG(INFO, "search_log_dir_[log_dir=%s] error[ret=%d], ObLogDirScanner initialize failed", log_dir, ret);
    }
  }

  return ret;
}

int ObLogDirScanner::get_min_log_id(uint64_t &log_id) const
{
  int ret = check_inner_stat();

  if (OB_SUCCESS == ret)
  {
    if (has_log())
    {
      log_id = min_log_id_;
    }
    else
    {
      log_id = 0;
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  else
  {
    log_id = 0;
  }

  return ret;
}

int ObLogDirScanner::get_max_log_id(uint64_t &log_id) const
{
  int ret = check_inner_stat();

  if (OB_SUCCESS == ret)
  {
    if (has_log())
    {
      log_id = max_log_id_;
    }
    else
    {
      log_id = 0;
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  else
  {
    log_id = 0;
  }

  return ret;
}

int ObLogDirScanner::get_max_ckpt_id(uint64_t &ckpt_id) const
{
  int ret = check_inner_stat();

  if (OB_SUCCESS == ret)
  {
    if (has_ckpt())
    {
      ckpt_id = max_ckpt_id_;
    }
    else
    {
      ckpt_id = 0;
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  else
  {
    ckpt_id = 0;
  }

  return ret;
}

bool ObLogDirScanner::has_log() const
{
  return has_log_;
}

bool ObLogDirScanner::has_ckpt() const
{
  return has_ckpt_;
}

int ObLogDirScanner::search_log_dir_(const char* log_dir)
{
  int ret = OB_SUCCESS;

  int err = 0;

  common::ObVector<ObLogFile> log_files;
  DIR* plog_dir = opendir(log_dir);
  if (NULL == plog_dir)
  {
    err = mkdir(log_dir, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
    if (err != 0
        && EEXIST != errno)
    {
      TBSYS_LOG(ERROR, "mkdir[\"%s\"] error[%s]", log_dir, strerror(errno));
      ret = OB_ERROR;
    }
    else
    {
      plog_dir = opendir(log_dir);
      if (NULL == plog_dir)
      {
        TBSYS_LOG(ERROR, "opendir[\"%s\"] error[%s]", log_dir, strerror(errno));
        ret = OB_ERROR;
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    struct dirent entry;
    memset(&entry, 0x00, sizeof(struct dirent));
    struct dirent *pentry = &entry;

    ObLogFile log_file;
    ObLogFile::FileType file_type;

    err = readdir_r(plog_dir, pentry, &pentry);
    while (0 == err && NULL != pentry)
    {
      ret = log_file.assign(pentry->d_name, file_type);
      if (ret != OB_SUCCESS)
      {
        break;
      }
      else
      {
        if (ObLogFile::LOG == file_type)
        {
          log_files.push_back(log_file);
          TBSYS_LOG(DEBUG, "find a valid log file(\"%s\")", log_file.name);
        }
        else if (ObLogFile::CKPT == file_type)
        {
          TBSYS_LOG(DEBUG, "find a valid checkpoint file(\"%s\")", log_file.name);
          if (max_ckpt_id_ < log_file.id)
          {
            max_ckpt_id_ = log_file.id;
            has_ckpt_ = true;
          }
        }
        else
        {
          TBSYS_LOG(DEBUG, "ignore file(\"%s\"): \"%s\" is not valid log file",
              pentry->d_name, pentry->d_name);
        }
        err = readdir_r(plog_dir, pentry, &pentry);
      }
    }
    if (0 != err)
    {
      TBSYS_LOG(ERROR, "readdir_r error(ret[%d], pentry[%p], plog_dir[%p]", err, pentry, plog_dir);
      ret = OB_ERROR;
    }
    err = closedir(plog_dir);
    if (err < 0)
    {
      TBSYS_LOG(ERROR, "closedir[DIR=%p] error[%s]", plog_dir, strerror(errno));
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCCESS == ret)
  {
    std::sort(log_files.begin(), log_files.end());
    ret = check_continuity_(log_files, min_log_id_, max_log_id_);
  }

  return ret;
}

int ObLogDirScanner::check_continuity_(const ObVector<ObLogFile> &files, uint64_t &min_file_id, uint64_t &max_file_id)
{
  int ret = OB_SUCCESS;

  if (files.size() == 0)
  {
    min_file_id = max_file_id = 0;
    has_log_ = false;
  }
  else
  {
    has_log_ = true;
    int size = files.size();
    min_file_id = max_file_id = files[size - 1].id;
    typedef ObVector<ObLogFile>::iterator FileIter;
    //FileIter i = files.begin();
    int i = size - 1;
    uint64_t pre_id = files[i].id;
    i--;
    for (; i >= 0; --i)
    {
      if (files[i].id != (pre_id - 1))
      {
        break;
      }
      else
      {
        min_file_id = files[i].id;
      }
      pre_id = files[i].id;
    }

    if (i >= 0)
    {
      ret = OB_DISCONTINUOUS_LOG;
    }
  }

  return ret;
}
