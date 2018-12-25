#include "ob_backup_rs_fetch_runnable.h"
#include "common/file_directory_utils.h"

namespace oceanbase
{
  namespace backupserver
  {
    const char* ObBackupRootFetchRunnable::FIRST_META = "first_tablet_meta";
    const char* ObBackupRootFetchRunnable::DEFAULT_FETCH_OPTION = "-e \"ssh -oStrictHostKeyChecking=no\" -avz --inplace";

    ObBackupRootFetchRunnable::ObBackupRootFetchRunnable() : is_backup_done_(false)
    {
      is_initialized_ = false;
      cwd_[0] = '\0';
      log_dir_[0] = '\0';
    }

    int ObBackupRootFetchRunnable::init(const ObServer& master, const char* log_dir)
    {
      int ret = 0;

      int log_dir_len = 0;
      if (is_initialized_)
      {
        TBSYS_LOG(ERROR, "ObFetchRunnable has been initialized");
        ret = OB_INIT_TWICE;
      }
      else
      {
        if (NULL == log_dir )
        {
          TBSYS_LOG(ERROR, "Parameter are invalid[log_dir=%p]",
              log_dir);
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          log_dir_len = static_cast<int32_t>(strlen(log_dir));
          if (log_dir_len >= OB_MAX_FILE_NAME_LENGTH)
          {
            TBSYS_LOG(ERROR, "Parameter is invalid[log_dir_len=%d log_dir=%s]", log_dir_len, log_dir);
            ret = OB_INVALID_ARGUMENT;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        usr_opt_ = (char*)ob_malloc(OB_MAX_FETCH_CMD_LENGTH, ObModIds::OB_FETCH_RUNABLE);
        if (NULL == usr_opt_)
        {
          TBSYS_LOG(ERROR, "ob_malloc for usr_opt_ error, size=%ld", OB_MAX_FETCH_CMD_LENGTH);
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          ret = set_usr_opt(DEFAULT_FETCH_OPTION);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "set default user option error, DEFAULT_FETCH_OPTION=%s ret=%d", DEFAULT_FETCH_OPTION, ret);
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        strncpy(log_dir_, log_dir, log_dir_len);
        log_dir_[log_dir_len] = '\0';
        master_ = master;
        is_initialized_ = true;
      }

      return ret;
    }

    void ObBackupRootFetchRunnable::run(tbsys::CThread* thread, void* arg)
    {
      int ret = OB_SUCCESS;

      UNUSED(thread);
      UNUSED(arg);

      if (!is_initialized_)
      {
        TBSYS_LOG(ERROR, "ObBackupFetchThread has not been initialized");
        ret = OB_NOT_INIT;
      }

      if (OB_SUCCESS == ret)
      {
        if (!_stop)
        {
          ret = get_first_meta();
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "get_ckpt_ error, ret=%d", ret);
          }
        }
      }
      TBSYS_LOG(INFO, "ObFetchRunnable finished[stop=%d ret=%d]", _stop, ret);
    }

    int ObBackupRootFetchRunnable::wait_fetch_done()
    {
      int ret = OB_SUCCESS;
      int count = 0;
      while (!is_backup_done_ && !_stop)
      {
        count++;
        if ((count % 100) == 0)
        {
          TBSYS_LOG(INFO, "wait fetch rs log used %d seconds", (count/10));
        }
        usleep(WAIT_INTERVAL);
      }
      TBSYS_LOG(INFO, "wait fetch done:, backup_done[%d], stop[%d]", is_backup_done_, _stop);
      if (!is_backup_done_)
      {
        ret = OB_ERROR;
      }
      return ret;
    }

    int ObBackupRootFetchRunnable::get_first_meta()
    {
      int ret = OB_SUCCESS;

      char *cmd = NULL;

      if (!is_initialized_)
      {
        TBSYS_LOG(ERROR, "ObBackupRootFetchRunnable has not been initialized");
        ret = OB_NOT_INIT;
      }

      if (OB_SUCCESS == ret)
      {
        cmd = static_cast<char*>(ob_malloc(OB_MAX_FETCH_CMD_LENGTH, ObModIds::OB_FETCH_RUNABLE));
        if (NULL == cmd)
        {
          TBSYS_LOG(WARN, "ob_malloc error, OB_MAX_FETCH_CMD_LENGTH=%ld", OB_MAX_FETCH_CMD_LENGTH);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "fetch first_tablet_meta");

        ret = gen_fetch_cmd_(ObBackupRootFetchRunnable::FIRST_META, NULL, cmd, OB_MAX_FETCH_CMD_LENGTH);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "gen_fetch_cmd_[file %s fn_ext=NULL] error[ret=%d]", ObBackupRootFetchRunnable::FIRST_META, ret);
          ret = OB_ERROR;
        }
        else
        {
          TBSYS_LOG(INFO, "fetch log[file=%s]: \"%s\"", ObBackupRootFetchRunnable::FIRST_META, cmd);
          ret = FSU::vsystem(cmd);
          if (0 != ret)
          {
            TBSYS_LOG(ERROR, "fetch[\"%s\"] error[ret=%d]", cmd, ret);
            ret = OB_ERROR;
          }
        }
      }

      if (exists_(ObBackupRootFetchRunnable::FIRST_META,NULL))
      {
        is_backup_done_ = true;
      }

      if (NULL != cmd)
      {
        ob_free(cmd);
        cmd = NULL;
      }

      return ret;
    }

    int ObBackupRootFetchRunnable::gen_fetch_cmd_(const char* file_name, const char* fn_ext, char* cmd, const int64_t size)
    {
      int ret = OB_SUCCESS;

      int err = 0;
      const int MAX_SERVER_ADDR_SIZE = 128;
      char master_addr[MAX_SERVER_ADDR_SIZE];
      const int MAX_FETCH_OPTION_SIZE = 128;
      char fetch_option[MAX_FETCH_OPTION_SIZE];
      char full_log_dir[OB_MAX_FILE_NAME_LENGTH];

      if (NULL == cmd || size <= 0)
      {
        TBSYS_LOG(ERROR, "Parameters are invalid[file=%s fn_ext=%s cmd=%p size=%ld", file_name, fn_ext, cmd, size);
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        if ('\0' == cwd_[0])
        {
          if (NULL == getcwd(cwd_, OB_MAX_FILE_NAME_LENGTH))
          {
            TBSYS_LOG(ERROR, "getcwd error[%s]", strerror(errno));
            ret = OB_ERROR;
          }
        }
      }

      // gen log directory
      if (OB_SUCCESS == ret)
      {
        if (log_dir_[0] == '/')
        {
          err = snprintf(full_log_dir, OB_MAX_FILE_NAME_LENGTH, "%s", log_dir_);
        }
        else
        {
          err = snprintf(full_log_dir, OB_MAX_FILE_NAME_LENGTH, "%s/%s", cwd_,  log_dir_);
        }

        if (err < 0)
        {
          TBSYS_LOG(ERROR, "snprintf full_log_dir[OB_MAX_FILE_NAME_LENGTH=%ld err=%d] error[%s]",
              OB_MAX_FILE_NAME_LENGTH, err, strerror(errno));
          ret = OB_ERROR;
        }
        else if (err >= OB_MAX_FILE_NAME_LENGTH)
        {
          TBSYS_LOG(ERROR, "full_log_dir buffer is not enough[OB_MAX_FILE_NAME_LENGTH=%ld err=%d]", OB_MAX_FILE_NAME_LENGTH, err);
          ret = OB_ERROR;
        }
      }

      // get master address and generate fetch option
      if (OB_SUCCESS == ret)
      {
        if (!master_.ip_to_string(master_addr, MAX_SERVER_ADDR_SIZE))
        {
          TBSYS_LOG(ERROR, "ObServer to_string error[master_=%p]", &master_);
          ret = OB_ERROR;
        }
        else
        {
          const char* FETCH_OPTION_FORMAT = "%s --bwlimit=%ld";
          err = snprintf(fetch_option, MAX_FETCH_OPTION_SIZE, FETCH_OPTION_FORMAT, usr_opt_, limit_rate_);
          if (err < 0)
          {
            TBSYS_LOG(ERROR, "snprintf fetch_option[MAX_FETCH_OPTION_SIZE=%d] error[%s]", MAX_FETCH_OPTION_SIZE, strerror(errno));
            ret = OB_ERROR;
          }
          else if (err >= MAX_FETCH_OPTION_SIZE)
          {
            TBSYS_LOG(ERROR, "fetch_option buffer size is not enough[MAX_FETCH_OPTION_SIZE=%d real=%d]", MAX_FETCH_OPTION_SIZE, err);
            ret = OB_ERROR;
          }
        }
      }

      // generate fetch command
      if (OB_SUCCESS == ret)
      {
        if (NULL == fn_ext || 0 == strlen(fn_ext))
        {
          const char* FETCH_CMD_WITHOUTEXT_FORMAT = "rsync %s %s:%s/%s %s/";
          err = snprintf(cmd, size, FETCH_CMD_WITHOUTEXT_FORMAT, fetch_option, master_addr, full_log_dir, file_name, log_dir_);
        }
        else
        {
          const char* FETCH_CMD_WITHEXT_FORMAT = "rsync %s %s:%s/%s%s %s/";
          err = snprintf(cmd, size, FETCH_CMD_WITHEXT_FORMAT, fetch_option, master_addr, full_log_dir, file_name, fn_ext, log_dir_);
        }

        if (err < 0)
        {
          TBSYS_LOG(ERROR, "snprintf cmd[size=%ld err=%d] error[%s]", size, err, strerror(errno));
          ret = OB_ERROR;
        }
        else if (err >= size)
        {
          TBSYS_LOG(ERROR, "cmd buffer is not enough[size=%ld err=%d]", size, err);
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    bool ObBackupRootFetchRunnable::exists_(const char* file_name, const char* fn_ext) const
    {
      bool res = false;

      char fn[OB_MAX_FILE_NAME_LENGTH];
      int ret = gen_full_name_(file_name, fn_ext, fn, OB_MAX_FILE_NAME_LENGTH);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "gen_full_name_ error[ret=%d file=%s fn_ext=%s]", ret, file_name, fn_ext);
      }
      else
      {
        TBSYS_LOG(WARN, "fn[file=%s]", fn);
        res = FileDirectoryUtils::exists(fn);

      }

      return res;
    }

    bool ObBackupRootFetchRunnable::remove_(const char * file_name, const char* fn_ext) const
    {
      bool res = false;

      char fn[OB_MAX_FILE_NAME_LENGTH];
      int ret = gen_full_name_(file_name, fn_ext, fn, OB_MAX_FILE_NAME_LENGTH);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "gen_full_name_ error[ret=%d id=%s fn_ext=%s]", ret, file_name, fn_ext);
      }
      else
      {
        res = FileDirectoryUtils::delete_file(fn);
      }

      return res;
    }

    int ObBackupRootFetchRunnable::gen_full_name_(const char* file_name, const char* fn_ext, char *buf, const int buf_len) const
    {
      int ret = OB_SUCCESS;

      int fn_ext_len = 0;
      int err = 0;

      if (NULL == buf || buf_len <= 0)
      {
        TBSYS_LOG(ERROR, "Parameters are invalid[buf=%p buf_len=%d]", buf, buf_len);
        ret = OB_ERROR;
      }
      else
      {
        if (NULL != fn_ext)
        {
          fn_ext_len = static_cast<int32_t>(strlen(fn_ext));
        }

        if (NULL == fn_ext || 0 == fn_ext_len)
        {
          const char* FN_WITHOUTEXT_FORMAT = "%s/%s";
          err = snprintf(buf, buf_len, FN_WITHOUTEXT_FORMAT, log_dir_, file_name);
        }
        else
        {
          const char* FN_WITHEXT_FORMAT = "%s/%s%s";
          err = snprintf(buf, buf_len, FN_WITHEXT_FORMAT, log_dir_, file_name, fn_ext);
        }

        if (err < 0)
        {
          TBSYS_LOG(ERROR, "snprintf error[err=%d]", err);
          ret = OB_ERROR;
        }
        else if (err >= buf_len)
        {
          TBSYS_LOG(ERROR, "buf is not enough[err=%d buf_len=%d]", err, buf_len);
          ret = OB_BUF_NOT_ENOUGH;
        }
      }

      return ret;
    }

    int ObBackupRootFetchRunnable::set_usr_opt(const char* opt)
    {
      int ret = OB_SUCCESS;

      int opt_len = static_cast<int32_t>(strlen(opt));
      if (opt_len >= OB_MAX_FETCH_CMD_LENGTH)
      {
        TBSYS_LOG(WARN, "usr_option is too long, opt_len=%d maximum_length=%ld", opt_len, OB_MAX_FETCH_CMD_LENGTH);
        ret = OB_BUF_NOT_ENOUGH;
      }
      else
      {
        strncpy(usr_opt_, opt, opt_len);
        usr_opt_[opt_len] = '\0';
      }

      return ret;
    }


  } /* backupserver */
} /* oceanbase */
