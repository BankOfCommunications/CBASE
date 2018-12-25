#include "ob_root_log_replay.h"

namespace oceanbase
{
  namespace rootserver
  {
    ObRootLogReplay::ObRootLogReplay()
    {
      is_initialized_ = false;
    }

    ObRootLogReplay::~ObRootLogReplay()
    {
    }

    void ObRootLogReplay::set_log_manager(ObRootLogManager* log_manager)
    {
      log_manager_ = log_manager;
    }

    void ObRootLogReplay::wait_replay(ObLogCursor& end_cursor)
    {
      master_end_cursor_ = end_cursor;
      if (!end_cursor.is_valid())
      {
        TBSYS_LOG(ERROR, "end_cursor[%s] is invalid", to_cstring(end_cursor));
      }
      ObLogReplayRunnable::wait();
    }

    int ObRootLogReplay::replay(common::LogCommand cmd, uint64_t seq, const char* log_data, const int64_t data_len)
    {
      int ret = log_manager_->get_log_worker()->apply(cmd, log_data, data_len);
      if (ret != common::OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "fatal error, replay log cmd:[%d], log_data[%.*s], seq[%lu], failed, err=[%d]. Quit...",
            cmd, static_cast<int32_t>(data_len), log_data, seq, ret);
        // coredump
        abort();
        // log_manager_->get_log_worker()->exit();
      }
      return ret;
    }

    void ObRootLogReplay::run(tbsys::CThread* thread, void* arg)
    {
      int ret = OB_SUCCESS;

      UNUSED(thread);
      UNUSED(arg);

      char* log_data = NULL;
      int64_t data_len = 0;
      LogCommand cmd = OB_LOG_UNKNOWN;
      uint64_t seq;
      int64_t wait_replay_retry_num = 0;

      if (!is_initialized_)
      {
        TBSYS_LOG(ERROR, "ObLogReplayRunnable has not been initialized");
        ret = OB_NOT_INIT;
      }
      else
      {
        while (!_stop)
        {
          ret = log_reader_.read_log(cmd, seq, log_data, data_len);
          if (OB_READ_NOTHING == ret)
          {
            if (ObRoleMgr::MASTER == role_mgr_->get_role() && master_end_cursor_.is_valid())
            {
              ObLogCursor replayed_cursor;
              if (OB_SUCCESS != (ret = get_replayed_cursor(replayed_cursor)))
              {
                TBSYS_LOG(ERROR, "get_replayed_cursor(master_end_cursor[%s])=>%d",
                          to_cstring(master_end_cursor_), ret);
                break;
              }
              else if (replayed_cursor.equal(master_end_cursor_))
              {
                master_end_cursor_.reset();
                stop();
              }
              else
              {
                // TBSYS_LOG(WARN, "wait_replay_retry_num[%ld], replay_cursor[%s] != master_end_cursor[%s], need wait replay",
                //           wait_replay_retry_num, to_cstring(replayed_cursor), to_cstring(master_end_cursor_));
                if (0 == (++wait_replay_retry_num % 1000))
                {
                  ret = OB_LOG_NOT_SYNC;
                  TBSYS_LOG(ERROR, "wait_replay_retry too many times[%ld], [%s->%s]",
                            wait_replay_retry_num, to_cstring(replayed_cursor), to_cstring(master_end_cursor_));
                }
              }
            }
            else
            {
              usleep(static_cast<useconds_t>(replay_wait_time_));
            }
            continue;
          }
          else if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "ObLogReader read_data error[ret=%d]", ret);
            break;
          }
          else
          {
            if (OB_LOG_NOP != cmd)
            {
              ret = replay(cmd, seq, log_data, data_len);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(ERROR, "replay log error[ret=%d]", ret);
                hex_dump(log_data, static_cast<int32_t>(data_len), false, TBSYS_LOG_LEVEL_ERROR);
                break;
              }
            }
          }
        }
      }

      // stop server
      if (NULL != role_mgr_) // double check
      {
        if (OB_SUCCESS != ret && OB_READ_NOTHING != ret)
        {
          TBSYS_LOG(WARN, "role_mgr_=%p", role_mgr_);
          role_mgr_->set_state(ObRoleMgr::ERROR);
        }
      }
      TBSYS_LOG(INFO, "ObLogReplayRunnable finished[stop=%d ret=%d]", _stop, ret);
    }

  } /* rootserver */
} /* oceanbase */
