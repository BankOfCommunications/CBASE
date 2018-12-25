/** (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */

#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include "common/ob_packet_factory.h"
#include "common/ob_client_manager.h"
#include "ob_mutator_reader.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace msync
  {
    const static int SEQ_MARKER_SIZE = 1<<12;
    static inline void* file_map(const char* path, size_t len)
    {
      char* buf = NULL;
      int fd;
      if ((fd =open(path, O_RDWR|O_CREAT, S_IRWXU)) < 0 || -1 == ftruncate(fd, len))
      {
        perror("file_map:");
        return NULL;
      }
      buf = (char*)mmap(NULL, len, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
      close(fd);
      if (MAP_FAILED == buf)
      {
        perror("file_map:");
        buf = NULL;
      }
      return buf;
    }
      
    class ObMsyncClient: public ObClientManager
    {
    public:
    ObMsyncClient() : stop_(false), host_(NULL), port_(0),
        timeout_(0), wait_time_(0), max_retry_times_(0), seq_marker_(NULL)
      {
      }
      ~ObMsyncClient()
      {
        wait();
        if (seq_marker_)
        {
          munmap(seq_marker_, SEQ_MARKER_SIZE);
        }
      }
      
      int initialize(const char* schema, const char* log_dir, const char* host, int port, uint64_t log_file_start_id, uint64_t last_seq_id, int64_t timeout, int64_t wait_time, int64_t max_retry_times, const char* seq_marker_file)
      {
        int err = OB_SUCCESS;
        host_ = host;
        port_ = port;
        timeout_ = timeout;
        wait_time_ = wait_time;
        max_retry_times_ = max_retry_times;
        packet_streamer_.setPacketFactory(&packet_factory_);

        seq_marker_ = (char*)file_map(seq_marker_file, SEQ_MARKER_SIZE);
        memset(seq_marker_, 0, sizeof(SEQ_MARKER_SIZE));
        
        if (NULL == seq_marker_)
        {
          err = OB_IO_ERROR;
          TBSYS_LOG(ERROR, "file_map(file='%s')=>%d", seq_marker_file, err);
        }
        else if (OB_SUCCESS != (err = reader_.initialize(schema, log_dir, log_file_start_id, last_seq_id, wait_time)))
        {
          TBSYS_LOG(WARN, "reader.initialize(log_dir='%s', log_file_id=%lu, last_seq_id=%lu)=>%d",
                    log_dir, log_file_start_id, last_seq_id, err);
        }
        else if (OB_SUCCESS != (err = ObClientManager::initialize(&transport_, &packet_streamer_)))
        {
          TBSYS_LOG(WARN, "clientMgr.initialize()=>%d", err);
        }
        else
        {
          transport_.start();
        }
        return err;
      }
      
      void wait()
      {
        transport_.stop();
        reader_.stop();
        stop_ = true;
        transport_.wait();
      }

      int start_sync_mutator();
      int after_sync(uint64_t seq)
      {
        TBSYS_LOG(INFO, "LogSyncMarker: %lu", seq);
        snprintf(seq_marker_, SEQ_MARKER_SIZE, "%lu", seq);
        seq_marker_[SEQ_MARKER_SIZE-1] = 0;
        return OB_SUCCESS;
      }
    private:
      int send_mutator_(ObDataBuffer mut);
      int send_mutator_may_need_retry_(ObDataBuffer mut);
      bool stop_;
      const char* host_;
      int port_;
      int64_t timeout_;
      int64_t wait_time_;
      int64_t max_retry_times_;
      ObMutatorReader reader_;
      char* seq_marker_;
    private:
      const static int32_t MY_VERSION = 1;
      char packet_buffer_[OB_MAX_PACKET_LENGTH];
      ObPacketFactory packet_factory_;
      tbnet::DefaultPacketStreamer packet_streamer_;
      tbnet::Transport transport_;
    };
  } //end namespace msync
} // end namespace oceanbase
