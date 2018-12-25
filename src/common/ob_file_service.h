#ifndef OCEANBASE_COMMON_FILE_SERVICE_H_
#define OCEANBASE_COMMON_FILE_SERVICE_H_

#include <tbsys.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include "ob_malloc.h"
#include "ob_string.h"
#include "utility.h"
#include "ob_base_server.h"
#include "ob_file.h"
#include "ob_packet_queue_thread.h"
#include "ob_result.h"
#include "ob_file.h"
#include "ob_single_server.h"
#include "ob_packet.h"

namespace oceanbase
{
  namespace common
  {
    /// Provide file download service for cs and ups
    class ObFileService
    {
    public:
      ObFileService();
      virtual ~ObFileService();

    public:
      static const int64_t BLOCK_SIZE = 1024 * 1024;

    public:
      int initialize(ObBaseServer* server, ObPacketQueueThread* queue_thread, 
          const int64_t network_timeout, const uint32_t max_concurrency_count);

      // handle the send_file request from client
      // this function use the stream network interface, 
      // can recieve a large file in this function once.
      int handle_send_file_request(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* request,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer);

    private:
      int inc_concurrency_count();
      void dec_concurrency_count();

      int receive_file_pre(ObFileAppender& file_appender, int64_t& file_size,
          ObString& file_path, ObString& tmp_file_path,
          easy_request_t* request, ObDataBuffer& in_buffer, 
          ObDataBuffer& out_buffer, int32_t& response_cid, const int64_t session_id);

      int receive_file_block(ObFileAppender& file_appender, char* buf,
          easy_request_t* request, ObDataBuffer& in_buffer,
          ObDataBuffer& out_buffer, int32_t& response_cid, const int64_t session_id);

      int receive_file_end(ObString& file_path, ObString& tmp_file_path, const int64_t file_size,
          easy_request_t* request, ObDataBuffer& out_buffer, 
          int32_t& response_cid, const int64_t session_id);

      int receive_file_loop(ObString& file_path, ObString& tmp_file_path, const int64_t file_size,
          ObFileAppender& file_appender, 
          easy_request_t* request, ObDataBuffer& out_buffer, 
          int32_t & response_cid, const int64_t session_id);

      int check_dir(const ObString& local_dir, const int64_t file_size);

    private:
      ObBaseServer *server_; 
      bool inited_;
      ObPacketQueueThread *queue_thread_;

    private:
      uint32_t max_concurrency_count_;
      uint32_t concurrency_count_;
      int64_t network_timeout_;
      int64_t block_size_;
      
      const static int64_t MIN_NETWORK_TIMEOUT = 500000;//500ms
      const static int32_t DEFAULT_VERSION = 1;
    };
  }
}

#endif /* OCEANBASE_COMMON_FILE_SERVICE_H_ */



