#ifndef OCEANBASE_COMMON_FILE_CLIENT_H_
#define OCEANBASE_COMMON_FILE_CLIENT_H_

#include <sys/stat.h>
#include <sys/statvfs.h>
#include <tbsys.h>
#include "ob_file.h"
#include "ob_file_service.h"
#include "ob_client_manager.h"
#include "ob_string.h"
#include "ob_result.h"
#include "utility.h"

namespace oceanbase
{
  namespace common
  {
    class ObFileClient
    {
    public:
      ObFileClient();
      virtual ~ObFileClient();
    
    public:
      int initialize(common::ThreadSpecificBuffer * rpc_buffer, 
          common::ObClientManager * client, const int64_t band_limit);

    public:
      // upload a file to a file server
      // param @timeout         the timeout limit for each packet transaction
      //       @band_limit      the band limit for this send file request
      //       @dest_server     the dest file server address
      //       @local_path      the path of local file
      //       @dest_dir the    the path of the remote dir
      //       @dest_file_name  the name of the remote file
      //
      int send_file(const int64_t timeout, const int64_t band_limit, 
          const ObServer& dest_server, const ObString& local_path, 
          const ObString& dest_dir, const ObString& dest_file_name);

    private:
      int get_rpc_buffer(ObDataBuffer & data_buffer) const;

      int send_file_pre(const int64_t timeout, const ObServer& dest_server,
          const int64_t file_size, const ObString& dest_dir, 
          const ObString& dest_file_name, ObDataBuffer& in_buffer, 
          ObDataBuffer& out_buffer,int64_t& session_id);

      int send_file_block(const int64_t timeout, const ObServer& dest_server,
          ObFileReader& file_reader, char *buf, const int64_t offset, 
          int64_t& read_size, ObDataBuffer& in_buffer, 
          ObDataBuffer& out_buffer,const int64_t session_id);

      int send_file_end(const int64_t timeout, const ObServer& dest_server,
          ObDataBuffer& in_buffer, ObDataBuffer& out_buffer,
          const int64_t session_id);

      int send_file_loop(const int64_t timeout, const int64_t band_limit, 
          const ObServer& dest_server, const ObString& local_path, 
          const ObString& dest_dir, const ObString& dest_file_name);


    private:
      bool inited_;
      common::ObClientManager *client_;
      common::ThreadSpecificBuffer * rpc_buffer_;   // rpc thread buffer
      int64_t block_size_;
      int64_t band_limit_;

      static const int32_t MAX_CONCURRENCY_COUNT = 128;
      static const int64_t MIN_BAND_LIMIT = 1024;
      static const int32_t DEFAULT_VERSION = 1;
    };
  }
}

#endif /* OCEANBASE_COMMON_FILE_CLIENT_H_ */

