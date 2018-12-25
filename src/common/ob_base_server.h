#ifndef OCEANBASE_COMMON_BASE_SERVER_H_
#define OCEANBASE_COMMON_BASE_SERVER_H_

#include <tbsys.h>
#include "easy_io_struct.h"
#include "easy_io.h"
#include <string.h>
#include "ob_define.h"
#include "ob_packet.h"
#include "data_buffer.h"
#include "ob_packet_factory.h"
#include "ob_packet_queue.h"

namespace oceanbase
{
  namespace common
  {
    class ObBaseServer
    {
      public:
        static const int MAX_TASK_QUEUE_SIZE = 1000;
        static const int DEV_NAME_LENGTH = 16;
        static const int OB_LIBEASY_STATISTICS_TIMER = 60; // 60s
        static const int FD_BUFFER_SIZE = 1024;

        ObBaseServer();
        virtual ~ObBaseServer();

        /** called before start server */
        virtual int initialize();

        /** wait for packet thread queue */
        virtual void wait_for_queue();

        /** wait for eio stop */
        virtual void wait();

        /** called when server is stop */
        virtual void destroy();

        /** WARNING: not thread safe, caller should make sure there is only one thread doing this */
        virtual int start(bool need_wait = true);

        virtual int start_service();

        /** WARNING: not thread safe, caller should make sure there is only one thread doing this */
        virtual void stop();

        void stop_eio();

        /** handle single packet*/
        virtual int handlePacket(ObPacket* packet) = 0;

        /** handle batch packets*/
        virtual int handleBatchPacket(ObPacketQueue &packetQueue) = 0;

        /** whether enable batch process mode */
        void set_batch_process(const bool batch);

        /** set the device name of the interface */
        int set_dev_name(const char* dev_name);

        /** set the port this server should listen on */
        int set_listen_port(const int port);

        uint64_t get_server_id() const;

        int send_response(const int32_t pcode, const int32_t version, const ObDataBuffer& buffer, easy_request_t* req, const uint32_t channel_id, const int64_t session_id = 0);
        static void easy_timer_cb(EV_P_ ev_timer *w, int revents);

        virtual void on_ioth_start() {};

      private:
        static void easy_on_ioth_start(void *arg)
        {
          ObBaseServer *server = (ObBaseServer*)arg;
          if (NULL != server)
          {
            server->on_ioth_start();
          }
        };

      protected:
        volatile bool stoped_;
        int io_thread_count_;
        int thread_count_;
        bool batch_;
        char dev_name_[DEV_NAME_LENGTH];
        int port_;
        uint64_t server_id_;

        ObPacketFactory packet_factory_;
        easy_io_t *eio_;
        easy_io_handler_pt server_handler_;
        uint32_t local_ip_;

    };
  } /* common */
} /* oceanbase */
#endif /* end of include guard: OCEANBASE_COMMON_BASE_SERVER_H_ */
