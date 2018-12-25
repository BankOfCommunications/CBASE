#include "ob_mysql_command_queue_thread.h"
#include "common/ob_atomic.h"
#include "common/ob_profile_type.h"
#include "obmysql/ob_mysql_define.h"
#include "common/ob_profile_fill_log.h"

using namespace oceanbase::common;
namespace  oceanbase
{
  namespace obmysql
  {
    timespec* mktimespec(timespec* ts, const int64_t time_ns)
    {
      const int64_t NS_PER_SEC = 1000000000;
      ts->tv_sec = time_ns / NS_PER_SEC;
      ts->tv_nsec = time_ns % NS_PER_SEC;
      return ts;
    }

    ObMySQLCommandQueueThread::ObMySQLCommandQueueThread()
      :host_()
    {
      _stop = 0;
      mktimespec(&timeout_, 10000000000);
      wait_finish_ = false;
      waiting_ = false;
      handler_ = NULL;
    }

    ObMySQLCommandQueueThread::~ObMySQLCommandQueueThread()
    {
      stop();
    }

    int ObMySQLCommandQueueThread::init(int queue_capacity)
    {
      return queue_.init(queue_capacity, NULL);
    }

    void ObMySQLCommandQueueThread::set_self_to_thread_queue(const ObServer & host)
    {
      host_ = host;
    }
    void ObMySQLCommandQueueThread::setThreadParameter(int thread_count, ObMySQLPacketQueueHandler* handler,
                                                       void* args)
    {
      setThreadCount(thread_count);
      handler_ = handler;
      UNUSED(args);
    }
    void ObMySQLCommandQueueThread::set_ip_port(const IpPort & ip_port)
    {
      ip_port_ = ip_port;
    }

    bool ObMySQLCommandQueueThread::push(ObMySQLCommandPacket* packet, int max_queue_len, bool block)
    {
      if (max_queue_len > 0 && queue_.size() >= max_queue_len)
      {
        if (!block)
        {
          return false;
        }
      }
      queue_.push(packet, NULL, block);
      return true;
    }

    void ObMySQLCommandQueueThread::stop(bool wait_finish)
    {
      _stop = true;
      wait_finish_ = wait_finish;
    }

    void ObMySQLCommandQueueThread::run(tbsys::CThread* thread,void* args)
    {
      UNUSED(thread);
      UNUSED(args);
      ObServer *host = GET_TSI_MULT(ObServer, TSI_COMMON_OBSERVER_1);
      *host = host_;
      ObMySQLCommandPacket* packet = NULL;
      void *task = NULL;
      while (!_stop)
      {
        if (OB_SUCCESS != queue_.pop(task, &timeout_))
        {
          continue;
        }
        packet = reinterpret_cast<ObMySQLCommandPacket*>(task);
        if (handler_)
        {
          int64_t pop_time = tbsys::CTimeUtil::getTime();
          // 这个时候还没有source_chid和chid
          uint32_t trace_seq = atomic_inc(&(SeqGenerator::seq_generator_));
          TraceId *generated_id = GET_TSI_MULT(TraceId, TSI_COMMON_PACKET_TRACE_ID_1);
          (generated_id->id).seq_ = trace_seq;
          (generated_id->id).ip_ = ip_port_.ip_;
          (generated_id->id).port_ = ip_port_.port_;
          PFILL_SET_TRACE_ID(generated_id->uval_);
          PFILL_SET_QUEUE_SIZE(queue_.size());
          uint8_t pcode = packet->get_type();
          if (pcode != COM_DELETE_SESSION)
          {
            PFILL_SET_WAIT_SQL_QUEUE_TIME(pop_time - packet->get_receive_ts());
            PFILL_SET_PCODE(pcode);
            if (pcode == COM_STMT_EXECUTE)
            {
              PFILL_SET_SQL("EXEC", 4);
            }
            else
            {
              PFILL_SET_SQL(packet->get_command().ptr(),packet->get_command().length());
            }
          }
          PFILL_ITEM_START(handle_sql_time);
          handler_->handle_packet_queue(packet, args);
          PFILL_ITEM_END(handle_sql_time);
          PFILL_PRINT();
          PFILL_CLEAR_LOG();
        }
      }
      while (queue_.size() > 0)
      {
        if (OB_SUCCESS != queue_.pop(task, &timeout_))
        {
          continue;
        }
        packet = reinterpret_cast<ObMySQLCommandPacket*>(task);
        if (handler_ && wait_finish_)
        {
          int64_t pop_time = tbsys::CTimeUtil::getTime();
          uint32_t trace_seq = atomic_inc(&(SeqGenerator::seq_generator_));
          TraceId *generated_id = GET_TSI_MULT(TraceId, TSI_COMMON_PACKET_TRACE_ID_1);
          (generated_id->id).seq_ = trace_seq;
          (generated_id->id).ip_ = ip_port_.ip_;
          (generated_id->id).port_ = ip_port_.port_;
          PFILL_SET_TRACE_ID(generated_id->uval_);
          if (packet->get_type() != COM_DELETE_SESSION)
          {
            PFILL_SET_WAIT_SQL_QUEUE_TIME(pop_time - packet->get_receive_ts());
            PFILL_SET_PCODE(packet->get_type());
          }
          PFILL_ITEM_START(handle_sql_time);
          handler_->handle_packet_queue(packet, args);
          PFILL_ITEM_END(handle_sql_time);
        }
      }
    }
  }
}
