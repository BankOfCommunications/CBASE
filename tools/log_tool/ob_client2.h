#include "common/ob_define.h"
#include "common/ob_result.h"
#include "common/utility.h"
#include "common/ob_packet_factory.h"
#include "common/ob_client_manager.h"
#include "common/ob_ack_queue.h"
#if USE_LIBEASY
#include "common/ob_tbnet_callback.h"
#include "easy_io_struct.h"
#include "easy_io.h"
#endif

using namespace oceanbase::common;
class SimpleFormatter
{
  public:
    SimpleFormatter(): len_(sizeof(buf_)), pos_(0) {}
    ~SimpleFormatter() {}
    const char* format(const char* format, ...) {
      char* src = NULL;
      int64_t count = 0;
      va_list ap;
      va_start(ap, format);
      if (NULL == buf_ || len_ <= 0 || pos_ >= len_)
      {}
      else if ((count = vsnprintf(buf_ + pos_, len_ - pos_, format, ap)) + 1 <= len_ - pos_)
      {
        src = buf_ + pos_;
        pos_ += count + 1;
      }
      va_end(ap);
      return src;
    }
  private:
    char buf_[1<<16];
    int64_t len_;
    int64_t pos_;
};

int dump_to_file(const char* file, const char* buf, int64_t len)
{
  int err = OB_SUCCESS;
  int fd = -1;
  int64_t write_cnt = 0;
  if (NULL == file || NULL == buf || len < 0)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if ((fd = open(file, O_WRONLY | O_CREAT)) < 0)
  {
    err = OB_IO_ERROR;
    TBSYS_LOG(ERROR, "open(%s)=>%s", file, strerror(errno));
  }
  else if ((write_cnt = write(fd, buf, len)) != len)
  {
    err = OB_IO_ERROR;
    TBSYS_LOG(ERROR, "write fail, request count=%ld, done count=%ld", len, write_cnt);
  }
  if (fd >= 0)
  {
    close(fd);
  }
  return err;
}

struct Dummy
{
  int serialize(char* buf, int64_t len, int64_t& pos) const
  {
    UNUSED(buf); UNUSED(len); UNUSED(pos);
    return OB_SUCCESS;
  }
  int deserialize(char* buf, int64_t len, int64_t& pos)
  {
    UNUSED(buf); UNUSED(len); UNUSED(pos);
    return OB_SUCCESS;
  }
};

template <class T>
int uni_serialize(const T &data, char *buf, const int64_t data_len, int64_t& pos)
{
  return data.serialize(buf, data_len, pos);
};

template <class T>
int uni_deserialize(T &data, char *buf, const int64_t data_len, int64_t& pos)
{
  return data.deserialize(buf, data_len, pos);
};

template <>
int uni_serialize<uint64_t>(const uint64_t &data, char *buf, const int64_t data_len, int64_t& pos)
{
  return serialization::encode_vi64(buf, data_len, pos, (int64_t)data);
};

template <>
int uni_serialize<int64_t>(const int64_t &data, char *buf, const int64_t data_len, int64_t& pos)
{
  return serialization::encode_vi64(buf, data_len, pos, data);
};

template <>
int uni_deserialize<uint64_t>(uint64_t &data, char *buf, const int64_t data_len, int64_t& pos)
{
  return serialization::decode_vi64(buf, data_len, pos, (int64_t*)&data);
};

template <>
int uni_deserialize<int64_t>(int64_t &data, char *buf, const int64_t data_len, int64_t& pos)
{
  return serialization::decode_vi64(buf, data_len, pos, &data);
};

template <>
int uni_serialize<ObDataBuffer>(const ObDataBuffer &data, char *buf, const int64_t data_len, int64_t& pos)
{
  int err = OB_SUCCESS;
  if (NULL == buf || pos < 0 || pos > data_len)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "INVALID_ARGUMENT buf=%p[%ld:%ld]", buf, pos, data_len);
  }
  else if (pos + data.get_position() > data_len)
  {
    err = OB_BUF_NOT_ENOUGH;
    TBSYS_LOG(ERROR, "pos[%ld] + data_len[%ld] > buf_limit[%ld]", pos, data.get_position(), data_len);
  }
  else
  {
    memcpy(buf + pos, data.get_data(), data.get_position());
    pos += data.get_position();
  }
  return err;
};

template <>
int uni_deserialize<ObDataBuffer>(ObDataBuffer& data, char *buf, const int64_t data_len, int64_t& pos)
{
  UNUSED(data);
  UNUSED(buf);
  UNUSED(data_len);
  UNUSED(pos);
  return OB_NOT_SUPPORTED;
};

ObDataBuffer& __get_thread_buffer(ThreadSpecificBuffer& thread_buf, ObDataBuffer& buf)
{
  ThreadSpecificBuffer::Buffer* my_buffer = thread_buf.get_buffer();
  my_buffer->reset();
  buf.set_data(my_buffer->current(), my_buffer->remain());
  return buf;
}

template <class Input, class Output>
int send_request_with_rt(int64_t& rt, ObClientManager* client_mgr, const ObServer& server,
                 const int32_t version, const int pcode, const Input &param, Output &result,
                 ObDataBuffer& buf, const int64_t timeout)
{
  int err = OB_SUCCESS;
  int64_t start_time = tbsys::CTimeUtil::getTime();
  err = send_request(client_mgr, server, version, pcode, param, result, buf, timeout);
  rt = tbsys::CTimeUtil::getTime() - start_time;
  return err;
}

ObPCap& get_pcap()
{
  static ObPCap pcap(getenv("pcap_cmd"));
  return pcap;
}

template <class Input, class Output>
int send_request(int64_t* rt, ObClientManager* client_mgr, const ObServer& server,
                 const int32_t version, const int pcode, const Input &param, Output &result,
                 ObDataBuffer& buf, const int64_t timeout)
{
  int err = OB_SUCCESS;
  int64_t pos = 0;
  ObResultCode result_code;
  int64_t start_time = tbsys::CTimeUtil::getTime();
  int64_t old_position = buf.get_position();
  char cbuf[1<<20];
  ObDataBuffer out_buf(cbuf, sizeof(cbuf));
  if (OB_SUCCESS != (err = uni_serialize(param, buf.get_data(), buf.get_capacity(), buf.get_position())))
  {
    TBSYS_LOG(ERROR, "serialize(buf=%p[%ld:%ld])=>%d", buf.get_data(), buf.get_position(), buf.get_capacity(), err);
  }
  else
  {
    // ObPCap& pcap = get_pcap();
    // ObPacket packet;
    // ObDataBuffer pkt_buf;
    // pkt_buf.set_data(buf.get_data() + old_position, buf.get_position() - old_position);
    // packet.set_data(pkt_buf);
    // packet.set_data_length((int32_t)(buf.get_position() - old_position));
    // pcap.handle_packet(&packet);
  }
  if (OB_SUCCESS != err)
  {}
  else if (OB_SUCCESS != (err = client_mgr->send_request(server, pcode, version, timeout, buf, out_buf)))
  {
    TBSYS_LOG(WARN, "failed to send request, ret=%d", err);
  }
  else if (OB_SUCCESS != (err = result_code.deserialize(out_buf.get_data(), out_buf.get_position(), pos)))
  {
    TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, err);
  }
  else if (OB_SUCCESS != (err = result_code.result_code_))
  {
    TBSYS_LOG(DEBUG, "result_code.result_code = %d", err);
    if (OB_PACKET_CHECKSUM_ERROR == result_code.result_code_)
    {
      char* data_buf = buf.get_data() + old_position;
      int64_t data_len = buf.get_position() - old_position;
      int64_t crc = ob_crc64(data_buf, data_len);
      SimpleFormatter formater;
      TBSYS_LOG(ERROR, "packet body checksum error: pcode=%d server=%s checksum=%ld", pcode, to_cstring(server), crc);
      if (OB_SUCCESS != dump_to_file(formater.format("crc_error.%x", crc), data_buf, data_len))
      {
        TBSYS_LOG(ERROR, "write crc fail");
      }
    }
  }
  else if (OB_SUCCESS != (err = uni_deserialize(result, out_buf.get_data(), out_buf.get_position(), pos)))
  {
    TBSYS_LOG(ERROR, "deserialize result data failed:pos[%ld], ret[%d]", pos, err);
  }
  else if (NULL != rt)
  {
    *rt = tbsys::CTimeUtil::getTime() - start_time;
  }
  return err;
}

const int64_t DEFAULT_VERSION = 1;
const int64_t DEFAULT_TIMEOUT = 2 * 1000*1000;
#if !USE_LIBEASY
class TbnetBaseClient
{
  public:
    const static int64_t MAX_N_TRANSPORT = 256;
    TbnetBaseClient(): n_transport_(0) {}
    virtual ~TbnetBaseClient(){
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = destroy()))
      {
        TBSYS_LOG(ERROR, "destroy()=>%d", err);
      }
    }
  public:
    virtual int initialize(int64_t n_transport = 1) {
      int err = OB_SUCCESS;
      streamer_.setPacketFactory(&factory_);
      TBSYS_LOG(INFO, "client[n_transport=%ld]", n_transport);
      if (0 >= n_transport)
      {
        err = OB_INVALID_ARGUMENT;
      }
      for(int64_t i = 0; i < n_transport; i++)
      {
        if (OB_SUCCESS != (err = client_[i].initialize(&transport_[i], &streamer_)))
        {
          TBSYS_LOG(ERROR, "client_mgr.initialize()=>%d", err);
        }
        else if (!transport_[i].start())
        {
          err = OB_ERROR;
          TBSYS_LOG(ERROR, "transport.start()=>%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        n_transport_ = n_transport;
      }
      return err;
    }

    virtual int destroy() {
      for(int64_t i = 0; i < n_transport_; i++)
      {
        transport_[i].stop();
      }
      return wait();
    }

    virtual int wait(){
      int err = OB_SUCCESS;
      for(int64_t i = 0; OB_SUCCESS == err && i < n_transport_; i++)
      {
        err = transport_[i].wait();
      }
      return err;
    }

    // ObClientManager * get_rpc() {
    //   return &client_;
    // }

    template <class Server, class Input, class Output>
    int send_request(const Server server_spec, const int pcode,
                     const Input &param, Output &result, int64_t id = 0, int64_t* rt = NULL,
                     const int32_t version=DEFAULT_VERSION, const int64_t timeout=DEFAULT_TIMEOUT) {
      int err = OB_SUCCESS;
      ObDataBuffer buf;
      ObServer server;
      if (OB_SUCCESS != (err = to_server(server, server_spec)))
      {
        TBSYS_LOG(ERROR, "invalid server");
      }
      else if (OB_SUCCESS != (err = ::send_request(rt, &client_[id % n_transport_], server, version, pcode, param, result, __get_thread_buffer(rpc_buffer_, buf), timeout)))
      {
      }
      return err;
    }
  protected:
    int64_t n_transport_;
    tbnet::DefaultPacketStreamer streamer_;
    ObPacketFactory factory_;
    tbnet::Transport transport_[MAX_N_TRANSPORT];
    ObClientManager client_[MAX_N_TRANSPORT];
    ThreadSpecificBuffer rpc_buffer_;
};
typedef TbnetBaseClient BaseClient;
#else
int process(easy_request_t* r)
{
  int ret = EASY_OK;
  if (NULL == r || NULL == r->ipacket)
  {
    ret = EASY_BREAK;
  }
  else
  {
    ret = EASY_AGAIN;
  }
  return ret;
}

struct AsyncReqMonitor: public IObAsyncClientCallback
{
  AsyncReqMonitor(): succ_count_(0), fail_count_(0), wait_time_(0)  {}
  virtual ~AsyncReqMonitor(){}
  int handle_response(ObAckQueue::WaitNode& node) {
    ObServer null_server;
    if (OB_SUCCESS != node.err_)
    {
      __sync_fetch_and_add(&fail_count_, 1);
    }
    else if (!(node.server_ == null_server))
    {
      __sync_fetch_and_add(&succ_count_, 1);
      __sync_fetch_and_add(&wait_time_, tbsys::CTimeUtil::getTime() - node.send_time_us_);
    }
    return OB_SUCCESS;
  }
  int on_ack(ObAckQueue::WaitNode& node) {
    UNUSED(node);
    return OB_SUCCESS;
  }
  void assign(AsyncReqMonitor& that) {
    succ_count_ = that.succ_count_;
    fail_count_ = that.fail_count_;
    wait_time_ = that.wait_time_;
  }
  void diff(AsyncReqMonitor& that) {
    succ_count_ -= that.succ_count_;
    fail_count_ -= that.fail_count_;
    wait_time_ -= that.wait_time_;
  }
  //add wangjiahao [Paxos ups_replication] :b
  int get_quorum_seq(int64_t& quorum_seq) const
  {
    UNUSED(quorum_seq);
    return -1;
  }
  //add :e
  int64_t succ_count_;
  int64_t fail_count_;
  int64_t wait_time_;
};
struct AsyncReqReporter {
  AsyncReqReporter(AsyncReqMonitor& mon): mon_(mon) {
    start_time_ = tbsys::CTimeUtil::getTime();
    last_report_time_ = start_time_;
    last_report_stat_.assign(mon);
  }
  ~AsyncReqReporter() {}
  void report() {
    int64_t cur_time = tbsys::CTimeUtil::getTime();
    AsyncReqMonitor cur_stat;
    AsyncReqMonitor diff_stat;
    cur_stat.assign(mon_);
    diff_stat.assign(cur_stat);
    diff_stat.diff(last_report_stat_);
    TBSYS_LOG(INFO, "AsyncReqReport: tps=[%ld][%ld], fail=[%ld][%ld]",
              1000000 * diff_stat.succ_count_/(1 + cur_time - last_report_time_), 1000000 * cur_stat.succ_count_/(1 + cur_time - start_time_),
              diff_stat.fail_count_, cur_stat.fail_count_);
    last_report_stat_ = cur_stat;
    last_report_time_ = cur_time;
  }
  AsyncReqMonitor& mon_;
  AsyncReqMonitor last_report_stat_;
  int64_t start_time_;
  int64_t last_report_time_;
};

class EasyBaseClient
{
  public:
    const static int64_t MAX_N_CLIENT = 256;
    EasyBaseClient(): n_client_(0), io_thread_count_(0) {
      memset(eio_, 0, sizeof(eio_));
    }
    virtual ~EasyBaseClient(){
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = destroy()))
      {
        TBSYS_LOG(ERROR, "destroy()=>%d", err);
      }
    }
  public:
    int init_server_handler(easy_io_handler_pt& server_handler) {
      int err = OB_SUCCESS;
      memset(&server_handler, 0, sizeof(easy_io_handler_pt));
      server_handler.encode = ObTbnetCallback::encode;
      server_handler.decode = ObTbnetCallback::decode;
      server_handler.process = process;
      server_handler.get_packet_id = ObTbnetCallback::get_packet_id;
      server_handler.on_disconnect = ObTbnetCallback::on_disconnect;
      server_handler.user_data = this;
      return err;
    }
    virtual int initialize(int64_t io_thread_count = 1, int64_t dedicate_thread_count=1, int64_t n_client = 1, int64_t ack_queue_len = 4096) {
      int err = OB_SUCCESS;
      TBSYS_LOG(INFO, "client[io_thread_count=%ld, n_client=%ld]", io_thread_count, n_client);
      if (0 >= io_thread_count || 0 >= dedicate_thread_count)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = init_server_handler(server_handler_)))
      {}
      for(int64_t i = 0; OB_SUCCESS == err && i < n_client; i++)
      {
        if (NULL == (eio_[i] = easy_eio_create(eio_[i], (int)io_thread_count)))
        {
          err = OB_CONN_ERROR;
        }
        else if (OB_SUCCESS != (err = client_[i].initialize(eio_[i], &server_handler_)))
        {
          TBSYS_LOG(ERROR, "client_mgr.initialize()=>%d", err);
        }
        else if (OB_SUCCESS != (err = client_[i].set_dedicate_thread_num((int)dedicate_thread_count)))
        {
          TBSYS_LOG(ERROR, "client_mgr.initialize()=>%d", err);
        }
        else if (OB_SUCCESS != (err = easy_eio_start(eio_[i])))
        {
          TBSYS_LOG(ERROR, "easy_eio_start(i=%ld)=>%d", i, err);
        }
      }
      if (OB_SUCCESS != err)
      {}
      else if (OB_SUCCESS != (err = ack_queue_.init(&async_mon_, client_, ack_queue_len)))
      {
        TBSYS_LOG(ERROR, "ack_queue.init()=>%d", err);
      }
      else
      {
        n_client_ = n_client;
        io_thread_count_ = io_thread_count;
        dedicate_thread_count_=dedicate_thread_count;
      }
      return err;
    }

    virtual int destroy() {
      int err = OB_SUCCESS;
      for(int64_t i = 0; i < n_client_; i++)
      {
        easy_eio_stop(eio_[i]);
        easy_eio_wait(eio_[i]);
        easy_eio_destroy(eio_[i]);
      }
      for(int64_t i = 0; i < n_client_; i++)
      {
        //client_[i].destroy();
      }
      return err;
    }

    virtual int wait(){
      int err = OB_SUCCESS;
      for(int64_t i = 0; i < n_client_; i++)
      {
        easy_eio_wait(eio_[i]);
      }
      return err;
    }

    // ObClientManager * get_rpc() {
    //   return &client_;
    // }

    template <class Server, class Input, class Output>
    int send_request(const Server server_spec, const int pcode,
                     const Input &param, Output &result, int64_t id = 0, int64_t* rt = NULL,
                     const int32_t version=DEFAULT_VERSION, const int64_t timeout=DEFAULT_TIMEOUT) {
      int err = OB_SUCCESS;
      ObDataBuffer buf;
      ObServer server;
      if (OB_SUCCESS != (err = to_server(server, server_spec)))
      {
        TBSYS_LOG(ERROR, "invalid server(%s)", to_cstring(server_spec));
      }
      else if (OB_SUCCESS != (err = ::send_request(rt, &client_[id % n_client_], server, version, pcode, param, result, __get_thread_buffer(rpc_buffer_, buf), timeout)))
      {
      }
      return err;
    }
    template <class Server, class Input>
    int post_request(const Server server_spec, const int pcode,
                     const Input &param, const int64_t timeout=DEFAULT_TIMEOUT) {
      int err = OB_SUCCESS;
      ObDataBuffer buf;
      ObServer server;
      static int64_t req_seq = 0;
      int64_t seq = __sync_fetch_and_add(&req_seq, 1);
      __get_thread_buffer(rpc_buffer_, buf);
      if (OB_SUCCESS != (err = to_server(server, server_spec)))
      {
        TBSYS_LOG(ERROR, "invalid server(%s)", to_cstring(server_spec));
      }
      else if (OB_SUCCESS != (err = uni_serialize(param, buf.get_data(), buf.get_capacity(), buf.get_position())))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld:%ld])=>%d", buf.get_data(), buf.get_position(), buf.get_capacity(), err);
      }
      else if (OB_SUCCESS != (err = ack_queue_.many_post(&server, 1, seq, seq+1, pcode, timeout, buf, seq % dedicate_thread_count_)))
      {
        TBSYS_LOG(ERROR, "post_to_servers(pkt=%d, seq=%ld)=>%d", pcode, seq, err);
      }
      return err;
    }
    AsyncReqMonitor& get_async_monitor() { return async_mon_; }
  protected:
    int64_t n_client_;
    int64_t io_thread_count_;
    int64_t dedicate_thread_count_;
    easy_io_t *eio_[MAX_N_CLIENT];
    easy_io_handler_pt server_handler_;
    ObClientManager client_[MAX_N_CLIENT];
    ObAckQueue ack_queue_;
    AsyncReqMonitor async_mon_;
    ThreadSpecificBuffer rpc_buffer_;
};
typedef EasyBaseClient BaseClient;
#endif
Dummy _dummy_;
