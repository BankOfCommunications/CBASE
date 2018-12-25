#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include "ob_packet.h"
#include "ob_pcap.h"

namespace oceanbase
{
  namespace common
  {
    int copy_from_ring_buf(char* dest, const char* src, const int64_t ring_buf_len,
                           const int64_t start_pos, const int64_t len)
    {
      int err = OB_SUCCESS;
      int64_t bytes_to_boundary = 0;
      if (NULL == dest || NULL == src || 0 >= ring_buf_len
          || len > ring_buf_len || 0 > start_pos)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "copy_from_ring_buf(dest=%p, src=%p[%ld], range=[%ld,+%ld]): invalid argument",
                  dest, src, ring_buf_len, start_pos, len);
      }
      else
      {
        bytes_to_boundary = ring_buf_len - start_pos % ring_buf_len;
        memcpy(dest, src + start_pos % ring_buf_len, min(len, bytes_to_boundary));
        if (bytes_to_boundary < len)
        {
          memcpy(dest + bytes_to_boundary, src, len - bytes_to_boundary);
        }
      }
      return err;
    }

    int copy_to_ring_buf(char* dest, const char* src, const int64_t ring_buf_len,
                           const int64_t start_pos, const int64_t len)
    {
      int err = OB_SUCCESS;
      int64_t bytes_to_boundary = 0;
      if (NULL == dest || NULL == src || 0 >= ring_buf_len || 0 > len || len > ring_buf_len
          || 0 > start_pos)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "copy_to_ring_buf(dest=%p, src=%p[%ld], range=[%ld,+%ld]): invalid argument",
                  dest, src, ring_buf_len, start_pos, len);
      }
      else
      {
        bytes_to_boundary = ring_buf_len - start_pos % ring_buf_len;
        memcpy(dest +  start_pos % ring_buf_len, src, min(len, bytes_to_boundary));
        if (bytes_to_boundary < len)
        {
          memcpy(dest, src + bytes_to_boundary, len - bytes_to_boundary);
        }
      }
      return err;
    }

    int popen(const char* cmd, int flag)
    {
      int fd = -1;
      int pid = 0;
      int fd_idx = 0;
      int pipefd[2] = {-1, -1};
      bool is_read_only = O_RDONLY == (flag & 0x11);
      if (NULL == cmd)
      {
        TBSYS_LOG(ERROR, "INVALID ARGUMENT: cmd == NULL");
      }
      else if (0 != pipe(pipefd))
      {
        TBSYS_LOG(ERROR, "pipe()=>%s", strerror(errno));
      }
      else if (0 > (pid = fork()))
      {
        TBSYS_LOG(ERROR, "fork()=>%s", strerror(errno));
      }
      else if (0 == pid)
      {
        fd_idx = is_read_only? 1: 0;
        if (0 != close(pipefd[fd_idx^1]))
        {
          TBSYS_LOG(ERROR, "close(%d)=>%s", pipefd[fd_idx^1], strerror(errno));
        }
        else if (0 > dup2(pipefd[fd_idx], fd_idx))
        {
          TBSYS_LOG(ERROR, "dup2(%d)=>%s", pipefd[fd_idx], strerror(errno));
        }
        else if (0 != execlp("sh", "sh", "-c", cmd, (char*)NULL))
        {
          TBSYS_LOG(ERROR, "exec(cmd=%s)=>%s", cmd, strerror(errno));
        }
      }
      else if (0 < pid)
      {
        fd_idx = is_read_only? 0: 1;
        if (0 != close(pipefd[fd_idx^1]))
        {
          TBSYS_LOG(ERROR, "close(%d)=>%s", pipefd[fd_idx^1], strerror(errno));
        }
        else
        {
          fd = pipefd[fd_idx];
        }
      }
      return fd;
    }

    int64_t write_all(int fd, char* buf, int64_t len)
    {
      int err = OB_SUCCESS;
      int64_t total_write_size = 0;
      int64_t write_size = 0;
      if (fd < 0 || NULL == buf || len < 0)
      {
        err = OB_INVALID_ARGUMENT;
      }
      for(; OB_SUCCESS == err && total_write_size < len; total_write_size += write_size)
      {
        write_size = write(fd, buf + total_write_size, len - total_write_size);
        if (0 >= write_size && errno != EAGAIN && errno != EINTR)
        {
          err = OB_IO_ERROR;
          TBSYS_LOG(ERROR, "write(fd=%d, size=%ld)=>%s", fd, len - total_write_size, strerror(errno));
        }
      }
      return total_write_size;
    }

    int64_t read_all(int fd, char* buf, int64_t len)
    {
      int err = OB_SUCCESS;
      int64_t total_read_size = 0;
      int64_t read_size = 0;
      if (fd < 0 || NULL == buf || len < 0)
      {
        err = OB_INVALID_ARGUMENT;
      }
      for(; OB_SUCCESS == err && total_read_size < len; total_read_size += read_size)
      {
        read_size = read(fd, buf + total_read_size, len - total_read_size);
        if (0 >= read_size && errno != EAGAIN && errno != EINTR && errno != EWOULDBLOCK)
        {
          err = OB_IO_ERROR;
          TBSYS_LOG(ERROR, "read(fd=%d, size=%ld)=>%s[%d]", fd, len - total_read_size, strerror(errno), errno);
        }
      }
      return total_read_size;
    }

    IRLock::IRLock(uint64_t bs_shift, uint64_t bcount): bs_shift_(bs_shift), bcount_(bcount), start_(0), end_(0)
    {
      if (bs_shift <= 0 || bs_shift > 64 || bcount_ <= 0)
      {
        TBSYS_LOG(ERROR, "INVALID ARGUMENT: bs_shif=%ld, bcount=%ld", bs_shift, bcount);
      }
      else
      {
        refs_ = new(std::nothrow)int64_t[bcount]();
        update_ref(end_, 1);
      }
    }

    IRLock::~IRLock()
    {
      if (NULL != refs_)
      {
        delete [] refs_;
        refs_ = NULL;
      }
    }

    void IRLock::debug()
    {
      for(uint64_t i = 0; i < bcount_; i++)
      {
        TBSYS_LOG(ERROR, "irlock[%ld]=%ld", i, refs_[i]);
      }
    }

    uint64_t IRLock::get_start()
    {
      uint64_t old_start = start_;
      uint64_t start = 0;
      for(start = start_; get_ref(start) <= 0; start += 1ULL<<bs_shift_)
        ;
      __sync_bool_compare_and_swap(&start_, old_start, start);
      return start_;
    }

    uint64_t IRLock::get_end()
    {
      return end_;
    }

    int IRLock::use(uint64_t pos)
    {
      int err = OB_SUCCESS;
      uint64_t end = end_;
      if (NULL == refs_)
      {
        err = OB_NOT_INIT;
      }
      else if (pos < end)
      {
        err = OB_ERR_UNEXPECTED;
      }
      else if (!__sync_bool_compare_and_swap(&end_, end, pos))
      {
        err = OB_EAGAIN;
      }
      else
      {
        update_ref(pos, 2);
        update_ref(end, -1);
      }
      return err;
    }

    int IRLock::done(uint64_t pos)
    {
      int err = OB_SUCCESS;
      if (NULL == refs_)
      {
        err = OB_NOT_INIT;
      }
      else if (pos > end_)
      {
        err = OB_ERR_UNEXPECTED;
      }
      else
      {
        update_ref(pos, -1);
      }
      return err;
    }

    QBuf::QBuf(uint64_t bs_shift, uint64_t bcount): size_mask_((1ULL<<bs_shift) * bcount - 1), reserved_size_(1ULL<<bs_shift),
                                                    lock_(0), irlock_(bs_shift, bcount),
                                                    push_(0), pop_(0)
    {
      buf_ = new(std::nothrow)char[size_mask_ + 1 + reserved_size_];
    }

    QBuf::~QBuf()
    {
      if (NULL != buf_)
      {
        delete []buf_;
        buf_ = NULL;
      }
    }

    int64_t QBuf::get_free()
    {
      return (int64_t)(pop_ + size_mask_ + 1) - (int64_t)(push_);
    }

    int QBuf::write(uint64_t pos, char* buf, int64_t len)
    {
      return copy_to_ring_buf(buf_, buf, size_mask_ + 1, pos, len);
    }

    int QBuf::read(uint64_t pos, char* buf, int64_t len)
    {
      return copy_from_ring_buf(buf, buf_, size_mask_ + 1, pos, len);
    }
    
    int QBuf::cpytail(uint64_t pos, uint64_t len)
    {
      int err = OB_SUCCESS;
      uint64_t next_align = (pos + size_mask_) & (~size_mask_);
      if (len > reserved_size_)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "INVALID_ARGUMENT: pos=%ld, len=%ld, reserved=%ld", pos, len, reserved_size_);
      }
      else if (pos + len > next_align)
      {
        memcpy(buf_ + size_mask_ + 1, buf_, pos + len - next_align);
      }
      return err;
    }

    int QBuf::append(char* buf, int64_t len)
    {
      int err = OB_SUCCESS;
      SeqLockGuard guard(lock_);
      if (push_ + len > irlock_.get_start() + size_mask_ + 1)
      {
        err = OB_EAGAIN;
      }
      else if (OB_SUCCESS != (err = write(push_, buf, len)))
      {
        TBSYS_LOG(ERROR, "write(pos=%ld, buf=%p[%ld])=>%d", push_, buf, len, err);
      }
      else
      {
        __sync_add_and_fetch(&push_, len);
      }
      return err;
    }

    int QBuf::pop_done(uint64_t pos)
    {
      return irlock_.done(pos);
    }

    int QBuf::pop(uint64_t& pos, char*& buf, int64_t& data_len)
    {
      int err = OB_SUCCESS;
      {
        SeqLockGuard guard(lock_);
        pos = pop_;
        if (pos + reserved_size_ > push_)
        {
          err = OB_EAGAIN;
        }
        else if (OB_SUCCESS != (err = read(pos, (char*)&data_len, sizeof(data_len))))
        {
          TBSYS_LOG(ERROR, "read(pos=%ld, buf=%p[%ld])=>%d", pos, &data_len, sizeof(data_len), err);
        }
        else if (data_len <= 0 || (uint64_t)data_len > reserved_size_ || pos + (uint64_t)data_len > push_)
        {
          err = OB_ERR_UNEXPECTED;
          TBSYS_LOG(ERROR, "INVALID DATA LEN: pos=%ld, data_len=%ld, push_=%ld", pos, data_len, push_);
        }
        else if (OB_SUCCESS != (err = irlock_.use(pos)))
        {
          TBSYS_LOG(ERROR, "irlock_.use(pos=%ld)=>%d", pos, err);
        }
        else
        {
          //TBSYS_LOG(INFO, "data_len=%ld", data_len);
          __sync_add_and_fetch(&pop_, data_len);
        }
      }
      if (OB_SUCCESS != err)
      {}
      else if (OB_SUCCESS != (err = cpytail(pos, data_len)))
      {
        TBSYS_LOG(ERROR, "cpy2tail(pos=%ld, data_len=%ld)=>%d", pos, data_len, err);
      }
      else
      {
        buf = buf_ + (pos&size_mask_);
      }
      return err;
    }

    int plain_decode_i64(char* buf, int64_t limit, int64_t& pos, int64_t& value)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (NULL == buf || limit < 0 || new_pos < 0 || new_pos > limit)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "INVALID_ARGUMENT: buf=%p[%ld:%ld]", buf, pos, limit);
      }
      else if (new_pos + (int64_t)sizeof(int64_t) > limit)
      {
        err = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(ERROR, "pos[%ld] + 8 > limit[%ld]", new_pos, limit);
      }
      else
      {
        value = *(int64_t*)(buf + pos);
        pos = new_pos + sizeof(int64_t);
      }
      return err;
    }

    int plain_encode_i64(char* buf, int64_t limit, int64_t& pos, int64_t value)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (NULL == buf || limit < 0 || new_pos < 0 || new_pos > limit)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "INVALID_ARGUMENT: buf=%p[%ld:%ld]", buf, pos, limit);
      }
      else if (new_pos + (int64_t)sizeof(int64_t) > limit)
      {
        err = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(ERROR, "pos[%ld] + 8 > limit[%ld]", new_pos, limit);
      }
      else
      {
        *(int64_t*)(buf + pos) = value;
        pos = new_pos + sizeof(int64_t);
      }
      return err;
    }

    int serialize_packet(char* buf, int64_t limit, int64_t& pos, ObPacket& pkt)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      int64_t data_len = pkt.get_data_length();
      ObDataBuffer* inbuf = pkt.get_buffer();
      int64_t header_size = 20;
      if (OB_SUCCESS != (err = plain_encode_i64(buf, limit, new_pos, data_len + header_size)))
      {
        TBSYS_LOG(ERROR, "encode_i64()=>%d", err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i32(buf, limit, new_pos, pkt.get_packet_code())))
      {
        TBSYS_LOG(ERROR, "encode_i64()=>%d", err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i64(buf, limit, new_pos, pkt.get_req_sign())))
      {
        TBSYS_LOG(ERROR, "encode_i64()=>%d", err);
      }
      else if (new_pos + data_len > limit)
      {
        err = OB_BUF_NOT_ENOUGH;
      }
      else
      {
        memcpy(buf + new_pos, inbuf->get_data() + inbuf->get_position(), data_len);
        pos = new_pos + data_len;
      }
      return err;
    }

    int deserialize_packet(char* buf, int64_t limit, int64_t& pos, ObPacket& pkt)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      int pkt_code = 0;
      int64_t data_len = 0;
      uint64_t req_sign = 0;
      const int64_t header_size = 20;
      ObDataBuffer* pkt_buf = pkt.get_buffer();
      pkt_buf->get_position() = 0;
      if (OB_SUCCESS != (err = plain_decode_i64(buf, limit, new_pos, data_len)))
      {
        TBSYS_LOG(ERROR, "decode_i64()=>%d", err);
      }
      else
      {
        data_len -= header_size;
      }
      if (OB_SUCCESS != err)
      {}
      else if(OB_SUCCESS != (err = serialization::decode_i32(buf, limit, new_pos, &pkt_code)))
      {
        TBSYS_LOG(ERROR, "decode_i32()=>%d", err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i64(buf, limit, new_pos, (int64_t*)&req_sign)))
      {
        TBSYS_LOG(ERROR, "decode_i64()=>%d", err);
      }
      else if (new_pos + data_len > limit)
      {
        err = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(ERROR, "pos[%ld] + data_len[%ld] > limit[%ld]", new_pos, data_len, limit);
      }
      else if (pkt_buf->get_position() + data_len > pkt_buf->get_capacity())
      {
        err = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(ERROR, "pos[%ld] + data_len[%ld] > limit[%ld]", pkt_buf->get_position(), data_len, pkt_buf->get_capacity());
      }
      else
      {
        pkt.set_packet_code(pkt_code);
        pkt.set_req_sign(req_sign);
        memcpy(pkt_buf->get_data() + pkt_buf->get_position(), buf + new_pos, data_len);
        pkt_buf->get_position() += data_len;
        pos = new_pos + data_len;
      }
      return err;
    }

    ObPCap::ObPCap(const char* pcap_cmd): lock_(0), fd_(-1), buf_(cbuf_, sizeof(cbuf_)),
                                           count_reporter_("pcap", 10000), total_size_(0)
    {
      if (NULL != pcap_cmd && pcap_cmd[0] != '\0')
      {
        if ((fd_ = popen(pcap_cmd, O_WRONLY)) < 0)
        {
          TBSYS_LOG(ERROR, "open(%s)=>%s", pcap_cmd, strerror(errno));
        }
      }
      TBSYS_LOG(INFO, "pcap.init: cmd=[%s], fd=%d", pcap_cmd, fd_);
    }

    ObPCap::~ObPCap()
    {
      if (fd_ > 0)
      {
        flush_buf();
        close(fd_);
      }
    }

    int ObPCap::flush_buf()
    {
      int err = OB_SUCCESS;
      if (write_all(fd_, buf_.get_data(), buf_.get_position()) != buf_.get_position())
      {
        err = OB_IO_ERROR;
        TBSYS_LOG(ERROR, "write_all(fd=%d, buf=%p[%ld])=>%d", fd_, buf_.get_data(), buf_.get_position(), err);
      }
      else
      {
        total_size_ += buf_.get_position();
        buf_.get_position() = 0;
        TBSYS_LOG(TRACE, "flush_buf(total_size=%ld)", total_size_);
      }
      return err;
    }

    int ObPCap::do_capture_packet(ObPacket& packet)
    {
      int err = OB_SUCCESS;
      SeqLockGuard lock_guard(lock_);
      if (buf_.get_remain() < OB_MAX_PACKET_LENGTH && OB_SUCCESS != (err = flush_buf()))
      {
        TBSYS_LOG(ERROR, "flush()=>%d", err);
      }
      else if (OB_SUCCESS != (err = serialize_packet(buf_.get_data(), buf_.get_capacity(), buf_.get_position(), packet)))
      {
        TBSYS_LOG(ERROR, "packet->serialize(buf_)=>%d", err);
      }
      else
      {
        //count_reporter_.inc();
      }
      return err;
    }

    int ObPCap::handle_packet(ObPacket* packet)
    {
      int err = OB_SUCCESS;
      if (NULL == packet)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (fd_ > 0 && OB_SUCCESS != (err = do_capture_packet(*packet)))
      {
        TBSYS_LOG(ERROR, "do_capture_packet(pkt=%p)=>%d", packet, err);
      }
      return err;
    }

    ObPFetcher::ObPFetcher(const char* pfetch_cmd, uint64_t bs_shift, uint64_t bcount): lock_(0), fd_(-1), qbuf_(bs_shift, bcount),
                                           count_reporter_("pfetch", 10000), total_size_(0)
    {
      if (NULL != pfetch_cmd && pfetch_cmd[0] != '\0')
      {
        if ((fd_ = popen(pfetch_cmd, O_RDONLY)) < 0)
        {
          TBSYS_LOG(ERROR, "open(%s)=>%s", pfetch_cmd, strerror(errno));
        }
      }
      TBSYS_LOG(INFO, "pfetcher.init: cmd=[%s], fd=%d", pfetch_cmd, fd_);
    }

    ObPFetcher::~ObPFetcher()
    {
      if (fd_ > 0)
      {
        close(fd_);
      }
    }

    int ObPFetcher::try_read_buf()
    {
      int err = OB_SUCCESS;
      OnceGuard guard(lock_);
      if (!guard.try_lock())
      {}
      else if (qbuf_.get_free() > 2 * (int64_t)sizeof(cbuf_))
      {
        int64_t read_size = 0;
        read_size = read_all(fd_, cbuf_, sizeof(cbuf_));
        if (read_size <= 0)
        {
          err = OB_READ_NOTHING;
        }
        else
        {
          err = OB_EAGAIN;
          while(OB_EAGAIN == err)
          {
            if (OB_SUCCESS != (err = qbuf_.append(cbuf_, read_size))
                && OB_EAGAIN != err)
            {
              TBSYS_LOG(ERROR, "qbuf.append()=>%d", err);
            }
          }
        }
        if (OB_SUCCESS == err)
        {
          TBSYS_LOG(TRACE, "read_buf(total_size=%ld)", total_size_ += read_size);
        }
      }
      return err;
    }

    int ObPFetcher::get_packet(ObPacket* packet)
    {
      int err = OB_SUCCESS;
      uint64_t pos = 0;
      char* buf = NULL;
      int64_t len = 0;
      if (NULL == packet)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (fd_ < 0)
      {
        err = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (err = try_read_buf()))
      {
        TBSYS_LOG(WARN, "read_buf()=>%d", err);
      }
      else if (OB_SUCCESS != (err = qbuf_.pop(pos, buf, len))
               && OB_EAGAIN != err)
      {
        TBSYS_LOG(ERROR, "qbuf.pop(pos=%ld, buf=%p[%ld])=>%d", pos, buf, len, err);
      }
      else if (OB_EAGAIN == err)
      {}
      else
      {
        int64_t tmp_pos = 0;
        if (OB_SUCCESS != (err = deserialize_packet(buf, len, tmp_pos, *packet)))
        {
          TBSYS_LOG(ERROR, "packet->deserialize(buf=%p[%ld])=>%d", buf, len, err);
        }
        else
        {
          count_reporter_.inc();
        }
        if (OB_SUCCESS != (err = qbuf_.pop_done(pos)))
        {
          TBSYS_LOG(ERROR, "pop_done(pos=%ld)=>%d", pos, err);
        }
      }
      return err;
    }

    int ObPFetcher::get_match_packet(ObPacket* packet, int pkt_code)
    {
      int err = OB_SUCCESS;
      if (NULL == packet)
      {
        err = OB_INVALID_ARGUMENT;
      }
      while(OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = get_packet(packet))
            && OB_EAGAIN != err)
        {
          TBSYS_LOG(ERROR, "get_packet()=>%d", err);
        }
        else if (OB_EAGAIN == err)
        {}
        else if (packet->get_packet_code() == pkt_code)
        {
          break;
        }
        TBSYS_LOG(DEBUG, "read packet: %d, err=%d", packet->get_packet_code(), err);
      }
      return err;
    }
  }; // end namespace common
}; // end namespace oceanbase
