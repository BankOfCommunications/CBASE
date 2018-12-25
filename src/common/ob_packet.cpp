#include "ob_packet.h"
#include "ob_thread_mempool.h"
#include "utility.h"

namespace oceanbase
{
  namespace common
  {
    ObVarMemPool ObPacket::out_mem_pool_(OB_MAX_PACKET_LENGTH);
    uint32_t ObPacket::global_chid = 0;
    ObPacket::ObPacket() : no_free_(false), ob_packet_header_size_(0), api_version_(0), timeout_(0), data_length_(0),
    priority_(NORMAL_PRI), target_id_(0), receive_ts_(0), session_id_(0), alloc_inner_mem_(false), trace_id_(0), req_sign_(0)
    {
    }

    ObPacket::~ObPacket()
    {
      if (alloc_inner_mem_)
      {
        out_mem_pool_.free(inner_buffer_.get_data());
      }
    }

    void ObPacket::free()
    {
      if (!no_free_)
      {
        delete this;
      }
    }

    uint32_t ObPacket::get_channel_id() const
    {
      return chid_;
    }

    void ObPacket::set_channel_id(uint32_t chid)
    {
      chid_ = chid;
    }

    int ObPacket::get_packet_len() const
    {
      return packet_len_;
    }

    void ObPacket::set_packet_len(int length)
    {
      packet_len_ = length;
    }

    void ObPacket::set_no_free()
    {
      no_free_ = true;
    }

    int32_t ObPacket::get_packet_code() const
    {
      return pcode_;
    }

    void ObPacket::set_packet_code(const int32_t packet_code)
    {
      pcode_ = packet_code;
    }

    int32_t ObPacket::get_target_id() const
    {
      return target_id_;
    }

    void ObPacket::set_target_id(const int32_t target_id)
    {
      target_id_ = target_id;
    }

    int64_t ObPacket::get_session_id() const
    {
      return session_id_;
    }

    void ObPacket::set_session_id(const int64_t session_id)
    {
      session_id_ = session_id;
    }

    int64_t ObPacket::get_expire_time() const
    {
      return expire_time_;
    }

    int32_t ObPacket::get_api_version() const
    {
      return static_cast<int32_t>(api_version_);
    }

    void ObPacket::set_api_version(const int32_t api_version)
    {
      api_version_ = static_cast<int16_t>(api_version);
    }

    void ObPacket::set_data(const ObDataBuffer& buffer)
    {
      buffer_ = buffer;
    }

    ObDataBuffer* ObPacket::get_buffer()
    {
      return &buffer_;
    }

    ObDataBuffer* ObPacket::get_inner_buffer()
    {
      return &inner_buffer_;
    }

    ObPacket* ObPacket::get_next() const
    {
      return _next;
    }

    easy_request_t* ObPacket::get_request() const
    {
      return req_;
    }

    void ObPacket::set_request(easy_request_t* r)
    {
      req_ = r;
    }

    int ObPacket::serialize()
    {
      int ret = do_check_sum();

      if (ret == OB_SUCCESS && target_id_ != OB_SELF_FLAG)
      {
        int64_t buf_size = header_.get_serialize_size();
        buf_size += buffer_.get_position();
        char* buff = (char*)out_mem_pool_.malloc(buf_size);
        if (buff == NULL)
        {
          ret = OB_MEM_OVERFLOW;
          TBSYS_LOG(ERROR, "alloc memory from out packet pool failed, buf size: %ld, errno: %d", buf_size, errno);
        }
        else
        {
          inner_buffer_.set_data(buff, buf_size);
          alloc_inner_mem_ = true;
        }
      }

      if (ret == OB_SUCCESS)
      {
        ret = header_.serialize(inner_buffer_.get_data(), inner_buffer_.get_capacity(), inner_buffer_.get_position());
      }

      if (ret == OB_SUCCESS)
      {
        if (inner_buffer_.get_remain() >= buffer_.get_position())
        {
          int64_t& current_position = inner_buffer_.get_position();
          memcpy(inner_buffer_.get_data() + inner_buffer_.get_position(), buffer_.get_data(), buffer_.get_position());
          current_position += buffer_.get_position();
        }
        else
        {
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    int ObPacket::serialize(ObDataBuffer* buffer)
    {
      int ret = OB_SUCCESS;
      if (NULL == buffer)
      {
        TBSYS_LOG(WARN, "invalid argument buffer is %p", buffer);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ret = do_check_sum();
        if (ret == OB_SUCCESS && target_id_ != OB_SELF_FLAG)
        {
          int64_t buf_size = header_.get_serialize_size();
          buf_size += buffer_.get_position();
          if (buf_size > buffer->get_capacity())
          {
            TBSYS_LOG(WARN, "buffer capacity is not enough, need %ld, but actual is %ld",
                      buf_size, buffer->get_capacity());
            ret = OB_ERROR;
          }
          else
          {
            buffer->get_position() = 0;
            ret = header_.serialize(buffer->get_data(), buffer->get_capacity(), buffer->get_position());
            if (ret == OB_SUCCESS)
            {
              if (buffer->get_remain() >= buffer_.get_position())
              {
                int64_t& current_position = buffer->get_position();
                memcpy(buffer->get_data() + buffer->get_position(), buffer_.get_data(), buffer_.get_position());
                current_position += buffer_.get_position();
              }
              else
              {
                ret = OB_ERROR;
              }
            }
          }
        }
      }
      return ret;
    }

    //add wangjiahao [Paxos ups_replication_tmplog] 20160609 :b
    int ObPacket::serialize(ObDataBuffer* buffer, int64_t cmt_log_seq)
    {
      int ret = OB_SUCCESS;
      if (NULL == buffer)
      {
        TBSYS_LOG(WARN, "invalid argument buffer is %p", buffer);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ret = do_check_sum(cmt_log_seq);
        if (ret == OB_SUCCESS && target_id_ != OB_SELF_FLAG)
        {
          int64_t buf_size = header_.get_serialize_size();
          buf_size += buffer_.get_position();
          if (buf_size > buffer->get_capacity())
          {
            TBSYS_LOG(WARN, "buffer capacity is not enough, need %ld, but actual is %ld",
                      buf_size, buffer->get_capacity());
            ret = OB_ERROR;
          }
          else
          {
            buffer->get_position() = 0;
            ret = header_.serialize(buffer->get_data(), buffer->get_capacity(), buffer->get_position());
            if (ret == OB_SUCCESS)
            {
              if (buffer->get_remain() >= buffer_.get_position())
              {
                int64_t& current_position = buffer->get_position();
                memcpy(buffer->get_data() + buffer->get_position(), buffer_.get_data(), buffer_.get_position());
                current_position += buffer_.get_position();
              }
              else
              {
                ret = OB_ERROR;
              }
            }
          }
        }
      }
      return ret;
    }

    int64_t ObPacket::get_cmt_log_seq()
    {
        return header_.reserved_;
    }
    //add :e

    int ObPacket::deserialize()
    {
      int ret = OB_SUCCESS;

      buffer_.set_data(inner_buffer_.get_data(), inner_buffer_.get_capacity());

      ret = header_.deserialize(buffer_.get_data(), buffer_.get_capacity(), buffer_.get_position());

      if (ret == OB_SUCCESS)
      {
        ret = do_sum_check();
      }
      else
      {
        TBSYS_LOG(ERROR, "deserialize header_ failed, ret=%d", ret);
      }

      // buffer_'s position now point to the user data
      return ret;
    }

    int ObPacket::do_check_sum()
    {
      int ret = OB_SUCCESS;

      header_.set_magic_num(OB_PACKET_CHECKSUM_MAGIC);
      header_.header_length_ = static_cast<int16_t>(header_.get_serialize_size());
      header_.version_ = 0;
      header_.reserved_ = 0;

      header_.data_length_ = static_cast<int32_t>(buffer_.get_position());
      header_.data_zlength_ = header_.data_length_; // not compressed

      header_.data_checksum_ = common::ob_crc64(buffer_.get_data(), buffer_.get_position());
      header_.set_header_checksum();
      return ret;
    }
    // add wangjiahao [Paxos ups_replication_tmplog] 20150609 :b
    int ObPacket::do_check_sum(const int64_t cmt_log_seq)
    {
      int ret = OB_SUCCESS;

      header_.set_magic_num(OB_PACKET_CHECKSUM_MAGIC);
      header_.header_length_ = static_cast<int16_t>(header_.get_serialize_size());
      header_.version_ = 0;
      header_.reserved_ = cmt_log_seq;

      header_.data_length_ = static_cast<int32_t>(buffer_.get_position());
      header_.data_zlength_ = header_.data_length_; // not compressed

      header_.data_checksum_ = common::ob_crc64(buffer_.get_data(), buffer_.get_position());
      header_.set_header_checksum();
      return ret;
    }
    //add :e

    int ObPacket::do_sum_check()
    {
      int ret = OB_SUCCESS;

      ret = header_.check_magic_num(OB_PACKET_CHECKSUM_MAGIC);

      if (ret == OB_SUCCESS)
      {
        ret = header_.check_header_checksum();
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "packet header check failed");
        }
      }
      else
      {
        TBSYS_LOG(WARN, "packet magic number check failed");
      }

      if (ret == OB_SUCCESS)
      {
        char* body_start = buffer_.get_data() + buffer_.get_position();
        int64_t body_length = header_.data_length_;

        ret = header_.check_check_sum(body_start, body_length);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "packet body checksum invalid");
        }
      }

      return ret;
    }

    bool ObPacket::encode(char* output, int64_t len)
    {
      int rc = OB_SUCCESS;
      bool ret = true;
      int64_t pos = 0;
      uint16_t ob_packet_header_size = get_ob_packet_header_size();
      if ( (OB_SUCCESS == (rc = serialization::encode_i32(output, len, pos, OB_TBNET_PACKET_FLAG)))
           && (OB_SUCCESS == (rc = serialization::encode_i32(output, len, pos, chid_)))
           && (OB_SUCCESS == (rc = serialization::encode_i32(output, len, pos, pcode_)))
           && (OB_SUCCESS == (rc = serialization::encode_i32(output, len, pos, packet_len_)))
           && (OB_SUCCESS == (rc = serialization::encode_i16(output, len, pos, ob_packet_header_size)))
           && (OB_SUCCESS == (rc = serialization::encode_i16(output, len, pos, api_version_)))
           && (OB_SUCCESS == (rc = serialization::encode_i64(output, len, pos, session_id_)))
           && (OB_SUCCESS == (rc = serialization::encode_i32(output, len, pos, timeout_))))

      {
#if !defined(_OB_VERSION) || _OB_VERSION<=300
#elif _OB_VERSION>300
        // lide.wd : 发包的时候，如果编译的时候是0.4 编译的，那么总是带上trace_id, 工具需要升级
        rc = serialization::encode_i64(output, len, pos, trace_id_);
        rc = serialization::encode_i64(output, len, pos, req_sign_);
#endif
        if (OB_SUCCESS == rc)
        {
          memcpy(output + pos, inner_buffer_.get_data(), inner_buffer_.get_position());
        }
      }

      if (OB_SUCCESS != rc)
      {
        TBSYS_LOG(ERROR, "encode packet into request output buffer faild, output is %p, pos is %ld",
                  output, pos);
        ret = false;
      }
      return ret;
    }

    int32_t ObPacket::get_data_length() const
    {
      return data_length_;
    }

    void ObPacket::set_data_length(int32_t datalen)
    {
      data_length_ = datalen;
    }

    void ObPacket::set_receive_ts(const int64_t receive_ts)
    {
      receive_ts_ = receive_ts;
    }

    int64_t ObPacket::get_receive_ts() const
    {
      return receive_ts_;
    }

    void ObPacket::set_packet_priority(const int32_t priority)
    {
      if (priority == NORMAL_PRI || priority == LOW_PRI)
      {
        priority_ = priority;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid packet priority: %d", priority);
      }
    }

    int32_t ObPacket::get_packet_priority() const
    {
      return priority_;
    }

    void ObPacket::set_source_timeout(const int64_t& timeout)
    {
      timeout_ = static_cast<int32_t>(timeout);
    }
    int64_t ObPacket::get_source_timeout() const
    {
      return timeout_;
    }

    void ObPacket::set_packet_buffer_offset(const int64_t buffer_offset)
    {
      buffer_offset_ = buffer_offset;
    }

    ObDataBuffer* ObPacket::get_packet_buffer()
    {
      return &inner_buffer_;
    }

    const ObDataBuffer* ObPacket::get_packet_buffer() const
    {
      return &inner_buffer_;
    }

    void ObPacket::set_packet_buffer(char* buffer, const int64_t buffer_length)
    {
      inner_buffer_.set_data(buffer, buffer_length);
    }

    int64_t ObPacket::get_header_size() const
    {
      return header_.get_serialize_size(); 
    }
    uint64_t ObPacket::get_trace_id() const
    {
      return trace_id_;
    }
    void ObPacket::set_trace_id(const uint64_t &trace_id)
    {
      trace_id_ = trace_id;
    }
    uint64_t ObPacket::get_req_sign() const
    {
      return req_sign_;
    }
    void ObPacket::set_req_sign(const uint64_t req_sign)
    {
      req_sign_ = req_sign;
    }
    void ObPacket::set_ob_packet_header_size(const uint16_t ob_packet_header_size)
    {
      ob_packet_header_size_ = ob_packet_header_size;
    }
    uint16_t ObPacket::get_ob_packet_header_size() const
    {
      return ob_packet_header_size_;
    }
  } /* common */
} /* oceanbase */
