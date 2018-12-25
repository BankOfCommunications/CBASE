#ifndef _OCEANBASE_TOOLS_OB_SERVER_CLIENT_
#define _OCEANBASE_TOOLS_OB_SERVER_CLIENT_

#include "tbnet.h"
#include "common/ob_define.h"
#include "common/ob_client_manager.h"
#include "common/ob_packet_factory.h"
#include "common/ob_result.h"
#include "common/ob_malloc.h"
#include "ob_admin_utils.h"

namespace oceanbase {
    namespace tools {
        class ObServerClient
        {
        public:
            ObServerClient() : rpc_buffer_(BUF_SIZE), inited_(false) {}
            virtual ~ObServerClient() {}

        public:
            virtual int initialize(const ObServer &server);
            virtual int destroy();
            virtual int wait();

            bool inited() const { return inited_; }

        protected:
            int send_command(const int pcode, const int64_t timeout);
            template <class Input>
            int send_command(const int pcode, const Input &param, const int64_t timeout);
            template <class Output>
            int send_request(const int pcode, Output &result, const int64_t timeout);
            template <class Input, class Output>
            int send_request(const int pcode, const Input &param, Output &result, const int64_t timeout);

            int get_thread_buffer_(ObDataBuffer& data_buff);
            ObClientManager * get_rpc() { return &client_; }

        private:
            tbnet::DefaultPacketStreamer streamer_;
            tbnet::Transport transport_;
            ObPacketFactory factory_;
            ObClientManager client_;

            static const int32_t BUF_SIZE = 2 * 1024 * 1024;
            ObServer server_;
            ThreadSpecificBuffer rpc_buffer_;
            bool inited_;
        };
    } // end of namespace tools
}   // end of namespace oceanbase

namespace oceanbase {
    namespace tools {
        inline int ObServerClient::initialize(const ObServer &server)
        {
            ob_init_memory_pool();
            streamer_.setPacketFactory(&factory_);
            client_.initialize(&transport_, &streamer_);
            server_ = server;
            inited_ = true;
            return transport_.start();
        }

        inline int ObServerClient::destroy()
        {
            transport_.stop();
            return transport_.wait();
        }

        inline int ObServerClient::wait()
        {
            return transport_.wait();
        }

        inline int ObServerClient::get_thread_buffer_(ObDataBuffer& data_buff) {
            int err = OB_SUCCESS;
            ThreadSpecificBuffer::Buffer* buffer = rpc_buffer_.get_buffer();

            buffer->reset();
            data_buff.set_data(buffer->current(), buffer->remain());

            return err;
        };

        inline int ObServerClient::send_command(const int pcode, const int64_t timeout)
        {
            int ret = OB_SUCCESS;
            static const int32_t MY_VERSION = 1;
            ObDataBuffer data_buff;
            get_thread_buffer_(data_buff);

            ObClientManager* client_mgr = get_rpc();
            ret = client_mgr->send_request(server_, pcode, MY_VERSION, timeout, data_buff);
            if (OB_SUCCESS != ret)
            {
                TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
            }
            else
            {
                // deserialize the response code
                int64_t pos = 0;
                if (OB_SUCCESS == ret)
                {
                    ObResultCode result_code;
                    ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
                    if (OB_SUCCESS != ret)
                    {
                        TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
                    }
                    else
                    {
                        ret = result_code.result_code_;
                    }
                }
            }
            return ret;
        };

        template <class Input>
        int ObServerClient::send_command(const int pcode, const Input &param, const int64_t timeout)
        {
            int ret = OB_SUCCESS;
            static const int32_t MY_VERSION = 1;
            ObDataBuffer data_buff;
            get_thread_buffer_(data_buff);

            ObClientManager* client_mgr = get_rpc();
            ret = temp_serialize(param, data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
            if (OB_SUCCESS == ret)
            {
                ret = client_mgr->send_request(server_, pcode, MY_VERSION, timeout, data_buff);
                if (OB_SUCCESS != ret)
                {
                    TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
                }
            }
            if (OB_SUCCESS == ret)
            {
                // deserialize the response code
                int64_t pos = 0;
                if (OB_SUCCESS == ret)
                {
                    ObResultCode result_code;
                    ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
                    if (OB_SUCCESS != ret)
                    {
                        TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
                    }
                    else
                    {
                        ret = result_code.result_code_;
                    }
                }
            }
            return ret;
        };

        template <class Output>
        int ObServerClient::send_request(const int pcode, Output &result, const int64_t timeout)
        {
            int ret = OB_SUCCESS;
            static const int32_t MY_VERSION = 1;
            ObDataBuffer data_buff;
            get_thread_buffer_(data_buff);

            ObClientManager* client_mgr = get_rpc();
            ret = client_mgr->send_request(server_, pcode, MY_VERSION, timeout, data_buff);
            if (OB_SUCCESS != ret)
            {
                TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
            }
            else
            {
                // deserialize the response code
                int64_t pos = 0;
                if (OB_SUCCESS == ret)
                {
                    ObResultCode result_code;
                    ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
                    if (OB_SUCCESS != ret)
                    {
                        TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
                    }
                    else
                    {
                        ret = result_code.result_code_;
                        if (OB_SUCCESS == ret
                            && OB_SUCCESS != (ret = temp_deserialize(result, data_buff.get_data(),
                                    data_buff.get_position(), pos)))
                        {
                            TBSYS_LOG(ERROR, "deserialize result data failed:pos[%ld], ret[%d]", pos, ret);
                        }
                    }
                }
            }
            return ret;
        }

        template <class Input, class Output>
        int ObServerClient::send_request(const int pcode, const Input &param,
            Output &result, const int64_t timeout)
        {
            int ret = OB_SUCCESS;
            static const int32_t MY_VERSION = 1;
            ObDataBuffer data_buff;
            get_thread_buffer_(data_buff);

            ObClientManager* client_mgr = get_rpc();
            ret = temp_serialize(param, data_buff.get_data(),
                data_buff.get_capacity(), data_buff.get_position());
            if (OB_SUCCESS == ret)
            {
                ret = client_mgr->send_request(server_, pcode, MY_VERSION, timeout, data_buff);
                if (OB_SUCCESS != ret)
                {
                    TBSYS_LOG(WARN, "failed to Nend request, ret=%d", ret);
                }
            }
            if (OB_SUCCESS == ret)
            {
                // deserialize the response code
                int64_t pos = 0;
                if (OB_SUCCESS == ret)
                {
                    ObResultCode result_code;
                    ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
                    if (OB_SUCCESS != ret)
                    {
                        TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
                    }
                    else
                    {
                        ret = result_code.result_code_;
                        if (OB_SUCCESS == ret
                            && OB_SUCCESS != (ret = temp_deserialize(result, data_buff.get_data(),
                                    data_buff.get_position(), pos)))
                        {
                            TBSYS_LOG(ERROR, "deserialize result data failed:pos[%ld], ret[%d]", pos, ret);
                        }
                    }
                }
            }
            return ret;
        }
    } // end of namespace tools
}   // end of namespace oceanbase


#endif /* _OCEANBASE_TOOLS_OB_SERVER_CLIENT_ */

