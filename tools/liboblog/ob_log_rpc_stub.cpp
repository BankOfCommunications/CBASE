////===================================================================
 //
 // ob_log_rpc_stub.cpp liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-05-23 by Yubai (yubai.lk@alipay.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 // 
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#include "tbsys.h"
#include "easy_io.h"
#include "common/ob_tbnet_callback.h"
#include "ob_log_rpc_stub.h"

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
    ObLogRpcStub::ObLogRpcStub() : inited_(false),
                                   buffer_(),
                                   client_(),
                                   eio_(NULL),
                                   client_handler_()
    {
      memset(&client_handler_, 0, sizeof(easy_io_handler_pt));
      client_handler_.encode = ObTbnetCallback::encode;
      client_handler_.decode = ObTbnetCallback::decode;
      client_handler_.get_packet_id = ObTbnetCallback::get_packet_id;
    }

    ObLogRpcStub::~ObLogRpcStub()
    {
      destroy();
    }

    int ObLogRpcStub::init()
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == (eio_ = easy_eio_create(eio_, 1)))
      {
        TBSYS_LOG(WARN, "easy_eio_create fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        eio_->do_signal = 0;
        if (EASY_OK != easy_eio_start(eio_))
        {
          TBSYS_LOG(WARN, "easy_eio_start fail");
          ret = OB_ERR_UNEXPECTED;
        }
        else if (OB_SUCCESS != (ret = client_.initialize(eio_, &client_handler_)))
        {
          TBSYS_LOG(WARN, "initialize client fail ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = ObRpcStub::init(&buffer_, &client_)))
        {
          TBSYS_LOG(WARN, "rpc_stub init fail ret=%d", ret);
        }
        else
        {
          inited_ = true;
        }
      }
      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    void ObLogRpcStub::destroy()
    {
      inited_ = false;
      if (NULL != eio_)
      {
        easy_eio_stop(eio_);
        easy_eio_wait(eio_);
        easy_eio_destroy(eio_);
        eio_ = NULL;
      }
    }

    int ObLogRpcStub::fetch_log(const common::ObServer &ups,
        const updateserver::ObFetchLogReq& req,
        updateserver::ObFetchedLog& log,
        const int64_t timeout,
        common::ObResultCode &rc)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        errno_sensitive_pt orig = tc_errno_sensitive_func();
        tc_errno_sensitive_func() = fetch_log_errno_sensitive;
        ret = send_1_return_1(ups, timeout, OB_FETCH_LOG, DEFAULT_VERSION, rc, req, log);
        tc_errno_sensitive_func() = orig;
      }
      return ret;
    }

    int ObLogRpcStub::fetch_schema(const common::ObServer &rs,
        common::ObSchemaManagerV2& schema,
        const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        int64_t timestamp = 0;
        bool only_core_tables = false;
        ret = send_2_return_1(rs, timeout, OB_FETCH_SCHEMA, DEFAULT_VERSION, timestamp, only_core_tables, schema);
      }
      return ret;
    }

    int ObLogRpcStub::get_max_log_seq(const common::ObServer &ups, const int64_t timeout, int64_t &max_log_id)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = send_0_return_1(ups, timeout, OB_RS_GET_MAX_LOG_SEQ, DEFAULT_VERSION, max_log_id);
      }

      return ret;
    }

    bool ObLogRpcStub::fetch_log_errno_sensitive(const int error)
    {
      bool bret = true;
      if (OB_NEED_RETRY == error)
      {
        bret = false;
      }
      return bret;
    }

  }
}

