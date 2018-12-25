#include "client_wrapper.h"

using namespace oceanbase;
using namespace common;

int MKClient::init(const char *addr, const int port)
{
  ObServer dst_host;
  dst_host.set_ipv4_addr(addr, port);
  return cli_.init(dst_host);
}

int MKClient::apply(common::ObMutator &mutator)
{
  return cli_.ups_apply(mutator, TIMEOUT_US);
}

int MKClient::get(common::ObGetParam &get_param, common::ObScanner &scanner)
{
  return cli_.ups_get(get_param, scanner, TIMEOUT_US);
}

int MKClient::scan(common::ObScanParam &scan_param, common::ObScanner &scanner)
{
  return cli_.ups_scan(scan_param, scanner, TIMEOUT_US);
}

////////////////////////////////////////////////////////////////////////////////////////////////////

int OBClient::update_ups_addr_()
{
  ObUpsList ups_list;
  cli_.set_server(rs_addr_);
  int ret = cli_.get_ups_list(ups_list, TIMEOUT_US);
  if (OB_SUCCESS == ret)
  {
    ret = OB_ENTRY_NOT_EXIST;
    for (int64_t i = 0; i < ups_list.ups_count_; i++)
    {
      if (UPS_MASTER == ups_list.ups_array_[i].stat_)
      {
        ups_addr_ = ups_list.ups_array_[i].addr_;
        ret = OB_SUCCESS;
        break;
      }
    }
  }
  return ret;
}

int OBClient::init(const char *addr, const int port)
{
  rs_addr_.set_ipv4_addr(addr, port);
  int ret = cli_.init(rs_addr_);
  if (OB_SUCCESS == ret)
  {
    ret = update_ups_addr_();
  }
  if (OB_SUCCESS == ret)
  {
    ms_addr_.init(rs_addr_, cli_.get_rpc());
  }
  if (OB_SUCCESS == ret)
  {
    ret = timer_.init();
  }
  if (OB_SUCCESS == ret)
  {
    ret = timer_.schedule(ms_addr_, MS_REFRESH_INTERVAL_US, true);
  }
  return ret;
}

int OBClient::apply(common::ObMutator &mutator)
{
  int ret = OB_SUCCESS;
  while (true)
  {
    cli_.set_server(ups_addr_);
    ret = cli_.ups_apply(mutator, TIMEOUT_US);
    if (OB_NOT_MASTER != ret)
    {
      break;
    }
    usleep(10 * 1000);
    update_ups_addr_();
  }
  return ret;
}

int OBClient::get(common::ObGetParam &get_param, common::ObScanner &scanner)
{
  cli_.set_server(ms_addr_.get_one());
  return cli_.ups_get(get_param, scanner, TIMEOUT_US);
}

int OBClient::scan(common::ObScanParam &scan_param, common::ObScanner &scanner)
{
  cli_.set_server(ms_addr_.get_one());
  return cli_.ups_scan(scan_param, scanner, TIMEOUT_US);
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ClientWrapper::ClientWrapper() : base_cli_(),
                                 mk_cli_(base_cli_),
                                 ob_cli_(base_cli_),
                                 cli_(NULL)
{
}

ClientWrapper::~ClientWrapper()
{
}

int ClientWrapper::init(const char *addr, const int port)
{
  char addr_buf[1024];
  sprintf(addr_buf, "%s", addr);
  if ('@' == addr[strlen(addr) - 1])
  {
    cli_ = &mk_cli_;
    TBSYS_LOG(INFO, "using mk_cli addr=%s", addr);
    addr_buf[strlen(addr_buf) - 1] = '\0';
  }
  else
  {
    cli_ = &ob_cli_;
    TBSYS_LOG(INFO, "using ob_cli addr=%s", addr);
  }
  return cli_->init(addr_buf, port);
}

void ClientWrapper::destroy()
{
  base_cli_.destroy();
}

int ClientWrapper::apply(ObMutator &mutator)
{
  return cli_->apply(mutator);
}

int ClientWrapper::get(ObGetParam &get_param, ObScanner &scanner)
{
  return cli_->get(get_param, scanner);
}

int ClientWrapper::scan(ObScanParam &scan_param, ObScanner &scanner)
{
  return cli_->scan(scan_param, scanner);
}

