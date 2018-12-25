#ifndef _MIXED_TEST_CLIENT_WRAPPER_
#define _MIXED_TEST_CLIENT_WRAPPER_
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "common/ob_ms_list.h"
#include "mock_client.h"
#include "utils.h"

using namespace oceanbase;

class IClient
{
  public:
    virtual ~IClient() {};
  public:
    virtual int init(const char *addr, const int port) = 0;
    virtual int apply(common::ObMutator &mutator) = 0;
    virtual int get(common::ObGetParam &get_param, common::ObScanner &scanner) = 0;
    virtual int scan(common::ObScanParam &scan_param, common::ObScanner &scanner) = 0;
};

class MKClient : public IClient
{
  public:
    MKClient(MockClient &cli) : cli_(cli) {};
  public:
    int init(const char *addr, const int port);
    int apply(common::ObMutator &mutator);
    int get(common::ObGetParam &get_param, common::ObScanner &scanner);
    int scan(common::ObScanParam &scan_param, common::ObScanner &scanner);
  private:
    MockClient &cli_;
};

class OBClient : public IClient
{
  static const int64_t MS_REFRESH_INTERVAL_US = 3000000L;
  public:
    OBClient(MockClient &cli) : cli_(cli) {};
  public:
    int init(const char *addr, const int port);
    int apply(common::ObMutator &mutator);
    int get(common::ObGetParam &get_param, common::ObScanner &scanner);
    int scan(common::ObScanParam &scan_param, common::ObScanner &scanner);
  private:
    int update_ups_addr_();
  private:
    MockClient &cli_;
    ObServer rs_addr_;
    ObServer ups_addr_;
    MsList ms_addr_;
    ObTimer timer_;
};

class ClientWrapper
{
  public:
    ClientWrapper();
    ~ClientWrapper();
  public:
    int init(const char *addr, const int port);
    void destroy();
    int apply(common::ObMutator &mutator);
    int get(common::ObGetParam &get_param, common::ObScanner &scanner);
    int scan(common::ObScanParam &scan_param, common::ObScanner &scanner);
  private:
    MockClient base_cli_;
    MKClient mk_cli_;
    OBClient ob_cli_;
    IClient *cli_;
};

#endif //_MIXED_TEST_CLIENT_WRAPPER_

