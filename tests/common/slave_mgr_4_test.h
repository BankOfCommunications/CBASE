
#include "common/ob_role_mgr.h"
#include "common/ob_slave_mgr.h"
#include "common/ob_base_client.h"
#include "common/ob_packet_factory.h"

using namespace oceanbase::common;

class SlaveMgr4Test
{
public:
  SlaveMgr4Test()
  {
    ObServer server;
    base_client_.initialize(server);
    rpc_stub.init(&base_client_.get_client_mgr());
    slave_mgr_.init(&role_mgr_, 1, &rpc_stub, 1000000, 15000000, 12000000);
  }

  ~SlaveMgr4Test()
  {
    base_client_.destroy();
  }

  ObSlaveMgr* get_slave_mgr()
  {
    return &slave_mgr_;
  }

private:
  ObSlaveMgr slave_mgr_;
  ObRoleMgr role_mgr_;

  ObBaseClient base_client_;
  ObCommonRpcStub rpc_stub;
};
