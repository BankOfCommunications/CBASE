#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace oceanbase {
namespace rootserver {
  namespace testing
  {

class MockObRootRpcStub : public ObRootRpcStub {
 public:
  MOCK_METHOD4(slave_register,
      int(const common::ObServer& master, const common::ObServer& slave_addr, common::ObFetchParam& fetch_param, const int64_t timeout));
  MOCK_METHOD3(set_obi_role,
      int(const common::ObServer& ups, const common::ObiRole& role, const int64_t timeout_us));
  MOCK_METHOD3(switch_schema,
      int(const common::ObServer& ups, const common::ObSchemaManagerV2& schema_manager, const int64_t timeout_us));
  MOCK_METHOD5(migrate_tablet,
      int(const common::ObServer& src_cs, const common::ObServer& dest_cs, const common::ObNewRange& range, bool keep_src, const int64_t timeout_us));
  MOCK_METHOD4(create_tablet,
      int(const common::ObServer& cs, const common::ObNewRange& range, const int64_t mem_version, const int64_t timeout_us));
  MOCK_METHOD3(delete_tablets,
      int(const common::ObServer& cs, const common::ObTabletReportInfoList &tablets, const int64_t timeout_us));
  MOCK_METHOD3(get_last_frozen_version,
      int(const common::ObServer& ups, const int64_t timeout_us, int64_t &frozen_version));
  MOCK_METHOD3(get_obi_role,
      int(const common::ObServer& master, const int64_t timeout_us, common::ObiRole &obi_role));
  MOCK_METHOD4(revoke_ups_lease,
      int(const common::ObServer &ups, const int64_t lease, const common::ObServer& master, const int64_t timeout_us));
  MOCK_METHOD4(import_tablets,
      int(const common::ObServer& cs, const uint64_t table_id, const int64_t version, const int64_t timeout_us));
  MOCK_METHOD3(heartbeat_to_cs,
      int(const common::ObServer& cs, const int64_t lease_time, const int64_t frozen_mem_version));
  MOCK_METHOD4(heartbeat_to_ms,
      int(const common::ObServer& ms, const int64_t lease_time, const int64_t schema_version, const common::ObiRole &role));
  MOCK_METHOD2(grant_lease_to_ups, int(const common::ObServer &addr, common::ObMsgUpsHeartbeat &msg));
  //MOCK_METHOD5(grant_lease_to_ups,
              // int(const common::ObServer& ups, const common::ObServer& master, const int64_t lease, const common::ObiRole &role, const int64_t config_version));
    MOCK_METHOD3(get_ups_max_log_seq,
                 int(const common::ObServer& ups, uint64_t &max_log_seq, const int64_t timeout_us));
  MOCK_METHOD3(shutdown_cs,
    int(const common::ObServer& cs, bool is_restart, const int64_t timeout_us));
MOCK_METHOD5(get_split_range,
    int(const common::ObServer& ups, const int64_t timeout_us, const uint64_t table_id,
      const int64_t forzen_version, common::ObTabletInfoList &tablets));
};
  } // namespace testing
}  // namespace rootserver
}  // namespace oceanbase
