
#include <gtest/gtest.h>

#include "ob_role_mgr.h"

#include "tbsys.h"
#include "ob_malloc.h"
#include <string>

using namespace oceanbase::common;
using namespace std;

namespace oceanbase
{
  namespace tests
  {
    namespace common
    {
      class TestObRoleMgr: public ::testing::Test
      {
      };

      TEST_F(TestObRoleMgr, test_set)
      {
        ObRoleMgr role;
        ASSERT_EQ(ObRoleMgr::MASTER, role.get_role());
        ASSERT_EQ(ObRoleMgr::INIT, role.get_state());
        role.set_role(ObRoleMgr::SLAVE);
        ASSERT_EQ(ObRoleMgr::SLAVE, role.get_role());
        role.set_state(ObRoleMgr::SWITCHING);
        ASSERT_EQ(ObRoleMgr::SWITCHING, role.get_state());
        role.set_role(ObRoleMgr::MASTER);
        ASSERT_EQ(ObRoleMgr::MASTER, role.get_role());
        role.set_state(ObRoleMgr::ACTIVE);
        ASSERT_EQ(ObRoleMgr::ACTIVE, role.get_state());
      }
    }
  }
}

int main(int argc, char** argv)
{
  TBSYS_LOGGER.setLogLevel("DEBUG");
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
