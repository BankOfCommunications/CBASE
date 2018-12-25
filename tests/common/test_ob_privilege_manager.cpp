#include <gtest/gtest.h>
#include "common/ob_privilege_manager.h"
#include "common/ob_privilege.h"
using namespace oceanbase;
using namespace oceanbase::common;

class ObPrivilegeManagerTest: public ::testing::Test
{
  public:
    ObPrivilegeManagerTest();
    void set_manager(const ObPrivilegeManager & manager);
    void SetUp();
    void TearDown();
    int64_t get_newest_version_index();
    const ObPrivilegeManager::PrivilegeWrapper* get_privilege_wrappers();
  private:
    const ObPrivilegeManager *manager_;
};
const ObPrivilegeManager::PrivilegeWrapper* ObPrivilegeManagerTest::get_privilege_wrappers()
{
  return manager_->privilege_wrappers_;
}
void ObPrivilegeManagerTest::set_manager(const ObPrivilegeManager & manager)
{
  manager_ = &manager;
}
ObPrivilegeManagerTest::ObPrivilegeManagerTest()
{
}
int64_t ObPrivilegeManagerTest::get_newest_version_index()
{
  return manager_->newest_version_index_;
}
void ObPrivilegeManagerTest::SetUp()
{
}
void ObPrivilegeManagerTest::TearDown()
{
}
TEST_F(ObPrivilegeManagerTest, basic)
{
  ObPrivilegeManager manager;
  this->set_manager(manager);
  ObPrivilege *p_privilege = (ObPrivilege*)ob_malloc(sizeof(ObPrivilege), ObModIds::TEST);
  ASSERT_EQ(OB_SUCCESS, p_privilege->init());
  ASSERT_EQ(OB_SUCCESS, manager.renew_privilege(*p_privilege));
  ASSERT_EQ(static_cast<int64_t>(0), this->get_newest_version_index());

  const ObPrivilege **pp_privilege = NULL;
  ASSERT_EQ(OB_SUCCESS, manager.get_newest_privilege(pp_privilege));
  ASSERT_EQ(static_cast<uint64_t>(1),(this->get_privilege_wrappers())[this->get_newest_version_index()].ref_count_);

  const ObPrivilege **pp_privilege2 = NULL;
  ASSERT_EQ(OB_SUCCESS, manager.get_newest_privilege(pp_privilege2));
  ASSERT_EQ(static_cast<uint64_t>(2),(this->get_privilege_wrappers())[this->get_newest_version_index()].ref_count_);
  ASSERT_EQ(OB_SUCCESS, manager.release_privilege(pp_privilege));
  ASSERT_EQ(static_cast<uint64_t>(1),(this->get_privilege_wrappers())[this->get_newest_version_index()].ref_count_);
  ASSERT_EQ(OB_SUCCESS, manager.release_privilege(pp_privilege));
  ASSERT_EQ(static_cast<uint64_t>(0),(this->get_privilege_wrappers())[this->get_newest_version_index()].ref_count_);
}
int main(int argc, char **argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
