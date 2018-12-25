#include <gtest/gtest.h>
#include <unistd.h>
#include <string.h>
#include "common/ob_malloc.h"
#include "common/ob_privilege.h"
#include "common/ob_row.h"
#include "common/ob_array.h"
#include "common/ob_privilege.h"
#include "common/ob_privilege_type.h"

using namespace oceanbase;
using namespace oceanbase::common;

TEST(ObPrivilege, test)
{
  ObPrivilege privilege;
  int ret = OB_SUCCESS;
  ret = privilege.init();
  ASSERT_EQ(OB_SUCCESS, ret);
  // add_user_table_privilege_per
  ObRow row;
  ObRowDesc row_desc;
  ObRow row2;
  ObRowDesc row_desc2;
  const uint64_t OB_USERS_TID = 20;
  const uint64_t OB_USERS_USERNAME_CID = 30;
  const uint64_t OB_USERS_USERID_CID = 31;
  const uint64_t OB_USERS_PASSWORD_CID = 32;
  const uint64_t OB_USERS_COMMENT_CID = 34;
  const uint64_t OB_USERS_PRIV_ALL_CID = 35;
  const uint64_t OB_USERS_PRIV_ALTER_CID = 36;
  const uint64_t OB_USERS_PRIV_CREATE_CID = 37;
  const uint64_t OB_USERS_PRIV_CREATE_USER_CID = 38;
  const uint64_t OB_USERS_PRIV_DELETE_CID = 39;
  const uint64_t OB_USERS_PRIV_DROP_CID = 40;
  const uint64_t OB_USERS_PRIV_GRANT_OPTION_CID = 41;
  const uint64_t OB_USERS_PRIV_INSERT_CID = 42;
  const uint64_t OB_USERS_PRIV_UPDATE_CID = 43;
  const uint64_t OB_USERS_PRIV_SELECT_CID = 44;
  const uint64_t OB_USERS_LOCKED_CID = 45;

  const uint64_t OB_TABLE_PRIVILEGES_TID = 40;
  const uint64_t OB_TABLE_PRIVILEGES_USERID_CID = 50;
  const uint64_t OB_TABLE_PRIVILEGES_TABLEID_CID = 51;
  const uint64_t OB_TABLE_PRIVILEGES_PRIV_ALL_CID = 52;
  const uint64_t OB_TABLE_PRIVILEGES_PRIV_ALTER_CID = 53;
  const uint64_t OB_TABLE_PRIVILEGES_PRIV_CREATE_CID = 54;
  const uint64_t OB_TABLE_PRIVILEGES_PRIV_CREATE_USER_CID = 55;
  const uint64_t OB_TABLE_PRIVILEGES_PRIV_DELETE_CID = 56;
  const uint64_t OB_TABLE_PRIVILEGES_PRIV_DROP_CID = 57;
  const uint64_t OB_TABLE_PRIVILEGES_PRIV_GRANT_OPTION_CID = 58;
  const uint64_t OB_TABLE_PRIVILEGES_PRIV_INSERT_CID = 59;
  const uint64_t OB_TABLE_PRIVILEGES_PRIV_UPDATE_CID = 60;
  const uint64_t OB_TABLE_PRIVILEGES_PRIV_SELECT_CID = 61;

  // set row desc, set table __users
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(OB_USERS_TID, OB_USERS_USERNAME_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(OB_USERS_TID, OB_USERS_USERID_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(OB_USERS_TID, OB_USERS_PASSWORD_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(OB_USERS_TID, OB_USERS_COMMENT_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(OB_USERS_TID, OB_USERS_PRIV_ALL_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(OB_USERS_TID, OB_USERS_PRIV_ALTER_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(OB_USERS_TID, OB_USERS_PRIV_CREATE_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(OB_USERS_TID, OB_USERS_PRIV_CREATE_USER_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(OB_USERS_TID, OB_USERS_PRIV_DELETE_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(OB_USERS_TID, OB_USERS_PRIV_DROP_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(OB_USERS_TID, OB_USERS_PRIV_GRANT_OPTION_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(OB_USERS_TID, OB_USERS_PRIV_INSERT_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(OB_USERS_TID, OB_USERS_PRIV_UPDATE_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(OB_USERS_TID, OB_USERS_PRIV_SELECT_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(OB_USERS_TID, OB_USERS_LOCKED_CID));
  //set row desc , set table __table_privileges
  ASSERT_EQ(OB_SUCCESS, row_desc2.add_column_desc(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_USERID_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc2.add_column_desc(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_TABLEID_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc2.add_column_desc(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_PRIV_ALL_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc2.add_column_desc(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_PRIV_ALTER_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc2.add_column_desc(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_PRIV_CREATE_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc2.add_column_desc(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_PRIV_CREATE_USER_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc2.add_column_desc(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_PRIV_DELETE_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc2.add_column_desc(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_PRIV_DROP_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc2.add_column_desc(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_PRIV_GRANT_OPTION_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc2.add_column_desc(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_PRIV_INSERT_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc2.add_column_desc(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_PRIV_UPDATE_CID));
  ASSERT_EQ(OB_SUCCESS, row_desc2.add_column_desc(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_PRIV_SELECT_CID));
  row.set_row_desc(row_desc);
  row2.set_row_desc(row_desc2);
  //=======================================================================================
  // set row data
  ObObj username;
  ObString str;
  str.assign_ptr(const_cast<char*>("zhangsan"), static_cast<int32_t>(strlen("zhangsan")));
  username.set_varchar(str);
  ASSERT_EQ(OB_SUCCESS, row.set_cell(OB_USERS_TID, OB_USERS_USERNAME_CID, username));
  ObObj userid;
  userid.set_int(10);
  ASSERT_EQ(OB_SUCCESS, row.set_cell(OB_USERS_TID, OB_USERS_USERID_CID, userid));

  ObObj password;
  str.assign_ptr(const_cast<char*>("encryptedpass"), static_cast<int32_t>(strlen("encryptedpass")));
  password.set_varchar(str);
  ASSERT_EQ(OB_SUCCESS, row.set_cell(OB_USERS_TID, OB_USERS_PASSWORD_CID, password));

  ObObj comment;
  str.assign_ptr(const_cast<char*>("comment content"), static_cast<int32_t>(strlen("comment content")));
  comment.set_varchar(str);
  ASSERT_EQ(OB_SUCCESS, row.set_cell(OB_USERS_TID, OB_USERS_COMMENT_CID, comment));
  /*  PRIV_ALL  PRIV_ALTER  PRIV_CREATE   PRIV_CREATE_USER    PRIV_DELETE   PRIV_DROP   PRIV_GRANT_OPTION   PRIV_INSERT   PRIV_UPDATE   PRIV_SELECT
   *    1           1            0               0                1           0               0                 1             0             0
   *
   *
   *
   */
  /* set privilege-related cell*/
  ObObj priv_all;
  priv_all.set_int(1);
  ASSERT_EQ(OB_SUCCESS, row.set_cell(OB_USERS_TID, OB_USERS_PRIV_ALL_CID, priv_all));
  ObObj priv_alter;
  priv_alter.set_int(1);
  ASSERT_EQ(OB_SUCCESS, row.set_cell(OB_USERS_TID, OB_USERS_PRIV_ALTER_CID, priv_alter));
  ObObj priv_create;
  priv_create.set_int(0);
  ASSERT_EQ(OB_SUCCESS, row.set_cell(OB_USERS_TID, OB_USERS_PRIV_CREATE_CID, priv_create));
  ObObj priv_create_user;
  priv_create_user.set_int(0);
  ASSERT_EQ(OB_SUCCESS, row.set_cell(OB_USERS_TID, OB_USERS_PRIV_CREATE_USER_CID, priv_create_user));
  ObObj priv_delete;
  priv_delete.set_int(1);
  ASSERT_EQ(OB_SUCCESS, row.set_cell(OB_USERS_TID, OB_USERS_PRIV_DELETE_CID, priv_delete));
  ObObj priv_drop;
  priv_drop.set_int(0);
  ASSERT_EQ(OB_SUCCESS, row.set_cell(OB_USERS_TID, OB_USERS_PRIV_DROP_CID, priv_drop));
  ObObj priv_grant_option;
  priv_grant_option.set_int(0);
  ASSERT_EQ(OB_SUCCESS, row.set_cell(OB_USERS_TID, OB_USERS_PRIV_GRANT_OPTION_CID, priv_grant_option));
  ObObj priv_insert;
  priv_insert.set_int(1);
  ASSERT_EQ(OB_SUCCESS, row.set_cell(OB_USERS_TID, OB_USERS_PRIV_INSERT_CID, priv_insert));
  ObObj priv_update;
  priv_update.set_int(0);
  ASSERT_EQ(OB_SUCCESS, row.set_cell(OB_USERS_TID, OB_USERS_PRIV_UPDATE_CID, priv_update));
  ObObj priv_select;
  priv_select.set_int(0);
  ASSERT_EQ(OB_SUCCESS, row.set_cell(OB_USERS_TID, OB_USERS_PRIV_SELECT_CID, priv_select));
  /* finish set privilege-related cell */
  ObObj locked;
  locked.set_int(0);//locked
  ASSERT_EQ(OB_SUCCESS, row.set_cell(OB_USERS_TID, OB_USERS_LOCKED_CID, locked));
  //=====================================================================================
  ObObj table_user_id;
  table_user_id.set_int(10);//user id
  ASSERT_EQ(OB_SUCCESS, row2.set_cell(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_USERID_CID, table_user_id));
  ObObj table_table_id;
  table_table_id.set_int(100);//table id
  ASSERT_EQ(OB_SUCCESS, row2.set_cell(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_TABLEID_CID, table_table_id));
  //start set privilege-related cell for table __table_privileges
  // user:       1100100100
  // user_table: 0000010100
  ObObj table_priv_all;
  table_priv_all.set_int(0);
  ASSERT_EQ(OB_SUCCESS, row2.set_cell(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_PRIV_ALL_CID, table_priv_all));
  ObObj table_priv_alter;
  table_priv_alter.set_int(0);
  ASSERT_EQ(OB_SUCCESS, row2.set_cell(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_PRIV_ALTER_CID, table_priv_alter));
  ObObj table_priv_create;
  table_priv_create.set_int(0);
  ASSERT_EQ(OB_SUCCESS, row2.set_cell(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_PRIV_CREATE_CID, table_priv_create));
  ObObj table_priv_create_user;
  table_priv_create_user.set_int(0);
  ASSERT_EQ(OB_SUCCESS, row2.set_cell(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_PRIV_CREATE_USER_CID, table_priv_create_user));
  ObObj table_priv_delete;
  table_priv_delete.set_int(0);
  ASSERT_EQ(OB_SUCCESS, row2.set_cell(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_PRIV_DELETE_CID, table_priv_delete));
  ObObj table_priv_drop;
  table_priv_drop.set_int(1);
  ASSERT_EQ(OB_SUCCESS, row2.set_cell(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_PRIV_DROP_CID, table_priv_drop));
  ObObj table_priv_grant_option;
  table_priv_grant_option.set_int(0);
  ASSERT_EQ(OB_SUCCESS, row2.set_cell(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_PRIV_GRANT_OPTION_CID, table_priv_grant_option));
  ObObj table_priv_insert;
  table_priv_insert.set_int(1);
  ASSERT_EQ(OB_SUCCESS, row2.set_cell(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_PRIV_INSERT_CID, table_priv_insert));
  ObObj table_priv_update;
  table_priv_update.set_int(0);
  ASSERT_EQ(OB_SUCCESS, row2.set_cell(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_PRIV_UPDATE_CID, table_priv_update));
  ObObj table_priv_select;
  table_priv_select.set_int(0);
  ASSERT_EQ(OB_SUCCESS, row2.set_cell(OB_TABLE_PRIVILEGES_TID, OB_TABLE_PRIVILEGES_PRIV_SELECT_CID, table_priv_select));
  /* finished set privilege-related cell for table __table_privileges */
  //============================================================================================================
  //start testing
  ASSERT_EQ(OB_SUCCESS, privilege.add_users_entry(row));
  ASSERT_EQ(OB_SUCCESS, privilege.add_table_privileges_entry(row2));

  ObString user1;
  ObString pass1;
  user1.assign_ptr(const_cast<char*>("zhangsan"), static_cast<int32_t>(strlen("zhangsan")));
  pass1.assign_ptr(const_cast<char*>("encryptedpass"), static_cast<int32_t>(strlen("encryptedpass")));
  ASSERT_EQ(OB_SUCCESS, privilege.user_is_valid(user1, pass1));

  ObString user2;
  ObString pass2;
  user2.assign_ptr(const_cast<char*>("lisi"), static_cast<int32_t>(strlen("lisi")));
  pass2.assign_ptr(const_cast<char*>("lisipass"), static_cast<int32_t>(strlen("lisipass")));
  ASSERT_EQ(OB_ERR_USER_NOT_EXIST, privilege.user_is_valid(user2, pass2));

  ObString user3;
  ObString pass3;
  user3.assign_ptr(const_cast<char*>("zhangsan"), static_cast<int32_t>(strlen("zhangsan")));
  pass3.assign_ptr(const_cast<char*>("xxxx"), static_cast<int32_t>(strlen("xxxx")));
  ASSERT_EQ(OB_ERR_WRONG_PASSWORD, privilege.user_is_valid(user3, pass3));
  //=========================================================================================================
  // OB_PRIV_ALL从第一个bit开始
  // user:       1100100100
  // user_table: 0000010100
  ObPrivilege::TablePrivilege table_privilege;
  // on table_id=100, user zhangsan have:
  // user:       1100100100
  // user_table: 0000010100
  //
  // we test:    0111010000 , result should be true
  table_privilege.table_id_ = 100;
  EXPECT_TRUE(table_privilege.privileges_.add_member(OB_PRIV_ALTER));
  EXPECT_TRUE(table_privilege.privileges_.add_member(OB_PRIV_CREATE));
  EXPECT_TRUE(table_privilege.privileges_.add_member(OB_PRIV_CREATE_USER));
  EXPECT_TRUE(table_privilege.privileges_.add_member(OB_PRIV_DROP));
  ObArray<ObPrivilege::TablePrivilege> arr;
  ASSERT_EQ(OB_SUCCESS, arr.push_back(table_privilege));
  ASSERT_EQ(OB_SUCCESS, privilege.has_needed_privileges(user1, arr));
  arr.pop_back();

  // we test:    1100001100 result should be false
  /*  PRIV_ALL  PRIV_ALTER  PRIV_CREATE   PRIV_CREATE_USER    PRIV_DELETE   PRIV_DROP   PRIV_GRANT_OPTION   PRIV_INSERT   PRIV_UPDATE   PRIV_SELECT*/
  EXPECT_TRUE(table_privilege.privileges_.add_member(OB_PRIV_ALL));
  EXPECT_TRUE(table_privilege.privileges_.add_member(OB_PRIV_ALTER));
  EXPECT_TRUE(table_privilege.privileges_.add_member(OB_PRIV_GRANT_OPTION));
  EXPECT_TRUE(table_privilege.privileges_.add_member(OB_PRIV_INSERT));
  ASSERT_EQ(OB_SUCCESS, arr.push_back(table_privilege));
  ASSERT_EQ(OB_ERR_NO_PRIVILEGE, privilege.has_needed_privileges(user1, arr));
  arr.pop_back();

}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
