/* (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  version 2 as published by the Free Software Foundation.
 *
 * Version: 0.1
 *
 * Authors:
 *    Wu Di <lide.wd@taobao.com>
 */
#include <string.h>
#include <unistd.h>
#include "ob_privilege.h"
#include "ob_object.h"
#include "ob_define.h"
#include "ob_bit_set.h"
#include "ob_privilege_type.h"
#include "common/ob_encrypted_helper.h"

using namespace oceanbase;
using namespace oceanbase::common;

ObPrivilege::TablePrivilege::TablePrivilege()
  :table_id_(OB_INVALID_ID),/*add dolphin [database manager]@20150607*/db_id_(OB_INVALID_ID),privileges_()
{
}
ObPrivilege::User::User()
  :user_name_(), user_id_(OB_INVALID_ID),password_(),
  comment_(),privileges_(),locked_(true)
{
}
ObPrivilege::UserPrivilege::UserPrivilege()
  :user_id_(OB_INVALID_ID), table_privilege_()
{
}
ObPrivilege::UserIdTableId::UserIdTableId()
  :user_id_(OB_INVALID_ID), table_id_(OB_INVALID_ID)
{

}
//add dolphin [database manager]@20150607
ObPrivilege::ObDatabase::ObDatabase():dbname_(),dbid_(OB_INVALID_ID),stat_(0) { }
ObPrivilege::TableDatabase::TableDatabase():dbname_(),table_name_() { }
ObPrivilege::DatabasePrivilege::DatabasePrivilege():dbid_(OB_INVALID_ID),privileges_() { }
ObPrivilege::UserIdDatabaseId::UserIdDatabaseId():userid_(OB_INVALID_ID),dbid_(OB_INVALID_ID) { }
int64_t ObPrivilege::UserIdDatabaseId::hash() const
{
  hash::hash_func<uint64_t > h;
  return h(userid_) + h(dbid_);
}

bool  ObPrivilege::UserIdDatabaseId::operator==(const UserIdDatabaseId & other) const
{
  if(this == &other) return true;
  return (userid_ == other.userid_) && (dbid_ == other.dbid_);
}
//add:e

int64_t ObPrivilege::UserIdTableId::hash() const
{
  hash::hash_func<uint64_t> h;
  return h(user_id_) + h(table_id_);
}
bool ObPrivilege::UserIdTableId::operator==(const UserIdTableId & other) const
{
  return (user_id_ == other.user_id_) && (table_id_ == other.table_id_);
}
ObPrivilege::ObPrivilege() :version_(0)
{
}
ObPrivilege::~ObPrivilege()
{
  username_map_.destroy();
  user_table_map_.destroy();
  //add dolphin [database manager]@20150607
  user_database_pri_map_.destroy();
  name_database_map_.destroy();
  table_database_map_.destroy();
  //add:e
}
void ObPrivilege::set_version(const int64_t version)
{
  version_ = version;
}
int64_t ObPrivilege::get_version() const
{
  return version_;
}
int ObPrivilege::init()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = username_map_.create(USERNAME_MAP_BUCKET_NUM)))
  {
    TBSYS_LOG(WARN, "init username hash map failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = user_table_map_.create(USERTABLE_MAP_BUCKET_NUM)))
  {
    TBSYS_LOG(WARN, "init user table hash map failed, ret=%d", ret);
  }
  //add dolphin [database manager]@20150607
  else if (OB_SUCCESS != (ret = user_database_pri_map_.create(USERDB_MAP_BUCKET_NUM)))
  {
    TBSYS_LOG(WARN, "init  user database privilege hash map failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = name_database_map_.create(DBNAME_MAP_BUCKET_NUM)))
  {
    TBSYS_LOG(WARN, "init name database hash map failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = table_database_map_.create(TABLE_DATABASE_BUCKET_NUM)))
  {
    TBSYS_LOG(WARN, "init table database hash map failed, ret=%d", ret);
  }
  //add:e
  return ret;
}
int ObPrivilege::user_is_valid(const ObString & username, const ObString & password) const
{
  int ret = OB_SUCCESS;
  if (username.length() <= 0)
  {
    ret = OB_ERR_USER_EMPTY;
    TBSYS_LOG(WARN, "usrname is empty string, ret=%d", ret);
  }
  else
  {
    User user;
    ret = username_map_.get(username, user);
    if (ret == -1 || hash::HASH_NOT_EXIST == ret)
    {
      TBSYS_LOG(WARN, "username:%.*s not exist, ret=%d", username.length(), username.ptr(), ret);
      ret = OB_ERR_USER_NOT_EXIST;
    }
    else
    {
      ObString pwd;
      char converted_password[SCRAMBLE_LENGTH * 2 + 1];
      pwd.assign_ptr(converted_password, SCRAMBLE_LENGTH * 2 + 1);
      ObEncryptedHelper::encrypt_from_scrambled(pwd, password);
      pwd.assign_ptr(converted_password, SCRAMBLE_LENGTH * 2);
      if (user.password_ == pwd)
      {
        if (user.locked_ == true)
        {
          ret = OB_ERR_USER_IS_LOCKED;
        }
        else
        {
          ret = OB_SUCCESS;
        }
      }
      else
      {
        TBSYS_LOG(WARN, "system password=%.*s, provided password=%.*s", user.password_.length(), user.password_.ptr(), pwd.length(), pwd.ptr());
        ret = OB_ERR_WRONG_PASSWORD;
      }
    }
  }
  return ret;
}

//add wenghaixing[database manage]20150610
int ObPrivilege::user_db_is_valid(const ObString &username, const ObString &db_name)const
{
  int ret = OB_SUCCESS;
  ObString db_check;
  ObString default_db = ObString::make_string(OB_DEFAULT_DB_NAME);
  if(db_name.length() <= 0)
  {
    db_check = default_db;
    ret = OB_SUCCESS;
    TBSYS_LOG(INFO, "this user select no database...use default db");
  }
  else
  {
    db_check = db_name;
  }
  if(true)
  {
   //first check  db privilege if user can connect database
   User user;
   ObDatabase db;
   UserIdDatabaseId user_db;
   DatabasePrivilege db_priv;
   ret = username_map_.get(username, user);
   if(-1 == ret || hash::HASH_NOT_EXIST == ret)
   {
     TBSYS_LOG(WARN, "username:%.*s not exist, ret = %d", username.length(),username.ptr(),ret);
     ret = OB_ERR_USER_NOT_EXIST;
   }
   else
   {
     ret = name_database_map_.get(db_check,db);
     if(-1 == ret || hash::HASH_NOT_EXIST == ret)
     {
       TBSYS_LOG(WARN, "databasename:%.*s not exist, ret = %d", db_check.length(),db_check.ptr(),ret);
       ret = OB_ERR_DATABASE_NOT_EXSIT;
     }
     else if(hash::HASH_EXIST == ret)
     {
       ret = OB_SUCCESS;
     }
   }
   if(OB_SUCCESS == ret)
   {
     //add liumz, [bugfix: allow admin login anyway]20160516:b
     if(username == OB_ADMIN_USER_NAME)
     {
     }
     //add:e
     else if(OB_SUCCESS == (ret = user_has_global_priv(user)))
     {
     }
     else
     {
       user_db.userid_ = user.user_id_;
       user_db.dbid_ = db.dbid_;
       ret = user_database_pri_map_.get(user_db, db_priv);
       if(hash::HASH_EXIST == ret)
       {
         if(db_priv.privileges_.is_empty())
         {
           ret = OB_ERR_NO_ACCESS_TO_DATABASE;
         }
         else
         {
           ret = OB_SUCCESS;
         }
       }
       if(OB_ERR_NO_ACCESS_TO_DATABASE == ret || -1 == ret || hash::HASH_NOT_EXIST == ret)
       {
         //second check if table privilege can admit user connect database
         bool can_connect = false;
         UserPrivMap::const_iterator user_prvi_iter = user_table_map_.begin();
         UserPrivilege user_pirv;
         UserIdTableId user_table;
         TableDatabase database;
         for(; user_prvi_iter != user_table_map_.end(); ++ user_prvi_iter)
         {
           user_table = user_prvi_iter->first;
           user_pirv = user_prvi_iter ->second;
           if(user_table.user_id_ == user.user_id_)
           {
             ret = table_database_map_.get((int64_t)user_table.table_id_,database);
             if(-1 == ret || hash::HASH_NOT_EXIST == ret)
             {
               TBSYS_LOG(WARN,"can not find table[%ld] is belong to which database...",user_table.table_id_);
               ret = OB_ERROR;
             }
             else if(hash::HASH_EXIST == ret )
             {
               if(database.dbname_ == db_check)
               {
                 if(user_pirv.table_privilege_.privileges_.is_empty())
                 {
                   continue;
                 }
                 else
                 {
                   can_connect = true;
                   break;
                 }
               }
             }
           }
         }
         if(can_connect)
         {
           ret = OB_SUCCESS;
         }
         else
         {
           ret = OB_ERR_NO_ACCESS_TO_DATABASE;
         }
       }
     }
   }
  }
  return ret;
}

int ObPrivilege::user_has_global_priv(const User &user) const
{
  int ret = OB_ERR_NO_ACCESS_TO_DATABASE;
  const ObBitSet<> privilege = user.privileges_;
  if(privilege.has_member(OB_PRIV_ALL))
  {
    ret = OB_SUCCESS;
  }
  else if(privilege.has_member(OB_PRIV_ALTER)
          ||privilege.has_member(OB_PRIV_CREATE)
          ||privilege.has_member(OB_PRIV_CREATE_USER)
          ||privilege.has_member(OB_PRIV_DELETE)
          ||privilege.has_member(OB_PRIV_DROP)
          ||privilege.has_member(OB_PRIV_INSERT)
          ||privilege.has_member(OB_PRIV_UPDATE)
          ||privilege.has_member(OB_PRIV_SELECT)
          ||privilege.has_member(OB_PRIV_REPLACE)
          )
  {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObPrivilege::is_user_exist(const ObString &user_name, User &user) const
{
  int ret = OB_SUCCESS;
  //User user;
  ret = username_map_.get(user_name, user);
  if(-1 == ret || hash::HASH_NOT_EXIST == ret)
  {
    TBSYS_LOG(WARN, "user:%.*s not exist", user_name.length(), user_name.ptr());
    ret = OB_ERR_USER_NOT_EXIST;
  }
  else if(hash::HASH_EXIST == ret)
  {
   // user_id = user.user_id_;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObPrivilege::is_db_exist(const ObString &db_name, uint64_t &db_id) const
{
  int ret = OB_SUCCESS;
  ObDatabase db;
  ret = name_database_map_.get(db_name, db);
  if(-1 == ret || hash::HASH_NOT_EXIST == ret)
  {
    TBSYS_LOG(WARN, "database:%.*s not exist", db_name.length(), db_name.ptr());
    ret = OB_ERR_DATABASE_NOT_EXSIT;
  }
  else if(hash::HASH_EXIST == ret)
  {
    ret = OB_SUCCESS;
    db_id = db.dbid_;
  }
/*
  if(OB_SUCCESS == ret)
  {
    TableDatabase iter_db;
    TableDbMap::const_iterator iter = table_database_map_.begin();
    for(;iter != table_database_map_.end(); ++iter)
    {
      iter_db = iter->second;
      if(db.dbname_ == iter_db.dbname_)
      {
        ret = OB_ERR_STILL_HAS_TABLE_IN_DATABASE;
        break;
      }
    }
  }
*/
  return ret;
}
//add e
bool ObPrivilege::compare_privilege(const ObBitSet<> & privilege1, const ObBitSet<> & privilege2) const
{
  bool ret = false;
  if (privilege1.has_member(OB_PRIV_ALL))
  {
    if (
        privilege1.compare_bit(privilege2, OB_PRIV_GRANT_OPTION) <= 0
        &&
        (
         privilege2.has_member(OB_PRIV_ALL)
         ||
         (  privilege2.has_member(OB_PRIV_ALTER)
            && privilege2.has_member(OB_PRIV_CREATE)
            && privilege2.has_member(OB_PRIV_CREATE_USER)
            && privilege2.has_member(OB_PRIV_DELETE)
            && privilege2.has_member(OB_PRIV_DROP)
            && privilege2.has_member(OB_PRIV_INSERT)
            && privilege2.has_member(OB_PRIV_UPDATE)
            && privilege2.has_member(OB_PRIV_SELECT)
            && privilege2.has_member(OB_PRIV_REPLACE)
         )
        )
       )
    {
      ret = true;
    }
    else
    {
      ret = false;
    }
  }
  else
  {
    if (
        privilege1.compare_bit(privilege2, OB_PRIV_GRANT_OPTION) <= 0
        &&
        (
         privilege2.has_member(OB_PRIV_ALL)
         ||
         (
          privilege1.compare_bit(privilege2, OB_PRIV_ALTER) <= 0
          && privilege1.compare_bit(privilege2, OB_PRIV_CREATE) <= 0
          && privilege1.compare_bit(privilege2, OB_PRIV_CREATE_USER) <= 0
          && privilege1.compare_bit(privilege2, OB_PRIV_DELETE) <= 0
          && privilege1.compare_bit(privilege2, OB_PRIV_DROP) <= 0
          && privilege1.compare_bit(privilege2, OB_PRIV_INSERT) <= 0
          && privilege1.compare_bit(privilege2, OB_PRIV_UPDATE) <=0
          && privilege1.compare_bit(privilege2, OB_PRIV_SELECT) <= 0
          && privilege1.compare_bit(privilege2, OB_PRIV_REPLACE) <= 0
         )
        )
       )
    {
      ret = true;
    }
    else
    {
      ret = false;
    }
  }
  return ret;
}
int ObPrivilege::has_needed_privileges(const ObString &username, const ObArray<TablePrivilege> & table_privileges) const
{
  int ret = OB_SUCCESS;
  uint64_t user_id = OB_INVALID_ID;
  User user;
  if (username.length() <= 0)
  {
    ret = OB_ERR_USER_EMPTY;
    TBSYS_LOG(WARN, "username is empty string, ret=%d", ret);
  }
  else
  {
    ret = username_map_.get(username, user);
    if ( -1 == ret || hash::HASH_NOT_EXIST == ret)
    {
      TBSYS_LOG(WARN, "username:%.*s not exist, ret=%d", username.length(), username.ptr(), ret);
      ret = OB_ERR_USER_NOT_EXIST;
    }
    else
    {
      ret = OB_SUCCESS;
      user_id = user.user_id_;
    }
  }
  // username found
  if (user_id != OB_INVALID_ID)
  {
    for (int i = 0 ;i < table_privileges.count(); ++i)
    {
      const TablePrivilege & table_privilege = table_privileges.at(i);
      /*
       *  case 1: table_id == OB_NOT_EXIST_TABLE_TID
       *    grant priv_type on * to user
       *    revoke priv_type on * from user
       *  case 2: table_id == OB_INVALID_ID
       *    这种用法只支持revoke这两种权限
       *    revoke all privileges, grant option from user
       *  case 3: normal case
       */
      if (table_privilege.table_id_ == OB_NOT_EXIST_TABLE_TID
          || table_privilege.table_id_ == OB_INVALID_ID)
      {
        if (compare_privilege(table_privilege.privileges_, user.privileges_))
        {
          continue;
        }
        else
        {
          ret = OB_ERR_NO_PRIVILEGE;
          TBSYS_LOG(WARN, "username %.*s don't have global privilege, user_id=%ld, ret=%d",
                username.length(), username.ptr(), user_id, ret);
          break;
        }
      }
      else
      {
        //全局权限中找不到的时候，去局部权限中找
        // 首先和__users的全局权限进行比较
        // 当grant权限的时候，使用grant priv_type on *的情况时，检查权限使用具体的表名时，需要先走这个分支
        if (compare_privilege(table_privilege.privileges_, user.privileges_))
        {
          continue;
        }
        UserIdTableId user_id_table_id;
        user_id_table_id.user_id_ = user_id;
        user_id_table_id.table_id_ = table_privilege.table_id_;
        UserPrivilege user_privilege;
        ret = user_table_map_.get(user_id_table_id, user_privilege);
        if (-1 == ret || hash::HASH_NOT_EXIST == ret)
        {
          TBSYS_LOG(WARN, "username %.*s don't have any privilege on table id=%ld, user_id = %ld, ret=%d",
              username.length(), username.ptr(), table_privilege.table_id_, user_id, ret);
          ret = OB_ERR_NO_PRIVILEGE;
          break;
        }
        else
        {
          ret = OB_SUCCESS;
          const ObBitSet<> & sys_pri = user_privilege.table_privilege_.privileges_;
          const ObBitSet<> & provided = table_privilege.privileges_;
          if (compare_privilege(provided, sys_pri))
          {
            continue;
          }
          else
          {
            ret = OB_ERR_NO_PRIVILEGE;
            TBSYS_LOG(WARN, "username %.*s don't have enough privilege on table id =%ld, user_id=%ld, ret=%d",
                username.length(), username.ptr(), table_privilege.table_id_, user_id, ret);
            break;
          }
        }
      }
    }
  }
  return ret;
}
//add liumz, [multi_database.priv_management.check_privilege]:20150610:b
int ObPrivilege::has_needed_db_privileges(const User &user, const TablePrivilege &needed_privilege, ObBitSet<> &fused_privilege) const
{
  int ret = OB_SUCCESS;
  ObDatabase db;
  UserIdDatabaseId user_id_db_id;
  DatabasePrivilege user_db_priv;
  ret = name_database_map_.get(needed_privilege.db_name_, db);
  if (-1 == ret || hash::HASH_NOT_EXIST == ret)
  {
    ret = OB_ERR_DATABASE_NOT_EXSIT;
    TBSYS_LOG(WARN, "database[%.*s] not exist.", needed_privilege.db_name_.length(), needed_privilege.db_name_.ptr());
  }
  else
  {
    ret = OB_SUCCESS;
    user_id_db_id.userid_ = user.user_id_;
    user_id_db_id.dbid_ = db.dbid_;
    ret = user_database_pri_map_.get(user_id_db_id, user_db_priv);
    if (-1 == ret || hash::HASH_NOT_EXIST == ret)
    {
      TBSYS_LOG(WARN, "username %.*s don't have any privilege on database %.*s, ret=%d",
          user.user_name_.length(), user.user_name_.ptr(), needed_privilege.db_name_.length(), needed_privilege.db_name_.ptr(), ret);
      ret = OB_ERR_NO_PRIVILEGE;
    }
    else
    {
      ret = OB_SUCCESS;
      ObBitSet<> db_priv = user_db_priv.privileges_;
      //fuse upper level privilege with db level privilege
      db_priv.add_members(user.privileges_);
      fused_privilege.add_members(db_priv);
      if (!compare_privilege(needed_privilege.privileges_, db_priv))
      {
        TBSYS_LOG(WARN, "username %.*s don't have enough privilege on database %.*s, ret=%d",
            user.user_name_.length(), user.user_name_.ptr(), needed_privilege.db_name_.length(), needed_privilege.db_name_.ptr(), ret);
        ret = OB_ERR_NO_PRIVILEGE;
      }
    }
  }
  return ret;
}
int ObPrivilege::has_needed_privileges_v2(const ObString &username, const ObArray<TablePrivilege> & table_privileges, bool is_global_priv) const
{
  int ret = OB_SUCCESS;
  uint64_t user_id = OB_INVALID_ID;
  User user;
  if (username.length() <= 0)
  {
    ret = OB_ERR_USER_EMPTY;
    TBSYS_LOG(WARN, "username is empty string, ret=%d", ret);
  }
  else
  {
    ret = username_map_.get(username, user);
    if ( -1 == ret || hash::HASH_NOT_EXIST == ret)
    {
      TBSYS_LOG(WARN, "username:%.*s not exist, ret=%d", username.length(), username.ptr(), ret);
      ret = OB_ERR_USER_NOT_EXIST;
    }
    else
    {
      ret = OB_SUCCESS;
      user_id = user.user_id_;
    }
  }
  // username found
  if (user_id != OB_INVALID_ID)
  {
    for (int i = 0 ;i < table_privileges.count(); ++i)
    {
      ObBitSet<> fused_privilege;
      const TablePrivilege & table_privilege = table_privileges.at(i);
      /*
       *  case 1: table_id == OB_NOT_EXIST_TABLE_TID && is_global_priv
       *    grant priv_type on * to user
       *    revoke priv_type on * from user
       *  case 2: table_id == OB_NOT_EXIST_TABLE_TID && !is_global_priv
       *    grant priv_type on db.* to user
       *    revoke priv_type on db.* from user
       *  case 3: table_id == OB_INVALID_ID
       *    这种用法只支持revoke这两种权限
       *    revoke all privileges, grant option from user
       *  case 4: normal case
       */
      if (table_privilege.table_id_ == OB_NOT_EXIST_TABLE_TID
          || table_privilege.table_id_ == OB_INVALID_ID)
      {
        if (compare_privilege(table_privilege.privileges_, user.privileges_))
        {
          continue;
        }
        //add liumz, [multi_database.priv_management.check_privilege]:20150610:b
        else if (table_privilege.table_id_ == OB_NOT_EXIST_TABLE_TID && !is_global_priv)
        {
          if (OB_SUCCESS != (ret = has_needed_db_privileges(user, table_privilege, fused_privilege)))
          {
            //we need db level priv, but we don't own enough.
            break;
          }
        }
        //add:e
        else
        {
          ret = OB_ERR_NO_PRIVILEGE;
          TBSYS_LOG(WARN, "username %.*s don't have global privilege, user_id=%ld, ret=%d",
                username.length(), username.ptr(), user_id, ret);
          break;
        }
      }
      else
      {
        // 首先和__users的全局权限进行比较
        if (compare_privilege(table_privilege.privileges_, user.privileges_))
        {
          continue;
        }
        //add liumz, [multi_database.priv_management.check_privilege]:20150610:b
        else if (!IS_SYS_TABLE_ID(table_privilege.table_id_) && OB_SUCCESS == (ret = has_needed_db_privileges(user, table_privilege, fused_privilege)))
        {
          //continue to check table level privs
          continue;
        }
        //add:e
        UserIdTableId user_id_table_id;
        user_id_table_id.user_id_ = user_id;
        user_id_table_id.table_id_ = table_privilege.table_id_;
        UserPrivilege user_privilege;
        ret = user_table_map_.get(user_id_table_id, user_privilege);
        if (-1 == ret || hash::HASH_NOT_EXIST == ret)
        {
          TBSYS_LOG(WARN, "username %.*s don't have any privilege on table id=%ld, user_id = %ld, ret=%d",
              username.length(), username.ptr(), table_privilege.table_id_, user_id, ret);
          ret = OB_ERR_NO_PRIVILEGE;
          break;
        }
        else
        {
          ret = OB_SUCCESS;
          //add liumz, [multi_database.priv_management.check_privilege]:20150610:b
          //fuse upper level privilege with table level privilege
          ObBitSet<> sys_pri = user_privilege.table_privilege_.privileges_;
          sys_pri.add_members(fused_privilege);
          //add:e
          //const ObBitSet<> & sys_pri = user_privilege.table_privilege_.privileges_;
          const ObBitSet<> & provided = table_privilege.privileges_;
          if (compare_privilege(provided, sys_pri))
          {
            continue;
          }
          else
          {
            ret = OB_ERR_NO_PRIVILEGE;
            TBSYS_LOG(WARN, "username %.*s don't have enough privilege on table id =%ld, user_id=%ld, ret=%d",
                username.length(), username.ptr(), table_privilege.table_id_, user_id, ret);
            break;
          }
        }
      }
    }
  }
  return ret;
}
//add:e
//add dolpin [database manager]@20150607
int ObPrivilege::add_database_name_entry(const ObRow &row)
{
  int ret = OB_SUCCESS;
  const ObObj *pcell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  ObDatabase database;
  ret = row.raw_get_cell(0, pcell, table_id, column_id);
  if (OB_SUCCESS == ret)
  {
    if (pcell->get_type() == ObVarcharType)
    {
      ObString str;
      if (OB_SUCCESS != (ret = pcell->get_varchar(str)))
      {
        TBSYS_LOG(WARN, "failed to get varchar from ObObj, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = string_buf_.write_string(str, &(database.dbname_))))
      {
        TBSYS_LOG(WARN, "failed to  write ObString(dbname) to string_buf_, ret=%d", ret);
      }
    }
      // 目前系统不支持default，处理这种情况
    else if (pcell->get_type() == ObNullType)
    {
      ObString str = ObString::make_string("");
      if (OB_SUCCESS != (ret = string_buf_.write_string(str, &(database.dbname_))))
      {
        TBSYS_LOG(WARN, "failed to  write ObString(dbname) to string_buf_, ret=%d", ret);
      }
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObVarcharType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(dbname) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(1, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t dbid = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(dbid)))
      {
        TBSYS_LOG(WARN, "failed to get_int(dbid) from ObObj, ret=%d", ret);
      }
      else
      {
        database.dbid_ = static_cast<uint64_t>(dbid);
      }
    }
      // 目前系统不支持default，处理这种情况
    else if (pcell->get_type() == ObNullType)
    {
      // 处理异常情况
      database.dbid_ = 0;
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(dbid) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = name_database_map_.set(database.dbname_, database);
    if (hash::HASH_EXIST == ret || hash::HASH_INSERT_SUCC == ret)
    {
      ret = OB_SUCCESS;
    }
    else
    {
      TBSYS_LOG(WARN, "add database failed, dbname:%.*s,ret=%d", database.dbname_.length(), database.dbname_.ptr(), ret);
    }
  }
  return ret;
}

int ObPrivilege::add_table_database_entry(const ObRow &row)
{
  int ret = OB_SUCCESS;
  const ObObj *pcell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  TableDatabase td;
  uint64_t tid;
  ret = row.raw_get_cell(0, pcell, table_id, column_id);
  if (OB_SUCCESS == ret)
  {
    if (pcell->get_type() == ObVarcharType)
    {
      ObString str;
      if (OB_SUCCESS != (ret = pcell->get_varchar(str)))
      {
        TBSYS_LOG(WARN, "failed to get varchar from ObObj, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = string_buf_.write_string(str, &(td.dbname_))))
      {
        TBSYS_LOG(WARN, "failed to  write ObString(dbname) to string_buf_, ret=%d", ret);
      }
    }
      // 目前系统不支持default，处理这种情况
    else if (pcell->get_type() == ObNullType)
    {
      ObString str = ObString::make_string("");
      if (OB_SUCCESS != (ret = string_buf_.write_string(str, &(td.dbname_))))
      {
        TBSYS_LOG(WARN, "failed to  write ObString(dbname) to string_buf_, ret=%d", ret);
      }
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObVarcharType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(dbname) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(1, pcell, table_id, column_id);
    if (pcell->get_type() == ObVarcharType)
    {
      ObString str;
      if (OB_SUCCESS != (ret = pcell->get_varchar(str)))
      {
        TBSYS_LOG(WARN, "failed to get varchar from ObObj, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = string_buf_.write_string(str, &(td.table_name_))))
      {
        TBSYS_LOG(WARN, "failed to  write ObString(tablename) to string_buf_, ret=%d", ret);
      }
    }
      // 目前系统不支持default，处理这种情况
    else if (pcell->get_type() == ObNullType)
    {
      ObString str = ObString::make_string("");
      if (OB_SUCCESS != (ret = string_buf_.write_string(str, &(td.table_name_))))
      {
        TBSYS_LOG(WARN, "failed to  write ObString(tablename) to string_buf_, ret=%d", ret);
      }
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObVarcharType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(tablename) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(2, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t t = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(t)))
      {
        TBSYS_LOG(WARN, "failed to get_int(dbid) from ObObj, ret=%d", ret);
      }
      else
      {
        tid = static_cast<uint64_t>(t);
      }
    }
      // 目前系统不支持default，处理这种情况
    else if (pcell->get_type() == ObNullType)
    {
      // 处理异常情况
      tid = 0;
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(dbid) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = table_database_map_.set(tid, td);
    if (hash::HASH_EXIST == ret || hash::HASH_INSERT_SUCC == ret)
    {
      ret = OB_SUCCESS;
    }
    else
    {
      TBSYS_LOG(WARN, "add table database failed, tablename:%.*s,ret=%d", td.table_name_.length(), td.table_name_.ptr(), ret);
    }
  }
  return ret;
}
int ObPrivilege::add_database_privilege_entry(const ObRow &row)
{
  int ret = OB_SUCCESS;
  const ObObj *pcell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  DatabasePrivilege dbPrivilege;
  UserIdDatabaseId id;
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(0, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t user_id = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(user_id)))
      {
        TBSYS_LOG(WARN, "failed to get_int(user_id) from ObObj, ret=%d", ret);
      }
      else
      {
        id.userid_ = static_cast<uint64_t>(user_id);
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
      // invalid user id
      id.userid_ = 0;
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(user_id) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(1, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t db_id = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(db_id)))
      {
        TBSYS_LOG(WARN, "failed to get_int(db_id) from ObObj, ret=%d", ret);
      }
      else
      {
        id.dbid_ = static_cast<uint64_t>(db_id);
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
      // invalid table id
      id.dbid_ = 0;
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(db_id) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(2, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_all = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_all)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_all) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_all == 1)
        {
          bool result = dbPrivilege.privileges_.add_member(OB_PRIV_ALL);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_ALL to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_all) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(3, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_alter = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_alter)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_alter) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_alter == 1)
        {
          bool result = dbPrivilege.privileges_.add_member(OB_PRIV_ALTER);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_ALTER to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_alter) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(4, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_create = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_create)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_create) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_create == 1)
        {
          bool result = dbPrivilege.privileges_.add_member(OB_PRIV_CREATE);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_CREATE to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_create) from row failed, ret=%d", ret);
  }

  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(5, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_create_user = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_create_user)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_create_user) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_create_user == 1)
        {
          bool result = dbPrivilege.privileges_.add_member(OB_PRIV_CREATE_USER);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_CREATE_USER to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_create_user) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(6, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_delete = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_delete)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_delete) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_delete == 1)
        {
          bool result = dbPrivilege.privileges_.add_member(OB_PRIV_DELETE);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_DELETE to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_delete) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(7, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_drop = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_drop)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_drop) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_drop == 1)
        {
          bool result = dbPrivilege.privileges_.add_member(OB_PRIV_DROP);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_DROP to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_drop) from row failed, ret=%d", ret);
  }


  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(8, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_grant_option = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_grant_option)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_drop) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_grant_option == 1)
        {
          bool result = dbPrivilege.privileges_.add_member(OB_PRIV_GRANT_OPTION);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_GRANT_OPTION to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_grant_option) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(9, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_insert = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_insert)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_insert) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_insert == 1)
        {
          bool result = dbPrivilege.privileges_.add_member(OB_PRIV_INSERT);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_INSERT to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_insert) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(10, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_update = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_update)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_update) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_update == 1)
        {
          bool result = dbPrivilege.privileges_.add_member(OB_PRIV_UPDATE);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_UPDATE to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_update) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(11, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_select = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_select)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_select) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_select == 1)
        {
          bool result = dbPrivilege.privileges_.add_member(OB_PRIV_SELECT);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_SELECT to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_select) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(12, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_replace = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_replace)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_replace) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_replace == 1)
        {
          bool result = dbPrivilege.privileges_.add_member(OB_PRIV_REPLACE);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_REPLACE to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_replace) from row failed, ret=%d", ret);
  }

  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(13, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t locked = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(locked)))
      {
        TBSYS_LOG(WARN, "failed to get_int(locked) from ObObj, ret=%d", ret);
      }
      else
      {
        dbPrivilege.isLocked_ = static_cast<uint8_t>(locked);
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(locked) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = user_database_pri_map_.set(id, dbPrivilege);
    //mod liumz, [multi_database]20151201:b
    if (hash::HASH_EXIST == ret || hash::HASH_INSERT_SUCC == ret)
    //if (hash::HASH_INSERT_SUCC == ret)
    //mod:e
    {
      ret = OB_SUCCESS;
    }
    else
    {
      TBSYS_LOG(WARN, "add user db privilege  failed, user_id:%ld,table_id:%ld,ret=%d", id.userid_, id.dbid_,ret);
    }
  }

  return ret;
}

//add:e

int ObPrivilege::add_table_privileges_entry(const ObRow & row)
{
  int ret = OB_SUCCESS;
  const ObObj *pcell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  UserPrivilege user_privilege;
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(0, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t user_id = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(user_id)))
      {
        TBSYS_LOG(WARN, "failed to get_int(user_id) from ObObj, ret=%d", ret);
      }
      else
      {
        user_privilege.user_id_ = static_cast<uint64_t>(user_id);
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
      // invalid user id
      user_privilege.user_id_ = 0;
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(user_id) from row failed, ret=%d", ret);
  }
  //add liumz, [drop table -> clean table priv]20150902:b
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(1, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t db_id = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(db_id)))
      {
        TBSYS_LOG(WARN, "failed to get_int(db_id) from ObObj, ret=%d", ret);
      }
      else
      {
        user_privilege.table_privilege_.db_id_ = static_cast<uint64_t>(db_id);
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
      // invalid table id
      user_privilege.table_privilege_.db_id_ = 0;
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(db_id) from row failed, ret=%d", ret);
  }
  //add:e
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(2, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t table_id = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(table_id)))
      {
        TBSYS_LOG(WARN, "failed to get_int(table_id) from ObObj, ret=%d", ret);
      }
      else
      {
        user_privilege.table_privilege_.table_id_ = static_cast<uint64_t>(table_id);
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
      // invalid table id
      user_privilege.table_privilege_.table_id_ = 0;
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(table_id) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(3, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_all = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_all)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_all) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_all == 1)
        {
          bool result = user_privilege.table_privilege_.privileges_.add_member(OB_PRIV_ALL);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_ALL to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_all) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(4, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_alter = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_alter)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_alter) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_alter == 1)
        {
          bool result = user_privilege.table_privilege_.privileges_.add_member(OB_PRIV_ALTER);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_ALTER to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_alter) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(5, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_create = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_create)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_create) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_create == 1)
        {
          bool result = user_privilege.table_privilege_.privileges_.add_member(OB_PRIV_CREATE);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_CREATE to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_create) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(6, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_create_user = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_create_user)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_create_user) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_create_user == 1)
        {
          bool result = user_privilege.table_privilege_.privileges_.add_member(OB_PRIV_CREATE_USER);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_CREATE_USER to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_create_user) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(7, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_delete = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_delete)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_delete) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_delete == 1)
        {
          bool result = user_privilege.table_privilege_.privileges_.add_member(OB_PRIV_DELETE);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_DELETE to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_delete) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(8, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_drop = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_drop)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_drop) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_drop == 1)
        {
          bool result = user_privilege.table_privilege_.privileges_.add_member(OB_PRIV_DROP);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_DROP to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_drop) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(9, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_grant_option = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_grant_option)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_grant_option) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_grant_option == 1)
        {
          bool result = user_privilege.table_privilege_.privileges_.add_member(OB_PRIV_GRANT_OPTION);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_GRANT_OPTION to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_grant_option) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(10, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_insert = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_insert)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_insert) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_insert == 1)
        {
          bool result = user_privilege.table_privilege_.privileges_.add_member(OB_PRIV_INSERT);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_INSERT to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_insert) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(11, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_update = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_update)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_update) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_update == 1)
        {
          bool result = user_privilege.table_privilege_.privileges_.add_member(OB_PRIV_UPDATE);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_UPDATE to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_update) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(12, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_select = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_select)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_select) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_select == 1)
        {
          bool result = user_privilege.table_privilege_.privileges_.add_member(OB_PRIV_SELECT);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_SELECT to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_select) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(13, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_replace = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_replace)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_replace) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_replace == 1)
        {
          bool result = user_privilege.table_privilege_.privileges_.add_member(OB_PRIV_REPLACE);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_REPLACE to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_replace) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    UserIdTableId id;
    id.user_id_ = user_privilege.user_id_;
    id.table_id_ = user_privilege.table_privilege_.table_id_;
    ret = user_table_map_.set(id, user_privilege);
    //mod liumz, [multi_database.for_dsadmin_user]20150716:b
    if (hash::HASH_EXIST == ret || hash::HASH_INSERT_SUCC == ret)
    //if (hash::HASH_INSERT_SUCC == ret)
    //mod:e
    {
      ret = OB_SUCCESS;
    }
    else
    {
      TBSYS_LOG(WARN, "add table privilege  failed, user_id:%ld,table_id:%ld,ret=%d", id.user_id_, id.table_id_,ret);
    }
  }
  return ret;

}
int ObPrivilege::add_users_entry(const ObRow & row)
{
  int ret = OB_SUCCESS;
  const ObObj *pcell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  User user;
  ret = row.raw_get_cell(0, pcell, table_id, column_id);
  if (OB_SUCCESS == ret)
  {
    if (pcell->get_type() == ObVarcharType)
    {
      ObString str;
      if (OB_SUCCESS != (ret = pcell->get_varchar(str)))
      {
        TBSYS_LOG(WARN, "failed to get varchar from ObObj, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = string_buf_.write_string(str, &(user.user_name_))))
      {
        TBSYS_LOG(WARN, "failed to  write ObString(username) to string_buf_, ret=%d", ret);
      }
    }
    // 目前系统不支持default，处理这种情况
    else if (pcell->get_type() == ObNullType)
    {
      ObString str = ObString::make_string("");
      if (OB_SUCCESS != (ret = string_buf_.write_string(str, &(user.user_name_))))
      {
        TBSYS_LOG(WARN, "failed to  write ObString(username) to string_buf_, ret=%d", ret);
      }
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObVarcharType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(username) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(1, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t user_id = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(user_id)))
      {
        TBSYS_LOG(WARN, "failed to get_int(user_id) from ObObj, ret=%d", ret);
      }
      else
      {
        user.user_id_ = static_cast<uint64_t>(user_id);
      }
    }
    // 目前系统不支持default，处理这种情况
    else if (pcell->get_type() == ObNullType)
    {
      // 处理异常情况
      user.user_id_ = 0;
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(userid) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(2, pcell, table_id, column_id);
    if (pcell->get_type() == ObVarcharType)
    {
      ObString str;
      if (OB_SUCCESS != (ret = pcell->get_varchar(str)))
      {
        TBSYS_LOG(WARN, "failed to get varchar from ObObj, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = string_buf_.write_string(str, &(user.password_))))
      {
        TBSYS_LOG(WARN, "failed to  write ObString(password) to string_buf_, ret=%d", ret);
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
      ObString str = ObString::make_string("");
      if (OB_SUCCESS != (ret = string_buf_.write_string(str, &(user.password_))))
      {
        TBSYS_LOG(WARN, "failed to  write ObString(password) to string_buf_, ret=%d", ret);
      }
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObVarcharType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(password) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(3, pcell, table_id, column_id);
    if (pcell->get_type() == ObVarcharType)
    {
      ObString str;
      if (OB_SUCCESS != (ret = pcell->get_varchar(str)))
      {
        TBSYS_LOG(WARN, "failed to get varchar from ObObj, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = string_buf_.write_string(str, &(user.comment_))))
      {
        TBSYS_LOG(WARN, "failed to  write ObString(comment) to string_buf_, ret=%d", ret);
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
      ObString str = ObString::make_string("");
      if (OB_SUCCESS != (ret = string_buf_.write_string(str, &(user.comment_))))
      {
        TBSYS_LOG(WARN, "failed to  write ObString(password) to string_buf_, ret=%d", ret);
      }
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObVarcharType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(comment) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(4, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_all = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_all)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_all) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_all == 1)
        {
          bool result = user.privileges_.add_member(OB_PRIV_ALL);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_ALL to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_all) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(5, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_alter = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_alter)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_alter) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_alter == 1)
        {
          bool result = user.privileges_.add_member(OB_PRIV_ALTER);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_ALTER to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_alter) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(6, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_create = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_create)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_create) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_create == 1)
        {
          bool result = user.privileges_.add_member(OB_PRIV_CREATE);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_CREATE to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_create) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(7, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_create_user = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_create_user)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_create_user) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_create_user == 1)
        {
          bool result = user.privileges_.add_member(OB_PRIV_CREATE_USER);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_CREATE_USER to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_create_user) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(8, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_delete = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_delete)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_delete) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_delete == 1)
        {
          bool result = user.privileges_.add_member(OB_PRIV_DELETE);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_DELETE to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_delete) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(9, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_drop = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_drop)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_drop) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_drop == 1)
        {
          bool result = user.privileges_.add_member(OB_PRIV_DROP);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_DROP to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_drop) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(10, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_grant_option = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_grant_option)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_grant_option) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_grant_option == 1)
        {
          bool result = user.privileges_.add_member(OB_PRIV_GRANT_OPTION);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_GRANT_OPTION to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_grant_option) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(11, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_insert = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_insert)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_insert) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_insert == 1)
        {
          bool result = user.privileges_.add_member(OB_PRIV_INSERT);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_INSERT to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_insert) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(12, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_update = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_update)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_update) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_update == 1)
        {
          bool result = user.privileges_.add_member(OB_PRIV_UPDATE);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_UPDATE to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_update) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(13, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_select = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_select)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_select) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_select == 1)
        {
          bool result = user.privileges_.add_member(OB_PRIV_SELECT);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_SELECT to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_select) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(14, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_replace = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_replace)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_replace) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_replace == 1)
        {
          bool result = user.privileges_.add_member(OB_PRIV_REPLACE);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_REPLACE to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_replace) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(15, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t locked = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(locked)))
      {
        TBSYS_LOG(WARN, "failed to get_int(locked) from ObObj, ret=%d", ret);
      }
      else
      {
        user.locked_ = static_cast<uint64_t>(locked);
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(locked) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = username_map_.set(user.user_name_, user);
    if (hash::HASH_EXIST == ret || hash::HASH_INSERT_SUCC == ret)
    {
      ret = OB_SUCCESS;
    }
    else
    {
      TBSYS_LOG(WARN, "add user failed, username:%.*s,ret=%d", user.user_name_.length(), user.user_name_.ptr(), ret);
    }
  }
  return ret;
}

/**
 * @synopsis  privilege_to_string
 *
 * @param privileges 不包括PRIV_ALL
 * @param buf
 * @param buf_len
 * @param pos
 */
void ObPrivilege::privilege_to_string(const ObBitSet<> &privileges, char *buf, const int64_t buf_len, int64_t &pos)
{
  if (privileges.has_member(OB_PRIV_ALTER))
  {
    databuff_printf(buf, buf_len, pos, "ALTER,");
  }
  if (privileges.has_member(OB_PRIV_CREATE))
  {
    databuff_printf(buf, buf_len, pos, "CREATE,");
  }
  if (privileges.has_member(OB_PRIV_CREATE_USER))
  {
    databuff_printf(buf, buf_len, pos, "CREATE USER,");
  }
  if (privileges.has_member(OB_PRIV_DELETE))
  {
    databuff_printf(buf, buf_len, pos, "DELETE,");
  }
  if (privileges.has_member(OB_PRIV_DROP))
  {
    databuff_printf(buf, buf_len, pos, "DROP,");
  }
  if (privileges.has_member(OB_PRIV_GRANT_OPTION))
  {
    databuff_printf(buf, buf_len, pos, "GRANT OPTION,");
  }
  if (privileges.has_member(OB_PRIV_INSERT))
  {
    databuff_printf(buf, buf_len, pos, "INSERT,");
  }
  if (privileges.has_member(OB_PRIV_UPDATE))
  {
    databuff_printf(buf, buf_len, pos, "UPDATE,");
  }
  if (privileges.has_member(OB_PRIV_SELECT))
  {
    databuff_printf(buf, buf_len, pos, "SELECT,");
  }
  if (privileges.has_member(OB_PRIV_REPLACE))
  {
    databuff_printf(buf, buf_len, pos, "REPLACE,");
  }
}

void ObPrivilege::privilege_to_string_v2(const ObBitSet<> &privileges, char *buf, const int64_t buf_len, int64_t &pos)
{
  databuff_printf(buf, buf_len, pos, "GRANT ");
  if (privileges.has_member(OB_PRIV_ALL))
  {
    databuff_printf(buf, buf_len, pos, "ALL PRIVILEGES,");
    if (privileges.has_member(OB_PRIV_GRANT_OPTION))
    {
      databuff_printf(buf, buf_len, pos, "GRANT OPTION,");
    }
  }
  else
  {
    if (privileges.has_member(OB_PRIV_ALTER))
    {
      databuff_printf(buf, buf_len, pos, "ALTER,");
    }
    if (privileges.has_member(OB_PRIV_CREATE))
    {
      databuff_printf(buf, buf_len, pos, "CREATE,");
    }
    if (privileges.has_member(OB_PRIV_CREATE_USER))
    {
      databuff_printf(buf, buf_len, pos, "CREATE USER,");
    }
    if (privileges.has_member(OB_PRIV_DELETE))
    {
      databuff_printf(buf, buf_len, pos, "DELETE,");
    }
    if (privileges.has_member(OB_PRIV_DROP))
    {
      databuff_printf(buf, buf_len, pos, "DROP,");
    }
    if (privileges.has_member(OB_PRIV_GRANT_OPTION))
    {
      databuff_printf(buf, buf_len, pos, "GRANT OPTION,");
    }
    if (privileges.has_member(OB_PRIV_INSERT))
    {
      databuff_printf(buf, buf_len, pos, "INSERT,");
    }
    if (privileges.has_member(OB_PRIV_UPDATE))
    {
      databuff_printf(buf, buf_len, pos, "UPDATE,");
    }
    if (privileges.has_member(OB_PRIV_SELECT))
    {
      databuff_printf(buf, buf_len, pos, "SELECT,");
    }
    if (privileges.has_member(OB_PRIV_REPLACE))
    {
      databuff_printf(buf, buf_len, pos, "REPLACE,");
    }
  }
}

void ObPrivilege::print_info() const
{
  NameUserMap::const_iterator iter = username_map_.begin();
  for (;iter != username_map_.end(); ++iter)
  {
    TBSYS_LOG(INFO, "user[%.*s]", iter->first.length(), iter->first.ptr());
  }
}
//add liumz, [database manager]20150610:b
int ObPrivilege::get_db_name(const uint64_t db_id, ObString &db_name) const
{
  int ret = OB_ERR_DATABASE_NOT_EXSIT;
  NameDbMap::const_iterator iter = name_database_map_.begin();
  for (; iter != name_database_map_.end(); iter++)
  {
    if ((iter->second).dbid_ == db_id)
    {
      ret = OB_SUCCESS;
      db_name = (iter->second).dbname_;
      break;
    }
  }
  return ret;
}
int ObPrivilege::get_user_name(const uint64_t user_id, ObString &user_name) const
{
  int ret = OB_ERR_USER_NOT_EXIST;
  NameUserMap::const_iterator iter = username_map_.begin();
  for (; iter != username_map_.end(); iter++)
  {
    if ((iter->second).user_id_ == user_id)
    {
      ret = OB_SUCCESS;
      user_name = (iter->second).user_name_;
      break;
    }
  }
  return ret;
}
//add:e

//add liumz, [priv management]20150902:b
int ObPrivilege::get_db_id(const uint64_t table_id, uint64_t &db_id) const
{
  int ret = OB_ERR_DATABASE_NOT_EXSIT;
  const TableDatabase *td = table_database_map_.get(table_id);
  if (NULL != td)
  {
    const ObDatabase* db = name_database_map_.get(td->dbname_);
    if (NULL != db)
    {
      ret = OB_SUCCESS;
      db_id = db->dbid_;
    }
  }
  return ret;
}

int ObPrivilege::get_user_db_id(const uint64_t table_id, ObArray<UserIdDatabaseId> &user_db_id) const
{
  int ret = OB_SUCCESS;
  UserIdDatabaseId udi;
  UserPrivMap::const_iterator iter = user_table_map_.begin();
  for (; iter != user_table_map_.end(); iter++)
  {
    if ((iter->first).table_id_ == table_id)
    {
      udi.userid_ = (iter->first).user_id_;
      udi.dbid_   = (iter->second).table_privilege_.db_id_;
      user_db_id.push_back(udi);
    }
  }
  return ret;
}
//add:e

//add wenghaixing [database manage] 20150702
int ObPrivilege::db_result_to_priv_empty(const ObRow &row, bool &empty)
{
  int ret = OB_SUCCESS;
  const ObObj *pcell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  DatabasePrivilege dbPrivilege;
  UserIdDatabaseId id;
  empty = false;
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(0, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t user_id = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(user_id)))
      {
        TBSYS_LOG(WARN, "failed to get_int(user_id) from ObObj, ret=%d", ret);
      }
      else
      {
        id.userid_ = static_cast<uint64_t>(user_id);
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
      // invalid user id
      id.userid_ = 0;
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(user_id) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(1, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t db_id = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(db_id)))
      {
        TBSYS_LOG(WARN, "failed to get_int(db_id) from ObObj, ret=%d", ret);
      }
      else
      {
        id.dbid_ = static_cast<uint64_t>(db_id);
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
      // invalid table id
      id.dbid_ = 0;
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(db_id) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(2, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_all = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_all)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_all) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_all == 1)
        {
          bool result = dbPrivilege.privileges_.add_member(OB_PRIV_ALL);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_ALL to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_all) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(3, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_alter = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_alter)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_alter) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_alter == 1)
        {
          bool result = dbPrivilege.privileges_.add_member(OB_PRIV_ALTER);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_ALTER to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_alter) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(4, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_create = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_create)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_create) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_create == 1)
        {
          bool result = dbPrivilege.privileges_.add_member(OB_PRIV_CREATE);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_CREATE to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_create) from row failed, ret=%d", ret);
  }

  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(5, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_create_user = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_create_user)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_create_user) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_create_user == 1)
        {
          bool result = dbPrivilege.privileges_.add_member(OB_PRIV_CREATE_USER);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_CREATE_USER to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_create_user) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(6, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_delete = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_delete)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_delete) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_delete == 1)
        {
          bool result = dbPrivilege.privileges_.add_member(OB_PRIV_DELETE);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_DELETE to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_delete) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(7, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_drop = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_drop)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_drop) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_drop == 1)
        {
          bool result = dbPrivilege.privileges_.add_member(OB_PRIV_DROP);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_DROP to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_drop) from row failed, ret=%d", ret);
  }


  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(8, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_grant_option = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_grant_option)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_drop) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_grant_option == 1)
        {
          bool result = dbPrivilege.privileges_.add_member(OB_PRIV_GRANT_OPTION);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_GRANT_OPTION to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_grant_option) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(9, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_insert = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_insert)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_insert) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_insert == 1)
        {
          bool result = dbPrivilege.privileges_.add_member(OB_PRIV_INSERT);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_INSERT to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_insert) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(10, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_update = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_update)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_update) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_update == 1)
        {
          bool result = dbPrivilege.privileges_.add_member(OB_PRIV_UPDATE);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_UPDATE to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_update) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(11, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_select = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_select)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_select) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_select == 1)
        {
          bool result = dbPrivilege.privileges_.add_member(OB_PRIV_SELECT);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_SELECT to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_select) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(12, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_replace = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_replace)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_replace) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_replace == 1)
        {
          bool result = dbPrivilege.privileges_.add_member(OB_PRIV_REPLACE);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_REPLACE to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_replace) from row failed, ret=%d", ret);
  }

  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(13, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t locked = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(locked)))
      {
        TBSYS_LOG(WARN, "failed to get_int(locked) from ObObj, ret=%d", ret);
      }
      else
      {
        dbPrivilege.isLocked_ = static_cast<uint8_t>(locked);
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(locked) from row failed, ret=%d", ret);
  }
  if(OB_SUCCESS == ret)
  {
    if(dbPrivilege.privileges_.is_empty())
    {
      empty = true;
    }
  }
  return ret;
}

int ObPrivilege::table_result_to_priv_empty(const ObRow &row, bool &empty)
{
  int ret = OB_SUCCESS;
  const ObObj *pcell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  UserPrivilege user_privilege;
  empty = false;
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(0, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t user_id = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(user_id)))
      {
        TBSYS_LOG(WARN, "failed to get_int(user_id) from ObObj, ret=%d", ret);
      }
      else
      {
        user_privilege.user_id_ = static_cast<uint64_t>(user_id);
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
      // invalid user id
      user_privilege.user_id_ = 0;
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(user_id) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(1, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t table_id = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(table_id)))
      {
        TBSYS_LOG(WARN, "failed to get_int(table_id) from ObObj, ret=%d", ret);
      }
      else
      {
        user_privilege.table_privilege_.table_id_ = static_cast<uint64_t>(table_id);
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
      // invalid table id
      user_privilege.table_privilege_.table_id_ = 0;
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(table_id) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(2, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_all = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_all)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_all) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_all == 1)
        {
          bool result = user_privilege.table_privilege_.privileges_.add_member(OB_PRIV_ALL);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_ALL to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_all) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(3, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_alter = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_alter)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_alter) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_alter == 1)
        {
          bool result = user_privilege.table_privilege_.privileges_.add_member(OB_PRIV_ALTER);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_ALTER to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_alter) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(4, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_create = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_create)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_create) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_create == 1)
        {
          bool result = user_privilege.table_privilege_.privileges_.add_member(OB_PRIV_CREATE);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_CREATE to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_create) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(5, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_create_user = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_create_user)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_create_user) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_create_user == 1)
        {
          bool result = user_privilege.table_privilege_.privileges_.add_member(OB_PRIV_CREATE_USER);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_CREATE_USER to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_create_user) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(6, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_delete = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_delete)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_delete) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_delete == 1)
        {
          bool result = user_privilege.table_privilege_.privileges_.add_member(OB_PRIV_DELETE);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_DELETE to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_delete) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(7, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_drop = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_drop)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_drop) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_drop == 1)
        {
          bool result = user_privilege.table_privilege_.privileges_.add_member(OB_PRIV_DROP);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_DROP to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_drop) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(8, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_grant_option = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_grant_option)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_grant_option) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_grant_option == 1)
        {
          bool result = user_privilege.table_privilege_.privileges_.add_member(OB_PRIV_GRANT_OPTION);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_GRANT_OPTION to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_grant_option) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(9, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_insert = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_insert)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_insert) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_insert == 1)
        {
          bool result = user_privilege.table_privilege_.privileges_.add_member(OB_PRIV_INSERT);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_INSERT to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_insert) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(10, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_update = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_update)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_update) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_update == 1)
        {
          bool result = user_privilege.table_privilege_.privileges_.add_member(OB_PRIV_UPDATE);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_UPDATE to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_update) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(11, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_select = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_select)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_select) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_select == 1)
        {
          bool result = user_privilege.table_privilege_.privileges_.add_member(OB_PRIV_SELECT);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_SELECT to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_select) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ret = row.raw_get_cell(12, pcell, table_id, column_id);
    if (pcell->get_type() == ObIntType)
    {
      int64_t priv_replace = 0;
      if (OB_SUCCESS != (ret = pcell->get_int(priv_replace)))
      {
        TBSYS_LOG(WARN, "failed to get_int(priv_replace) from ObObj, ret=%d", ret);
      }
      else
      {
        if (priv_replace == 1)
        {
          bool result = user_privilege.table_privilege_.privileges_.add_member(OB_PRIV_REPLACE);
          if (!result)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "add OB_PRIV_REPLACE to ObBitSet<> failed, ret=%d", ret);
          }
        }
      }
    }
    else if (pcell->get_type() == ObNullType)
    {
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "raw_get_cell(priv_replace) from row failed, ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    if(user_privilege.table_privilege_.privileges_.is_empty())
    {
      empty = true;
    }
  }
  return ret;
}
//add e
