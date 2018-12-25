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
#ifndef OB_PRIVILEGE_H_
#define OB_PRIVILEGE_H_

#include "ob_string.h"
#include "hash/ob_hashmap.h"
#include "ob_string_buf.h"
#include "ob_row.h"
#include "ob_array.h"
#include "ob_bit_set.h"
#define USERNAME_MAP_BUCKET_NUM 100
#define USERTABLE_MAP_BUCKET_NUM 100
//add dolphin [database manager]@20150607
#define DBNAME_MAP_BUCKET_NUM 100
#define USERDB_MAP_BUCKET_NUM 100
#define TABLE_DATABASE_BUCKET_NUM 100
//add:e
namespace oceanbase
{
  namespace common
  {
    // do not need serialization and deserialization
    class ObPrivilege
    {
      public:
        struct TablePrivilege
        {
          TablePrivilege();
          ObString db_name_;//add liumz, set when do_privilege_check()
          uint64_t table_id_;
          uint64_t db_id_;//add dolphin [database manager]@20150608
          ObBitSet<> privileges_;
        };
        struct User
        {
          User();
          ObString user_name_;
          uint64_t user_id_;
          ObString password_;
          ObString comment_;
          ObBitSet<> privileges_;
          bool locked_;
        };
        struct UserPrivilege
        {
          UserPrivilege();
          uint64_t user_id_;
          TablePrivilege table_privilege_;
        };
        struct UserIdTableId
        {
          UserIdTableId();
          uint64_t user_id_;
          uint64_t table_id_;
          int64_t hash() const;
          bool operator==(const UserIdTableId & other) const;
        };
        //add dolphin [database manager]@20150607
        struct ObDatabase
        {
          ObDatabase();
          ObString dbname_;
          uint64_t dbid_;
          uint8_t stat_;
        };
        struct  UserIdDatabaseId
        {
          UserIdDatabaseId();
          uint64_t userid_;
          uint64_t dbid_;
          int64_t hash() const;
          bool operator==(const UserIdDatabaseId&) const;
        };
        /**
         * we add the privilege for database level,
         * so when we judge privilege,first check user privilege,
         * then databse privilege,table privilege last
         * @since [database manager]
         */
        struct DatabasePrivilege
        {
          DatabasePrivilege();
          uint64_t dbid_;
          ObBitSet<> privileges_;
          bool isLocked_;
        };
        struct TableDatabase
        {
          TableDatabase();
          ObString dbname_;
          ObString table_name_;
        };
        //add:e
        // typedef hash::ObHashMap<ObString, User, hash::NoPthreadDefendMode,
        //                         hash::hash_func<ObString>, hash::equal_to<ObString>,
        //                         hash::SimpleAllocer<hash::HashMapTypes<ObString, User>::AllocType, 384> > NameUserMap;
        // typedef hash::ObHashMap<UserIdTableId, UserPrivilege, hash::NoPthreadDefendMode,
        //                         hash::hash_func<UserIdTableId>, hash::equal_to<UserIdTableId>,
        //                         hash::SimpleAllocer<hash::HashMapTypes<UserIdTableId, UserPrivilege>::AllocType, 384> > UserPrivMap;
        typedef hash::ObHashMap<ObString, User, hash::NoPthreadDefendMode> NameUserMap;
        typedef hash::ObHashMap<UserIdTableId, UserPrivilege, hash::NoPthreadDefendMode> UserPrivMap;
        //add dolphin [database manager]@20150607
        typedef hash::ObHashMap<ObString,ObDatabase,hash::NoPthreadDefendMode> NameDbMap;
        typedef hash::ObHashMap<UserIdDatabaseId,DatabasePrivilege,hash::NoPthreadDefendMode> UserDbPriMap;
        typedef hash::ObHashMap<int64_t,TableDatabase,hash::NoPthreadDefendMode> TableDbMap;
        //add:e
      public:
        static void privilege_to_string(const ObBitSet<> &privileges, char *buf, const int64_t buf_len, int64_t &pos);
        //add liumz, [database manager]20150610:b
        static void privilege_to_string_v2(const ObBitSet<> &privileges, char *buf, const int64_t buf_len, int64_t &pos);
		//add:e
        ObPrivilege();
        ~ObPrivilege();
        int init();
        /**
         * @synopsis  user_is_valid
         *
         * @param username
         * @param password
         *
         * @returns OB_SUCCESS if username is valid
         */
        int user_is_valid(const ObString & username, const ObString & password) const;
        /**
         * @synopsis  has_needed_privileges
         *
         * @param username
         * @param table_privileges
         *
         * @returns OB_SUCCESS if username has the needed privileges
         */
        int has_needed_privileges(const ObString &username, const ObArray<TablePrivilege> & table_privileges) const;
        //add liumz, [multi_database.priv_management.check_privilege]:20150610:b
        int has_needed_privileges_v2(const ObString &username, const ObArray<TablePrivilege> & table_privileges, bool is_global_priv = false) const;
        /**
         * @brief has_needed_db_privileges
         * @param user
         * @param needed_privilege
         * @param fused_privilege, fuse user.privileges_ with db.privileges_, used when check table level privs
         * @return OB_SUCCESS if OB_SUCCESS if username has the needed db privileges
         */
        int has_needed_db_privileges(const User &user, const TablePrivilege &needed_privilege, ObBitSet<> &fused_privilege) const;
        //add:e
        /**
         * @synopsis  全量导入__users表中内容
         *
         * @param row: 从ObResultSet迭代出来的，一行一行的往里加
         *
         * @returns OB_SUCCESS if add row succeed
         */
        int add_users_entry(const ObRow & row);
        int add_table_privileges_entry(const ObRow & row);
        //add dolphin [database manager]@20150607
        /**
         * @synopsis: fill all database name records
         * @param row: the every database name record
         * @return OB_SUCCESS if succeed or OB_ERR_UNEXPECTED
         */
        int add_database_name_entry(const ObRow& row);
        /**
         *
         * @synopsis: fill all database privileges for all user
         *
         * @param row: the database privilege for one user
         *
         * @return OB_SUCCESS if succeed or OB_ERR_UNEXPECTED
         */
        int add_database_privilege_entry(const ObRow& row);
        /**
         * @synopis: fill the datase and table record
         *
         * @param row:  the table & database record
         *
         * @return OB_SUCCESS if succed or OB_ERR_UNEXPECTED
         */
        int add_table_database_entry(const ObRow& row);
        //add:e
      /**
         * @synopsis  比较两个privilege
         *
         * @param privilege1
         * @param privilege2
         *
         * @returns 如果privilege2包含(包括相等)privilege1，则返回true，否则返回false
         */
        bool compare_privilege(const ObBitSet<> & privilege1, const ObBitSet<> & privilege2) const;
        void set_version(const int64_t version);
        int64_t get_version() const;
        void print_info(void) const;
        NameUserMap* get_username_map() { return &username_map_;}
        UserPrivMap* get_user_table_map()  {return &user_table_map_;}
      //add dolphin [database manager]@20150607
        const NameDbMap* get_database_name_map() const  {return &name_database_map_;}
        UserDbPriMap* get_user_database_privilege_map() {return &user_database_pri_map_;}
        const UserDbPriMap* get_user_database_privilege_map_const() const{return &user_database_pri_map_;}
        const NameUserMap* get_username_map_const() const{ return &username_map_;}
        TableDbMap*  get_table_database_map() {return &table_database_map_;}
      //add:e
        //add wenghaixing [database manage]20150610
        int user_db_is_valid(const ObString &username, const ObString &db_name) const;
        int user_has_global_priv(const User &user) const;
        int is_db_exist(const ObString &db_name,  uint64_t &db_id) const;
        int is_user_exist(const ObString &user_name, User &user)const;
        int db_result_to_priv_empty(const ObRow &row, bool &empty);
        int table_result_to_priv_empty(const ObRow &row, bool &empty);
        //add e
        //add liumz, [database manager]20150610:b
        int get_db_name(const uint64_t db_id, ObString &db_name) const;
        int get_user_name(const uint64_t user_id, ObString &user_name) const;
        //add:e
        //add liumz, [priv management]20150902:b
        int get_db_id(const uint64_t table_id, uint64_t &db_id) const;
        int get_user_db_id(const uint64_t table_id, ObArray<UserIdDatabaseId> &user_db_id) const;
        //add:e
      private:
        // key:username
        NameUserMap username_map_;
        UserPrivMap user_table_map_;
        //add dolphin [database manager]@20150607:b
        NameDbMap  name_database_map_;
        UserDbPriMap user_database_pri_map_;
        TableDbMap table_database_map_;
        //add:e
        ObStringBuf string_buf_;
        int64_t version_;

    };
  }// namespace common
}//namespace oceanbase
#endif
