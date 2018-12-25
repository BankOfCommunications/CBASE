#include "common/ob_meta_cache.h"
#include "common/ob_malloc.h"
#include "common/ob_schema.h"
#include "common/utility.h"
#include "test_rowkey_helper.h"
#include <gtest/gtest.h>
#include <unistd.h>

using namespace oceanbase::common;
namespace oceanbase
{
  namespace common
  {
    class UserRpc:public IRpcStub 
    {
    public:
      UserRpc():buffer_(16384)
        {}
      virtual ~UserRpc()
        {}
      int rpc_get(ObGetParam &get_param, ObScanner &scanner, const int64_t timeout);
      ThreadSpecificBuffer buffer_;
      CharArena allocator_;
    };

    class PermRpc:public IRpcStub
    {
    public:
      PermRpc():buffer_(16384)
        {
        }
      virtual ~PermRpc()
        {}
      int rpc_get(ObGetParam& get_param, ObScanner& scanner, const int64_t timeout);
      ThreadSpecificBuffer buffer_;
    };

    class SkeyRpc:public IRpcStub
    {
    public:
      SkeyRpc():buffer_(16384)
        {}
      virtual ~SkeyRpc()
        {}
      int rpc_get(ObGetParam& get_param, ObScanner& scanner, const int64_t timeout);
      ThreadSpecificBuffer buffer_;
    };

    int UserRpc::rpc_get(ObGetParam& get_param, ObScanner& scanner, const int64_t timeout)
    {
      UNUSED(timeout);
      int ret = OB_SUCCESS;
      ObCellInfo *get_cell;
      get_cell = get_param[0];
      ObRowkey row_key = make_rowkey("fangji", &allocator_);
      fprintf(stdout, "get cell rowkey:%s\n", to_cstring(get_cell->row_key_));
      EXPECT_EQ(1, (int)get_param.get_cell_size());
      EXPECT_EQ(USER_TABLE_ID, (int)get_cell->table_id_);
      EXPECT_EQ(4, (int)get_cell->column_id_);
      EXPECT_EQ(row_key, get_cell->row_key_);
                

      ObCellInfo cell_info;
      ObString str;
      ObObj value;
      ThreadSpecificBuffer::Buffer * buffer = buffer_.get_buffer();
      buffer->reset();
      char * pass = (char*)"password";
      str.assign(buffer->current(), static_cast<int32_t>(strlen(pass)));
      buffer->write(pass, static_cast<int32_t>(strlen(pass)));
      value.set_varchar(str);
      cell_info.table_id_  = USER_TABLE_ID;
      cell_info.column_id_ = USER_COL_ID;
      cell_info.value_ = value;
      scanner.add_cell(cell_info);
      return ret;
    }

    int PermRpc::rpc_get(ObGetParam& get_param, ObScanner& scanner, const int64_t timeout)
    {
      UNUSED(timeout);
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      ObCellInfo *get_cell;
      get_cell = get_param[0];
      EXPECT_EQ(1, (int)get_param.get_cell_size());
      EXPECT_EQ(PERM_TABLE_ID, (int)get_cell->table_id_);
      EXPECT_EQ(4, (int)get_cell->column_id_);
      char sbuffer[4096];
      common::serialization::encode_i64(sbuffer, 4096, pos, 1000);
      memcpy(sbuffer + pos, "fangji", 6);
      EXPECT_EQ(0, memcmp(sbuffer, get_cell->row_key_.ptr(), get_cell->row_key_.length()));

      ObCellInfo cell_info;
      ObString str;
      ObObj value;
      ThreadSpecificBuffer::Buffer* buffer = buffer_.get_buffer();
      buffer->reset();
      scanner.reset();
      value.set_int(755);
      cell_info.value_ = value;
      cell_info.table_id_  = PERM_TABLE_ID;
      cell_info.column_id_ = PERM_COL_ID;
      scanner.add_cell(cell_info);
      return ret;
    }

    int SkeyRpc::rpc_get(ObGetParam& get_param, ObScanner& scanner, const int64_t timeout)
    {
      UNUSED(timeout);
      int ret = OB_SUCCESS;
      ObCellInfo *get_cell;
      get_cell = get_param[0];
      EXPECT_EQ(1, (int)get_param.get_cell_size());
      EXPECT_EQ(SKEY_TABLE_ID, (int)get_cell->table_id_);
      EXPECT_EQ(4, (int)get_cell->column_id_);
      EXPECT_LT(0, get_cell->row_key_.length());
      
      ObCellInfo cell_info;
      ObString str;
      ObObj value;
      scanner.reset();
      ThreadSpecificBuffer::Buffer * buffer = buffer_.get_buffer();
      buffer->reset();
      char * skey = (char*)"skeyskey";
      str.assign(buffer->current(), static_cast<int32_t>(strlen(skey)));
      buffer->write(skey, static_cast<int32_t>(strlen(skey)));
      value.set_varchar(str);
      cell_info.value_ = value;
      cell_info.table_id_  = SKEY_TABLE_ID;
      cell_info.column_id_ = SKEY_COL_ID;
      scanner.add_cell(cell_info);
      return ret;
    }
  }
}

TEST(MetaCache, UserTest)
{
  UserRpc urpc;
  char * str = (char*)"fangji";
  ObString keyvalue;
  keyvalue.assign(str, static_cast<int32_t>(strlen(str)));
  ObUserInfoKey userkey(keyvalue);
  ObUserInfoValue uservalue;

  const int64_t version = 10;
  ObSKeyInfoKey pkeykey(version);
  ObSKeyInfoValue pkeyvalue;

  RefGuard<ObUserInfoKey, ObUserInfoValue> user_info_guard;
  int64_t timeout = 1000;
  int64_t max_size = 1048576;
  ObMetaCache<ObUserInfoKey, ObUserInfoValue> user_info_cache;
  user_info_cache.init(timeout, &urpc, max_size);
  user_info_cache.get(userkey, uservalue, 1000, user_info_guard);
  EXPECT_EQ(0, memcmp(uservalue.get_passwd().ptr(), "password", uservalue.get_length()));
}

TEST(MetaCache, PermTest)
{
  PermRpc prpc;
  char * str = (char*)"fangji";
  ObString keyvalue;
  keyvalue.assign(str, static_cast<int32_t>(strlen(str)));

  const int64_t table_id = 1000;
  ObPermInfoKey permkey(table_id, keyvalue);
  ObPermInfoValue permvalue;
  
  int64_t timeout = 1000;
  int64_t max_size = 1048576;
  
  ObMetaCache<ObPermInfoKey, ObPermInfoValue> perm_info_cache;
  RefGuard<ObPermInfoKey, ObPermInfoValue> perm_info_guard;
  perm_info_cache.init(timeout, &prpc, max_size);
  perm_info_cache.get(permkey, permvalue, 1000, perm_info_guard);
  EXPECT_EQ(permvalue.get_perm_mask(), 755);
}


TEST(MetaCache, PkeyTest)
{
  SkeyRpc srpc;
  char * str = (char*)"fangji";
  ObString keyvalue;
  keyvalue.assign(str, static_cast<int32_t>(strlen(str)));
  const int64_t version = 10;
  ObSKeyInfoKey pkeykey(version);
  ObSKeyInfoValue pkeyvalue;


  int64_t timeout = 10000;
  int64_t max_size = 1048576;
    
  ObMetaCache<ObSKeyInfoKey, ObSKeyInfoValue> pkey_info_cache;
  RefGuard<ObSKeyInfoKey, ObSKeyInfoValue> skey_info_guard;
  RefGuard<ObSKeyInfoKey, ObSKeyInfoValue> skey_info_guard1;
  pkey_info_cache.init(timeout, &srpc, max_size);
  pkey_info_cache.get(pkeykey, pkeyvalue, 1000, skey_info_guard);
  usleep(100);
  pkey_info_cache.get(pkeykey, pkeyvalue, 1000, skey_info_guard1);
  EXPECT_EQ(0, memcmp(pkeyvalue.get_skey().ptr(), "skeyskey", pkeyvalue.get_length()));
}

TEST(MetaCache, adjust)
{
  SkeyRpc srpc;
  char str[16];
  ObString keyvalue;
  keyvalue.assign(str, static_cast<int32_t>(strlen(str)));
  const int64_t version = 10;
  ObSKeyInfoKey pkeykey(version);

  int64_t timeout = 1000000;
  int64_t max_size = 1048576;
  ObMetaCache<ObSKeyInfoKey, ObSKeyInfoValue> pkey_info_cache;

  pkey_info_cache.init(timeout, &srpc, max_size);
  for(int i = 0; i < 100; ++i)
  {
    const int64_t version = i;
    RefGuard<ObSKeyInfoKey, ObSKeyInfoValue> skey_info_guard;
    ObSKeyInfoKey pkeykey(version);
    ObSKeyInfoValue pkeyvalue;
    pkey_info_cache.get(pkeykey,pkeyvalue,1000, skey_info_guard);
    EXPECT_EQ(0, memcmp(pkeyvalue.get_skey().ptr(), "skeyskey", pkeyvalue.get_length()));
  }
  sleep(3);
  pkey_info_cache.adjust();
  
  RefGuard<ObSKeyInfoKey, ObSKeyInfoValue> skey_info_guard;
  ObSKeyInfoValue pkeyvalue;
  pkey_info_cache.get(pkeykey,pkeyvalue,1000, skey_info_guard);
  EXPECT_EQ(0, memcmp(pkeyvalue.get_skey().ptr(), "skeyskey", pkeyvalue.get_length()));
}

TEST(MetaCache, clear)
{
  SkeyRpc srpc;
  char str[16];
  ObString keyvalue;
  keyvalue.assign(str, static_cast<int32_t>(strlen(str)));
  const int64_t version = 10;
  ObSKeyInfoKey pkeykey(version);

  int64_t timeout = 1000000;
  int64_t max_size = 1048576;
  ObMetaCache<ObSKeyInfoKey, ObSKeyInfoValue> pkey_info_cache;

  pkey_info_cache.init(timeout, &srpc, max_size);
  for(int i = 0; i < 100; ++i)
  {
    const int64_t version = i;
    RefGuard<ObSKeyInfoKey, ObSKeyInfoValue> skey_info_guard;
    ObSKeyInfoKey pkeykey(version);
    ObSKeyInfoValue pkeyvalue;
    pkey_info_cache.get(pkeykey,pkeyvalue,1000, skey_info_guard);
    EXPECT_EQ(0, memcmp(pkeyvalue.get_skey().ptr(), "skeyskey", pkeyvalue.get_length()));
  }
  EXPECT_EQ(0, pkey_info_cache.clear());
}


int main(int argc, char** argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
