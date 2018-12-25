#include "gtest/gtest.h"
#include "common/ob_login_mgr.h"

using namespace oceanbase;
using namespace common;

static const char *nil = NULL;

class MockToken : public IToken
{
  public:
    static const int64_t MAGIC_NUM = 0x6e656b6f745f676d; // mg_token
  public:
    MockToken()
    {
    };
    ~MockToken()
    {
    };
  public:
    int serialize(char *buf, const int64_t buf_len, int64_t& pos)
    {
      *((int64_t*)(buf + pos)) = MAGIC_NUM;
      pos += sizeof(int64_t);

      info_.serialize(buf, buf_len, pos);

      *((int64_t*)(buf + pos)) = timestamp_;
      pos += sizeof(int64_t);
      *((int64_t*)(buf + pos)) = skey_version_;
      pos += sizeof(int64_t);
      *((int64_t*)(buf + pos)) = skey_;
      pos += sizeof(int64_t);

      return OB_SUCCESS;
    };
    int deserialize(const char* buf, const int64_t data_len, int64_t& pos)
    {
      int ret = OB_SUCCESS;

      int64_t magic = *((int64_t*)(buf + pos));

      if (MAGIC_NUM != magic)
      {
        ret = OB_ENTRY_NOT_EXIST;
      }
      else
      {
        pos += sizeof(int64_t);
        info_.deserialize(buf, data_len, pos);

        timestamp_ = *((int64_t*)(buf + pos));
        pos += sizeof(int64_t);
        skey_version_ = *((int64_t*)(buf + pos));
        pos += sizeof(int64_t);
        skey_ = *((int64_t*)(buf + pos));
        pos += sizeof(int64_t);
      }

      return ret;
    };
    int64_t get_serialize_size() const
    {
      return (info_.get_serialize_size() + sizeof(int64_t) * 4);
    };
  public:
    int set_username(const ObString &username)
    {
      return info_.set_username(username);
    };
    int set_password(const ObString &password)
    {
      return info_.set_password(password);
    };
    int set_timestamp(const int64_t timestamp)
    {
      timestamp_ = timestamp;
      return OB_SUCCESS;
    };
    int get_username(ObString &username) const
    {
      username = info_.get_username();
      return OB_SUCCESS;
    };
    int get_timestamp(int64_t &timestamp) const
    {
      timestamp = timestamp_;
      return OB_SUCCESS;
    };
  public:
    int64_t get_skey_version() const
    {
      return skey_version_;
    };
    int encrypt(const int64_t skey_version, const int64_t skey)
    {
      int ret = OB_SUCCESS;
      if (0 == skey)
      {
        ret = OB_ENCRYPT_FAILED;
      }
      else
      {
        skey_version_ = skey_version;
        skey_ = skey;
      }
      return ret;
    };
    int decrypt(const int64_t skey)
    {
      int ret = OB_SUCCESS;
      if (skey != skey_)
      {
        ret = OB_DECRYPT_FAILED;
      }
      return ret;
    };
  public:
    ObLoginInfo info_;
    int64_t timestamp_;
    int64_t skey_version_;
    int64_t skey_;
};

int64_t g_cur_version = 1;

class MockSKeyTable : public ISKeyTable
{
  public:
    MockSKeyTable()
    {
      skeys_[0] = 0;
      skeys_[1] = 1001;
    };
    ~MockSKeyTable()
    {
    };
  public:
    int get_skey(const int64_t version, int64_t &skey) const
    {
      int ret = OB_SUCCESS;
      if (0 > version || 1 < version)
      {
        ret = OB_SKEY_VERSION_WRONG;
      }
      else
      {
        skey = skeys_[version];
      }
      return ret;
    };
    int get_cur_skey(int64_t &version, int64_t &skey) const
    {
      int ret = OB_SUCCESS;
      if (0 > g_cur_version || 1 < g_cur_version)
      {
        ret = OB_RESPONSE_TIME_OUT;
      }
      else
      {
        version = g_cur_version;
        skey = skeys_[g_cur_version];
      }
      return ret;
    };
  private:
    int64_t skeys_[2];
};

class MockUserTable : public IUserTable
{
  public:
    MockUserTable()
    {
    };
    ~MockUserTable()
    {
    };
  public:
    int userinfo_check(const ObString &username, const ObString &password) const
    {
      int ret = OB_SUCCESS;
      const ObString root(4, 4, (char*)"root");
      if (root != username)
      {
        ret = OB_USER_NOT_EXIST;
      }
      else if (root != password)
      {
        ret = OB_PASSWORD_WRONG;
      }
      return ret;
    };
};

TEST(TestObLoginInfo, set_username)
{
  ObLoginInfo li;
  ObString username;

  // have not set
  EXPECT_NE(nil, li.get_username().ptr());
  EXPECT_EQ(0, li.get_username().length());

  // set null ptr
  EXPECT_EQ(OB_INVALID_ARGUMENT, li.set_username(username));
  // set 0 length
  EXPECT_EQ(OB_INVALID_ARGUMENT, li.set_username(li.get_username()));

  // set too long
  username.assign_ptr((char*)"abcdefghijklmnopqrstuvwxyz1234567", 33);
  EXPECT_EQ(OB_INVALID_ARGUMENT, li.set_username(username));

  // set valid username
  username.assign_ptr((char*)"admin", 5);
  EXPECT_EQ(OB_SUCCESS, li.set_username(username));
  EXPECT_EQ(true, username == li.get_username());

  // reset valid username
  username.assign_ptr((char*)"root", 5);
  EXPECT_EQ(OB_SUCCESS, li.set_username(username));
  EXPECT_EQ(true, username == li.get_username());
}

TEST(TestObLoginInfo, set_password)
{
  ObLoginInfo li;
  ObString password;

  // have not set
  EXPECT_NE(nil, li.get_password().ptr());
  EXPECT_EQ(0, li.get_password().length());

  // set null ptr
  EXPECT_EQ(OB_INVALID_ARGUMENT, li.set_password(password));
  // set 0 length
  EXPECT_EQ(OB_INVALID_ARGUMENT, li.set_password(li.get_password()));

  // set too long
  password.assign_ptr((char*)"abcdefghijklmnopqrstuvwxyz1234567", 33);
  EXPECT_EQ(OB_INVALID_ARGUMENT, li.set_password(password));

  // set valid password
  password.assign_ptr((char*)"admin", 5);
  EXPECT_EQ(OB_SUCCESS, li.set_password(password));
  EXPECT_EQ(true, password == li.get_password());

  // reset valid password
  password.assign_ptr((char*)"root", 5);
  EXPECT_EQ(OB_SUCCESS, li.set_password(password));
  EXPECT_EQ(true, password == li.get_password());
}

TEST(TestObLoginInfo, serialize)
{
  ObLoginInfo li;
  ObString username;
  ObString password;

  const int64_t buf_len = 1024;
  char buf[buf_len];
  int64_t pos = 0;
  int64_t pos_before = pos;
  int64_t prev_size = 0;

  // null buf
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, li.serialize(NULL, buf_len, pos));
  EXPECT_EQ(0, pos);

  // 0 buf_len
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, li.serialize(buf, 0, pos));
  EXPECT_EQ(0, pos);

  // 0 > pos
  pos = -1;
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, li.serialize(buf, buf_len, pos));
  EXPECT_EQ(-1, pos);

  // pos == buf_len
  pos = buf_len;
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, li.serialize(buf, buf_len, pos));
  EXPECT_EQ(buf_len, pos);

  // encode blank succ
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, li.serialize(buf, buf_len, pos));
  EXPECT_EQ(li.get_serialize_size(), pos);

  // set username and password to encode
  username.assign_ptr((char*)"admin", 5);
  password.assign_ptr((char*)"admin", 5);
  EXPECT_EQ(OB_SUCCESS, li.set_username(username));
  EXPECT_EQ(OB_SUCCESS, li.set_password(password));
  pos_before = pos;
  EXPECT_EQ(OB_SUCCESS, li.serialize(buf, buf_len, pos));
  EXPECT_EQ(li.get_serialize_size(), pos - pos_before);
  prev_size = li.get_serialize_size();

  // set another username and password to encode
  username.assign_ptr((char*)"root", 4);
  password.assign_ptr((char*)"root", 4);
  EXPECT_EQ(OB_SUCCESS, li.set_username(username));
  EXPECT_EQ(OB_SUCCESS, li.set_password(password));
  pos_before = pos;
  EXPECT_EQ(OB_SUCCESS, li.serialize(buf, buf_len, pos));
  EXPECT_EQ(li.get_serialize_size(), pos - pos_before);
  EXPECT_EQ(2, prev_size - li.get_serialize_size());
}

TEST(TestObLoginInfo, deserialize)
{
  ObLoginInfo src_li;
  ObLoginInfo dest_li;
  ObString username;
  ObString password;

  const int64_t data_len = 1024;
  char buf[data_len];
  char tmp_buf[data_len];
  int64_t pos = 0;

  // null buf
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, src_li.deserialize(NULL, data_len, pos));
  EXPECT_EQ(0, pos);

  // 0 data_len
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, src_li.deserialize(buf, 0, pos));
  EXPECT_EQ(0, pos);

  // 0 > pos
  pos = -1;
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, src_li.deserialize(buf, data_len, pos));
  EXPECT_EQ(-1, pos);

  // pos >= data_len
  pos = data_len;
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, src_li.deserialize(buf, data_len, pos));
  EXPECT_EQ(data_len, pos);

  // decode blank succ
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, src_li.serialize(buf, data_len, pos));
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, dest_li.deserialize(buf, data_len, pos));
  EXPECT_EQ(src_li.get_serialize_size(), pos);
  EXPECT_EQ(dest_li.get_serialize_size(), pos);
  EXPECT_EQ(true, src_li.get_username() == dest_li.get_username());
  EXPECT_EQ(true, src_li.get_password() == dest_li.get_password());

  // set username and password to decode
  username.assign_ptr((char*)"root", 4);
  password.assign_ptr((char*)"root", 4);
  EXPECT_EQ(OB_SUCCESS, src_li.set_username(username));
  EXPECT_EQ(OB_SUCCESS, src_li.set_password(password));
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, src_li.serialize(buf, data_len, pos));
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, dest_li.deserialize(buf, data_len, pos));
  EXPECT_EQ(src_li.get_serialize_size(), pos);
  EXPECT_EQ(dest_li.get_serialize_size(), pos);
  EXPECT_EQ(true, src_li.get_username() == dest_li.get_username());
  EXPECT_EQ(true, src_li.get_password() == dest_li.get_password());

  // magic_num wrong
  pos = 0;
  memset(tmp_buf, -1, data_len);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, dest_li.deserialize(tmp_buf, data_len, pos));
  EXPECT_EQ(0, pos);

  // version wrong
  pos = 0;
  memcpy(tmp_buf, buf, sizeof(int64_t));
  EXPECT_EQ(OB_ERROR, dest_li.deserialize(tmp_buf, data_len, pos));
  EXPECT_EQ(0, pos);

  // username decode fail
  pos = 0;
  memcpy(tmp_buf, buf, sizeof(int64_t) * 2);
  EXPECT_EQ(OB_ERROR, dest_li.deserialize(tmp_buf, data_len, pos));
  EXPECT_EQ(0, pos);

  // password decode fail
  pos = 0;
  memcpy(tmp_buf, buf, sizeof(int64_t) * 2 + src_li.get_username().get_serialize_size());
  EXPECT_EQ(OB_ERROR, dest_li.deserialize(tmp_buf, data_len, pos));
  EXPECT_EQ(0, pos);
}

TEST(TestLoginMgr, handle_login)
{
  ObLoginMgr lm;
  ObLoginInfo li;
  MockToken token;
  MockSKeyTable skey_table;
  MockUserTable user_table;

  // not init
  EXPECT_EQ(OB_NOT_INIT, lm.handle_login(li, token));

  lm.init(&skey_table, &user_table);
  
  // wrong username
  EXPECT_EQ(OB_USER_NOT_EXIST, lm.handle_login(li, token));

  // wrong password
  li.set_username(ObString(4, 4, (char*)"root"));
  EXPECT_EQ(OB_PASSWORD_WRONG, lm.handle_login(li, token));

  // get cur skey fail
  li.set_password(ObString(4, 4, (char*)"root"));
  g_cur_version = -1;
  EXPECT_EQ(OB_RESPONSE_TIME_OUT, lm.handle_login(li, token));

  // encrypt fail
  g_cur_version = 0;
  EXPECT_EQ(OB_ENCRYPT_FAILED, lm.handle_login(li, token));

  // handle login succ
  g_cur_version = 1;
  EXPECT_EQ(OB_SUCCESS, lm.handle_login(li, token));
}

TEST(TestLoginMgr, check_token)
{
  ObLoginMgr lm;
  ObLoginInfo li;
  MockToken token;
  MockToken src_token;
  MockSKeyTable skey_table;
  MockUserTable user_table;

  const int64_t data_len = 1024;
  char buf[data_len];
  int64_t pos = 0;

  // not init
  EXPECT_EQ(OB_NOT_INIT, lm.check_token(buf, data_len, pos, token));

  lm.init(&skey_table, &user_table);

  // do not have token
  memset(buf, 0, data_len);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, lm.check_token(buf, data_len, pos, token));

  // get skey fail 
  src_token.encrypt(-1, 1);
  pos = 0;
  src_token.serialize(buf, data_len, pos);
  pos = 0;
  EXPECT_EQ(OB_SKEY_VERSION_WRONG, lm.check_token(buf, data_len, pos, token));

  // decrypt fail
  src_token.encrypt(0, -1);
  pos = 0;
  src_token.serialize(buf, data_len, pos);
  pos = 0;
  EXPECT_EQ(OB_DECRYPT_FAILED, lm.check_token(buf, data_len, pos, token));

  // token expired
  src_token.set_username(ObString(4, 4, (char*)"root"));
  src_token.encrypt(1, 1001);
  src_token.set_timestamp(tbsys::CTimeUtil::getTime() / 1000000 - 300);
  pos = 0;
  src_token.serialize(buf, data_len, pos);
  pos = 0;
  EXPECT_EQ(OB_TOKEN_EXPIRED, lm.check_token(buf, data_len, pos, token));

  // check succ
  src_token.set_username(ObString(4, 4, (char*)"root"));
  src_token.encrypt(1, 1001);
  src_token.set_timestamp((tbsys::CTimeUtil::getTime() / 1000000) << 32);
  pos = 0;
  src_token.serialize(buf, data_len, pos);
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, lm.check_token(buf, data_len, pos, token));
}

TEST(TestLoginMgr, login_and_check)
{
  ObLoginMgr lm;
  ObLoginInfo li;
  MockToken token;
  MockToken src_token;
  MockSKeyTable skey_table;
  MockUserTable user_table;
  ObString username;
  ObString password;

  const int64_t data_len = 1024;
  char buf[data_len];
  int64_t pos = 0;

  lm.init(&skey_table, &user_table);

  li.set_username(ObString(4, 4, (char*)"root"));
  li.set_password(ObString(4, 4, (char*)"root"));
  g_cur_version = 1;
  EXPECT_EQ(OB_SUCCESS, lm.handle_login(li, src_token));
  pos = 0;
  src_token.serialize(buf, data_len, pos);
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, lm.check_token(buf, data_len, pos, token));

  token.get_username(username);
  EXPECT_EQ(true, ObString(4, 4, (char*)"root") == username);
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

