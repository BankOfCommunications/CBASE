#include "sqltest.h"


SqlTest::SqlTest()
{

}

SqlTest::~SqlTest()
{
}

int SqlTest::init()
{
  int ret = OB_SUCCESS;

  ret = sqltest_param_.load_from_config();
  sqltest_param_.dump_param();

  if (OB_SUCCESS == ret)
  {
    ObSchemaManagerV2 schema_manager;
    tbsys::CConfig config;
    if (!schema_manager.parse_from_file(sqltest_param_.get_schema_file(), config))
    {
      TBSYS_LOG(WARN, "failed to parse schema file, schema_file=%s, err=%d",
          sqltest_param_.get_schema_file(), ret);
    }
    else
    {
      ObSchemaManagerV2& sqltest_schema_manager = sqltest_schema_.get_schema_mgr();
      ret = translate_user_schema(schema_manager, sqltest_schema_manager);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to translate schema manager, ret=%d", ret);
      }
      else
      {
        sqltest_schema_manager.print(stdout);
      }
    }

    if (0 == ret)
    {
      const char* key_names[2] = {
        "prefix",
        "suffix"
      };
      sqltest_schema_.set_keys(0, 2, key_names);
      ret = sqltest_schema_.init();
      if (0 != ret)
      {
        TBSYS_LOG(WARN, "failed to init sqltest schema, ret=%d", ret);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = sql_builder_.init(sqltest_schema_);
    if (0 != ret)
    {
      TBSYS_LOG(WARN, "failed to init sql builder, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = sqltest_pattern_.init(sqltest_param_.get_pattern_file());
    if (0 != ret)
    {
      TBSYS_LOG(WARN, "failed to init sqltest_pattern, ret=%d", ret);
    }
    else
    {
      sqltest_pattern_.dump();
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = sqltest_dao_.init(sqltest_param_.get_ob_ip(), sqltest_param_.get_ob_port(),
        sqltest_param_.get_mysql_ip(), sqltest_param_.get_mysql_port(),
        sqltest_param_.get_mysql_user(), sqltest_param_.get_mysql_pass(),
        sqltest_param_.get_mysql_db());
    if (0 != ret)
    {
      TBSYS_LOG(WARN, "failed to init sqltest dao, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    Rule read_rule;
    Rule write_rule;
    KeyRule key_rule;
    read_rule.add_rule(GEN_RANDOM, GEN_RANDOM, 100);  // get: random prefix and random suffix

    write_rule.add_rule(GEN_SEQ, GEN_SEQ, 10);       // write: seq prefix and seq suffix
    write_rule.add_rule(GEN_RANDOM, GEN_RANDOM, 45); // write: random prefix and random suffix
    write_rule.add_rule(GEN_RANDOM, GEN_SEQ, 45);    // write: random prefix and seq suffix

    key_rule.init(read_rule, write_rule);
    ret = key_gen_.init(sqltest_dao_.get_ob_client(), key_rule);
    if (0 != ret)
    {
      TBSYS_LOG(WARN, "failed to init key gen, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = read_worker_.init(sql_builder_, key_gen_, sqltest_dao_, sqltest_pattern_);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "failed to init read worker, ret=%d", ret);
    }
    else
    {
      ret = write_worker_.init(sql_builder_, key_gen_, sqltest_dao_);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to init write worker, ret=%d", ret);
      }
    }
  }

  return ret;
}

int SqlTest::start()
{
  int ret                     = OB_SUCCESS; 
  int64_t write_thread_count  = 0;
  int64_t read_thread_count   = 0;

  ret = ob_init_memory_pool();
  if (OB_SUCCESS == ret)
  {
    ret = init();
  }

  if (OB_SUCCESS == ret)
  {
    write_thread_count = sqltest_param_.get_write_thread_count();
    read_thread_count = sqltest_param_.get_read_thread_count();

    if (write_thread_count > 0)
    {
      write_worker_.setThreadCount(write_thread_count);
      write_worker_.start();
    }

    if (read_thread_count > 0)
    {
      read_worker_.setThreadCount(read_thread_count);
      read_worker_.start();
    }

    if (OB_SUCCESS == ret)
    {
      ret = wait();
    }
  }

  return ret;
}

int SqlTest::stop()
{
  write_worker_.stop();
  read_worker_.stop();

  return OB_SUCCESS;
}

int SqlTest::wait()
{
  write_worker_.wait();
  read_worker_.wait();

  return OB_SUCCESS;
}

int SqlTest::translate_user_schema(const ObSchemaManagerV2& ori_schema_mgr,
    ObSchemaManagerV2& user_schema_mgr)
{
  int ret = OB_SUCCESS;

  user_schema_mgr.set_version(ori_schema_mgr.get_version());
  user_schema_mgr.set_app_name(ori_schema_mgr.get_app_name());

  const ObTableSchema* table_schema = NULL;
  const ObColumnSchemaV2* column_schema = NULL;
  const uint64_t MIN_USER_TABLE_ID = 1002;

  // add user table schema
  for (table_schema = ori_schema_mgr.table_begin();
      table_schema != ori_schema_mgr.table_end() && table_schema != NULL && OB_SUCCESS == ret;
      table_schema++)
  {
    TBSYS_LOG(INFO, "table_id=%lu", table_schema->get_table_id());
    if (table_schema->get_table_id() < MIN_USER_TABLE_ID) continue; // omit system table and prefix_status table
    ret = user_schema_mgr.add_table(*((ObTableSchema*) table_schema));
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "failed to add table schema");
    }
  }

  for (column_schema = ori_schema_mgr.column_begin();
      column_schema != ori_schema_mgr.column_end() && column_schema != NULL && OB_SUCCESS == ret;
      column_schema++)
  {
    if (column_schema->get_table_id() < MIN_USER_TABLE_ID) continue; // omit system table
    ret = user_schema_mgr.add_column(*((ObColumnSchemaV2*) column_schema));
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "failed to add column schema");
    }
  }

  user_schema_mgr.sort_column();
  return ret;
}

