#include "bigquerytest.h"
#include "common/ob_malloc.h"


BigqueryTest::BigqueryTest()
{

}

BigqueryTest::~BigqueryTest()
{
}

int BigqueryTest::init()
{
  int ret = OB_SUCCESS;

  ret = bigquerytest_param_.load_from_config();
  bigquerytest_param_.dump_param();

  if (OB_SUCCESS == ret)
  {
    ServerAddr server_addr[100];
    int64_t addr_num = 0;
    translate_server_addr(bigquerytest_param_.get_ob_addr(), server_addr, addr_num);
    ret = client_.init(server_addr, addr_num, "admin", "admin", "test");
    if (0 != ret)
    {
      TBSYS_LOG(WARN, "failed to init client, server_addr=%p, addr_num=%ld, ret=%d",
          server_addr, addr_num, ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = prefix_info_.init(client_);
    if (0 != ret)
    {
      TBSYS_LOG(WARN, "failed to init prefix info, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = key_gen_.init(prefix_info_, bigquerytest_param_);
    if (0 != ret)
    {
      TBSYS_LOG(WARN, "failed to init key gen, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = read_worker_.init(key_gen_, client_, prefix_info_);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "failed to init read worker, ret=%d", ret);
    }
    else
    {
      ret = write_worker_.init(key_gen_, client_, prefix_info_);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to init write worker, ret=%d", ret);
      }
    }
  }

  return ret;
}

void BigqueryTest::translate_server_addr(const char* ob_addr, ServerAddr* addr, int64_t& addr_num)
{
  assert(NULL != ob_addr && NULL != addr);
  char tmp_str[1024];
  strcpy(tmp_str, ob_addr);
  char* token = strtok(tmp_str, " ");
  while (NULL != token)
  {
    if (token[0] != '\0')
    {
      char token1[128];
      strcpy(token1, token);
      char * tmp = NULL;
      for (int64_t i = 0; token1[i] != '\0'; ++i)
      {
        if (':' == token1[i])
        {
          tmp = token1 + i;
          break;
        }
      }
      assert(tmp != NULL);
      *tmp = '\0';
      strcpy(addr[addr_num].host, token1);
      sscanf(tmp + 1, "%d", &addr[addr_num].port);

      TBSYS_LOG(INFO, "[SERVER_ADDR] host=%s, port=%d", addr[addr_num].host, addr[addr_num].port);
      ++addr_num;
    }
    token = strtok(NULL, " ");
  }
}

int BigqueryTest::start()
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
    write_thread_count = bigquerytest_param_.get_write_thread_count();
    read_thread_count = bigquerytest_param_.get_read_thread_count();

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

int BigqueryTest::stop()
{
  write_worker_.stop();
  read_worker_.stop();

  return OB_SUCCESS;
}

int BigqueryTest::wait()
{
  write_worker_.wait();
  read_worker_.wait();

  return OB_SUCCESS;
}


