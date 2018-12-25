#include <stdio.h>
#include <unistd.h>
#include <stdint.h>

#include "key_btree.h"
#include "key_btree.cpp"

class TestKey
{
public:
  TestKey() {}
  TestKey(const int32_t size, const char *str)
  {
    set_value(size, str);
  }
  ~TestKey() {}
  void set_value(const int32_t size, const char *str)
  {
    size_ = size;
    if (size_ > 12) size_ = 12;
    memcpy(str_, str, size_);
  }
  void set_value(const char *str)
  {
    set_value(strlen(str) + 1, str);
  }
  int32_t operator- (const TestKey &k) const
  {
    for(int32_t i = 0; i < size_ && i < k.size_; i++)
    {
      if (str_[i] < k.str_[i])
        return -1;
      else if (str_[i] > k.str_[i])
        return 1;
    }
    return (size_ - k.size_);
  }
private:
  int32_t size_;
  char str_[12];

public:
  static const int32_t MAX_SIZE;
};
const int32_t TestKey::MAX_SIZE = 16;
typedef oceanbase::common::KeyBtree<TestKey, int32_t> StringBtree;

// 从文件里取KEY
FILE *fp = NULL;
StringBtree *btree = NULL;

int32_t get_next_id(int32_t &value)
{
  value = 0;
  if (fp != NULL)
  {
    char *p, buffer[32];
    if (NULL == fgets(buffer, 32, fp))
    {
      return -1;
    }
    p = strchr(buffer, ' ');
    if (NULL == p)
      return -1;
    p ++;
    *(p + 8) = '\0';
    value = strtol(p, (char **)NULL, 16);
    fprintf(stderr, "key: %08X\n", value);
  }
  else
  {
    value = (rand() & 0x7FFFFFFF);
    printf("KEY: %08X\n", value);
  }
  return 0;
}

void test_insert(int32_t cnt)
{
  TestKey key;
  int32_t ret, j;
  char buffer[32];
  int32_t failure = 0;

  for(int32_t i = 0; i < cnt; i++)
  {
    if (get_next_id(j)) break;
    sprintf(buffer, "%08X", j);
    key.set_value(buffer);
    ret = btree->put(key, i + 1);
    if (ret != oceanbase::common::ERROR_CODE_OK)
    {
      failure ++;
    }
  }
  //btree->print(NULL);
  fprintf(stderr, "insert failure: %d\n", failure);
}

void test_remove(int32_t cnt, int32_t remove_cnt)
{
  TestKey key;
  int32_t ret, j;
  char buffer[32];
  int32_t *ids = (int32_t*)malloc(cnt * sizeof(int32_t));
  int32_t failure = 0;

  for(int32_t i = 0; i < cnt; i++)
  {
    if (get_next_id(j)) break;
    ids[i] = j;
    sprintf(buffer, "%08X", j);
    key.set_value(buffer);
    btree->put(key, i + 1);
  }
  //btree->print(NULL);
  // remove
  for(int32_t i = 0; i < remove_cnt; i++)
  {
    sprintf(buffer, "%08X", ids[i]);
    fprintf(stderr, "buffer: %s\n", buffer);
    key.set_value(buffer);
    ret = btree->remove(key);
    if (ret != oceanbase::common::ERROR_CODE_OK)
    {
      failure ++;
    }
  }
  //btree->print(NULL);
  free(ids);
  fprintf(stderr, "remove failure: %d\n", failure);
}

void test_search(int32_t cnt)
{
  TestKey key;
  int32_t j;
  char buffer[32];
  int32_t *ids = (int32_t*)malloc(cnt * sizeof(int32_t));

  for(int32_t i = 0; i < cnt; i++)
  {
    if (get_next_id(j)) break;
    ids[i] = j;
    sprintf(buffer, "%08X", j);
    key.set_value(buffer);
    btree->put(key, i + 1);
  }

  // search
  int32_t value = 0;
  int32_t success = 0;
  for(int32_t i = 0; i < cnt; i++)
  {
    sprintf(buffer, "%08X", ids[i]);
    key.set_value(buffer);
    btree->get(key, value);
    if (value == i + 1) success ++;
  }
  if (success == cnt)
  {
    printf("test_search success\n");
  }
  else
  {
    printf("test_search failure: %d <> %d\n", success, cnt);
  }
  if (NULL != ids)
  {
    free(ids);
    ids = NULL;
  }
}

void test_insert_batch(int32_t cnt)
{
  oceanbase::common::BtreeWriteHandle handle;

  TestKey key;
  int32_t ret, j;
  char buffer[32];
  int32_t failure = 0;

  ret = btree->get_write_handle(handle);
  assert(oceanbase::common::ERROR_CODE_OK == ret);
  for(int32_t i = 0; i < cnt; i++)
  {
    if (get_next_id(j)) break;
    sprintf(buffer, "%08X", j);
    key.set_value(buffer);
    ret = btree->put(handle, key, i + 1);
    if (ret != oceanbase::common::ERROR_CODE_OK)
    {
      failure ++;
    }
  }
  handle.end();
  //btree->print(NULL);

  fprintf(stderr, "insert_batch failure: %d\n", failure);
}

void test_range_search(int32_t cnt)
{
  oceanbase::common::BtreeReadHandle handle;

  TestKey key;
  int32_t j;
  char buffer[32];
  int32_t *ids = (int32_t*)malloc(cnt * sizeof(int32_t));

  for(int32_t i = 0; i < cnt; i++)
  {
    if (get_next_id(j)) break;
    ids[i] = j;
    sprintf(buffer, "%08X", j);
    key.set_value(buffer);
    btree->put(key, i + 1);
  }
  fflush(stdout);

  // range search
  int32_t value = 0;
  int32_t success = 0;
  int32_t ret = btree->get_read_handle(handle);
  assert(oceanbase::common::ERROR_CODE_OK == ret);
  btree->set_key_range(handle, btree->get_min_key(), 0, btree->get_max_key(), 0);
  while(oceanbase::common::ERROR_CODE_OK == btree->get_next(handle, value))
  {
    success ++;
  }
  if (success == cnt)
    printf("test_search success\n");
  else
    printf("test_search failure: %d <> %d\n", success, cnt);

  success = 0;
  oceanbase::common::BtreeReadHandle handle1;
  btree->get_read_handle(handle1);
  btree->set_key_range(handle1, btree->get_max_key(), 1, btree->get_min_key(), 1);
  while(oceanbase::common::ERROR_CODE_OK == btree->get_next(handle1, value))
  {
    success ++;
  }
  if (success == cnt - 2)
    printf("test_search success\n");
  else
    printf("test_search failure: %d <> %d\n", success, cnt);
  if (NULL != ids)
  {
    free(ids);
    ids = NULL;
  }
}

int32_t main(int32_t argc, char *argv[])
{
  int32_t o;
  srand(time(NULL));
  int32_t cnt = 20;
  int32_t cnt1;
  int32_t type = 0;
  while ((o = getopt(argc, argv, "t:c:n:f:")) != EOF)
  {
    switch (o)
    {
    case 't':
      type = atoi(optarg);
      break;
    case 'c':
      cnt = atoi(optarg);
      break;
    case 'n':
      cnt1 = atoi(optarg);
      break;
    case 'f':
      fp = fopen(optarg, "rb");
      if (NULL == fp)
      {
        fprintf(stderr, "open file failure: %s\n", optarg);
        return -1;
      }
      break;
    default:
      break;
    }
  }
  cnt1 = cnt - 10;

  btree = new StringBtree(TestKey::MAX_SIZE);
  if (1 == type)
  {
    test_insert(cnt);
  }
  else if (2 == type)
  {
    test_remove(cnt, cnt1);
  }
  else if (3 == type)
  {
    test_search(cnt);
  }
  else if (4 == type)
  {
    test_insert_batch(cnt);
  }
  else if (5 == type)
  {
    test_range_search(cnt);
  }
  else
  {
    fprintf(stderr, "%s -t type -c count [-n count_1] [-f 1.txt]\n", argv[0]);
    fprintf(stderr, "type error: %d\n", type);
  }
  delete btree;

  if (fp)
  {
    fclose(fp);
    fp = NULL;
  }

  return 0;
}
