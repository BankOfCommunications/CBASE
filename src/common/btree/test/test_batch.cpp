#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <key_btree.h>
#include <test_key.h>
#include <signal.h>

using namespace oceanbase;
using namespace common;

int32_t max_id = 1000;
typedef KeyBtree<CollectInfo, int32_t> StringBtree;
int m_stop = 0;
int m_clear = 0;
int m_balloc = 0;
int m_type[10];
int64_t m_read_cnt = 0;
int64_t m_read_cnt1 = 0;
int64_t m_read_cnt2 = 0;
int64_t m_write_cnt = 0;
int64_t m_start_time = 0;
BtreeDefaultAlloc key_allocator;
typedef struct alist
{
  struct alist *next;
} alist;

void *m_thread_write(void *args)
{
  StringBtree *btree = reinterpret_cast<StringBtree*>(args);
  CollectInfo key;

  // put key
  {
    fprintf(stderr, "batch insert.\n");
    int64_t t = 0;
    int ecnt = 10000;
    int loop = max_id / ecnt;
    CollectInfo key;
    for(int i = 0; i < loop; i++)
    {
      BtreeWriteHandle handle;
      btree->get_write_handle(handle);
      for(int j = 0; j < ecnt; j++)
      {
        key.set_value(t++);
        btree->put(handle, key, t);
      }
      m_write_cnt += ecnt;
    }
    fprintf(stderr, "batch insert end=%ld\n", t);
    m_write_cnt = 0;
  }

  int t = 0;
  int r = 0;
  while(m_stop == 0)
  {
    t = rand() % max_id;
    r = t % 13;

    key.set_value(t);
    if (m_type[0] && r < 4)
    {
      btree->put(key, t + 1);
    }
    else if (m_type[1] && t < 6)
    {
      btree->remove(key);
    }
    else if (m_type[2] && t < 9)
    {
      BtreeWriteHandle handle;
      btree->get_write_handle(handle);
      btree->put(handle, key, t + 1);
      if (t % 11 == 0) handle.rollback();
    }
    else if (m_type[3] && t < 13)
    {
      BtreeWriteHandle handle;
      btree->get_write_handle(handle);
      {
        BtreeWriteHandle *h1 = handle.get_write_handle();
        btree->put(*h1, key, t + 1);
        if (t % 17 == 0) h1->rollback();
      }
      if (t % 11 == 0) handle.rollback();
    }
    else if (m_type[0] + m_type[1] + m_type[2] + m_type[3] == 0)
    {
      m_write_cnt --;
      usleep(1);
    }
    if (m_clear || t < m_type[6])
    {
      btree->clear();
      m_clear = 0;
      //fprintf(stderr, "CLEAR!!: %ld\n", m_write_cnt);
    }

    if (m_balloc)
    {
      int64_t fc1 = 0, fc = key_allocator.get_free_count();
      alist *list = NULL;
      alist *p = NULL;
      for(int64_t i = 0; i < fc - 1; i++)
      {
        p = (alist*)key_allocator.alloc();
        p->next = list;
        list = p;
      }
      fprintf(stderr, "ALLOC: freecnt: %ld\n", key_allocator.get_free_count());
      while(list)
      {
        p = list->next;
        key_allocator.release((char*)list);
        list = p;
        fc1 ++;
      }
      fprintf(stderr, "ALLOC: freecnt: %ld,%ld,%ld\n", fc, fc1, key_allocator.get_free_count());
      m_balloc = 0;
    }
    m_write_cnt ++;
  }
  return (void*) NULL;
}

int sp(int64_t cnt, int64_t t)
{
  if(t <= 0) return 0;
  cnt *= 1000000;
  return (cnt / t);
}

void *m_thread_stat(void *args)
{
  StringBtree *btree = reinterpret_cast<StringBtree*>(args);
  int64_t t1, t0 = getTime();
  int64_t n1, n2, n3, n5, t, tt;
  int64_t e1 = 0, e2 = 0, e3 = 0, e5 = 0;

  while(m_stop == 0)
  {
    sleep(10);
    t1 = getTime();
    n1 = m_read_cnt;
    n2 = m_read_cnt1;
    n5 = m_read_cnt2;
    n3 = m_write_cnt;
    t = t1 - t0;
    tt = t1 - m_start_time;

    fprintf(stderr, "rd:%ld(%d,%d), range:%ld(%d,%d)(%ld,%d,%d) wr:%ld(%d,%d) obj:%ld key:%ld node:%d alloc:%ld mem:%d, fl:%d\n",
            n1, sp(n1 - e1, t), sp(n1, tt),
            n2, sp(n2 - e2, t), sp(n2, tt),
            n5, sp(n5 - e5, t), sp(n5, tt),
            n3, sp(n3 - e3, t), sp(n3, tt),
            btree->get_object_count(), key_allocator.get_use_count(), btree->get_node_count(),
            btree->get_alloc_count(), btree->get_alloc_memory(), btree->get_freelist_size());
    t0 = t1;
    e1 = n1;
    e2 = n2;
    e3 = n3;
    e5 = n5;
    if (btree->get_object_count() >= max_id - 10) m_clear = 1;
  }
  return (void*) NULL;
}

void *m_thread_read(void *args)
{
  StringBtree *btree = reinterpret_cast<StringBtree*>(args);
  CollectInfo key, tokey;
  int32_t value = 0;
  int t = 0;
  int r = 0;
  int cnt = 0;
  int cnt1 = 0;
  int cnt2 = 0;

  while(m_stop == 0)
  {
    t = rand() % max_id;
    r = t % 20;
    if (m_type[4] && r < 1)
    {
      BtreeReadHandle handle;
      btree->get_read_handle(handle);
      key.set_value(t);
      if (t % 2)
        tokey.set_value(t + t % 100);
      else
        tokey.set_value(t - t % 100);

      btree->set_key_range(handle, &key, 0, &tokey, 0);
      while(ERROR_CODE_OK == btree->get_next(handle, value))
      {
        cnt2 ++;
        if (cnt2 > 2000) break;
      }
      atomic_add_return(cnt2, &m_read_cnt2);
      cnt2 = 0;
      cnt1 ++;
    }
    else if (m_type[5])
    {
      key.set_value(t);
      btree->get(key, value);
      cnt ++;
    }
    else
    {
      usleep(1);
    }
    if ((cnt + cnt1) > 1000)
    {
      atomic_add_return(cnt, &m_read_cnt);
      atomic_add_return(cnt1, &m_read_cnt1);
      cnt = cnt1 = cnt2 = 0;
    }
  }
  return (void*) NULL;
}

StringBtree btree(COLLECTINFO_KEY_MAX_SIZE, &key_allocator, NULL);
void dump(int sig)
{
  if (sig == 3)
  {
    m_clear = 1;
  }
  else if (sig == 40)
  {
    FILE *fp = fopen("mtype.txt", "rb");
    if (fp)
    {
      int v = 0;
      for(int i = 0; i < 7; i++)
      {
        if(fscanf(fp, "%d ", &v) == 1) m_type[i] = v;
      }
      fclose(fp);
    }
    for(int i = 0; i < 7; i++)
    {
      fprintf(stderr, "mtype[%d]=%d\n", i, m_type[i]);
    }
  }
  else if (sig == 50)
  {
    m_balloc = 1;
  }
  else if (sig == 6)
  {
    fflush(stderr);
  }
  fprintf(stderr, "signal: %d clear:%d %d\n", sig, m_clear, m_balloc);
}
int main(int argc, char *argv[])
{
  //char BUFFER[1024*1024*2];
  //setbuf(stderr, BUFFER);
  srand(time(NULL));
  signal(3, dump);
  signal(6, dump);
  signal(40, dump);
  signal(50, dump);
  int rc = 1;
  int rw = 1;
  int cnt = 0;

  for(int i = 0; i < 10; i++) m_type[i] = 1;
  if (argc > 1) max_id = atoi(argv[1]);
  if (argc > 2) rc = atoi(argv[2]);
  fprintf(stderr, "ReadThreadCount:%d, WriteThreadCount:%d\n", rc, rw);

  m_start_time = getTime();
  pthread_t tids[rc + rw + 1];
  if (rw > 1) btree.set_write_lock_enable(1);
  pthread_create(&tids[cnt ++], NULL, m_thread_stat, &btree);

  m_start_time = getTime();
  while(cnt < rc + 1)
  {
    pthread_create(&tids[cnt], NULL, m_thread_read, &btree);
    cnt ++;
  }
  while(cnt < rc + rw + 1)
  {
    pthread_create(&tids[cnt], NULL, m_thread_write, &btree);
    cnt ++;
  }
  for(int i = 0; i < cnt; i++)
  {
    pthread_join(tids[i], NULL);
  }

  return 0;
}

