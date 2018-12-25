#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include "ob_malloc.h"
#include "ob_file.h"

#include "gtest/gtest.h"

using namespace oceanbase;
using namespace common;

static char *fname = (char*)"./test_file.data";
static char *async_fname = (char*)"./test_file.async.data";
static char *sync_fname = (char*)"./test_file.sync.data";
static char *buf = NULL;
static const int64_t size = 16 * 1024 * 1024;
static const int64_t mb_num = 16;
static const int64_t byte_num = 1024 * 1024;
static int64_t seq = 1;
static int64_t total_size = 0;

class FileBuffer : public IFileBuffer
{
  public:
    FileBuffer() : buffer_(NULL), base_(0)
    {
    };
    ~FileBuffer()
    {
      if (NULL != buffer_)
      {
        ::free(buffer_);
        buffer_ = NULL;
      }
    };
  public:
    char *get_buffer()
    {
      return buffer_;
    };
    int64_t get_base_pos()
    {
      return base_;
    };
    void set_base_pos(const int64_t pos)
    {
      base_ = pos;
    };
    int assign(const int64_t size, const int64_t align)
    {
      if (NULL != buffer_)
      {
        ::free(buffer_);
        buffer_ = NULL;
      }
      buffer_ = (char*)::memalign(align, size);
      return OB_SUCCESS;
    };
    int assign(const int64_t size)
    {
      if (NULL != buffer_)
      {
        ::free(buffer_);
        buffer_ = NULL;
      }
      buffer_ = (char*)::malloc(size);
      return OB_SUCCESS;
    };
  private:
    char *buffer_;
    int64_t base_;
};

void init()
{
  seq = 1;
  total_size = 0;
}

int64_t build_buf(const bool large)
{
  if (NULL == buf)
  {
    buf = (char*)malloc(size);
  }

  int64_t mb_size = large ? rand() % mb_num : 0;
  int64_t byte_size = rand() % byte_num;
  byte_size = upper_align(byte_size, 8);
  int64_t buf_size = mb_size * 1024 * 1024 + byte_size;
  for (int64_t i = 0; i < buf_size / 8; i++)
  {
    *((int64_t*)&buf[i * 8]) = seq++;
    if (1310210 == *((int64_t*)&buf[i * 8]))
    {
      fprintf(stderr, "\n");
    }
  }
  total_size += buf_size;
  return buf_size;
}

void release_buf()
{
  if (NULL != buf)
  {
    free(buf);
    buf = NULL;
  }
}

TEST(TestObFile, direct)
{
  init();
  ObFileAppender appender;
  ObFileReader reader;

  ObString str_fname;
  str_fname.assign(fname, static_cast<int32_t>(strlen(fname)));

  int ret = appender.open(str_fname, true, true, true);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = reader.open(str_fname, true);
  EXPECT_EQ(OB_SUCCESS, ret);

  int64_t size = build_buf(true);
  fprintf(stderr, "buf_size=%ld\n", size);
  ret = appender.append(buf, size, true);
  EXPECT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < mb_num; i++)
  {
    size = build_buf(false);
    fprintf(stderr, "write_buf_size=%ld\n", size);
    ret = appender.append(buf, size, false);
    EXPECT_EQ(OB_SUCCESS, ret);
    ret = appender.append(buf, -1, false);
    EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
    ret = appender.append(buf, 0, false);
    EXPECT_EQ(OB_SUCCESS, ret);
    ret = appender.append(buf, 0, true);
    EXPECT_EQ(OB_SUCCESS, ret);
    ret = appender.append(NULL, 0, false);
    EXPECT_EQ(OB_SUCCESS, ret);
  }

  appender.close();

  EXPECT_EQ(total_size, get_file_size(fname));

  int64_t offset = 0;
  int64_t check = 1;
  ObFileBuffer fb;
  while (offset < total_size)
  {
    size = rand() % byte_num;
    size = upper_align(size, 8);
    int64_t read_ret = 0;

    ret = reader.pread(buf, -1, offset, read_ret);
    EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
    ret = reader.pread(-1, offset, fb, read_ret);
    EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
    ret = reader.pread(buf, 0, offset, read_ret);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(0, read_ret);
    ret = reader.pread(NULL, 0, offset, read_ret);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(0, read_ret);
    ret = reader.pread(0, offset, fb, read_ret);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(0, read_ret);

    ret = reader.pread(buf, size, offset, read_ret);
    EXPECT_EQ(OB_SUCCESS, ret);
    int64_t read_ret2 = 0;
    ret = reader.pread(size, offset, fb, read_ret2);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(read_ret, read_ret2);
    EXPECT_EQ(0, memcmp(buf, fb.get_buffer() + fb.get_base_pos(), read_ret));
    fprintf(stderr, "read_buf_size=%ld\n", read_ret);
    offset += read_ret;
    for (int64_t i = 0; i < read_ret / 8; i++)
    {
      EXPECT_EQ(check++, *((int64_t*)&(buf[i * 8])));
    }
  }
  EXPECT_EQ(check, seq);

  release_buf();
}

TEST(TestObFile, normal)
{
  init();
  ObFileAppender appender;
  ObFileReader reader;

  ObString str_fname;
  str_fname.assign(fname, static_cast<int32_t>(strlen(fname)));

  int ret = appender.open(str_fname, false, true, true);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = reader.open(str_fname, false);
  EXPECT_EQ(OB_SUCCESS, ret);

  int64_t size = build_buf(true);
  fprintf(stderr, "buf_size=%ld\n", size);
  ret = appender.append(buf, size, true);
  EXPECT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < mb_num; i++)
  {
    size = build_buf(false);
    fprintf(stderr, "write_buf_size=%ld\n", size);
    ret = appender.append(buf, size, false);
    EXPECT_EQ(OB_SUCCESS, ret);
    //ret = appender.append(buf, -1, false);
    //EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
    //ret = appender.append(buf, 0, false);
    //EXPECT_EQ(OB_SUCCESS, ret);
    //ret = appender.append(buf, 0, true);
    //EXPECT_EQ(OB_SUCCESS, ret);
    //ret = appender.append(NULL, 0, false);
    //EXPECT_EQ(OB_SUCCESS, ret);
  }

  sleep(10);
  appender.close();

  EXPECT_EQ(total_size, get_file_size(fname));

  int64_t offset = 0;
  int64_t check = 1;
  ObFileBuffer fb;
  while (offset < total_size)
  {
    size = rand() % byte_num;
    size = upper_align(size, 8);
    int64_t read_ret = 0;

    ret = reader.pread(buf, -1, offset, read_ret);
    EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
    ret = reader.pread(-1, offset, fb, read_ret);
    EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
    ret = reader.pread(buf, 0, offset, read_ret);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(0, read_ret);
    ret = reader.pread(NULL, 0, offset, read_ret);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(0, read_ret);
    ret = reader.pread(0, offset, fb, read_ret);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(0, read_ret);

    ret = reader.pread(buf, size, offset, read_ret);
    EXPECT_EQ(OB_SUCCESS, ret);
    int64_t read_ret2 = 0;
    ret = reader.pread(size, offset, fb, read_ret2);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(read_ret, read_ret2);
    EXPECT_EQ(0, memcmp(buf, fb.get_buffer() + fb.get_base_pos(), read_ret));
    fprintf(stderr, "read_buf_size=%ld\n", read_ret);
    offset += read_ret;
    for (int64_t i = 0; i < read_ret / 8; i++)
    {
      EXPECT_EQ(check++, *((int64_t*)&(buf[i * 8])));
      //assert(check++ == *((int64_t*)&(buf[i * 8])));
    }
  }
  EXPECT_EQ(check, seq);

  release_buf();
}

TEST(TestObFile, append_bounder)
{
  ObFileAppender appender;
  ObString str_fname;
  str_fname.assign(fname, static_cast<int32_t>(strlen(fname) + 1));
  appender.open(str_fname, true, true, true);
  char *buf = (char*)malloc(2 * 1024 * 1024);
  int64_t size = 2 * 1024 * 1024 - 8 * 1024;
  appender.append(buf, size, false);
  size = 8 * 1024 + 1023;
  appender.append(buf, size, false);
  size = 2 * 1024 * 1024 - 1;
  appender.append(buf, size, false);
  EXPECT_EQ(4 * 1024 * 1024 + 1022, appender.get_file_pos());
  appender.close();
  free(buf);
}

TEST(TestObFile, async_append_bounder)
{
  ObFileAsyncAppender appender;
  ObString str_fname;
  str_fname.assign(async_fname, static_cast<int32_t>(strlen(async_fname) + 1));
  appender.open(str_fname, true, true, true);
  char *buf = (char*)malloc(70 * 1024 * 1024);
  memset(buf, '$', 70 * 1024 * 1024);
  int64_t size = 2 * 1024 * 1024 - 8 * 1024;
  appender.append(buf, size, false);
  size = 8 * 1024 + 1023;
  appender.append(buf, size, false);
  size = 2 * 1024 * 1024 - 1;
  appender.append(buf, size, false);
  size = 64 * 1024 * 1024;
  appender.append(buf, size, false);
  appender.fsync();
  EXPECT_EQ(68 * 1024 * 1024 + 1022, appender.get_file_pos());
  appender.close();
  free(buf);
}

TEST(TestObFile, sync_append_bounder)
{
  ObFileAppender appender;
  ObString str_fname;
  str_fname.assign(sync_fname, static_cast<int32_t>(strlen(sync_fname) + 1));
  appender.open(str_fname, true, true, true);
  char *buf = (char*)malloc(70 * 1024 * 1024);
  memset(buf, '$', 70 * 1024 * 1024);
  int64_t size = 2 * 1024 * 1024 - 8 * 1024;
  appender.append(buf, size, false);
  size = 8 * 1024 + 1023;
  appender.append(buf, size, false);
  size = 2 * 1024 * 1024 - 1;
  appender.append(buf, size, false);
  size = 64 * 1024 * 1024;
  appender.append(buf, size, false);
  appender.fsync();
  EXPECT_EQ(68 * 1024 * 1024 + 1022, appender.get_file_pos());
  appender.close();
  free(buf);
}

class FileInfo : public IFileInfo
{
  public:
    FileInfo(const char *fname) : fd_(-1)
    {
      fd_ = open(fname, O_RDONLY | O_DIRECT);
    };
    ~FileInfo()
    {
      if (-1 != fd_)
      {
        close(fd_);
        fd_ = -1;
      }
    };
  public:
    int get_fd() const
    {
      return fd_;
    };
  private:
    int fd_;
};

void test(const char *fname)
{
  char *buffer = new char[2 * 1024 * 1024];
  int64_t timeu = tbsys::CTimeUtil::getTime();
  //int fd = open("./test.data", O_WRONLY | O_TRUNC, S_IRWXU);
  //fprintf(stderr, "open timeu=%ld\n", tbsys::CTimeUtil::getTime() - timeu);
  //for (int64_t i = 0; i < 80; i++)
  //{
  //  write(fd, buffer, 2 * 1024 * 1024);
  //}
  //close(fd);
  //fprintf(stderr, "write timeu=%ld\n", tbsys::CTimeUtil::getTime() - timeu);

  FileInfo fi(fname);
  fprintf(stderr, "open timeu=%ld\n", tbsys::CTimeUtil::getTime() - timeu);
  ObFileBuffer fbuffer;
  ObFileReader::read_record(fi, 2 * 1024 * 1024 - 419, 419, fbuffer);
  fprintf(stderr, "read pos=%ld timeu=%ld\n", fbuffer.get_base_pos(), tbsys::CTimeUtil::getTime() - timeu);
  delete[] buffer;
  exit(0);
}

int main(int argc, char **argv)
{
  //test(argv[1]);
  TBSYS_LOGGER.setLogLevel("debug");
  int tm = static_cast<int32_t>(time(NULL));
  srand(tm);
  fprintf(stderr, "seed=%d\n", tm);
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv); 
  return RUN_ALL_TESTS();
}
