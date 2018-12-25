#include "test_utils2.h"

namespace oceanbase
{
  namespace test
  {
    bool check_buf(const char* buf, const int64_t len, int64_t pos)
    {
      return NULL != buf && 0 <= len && 0 <= pos && pos <= len;
    }

    char* sf(char* buf, const int64_t len, int64_t& pos, const char* format, va_list ap)
    {
      char* result = NULL;
      int64_t str_len = 0;
      if (!check_buf(buf, len, pos) || NULL == format)
      {}
      else if (0 > (str_len = vsnprintf(buf + pos, len - pos, format, ap)))
      {}
      else
      {
        result = buf + pos;
        pos += str_len + 1;
      }
      return result;
    }

    char* sf(char* buf, const int64_t len, int64_t& pos, const char* format, ...)
    {
      va_list ap;
      char* result = NULL;
      va_start(ap, format);
      result = sf(buf, len, pos, format, ap);
      va_end(ap);
      return result;
    }

    int sh(const char* cmd)
    {
      return NULL == cmd ? OB_INVALID_ARGUMENT: system(cmd);
    }

    char* popen(char* buf, const int64_t len, int64_t& pos, const char* cmd)
    {
      char* result = NULL;
      FILE* fp = NULL;
      int64_t count = 0;
      if (NULL == buf || 0 > len || 0 > pos || pos > len)
      {}
      else if (NULL == (fp = ::popen(cmd, "r")))
      {}
      else
      {
        result = buf + pos;
        count = fread(buf + pos, 1, len - pos - 1, fp);
        buf[pos += count] = 0;
      }
      return result;
    }

    ObDataBuffer& get_thread_buffer(ThreadSpecificBuffer& thread_buf, ObDataBuffer& buf)
    {
      ThreadSpecificBuffer::Buffer* my_buffer = thread_buf.get_buffer();
      my_buffer->reset();
      buf.set_data(my_buffer->current(), my_buffer->remain());
      return buf;
    }

    int rand_str(char* str, int64_t len)
    {
      int err = OB_SUCCESS;
      for(int64_t i = 0; i < len; i++)
      {
        str[i] = (char)('a' + (char)(random()%26));
      }
      str[len-1] = 0;
      return err;
    }

    int64_t get_file_len(const char* path)
    {
      int err = OB_SUCCESS;
      int64_t len = -1;
      struct stat stat_buf;
      if (NULL == path)
      {
        err = -EINVAL;
      }
      else if (0 != stat(path, &stat_buf))
      {
        err = -errno;
      }
      else
      {
        len = stat_buf.st_size;
      }
      return len;
    }
  }; // end namespace updateserver
}; // end namespace oceanbase
