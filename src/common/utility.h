/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ????.cpp is for what ...
 *
 *  Version: $Id: ipvsadm.c,v 1.27 2005/12/10 16:00:07 wensong Exp $
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 *        - some work details if you want
 */
#ifndef OCEANBASE_COMMON_UTILITY_H_
#define OCEANBASE_COMMON_UTILITY_H_

#include <stdint.h>
#include <execinfo.h>
#include <unistd.h>
#include "tbsys.h"
#include "easy_define.h"
#include "easy_define.h"
#include "easy_io_struct.h"
#include "ob_server.h"
#include "ob_object.h"
#include "ob_malloc.h"
#include "ob_allocator.h"
#define STR_BOOL(b) ((b) ? "true" : "false")

#define BACKTRACE(LEVEL, cond, _fmt_, args...) \
  do \
  { \
    if (cond) \
    { \
      void *buffer[100]; \
      int size = backtrace(buffer, 100); \
      char **strings = backtrace_symbols(buffer, size); \
      if (NULL != strings) \
      { \
        TBSYS_LOG(LEVEL, _fmt_ " BackTrace Start: ", ##args); \
        for (int i = 0; i < size; i++) \
        { \
          TBSYS_LOG(LEVEL, "BT[%d] @[%s]", i, strings[i]); \
        } \
        ::free(strings);                        \
      } \
    } \
  } while (false)

#define htonll(i) \
  ( \
  (((uint64_t)i & 0x00000000000000ff) << 56) | \
  (((uint64_t)i & 0x000000000000ff00) << 40) | \
  (((uint64_t)i & 0x0000000000ff0000) << 24) | \
  (((uint64_t)i & 0x00000000ff000000) << 8) | \
  (((uint64_t)i & 0x000000ff00000000) >> 8) | \
  (((uint64_t)i & 0x0000ff0000000000) >> 24) | \
  (((uint64_t)i & 0x00ff000000000000) >> 40) | \
  (((uint64_t)i & 0xff00000000000000) >> 56)   \
  )

#define DEFAULT_TIME_FORMAT "%Y-%m-%d %H:%M:%S"

#define DEFINE_HAS_MEMBER(member) \
    namespace oceanbase \
    { \
      namespace common \
      { \
        template <typename T, typename Sign> \
        struct __has_##member##__ \
        { \
            typedef char yes[1]; \
            typedef char no [2]; \
            \
            template <typename U, U> \
            struct type_check; \
            \
            template <typename _1> \
            static yes &chk(type_check<Sign, &_1::member> *); \
            \
            template <typename> \
            static no  &chk(...); \
            \
            static bool const value = sizeof(chk<T>(0)) == sizeof(yes); \
        }; \
      } \
    }

#define HAS_MEMBER(type, sign, member) \
  oceanbase::common::__has_##member##__<type, sign>::value

DEFINE_HAS_MEMBER(MAX_PRINTABLE_SIZE)
DEFINE_HAS_MEMBER(to_cstring)
DEFINE_HAS_MEMBER(reset)

namespace oceanbase
{
  namespace common
  {
    class ObCellInfo;
    class ObScanner;
    class ObRowkey;
    class ObVersionRange;
    class ObNewRange;
    //add wenghaixing [scondary index col checksum cal]20141210
    struct Token;
    //add e
    void hex_dump(const void* data, const int32_t size,
        const bool char_type = true, const int32_t log_level = TBSYS_LOG_LEVEL_DEBUG);
    int32_t parse_string_to_int_array(const char* line,
        const char del, int32_t *array, int32_t& size);
    int32_t hex_to_str(const void* in_data, const int32_t data_length, void* buff, const int32_t buff_size);
    int32_t str_to_hex(const void* in_data, const int32_t data_length, void* buff, const int32_t buff_size);
    //add wenghaixing [secondary index col checksum calculate] 20141210
    int transform_str_to_int(const char* data, const int64_t &dlen, uint64_t &value);
    int tokenize(const char *data, int64_t dlen, char delima, int &token_nr, Token *tokens);
    //add e
    int64_t lower_align(int64_t input, int64_t align);
    int64_t upper_align(int64_t input, int64_t align);
    bool is2n(int64_t input);
    bool all_zero(const char *buffer, const int64_t size);
    bool all_zero_small(const char *buffer, const int64_t size);
    const char* get_file_path(const char* file_dir, const char* file_name);
    char* str_trim(char *str);
    char* ltrim(char *str);
    char* rtrim(char *str);
    void databuff_printf(char *buf, const int64_t buf_len, int64_t& pos, const char* fmt, ...) __attribute__ ((format (printf, 4, 5)));
    template<class T>
    void databuff_print_obj(char *buf, const int64_t buf_len, int64_t& pos, const T &obj)
    {
      OB_ASSERT(pos >= 0 && pos <= buf_len);
      pos += obj.to_string(buf + pos, buf_len - pos);
    }
    const char *inet_ntoa_r(const uint64_t ipport);
    const char *inet_ntoa_r(easy_addr_t addr);
    const char* strtype(ObObjType type);
    void print_rowkey(FILE *fd, ObString &rowkey);
    void print_root_table(FILE* fd, ObScanner &scanner);
    const char *time2str(const int64_t time_s, const char *format = DEFAULT_TIME_FORMAT);

    int mem_chunk_serialize(char* buf, int64_t len, int64_t& pos, const char* data, int64_t data_len);
    int mem_chunk_deserialize(const char* buf, int64_t len, int64_t& pos, char* data, int64_t data_len, int64_t& real_len);
    int64_t min(const int64_t x, const int64_t y);
    int64_t max(const int64_t x, const int64_t y);

    int get_double_expand_size(int64_t &new_size, const int64_t limit_size);
    /**
     * allocate new memory that twice larger to store %oldp
     * @param oldp: old memory content.
     * @param old_size: old memory size.
     * @param limit_size: expand memory cannot beyond this limit.
     * @param new_size: expanded memory size.
     * @param allocator: memory allocator.
     */
    template <typename T, typename Allocator>
    int double_expand_storage(T *&oldp, const int64_t old_size,
        const int64_t limit_size, int64_t &new_size, Allocator& allocator)
    {
      int ret = OB_SUCCESS;
      new_size = old_size;
      void* newp = NULL;
      if (OB_SUCCESS != (ret = get_double_expand_size(new_size, limit_size)))
      {
      }
      else if (NULL == (newp = allocator.alloc(new_size * sizeof(T))))
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        memcpy(newp, oldp, old_size * sizeof(T));
        if (NULL != oldp) allocator.free(reinterpret_cast<char*>(oldp));
        oldp = reinterpret_cast<T*>(newp);
      }
      return ret;
    }

    template <typename T>
    int double_expand_storage(T *&oldp, const int64_t old_size,
        const int64_t limit_size, int64_t &new_size, const int32_t mod_id)
    {
      ObMalloc allocator;
      allocator.set_mod_id(mod_id);
      return double_expand_storage(oldp, old_size, limit_size, new_size, allocator);
    }

    easy_addr_t convert_addr_from_server(const ObServer *server);
    int64_t convert_addr_to_server(easy_addr_t addr);

    extern const char *print_role(const common::ObRole role);
    extern const char *print_obj(const common::ObObj &obj);
    extern const char *print_string(const common::ObString &str);
    extern const char *print_cellinfo(const common::ObCellInfo *ci, const char *ext_info = NULL);
    extern const char *range2str(const common::ObVersionRange &range);
    extern const char *scan_range2str(const common::ObNewRange &scan_range);
    extern bool str_isprint(const char *str, const int64_t length);
    extern int replace_str(char* src_str, const int64_t src_str_buf_size,
      const char* match_str, const char* replace_str);
    extern void dump_scanner(const common::ObScanner &scanner,
        const int32_t log_level = TBSYS_LOG_LEVEL_DEBUG, const int32_t type = 1);

    inline const char* get_peer_ip(easy_request_t *req)
    {
      static char mess[8] = "unknown";
      if (OB_LIKELY(NULL != req
                    && NULL != req->ms
                    && NULL != req->ms->c))
      {
        return inet_ntoa_r(req->ms->c->addr);
      }
      else
      {
        return mess;
      }
    }

    inline const char* get_peer_ip(easy_connection_t *c)
    {
      static char mess[8] = "unknown";
      if (OB_LIKELY(NULL != c))
      {
        return inet_ntoa_r(c->addr);
      }
      else
      {
        return mess;
      }
    }

    inline int get_fd(easy_request_t *req)
    {
      if (OB_LIKELY(NULL != req
                    && NULL != req->ms
                    && NULL != req->ms->c))
      {
        return req->ms->c->fd;
      }
      else
      {
        return -1;
      }
    }

    inline void init_easy_buf(easy_buf_t *buf, char* data, easy_request_t *req, uint64_t size)
    {
      if (NULL != buf && NULL != data)
      {
        buf->pos = data;
        buf->last = buf->pos;
        buf->end = buf->last + size;
        buf->cleanup = NULL;
        if (NULL != req && NULL != req->ms)
        {
          buf->args = req->ms->pool;
        }
        buf->flags = 0;
        easy_list_init(&buf->node);
      }
    }

    inline easy_addr_t get_easy_addr(easy_request_t *req)
    {
      static easy_addr_t empty = {0, 0, {0}, 0};
      if (OB_LIKELY(NULL != req
                    && NULL != req->ms
                    && NULL != req->ms->c))
      {
        return req->ms->c->addr;
      }
      else
      {
        return empty;
      }
    }

    template <typename T, bool NO_DEFAULT = true>
    struct PrintableSizeGetter
    {
      static const int64_t value = T::MAX_PRINTABLE_SIZE;
    };

    template <typename T>
    struct PrintableSizeGetter<T, false>
    {
      static const int64_t value = 16 * 1024;;
    };

    template <bool c>
    struct BoolType
    {
      static const bool value = c;
    };
    typedef BoolType<false> FalseType;
    typedef BoolType<true> TrueType;

    template <typename T>
    int64_t to_string(const T &obj, char *buffer, const int64_t buffer_size)
    {
      return obj.to_string(buffer, buffer_size);
    }
    template <>
    int64_t to_string<ObString>(const ObString &obj, char *buffer, const int64_t buffer_size);
    template <>
    int64_t to_string<int64_t>(const int64_t &obj, char *buffer, const int64_t buffer_size);

    template <typename T, int64_t BUFFER_NUM>
    const char* to_cstring(const T &obj)
    {
      static const int64_t BUFFER_SIZE = PrintableSizeGetter<T, HAS_MEMBER(T, const int64_t*, MAX_PRINTABLE_SIZE)>::value;
      static __thread char buffers[BUFFER_NUM][BUFFER_SIZE];
      static __thread uint64_t i = 0;
      char *buffer = buffers[i++ % BUFFER_NUM];
      memset(buffer, 0, BUFFER_SIZE);
      to_string(obj, buffer, BUFFER_SIZE);
      return buffer;
    }

    template <typename T>
    const char *to_cstring(const T &obj, FalseType)
    {
      // ATTENTION: u can call to_cstring 5 times at most in one log
      return to_cstring<T, 5>(obj);
    }

    template <typename T>
    const char *to_cstring(const T &obj, TrueType)
    {
      return obj.to_cstring();
    }

    template <typename T>
    const char* to_cstring(const T &obj)
    {
      return to_cstring(obj, BoolType<HAS_MEMBER(T, const char* (T::*)() const, to_cstring)>());
    }

    template <>
    const char* to_cstring<const char*>(const char* const& str);
    template <>
    const char* to_cstring<int64_t>(const int64_t &v);

    template <typename Allocator>
      int deep_copy_ob_string(Allocator& allocator, const ObString& src, ObString& dst)
      {
        int ret = OB_SUCCESS;
        char *ptr = NULL;
        if (NULL == src.ptr() || 0 >= src.length())
        {
          dst.assign_ptr(NULL, 0);
        }
        else if (NULL == (ptr = reinterpret_cast<char*>(allocator.alloc(src.length()))))
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          memcpy(ptr, src.ptr(), src.length());
          dst.assign(ptr, src.length());
        }
        return ret;
      }

    template <typename Allocator>
      int deep_copy_ob_obj(Allocator& allocator, const ObObj& src, ObObj& dst)
      {
        int ret = OB_SUCCESS;
        dst = src;
        if (ObVarcharType == src.get_type())
        {
          ObString value;
          ObString new_value;
          if (OB_SUCCESS != ( ret = src.get_varchar(value)))
          {
            TBSYS_LOG(WARN, "get_varchar failed, obj=%s", to_cstring(src));
          }
          else if (OB_SUCCESS != (ret = deep_copy_ob_string(allocator, value, new_value)))
          {
            TBSYS_LOG(WARN, "deep_copy_ob_string failed, obj=%s", to_cstring(src));
          }
          else
          {
            dst.set_varchar(new_value);
          }
        }
        return ret;
      }

    struct SeqLockGuard
    {
      SeqLockGuard(volatile uint64_t& seq): seq_(seq)
      {
        uint64_t tmp_seq = 0;
        do {
          tmp_seq = seq_;
        } while((tmp_seq & 1) || !__sync_bool_compare_and_swap(&seq_, tmp_seq, tmp_seq + 1));
      }
      ~SeqLockGuard()
      {
        __sync_synchronize();
        seq_++;
        __sync_synchronize();
      }
      volatile uint64_t& seq_;
    };
    struct OnceGuard
    {
      OnceGuard(volatile uint64_t& seq): seq_(seq), locked_(false)
      {
      }
      ~OnceGuard()
      {
        if (locked_)
        {
          __sync_synchronize();
          seq_++;
        }
      }
      bool try_lock()
      {
        uint64_t cur_seq = 0;
        locked_ = (0 == ((cur_seq = seq_) & 1)) && __sync_bool_compare_and_swap(&seq_, cur_seq, cur_seq + 1);
        return locked_;
      }
      volatile uint64_t& seq_;
      bool locked_;
    };

    struct CountReporter
    {
      CountReporter(const char* id, int64_t report_mod):
        id_(id), seq_lock_(0), report_mod_(report_mod),
        count_(0), start_ts_(0),
        last_report_count_(0), last_report_time_(0) {}
      ~CountReporter() {
        TBSYS_LOG(INFO, "%s=%ld", id_, count_);
      }
      void inc() {
        int64_t count = __sync_add_and_fetch(&count_, 1);
        if (0 == (count % report_mod_))
        {
          SeqLockGuard lock_guard(seq_lock_);
          int64_t cur_ts = tbsys::CTimeUtil::getTime();
          TBSYS_LOG(INFO, "%s=%ld:%ld\n", id_, count, 1000000 * (count - last_report_count_)/(cur_ts - last_report_time_));
          last_report_count_ = count;
          last_report_time_ = cur_ts;
        }
      }
      const char* id_;
      uint64_t seq_lock_;
      int64_t report_mod_;
      int64_t count_;
      int64_t start_ts_;
      int64_t last_report_count_;
      int64_t last_report_time_;
    };

    inline int64_t get_cpu_num()
    {
      return sysconf(_SC_NPROCESSORS_ONLN);
    }

    inline int64_t get_phy_mem_size()
    {
      return sysconf(_SC_PAGE_SIZE) * sysconf(_SC_PHYS_PAGES);
    }

    inline void ob_set_err_msg(const char* format, ...)
    {
      va_list ap;
      const int64_t buf_size = 1024;
      char buf[buf_size];
      va_start(ap, format);
      vsnprintf(buf, buf_size, format, ap);
      va_end(ap);
      tbsys::WarningBuffer *wb = tbsys::get_tsi_warning_buffer();
      if (OB_LIKELY(NULL != wb))
      {
        wb->set_err_msg(buf);
      }
    }

    inline void ob_reset_err_msg()
    {
      tbsys::WarningBuffer *wb = tbsys::get_tsi_warning_buffer();
      if (OB_LIKELY(NULL != wb))
      {
        wb->set_err_msg("");
      }
    }

    inline const ObString ob_get_err_msg(void)
    {
      ObString ret;
      tbsys::WarningBuffer *wb = tbsys::get_tsi_warning_buffer();
      if (OB_LIKELY(NULL != wb))
      {
        ret = ObString::make_string(wb->get_err_msg());
      }
      return ret;
    }

    inline const char *str_dml_type(const ObDmlType i)
    {
      const char *ret = "overflow";
      static const char *str[] = {"unknow", "replace", "insert", "update", "delete"};
      if (0 <= i && i < (int)sizeof(str))
      {
        ret = str[i];
      }
      return ret;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////
    
    template <class T,
              uint64_t N_THREAD = 1024>
    class ObTSIArray
    {
      struct ThreadNode
      {
        T v;
      } CACHE_ALIGNED;
      public:
        ObTSIArray() : thread_num_(0)
        {
          int tmp_ret = pthread_key_create(&key_, NULL);
          if (0 != tmp_ret)
          {
            TBSYS_LOG(ERROR, "pthread_key_create fail ret=%d", tmp_ret);
          }
        };
        ~ObTSIArray()
        {
          pthread_key_delete(key_);
        };
      public:
        T &get_tsi()
        {
          volatile ThreadNode* thread_node = (volatile ThreadNode*)pthread_getspecific(key_);
          if (NULL == thread_node)
          {
            thread_node = &array_[__sync_fetch_and_add(&thread_num_, 1) % N_THREAD];
            int tmp_ret = pthread_setspecific(key_, (void*)thread_node);
            if (0 != tmp_ret)
            {
              TBSYS_LOG(ERROR, "pthread_setspecific fail ret=%d", tmp_ret);
            }
          }
          return (T&)(thread_node->v);
        };
        const T &at(const uint64_t i) const
        {
          return (const T&)array_[i % N_THREAD].v;
        };
        T &at(const uint64_t i)
        {
          return (T&)array_[i % N_THREAD].v;
        };
        uint64_t get_thread_num() const
        {
          return thread_num_;
        };
        uint64_t get_thread_num()
        {
          return thread_num_;
        };
      private:
        pthread_key_t key_                    CACHE_ALIGNED;
        volatile uint64_t thread_num_         CACHE_ALIGNED;
        volatile ThreadNode array_[N_THREAD]  CACHE_ALIGNED;
    };

#define REACH_TIME_INTERVAL(i) \
  ({ \
    bool bret = false; \
    static volatile int64_t last_time = 0; \
    int64_t cur_time = tbsys::CTimeUtil::getTime(); \
    int64_t old_time = last_time; \
    if ((i + last_time) < cur_time \
        && old_time == ATOMIC_CAS(&last_time, old_time, cur_time)) \
    { \
      bret = true; \
    } \
    bret; \
  })

// exclusive first time
#define REACH_TIME_INTERVAL_RANGE(i, j) \
  ({ \
    bool bret = false; \
    static volatile int64_t last_time = tbsys::CTimeUtil::getTime(); \
    int64_t cur_time = tbsys::CTimeUtil::getTime(); \
    int64_t old_time = last_time; \
    if ((j + last_time) < cur_time) \
    { \
      ATOMIC_CAS(&last_time, old_time, cur_time); \
    } \
    old_time = last_time; \
    if ((i + last_time) < cur_time \
        && old_time == ATOMIC_CAS(&last_time, old_time, cur_time)) \
    { \
      bret = true; \
    } \
    bret; \
  })

#define TC_REACH_TIME_INTERVAL(i) \
  ({ \
    bool bret = false; \
    static __thread int64_t last_time = 0; \
    int64_t cur_time = tbsys::CTimeUtil::getTime(); \
    if ((i + last_time) < cur_time) \
    { \
      last_time = cur_time; \
      bret = true; \
    } \
    bret; \
  })

#define REACH_COUNT_INTERVAL(i) \
  ({ \
    bool bret = false; \
    static volatile int64_t count = 0; \
    if (0 == (ATOMIC_ADD(&count, 1) % i)) \
    { \
      bret = true; \
    } \
    bret; \
  })

#define TC_REACH_COUNT_INTERVAL(i) \
  ({ \
    bool bret = false; \
    static __thread int64_t count = 0; \
    if (0 == (++count % i)) \
    { \
      bret = true; \
    } \
    bret; \
  })

  } // end namespace common
} // end namespace oceanbase


#endif //OCEANBASE_COMMON_UTILITY_H_
