#ifndef __OB_OBCON_UTILS_H__
#define __OB_OBCON_UTILS_H__

#include "common/ob_string.h"
#include "common/ob_object.h"
#include "common/ob_server.h"
#include "common/ob_scanner.h"
#include "common/ob_schema.h"
#if ROWKEY_IS_OBJ
#include "common/ob_rowkey.h"
#else
typedef oceanbase::common::ObString ObRowkey;
typedef int64_t ObRowkeyInfo;
class ObIAllocator
{
  public:
    virtual ~ObIAllocator() {};
  public:
    virtual void *alloc(const int64_t sz) = 0;
    virtual void free(void *ptr) = 0;
};
#endif

using namespace oceanbase::common;
#define array_len(A) (sizeof(A)/sizeof(A[0]))
#define is_env_set(key, _default) (0 == (strcmp("true", getenv(key)?: _default)))
#define _cfg(k, v) getenv(k)?:v
#define _cfgi(k, v) atoll(getenv(k)?:v)

#define MAX_STR_BUF_SIZE 1024

template<typename T>
T max(T x, T y)
{
  return x > y? x: y;
}

template<typename T>
T min(T x, T y)
{
  return x > y? y: x;
}

inline const char* str_bool(const bool b)
{
  return b? "true": "false";
}

inline bool check_buf(const char* buf, const int64_t len, const int64_t pos)
{
  return NULL != buf && 0 <= len && 0 <= pos && pos <= len;
}

const char* obj_type_repr(const ObObjType _type);
int to_obj(ObObj& obj, const int64_t v);
int to_obj(ObObj& obj, const ObString& v);
int to_obj(ObObj& obj, const char* v);

int strformat(char* buf, const int64_t len, int64_t& pos, const char* format, ...)
  __attribute__ ((format (printf, 4, 5)));
int strformat(ObDataBuffer& buf, const char* format, ...)
  __attribute__ ((format (printf, 2, 3)));
int bin2hex(char* buf, const int64_t len, int64_t& pos, ObString& str, const char* data, const int64_t size);
int hex2bin(char* buf, const int64_t len, int64_t& pos, ObString& str, const char* data, const int64_t size);
int bin2hex(ObDataBuffer& buf, ObString& str, const char* data, const int64_t size);
int hex2bin(ObDataBuffer& buf, ObString& str, const char* data, const int64_t size);

int split(char* buf, const int64_t len, int64_t& pos, const char* str, const char* delim,
          int max_n_secs, int& n_secs, const char** secs);
int split(ObDataBuffer& buf, const char* str, const char* delim, const int max_n_secs, int& n_secs, char** const secs);
int repr(char* buf, const int64_t len, int64_t& pos, const char* value);
int repr(char* buf, const int64_t len, int64_t& pos, const ObObj& value);
int repr(char* buf, const int64_t len, int64_t& pos, const  ObString& _str);
int repr(char* buf, const int64_t len, int64_t& pos, const ObRowkey& rowkey);
int repr(char* buf, const int64_t len, int64_t& pos, const ObServer& server);
int repr(char* buf, const int64_t len, int64_t& pos, ObScanner& scanner, int64_t row_limit=-1);
int obstring2cstr(char* buf, const int64_t len, int64_t& pos, const char*& dest, ObString& src);
int copy_str(char* buf, const int64_t len, int64_t& pos, char*& str, const char* _str);
int alloc_str(char* buf, const int64_t len, int64_t& pos, ObString& str, const char* _str);
int alloc_str(char* buf, const int64_t len, int64_t& pos, ObString& str, const ObString _str);
int to_server(ObServer& server, const char* spec);
int to_server(ObServer& server, const ObServer server2);
int parse_range(char* buf, const int64_t len, int64_t& pos, const char* _range, char*& start, char*& end);
int parse_servers(const char* tablet_servers, const int max_n_servers, int& n_servers, ObServer *servers);
int parse_range(ObDataBuffer& buf, const char* _range, char*& start, char*& end);
int parse_rowkey(ObDataBuffer& buf, ObRowkey& rowkey, const char* str, const int64_t len);
int rand_str(char* str, int64_t len, int64_t seed = 0);
int rand_obj(ObDataBuffer& buf, ObObjType type, ObObj& value, int64_t seed = 0);
int rand_obj(ObDataBuffer& buf, ObObjType type, int64_t size, ObObj& value, int64_t seed = 0);
int rand_rowkey(ObDataBuffer& buf, ObRowkey& rowkey, const ObRowkeyInfo& rowkey_info, const int64_t seed);
int expand(char* buf, int64_t len, int64_t& pos, const char* spec_);
const char* choose(const char* spec, int64_t seed);

class SimpleBufAllocator:public ObIAllocator
{
  public:
    SimpleBufAllocator(): buf_(NULL) {}
    SimpleBufAllocator(ObDataBuffer& buf): buf_(&buf) {}
    ~SimpleBufAllocator() {}
    void set_buf(ObDataBuffer* buf) { buf_ = buf; }
    void free(void* p) { UNUSED(p); }
    void set_mod_id(int32_t mod_id) {UNUSED(mod_id);};
    void* alloc(const int64_t size){
      void* p = NULL;
      if (NULL == buf_)
      {
        TBSYS_LOG(ERROR, "buf_ == NULL");
      }
      else if (buf_->get_remain() < size)
      {
        TBSYS_LOG(ERROR, "buf.alloc(remain=%ld, size=%ld):MEM_OVERFLOW", buf_->get_remain(), size);
      }
      else
      {
        p = buf_->get_data() + buf_->get_position();
        buf_->get_position() += size;
      }
      return p;
    }
  private:
    ObDataBuffer* buf_;
};

template<typename T>
int repr(ObDataBuffer& buf, T& obj)
{
  return repr(buf.get_data(), buf.get_capacity(), buf.get_position(), obj);
}

template<typename T>
int alloc_str(ObDataBuffer& buf, ObString& str, T value)
{
  return alloc_str(buf.get_data(), buf.get_capacity(), buf.get_position(), str, value);
}

int64_t rand2(int64_t key);

typedef struct _rng_t {
  uint64_t s[3];
} rng_t;

void init_rng64(rng_t* state, uint64_t x);
uint64_t rng64(rng_t *_s);

#define reg_parse2(pat, str, buf, ...) (__reg_parse(pat, str, buf.get_data(), buf.get_capacity(), buf.get_position(), ##__VA_ARGS__, NULL) != 0? OB_PARTIAL_FAILED: 0)
#endif /* __OB_OBCON_UTILS_H__ */
