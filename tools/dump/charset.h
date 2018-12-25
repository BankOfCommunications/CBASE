#ifndef __CHARSET_H__
#define __CHARSET_H__

#include <iconv.h>
#include <cstring>
#include "common/utility.h"
#include "common/ob_define.h"

//mod by zhuxh:20161011 [add class Charset]
//Entirely modified.

namespace oceanbase
{
namespace common
{

#define MAX_CHARSET_NAME_LENGTH 30
enum CodeConversionFlag {e_non,e_g2u,e_u2g};

//single object mulfunctional for multiple threads
class Charset
{
public:
  Charset();
  Charset(const char * from_charset, const char * to_charset);
  Charset(bool g2u);
  ~Charset();

  void init(const char * from_charset, const char * to_charset);
  void init(bool g2u);
  int get_errno();
  bool fail();

  //buff A to buff B, 2 buffs
  int code_convert(
    char *inbuf,  size_t & inlen,
    char *outbuf, size_t & outlen
  );
  //Convert locally, 1 buff
  int code_convert( char * buf, size_t capacity, size_t & len );

  bool is_part_gbk(char * str, int len);

  static int ctr;

private:
  Charset( const Charset & );
  Charset & operator = ( const Charset & );

  char from_charset[MAX_CHARSET_NAME_LENGTH];
  char to_charset[MAX_CHARSET_NAME_LENGTH];
  int _errno_;
  iconv_t cd;
};

}//namespace common
}//namespace oceanbase

#endif
