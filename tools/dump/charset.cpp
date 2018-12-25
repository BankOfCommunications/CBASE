#include "charset.h"

/*
mod by zhuxh:20161011 [add class Charset]
Mostly modified.

As is known, in both gbk & utf-8 charsets, every ascii char including alphabet takes 1 byte.
Every Chinese char takes 2 in gbk and 3 in utf-8.
That's why a string can be 1.5 times longer at most after conversion from gbk to utf-8.
*/

namespace oceanbase
{
namespace common
{

int Charset::ctr = 0;

Charset::Charset():
  _errno_(OB_NOT_INIT)
{
}

Charset::Charset( const char * from_charset, const char * to_charset ):
  _errno_(OB_NOT_INIT)
{
  init(from_charset,to_charset);
}

Charset::Charset( bool g2u ):
  _errno_(OB_NOT_INIT)
{
  init(g2u);
}

Charset::~Charset()
{
  if( ! fail() )
  {
    iconv_close(cd);
    ctr --;
  }
}

void Charset::init(const char * _from_charset, const char * _to_charset)
{
  /*
  if( ctr >= 1 )
  {
    _errno_ = OB_INIT_TWICE;
    TBSYS_LOG(ERROR, "Only one class Charset permitted.");
  }
  else
  */
  if( OB_NOT_INIT == _errno_ )
  {
    strcpy( from_charset, _from_charset );
    strcpy(   to_charset,   _to_charset );
    if ( (cd = iconv_open( to_charset, from_charset) ) == 0 )
    {
      _errno_ = OB_INIT_FAIL;
      TBSYS_LOG(ERROR, "Conversion from %s to %s error.",from_charset,to_charset);
    }
    else
    {
      _errno_ = OB_SUCCESS;
      ctr ++;
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "This object has been initialized.");
  }
}

void Charset::init(bool g2u)
{
  if( g2u )
    init("GB18030", "UTF-8");
  else
    init("UTF-8", "GB18030");
}

int Charset::get_errno()
{
  return _errno_;
}

//init error. Just like fail() in class fstream
bool Charset::fail()
{
  return OB_NOT_INIT == _errno_ || OB_INIT_TWICE == _errno_ || OB_INIT_FAIL == _errno_;
}

/*
In function code_convert(), outlen returns length of converted string. So does inlen.
E.G. string "你好这个世界", gbk to utf-8
+-------+-----+------+
|success|inlen|outlen|
+-------+-----+------+
|input  |12   |10000 |
+-------+-----+------+
|output |12   |18    |
+-------+-----+------+
In case of error, for an example, "你好这个世" converts successfully only with "界" left.
+-------+-----+------+
|error  |inlen|outlen|
+-------+-----+------+
|input  |12   |10000 |
+-------+-----+------+
|output |10   |15    |
+-------+-----+------+
 */
int Charset::code_convert(
    char *inbuf,  size_t & inlen,
    char *outbuf, size_t & outlen)
{
  int ret = OB_SUCCESS;

  if( ! fail() )
  {
    char *pin = inbuf;
    char *pout = outbuf;
    size_t inleft = inlen;
    size_t outleft = outlen;

    //memset(pout, 0, outleft);

    size_t ic = iconv(cd, &pin, &inleft, &pout, &outleft);
    inlen  -= inleft;
    outlen -= outleft;
    if ( ic == static_cast<size_t>(-1) )
      ret = OB_CONVERT_ERROR;
    //pout[outlen] = '\0';
  }
  else
    ret = OB_NOT_INIT;
  return ret;
}

//Convert locally
int Charset::code_convert( char * buf, size_t capacity, size_t & len )
{
  int ret = OB_SUCCESS;

  if( ! fail() )
  {
    size_t tmplen = len*3/2+5; //Length of utf-8 can be 1.5 times than that of gbk at most
    char * tmpbuf = new(std::nothrow) char[tmplen];
    if(!tmpbuf)
    {
      TBSYS_LOG(ERROR, "fail to allocate memories during conversion");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      //memset(tmpbuf,0,tmplen);
      ret = code_convert(buf, len, tmpbuf, tmplen);
      if( OB_SUCCESS != ret )
      {
        TBSYS_LOG(ERROR, "Conversion error");
      }
      else if( tmplen > capacity )
      {
        TBSYS_LOG(ERROR, "Memory over flow during conversion");
        ret = OB_MEM_OVERFLOW;
      }
      else
      {
        len = tmplen;
        memcpy(buf,tmpbuf,len);
      }
      delete []tmpbuf;
    }
  }
  else // if( ! fail() )
    ret = OB_NOT_INIT;
  return ret;
}

//add by zhuxh:20151202
bool Charset::is_part_gbk(char * str, int len)
{
  int i=len-1;
  int ctr=0;
  for(;i>0;i--,ctr++)
    if( static_cast<unsigned int>(str[i]) <= 0x80 )
      break;
  //if( ctr&1 )TBSYS_LOG(INFO, "TEMP LOG BEGIN\n%d:\n%.*s\nTEMP LOG END\n",ctr,len,str);//ctr,str+i+1);
  return ctr&1;
}

}//namespace common
}//namespace oceanbase
