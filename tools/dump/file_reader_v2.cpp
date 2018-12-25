#include "file_reader_v2.h"
#include "common/utility.h"
#include "db_utils.h"

//mod by zhuxh:20161018 [add class Charset] :b
//#define DEFAULT_G2U_CHAR '\0'
const char FileReader::DEFAULT_G2U_CHAR = '\0';
//mod by zhuxh:20161018 [add class Charset] :e

RecordBlock::RecordBlock()
{
  curr_rec_ = rec_num_ = 0;
  offset_ = 0;
}

bool RecordBlock::next_record(Slice &slice)
{
  if (offset_ + sizeof(uint32_t) < buffer_.size()) {
    const char *data = buffer_.data() + offset_;
    int dlen = decode_int32(data);
    Slice tmp(data + sizeof(uint32_t), dlen);
    slice = tmp;
    offset_ = offset_ + sizeof(uint32_t) + dlen;
    return true;
  } else {
    return false;
  }
}

//del by zhuxh:20151201
//The definition has been moved to ob_import_param_v2.cpp
//int kReadBufferSize = 1 * 512 * 1024;

//mod by zhuxh:20161018 [add class Charset]
FileReader::FileReader(const char *file_name, bool g2u):
  g2u_(g2u),
  g2u_char(DEFAULT_G2U_CHAR),
  _errno_(OB_SUCCESS),
  charset()
{
  file_name_ = file_name;
  reader_buffer_ = NULL;
  buffer_pos_ = 0;
  buffer_size_ = 0;
  eof_ = false;
  bad_file_ = NULL;

  /*
  //add by zhuxh:20151202 :b
  g2u_ = false;
  g2u_char = DEFAULT_G2U_CHAR;
  //add :e
  */

  if( g2u_ )
  {
    charset.init(true);
    if( charset.fail() )
    {
      _errno_ = OB_CONVERT_ERROR;
    }
  }
}

//add by zhuxh:20151018 [add class Charset]
bool FileReader::fail()
{
  return OB_SUCCESS != _errno_;
}

FileReader::~FileReader()
{
  if (reader_buffer_ != NULL)
    delete [] reader_buffer_;
}
//open file
int FileReader::open()
{
  int ret = 0;
  int fd = file_.open(file_name_, O_RDONLY);
  if (fd > 0) {
    reader_buffer_ = new(std::nothrow) char[kReadBufferSize];
    if (reader_buffer_ == NULL) {
      ret = -1;
      TBSYS_LOG(ERROR, "can't allocate memory for read_buffer, size=%d", kReadBufferSize);
    }
  } else {
    ret = -1;
    TBSYS_LOG(ERROR, "can't open file, name=%s", file_name_);
  }

  return ret;
}

//mod by zhuxh:20151202 for gbk=>utf-8
int FileReader::get_records(RecordBlock &block, const RecordDelima &rec_delima, const RecordDelima &col_delima, int64_t max_rec_extracted)
{
  int ret = OB_SUCCESS;
  int len = 0;

  int with_last_gbk_char = 0;
  //When we need to transform data from GBK to UTF-8, we can't use all space,
  //Part of which is spared because UTF-8 strings are commonly longer.
  //add by zhuxh:20151201
  int valid_buff = ( g2u_ ? kReadBufferSize*2/3-6 : kReadBufferSize);

  if( valid_buff > buffer_size_ && g2u_char != DEFAULT_G2U_CHAR )//happens in case of gbk to utf-8
  {
    //At the last time, get_records() leaves a char stored in g2u_char, namely half a Chinese word
    //which will join the following data read from file
    with_last_gbk_char = 1;
    reader_buffer_[buffer_size_] = g2u_char;
    g2u_char = DEFAULT_G2U_CHAR;
  }

  if (buffer_size_ < valid_buff) {
    len = static_cast<int32_t>(file_.read(reader_buffer_ + buffer_size_ + with_last_gbk_char, valid_buff - buffer_size_ - with_last_gbk_char)) + with_last_gbk_char;
    if (len != -1)
      buffer_size_ += len;
    else  {
      ret = -1;
      TBSYS_LOG(ERROR, "can't read from file, name=%s", file_name_);
    }
  }

  //if(with_last_gbk_char)
  //  TBSYS_LOG(INFO, "TEMP LOG BEGIN ====== the latter string\n%.*s\nTEMP LOG END ======\n",10,reader_buffer_ + buffer_size_ - len);

  //add by zhuxh:20150816
  //gbk => utf-8
  if( ret == OB_SUCCESS && g2u_ )
  {
    size_t gbklen = (size_t)len;
    int64_t buffer_size_before = buffer_size_ - static_cast<int64_t>(len);
    char * gbk = reader_buffer_ + buffer_size_before;
    if( buffer_size_before + (len*3/2) >= kReadBufferSize )//utf-8 is always longer than gbk
    {
      TBSYS_LOG(ERROR, "Inadequate buffer for transformation from gbk to utf-8!");
      ret = OB_ARRAY_OUT_OF_RANGE;
    }
    else
    {
      if( charset.is_part_gbk(gbk,static_cast<int>(gbklen)) ) //mod by zhuxh:20151201 [add class Charset]
      {
        //Hypothetically code of 我 is [0xAA][0xBB], separated by our file reader.
        //This time we get 0xAA at the end and leave 0xBB in the file.
        //So we will keep 0xAA in g2u_char which will join 0xBB next time.
        g2u_char = gbk[gbklen-1];
        gbklen--;
      }
      //mod by zhuxh:20161018 [add class Charset] :b
      ret = charset.code_convert(gbk, kReadBufferSize-buffer_size_before, gbklen); //gbklen => utf-8 length
      if( OB_SUCCESS == ret )
      {
        buffer_size_ += static_cast<int64_t>(gbklen) - static_cast<int64_t>(len);
        len = static_cast<int>(gbklen);
      }
      else
      {
        TBSYS_LOG(ERROR, "code conversion error.");
      }
      //mod by zhuxh:20161018 [add class Charset] :e
      //if(b)

      //int tmp = static_cast<int>(min(500,buffer_size_));
      //TBSYS_LOG(INFO, "len: %d; buffersize: %ld\n",len,buffer_size_);
      //TBSYS_LOG(INFO, "UTF-8:\n%.*s...%.*s\n",tmp,reader_buffer_,tmp,reader_buffer_+buffer_size_-tmp);
    }
  }
  //TBSYS_LOG(INFO, "TEMP LOG BEGIN\nbuffer_size_ = %ld\nlen = %d\n%.*s\nTEMP LOG END",buffer_size_-len,len,len,reader_buffer_ + buffer_size_ - len);

  //mod by liyongfeng for rowbyrow
  if (ret == OB_SUCCESS) {
    //mod by liyongfeng for rowbyrow
    //if(len == 0) {
    if (len == 0 && buffer_size_ == 0) {
    //mod:e
      eof_ = true;
      TBSYS_LOG(DEBUG, "FileReader:read file end, name=%s", file_name_);
    } 

    if (buffer_size_ != 0) {
      TBSYS_LOG(DEBUG, "extract record in file, buffersize = %ld", buffer_size_);
      ret = extract_record(block, rec_delima, col_delima, max_rec_extracted);
	  if (len == 0 && ret == 1) {
		  TBSYS_LOG(WARN, "last line record is partial, add rec_delima");
		  rec_delima.append_delima(reader_buffer_, buffer_size_, kReadBufferSize);
		  rec_delima.skip_delima(buffer_size_);
	  }
    } else {
      TBSYS_LOG(DEBUG, "All data is processed");
    }
  }

  return ret;
}

//mod by zhuxh:20160121
bool FileReader::is_partial_record(int64_t &pos, const RecordDelima &rec_delima, const RecordDelima &col_delima)
{
  bool partial_rec = false;
  //UNUSED(col_delima);

  //add by zhuxh:20151110 :b
  //a data file may be like
  //abcd^"1234\n5678\n90"^hello
  //At this time \n should not be judged as a rec_delima
  //bool fq=false;//flag of quotaion mark, means if [pos] is between 2 quotation marks

  int step=0;
  bool running=true;
  char q='\"';
  bool left_is_col_delima = false;
  //mod by zhuxh:20160912 [quotation mark] :b
  int ctr=0; // ctr==0 means we are at the beginning of the buf.
  int quotation_mark_ctr=0; //the number of " since I enter the area between "", case step 1
  for ( ; pos < buffer_size_; pos++,ctr++)
  //mod by zhuxh:20160912 [quotation mark] :e
  {
#if 0
    if( reader_buffer_[pos] == q )
      fq=!fq;
    else if( !fq && rec_delima.is_delima(reader_buffer_, pos, buffer_size_) )
      break;
#endif
    switch(step)
    {
    case 0: //normally
      if( rec_delima.is_delima(reader_buffer_, pos, buffer_size_) )
      {
        running=false;
        break;
      }
      //mod by zhuxh:20160912 [quotation mark] :b
      else if( reader_buffer_[pos] == q && ( left_is_col_delima || ctr == 0 ) ) // If my left char is delima or I'm the first char
      {
        step++;
        quotation_mark_ctr=1;
      }
      break;

    case 1: //between double quotation marks
      //quit when I'm \n, my left is " and since last delima, there has been an even number of "
      //Ex.
      //^O"xxxxx""yyyyy""zzzzz"\n
      if( reader_buffer_[pos]==q )
        quotation_mark_ctr ++;
      else if( pos>0 && reader_buffer_[pos-1]==q && (quotation_mark_ctr & 1) == 0 && rec_delima.is_delima(reader_buffer_, pos, buffer_size_) )
      //mod by zhuxh:20160912 [quotation mark] :e
      {
        running=false;
        break;
      }
      else if( col_delima.is_delima(reader_buffer_, pos, buffer_size_) )
        step=0;
      break;
    }
    if( col_delima.is_delima(reader_buffer_, pos, buffer_size_) )
    {
      left_is_col_delima = true;
      if( col_delima.type_  != RecordDelima::CHAR_DELIMA && buffer_size_ > pos+1 ) pos++;
    }
    else
      left_is_col_delima = false;
    if(!running)break;
  }
  //add :e

  if (pos >= buffer_size_) {                    /* stop */ 
    partial_rec = true;
  }

  return partial_rec;
}

int FileReader::skip_overflow_record(const RecordDelima &rec_delima, const RecordDelima &col_delima)
{
  int ret = 0;
  int len = 0;
  bool partial_rec = false;
  if (NULL != bad_file_)
  {
    bad_file_->get_append_lock().lock();
  }
  while (0 == ret)
  {
    len = (int)buffer_size_;
    if (buffer_size_ < kReadBufferSize) {
      len = static_cast<int32_t>(file_.read(reader_buffer_ + buffer_size_, kReadBufferSize - buffer_size_));
      if (len != -1)
        buffer_size_ += len;
      else  {
        ret = -1;
        TBSYS_LOG(ERROR, "can't read from file, name=%s", file_name_);
      }
    }

    if (ret == 0) {
      //mod by liyongfeng for rowbyrow
      //if (len == 0) {
      if (len == 0 && buffer_size_ == 0) {
      //mod:e
        eof_ = true;
        TBSYS_LOG(DEBUG, "FileReader:read file end, name=%s", file_name_);
        break;
      }

      if (buffer_size_ != 0) {
        int64_t pos = 0;
        partial_rec = is_partial_record(pos, rec_delima, col_delima);
        if (!partial_rec)
        {
          rec_delima.skip_delima(pos);
        }
        if (NULL != bad_file_)
        {
          bad_file_->AppendNoLock(reader_buffer_, pos);
        }
        int64_t j = 0;
        for (int64_t i = pos; i < buffer_size_; i ++)
        {
          reader_buffer_[j++] = reader_buffer_[i];
        }
        buffer_size_ = j;
        if (!partial_rec) break;
      } else {
        ret = -1;
        TBSYS_LOG(WARN, "buffer_size_ should not be zero");
      }
    }
  }
  if (NULL != bad_file_)
  {
    bad_file_->get_append_lock().unlock();
  }
  return ret;
}

int FileReader::extract_record(RecordBlock &block, const RecordDelima &rec_delima, const RecordDelima &col_delima, int64_t max_rec_extracted)
{
  int64_t pos = 0;
  int64_t rec_nr = 0;
  int64_t last_pos = 0;
  int ret = 0;
  bool has_partial_rec = false;

  std::string &buffer = block.buffer();

  do
  {
    while (pos < buffer_size_) {
      char *p = reader_buffer_ + pos;
      last_pos = pos;

      if (rec_nr >= max_rec_extracted) {
        TBSYS_LOG(DEBUG, "extract rec count[%ld] reaches max_rec_extracted[%ld]", rec_nr, max_rec_extracted);
        break;
      }

      if ((has_partial_rec = is_partial_record(pos, rec_delima, col_delima)) == true) {
        TBSYS_LOG(DEBUG, "partial record meet, will be proceed next time");
        break;
      }

      rec_nr++;
      col_delima.append_delima(reader_buffer_, pos, buffer_size_); /* replace rec_delima with col_delima */
      rec_delima.skip_delima(pos);                                      /* skip rec_delima */

      /* include rec_delima in buffer */
      int32_t psize = static_cast<int32_t>(pos - last_pos);
      append_int32(buffer, psize);

      last_pos = pos;
      buffer.append(p, psize);                   /* append length of record here */
    }

    TBSYS_LOG(DEBUG, "record block actual used size[%ld], rec_nr[%ld]", pos, rec_nr);

    if ((has_partial_rec && last_pos < buffer_size_)    /* partial record reside in buffer */
      || (rec_nr >= max_rec_extracted && last_pos < buffer_size_)) {
      shrink(last_pos);
    } else {
      buffer_size_ = 0;
    }

    if (ret == 0) {
      block.set_rec_num(rec_nr);
    }

    if (rec_nr == 0 && buffer_size_ == kReadBufferSize) {
      TBSYS_LOG(WARN, "line is too large");
      ret = skip_overflow_record(rec_delima, col_delima);
      if (ret != 0)
      {
        TBSYS_LOG(WARN, "fail to skip overflow record");
      }
    }
	else if(rec_nr == 0 && buffer_size_ < kReadBufferSize) {
      //mod by zhuxh:20151130
      //INCOMPLETE_DATA == 39
      TBSYS_LOG(ERROR, "ERROR %d\nCan't get a complete record. The data beginning from the followings are discarded:\n%.*s",39,static_cast<int>(min(100,pos-last_pos)),reader_buffer_ + last_pos);
      ret = 1;
      break;
	}
    else
    {
      break;
    }
  }
  while (!eof_);

  return ret;
}


void FileReader::append_int32(std::string &buffer, uint32_t value)
{
  char tmp[sizeof(uint32_t)];
  encode_int32(tmp, value);
  buffer.append(tmp, sizeof(uint32_t));
}

void FileReader::handle_last_record(RecordBlock &block)
{
  std::string &buffer = block.buffer();
  append_int32(buffer, static_cast<int32_t>(buffer_size_));
  buffer.append(reader_buffer_, buffer_size_);
  block.set_rec_num(1);
  buffer_size_ = 0;
}

void FileReader::shrink(int64_t pos)
{
  int64_t total = buffer_size_ - pos;
  assert(pos < buffer_size_);

  for(int64_t i = 0;i < total; i++) {
    reader_buffer_[i] = *(reader_buffer_ + pos + i);
  }

  buffer_size_ = total;
}

//delete by zhuxh:20150816
#if 0
EncodingFileReader::EncodingFileReader(const char *file_name)
  :FileReader(file_name)
{
  raw_buffer_ = NULL;
  raw_buffer_size_ = 0;
}

EncodingFileReader::~EncodingFileReader()
{
  if (raw_buffer_ != NULL)
    delete [] raw_buffer_;
}

int EncodingFileReader::open()
{
  int ret = 0;
  raw_buffer_ = new(std::nothrow) char[kReadBufferSize];
  if (raw_buffer_ == NULL) {
    ret = -1;
    TBSYS_LOG(ERROR, "can't allocate memory for raw_buffer_, size=%d", kReadBufferSize);
  }
  if (ret == 0) {
    cd_ = iconv_open("utf-8", "gbk");
    if (NULL == cd_) {
      ret = -1;
      TBSYS_LOG(ERROR, "fail to open iconv gbk -> utf8");
    }
  }
  if (ret == 0) {
    ret = FileReader::open();
  }
  return ret;
}

int EncodingFileReader::get_records(RecordBlock &block, const RecordDelima &rec_delima, const RecordDelima &col_delima, int64_t max_rec_extracted)
{
  int ret = 0;
  int len = 0;

  if (buffer_size_ < kReadBufferSize) {
    len = static_cast<int32_t>(file_.read(reader_buffer_ + buffer_size_, kReadBufferSize - buffer_size_));
    if (len != -1)
      buffer_size_ += len;
    else  {
      ret = -1;
      TBSYS_LOG(ERROR, "can't read from file, name=%s", file_name_);
    }
  }

  if (ret == 0) {
    //mod by liyongfeng for rowbyrow
    //if (len == 0) {
    if (len == 0 && buffer_size_ == 0) {
    //mod:e
      eof_ = true;
      TBSYS_LOG(DEBUG, "FileReader:read file end, name=%s", file_name_);
    } 

    if (buffer_size_ != 0) {
      TBSYS_LOG(DEBUG, "extract record in file, buffersize = %ld", buffer_size_);
      ret = extract_record(block, rec_delima, col_delima, max_rec_extracted);
    } else {
      TBSYS_LOG(DEBUG, "All data is processed");
    }
  }

  return ret;
}

int EncodingFileReader::convert_string_encode()
{
  int ret = 0;
  char *raw_buf_ptr = raw_buffer_;
  char *reader_buf_ptr = reader_buffer_ + buffer_size_;
  size_t raw_buf_size = raw_buffer_size_;
  size_t read_buf_remain_size = kReadBufferSize - buffer_size_;

  size_t iconv_ret = iconv(cd_, &raw_buf_ptr, &raw_buf_size, &reader_buf_ptr, &read_buf_remain_size);
  if (-1 == (int)iconv_ret && errno != E2BIG) {
    ret = -1;
    TBSYS_LOG(ERROR, "fail to iconv, errno[%d]", errno);
    return ret;
  }

  if (eof_ && 0 != raw_buf_size) {
    ret = -1;
    TBSYS_LOG(ERROR, "when file is end, raw_buf_size should be zero");
    //可能发生错误，这里面也许有bug
  }

  buffer_size_ = kReadBufferSize - read_buf_remain_size;
  memcpy(raw_buffer_, raw_buf_ptr, raw_buf_size);
  raw_buffer_size_ = raw_buf_size;

  return ret;
}
#endif

//del by zhuxh:20160627
//Functions of code conversion have been moved into charset.cpp/.h
#if 0
//add by zhuxh:20150816 :b
int code_convert(
    const char *from_charset,
    const char *to_charset,
    char *inbuf, size_t inlen,
    char *outbuf, size_t outlen)
{
  iconv_t cd;
  char **pin = &inbuf;
  char **pout = &outbuf;

  cd = iconv_open(to_charset, from_charset);
  if (cd == 0)
    return -1;
  memset(outbuf, 0, outlen);
  if (iconv(cd, pin, &inlen, pout, &outlen) == static_cast<size_t>(-1))
    return -1;
  iconv_close(cd);
  //*pout = '\0';

  return 0;
}

int u2g(char *inbuf, size_t inlen, char *outbuf, size_t outlen) {
  return code_convert("utf-8", "gbk", inbuf, inlen, outbuf, outlen);
}

int g2u(char *inbuf, size_t inlen, char *outbuf, size_t outlen) {
  return code_convert("GB18030", "UTF-8", inbuf, inlen, outbuf, outlen);
}

int g2u(char * buf, size_t & len)
{
  int ret = OB_SUCCESS;
  size_t tmplen = len*3/2+5; //Length of utf-8 can be 1.5 times than that of gbk at most
  char * tmpbuf = new(std::nothrow) char[tmplen];
  if(!tmpbuf)
  {
    TBSYS_LOG(ERROR, "fail to allocate memories in transform from gbk to utf-8!");
    ret = OB_ERROR;
  }
  else
  {
    memset(tmpbuf,0,tmplen);
    g2u(buf, len, tmpbuf, tmplen);
    len = strlen(tmpbuf);
    memcpy(buf,tmpbuf,len);
    delete []tmpbuf;
  }
  return ret;
}
//add:e

//add by zhuxh:20151202
bool is_part_gbk(char * str, int len)
{
  int i=len-1;
  int ctr=0;
  for(;i>0;i--,ctr++)
    if( static_cast<unsigned int>(str[i]) <= 0x80 )
      break;
  //if( ctr&1 )TBSYS_LOG(INFO, "TEMP LOG BEGIN\n%d:\n%.*s\nTEMP LOG END\n",ctr,len,str);//ctr,str+i+1);
  return ctr&1;
}
#endif
