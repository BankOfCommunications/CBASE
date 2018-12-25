#ifndef __FILE_READER_H__
#define  __FILE_READER_H__

#include "common/file_utils.h"
#include "slice.h"
#include "tokenizer_v2.h"
#include <string>
//mod by zhuxh:20160627 [charset functions transfer] :b
//#include "iconv.h"
#include "charset.h"
//mod :e
#include "file_appender.h"

class RecordBlock {
  public:
    RecordBlock();

    bool next_record(Slice &slice);

    int has_record() const { return curr_rec_ < rec_num_; }

    void reset() { curr_rec_ = 0; offset_ = 0; }

    std::string &buffer() { return buffer_; }

    void set_rec_num(int64_t num) { rec_num_ = num; }

    int64_t get_rec_num() const { return rec_num_; }
  private:
    std::string buffer_;
    int64_t rec_num_;
    int64_t curr_rec_;
    int64_t offset_;
};

extern int kReadBufferSize;

class FileReader {
  public:
    //static const int kReadBufferSize = 1 * 128 * 1024;  /* 128K */

    FileReader(const char *file_name, bool g2u); //mod by zhuxh:20161018 [add class Charset]
    virtual ~FileReader();

    //open file
    virtual int open();

    int read();
    void set_bad_file(AppendableFile *bad_file)
    {
      bad_file_ = bad_file;
    }

    virtual int get_records(RecordBlock &block, const RecordDelima &rec_delima, const RecordDelima &col_delima, int64_t max_rec_extracted);

    bool eof() const { return eof_; }
    int64_t get_buffer_size() { return buffer_size_; }

    //gbk to utf-8 //add by zhuxh:20150816 :b
    bool g2u() const { return g2u_; }
    void set_g2u(bool _g2u) {g2u_ = _g2u;}
    //add :e

    //add by zhuxh:20161018 [add class Charset] :b
    const static char DEFAULT_G2U_CHAR;
    bool fail();
    //add by zhuxh:20161018 [add class Charset] :e

  protected:
    void shrink(int64_t pos);

    int skip_overflow_record(const RecordDelima &rec_delima, const RecordDelima &col_delima);

    int extract_record(RecordBlock &block, const RecordDelima &rec_delima, const RecordDelima &col_delima, int64_t max_rec_extracted);

    void append_int32(std::string &buffer, uint32_t value);

    void handle_last_record(RecordBlock &block);

    bool is_partial_record(int64_t &pos, const RecordDelima &rec_delima, const RecordDelima &col_delima);

    const char *file_name_;

    char *reader_buffer_;
    int64_t buffer_pos_;
    int64_t buffer_size_;
    AppendableFile *bad_file_;

    bool eof_;

    oceanbase::common::FileUtils file_;

    bool g2u_; //gbk to utf-8 //add by zhuxh:20150816
    char g2u_char; //add by zhuxh:20151202

    //add by zhuxh:20161018 [add class Charset] :b
    int _errno_;
    Charset charset;
    //add by zhuxh:20161018 [add class Charset] :e
};

//delete by zhuxh:20150816
#if 0
class EncodingFileReader : public FileReader
{
  public:
    EncodingFileReader(const char *file_name);
    virtual ~EncodingFileReader();

    int open();
    int get_records(RecordBlock &block, const RecordDelima &rec_delima, const RecordDelima &col_delima, int64_t max_rec_extracted);

  private:
    int convert_string_encode();

  private:
    iconv_t cd_;
    char *raw_buffer_;
    int64_t raw_buffer_size_;
};
#endif

//del by zhuxh:20160627
//Functions of code conversion have been moved into charset.cpp/.h
#if 0
//add by zhuxh:20150816 :b
int code_convert(
    const char *from_charset,
    const char *to_charset,
    char *inbuf, size_t inlen,
    char *outbuf, size_t outlen);

int u2g(char *inbuf, size_t inlen, char *outbuf, size_t outlen);
int g2u(char *inbuf, size_t inlen, char *outbuf, size_t outlen);

int g2u(char * buf, size_t & len);
//add :e

bool is_part_gbk(char * str, int len);//add by zhuxh:20151202
#endif

#endif
