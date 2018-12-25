#ifndef __FILE_READER_H__
#define  __FILE_READER_H__

#include "common/file_utils.h"
#include "slice.h"
#include "tokenizer.h"
#include <string>
#include "iconv.h"
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

    FileReader(const char *file_name);
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
};

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
