#include "file_reader.h"
#include "common/utility.h"
#include "db_utils.h"

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

int kReadBufferSize = 1 * 512 * 1024;

FileReader::FileReader(const char *file_name)
{
  file_name_ = file_name;
  reader_buffer_ = NULL;
  buffer_pos_ = 0;
  buffer_size_ = 0;
  eof_ = false;
  bad_file_ = NULL;
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

int FileReader::get_records(RecordBlock &block, const RecordDelima &rec_delima, const RecordDelima &col_delima, int64_t max_rec_extracted)
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

  //mod by liyongfeng for rowbyrow
  if (ret == 0) {
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

bool FileReader::is_partial_record(int64_t &pos, const RecordDelima &rec_delima, const RecordDelima &col_delima)
{
  bool partial_rec = false;
  UNUSED(col_delima);

  for ( ;pos < buffer_size_ && !rec_delima.is_delima(reader_buffer_, pos, buffer_size_); pos++);

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
		TBSYS_LOG(WARN, "can't get a complete record");
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
