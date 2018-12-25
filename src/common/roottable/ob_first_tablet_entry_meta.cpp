/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_first_tablet_entry_meta.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_first_tablet_entry_meta.h"
#include "ob_first_tablet_entry.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_atomic.h"
#include <cstdio>
using namespace oceanbase::common;

struct ObFirstTabletEntryMeta::RefCountRow
{
  ObTabletMetaTableRow meta_;
  volatile uint64_t ref_count_;
  public:
    RefCountRow():ref_count_(0) {}
};

struct ObFirstTabletEntryMeta::MyIterator: public ConstIterator
{
  public:
    MyIterator();
    ~MyIterator();

    int next(const ObTabletMetaTableRow *&row);
    int rewind();
  private:
    friend class ObFirstTabletEntryMeta;
    void reset(ObFirstTabletEntryMeta *aggr);
  private:
    ObFirstTabletEntryMeta* aggr_;
    int64_t pos_;
    uint32_t read_handle_;
};

ObFirstTabletEntryMeta::MyIterator::MyIterator()
  :aggr_(NULL), pos_(-1), read_handle_(INVALID_HANDLE)
{
  TBSYS_LOG(DEBUG, "construct ObFirstTabletEntryMeta::MyIterator addr=%p", this);
}

ObFirstTabletEntryMeta::MyIterator::~MyIterator()
{
  // @note 析构时应该把read_handle_指向元素的引用计数减一，但是因为MyIterator是用TSI分配的，而aggr_是从堆分配的，
  // MyIterator析构时aggr_指向的对象已经无效，不能访问
  aggr_ = NULL;
  pos_ = -1;
  read_handle_ = INVALID_HANDLE;
  TBSYS_LOG(DEBUG, "destruct ObFirstTabletEntryMeta::MyIterator addr=%p", this);
}

int ObFirstTabletEntryMeta::MyIterator::next(const ObTabletMetaTableRow *&row)
{
  int ret = OB_SUCCESS;
  if (NULL == aggr_
      || INVALID_HANDLE == read_handle_)
  {
    TBSYS_LOG(ERROR, "iterator not init");
    ret = OB_ERR_UNEXPECTED;
  }
  else if (-1 == pos_)
  {
    row = &aggr_->first_meta_[read_handle_].meta_;
    ++pos_;
  }
  else
  {
    row = NULL;
    ret = OB_ITER_END;
  }
  return ret;
}

int ObFirstTabletEntryMeta::MyIterator::rewind()
{
  int ret = OB_SUCCESS;
  pos_ = -1;
  return ret;
}

void ObFirstTabletEntryMeta::MyIterator::reset(ObFirstTabletEntryMeta *aggr)
{
  // @note 从此处到后面tagtag之间的代码是为了完成ObFirstTabletEntryMetaTest的单测而特殊处理的，
  // 因为MyIterator从TSI分配所以对象在整个进程生命周期有效，而各个单测case需要相互独立，每个case
  // 新建自己的ObFirstTabletEntryMeta对象，因而这里需要重置aggr_的值
  if (NULL != aggr_ && aggr_ != aggr)
  {
    aggr_ = NULL;
    pos_ = -1;
    read_handle_ = INVALID_HANDLE;
  }
  aggr_ = aggr;
  pos_ = -1;
  // tagtag

  if (INVALID_HANDLE != read_handle_)
  {
    // 释放本线程前一次使用iterator对象未释放的资源：减引用计数
    atomic_dec(&aggr_->first_meta_[read_handle_].ref_count_);
    read_handle_ = INVALID_HANDLE;
  }
  while(true)
  {
    uint64_t read_handle = aggr_->read_handle_;
    uint64_t oldref = aggr_->first_meta_[read_handle].ref_count_;
    uint64_t newref = oldref + 1;
    if (0 != oldref)
    {
      // increase ref_count if not zero
      if (oldref == atomic_compare_exchange(&aggr_->first_meta_[read_handle].ref_count_, newref, oldref))
      {
        read_handle_ = static_cast<uint32_t>(read_handle);
        TBSYS_LOG(DEBUG, "init iterator with read handle=%u", read_handle_);
        break;
      }
      else
      {
        TBSYS_LOG(DEBUG, "oldref=%lu newref=%lu", oldref, newref);
      }
    }
    else
    {
      TBSYS_LOG(DEBUG, "readhandle_=%lu oldref=%lu", read_handle, oldref);
    }
  } // end while
}

////////////////////////////////////////////////////////////////
ObFirstTabletEntryMeta::ObFirstTabletEntryMeta(const int32_t max_thread_count, const char* filename)
  :meta_array_size_(0), read_handle_(INVALID_HANDLE),
   write_handle_(INVALID_HANDLE), first_meta_(NULL)
{
  memset(filename_, 0, sizeof(filename_));
  OB_ASSERT(0 < max_thread_count);
  meta_array_size_ = max_thread_count + 1;
  first_meta_ = new(std::nothrow) RefCountRow[meta_array_size_];
  if (NULL == first_meta_)
  {
    TBSYS_LOG(ERROR, "no memory");
  }
  else
  {
    TBSYS_LOG(INFO, "init meta array size=%d", meta_array_size_);
  }
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = set_filename(filename)))
  {
    TBSYS_LOG(WARN, "failed to set filename, err=%d", ret);
  }
  TBSYS_LOG(DEBUG, "construct ObFirstTabletEntryMeta, this=%p", this);
}

ObFirstTabletEntryMeta::~ObFirstTabletEntryMeta()
{
  if (NULL != first_meta_)
  {
    delete [] first_meta_;
    first_meta_ = NULL;
  }
  meta_array_size_ = 0;
  read_handle_ = INVALID_HANDLE;
  write_handle_ = INVALID_HANDLE;
  TBSYS_LOG(DEBUG, "destruct ObFirstTabletEntryMeta, this=%p", this);
}

int ObFirstTabletEntryMeta::init(const Value &v)
{
  int ret = OB_SUCCESS;
  if (NULL == first_meta_)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(ERROR, "no memory");
  }
  else if (OB_SUCCESS != (ret = check_first_meta_row(v)))
  {
    TBSYS_LOG(ERROR, "invalid argument row, err=%d", ret);
  }
  else if (INVALID_HANDLE != read_handle_)
  {
    ret = OB_INIT_TWICE;
    TBSYS_LOG(WARN, "meta already init, handle=%u", read_handle_);
  }
  else
  {
    read_handle_ = 0;
    first_meta_[read_handle_].meta_ = v;
    first_meta_[read_handle_].ref_count_ = 1;
    ret = store(first_meta_[read_handle_].meta_, filename_);
    TBSYS_LOG(INFO, "first tablet entry meta inited");
  }
  return ret;
}

int ObFirstTabletEntryMeta::check_integrity()
{
  int ret = OB_SUCCESS;
  if (NULL == first_meta_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "first_meta_ is NULL");
  }
  else if (read_handle_ == INVALID_HANDLE)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "read_handle is invalid");
  }
  else if ('\0' == filename_[0])
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "filename is empty");
  }
  return ret;
}

int ObFirstTabletEntryMeta::aquire_iterator(MyIterator *&it)
{
  // get the thread local iterator
  int ret = OB_SUCCESS;
  it = GET_TSI_MULT(MyIterator, TSI_COMMON_THE_META_1);
  if (NULL == it)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(ERROR, "no memory");
  }
  else
  {
    it->reset(this);
  }
  return ret;
}

int ObFirstTabletEntryMeta::search(const KeyRange& key_range, ConstIterator *&first)
{
  int ret = OB_SUCCESS;
  MyIterator *it = NULL;
  if (OB_FIRST_TABLET_ENTRY_TID != key_range.table_id_)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "invalid table id, tid=%lu", key_range.table_id_);
  }
  else if (OB_SUCCESS != (ret = check_integrity()))
  {
    TBSYS_LOG(ERROR, "integrity error");
  }
  else if (OB_SUCCESS != (ret = aquire_iterator(it)))
  {
    TBSYS_LOG(ERROR, "aquire iterator error, err=%d", ret);
  }
  else
  {
    first = it;
  }
  return ret;
}


int ObFirstTabletEntryMeta::insert(const Value &v)
{
  int ret = OB_ERR_UNEXPECTED;
  UNUSED(v);
  TBSYS_LOG(ERROR, "the first tablet entry table can not split and should not be inserted");
  return ret;
}

int ObFirstTabletEntryMeta::erase(const Key &k)
{
  int ret = OB_ERR_UNEXPECTED;
  UNUSED(k);
  TBSYS_LOG(ERROR, "the first tablet entry table can not be erased");
  return ret;
}

int ObFirstTabletEntryMeta::check_first_meta_row(const Value &v)
{
  int ret = OB_INVALID_ARGUMENT;
  if (OB_FIRST_TABLET_ENTRY_TID != v.get_tid())
  {
    TBSYS_LOG(ERROR, "invalid table id, tid=%lu", v.get_tid());
  }
  else if (ObString::make_string(FIRST_TABLET_TABLE_NAME) != v.get_table_name())
  {
    TBSYS_LOG(ERROR, "invalid table name, tname=%.*s",
              v.get_table_name().length(), v.get_table_name().ptr());
  }
  else if (1 != v.get_end_key().get_obj_cnt()
           || NULL == v.get_end_key().get_obj_ptr()
           || !(*v.get_end_key().get_obj_ptr()).is_max_value())
  {
    TBSYS_LOG(ERROR, "invalid end_key, obj_cnt=%ld obj_ptr=%p",
              v.get_end_key().get_obj_cnt(),
              v.get_end_key().get_obj_ptr());
  }
  else if (1 != v.get_start_key().get_obj_cnt()
           || NULL == v.get_start_key().get_obj_ptr()
           || !(*v.get_start_key().get_obj_ptr()).is_min_value())
  {
    TBSYS_LOG(ERROR, "invalid start_key, obj_cnt=%ld obj_ptr=%p",
              v.get_start_key().get_obj_cnt(),
              v.get_start_key().get_obj_ptr());
  }
  else
  {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObFirstTabletEntryMeta::aquire_write_handle()
{
  int ret = OB_SUCCESS;
  const int max_retry = 10;
  for (int retry = 0; retry < max_retry && INVALID_HANDLE == write_handle_; ++retry)
  {
    for (uint64_t i = 0; i < meta_array_size_; ++i)
    {
      if (0 == first_meta_[i].ref_count_)
      {
        write_handle_ = static_cast<uint32_t>(i);
        break;
      }
    }
  }
  if (INVALID_HANDLE == write_handle_
      || INVALID_HANDLE == read_handle_)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "can not aquire write_handle, read_handle=%u whandle=%u",
              read_handle_, write_handle_);
  }
  else
  {
    first_meta_[write_handle_].meta_ = first_meta_[read_handle_].meta_; // copy
  }
  return ret;
}

int ObFirstTabletEntryMeta::update(const Value &v)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = check_first_meta_row(v)))
  {
    TBSYS_LOG(ERROR, "invalid first meta row");
  }
  else if (OB_SUCCESS != (ret = check_integrity()))
  {
    TBSYS_LOG(ERROR, "read first meta error");
  }
  else if (OB_SUCCESS != (ret = aquire_write_handle()))
  {
    TBSYS_LOG(ERROR, "failed to aquire write handle");
  }
  else
  {
    TBSYS_LOG(DEBUG, "update with write handle=%u", write_handle_);
    first_meta_[write_handle_].meta_.update(v);
  }
  return ret;
}

// @pre INVALID_HANDLE != read_handle_ && INVALID_HANDLE != write_handle_
void ObFirstTabletEntryMeta::commit_write_handle()
{
  atomic_inc(&first_meta_[write_handle_].ref_count_);
  uint32_t old_read_handle = read_handle_;
  atomic_exchange(&read_handle_, write_handle_);
  atomic_dec(&first_meta_[old_read_handle].ref_count_);
  TBSYS_LOG(DEBUG, "commit write handle=%u", write_handle_);
  write_handle_ = INVALID_HANDLE;
}

int ObFirstTabletEntryMeta::commit()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = check_integrity()))
  {
    TBSYS_LOG(ERROR, "integrity error");
  }
  else if (INVALID_HANDLE != write_handle_)
  {
    commit_write_handle();
    ret = store(first_meta_[read_handle_].meta_, filename_);
  }
  return ret;
}

int ObFirstTabletEntryMeta::set_filename(const char* filename)
{
  int ret = OB_SUCCESS;
  if (NULL == filename)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "argument filename is NULL");
  }
  else
  {
    int64_t data_len = strlen(filename) + 1;
    if (static_cast<int64_t>(sizeof(filename_)) < data_len)
    {
      ret = OB_BUF_NOT_ENOUGH;
      TBSYS_LOG(WARN, "filename too long");
    }
    else
    {
      memcpy(filename_, filename, data_len);
    }
  }
  return ret;
}

int ObFirstTabletEntryMeta::init()
{
  int ret = OB_SUCCESS;
  ObObj objs1[1];
  objs1[0].set_min_value();
  ObRowkey start_key;
  start_key.assign(objs1, 1);
  ObObj objs2[1];
  objs2[0].set_max_value();
  ObRowkey end_key;
  end_key.assign(objs2, 1);
  ObTabletMetaTableRow row;
  row.set_start_key(start_key);
  row.set_end_key(end_key);
  if (OB_SUCCESS != (ret = load(filename_, row)))
  {
    TBSYS_LOG(WARN, "failed to load from file, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = init(row)))
  {
    TBSYS_LOG(WARN, "failed to init with row, err=%d", ret);
  }
  return ret;
}

int ObFirstTabletEntryMeta::load(const char* filename, ObTabletMetaTableRow &row)
{
  int ret = OB_SUCCESS;
  if (NULL == filename
      || '\0' == filename[0])
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    const int64_t max_data_size = 64*1024;
    char* buf = static_cast<char*>(ob_malloc(max_data_size, ObModIds::OB_FIRST_TABLET_META));
    int fd = -1;
    ssize_t readlen = -1;
    int64_t pos = 0;
    if (NULL == buf)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (-1 == (fd = open(filename, O_RDONLY)))
    {
      ret = OB_IO_ERROR;
      TBSYS_LOG(WARN, "failed to openfile, file=%s err=%s", filename, strerror(errno));
    }
    else if (-1 == (readlen = read(fd, buf, max_data_size)))
    {
      ret = OB_IO_ERROR;
      TBSYS_LOG(ERROR, "failed to read file, file=%s err=%s", filename, strerror(errno));
    }
    else if (OB_SUCCESS != (ret = row.deserialize(buf, readlen, pos)))
    {
      TBSYS_LOG(ERROR, "deserialize meta row error, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = check_first_meta_row(row)))
    {
      TBSYS_LOG(ERROR, "invalid first meta row");
    }
    else
    {
      TBSYS_LOG(INFO, "load the first meta from file, file=%s", filename);
      row.set_start_key(ObRowkey::MIN_ROWKEY);
      row.set_end_key(ObRowkey::MAX_ROWKEY);
      row.set_table_name(ObString::make_string(FIRST_TABLET_TABLE_NAME));
      row.clear_changed();
    }
    if (-1 != fd)
    {
      close(fd);
    }
    if (NULL != buf)
    {
      ob_free(buf);
      buf = NULL;
    }
  }
  return ret;
}

int ObFirstTabletEntryMeta::backup(const char* filename)
{
  int ret = OB_SUCCESS;
  int64_t filename_len = 0;
  if (NULL == filename
      || '\0' == filename[0]
      || (filename_len = strlen(filename)) >= static_cast<int64_t>(sizeof(filename_)))
  {
    TBSYS_LOG(ERROR, "invalid filename");
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    // backup old file
    const int64_t backup_filelen = OB_MAX_FILE_NAME_LENGTH + 8;
    char backup_filename[backup_filelen];
    int printlen = snprintf(backup_filename, backup_filelen, "%s~", filename);
    if (printlen != filename_len + 1)
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "failed to generate backup filename");
    }
    else if (-1 == unlink(backup_filename)
             && ENOENT != errno)
    {
      ret = OB_IO_ERROR;
      TBSYS_LOG(ERROR, "failed to remove file, err=%s file=%s", strerror(errno), backup_filename);
    }
    else if (-1 == rename(filename, backup_filename)
             && ENOENT != errno)
    {
      ret = OB_IO_ERROR;
      TBSYS_LOG(ERROR, "failed to rename file, err=%s src=%s dest=%s",
                strerror(errno), filename, backup_filename);
    }
  }
  return ret;
}

int ObFirstTabletEntryMeta::store(const ObTabletMetaTableRow &row, const char* filename)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = check_first_meta_row(row)))
  {
    TBSYS_LOG(ERROR, "invalid row, err=%d", ret);
    ret = OB_ERR_UNEXPECTED;
  }
  else if (OB_SUCCESS != (ret = backup(filename)))
  {
    TBSYS_LOG(WARN, "failed to backup file, err=%d filename=%s", ret, filename);
  }
  else
  {
    const int64_t max_data_size = 64*1024;
    char* buf = static_cast<char*>(ob_malloc(max_data_size, ObModIds::OB_FIRST_TABLET_META));
    int fd = -1;
    ssize_t writelen = -1;
    int64_t pos = 0;
    if (NULL == buf)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (-1 == (fd = open(filename, O_CREAT|O_WRONLY|O_TRUNC|O_EXCL, 0644)))
    {
      ret = OB_IO_ERROR;
      TBSYS_LOG(ERROR, "failed to openfile, file=%s err=%s", filename, strerror(errno));
    }
    else if (OB_SUCCESS != (ret = row.serialize(buf, max_data_size, pos)))
    {
      TBSYS_LOG(ERROR, "deserialize meta row error, err=%d", ret);
    }
    else if (pos != (writelen = write(fd, buf, pos)))
    {
      ret = OB_IO_ERROR;
      TBSYS_LOG(ERROR, "failed to write file, file=%s err=%s", filename, strerror(errno));
    }
    else
    {
      TBSYS_LOG(INFO, "store the first meta to file, file=%s", filename);
    }
    if (-1 != fd)
    {
      close(fd);
    }
    if (NULL != buf)
    {
      ob_free(buf);
      buf = NULL;
    }
  }
  return ret;
}
