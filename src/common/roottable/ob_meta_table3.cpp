/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_meta_table3.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_meta_table3.h"
#include "common/ob_scanner.h"
#include "common/ob_scan_param.h"
#include "common/ob_pool.h"
#include "common/utility.h"
#include "ob_meta_table_schema.h"
#include "common/ob_mutator.h"
using namespace oceanbase::common;

struct ObMetaTable3::MyIterator: public ConstIterator
{
  public:
    MyIterator(ObMetaTable3& aggregate,
               ConstIterator &my_meta,
               const KeyRange &search_range,
               ObScanner& scanner,
               const uint64_t tid,
               const ObString &tname,
               const uint64_t indexed_tid,
               const ObString &indexed_tname);
    ~MyIterator();

    int next(const ObTabletMetaTableRow *&row);
    int rewind();

    void* operator new (std::size_t size, const std::nothrow_t& nothrow_constant) throw();
    void operator delete (void* ptr) throw ();
  private:
    int add_scan_columns(ObScanParam &scan_param);
    int get_location(ConstIterator &meta_it, const ObRowkey &startkey, const bool inclusive_end,
                     ObScanner &location);
    int scan_beginning(ConstIterator &meta_it, const KeyRange &search_range, ObScanner& scanner);
    int check_rowkey_obj_type(int32_t rowkey_obj_idx, const ObObj &obj) const;
    int check_row(ObCellInfo *row_cells, int64_t cell_num);
    int cons_next_row(const ObTabletMetaTableRow *&row);
    int scan_more();
    int get_table_schema();
    bool is_startkey_in_range(const ObRowkey &startkey, const bool inclusive_end,
                         const ObTabletMetaTableRow &crow) const;
  private:
    static ObLockedPool MY_ITERATOR_POOL;
    // data members
    ObMetaTable3& aggr_;
    ConstIterator &my_meta_;
    KeyRange search_range_;
    ObScanner &scanner_;
    uint64_t my_tid_;
    ObString tname_;
    uint64_t indexed_tid_;
    ObString indexed_tname_;
    TableSchema table_schema_;  // used as cache
    ObTabletMetaTableRow curr_row_;
    bool began_iterate_;
    ObTabletMetaTableRow curr_meta_row_;
    ObObj startkey_objs_[OB_MAX_ROWKEY_COLUMN_NUMBER];
    int64_t read_scanner_row_count_;
};

ObLockedPool ObMetaTable3::MyIterator::MY_ITERATOR_POOL(sizeof(MyIterator));

ObMetaTable3::MyIterator::MyIterator(ObMetaTable3& aggregate, ConstIterator &my_meta,
                                     const KeyRange &search_range, ObScanner& scanner,
                                     const uint64_t tid, const ObString &tname,
                                     const uint64_t indexed_tid,
                                     const ObString &indexed_tname)
  :aggr_(aggregate), my_meta_(my_meta),
   search_range_(search_range), scanner_(scanner),
   my_tid_(tid), tname_(tname),
   indexed_tid_(indexed_tid), indexed_tname_(indexed_tname),
   began_iterate_(false),
   read_scanner_row_count_(0)
{
  table_schema_.table_id_ = OB_INVALID_ID;
  scanner_.reset();
  curr_meta_row_.set_tid(OB_INVALID_ID);
}

ObMetaTable3::MyIterator::~MyIterator()
{
}

void* ObMetaTable3::MyIterator::operator new (std::size_t size, const std::nothrow_t& nothrow_constant) throw()
{
  UNUSED(size);
  UNUSED(nothrow_constant);
  return MY_ITERATOR_POOL.alloc();
}

void ObMetaTable3::MyIterator::operator delete (void* ptr) throw ()
{
  MY_ITERATOR_POOL.free(ptr);
}

bool ObMetaTable3::MyIterator::is_startkey_in_range(const ObRowkey &startkey, const bool inclusive_startkey,
                                               const ObTabletMetaTableRow &crow) const
{
  bool ret = false;
  if (startkey > crow.get_start_key()
      && startkey <= crow.get_end_key())
  {
    ret = true;
  }
  else if (startkey == crow.get_start_key()
           && !inclusive_startkey)
  {
    ret = true;
  }
  return ret;
}

int ObMetaTable3::MyIterator::get_location(ConstIterator &meta_it, const ObRowkey &startkey,
                                           const bool inclusive_startkey, ObScanner &location)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID != curr_meta_row_.get_tid()) // whether curr_meta_row is valid
  {
    if (is_startkey_in_range(startkey, inclusive_startkey, curr_meta_row_))
    {
      ret = curr_meta_row_.add_into_scanner_as_old_roottable(location);
    }
    else
    {
      curr_meta_row_.set_tid(OB_INVALID_ID);
    }
  }
  if (OB_INVALID_ID == curr_meta_row_.get_tid())
  {
    const ObTabletMetaTable::Value *crow = NULL;
    while (OB_SUCCESS == meta_it.next(crow))
    {
      if (is_startkey_in_range(startkey, inclusive_startkey, *crow))
      {
        curr_meta_row_ = *crow;
        ret = curr_meta_row_.add_into_scanner_as_old_roottable(location);
        break;
      }
    }
    if (OB_INVALID_ID == curr_meta_row_.get_tid())
    {
      TBSYS_LOG(WARN, "failed to find the location, startkey=%s", to_cstring(startkey));
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "find the location, startkey=%s meta_range=%s:%s",
              to_cstring(startkey), to_cstring(curr_meta_row_.get_start_key()),
              to_cstring(curr_meta_row_.get_end_key()));
  }
  return ret;
}

int ObMetaTable3::MyIterator::add_scan_columns(ObScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = get_table_schema()))
  {
    TBSYS_LOG(WARN, "failed to get table schema, err=%d", ret);
  }
  else
  {
    ObMetaTableColumnSchema col_schema(table_schema_);
    for (int64_t i = 0; i < col_schema.get_columns_num(); ++i)
    {
      uint64_t cid = col_schema.get_cid_by_idx(i);
      if (OB_INVALID_ID == cid)
      {
        TBSYS_LOG(ERROR, "failed to get column id, i=%ld", i);
      }
      else if (OB_SUCCESS != (ret = scan_param.add_column(cid)))
      {
        TBSYS_LOG(WARN, "failed to add column, err=%d", ret);
        break;
      }
    }
  }
  return ret;
}

int ObMetaTable3::MyIterator::scan_beginning(ConstIterator &meta_it, const KeyRange &search_range, ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  scanner.reset();
  ObString null_string;
  ObScanParam *scan_param = aggr_.scan_param_allocator_.alloc();
  ObScanner *my_location = aggr_.scanner_allocator_.alloc();
  if (NULL == scan_param)
  {
    TBSYS_LOG(ERROR, "failed to alloc scan param");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (NULL == my_location)
  {
    TBSYS_LOG(ERROR, "failed to alloc scanner");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    scan_param->reset();
    my_location->reset();
    scan_param->set(my_tid_, null_string, search_range);
    if (OB_SUCCESS != (ret = scan_param->set_limit_info(0, SCAN_LIMIT_COUNT)))
    {
      TBSYS_LOG(WARN, "failed to set limit info, err=%d", ret);
    }
    // scan all columns
    else if (OB_SUCCESS != (ret = add_scan_columns(*scan_param)))
    {
      TBSYS_LOG(WARN, "failed to add scan columsn, err=%d", ret);
    }
    // find the locations of the tablet which the startkey is in
    else if (OB_SUCCESS != (ret = get_location(meta_it, search_range.start_key_,
                                               search_range.border_flag_.inclusive_start(), *my_location)))
    {
      TBSYS_LOG(WARN, "failed to get location, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = scan_param->set_location_info(*my_location)))
    {
      TBSYS_LOG(WARN, "failed to add location, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = aggr_.scan(*scan_param, scanner)))
    {
      TBSYS_LOG(WARN, "scan error, err=%d range=%s", ret, to_cstring(search_range));
    }
    else
    {
      scanner.reset_row_iter();
      read_scanner_row_count_ = 0;
      TBSYS_LOG(DEBUG, "scan meta table succ, range=%s", to_cstring(search_range));
    }
  }
  if (NULL != scan_param)
  {
    aggr_.scan_param_allocator_.free(scan_param);
  }
  if (NULL != my_location)
  {
    aggr_.scanner_allocator_.free(my_location);
  }
  return ret;
}

int ObMetaTable3::MyIterator::next(const ObTabletMetaTableRow *&row)
{
  int ret = OB_SUCCESS;
  if (began_iterate_)
  {
    if (OB_ITER_END == (ret = scanner_.next_row())) // finished the current scanner
    {
      bool is_fullfilled = false;
      int64_t item_num = 0;
      if (OB_SUCCESS != (ret = scanner_.get_is_req_fullfilled(is_fullfilled, item_num)))
      {
        TBSYS_LOG(WARN, "failed to get fullfilled, err=%d", ret);
      }
      else
      {
        if (is_fullfilled && read_scanner_row_count_ < SCAN_LIMIT_COUNT)
        {
          ret = OB_ITER_END;      // done
        }
        else
        {
          if (OB_SUCCESS != (ret = scan_more()))
          {
            if (OB_ITER_END != ret)
            {
              TBSYS_LOG(WARN, "failed to scan more, err=%d", ret);
            }
          }
          else
          {
            ret = cons_next_row(row);
            if (OB_SUCCESS == ret)
            {
              ++read_scanner_row_count_;
            }
          }
        }
      }
    }
    else if (OB_SUCCESS == ret)
    {
      ++read_scanner_row_count_;
      ret = cons_next_row(row);
    }
    else
    {
      TBSYS_LOG(WARN, "scanner failed to scan next, err=%d", ret);
    }
  }
  else
  {
    ObNewRange search_range = search_range_;
    search_range.end_key_ = ObRowkey::MAX_ROWKEY;
    search_range.border_flag_.set_inclusive_end();
    if (OB_SUCCESS != (ret = scan_beginning(my_meta_, search_range, scanner_)))
    {
      TBSYS_LOG(WARN, "failed to scan the beginning, err=%d", ret);
    }
    else
    {
      ret = cons_next_row(row);
      if (OB_SUCCESS == ret)
      {
        began_iterate_ = true;
        ++read_scanner_row_count_;
      }
    }
  }
  return ret;
}

int ObMetaTable3::MyIterator::scan_more()
{
  int ret = OB_SUCCESS;
  ObRowkey last_rowkey;
  if (OB_SUCCESS != (ret = scanner_.get_last_row_key(last_rowkey)))
  {
    TBSYS_LOG(ERROR, "failed to get range from scanner, err=%d", ret);
  }
  else if (last_rowkey == ObRowkey::MAX_ROWKEY) // safe check
  {
    TBSYS_LOG(DEBUG, "no more data to scan");
    ret = OB_ITER_END;
  }
  else
  {
    KeyRange new_range;
    new_range = search_range_;
    new_range.border_flag_.unset_inclusive_start();
    new_range.border_flag_.set_inclusive_end();
    new_range.start_key_ = last_rowkey;
    new_range.end_key_ = ObRowkey::MAX_ROWKEY;
    TBSYS_LOG(DEBUG, "scan more, read_scanner_row_count=%ld startkey=%s",
              read_scanner_row_count_, to_cstring(last_rowkey));
    if (OB_SUCCESS != (ret = scan_beginning(my_meta_, new_range, scanner_)))
    {
      TBSYS_LOG(WARN, "failed to scan more, err=%d", ret);
    }
  }
  return ret;
}

int ObMetaTable3::MyIterator::get_table_schema()
{
  int ret = OB_SUCCESS;
  if (my_tid_ != table_schema_.table_id_
      || OB_INVALID_ID == table_schema_.table_id_)
  {
    if (OB_SUCCESS != (ret = aggr_.get_table_schema(tname_, table_schema_)))
    {
      TBSYS_LOG(WARN, "failed to get table schema, err=%d", ret);
    }
    else if (TableSchema::META != table_schema_.table_type_)
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "wrong table type, tname=%.*s type=%d",
                tname_.length(), tname_.ptr(), table_schema_.table_type_);
    }
  }
  return ret;
}

int ObMetaTable3::MyIterator::check_rowkey_obj_type(int32_t rowkey_obj_idx, const ObObj &obj) const
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int k = 0; k < table_schema_.columns_.count(); ++k)
  {
    const ColumnSchema &cschema = table_schema_.columns_.at(k);
    if (rowkey_obj_idx + 1 == cschema.rowkey_id_) // rowkey_id start from 1
    {
      if (obj.get_type() != cschema.data_type_
          && (!obj.is_min_value())
          && (!obj.is_max_value()))
      {
        TBSYS_LOG(WARN, "invalid meta table row key, cid=%lu ctype=%d obj_type=%d",
                  cschema.column_id_, cschema.data_type_, obj.get_type());
        break;
      }
      found = true;
      break;
    }
  } // end for
  if (!found)
  {
    TBSYS_LOG(WARN, "rowkey obj not found in table schema, idx=%d", rowkey_obj_idx);
    ret = OB_ERR_DATA_FORMAT;
  }
  return ret;
}

int ObMetaTable3::MyIterator::check_row(ObCellInfo *row_cells, int64_t cell_num)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = get_table_schema()))
  {
    TBSYS_LOG(WARN, "failed to get table schema, err=%d", ret);
  }
  else
  {
    ObMetaTableColumnSchema col_schema(table_schema_);
    if (col_schema.get_columns_num() != cell_num)
    {
      ret = OB_ERR_DATA_FORMAT;
      TBSYS_LOG(WARN, "invalid meta table row, return_col_num=%ld schema_col_num=%ld",
              cell_num, col_schema.get_columns_num());
    }
    else
    {
      // check fixed columns
      for (int64_t i = 0; i < cell_num && OB_SUCCESS == ret; ++i)
      {
        // check table id
        if (my_tid_ != row_cells[i].table_id_)
        {
          ret = OB_ERR_DATA_FORMAT;
          TBSYS_LOG(WARN, "invalid meta table row, tid=%lu my_tid=%lu",
                    row_cells[i].table_id_, my_tid_);
        }
        // check rowkey/endkey objs, only check the first cell/column
        if (0 == i)
        {
          int64_t rowkey_obj_cnt = row_cells[i].row_key_.get_obj_cnt();
          if (0 >= rowkey_obj_cnt
              || table_schema_.rowkey_column_num_ != rowkey_obj_cnt)
          {
            ret = OB_ERR_DATA_FORMAT;
            TBSYS_LOG(WARN, "invalid meta table row, rowkey_obj_count=%ld rowkey_col_num=%d",
                      row_cells[i].row_key_.get_obj_cnt(), table_schema_.rowkey_column_num_);
          }
          else
          {
            // check rowkey obj type
            TBSYS_LOG(DEBUG, "check meta table rowkey=%s", to_cstring(row_cells[i].row_key_));
            const ObObj *rowkey_objs = row_cells[i].row_key_.get_obj_ptr();
            for (int j = 0; j < rowkey_obj_cnt; ++j)
            {
              if (OB_SUCCESS != (ret = check_rowkey_obj_type(j, rowkey_objs[j])))
              {
                TBSYS_LOG(WARN, "rowkey column not found or invalid type, j=%d", j);
                break;
              }
            }   // end for
          }
        }     // end if 0 == i
        // check value type
        if (i < col_schema.get_cidx(col_schema.CF_STARTKEY_COL, 0))
        {
          if (ObIntType != row_cells[i].value_.get_type())
          {
            ret = OB_ERR_DATA_FORMAT;
            TBSYS_LOG(WARN, "invalid meta table row, col=%ld type=%d",
                      i, row_cells[i].value_.get_type());
          }
        }
        else
        {
          // start key objs
          int64_t startkey_obj_idx = i - col_schema.get_cidx(col_schema.CF_STARTKEY_COL, 0);
          if (OB_SUCCESS != (ret = check_rowkey_obj_type(static_cast<int32_t>(startkey_obj_idx), row_cells[i].value_)))
          {
            TBSYS_LOG(WARN, "start key column not found or invalid type");
          }
        }
        // check indexed_tid
        if (col_schema.get_cidx(col_schema.CF_TID, 0) == i)
        {
          int64_t int_val = 0;
          row_cells[i].value_.get_int(int_val);
          if (static_cast<uint64_t>(int_val) != indexed_tid_)
          {
            ret = OB_ERR_DATA_FORMAT;
            TBSYS_LOG(WARN, "invalid meta table content, col_tid=%ld", int_val);
          }
        }
      } // end for
    }
  }
  return ret;
}

int ObMetaTable3::MyIterator::cons_next_row(const ObTabletMetaTableRow *&row)
{
  int ret = OB_SUCCESS;
  ObCellInfo *row_cells = NULL;
  int64_t cell_num = 0;
  if (OB_SUCCESS != (ret = scanner_.get_row(&row_cells, &cell_num)))
  {
    TBSYS_LOG(WARN, "failed to get row in scanner, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = check_row(row_cells, cell_num)))
  {
    TBSYS_LOG(WARN, "invalid first tablet entry row, err=%d", ret);
  }
  else
  {
    ObMetaTableColumnSchema col_schema(table_schema_);
    // 1. tid has been checked
    curr_row_.set_tid(indexed_tid_);
    curr_row_.set_table_name(indexed_tname_);
    // 2. replicas
    int64_t int_val = 0;
    ObTabletReplica replica;
    for (int i = 0; i < curr_row_.get_max_replica_count(); ++i)
    {
      row_cells[col_schema.get_cidx(col_schema.CF_VERSION, i)].value_.get_int(int_val);
      replica.version_ = int_val;
      row_cells[col_schema.get_cidx(col_schema.CF_ROW_COUNT, i)].value_.get_int(int_val);
      replica.row_count_ = int_val;
      row_cells[col_schema.get_cidx(col_schema.CF_SIZE, i)].value_.get_int(int_val);
      replica.occupy_size_ = int_val;
      row_cells[col_schema.get_cidx(col_schema.CF_CHECKSUM, i)].value_.get_int(int_val);
      replica.checksum_ = int_val;
      row_cells[col_schema.get_cidx(col_schema.CF_PORT, i)].value_.get_int(int_val);
      int32_t port = static_cast<int32_t>(int_val);
      row_cells[col_schema.get_cidx(col_schema.CF_IPV4, i)].value_.get_int(int_val);
      if (0 != int_val)
      {
        replica.cs_.set_ipv4_addr(static_cast<int32_t>(int_val), port);
      }
      else
      {
        // @todo ipv6
        TBSYS_LOG(WARN, "ipv6 not used yet");
      }
      curr_row_.set_replica(i, replica);
    } // end for
    if (table_schema_.rowkey_column_num_ > OB_MAX_ROWKEY_COLUMN_NUMBER)
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "too many rowkey columns, num=%d", table_schema_.rowkey_column_num_);
    }
    else
    {
      // 3. endkey is the rowkey
      curr_row_.set_end_key(row_cells[0].row_key_);
      // 4. startkey
      for (int32_t i = 0; i < table_schema_.rowkey_column_num_; ++i)
      {
        startkey_objs_[i] = row_cells[col_schema.get_cidx(col_schema.CF_STARTKEY_COL, i)].value_;
      }
      ObRowkey startkey;
      startkey.assign(startkey_objs_, table_schema_.rowkey_column_num_);
      curr_row_.set_start_key(startkey);

      // check whether finished
      if (curr_row_.get_start_key() >= search_range_.end_key_)
      {
        TBSYS_LOG(DEBUG, "finished iterate");
        ret = OB_ITER_END;
      }
      else
      {
        curr_row_.clear_changed();
        row = &curr_row_;
      }
    }
  }
  return ret;
}

int ObMetaTable3::MyIterator::rewind()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = my_meta_.rewind()))
  {
    TBSYS_LOG(WARN, "failed to rewind the meta iterator, err=%d", ret);
  }
  else
  {
    began_iterate_ = false;
    scanner_.reset();
    curr_meta_row_.set_tid(OB_INVALID_ID);
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObMetaTable3::ObMetaTable3(ObSchemaService &schema_service, ObMutator &mutator,
                           ObScanHelper &scan_helper, ObCachedAllocator<ObScanner> &scanner_allocator,
                           ObCachedAllocator<ObScanParam> &scan_param_allocator)
  :schema_service_(schema_service), mutator_(mutator),
   scan_helper_(scan_helper), scanner_allocator_(scanner_allocator),
   scan_param_allocator_(scan_param_allocator),
   my_meta_(NULL), my_tid_(OB_INVALID_ID), indexed_tid_(OB_INVALID_ID)
{
}

ObMetaTable3::~ObMetaTable3()
{
  destroy();
}

void ObMetaTable3::reset(ConstIterator *meta_it, const uint64_t tid, const ObString &tname,
                         const uint64_t indexed_tid, const ObString &indexed_tname)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tid
      || OB_FIRST_META_VIRTUAL_TID == tid
      || OB_FIRST_TABLET_ENTRY_TID == tid)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "invalid table id, tid=%lu", tid);
  }
  else if (OB_INVALID_ID == indexed_tid
           || OB_FIRST_META_VIRTUAL_TID == indexed_tid
           || OB_FIRST_TABLET_ENTRY_TID == indexed_tid)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "invalid table id, indexed_tid=%lu", indexed_tid);
  }
  else
  {
    my_meta_ = meta_it;         // can be NULL
    my_tid_ = tid;
    my_tname_ = tname;
    indexed_tid_ = indexed_tid;
    indexed_tname_ = indexed_tname;
  }
}

int ObMetaTable3::aquire_scanner(ObScanner *&scanner)
{
  int ret = OB_SUCCESS;
  if (NULL != scanner)
  {
    TBSYS_LOG(ERROR, "scanner is not NULL");
    ret = OB_ERR_UNEXPECTED;
  }
  else
  {
    scanner = scanner_allocator_.alloc();
    if (NULL == scanner)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (OB_SUCCESS != (ret = scanners_.push_back(scanner)))
    {
      TBSYS_LOG(WARN, "failed to push into array, err=%d", ret);
      scanner_allocator_.free(scanner);
      scanner = NULL;
    }
    else
    {
      scanner->reset();
    }
  }
  return ret;
}

int ObMetaTable3::aquire_iterator(ConstIterator &my_meta, const KeyRange &search_range,
                                  ObScanner& scanner, MyIterator *&it)
{
  int ret = OB_SUCCESS;
  if (NULL != it)
  {
    TBSYS_LOG(ERROR, "scanner is not NULL");
    ret = OB_ERR_UNEXPECTED;
  }
  else
  {
    it = new(std::nothrow) MyIterator(*this, my_meta, search_range, scanner,
                                      my_tid_, my_tname_, indexed_tid_, indexed_tname_);
    if (NULL == it)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (OB_SUCCESS != (ret = iterators_.push_back(it)))
    {
      TBSYS_LOG(WARN, "failed to push into array, err=%d", ret);
      delete it;
      it = NULL;
    }
  }
  return ret;
}

void ObMetaTable3::destroy()
{
  TBSYS_LOG(DEBUG, "destruct meta table, #iterators=%ld", iterators_.count());
  for (int i = 0; i < iterators_.count(); ++i)
  {
    MyIterator * it = iterators_.at(i);
    if (NULL != it)
    {
      delete it;
    }
  }
  iterators_.clear();
  TBSYS_LOG(DEBUG, "destruct meta table #scanners=%ld", scanners_.count());
  for (int i = 0; i < scanners_.count(); ++i)
  {
    ObScanner *scanner = scanners_.at(i);
    if (NULL != scanner)
    {
      scanner_allocator_.free(scanner);
    }
  }
  scanners_.clear();
}

int ObMetaTable3::check_tid(const uint64_t op_tid)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == my_tid_
      || OB_FIRST_META_VIRTUAL_TID == my_tid_
      || OB_FIRST_TABLET_ENTRY_TID == my_tid_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "invalid table id, tid=%lu", my_tid_);
  }
  else if (OB_INVALID_ID == indexed_tid_
           || OB_FIRST_META_VIRTUAL_TID == indexed_tid_
           || OB_FIRST_TABLET_ENTRY_TID == indexed_tid_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "invalid table id, tid=%lu", indexed_tid_);
  }
  else if (indexed_tid_ != op_tid)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "the table is not indexed by this meta table, meta_tid=%lu indexed_tid=%lu op_tid=%lu",
              my_tid_, indexed_tid_, op_tid);
  }
  return ret;
}

int ObMetaTable3::get_table_schema(const ObString &tname, TableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = schema_service_.get_table_schema(tname, table_schema)))
  {
    TBSYS_LOG(WARN, "failed to get table schema, err=%d", ret);
  }
  else if (TableSchema::META != table_schema.table_type_)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "this table is not meta table, tname=%.*s type=%d",
              tname.length(), tname.ptr(), table_schema.table_type_);
  }
  else if (0 >= table_schema.rowkey_column_num_)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "BUG rowkey column num=%d", table_schema.rowkey_column_num_);
  }
  return ret;
}

int ObMetaTable3::scan(const ObScanParam& scan_param, ObScanner &out) const
{
  return scan_helper_.scan(scan_param, out);
}

int ObMetaTable3::search(const KeyRange& key_range, ConstIterator *&first)
{
  int ret = OB_SUCCESS;
  ObScanner* scanner = NULL;
  MyIterator* it = NULL;
  if (OB_SUCCESS != (ret = check_range(key_range)))
  {
    TBSYS_LOG(ERROR, "invalid search range, range=%s", to_cstring(key_range));
  }
  else if (NULL == my_meta_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "my_meta_ is NULL");
  }
  else if (OB_SUCCESS != (ret = check_tid(key_range.table_id_)))
  {
    TBSYS_LOG(WARN, "check tid failed, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = aquire_scanner(scanner)))
  {
    TBSYS_LOG(WARN, "failed to get scanner, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = aquire_iterator(*my_meta_, key_range, *scanner, it)))
  {
    TBSYS_LOG(WARN, "failed to aquire iterator, err=%d", ret);
  }
  else
  {
    first = it;
  }
  return ret;
}

int ObMetaTable3::insert(const Value &v)
{
  int ret = OB_SUCCESS;
  TableSchema table_schema;
  if (OB_SUCCESS != (ret = check_tid(v.get_tid())))
  {
    TBSYS_LOG(WARN, "invalid table id=%lu", v.get_tid());
  }
  else if (OB_SUCCESS != (ret = get_table_schema(my_tname_, table_schema)))
  {
    TBSYS_LOG(WARN, "failed to get table schema, err=%d tname=%.*s",
              ret, my_tname_.length(), my_tname_.ptr());
  }
  else
  {
    ret = row_to_mutator(table_schema, v, true, mutator_);
  }
  return ret;
}

int ObMetaTable3::erase(const Key &k)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = check_tid(k.table_id_)))
  {
    TBSYS_LOG(WARN, "invalid table id=%lu", k.table_id_);
  }
  else if (OB_SUCCESS != (ret = mutator_.del_row(my_tid_, k.rowkey_)))
  {
    TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
  }
  else
  {
    TBSYS_LOG(DEBUG, "erase row, my_tid=%lu indexed_tid=%lu rowkey=%s",
              my_tid_, k.table_id_, to_cstring(k.rowkey_));
  }
  return ret;
}

int ObMetaTable3::row_to_mutator(const TableSchema &table_schema, const Value &v, const bool is_insert, ObMutator &mutator)
{
  int ret = OB_SUCCESS;
  ObMetaTableColumnSchema col_schema(table_schema);
  const ObRowkey &rowkey = v.get_end_key();
  ObObj value;
  for (int i = 0; i < v.get_max_replica_count(); ++i)
  {
    if (v.has_changed_replica(i) || is_insert)
    {
      value.set_int(v.get_replica(i).version_);
      if (OB_SUCCESS != (ret = mutator.update(my_tid_, rowkey, col_schema.get_cid(col_schema.CF_VERSION, i), value)))
      {
        TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
        break;
      }
      value.set_int(v.get_replica(i).row_count_);
      if (OB_SUCCESS != (ret = mutator.update(my_tid_, rowkey, col_schema.get_cid(col_schema.CF_ROW_COUNT, i), value)))
      {
        TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
        break;
      }
      value.set_int(v.get_replica(i).occupy_size_);
      if (OB_SUCCESS != (ret = mutator.update(my_tid_, rowkey, col_schema.get_cid(col_schema.CF_SIZE, i), value)))
      {
        TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
        break;
      }
      value.set_int(v.get_replica(i).checksum_);
      if (OB_SUCCESS != (ret = mutator.update(my_tid_, rowkey, col_schema.get_cid(col_schema.CF_CHECKSUM, i), value)))
      {
        TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
        break;
      }
      value.set_int(v.get_replica(i).cs_.get_port());
      if (OB_SUCCESS != (ret = mutator.update(my_tid_, rowkey, col_schema.get_cid(col_schema.CF_PORT, i), value)))
      {
        TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
        break;
      }
      if (ObServer::IPV4 == v.get_replica(i).cs_.get_version())
      {
        value.set_int(v.get_replica(i).cs_.get_ipv4());
        if (OB_SUCCESS != (ret = mutator.update(my_tid_, rowkey, col_schema.get_cid(col_schema.CF_IPV4, i), value)))
        {
          TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
          break;
        }
      }
      else
      {                         // ipv6
        value.set_int(v.get_replica(i).cs_.get_ipv6_high());
        if (OB_SUCCESS != (ret = mutator.update(my_tid_, rowkey, col_schema.get_cid(col_schema.CF_IPV6_HIGH, i), value)))
        {
          TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
          break;
        }
        value.set_int(v.get_replica(i).cs_.get_ipv6_low());
        if (OB_SUCCESS != (ret = mutator.update(my_tid_, rowkey, col_schema.get_cid(col_schema.CF_IPV6_LOW, i), value)))
        {
          TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
          break;
        }
      }
    }
  } // end for
  // startkey
  if (OB_SUCCESS == ret
      && (v.has_changed_start_key() || is_insert))
  {
    const ObRowkey &startkey = v.get_start_key();
    for (int i = 0; i < startkey.get_obj_cnt(); ++i)
    {
      if (OB_SUCCESS != (ret = mutator.update(my_tid_, rowkey, col_schema.get_cid(col_schema.CF_STARTKEY_COL, i),
                                              startkey.get_obj_ptr()[i])))
      {
        TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
        break;
      }
    }
  }
  if (OB_SUCCESS == ret && is_insert)
  {
    value.set_int(v.get_tid());
    if (OB_SUCCESS != (ret = mutator.update(my_tid_, rowkey, col_schema.get_cid(col_schema.CF_TID, 0), value)))
    {
      TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
    }
  }
  return ret;
}

int ObMetaTable3::update(const Value &v)
{
  int ret = OB_SUCCESS;
  TableSchema table_schema;
  if (OB_SUCCESS != (ret = check_tid(v.get_tid())))
  {
    TBSYS_LOG(WARN, "invalid table id=%lu", v.get_tid());
  }
  else if (OB_SUCCESS != (ret = get_table_schema(my_tname_, table_schema)))
  {
    TBSYS_LOG(WARN, "failed to get table schema, err=%d tname=%.*s",
              ret, my_tname_.length(), my_tname_.ptr());
  }
  else
  {
    ret = row_to_mutator(table_schema, v, false, mutator_);
  }
  return ret;
}

int ObMetaTable3::commit()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = scan_helper_.mutate(mutator_)))
  {
    TBSYS_LOG(WARN, "failed to apply mutator, err=%d", ret);
  }
  else
  {
    mutator_.reset();
    TBSYS_LOG(WARN, "directly call meta table's commit, tid=%lu", my_tid_);
  }
  return ret;
}

