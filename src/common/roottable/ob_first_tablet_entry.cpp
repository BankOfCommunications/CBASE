/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_first_tablet_entry.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "common/utility.h"
#include "common/ob_string.h"
#include "common/ob_scanner.h"
#include "common/ob_pool.h"
#include "ob_first_tablet_entry.h"
#include "ob_first_tablet_entry_schema.h"
#include "ob_meta_table_schema.h"
using namespace oceanbase::common;

struct ObFirstTabletEntryTable::MyIterator: public ObTabletMetaTableRowIterator
{
  public:
    MyIterator(ObScanner& scanner);
    ~MyIterator();

    int next(const ObTabletMetaTableRow *&row);
    int rewind();

    void* operator new (std::size_t size, const std::nothrow_t& nothrow_constant) throw();
    void operator delete (void* ptr) throw ();
  private:
    int cons_next_row(const ObTabletMetaTableRow *&row);
    int check_row(ObCellInfo *row_cells, int64_t cell_num) const;
  private:
    static ObLockedPool MY_ITERATOR_POOL;
    // data members
    ObScanner &scanner_;
    MyRow curr_row_;
};

ObLockedPool ObFirstTabletEntryTable::MyIterator::MY_ITERATOR_POOL(sizeof(MyIterator));

ObFirstTabletEntryTable::MyIterator::MyIterator(ObScanner &scanner)
  :scanner_(scanner)
{
  scanner_.reset_row_iter();
}

ObFirstTabletEntryTable::MyIterator::~MyIterator()
{
}

int ObFirstTabletEntryTable::MyIterator::next(const ObTabletMetaTableRow *&row)
{
  int ret = OB_SUCCESS;
  if (scanner_.is_empty())
  {
    ret = OB_ITER_END;
  }
  else if (OB_SUCCESS == (ret = scanner_.next_row()))
  {
    ret = cons_next_row(row);
  }
  else if (OB_ITER_END != ret)
  {
    TBSYS_LOG(WARN, "scanner failed to get next row, err=%d", ret);
  }
  return ret;
}

int ObFirstTabletEntryTable::MyIterator::cons_next_row(const ObTabletMetaTableRow *&row)
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
    curr_row_.set_start_key(ObRowkey::MIN_ROWKEY);
    curr_row_.set_end_key(ObRowkey::MAX_ROWKEY);
    int64_t int_val = 0;
    ObString varchar_val;
    row_cells[0].row_key_.get_obj_ptr()[0].get_varchar(varchar_val); // rowkey is the table name
    //add dolphin [database manager]@20150618:b
    ObString dbname;
    row_cells[0].row_key_.get_obj_ptr()[1].get_varchar(dbname);
    ObString dt;
    char buf[OB_MAX_DATBASE_NAME_LENGTH + OB_MAX_TABLE_NAME_LENGTH + 1];
    dt.assign(buf,OB_MAX_TABLE_NAME_LENGTH+OB_MAX_DATBASE_NAME_LENGTH+1);
    dt.concat(dbname,varchar_val);
    curr_row_.set_table_name(dt);
    //add:e
    //curr_row_.set_table_name(varchar_val);//delete dolphin [database manager]@20150610
    row_cells[SCHEMA.get_cidx(SCHEMA.CF_TID, 0)].value_.get_int(int_val);
    curr_row_.set_tid(int_val);
    row_cells[SCHEMA.get_cidx(SCHEMA.CF_META_TNAME, 0)].value_.get_varchar(varchar_val);
    curr_row_.set_meta_table_name(varchar_val);
    row_cells[SCHEMA.get_cidx(SCHEMA.CF_META_TID, 0)].value_.get_int(int_val);
    curr_row_.set_meta_tid(int_val);
    ObTabletReplica replica;
    for (int i = 0; i < MyRow::get_max_replica_count(); ++i)
    {
      row_cells[SCHEMA.get_cidx(SCHEMA.CF_VERSION, i)].value_.get_int(int_val);
      replica.version_ = int_val;
      row_cells[SCHEMA.get_cidx(SCHEMA.CF_ROW_COUNT, i)].value_.get_int(int_val);
      replica.row_count_ = int_val;
      row_cells[SCHEMA.get_cidx(SCHEMA.CF_SIZE, i)].value_.get_int(int_val);
      replica.occupy_size_ = int_val;
      row_cells[SCHEMA.get_cidx(SCHEMA.CF_CHECKSUM, i)].value_.get_int(int_val);
      replica.checksum_ = int_val;
      row_cells[SCHEMA.get_cidx(SCHEMA.CF_PORT, i)].value_.get_int(int_val);
      int32_t port = static_cast<int32_t>(int_val);
      row_cells[SCHEMA.get_cidx(SCHEMA.CF_IPV4, i)].value_.get_int(int_val);
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
    curr_row_.clear_changed();
    row = &curr_row_;
  }
  return ret;
}

int ObFirstTabletEntryTable::MyIterator::check_row(ObCellInfo *row_cells, int64_t cell_num) const
{
  int ret = OB_SUCCESS;
  if (SCHEMA.get_columns_num() != cell_num)
  {
    ret = OB_ERR_DATA_FORMAT;
    TBSYS_LOG(WARN, "invalid first tablet entry row, col_num=%ld expected=%lu",
              cell_num, SCHEMA.get_columns_num());
  }
  else
  {
    for (int64_t i = 0; i < cell_num && OB_SUCCESS == ret; ++i)
    {
      // check table id
      if (OB_FIRST_TABLET_ENTRY_TID != row_cells[i].table_id_)
      {
        ret = OB_ERR_DATA_FORMAT;
        TBSYS_LOG(WARN, "invalid first table entry row, tid=%lu",
                  row_cells[i].table_id_);
      }
      // check rowkey obj count
      if (1 != row_cells[i].row_key_.get_obj_cnt())
      {
        ret = OB_ERR_DATA_FORMAT;
        TBSYS_LOG(WARN, "invalid first table entry row, rowkey_obj_count=%ld",
                  row_cells[i].row_key_.get_obj_cnt());
      }
      // check rowkey obj type
      if (ObVarcharType != row_cells[i].row_key_.get_obj_ptr()[0].get_type())
      {
        ret = OB_ERR_DATA_FORMAT;
        TBSYS_LOG(WARN, "invalid first table entry row, type=%d",
                  row_cells[i].row_key_.get_obj_ptr()[0].get_type());
      }
      // check column id
      if (SCHEMA.get_cid_by_idx(i) != row_cells[i].column_id_)
      {
        ret = OB_ERR_DATA_FORMAT;
        TBSYS_LOG(WARN, "invalid first table entry row, col=%ld cid=%lu",
                  i, row_cells[i].column_id_);
      }
      // check value
      if (SCHEMA.get_cidx(SCHEMA.CF_META_TNAME, 0) == i)     // TNAME or META_TNAME
      {
        if (ObVarcharType != row_cells[i].value_.get_type())
        {
          ret = OB_ERR_DATA_FORMAT;
          TBSYS_LOG(WARN, "invalid first table entry row, col=%ld type=%d",
                    i, row_cells[i].value_.get_type());
        }
      }
      else
      {
        if (ObIntType != row_cells[i].value_.get_type())
        {
          ret = OB_ERR_DATA_FORMAT;
          TBSYS_LOG(WARN, "invalid first table entry row, col=%ld type=%d",
                    i, row_cells[i].value_.get_type());
        }
      }
    } // end for
  }
  return ret;
}

int ObFirstTabletEntryTable::MyIterator::rewind()
{
  int ret = OB_SUCCESS;
  scanner_.reset_row_iter();
  return ret;
}

void* ObFirstTabletEntryTable::MyIterator::operator new (std::size_t size, const std::nothrow_t& nothrow_constant) throw()
{
  UNUSED(size);
  UNUSED(nothrow_constant);
  return MY_ITERATOR_POOL.alloc();
}

void ObFirstTabletEntryTable::MyIterator::operator delete (void* ptr) throw ()
{
  MY_ITERATOR_POOL.free(ptr);
}

//////////////////////////////////////////////////////////////// ObFirstTableEntryTable
ObFirstTabletEntryColumnSchema ObFirstTabletEntryTable::SCHEMA; // my schema

ObFirstTabletEntryTable::ObFirstTabletEntryTable(ObSchemaService &schema_service, ObTabletMetaTable &my_meta, ObMutator &mutator,
                                                 ObScanHelper &scan_helper, ObCachedAllocator<ObScanner> &scanner_allocator,
                                                 ObCachedAllocator<ObScanParam> &scan_param_allocator)
  :schema_service_(schema_service), my_meta_(my_meta), mutator_(mutator),
   scan_helper_(scan_helper), scanner_allocator_(scanner_allocator),
   scan_param_allocator_(scan_param_allocator)
{
}

ObFirstTabletEntryTable::~ObFirstTabletEntryTable()
{
  destroy();
}

int ObFirstTabletEntryTable::check_state() const
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObFirstTabletEntryTable::aquire_scanner(ObScanner *&scanner)
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

int ObFirstTabletEntryTable::aquire_iterator(ObScanner& scanner, MyIterator *&it)
{
  int ret = OB_SUCCESS;
  if (NULL != it)
  {
    TBSYS_LOG(ERROR, "scanner is not NULL");
    ret = OB_ERR_UNEXPECTED;
  }
  else
  {
    it = new(std::nothrow) MyIterator(scanner);
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

void ObFirstTabletEntryTable::destroy()
{
  TBSYS_LOG(DEBUG, "destroy ObFirstTabletEntryTable #iterators=%ld", iterators_.count());
  for (int i = 0; i < iterators_.count(); ++i)
  {
    MyIterator * it = iterators_.at(i);
    if (NULL != it)
    {
      delete it;
    }
  }
  iterators_.clear();
  TBSYS_LOG(DEBUG, "destroy ObFirstTabletEntryTable #scanners=%ld", scanners_.count());
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

int ObFirstTabletEntryTable::get_location(ObScanner &scanner)
{
  int ret = OB_SUCCESS;
  ConstIterator *it = NULL;
  const Value* row = NULL;
  KeyRange range;
  range.table_id_ = OB_FIRST_TABLET_ENTRY_TID;
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  range.start_key_ = ObRowkey::MIN_ROWKEY;
  range.end_key_ = ObRowkey::MAX_ROWKEY;
  if (OB_SUCCESS != (ret = my_meta_.search(range, it)))
  {
    TBSYS_LOG(WARN, "failed to search the first meta, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = it->next(row)))
  {
    TBSYS_LOG(ERROR, "failed to read the first meta, err=%d", ret);
  }
  else
  {
    ret = row->add_into_scanner_as_old_roottable(scanner);
  }
  return ret;
}

int ObFirstTabletEntryTable::cons_scan_param(const ObNewRange &range, ObScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  ObString name_string;
  name_string.assign_ptr(const_cast<char*>(FIRST_TABLET_TABLE_NAME), static_cast<int32_t>(strlen(FIRST_TABLET_TABLE_NAME)));
  scan_param.reset();
  scan_param.set(OB_INVALID_ID, name_string, range);
  for (int i = 0; i < SCHEMA.get_columns_num(); ++i)
  {
    if (OB_SUCCESS != (ret = scan_param.add_column(SCHEMA.get_cid_by_idx(i))))
    {
      TBSYS_LOG(ERROR, "failed to add column name, err=%d", ret);
      break;
    }
  }
  if (OB_SUCCESS == ret)
  {
    ObScanner * my_location = NULL;
    if (OB_SUCCESS != (aquire_scanner(my_location)))
    {
      TBSYS_LOG(WARN, "failed to aquire scanner. err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = get_location(*my_location)))
    {
      TBSYS_LOG(WARN, "failed to get location, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = scan_param.set_location_info(*my_location)))
    {
      TBSYS_LOG(WARN, "failed to add location, err=%d", ret);
    }
  }
  return ret;
}

int ObFirstTabletEntryTable::get_meta_tid(const uint64_t tid, ObString &tname, uint64_t &meta_tid, ObString& meta_tname)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tid
      || OB_FIRST_META_VIRTUAL_TID == tid
      || OB_NOT_EXIST_TABLE_TID == tid)
  {
    TBSYS_LOG(ERROR, "invalid tid, tid=%lu", tid);
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_FIRST_TABLET_ENTRY_TID == tid)
  {
    tname = ObString::make_string(FIRST_TABLET_TABLE_NAME);
    meta_tid = OB_FIRST_META_VIRTUAL_TID;
  }
  else
  {
    KeyRange range;
    range.start_key_ = ObRowkey::MIN_ROWKEY;
    range.end_key_ = ObRowkey::MAX_ROWKEY;
    range.border_flag_.unset_inclusive_start();
    range.border_flag_.set_inclusive_end();
    range.table_id_ = tid;
    ConstIterator *it = NULL;
    const Value* row = NULL;
    if (OB_SUCCESS != (ret = this->search(range, it)))
    {
      TBSYS_LOG(WARN, "failed to search key, err=%d tid=%lu", ret, tid);
    }
    else if (OB_SUCCESS != (ret = it->next(row)))
    {
      TBSYS_LOG(WARN, "table not exist, err=%d tid=%lu", ret, tid);
      ret = OB_ENTRY_NOT_EXIST;
    }
    else
    {
      const MyRow *my_row = static_cast<const MyRow*>(row);
      if (OB_SUCCESS != (ret = string_pool_.write_string(my_row->get_table_name(), &tname)))
      {
        TBSYS_LOG(WARN, "failed to copy table name");
      }
      else if (OB_SUCCESS != (ret = string_pool_.write_string(my_row->get_meta_table_name(), &meta_tname)))
      {
        TBSYS_LOG(WARN, "failed to copy table name");
      }
      else
      {
        meta_tid = my_row->get_meta_tid();
      }
    }
  }
  return ret;
}

int ObFirstTabletEntryTable::scan(const ObScanParam& scan_param, ObScanner &out) const
{
  return scan_helper_.scan(scan_param, out);
}

int ObFirstTabletEntryTable::scan_row(const ObString &tname, ObScanner *&out)
{
  int ret = OB_SUCCESS;
  ObScanParam *scan_param = scan_param_allocator_.alloc();
  ObObj key[1];
  key[0].set_varchar(tname);
  ObNewRange single_key_range;
  single_key_range.border_flag_.set_inclusive_start();
  single_key_range.border_flag_.set_inclusive_end();
  single_key_range.start_key_.assign(key, 1);
  single_key_range.end_key_.assign(key, 1);
  out = NULL;
  if (NULL == scan_param)
  {
    TBSYS_LOG(ERROR, "failed to alloc scan param");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (NULL != out)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "out is not null");
  }
  else if (OB_SUCCESS != (ret = cons_scan_param(single_key_range, *scan_param)))
  {
    TBSYS_LOG(WARN, "failed to cons scan param, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = aquire_scanner(out)))
  {
    TBSYS_LOG(ERROR, "failed to aquire scanner");
  }
  else if (OB_SUCCESS != (ret = scan(*scan_param, *out)))
  {
    TBSYS_LOG(WARN, "failed to scan, err=%d", ret);
  }
  if (NULL != scan_param)
  {
    scan_param_allocator_.free(scan_param);
  }
  return ret;
}

int ObFirstTabletEntryTable::search(const KeyRange& key_range, ConstIterator *&first)
{
  int ret = OB_SUCCESS;
  ObString tname;
  MyIterator *it = NULL;
  ObScanner *scanner = NULL;

  if (OB_INVALID_ID == key_range.table_id_
      || OB_FIRST_META_VIRTUAL_TID == key_range.table_id_
      || OB_FIRST_TABLET_ENTRY_TID == key_range.table_id_)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "invalid search key, tid=%lu", key_range.table_id_);
  }
  else if (OB_SUCCESS != (ret = check_range(key_range)))
  {
    TBSYS_LOG(ERROR, "invalid search range, range=%s", to_cstring(key_range));
  }
  else if (OB_SUCCESS != (ret = check_state()))
  {
    TBSYS_LOG(ERROR, "not init");
  }
  else if (OB_SUCCESS != (ret = table_id_to_name(key_range.table_id_, tname)))
  {
    TBSYS_LOG(WARN, "failed to get name from tid, err=%d tid=%lu", ret, key_range.table_id_);
  }
  else if (OB_SUCCESS != (ret = scan_row(tname, scanner)))
  {
    TBSYS_LOG(WARN, "failed to scan row, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = aquire_iterator(*scanner, it)))
  {
    TBSYS_LOG(ERROR, "failed to aquire iterator");
  }
  else
  {
    first = it;
  }
  return ret;
}

// int ObFirstTabletEntryTable::table_id_to_name(const uint64_t tid, ObString &name)
// {
//   int ret = OB_SUCCESS;
//   if (OB_INVALID_ID == tid
//       || OB_FIRST_META_VIRTUAL_TID == tid
//       || OB_FIRST_TABLET_ENTRY_TID == tid)
//   {
//     ret = OB_INVALID_ARGUMENT;
//     TBSYS_LOG(WARN, "invalid table, tid=%lu", tid);
//   }
//   else
//   {
//     //@todo ret = schema_service_.get_table_name(tid, name);
//     UNUSED(name);
//   }
//   return ret;
// }

int ObFirstTabletEntryTable::table_id_to_name(const uint64_t tid, ObString &name)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tid
      || OB_FIRST_META_VIRTUAL_TID == tid
      || OB_FIRST_TABLET_ENTRY_TID == tid)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid table, tid=%lu", tid);
  }
  else
  {
    if (OB_SUCCESS != (ret = table_id_to_name_in_cache(tid, name)))
    {
      ret = table_id_to_name_by_scan(tid, name);
    }
  }
  return ret;
}

int ObFirstTabletEntryTable::table_id_to_name_in_cache(const uint64_t tid, ObString &name)
{
  int ret = OB_ENTRY_NOT_EXIST;
  int64_t s = id_name_map_.count();
  for (int i = 0; i < s; ++i)
  {
    const IdToName &id_name = id_name_map_.at(i);
    if (tid == id_name.first)
    {
      name.assign_ptr(const_cast<char*>(id_name.second.ptr()), id_name.second.length());
      ret = OB_SUCCESS;
      break;
    }
  }
  return ret;
}

int ObFirstTabletEntryTable::table_id_to_name_in_scanner(ObScanner& scanner, const uint64_t tid, ObString &name, bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  MyIterator it(scanner);
  const Value *row = NULL;
  const MyRow *mrow = NULL;
  while(OB_SUCCESS == ret && OB_SUCCESS == it.next(row))
  {
    mrow = static_cast<const MyRow*>(row);
    // add into cache
    ObString tname_copy;
    if (OB_SUCCESS != (ret = string_pool_.write_string(mrow->get_table_name(), &tname_copy)))
    {
      TBSYS_LOG(ERROR, "failed to copy table name, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = id_name_map_.push_back(std::make_pair(mrow->get_tid(), tname_copy))))
    {
      TBSYS_LOG(ERROR, "failed to push into array, err=%d", ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "scan first tablet table, tname=%.*s tid=%lu",
                tname_copy.length(), tname_copy.ptr(), mrow->get_tid());
    }
    if (mrow->get_tid() == tid)
    {
      name.assign_ptr(const_cast<char*>(tname_copy.ptr()), tname_copy.length());
      found = true;
    }
  }
  if (OB_ITER_END == ret)
  {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObFirstTabletEntryTable::cons_scan_param(const ObRowkey &start_key, ObScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  scan_param.reset();
  ObNewRange range;
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.set_inclusive_end();
  range.start_key_ = start_key;
  range.end_key_ = ObRowkey::MAX_ROWKEY;
  ret = cons_scan_param(range, scan_param);
  return ret;
}

int ObFirstTabletEntryTable::table_id_to_name_by_scan(const uint64_t tid, ObString &name)
{
  int ret = OB_SUCCESS;
  ObRowkey start_key = ObRowkey::MIN_ROWKEY;
  ObScanner *scanner = NULL;
  ObScanParam *scan_param = scan_param_allocator_.alloc();
  if (NULL == scan_param)
  {
    TBSYS_LOG(ERROR, "failed to alloc scan param");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (OB_SUCCESS != (ret = aquire_scanner(scanner)))
  {
    TBSYS_LOG(ERROR, "failed to aquire scanner");
  }
  else
  {
    scan_param->reset();
  }
  bool found = false;
  while(!found && OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = cons_scan_param(start_key, *scan_param)))
    {
      TBSYS_LOG(WARN, "failed to cons scan param, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = scan(*scan_param, *scanner)))
    {
      TBSYS_LOG(WARN, "failed to scan, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = table_id_to_name_in_scanner(*scanner, tid, name, found)))
    {
      TBSYS_LOG(WARN, "failed to find in scanner, err=%d", ret);
    }
    else if (!found)
    {
      bool is_fullfilled = false;
      int64_t item_num = 0;
      if (OB_SUCCESS != (ret = scanner->get_is_req_fullfilled(is_fullfilled, item_num)))
      {
        TBSYS_LOG(WARN, "scanner get fullfilled error, err=%d", ret);
      }
      else
      {
        if (is_fullfilled)
        {
          ret = OB_ENTRY_NOT_EXIST;
        }
        else
        {
          if (OB_SUCCESS != (ret = scanner->get_last_row_key(start_key)))
          {
            TBSYS_LOG(WARN, "failed to get last rowkey, err=%d", ret);
          }
        }
      }
    } // end if (!found)
  } // end while
  if (NULL != scan_param)
  {
    scan_param_allocator_.free(scan_param);
  }
  return ret;
}

inline void ObFirstTabletEntryTable::fill_int_column(ColumnSchema &col_schema) const
{
  col_schema.column_group_id_ = OB_DEFAULT_COLUMN_GROUP_ID;
  col_schema.rowkey_id_ = 0;
  col_schema.join_table_id_ = OB_NOT_EXIST_TABLE_TID;
  col_schema.join_column_id_ = OB_NOT_EXIST_COLUMN_ID;
  col_schema.data_type_ = ObIntType;
  col_schema.data_length_ = sizeof(int64_t);
  col_schema.data_precision_ = 0;
  col_schema.data_scale_ = 0;
  col_schema.nullable_ = false;
}

int ObFirstTabletEntryTable::cons_meta_table_schema(const uint64_t tid, const TableSchema &oschema,
                                                    TableSchema &tschema) const
{
  int ret = OB_SUCCESS;

  snprintf(tschema.table_name_, OB_MAX_TABLE_NAME_LENGTH, "%s%s",
           OB_META_TABLE_NAME_PREFIX, oschema.table_name_);
  tschema.table_id_ = tid;
  tschema.table_type_ = TableSchema::META;
  tschema.load_type_ = TableSchema::DISK;
  tschema.table_def_type_ = TableSchema::INTERNAL;
  tschema.rowkey_column_num_ = oschema.rowkey_column_num_;
  tschema.replica_num_ = oschema.replica_num_;
  tschema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID;
  // add fixed columns
  for (int64_t i = 0; i < ObMetaTableColumnSchema::get_fixed_columns_num(); ++i)
  {
    ColumnSchema col_schema;
    const ObString &cname = ObMetaTableColumnSchema::get_fixed_cname_by_idx(i);
    snprintf(col_schema.column_name_, OB_MAX_COLUMN_NAME_LENGTH, "%.*s",
             cname.length(), cname.ptr());
    col_schema.column_id_ = tschema.max_used_column_id_++;
    fill_int_column(col_schema);

    if (OB_SUCCESS != (ret = tschema.columns_.push_back(col_schema)))
    {
      TBSYS_LOG(WARN, "failed to push into col array, err=%d", ret);
      break;
    }
  } // end for

  if (OB_SUCCESS == ret)
  {
    // add endkey & startkey columns
    int32_t rowkey_id = 1;
    for (int32_t j = 0; j < oschema.columns_.count(); ++j)
    {
      const ColumnSchema& ocschema = oschema.columns_.at(j);
      if (rowkey_id == ocschema.rowkey_id_)
      {
        ColumnSchema col_schema;
        snprintf(col_schema.column_name_, OB_MAX_COLUMN_NAME_LENGTH, "%.*s%d",
                 meta_table_cname::ENDKEY_OBJ_PREFIX.length(),
                 meta_table_cname::ENDKEY_OBJ_PREFIX.ptr(), rowkey_id);

        col_schema.column_id_ = tschema.max_used_column_id_++;
        col_schema.column_group_id_ = OB_DEFAULT_COLUMN_GROUP_ID;
        col_schema.rowkey_id_ = ocschema.rowkey_id_;
        col_schema.join_table_id_ = OB_NOT_EXIST_TABLE_TID;
        col_schema.join_column_id_ = OB_NOT_EXIST_COLUMN_ID;
        col_schema.data_type_ = ocschema.data_type_;
        col_schema.data_length_ = ocschema.data_length_;
        col_schema.data_precision_ = ocschema.data_precision_;
        col_schema.data_scale_ = ocschema.data_scale_;
        col_schema.nullable_ = false;

        if (OB_SUCCESS != (ret = tschema.columns_.push_back(col_schema)))
        {
          TBSYS_LOG(WARN, "failed to push into column array, err=%d", ret);
          break;
        }

        snprintf(col_schema.column_name_, OB_MAX_COLUMN_NAME_LENGTH, "%.*s%d",
                 meta_table_cname::STARTKEY_OBJ_PREFIX.length(),
                 meta_table_cname::STARTKEY_OBJ_PREFIX.ptr(), rowkey_id);
        col_schema.column_id_ = tschema.max_used_column_id_++;
        if (OB_SUCCESS != (ret = tschema.columns_.push_back(col_schema)))
        {
          TBSYS_LOG(WARN, "failed to push into column array, err=%d", ret);
        }

        if (rowkey_id >= tschema.rowkey_column_num_)
        {
          break;
        }
        rowkey_id++;
      }
    }   // end for
    if (rowkey_id < tschema.rowkey_column_num_)
    {
      TBSYS_LOG(ERROR, "no rowkey column found, i=%d", rowkey_id);
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int ObFirstTabletEntryTable::create_meta_table(const ObString &indexed_tname, ObString &meta_tname,
                                               uint64_t &meta_tid)
{
  int ret = OB_SUCCESS;
  TableSchema oschema;
  TableSchema tschema;
  uint64_t max_used_tid = OB_INVALID_ID;
  if (OB_SUCCESS != (ret = schema_service_.get_max_used_table_id(max_used_tid)))
  {
    TBSYS_LOG(WARN, "failed to get max used tid, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = schema_service_.get_table_schema(indexed_tname, oschema)))
  {
    TBSYS_LOG(WARN, "failed to get table schema, err=%d tname=%.*s",
              ret, indexed_tname.length(), indexed_tname.ptr());
  }
  else if (OB_META_TABLE_NAME_PREFIX_LENGTH + static_cast<int64_t>(strlen(oschema.table_name_)) + 1
           > OB_MAX_TABLE_NAME_LENGTH)
  {
    ret = OB_BUF_NOT_ENOUGH;
    TBSYS_LOG(ERROR, "meta table name too long, tname=%s", oschema.table_name_);
  }
  else if (OB_SUCCESS != (ret = cons_meta_table_schema(max_used_tid+1, oschema, tschema)))
  {
    TBSYS_LOG(WARN, "failed to construct the new schema, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = schema_service_.create_table(tschema)))
  {
    TBSYS_LOG(WARN, "failed to create table, err=%d tname=%s", ret, tschema.table_name_);
  }
  else
  {
    TBSYS_LOG(INFO, "created meta table, tname=%s", tschema.table_name_);
    int32_t tname_len = static_cast<int32_t>(strlen(tschema.table_name_));
    if (NULL != meta_tname.ptr()
        && tname_len <= meta_tname.size())
    {
      meta_tname.write(tschema.table_name_, tname_len);
    }
    else
    {
      TBSYS_LOG(ERROR, "meta_tname's buffer not enough, tname_len=%d", tname_len);
    }
    meta_tid = tschema.table_id_;
  }
  return ret;
}

int ObFirstTabletEntryTable::mutator_init_row(const ObString &table_name, const uint64_t tid, ObMutator &mutator)
{
  int ret = OB_SUCCESS;
  mutator.reset();
  //delete dolphin [database manager]@20150616:b
/*  ObObj rowkey_objs[1];
  rowkey_objs[0].set_varchar(tname);
  ObRowkey rowkey(rowkey_objs, 1);*/
  //delete:e
  //add dolphin [database manager]@20150616:b
  ObObj table_name_obj[2];
  char dn[OB_MAX_DATBASE_NAME_LENGTH];
  char tn[OB_MAX_TABLE_NAME_LENGTH];
  ObString dname;
  ObString tname;
  dname.assign_buffer(dn,OB_MAX_DATBASE_NAME_LENGTH);
  tname.assign_buffer(tn,OB_MAX_TABLE_NAME_LENGTH);
  table_name.split_two(dname,tname);
  table_name_obj[0].set_varchar(tname);
  table_name_obj[1].set_varchar(dname);
  ObRowkey rowkey(table_name_obj,2);
  //add:e
  ObObj value;
  for (int i = 0; i < Value::get_max_replica_count(); ++i)
  {
    value.set_int(OB_INVALID_VERSION);
    if (OB_SUCCESS != (ret = mutator.update(OB_FIRST_TABLET_ENTRY_TID, rowkey, SCHEMA.get_cid(SCHEMA.CF_VERSION, i), value)))
    {
      TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
      break;
    }
    value.set_int(0);
    if (OB_SUCCESS != (ret = mutator.update(OB_FIRST_TABLET_ENTRY_TID, rowkey, SCHEMA.get_cid(SCHEMA.CF_ROW_COUNT, i), value)))
    {
      TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
      break;
    }
    value.set_int(0);
    if (OB_SUCCESS != (ret = mutator.update(OB_FIRST_TABLET_ENTRY_TID, rowkey, SCHEMA.get_cid(SCHEMA.CF_SIZE, i), value)))
    {
      TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
      break;
    }
    value.set_int(0);
    if (OB_SUCCESS != (ret = mutator.update(OB_FIRST_TABLET_ENTRY_TID, rowkey, SCHEMA.get_cid(SCHEMA.CF_CHECKSUM, i), value)))
    {
      TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
      break;
    }
    value.set_int(0);
    if (OB_SUCCESS != (ret = mutator.update(OB_FIRST_TABLET_ENTRY_TID, rowkey, SCHEMA.get_cid(SCHEMA.CF_PORT, i), value)))
    {
      TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
      break;
    }
    value.set_int(0);
    if (OB_SUCCESS != (ret = mutator.update(OB_FIRST_TABLET_ENTRY_TID, rowkey, SCHEMA.get_cid(SCHEMA.CF_IPV4, i), value)))
    {
      TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
      break;
    }
    value.set_int(0);
    if (OB_SUCCESS != (ret = mutator.update(OB_FIRST_TABLET_ENTRY_TID, rowkey, SCHEMA.get_cid(SCHEMA.CF_IPV6_HIGH, i), value)))
    {
      TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
      break;
    }
    value.set_int(0);
    if (OB_SUCCESS != (ret = mutator.update(OB_FIRST_TABLET_ENTRY_TID, rowkey, SCHEMA.get_cid(SCHEMA.CF_IPV6_LOW, i), value)))
    {
      TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
      break;
    }
  } // end for
  if (OB_SUCCESS == ret)
  {
    value.set_int(OB_FIRST_TABLET_ENTRY_TID);
    if (OB_SUCCESS != (ret = mutator.update(OB_FIRST_TABLET_ENTRY_TID, rowkey, SCHEMA.get_cid(SCHEMA.CF_META_TID, 0), value)))
    {
      TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    value.set_varchar(ObString::make_string(FIRST_TABLET_TABLE_NAME));
    if (OB_SUCCESS != (ret = mutator.update(OB_FIRST_TABLET_ENTRY_TID, rowkey, SCHEMA.get_cid(SCHEMA.CF_META_TNAME, 0), value)))
    {
      TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    // unnecessary but needed in mock testing
    value.set_int(tid);
    if (OB_SUCCESS != (ret = mutator.update(OB_FIRST_TABLET_ENTRY_TID, rowkey, SCHEMA.get_cid(SCHEMA.CF_TID, 0), value)))
    {
      TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
    }
  }
  return ret;
}

int ObFirstTabletEntryTable::mutator_set_meta_tid_name(const ObString &tname, const uint64_t meta_tid,
                                            const ObString &meta_tname, ObMutator &mutator)
{
  int ret = OB_SUCCESS;
  mutator.reset();
  ObObj rowkey_objs[1];
  rowkey_objs[0].set_varchar(tname);
  ObRowkey rowkey(rowkey_objs, 1);
  ObObj obj_meta_tid;
  obj_meta_tid.set_int(meta_tid);
  ObObj obj_meta_tname;
  obj_meta_tname.set_varchar(meta_tname);

  if (OB_SUCCESS != (ret = mutator.update(OB_FIRST_TABLET_ENTRY_TID, rowkey,
                                          SCHEMA.get_cid(SCHEMA.CF_META_TID, 0), obj_meta_tid)))
  {
    TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = mutator.update(OB_FIRST_TABLET_ENTRY_TID, rowkey,
                                               SCHEMA.get_cid(SCHEMA.CF_META_TNAME, 0), obj_meta_tname)))
  {
    TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
  }
  return ret;
}

int ObFirstTabletEntryTable::mutator_write_meta_table(const ObString &meta_tname, const uint64_t indexed_tid,
                                                      ObMutator &mutator)
{
  int ret = OB_SUCCESS;
  TableSchema tschema;
  ObNewRange search_range;
  search_range.start_key_ = ObRowkey::MIN_ROWKEY;
  search_range.end_key_ = ObRowkey::MAX_ROWKEY;
  search_range.border_flag_.unset_inclusive_start();
  search_range.border_flag_.set_inclusive_end();
  search_range.table_id_ = indexed_tid;
  ObTabletMetaTableRowIterator *it = NULL;
  const ObTabletMetaTableRow *crow = NULL;
  if (OB_SUCCESS != (ret = this->search(search_range, it)))
  {
    TBSYS_LOG(WARN, "failed to search, err=%d tid=%lu", ret, indexed_tid);
  }
  else if (OB_SUCCESS != (ret = it->next(crow)))
  {
    TBSYS_LOG(WARN, "failed to get next row, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = schema_service_.get_table_schema(meta_tname, tschema)))
  {
    TBSYS_LOG(WARN, "failed to get table schema, err=%d tname=%.*s",
              ret, meta_tname.length(), meta_tname.ptr());
  }
  else
  {
    TBSYS_LOG(DEBUG, "copy replicas from the only tablet for table, tid=%lu", crow->get_tid());
    ObMetaTableColumnSchema SCHEMA(tschema);
    ObObj rowkey_objs[OB_MAX_ROWKEY_COLUMN_NUMBER];
    OB_ASSERT(tschema.rowkey_column_num_ <= OB_MAX_ROWKEY_COLUMN_NUMBER);
    for (int64_t i = 0; i < tschema.rowkey_column_num_; ++i)
    {
      rowkey_objs[i].set_max_value();
    }
    ObRowkey rowkey;
    rowkey.assign(rowkey_objs, tschema.rowkey_column_num_);
    ObObj value;
    for (int32_t i = 0; i < crow->get_max_replica_count(); ++i)
    {
      const ObTabletReplica &r = crow->get_replica(i);
      value.set_int(r.version_);
      if (OB_SUCCESS != (ret = mutator.update(tschema.table_id_, rowkey, SCHEMA.get_cid(SCHEMA.CF_VERSION, i), value)))
      {
        TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
        break;
      }
      value.set_int(r.row_count_);
      if (OB_SUCCESS != (ret = mutator.update(tschema.table_id_, rowkey, SCHEMA.get_cid(SCHEMA.CF_ROW_COUNT, i), value)))
      {
        TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
        break;
      }
      value.set_int(r.occupy_size_);
      if (OB_SUCCESS != (ret = mutator.update(tschema.table_id_, rowkey, SCHEMA.get_cid(SCHEMA.CF_SIZE, i), value)))
      {
        TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
        break;
      }
      value.set_int(r.checksum_);
      if (OB_SUCCESS != (ret = mutator.update(tschema.table_id_, rowkey, SCHEMA.get_cid(SCHEMA.CF_CHECKSUM, i), value)))
      {
        TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
        break;
      }
      value.set_int(r.cs_.get_port());
      if (OB_SUCCESS != (ret = mutator.update(tschema.table_id_, rowkey, SCHEMA.get_cid(SCHEMA.CF_PORT, i), value)))
      {
        TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
        break;
      }
      value.set_int(r.cs_.get_ipv4());
      if (OB_SUCCESS != (ret = mutator.update(tschema.table_id_, rowkey, SCHEMA.get_cid(SCHEMA.CF_IPV4, i), value)))
      {
        TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
        break;
      }
      value.set_int(0);
      if (OB_SUCCESS != (ret = mutator.update(tschema.table_id_, rowkey, SCHEMA.get_cid(SCHEMA.CF_IPV6_HIGH, i), value)))
      {
        TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
        break;
      }
      value.set_int(0);
      if (OB_SUCCESS != (ret = mutator.update(tschema.table_id_, rowkey, SCHEMA.get_cid(SCHEMA.CF_IPV6_LOW, i), value)))
      {
        TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
        break;
      }
    }

    if (OB_SUCCESS == ret)
    {
      // tid
      value.set_int(indexed_tid);
      if (OB_SUCCESS != (ret = mutator.update(tschema.table_id_, rowkey, SCHEMA.get_cid(SCHEMA.CF_TID, 0), value)))
      {
        TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      // startkey columns
      int32_t rowkey_id = 1;
      value.set_min_value();
      for (int32_t j = 0; j < tschema.columns_.count(); ++j)
      {
        const ColumnSchema& cschema = tschema.columns_.at(j);
        if (rowkey_id == cschema.rowkey_id_)
        {
          if (OB_SUCCESS != (ret = mutator.update(tschema.table_id_, rowkey, cschema.column_id_, value)))
          {
            TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
            break;
          }
          if (rowkey_id >= tschema.rowkey_column_num_)
          {
            break;
          }
          rowkey_id++;
        }
      }   // end for
      if (rowkey_id < tschema.rowkey_column_num_)
      {
        TBSYS_LOG(ERROR, "no rowkey column found, i=%d", rowkey_id);
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }
  return ret;
}

int ObFirstTabletEntryTable::insert(const Value &v)
{
  int ret = OB_SUCCESS;
  ObMutator mutator;
  uint64_t meta_tid = OB_INVALID_ID;
  ObString meta_tname;
  char meta_tname_buf[OB_MAX_TABLE_NAME_LENGTH];
  meta_tname.assign_buffer(meta_tname_buf, OB_MAX_TABLE_NAME_LENGTH);

  if (OB_SUCCESS != (ret = create_meta_table(v.get_table_name(), meta_tname, meta_tid)))
  {
    TBSYS_LOG(WARN, "failed to create meta table, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = mutator_init_row(meta_tname, meta_tid, mutator)))
  {
    TBSYS_LOG(WARN, "failed to fill mutator, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = scan_helper_.mutate(mutator)))
  {
    TBSYS_LOG(WARN, "failed to apply mutator, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = mutator.reset())
           || OB_SUCCESS != (ret = mutator_write_meta_table(meta_tname, v.get_tid(), mutator)))
  {
    TBSYS_LOG(WARN, "failed to fill mutator, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = scan_helper_.mutate(mutator)))
  {
    TBSYS_LOG(WARN, "failed to apply mutator, err=%d", ret);
  }
  // modify the current table's meta table
  else if (OB_SUCCESS != (ret = mutator.reset())
           || OB_SUCCESS != (ret = mutator_set_meta_tid_name(v.get_table_name(), meta_tid, meta_tname, mutator)))
  {
    TBSYS_LOG(WARN, "failed to fill mutator, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = scan_helper_.mutate(mutator)))
  {
    TBSYS_LOG(WARN, "failed to apply mutator, err=%d", ret);
  }
  else
  {
    // finally, let the caller try again
    ret = OB_EAGAIN;
  }
  return ret;
}


int ObFirstTabletEntryTable::erase(const Key &k)
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(WARN, "try to delete table from the first_tablet_entry_table, tid=%lu", k.table_id_);
  // do nothing, tablet entry in first table cannot be erased directly
  return ret;
}

int ObFirstTabletEntryTable::row_to_mutator(const Value &v, ObMutator &mutator)
{
  int ret = OB_SUCCESS;
  ObObj rowkey_objs[1];
  rowkey_objs[0].set_varchar(v.get_table_name());
  ObRowkey rowkey(rowkey_objs, 1);
  ObObj value;
  for (int i = 0; i < v.get_max_replica_count(); ++i)
  {
    if (v.has_changed_replica(i))
    {
      value.set_int(v.get_replica(i).version_);
      if (OB_SUCCESS != (ret = mutator.update(OB_FIRST_TABLET_ENTRY_TID, rowkey, SCHEMA.get_cid(SCHEMA.CF_VERSION, i), value)))
      {
        TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
        break;
      }
      value.set_int(v.get_replica(i).row_count_);
      if (OB_SUCCESS != (ret = mutator.update(OB_FIRST_TABLET_ENTRY_TID, rowkey, SCHEMA.get_cid(SCHEMA.CF_ROW_COUNT, i), value)))
      {
        TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
        break;
      }
      value.set_int(v.get_replica(i).occupy_size_);
      if (OB_SUCCESS != (ret = mutator.update(OB_FIRST_TABLET_ENTRY_TID, rowkey, SCHEMA.get_cid(SCHEMA.CF_SIZE, i), value)))
      {
        TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
        break;
      }
      value.set_int(v.get_replica(i).checksum_);
      if (OB_SUCCESS != (ret = mutator.update(OB_FIRST_TABLET_ENTRY_TID, rowkey, SCHEMA.get_cid(SCHEMA.CF_CHECKSUM, i), value)))
      {
        TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
        break;
      }
      value.set_int(v.get_replica(i).cs_.get_port());
      if (OB_SUCCESS != (ret = mutator.update(OB_FIRST_TABLET_ENTRY_TID, rowkey, SCHEMA.get_cid(SCHEMA.CF_PORT, i), value)))
      {
        TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
        break;
      }
      if (ObServer::IPV4 == v.get_replica(i).cs_.get_version())
      {
        value.set_int(v.get_replica(i).cs_.get_ipv4());
        if (OB_SUCCESS != (ret = mutator.update(OB_FIRST_TABLET_ENTRY_TID, rowkey, SCHEMA.get_cid(SCHEMA.CF_IPV4, i), value)))
        {
          TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
          break;
        }
      }
      else
      {                         // ipv6
        value.set_int(v.get_replica(i).cs_.get_ipv6_high());
        if (OB_SUCCESS != (ret = mutator.update(OB_FIRST_TABLET_ENTRY_TID, rowkey, SCHEMA.get_cid(SCHEMA.CF_IPV6_HIGH, i), value)))
        {
          TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
          break;
        }
        value.set_int(v.get_replica(i).cs_.get_ipv6_low());
        if (OB_SUCCESS != (ret = mutator.update(OB_FIRST_TABLET_ENTRY_TID, rowkey, SCHEMA.get_cid(SCHEMA.CF_IPV6_LOW, i), value)))
        {
          TBSYS_LOG(WARN, "failed to add into mutator, err=%d", ret);
          break;
        }
      }
    }
  } // end for
  return ret;
}

int ObFirstTabletEntryTable::update(const Value &v)
{
  int ret = OB_SUCCESS;
  if (v.get_end_key() != ObRowkey::MAX_ROWKEY
      || v.get_start_key() != ObRowkey::MIN_ROWKEY
      || 0 == v.get_table_name().length()
      || NULL == v.get_table_name().ptr())
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid endkey or startkey, endkey=%s startkey=%s tname_len=%d",
              to_cstring(v.get_end_key()), to_cstring(v.get_start_key()),
              v.get_table_name().length());
  }
  else
  {
    // normal update
    ret = row_to_mutator(v, mutator_);
  }
  return ret;
}

int ObFirstTabletEntryTable::commit() // for testing only
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = scan_helper_.mutate(mutator_)))
  {
    TBSYS_LOG(WARN, "failed to apply mutator, err=%d", ret);
  }
  else
  {
    mutator_.reset();
    TBSYS_LOG(WARN, "directly call first_tablet_entry_table's commit");
  }
  return ret;
}

