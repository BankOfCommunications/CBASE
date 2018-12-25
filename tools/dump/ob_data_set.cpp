#include "ob_data_set.h"
#include "common/utility.h"

using namespace oceanbase::common;
using namespace oceanbase::api;

ObDataSet::ObDataSet(OceanbaseDb *db)
{
  assert(db != NULL);
  db_ = db;
  inited_ = false;
  read_data_ = false;
  fullfilled_ = false;
  has_tablet_info_ = false;
  inclusive_start_ = true;
  version_ = 0;
}

bool ObDataSet::has_next()
{
  return inited_ && has_more_;
}

int ObDataSet::get_record(DbRecord *&recp)
{
  return ds_itr_.get_record(&recp);
}

/* (start_key, end_key] */
int ObDataSet::set_data_source(const std::string table,
                    const std::vector<std::string> &cols, const ObRowkey &start_key, 
                    const ObRowkey &end_key, int64_t version)
{
  version_ = version;
  start_key_ = start_key;
  end_key_ = end_key;
  columns_ = cols;
  table_ = table;

  has_more_ = false;
  read_data_ = false;
  fullfilled_ = false;
  has_tablet_info_ = false;

  int ret = OB_SUCCESS;
  if (end_key.ptr() == NULL || end_key.length() == 0) {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "error end key is specified");
  } else {
    start_key_ = start_key;
    end_key_ = end_key;
    last_end_key_ = start_key;
  }

  if (db_ == NULL) {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "Database is not specified");
  }

  if (ret == OB_SUCCESS) {
    if (!ds_.inited()) {
      ret = ds_.init();

      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "can't init record set");
      }
    }
  }

  if (ret == OB_SUCCESS) {
    ret = read_more(start_key_, end_key_, version_);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(WARN, "can't read more from oceanbase");
    }
  }

  if (ret == OB_SUCCESS) {
    inited_ = true;
  }

  return ret;
}

/* (start_key, end_key] */
int ObDataSet::set_data_source(const TabletInfo &info, const std::string table,
                    const std::vector<std::string> &cols, const ObRowkey &start_key, 
                    const ObRowkey &end_key, int64_t version)
{
  int ret = set_data_source(table, cols, start_key, end_key, version);
  
  tablet_info_ = info;
  has_tablet_info_ = true;

  return ret;
}

int ObDataSet::next()
{
  int ret = OB_SUCCESS;

  if (inited_ == true) {
    ds_itr_++;

    if (ds_itr_ != ds_.end()) {
      has_more_ = true;
    } else if (fullfilled_ == false) {
      ret = read_more(last_end_key_, end_key_, version_);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(WARN, "can't read more from db");
      }
    } else {
      has_more_ = false;
      TBSYS_LOG(DEBUG, "DataSet iterate to end");
    }
  } else {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "ObDataSet is not inited");
  }

  return ret;
}

int ObDataSet::read_more(const ObRowkey &start_key, const ObRowkey &end_key, int64_t version)
{
  {
    char start_buff[128];
    char end_buf[128];

    int len = start_key.to_string(start_buff, 128);
    start_buff[len] = 0;
    len = end_key.to_string(end_buf, 128);
    end_buf[len] = 0;

    TBSYS_LOG(INFO, "reading range--(%s:%s]", start_buff, end_buf);
  }

  int ret = OB_SUCCESS;
  if (has_tablet_info_) {
    ret = db_->scan(tablet_info_, table_, columns_, start_key, end_key, ds_, version, inclusive_start_, true);
  } else {
    ret = db_->scan(table_, columns_, start_key, end_key, ds_, version, inclusive_start_, true);
  }

  if (ret == OB_SUCCESS) {
    if (ds_.empty()) {
      fullfilled_ = true;
    } else {
      ret = ds_.get_last_rowkey(last_end_key_);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(WARN, "can't get last_end_key");
      }
      if (last_end_key_ != end_key_) {
        fullfilled_ = false;
      } else {
        fullfilled_ = true;
      }
    }
  } else {
    TBSYS_LOG(WARN, "scan data from tablet failed");
  }

  if (ret == OB_SUCCESS) {
    ds_itr_ = ds_.begin();
    has_more_ = ds_itr_ != ds_.end();

    if (!has_more_ && !fullfilled_) {
      ret = read_more(last_end_key_, end_key_, version_);
    }
  }

  if (ret != OB_SUCCESS) {
    has_more_ = false;
  }

  return ret;
}


