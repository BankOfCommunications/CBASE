/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_meta_cache.cpp is for what ...
 *
 * Version: ***: ob_meta_cache.cpp  Thu Aug 18 14:44:57 2011 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji.hcm <fangji.hcm@taobao.com>
 *     -some work detail if you want
 *
 */

#include "ob_schema.h"
#include "ob_meta_cache.h"
#include "ob_action_flag.h"

namespace oceanbase
{
  namespace common
  {
    const int64_t BUF_SIZE = 1024 * 16;     //buffer size
    ObPermInfoKey::ObPermInfoKey() : table_id_(OB_INVALID_ID),user_name_(NULL),
                                     length_(0),buffer_(NULL)
    {
    }

    ObPermInfoKey::ObPermInfoKey(const uint64_t table_id, ObString& user_name) : table_id_(table_id),
                                                                                 user_name_(user_name.ptr()),
                                                                                 length_(user_name.length()),
                                                                                 buffer_(NULL)
    {
    }

    ObPermInfoKey::ObPermInfoKey(const ObPermInfoKey& other) : table_id_(other.table_id_),
                                                               user_name_(other.user_name_),
                                                               length_(other.length_),
                                                               buffer_(NULL)
    {
    }

    ObPermInfoKey::~ObPermInfoKey()
    {
      if (NULL != buffer_)
      {
        ob_free(buffer_);
        buffer_ = NULL;
      }
    }

    int ObPermInfoKey::build_get_param(ObGetParam & get_param)
    {
      int ret = OB_SUCCESS;
      ObCellInfo cell_info;
      ObRowkey rowkey;
      ObObj rowkey_obj;
      ObVersionRange ver_range;
      int64_t pos = 0;
      if (NULL == buffer_)
      {
        buffer_ = reinterpret_cast<char*>(ob_malloc(BUF_SIZE, ObModIds::OB_PERM_INFO));
      }

      if (NULL != buffer_)
      {
        int64_t encode_pos = pos;
        ret = serialization::encode_i64(buffer_, BUF_SIZE, encode_pos, table_id_);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "encode failed table_id=%ld", table_id_);
        }
        else
        {
          memcpy(buffer_ + encode_pos, user_name_, length_);
          encode_pos += length_;
          rowkey_obj.set_varchar(
              ObString(0, static_cast<int32_t>(encode_pos - pos), buffer_ + pos));
          rowkey.assign(&rowkey_obj, 1);
          get_param.set_is_result_cached(true);
          cell_info.table_id_  = PERM_TABLE_ID;
          cell_info.row_key_   = rowkey;
          cell_info.column_id_ = PERM_COL_ID;
          ret = get_param.add_cell(cell_info);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "Get param add cell failed table_id[%lu]", cell_info.table_id_);
          }
          else
          {
            ver_range.start_version_ = 0;
            ver_range.end_version_   = INT64_MAX - 1;
            ver_range.border_flag_.set_inclusive_start();
            ver_range.border_flag_.set_inclusive_end();
            get_param.set_version_range(ver_range);
          }
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "malloc key buffer failed");
        ret = OB_ERROR;
      }
      return ret;
    }

    int64_t ObPermInfoKey::hash() const
    {
      uint32_t hash_val = 0;
      hash_val = oceanbase::common::murmurhash2(&table_id_, sizeof(table_id_), hash_val);
      if (NULL != user_name_ && 0 < length_)
      {
        hash_val = oceanbase::common::murmurhash2(user_name_, static_cast<int32_t>(length_), hash_val);
      }
      return hash_val;
    }

    bool ObPermInfoKey::operator==(const ObPermInfoKey &other) const
    {
      bool ret = false;
      ret = ((table_id_ == other.table_id_)
             &&(length_ == other.length_));

      if (ret && 0 < length_)
      {
        if (NULL != user_name_ && NULL != other.user_name_)
        {
          ret = (memcmp(user_name_, other.user_name_, length_) == 0);
        }
        else
        {
          ret = false;
        }
      }
      return ret;
    }

    int ObPermInfoKey::set_table_id(uint64_t table_id)
    {
      int ret = OB_SUCCESS;
      if (0 >= table_id || OB_INVALID_ID == table_id)
      {
        TBSYS_LOG(WARN, "illegal argument table_id[%lu]", table_id);
        ret = OB_ERROR;
      }
      else
      {
        table_id_ = table_id;
      }

      return ret;
    }

    int ObPermInfoKey::set_user_name(ObString name)
    {
      int ret = OB_SUCCESS;
      if (NULL == name.ptr())
      {
        TBSYS_LOG(WARN, "illegal argument name[%p]", name.ptr());
        ret = OB_ERROR;
      }
      else
      {
        user_name_ = name.ptr();
        length_ = name.length();
      }
      return ret;
    }

    int ObPermInfoKey::set_user_name(char * name)
    {
      int ret = OB_SUCCESS;
      if (NULL == name)
      {
        TBSYS_LOG(WARN, "illegal argument name[%p]", name);
        ret = OB_ERROR;
      }
      else
      {
        user_name_ = name;
      }
      return ret;
    }

    int ObPermInfoKey:: set_length(int64_t length)
    {
      int ret = OB_SUCCESS;
      if (0 >= length)
      {
        TBSYS_LOG(WARN, "illegal argument length[%ld]", length);
        ret = OB_ERROR;
      }
      else
      {
        length_ = length;
      }
      return ret;
    }

    ObUserInfoKey::ObUserInfoKey() : user_name_(NULL),length_(0),
                                     buffer_(NULL)
    {
    }

    ObUserInfoKey::ObUserInfoKey(ObString& user_name) : user_name_(user_name.ptr()),
                                                        length_(user_name.length()),
                                                        buffer_(NULL)
    {
    }

    ObUserInfoKey::ObUserInfoKey(const ObUserInfoKey& other) : user_name_(other.user_name_),
                                                               length_(other.length_),
                                                               buffer_(NULL)
    {
    }

    ObUserInfoKey::~ObUserInfoKey()
    {
      if (NULL != buffer_)
      {
        ob_free(buffer_);
        buffer_ = NULL;
      }
    }

    int ObUserInfoKey:: build_get_param(ObGetParam& get_param)
    {
      int ret = OB_SUCCESS;
      ObCellInfo cell_info;
      ObRowkey rowkey;
      ObObj rowkey_obj;
      ObVersionRange ver_range;
      if (NULL == buffer_)
      {
        buffer_ = reinterpret_cast<char*>(ob_malloc(BUF_SIZE, ObModIds::OB_USER_INFO_KEY));
      }

      if (NULL != buffer_)
      {
        memcpy(buffer_, user_name_, length_);
        rowkey_obj.set_varchar(ObString(0, static_cast<int32_t>(length_), buffer_));
        rowkey.assign(&rowkey_obj, 1);

        get_param.set_is_result_cached(true);
        cell_info.table_id_  = USER_TABLE_ID;
        cell_info.row_key_   = rowkey;
        cell_info.column_id_ = USER_COL_ID;
        ret = get_param.add_cell(cell_info);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "Get param add cell failed table_id[%lu]", cell_info.table_id_);
        }
        else
        {
          ver_range.start_version_ = 0;
          ver_range.end_version_   = INT64_MAX - 1;
          ver_range.border_flag_.set_inclusive_start();
          ver_range.border_flag_.set_inclusive_end();
          get_param.set_version_range(ver_range);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "malloc key buffer failed");
        ret = OB_ERROR;
      }
      return ret;
    }

    int64_t ObUserInfoKey::hash() const
    {
      uint32_t hash_val = 0;
      if (NULL != user_name_ && 0 < length_)
      {
        hash_val = oceanbase::common::murmurhash2(user_name_, static_cast<int32_t>(length_), hash_val);
      }
      return hash_val;
    }

    bool ObUserInfoKey::operator==(const ObUserInfoKey& other) const
    {
      bool ret = false;
      ret = (length_ == other.length_);
      if (ret && 0 < length_)
      {
        if (NULL != user_name_ && NULL != other.user_name_)
        {
          ret = (0 == memcmp(user_name_, other.user_name_, length_));
        }
        else
        {
          ret = false;
        }
      }
      return ret;
    }

    int ObUserInfoKey::set_user_name(ObString name)
    {
      int ret = OB_SUCCESS;
      if (NULL == name.ptr())
      {
        TBSYS_LOG(WARN, "illegal argument name[%p]", name.ptr());
        ret = OB_ERROR;
      }
      else
      {
        user_name_ = name.ptr();
        length_ = name.length();
      }
      return ret;
    }

    int ObUserInfoKey::set_user_name(char* name)
    {
      int ret = OB_SUCCESS;
      if (NULL == name)
      {
        TBSYS_LOG(WARN, "illegal argument name[%p]", name);
        ret = OB_ERROR;
      }
      else
      {
        user_name_ = name;
      }
      return ret;
    }

    int ObUserInfoKey::set_length(int64_t length)
    {
      int ret = OB_SUCCESS;
      if (0 >= length)
      {
        TBSYS_LOG(WARN, "illegal argument length[%ld]", length);
        ret = OB_SUCCESS;
      }
      else
      {
        length_ = length;
      }
      return ret;
    }

    ObSKeyInfoKey::ObSKeyInfoKey() : skey_version_(-1), buffer_(NULL)
    {
    }

    ObSKeyInfoKey::ObSKeyInfoKey(const int64_t version) : skey_version_(version), buffer_(NULL)
    {
    }

    ObSKeyInfoKey::ObSKeyInfoKey(const ObSKeyInfoKey& other) : skey_version_(other.skey_version_),
                                                               buffer_(NULL)
    {
    }

    ObSKeyInfoKey::~ObSKeyInfoKey()
    {
      if (NULL != buffer_)
      {
        ob_free(buffer_);
        buffer_ = NULL;
      }
    }

    int ObSKeyInfoKey::build_get_param(ObGetParam& get_param)
    {
      int ret = OB_SUCCESS;
      ObCellInfo cell_info;
      ObRowkey rowkey;
      ObObj rowkey_obj;
      ObVersionRange ver_range;
      int64_t pos = 0;
      if (NULL == buffer_)
      {
        buffer_ = reinterpret_cast<char*>(ob_malloc(BUF_SIZE, ObModIds::OB_SKEY_INFO_KEY));
      }

      ret = serialization::encode_i64(buffer_, BUF_SIZE, pos, skey_version_);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "encode failed pkey_version=%ld", skey_version_);
      }
      else
      {
        rowkey_obj.set_varchar(ObString(0, static_cast<int32_t>(pos), buffer_));
        rowkey.assign(&rowkey_obj, 1);
        get_param.reset();
        get_param.set_is_result_cached(true);
        cell_info.table_id_  = SKEY_TABLE_ID;
        cell_info.row_key_   = rowkey;
        cell_info.column_id_ = SKEY_COL_ID;
        ret = get_param.add_cell(cell_info);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "Get param add cell failed table_id[%lu]", cell_info.table_id_);
        }
        else
        {
          ver_range.start_version_ = 0;
          ver_range.end_version_   = INT64_MAX - 1;
          ver_range.border_flag_.set_inclusive_start();
          ver_range.border_flag_.set_inclusive_end();
          get_param.set_version_range(ver_range);
        }
      }
      return ret;
    }

    int64_t ObSKeyInfoKey::hash() const
    {
      uint32_t hash_val = 0;
      hash_val = oceanbase::common::murmurhash2(&skey_version_, sizeof(skey_version_), hash_val);
      return hash_val;
    }

    bool ObSKeyInfoKey::operator==(const ObSKeyInfoKey& other) const
    {
      bool ret = false;
      ret = (skey_version_ == other.get_skey_version());
      return ret;
    }

    int ObSKeyInfoKey::set_skey_version(int64_t version)
    {
      int ret = OB_SUCCESS;
      if (0 > version)
      {
        TBSYS_LOG(WARN, "illegal argument version[%ld]", version);
        ret = OB_ERROR;
      }
      else
      {
        skey_version_ = version;
      }
      return ret;
    }

    ObPermInfoValue::ObPermInfoValue() : perm_mask_(0), timestamp_(0)
    {
    }

    ObPermInfoValue::ObPermInfoValue(const int64_t perm) : perm_mask_(perm),
                                                           timestamp_(0)
    {
    }

    bool ObPermInfoValue::expired(const int64_t curr_time, const int64_t timeout) const
    {
      return (timestamp_ + timeout) > curr_time? false: true;
    }

    int ObPermInfoValue::trans_from_scanner(const ObScanner& scanner)
    {
      int ret                 = OB_SUCCESS;
      ObCellInfo* cell_info   = NULL;
      bool is_row_changed     = false;
      ObObjType type          = ObMinType;
      ObScannerIterator iter;
      if (OB_SUCCESS == ret)
      {
        iter = scanner.begin();
        iter.get_cell(&cell_info, &is_row_changed);
        if(NULL == cell_info)
        {
          TBSYS_LOG(WARN, "get null cell_info");
          ret = OB_ERROR;
        }
        else
        {
          type = cell_info->value_.get_type();
          switch (type)
          {
          case ObIntType:
            cell_info->value_.get_int(perm_mask_);
            timestamp_ = tbsys::CTimeUtil::getTime();
            break;
          case ObExtendType:
            if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell_info->value_.get_ext())
            {
              ret = OB_ENTRY_NOT_EXIST;
            }
            else
            {
              TBSYS_LOG(WARN, "unknow extend value=%ld", cell_info->value_.get_ext());
              ret = OB_ERROR;
            }
            break;
          default:
            TBSYS_LOG(WARN, "unexpected obj type");
            ret = OB_ERROR;
            break;
          }
        }
      }
      return ret;
    }

    int ObPermInfoValue::set_perm_mask(int64_t mask)
    {
      int ret = OB_SUCCESS;
      if (0 > mask)
      {
        TBSYS_LOG(WARN, "illegal argument perm_mask[%ld]", mask);
        ret = OB_ERROR;
      }
      else
      {
        perm_mask_ = mask;
      }
      return ret;
    }

    int ObPermInfoValue::set_time_stamp(int64_t timestamp)
    {
      int ret = OB_SUCCESS;
      if (0 > timestamp)
      {
        TBSYS_LOG(WARN, "illegal argument timestamp[%ld]", timestamp);
        ret = OB_ERROR;
      }
      else
      {
        timestamp_ = timestamp;
      }
      return ret;
    }

    ObUserInfoValue::ObUserInfoValue() : passwd_(NULL), length_(0), timestamp_(0)
    {
    }

    ObUserInfoValue::ObUserInfoValue(ObString& password) : passwd_(password.ptr()),
                                                           length_(password.length())
    {
    }

    bool ObUserInfoValue::expired(const int64_t curr_time, const int64_t timeout) const
    {
      return (timestamp_ + timeout) > curr_time? false: true;
    }

    int ObUserInfoValue::trans_from_scanner(const ObScanner& scanner)
    {
      int ret                 = OB_SUCCESS;
      ObCellInfo* cell_info   = NULL;
      bool is_row_changed     = false;
      ObObjType type          = ObMinType;
      ObScannerIterator iter;
      ObString value;
      if (OB_SUCCESS == ret)
      {
        iter = scanner.begin();
        iter.get_cell(&cell_info, &is_row_changed);
        if(NULL == cell_info)
        {
          TBSYS_LOG(WARN, "get null cell_info");
          ret = OB_ERROR;
        }
        else
        {
          type = cell_info->value_.get_type();
          switch (type)
          {
          case ObVarcharType:
            cell_info->value_.get_varchar(value);
            passwd_    = value.ptr();
            length_    = value.length();
            timestamp_ = tbsys::CTimeUtil::getTime();
            break;
          case ObExtendType:
            if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell_info->value_.get_ext())
            {
              ret = OB_ENTRY_NOT_EXIST;
            }
            else
            {
              TBSYS_LOG(WARN, "unknow extend value=%ld", cell_info->value_.get_ext());
              ret = OB_ERROR;
            }
            break;
          default:
            TBSYS_LOG(WARN, "unexpected obj type");
            ret = OB_ERROR;
            break;
          }
        }
      }
      return ret;
    }

    int ObUserInfoValue::set_passwd(ObString pass)
    {
      int ret = OB_SUCCESS;
      if (NULL == pass.ptr())
      {
        TBSYS_LOG(WARN, "illegal argument passwd[%p]", pass.ptr());
        ret = OB_ERROR;
      }
      else
      {
        passwd_ = pass.ptr();
        length_ = pass.length();
      }
      return ret;
    }

    int ObUserInfoValue::set_passwd(char* pass)
    {
      int ret = OB_SUCCESS;
      if (NULL == pass)
      {
        TBSYS_LOG(WARN, "illegal argument passwd[%p]", pass);
        ret = OB_ERROR;
      }
      else
      {
        passwd_ = pass;
      }
      return ret;
    }

    int ObUserInfoValue::set_length(int64_t length)
    {
      int ret = OB_SUCCESS;
      if (0 > length)
      {
        TBSYS_LOG(WARN, "illegal argument length[%ld]", length);
        ret = OB_ERROR;
      }
      else
      {
        length_ = length;
      }
      return ret;
    }

    int ObUserInfoValue::set_time_stamp(int64_t timestamp)
    {
      int ret = OB_SUCCESS;
      if (0 > timestamp)
      {
        TBSYS_LOG(WARN, "illegal argument timestamp[%ld]", timestamp);
        ret = OB_ERROR;
      }
      else
      {
        timestamp_ = timestamp;
      }
      return ret;
    }

    ObSKeyInfoValue::ObSKeyInfoValue() : skey_(NULL),length_(0),timestamp_(0)
    {
    }

    bool ObSKeyInfoValue::expired(const int64_t curr_time, const int64_t timeout) const
    {
      return (timestamp_ + timeout) > curr_time? false: true;
    }

    int ObSKeyInfoValue::trans_from_scanner(const ObScanner& scanner)
    {
      int ret                 = OB_SUCCESS;
      ObCellInfo* cell_info   = NULL;
      bool is_row_changed     = false;
      ObObjType type          = ObMinType;
      ObScannerIterator iter;
      ObString value;
      if (OB_SUCCESS == ret)
      {
        iter = scanner.begin();
        iter.get_cell(&cell_info, &is_row_changed);
        if(NULL == cell_info)
        {
          TBSYS_LOG(WARN, "get null cell_info");
          ret = OB_ERROR;
        }
        else
        {
          type = cell_info->value_.get_type();
          switch (type)
          {
          case ObVarcharType:
            cell_info->value_.get_varchar(value);
            skey_      = value.ptr();
            length_    = value.length();
            timestamp_ = tbsys::CTimeUtil::getTime();
            break;
          case ObExtendType:
            if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell_info->value_.get_ext())
            {
              ret = OB_ENTRY_NOT_EXIST;
            }
            else
            {
              TBSYS_LOG(WARN, "unknow extend value=%ld", cell_info->value_.get_ext());
              ret = OB_ERROR;
            }
            break;
          default:
            TBSYS_LOG(WARN, "unexpected obj type");
            ret = OB_ERROR;
            break;
          }
        }
      }
      return ret;
    }

    int ObSKeyInfoValue::set_skey(ObString key)
    {
      int ret = OB_SUCCESS;
      if (NULL == key.ptr())
      {
        TBSYS_LOG(WARN, "illegal argument skey[%p]", key.ptr());
        ret = OB_ERROR;
      }
      else
      {
        skey_ = key.ptr();
        length_ = key.length();
      }
      return ret;
    }

    int ObSKeyInfoValue::set_skey(char* key)
    {
      int ret = OB_SUCCESS;
      if (NULL == key)
      {
        TBSYS_LOG(WARN, "illegal argument skey[%p]", key);
        ret = OB_ERROR;
      }
      else
      {
        skey_ = key;
      }
      return ret;
    }

    int ObSKeyInfoValue::set_length(int64_t length)
    {
      int ret = OB_SUCCESS;
      if (0 > length)
      {
        TBSYS_LOG(WARN, "illegal argument length[%ld]", length);
        ret = OB_ERROR;
      }
      else
      {
        length_ = length;
      }
      return ret;
    }

    int ObSKeyInfoValue::set_time_stamp(int64_t timestamp)
    {
      int ret = OB_SUCCESS;
      if (0 > timestamp)
      {
        TBSYS_LOG(WARN, "illegal argument timestamp[%ld]", timestamp);
        ret = OB_ERROR;
      }
      else
      {
        timestamp_ = timestamp;
      }
      return ret;
    }
  }
}
