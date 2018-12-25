/*
 *   (C) 2007-2010 Taobao Inc.
 *   
 *         
 *   Version: 0.1
 *           
 *   Authors:
 *      qushan <qushan@taobao.com>
 *        - some work details if you want
 *               
 */
#include <algorithm>
#include "tbsys.h"
#include "utility.h"
#include "ob_tablet_info.h"

namespace oceanbase 
{ 
  namespace common 
  {

    // --------------------------------------------------------
    // class ObTabletLocation implements
    // --------------------------------------------------------

    void ObTabletLocation::dump(const ObTabletLocation & location)
    {
      TBSYS_LOG(INFO,"tablet_version :%ld, tablet_seq: %ld, location:%s\n", 
          location.tablet_version_, location.tablet_seq_, to_cstring(location.chunkserver_));
    }

    DEFINE_SERIALIZE(ObTabletLocation)
    {
      int ret = OB_ERROR;
      ret = serialization::encode_vi64(buf, buf_len, pos, static_cast<int64_t>(tablet_version_));

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, pos, 
          static_cast<int64_t>(tablet_seq_));
      }

      if (ret == OB_SUCCESS)
        ret = chunkserver_.serialize(buf, buf_len, pos);

      return ret;
    }

    DEFINE_DESERIALIZE(ObTabletLocation)
    {
      int ret = OB_ERROR;
      ret = serialization::decode_vi64(buf, data_len, pos, (int64_t*)&tablet_version_);

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, pos, 
          (int64_t*)&tablet_seq_);
      }
      if (ret == OB_SUCCESS)
        ret = chunkserver_.deserialize(buf, data_len, pos);

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTabletLocation)
    {
      int64_t total_size = 0;
      total_size += serialization::encoded_length_vi64(tablet_version_);
      total_size += serialization::encoded_length_vi64(tablet_seq_);
      total_size += chunkserver_.get_serialize_size();
      return total_size;
    }
    
    DEFINE_SERIALIZE(ObTabletInfo)
    {
      int ret = OB_ERROR;
      ret = serialization::encode_vi64(buf, buf_len, pos, row_count_);

      if (ret == OB_SUCCESS)
        ret = serialization::encode_vi64(buf, buf_len, pos, occupy_size_);

      if (ret == OB_SUCCESS)
        ret = serialization::encode_vi64(buf, buf_len, pos, crc_sum_);

      if (ret == OB_SUCCESS)
        ret = serialization::encode_vi64(buf, buf_len, pos, row_checksum_);

      if (ret == OB_SUCCESS)
        ret = serialization::encode_i16(buf, buf_len, pos, version_);

      if (ret == OB_SUCCESS)
        ret = serialization::encode_i16(buf, buf_len, pos, reserved16_);

      if (ret == OB_SUCCESS)
        ret = serialization::encode_i32(buf, buf_len, pos, reserved32_);

      if (ret == OB_SUCCESS)
        ret = serialization::encode_i64(buf, buf_len, pos, reserved64_);

      if (ret == OB_SUCCESS)
        ret = range_.serialize(buf, buf_len, pos);

      return ret;
    }

    DEFINE_DESERIALIZE(ObTabletInfo)
    {
      int ret = OB_ERROR;
      ret = serialization::decode_vi64(buf, data_len, pos, &row_count_);

      if (OB_SUCCESS == ret)
        ret = serialization::decode_vi64(buf, data_len, pos, &occupy_size_);

      if (OB_SUCCESS == ret)
        ret = serialization::decode_vi64(buf, data_len, pos, reinterpret_cast<int64_t *>(&crc_sum_));

      if (OB_SUCCESS == ret)
        ret = serialization::decode_vi64(buf, data_len, pos, reinterpret_cast<int64_t *>(&row_checksum_));

      if (OB_SUCCESS == ret)
        ret = serialization::decode_i16(buf, data_len, pos, &version_);

      if (OB_SUCCESS == ret)
        ret = serialization::decode_i16(buf, data_len, pos, &reserved16_);

      if (OB_SUCCESS == ret)
        ret = serialization::decode_i32(buf, data_len, pos, &reserved32_);

      if (OB_SUCCESS == ret)
        ret = serialization::decode_i64(buf, data_len, pos, &reserved64_);

      if (OB_SUCCESS == ret)
      {
        ret = range_.deserialize(buf, data_len, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to deserialize range, ret=%d, buf=%p, data_len=%ld, pos=%ld",
              ret, buf, data_len, pos);
        }
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTabletInfo)
    {
      int64_t total_size = 0;

      total_size += serialization::encoded_length_vi64(row_count_);
      total_size += serialization::encoded_length_vi64(occupy_size_);
      total_size += serialization::encoded_length_vi64(crc_sum_);
      total_size += serialization::encoded_length_vi64(row_checksum_);
      total_size += serialization::encoded_length_i16(version_);
      total_size += serialization::encoded_length_i16(reserved16_);
      total_size += serialization::encoded_length_i32(reserved32_);
      total_size += serialization::encoded_length_i64(reserved64_);
      total_size += range_.get_serialize_size();

      return total_size;
    }

    /*
    int ObTabletInfo::deep_copy(CharArena &allocator, const ObTabletInfo &other, bool new_start_key, bool new_end_key)
    {
      int ret = OB_SUCCESS;
      
      this->row_count_ = other.row_count_;
      this->occupy_size_ = other.occupy_size_;
      this->crc_sum_ = other.crc_sum_;
      this->range_.table_id_ = other.range_.table_id_;
      this->range_.border_flag_ = other.range_.border_flag_;


      if (new_start_key) 
      {
        common::ObString::obstr_size_t sk_len = other.range_.start_key_.length();
        char* sk = reinterpret_cast<char*>(allocator.alloc(sk_len));
        if (NULL == sk)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(ERROR, "no memory");
        }
        else
        {
          memcpy(sk, other.range_.start_key_.ptr(), sk_len);
          this->range_.start_key_.assign(sk, sk_len);
        }
      }
      else
      {
        this->range_.start_key_ = other.range_.start_key_;
      }

      if (new_end_key)
      {
        common::ObString::obstr_size_t ek_len = other.range_.end_key_.length();
        char* ek = reinterpret_cast<char*>(allocator.alloc(ek_len));
        if (NULL == ek)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(ERROR, "no memory");
        }
        else
        {
          memcpy(ek, other.range_.end_key_.ptr(), ek_len);
          this->range_.end_key_.assign(ek, ek_len);
        }
      }
      else
      {
        this->range_.end_key_ = other.range_.end_key_;
      }
      return ret;
    }
    */
    
    // ObTabletReportInfo
    DEFINE_SERIALIZE(ObTabletReportInfo)
    {
      int ret = OB_SUCCESS;
      ret = tablet_info_.serialize(buf, buf_len, pos);
      if (ret == OB_SUCCESS)
        ret = tablet_location_.serialize(buf, buf_len, pos);
      return ret;
    }

    DEFINE_DESERIALIZE(ObTabletReportInfo)
    {
      int ret = OB_SUCCESS;
      ret = tablet_info_.deserialize(buf, data_len, pos);
      if (ret == OB_SUCCESS)
        ret = tablet_location_.deserialize(buf, data_len, pos);
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTabletReportInfo)
    {
      int64_t total_size = 0;

      total_size += tablet_info_.get_serialize_size();
      total_size += tablet_location_.get_serialize_size();

      return total_size;
    }

    bool ObTabletReportInfo::operator== (const ObTabletReportInfo &other) const
    {
      return tablet_info_.equal(other.tablet_info_)
        && tablet_location_.tablet_version_ == other.tablet_location_.tablet_version_
        && tablet_location_.chunkserver_ == other.tablet_location_.chunkserver_;
    }
    
    // ObTabletReportInfoList
    DEFINE_SERIALIZE(ObTabletReportInfoList)
    {
      int ret = OB_ERROR;
      
      int64_t size = tablet_list_.get_array_index();
      ret = serialization::encode_vi64(buf, buf_len, pos, size);

      if (ret == OB_SUCCESS)
      {
        for (int64_t i = 0; i < size; ++i)
        {
          ret = tablets_[i].serialize(buf, buf_len, pos);
          if (ret != OB_SUCCESS)
            break;
        }
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObTabletReportInfoList)
    {
      int ret = OB_ERROR;
      ObObj* ptr = NULL;

      int64_t size = 0;
      ret = serialization::decode_vi64(buf, data_len, pos, &size);

      if (ret == OB_SUCCESS && size > 0)
      {
        for (int64_t i=0; i<size; ++i)
        {
          ObTabletReportInfo tablet;
          ptr = reinterpret_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * OB_MAX_ROWKEY_COLUMN_NUMBER * 2));
          if (NULL == ptr) 
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
          else
          {
            tablet.tablet_info_.range_.start_key_.assign(ptr, OB_MAX_ROWKEY_COLUMN_NUMBER);
            tablet.tablet_info_.range_.end_key_.assign(ptr + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
            ret = tablet.deserialize(buf, data_len, pos);
          }
          if (ret != OB_SUCCESS)
            break;

          tablet_list_.push_back(tablet);
        }
      }

      return ret;
    }

    bool ObTabletReportInfoList::operator== (const ObTabletReportInfoList &other) const
    {
      bool ret = true;
      if (tablet_list_.get_array_index() != other.tablet_list_.get_array_index())
      {
        ret = false;
      }
      else
      {
        for (int i = 0; i < tablet_list_.get_array_index(); ++i)
        {
          if (!(tablets_[i] == other.tablets_[i]))
          {
            ret = false;
            break;
          }
        }
      }
      return ret;
    }
    
    DEFINE_GET_SERIALIZE_SIZE(ObTabletReportInfoList)
    {
      int64_t total_size = 0;
      
      int64_t size = tablet_list_.get_array_index();
      total_size += serialization::encoded_length_vi64(size);

      if (size > 0)
      {
        for (int64_t i=0; i<size; ++i)
          total_size += tablets_[i].get_serialize_size();
      }

      return total_size;
    }

    // ObIndexTabletReportInfoList
    DEFINE_SERIALIZE(ObIndexTabletReportInfoList)
    {
      int ret = OB_ERROR;

      int64_t size = tablet_list_.get_array_index();
      ret = serialization::encode_vi64(buf, buf_len, pos, size);

      if (ret == OB_SUCCESS)
      {
        for (int64_t i = 0; i < size; ++i)
        {
          ret = tablets_[i].serialize(buf, buf_len, pos);
          if (ret != OB_SUCCESS)
            break;
        }
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObIndexTabletReportInfoList)
    {
      int ret = OB_ERROR;
      ObObj* ptr = NULL;

      int64_t size = 0;
      ret = serialization::decode_vi64(buf, data_len, pos, &size);

      if (ret == OB_SUCCESS && size > 0)
      {
        for (int64_t i=0; i<size; ++i)
        {
          ObTabletReportInfo tablet;
          ptr = reinterpret_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * OB_MAX_ROWKEY_COLUMN_NUMBER * 2));
          if (NULL == ptr)
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
          else
          {
            tablet.tablet_info_.range_.start_key_.assign(ptr, OB_MAX_ROWKEY_COLUMN_NUMBER);
            tablet.tablet_info_.range_.end_key_.assign(ptr + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
            ret = tablet.deserialize(buf, data_len, pos);
          }
          if (ret != OB_SUCCESS)
            break;

          tablet_list_.push_back(tablet);
        }
      }

      return ret;
    }

    bool ObIndexTabletReportInfoList::operator== (const ObIndexTabletReportInfoList &other) const
    {
      bool ret = true;
      if (tablet_list_.get_array_index() != other.tablet_list_.get_array_index())
      {
        ret = false;
      }
      else
      {
        for (int i = 0; i < tablet_list_.get_array_index(); ++i)
        {
          if (!(tablets_[i] == other.tablets_[i]))
          {
            ret = false;
            break;
          }
        }
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObIndexTabletReportInfoList)
    {
      int64_t total_size = 0;

      int64_t size = tablet_list_.get_array_index();
      total_size += serialization::encoded_length_vi64(size);

      if (size > 0)
      {
        for (int64_t i=0; i<size; ++i)
          total_size += tablets_[i].get_serialize_size();
      }

      return total_size;
    }

    // ObIndexTabletInfoList
    DEFINE_SERIALIZE(ObIndexTabletInfoList)
    {
      int ret = OB_ERROR;

      int64_t size = tablet_list.get_array_index();
      ret = serialization::encode_vi64(buf, buf_len, pos, size);

      if (ret == OB_SUCCESS)
      {
        for (int64_t i=0; i<size; ++i)
        {
          ret = tablets[i].serialize(buf, buf_len, pos);
          if (ret != OB_SUCCESS)
            break;
        }
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObIndexTabletInfoList)
    {
      int ret = OB_ERROR;

      int64_t size = 0;
      ret = serialization::decode_vi64(buf, data_len, pos, &size);

      if (ret == OB_SUCCESS && size > 0)
      {
        ObTabletInfo tablet;
        for (int64_t i=0; i<size; ++i)
        {
          ret = tablet.deserialize(buf, data_len, pos);
          if (ret != OB_SUCCESS)
            break;

          tablet_list.push_back(tablet);
        }
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObIndexTabletInfoList)
    {
      int64_t total_size = 0;

      int64_t size = tablet_list.get_array_index();
      total_size += serialization::encoded_length_vi64(size);

      if (size > 0)
      {
        for (int64_t i=0; i<size; ++i)
          total_size += tablets[i].get_serialize_size();
      }

      return total_size;
    }

    // ObTabletInfoList
    DEFINE_SERIALIZE(ObTabletInfoList)
    {
      int ret = OB_ERROR;
      
      int64_t size = tablet_list.get_array_index();
      ret = serialization::encode_vi64(buf, buf_len, pos, size);

      if (ret == OB_SUCCESS)
      {
        for (int64_t i=0; i<size; ++i)
        {
          ret = tablets[i].serialize(buf, buf_len, pos);
          if (ret != OB_SUCCESS)
            break;
        }
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObTabletInfoList)
    {
      int ret = OB_ERROR;

      int64_t size = 0;
      ret = serialization::decode_vi64(buf, data_len, pos, &size);

      if (ret == OB_SUCCESS && size > 0)
      {
        ObTabletInfo tablet;
        for (int64_t i=0; i<size; ++i)
        {
          ret = tablet.deserialize(buf, data_len, pos);
          if (ret != OB_SUCCESS)
            break;

          tablet_list.push_back(tablet);
        }
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTabletInfoList)
    {
      int64_t total_size = 0;
      
      int64_t size = tablet_list.get_array_index();
      total_size += serialization::encoded_length_vi64(size);

      if (size > 0)
      {
        for (int64_t i=0; i<size; ++i)
          total_size += tablets[i].get_serialize_size();
      }

      return total_size;
    }

    // ObTableImportInfoList
    DEFINE_SERIALIZE(ObTableImportInfoList)
    {
      int ret = OB_ERROR;
      
      int64_t size = table_list_.get_array_index();
      ret = serialization::encode_vi64(buf, buf_len, pos, size);

      if (ret == OB_SUCCESS)
      {
        for (int64_t i=0; i<size; ++i)
        {
          ret = serialization::encode_vi64(buf, buf_len, pos, 
            static_cast<int64_t>(tables_[i]));
          if (ret != OB_SUCCESS)
            break;
        }
      }

      if (OB_SUCCESS == ret
          && OB_SUCCESS == (ret = serialization::encode_vi64(buf, buf_len, pos, tablet_version_))
          && OB_SUCCESS == (ret = serialization::encode_bool(buf, buf_len, pos, import_all_))
          && OB_SUCCESS == (ret = serialization::encode_bool(buf, buf_len, pos, response_rootserver_)))
      {
        //do nothing
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObTableImportInfoList)
    {
      int ret = OB_ERROR;
      uint64_t table_id = OB_INVALID_ID; 
      int64_t size = 0;
      reset();
      ret = serialization::decode_vi64(buf, data_len, pos, &size);

      if (ret == OB_SUCCESS && size > 0)
      {
        for (int64_t i=0; i<size; ++i)
        {
          ret = serialization::decode_vi64(buf, data_len, pos, 
            reinterpret_cast<int64_t*>(&table_id));
          if (ret != OB_SUCCESS)
            break;

          table_list_.push_back(table_id);
        }
      }

      if (OB_SUCCESS == ret
          && OB_SUCCESS == (ret = serialization::decode_vi64(buf, data_len, pos, &tablet_version_))
          && OB_SUCCESS == (ret = serialization::decode_bool(buf, data_len, pos, &import_all_))
          && OB_SUCCESS == (ret = serialization::decode_bool(buf, data_len, pos, &response_rootserver_)))
      {
        sort();        
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTableImportInfoList)
    {
      int64_t total_size = 0;
      
      int64_t size = table_list_.get_array_index();
      total_size += serialization::encoded_length_vi64(size);

      if (size > 0)
      {
        for (int64_t i=0; i<size; ++i)
        {
          total_size += serialization::encoded_length_vi64(tables_[i]);
        }
      }
      total_size += serialization::encoded_length_vi64(tablet_version_);
      total_size += serialization::encoded_length_bool(import_all_);
      total_size += serialization::encoded_length_bool(response_rootserver_);

      return total_size;
    }

    int64_t ObTableImportInfoList::to_string(char* buffer, const int64_t length) const
    {
      int64_t pos = 0;
      int64_t size = table_list_.get_array_index();

      databuff_printf(buffer, length, pos, "table_count=%ld, table_ids=", size);
      if (size > 0)
      {
        for (int64_t i=0; i<size; ++i)
        {
          databuff_printf(buffer, length, pos, "%lu,", tables_[i]);
        }
      }

      databuff_printf(buffer, length, pos, "import_all=%d, ", import_all_);
      databuff_printf(buffer, length, pos, "response_rootserver=%d", response_rootserver_);

      return pos;
    }

  } // end namespace common
} // end namespace oceanbase

