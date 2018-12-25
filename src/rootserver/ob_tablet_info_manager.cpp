/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*
*
*   Version: 0.1 2010-10-20
*
*   Authors:
*          daoan(daoan@taobao.com)
*
*
================================================================*/
#include "rootserver/ob_tablet_info_manager.h"
#include "common/utility.h"
namespace oceanbase
{
  namespace rootserver
  {
    using namespace common;
    ObTabletCrcHistoryHelper::ObTabletCrcHistoryHelper()
    {
      reset();
    }
    void ObTabletCrcHistoryHelper::reset()
    {
      memset(version_, 0, sizeof(int64_t) * MAX_KEEP_HIS_COUNT);
      rest_all_crc_sum();
    }
    void ObTabletCrcHistoryHelper::rest_all_crc_sum()
    {
      memset(crc_sum_, 0, sizeof(uint64_t) * MAX_KEEP_HIS_COUNT);
      memset(row_checksum_, 0, sizeof(uint64_t) * MAX_KEEP_HIS_COUNT);
    }
    int ObTabletCrcHistoryHelper::check_and_update(const int64_t version, const uint64_t crc_sum, const uint64_t row_checksum)
    {
      int ret = OB_SUCCESS;
      int64_t min_version = 0;
      int64_t max_version = 0;
      get_min_max_version(min_version, max_version);
      //liumz, replace min_version with newest version
      if (version > max_version)
      {
        update_crc_sum(min_version, version, crc_sum, row_checksum);
      }
      else
      {
        bool found_same_version = false;
        for (int i = 0; i < MAX_KEEP_HIS_COUNT; i++)
        {
          if (version_[i] == version)
          {
            found_same_version = true;
            if (crc_sum_[i] == 0)
            {
              //liumz, step here
              crc_sum_[i] = crc_sum;
              row_checksum_[i] = row_checksum;
            }
            else
            {
              if (crc_sum != 0 && crc_sum != crc_sum_[i])
              {
                TBSYS_LOG(WARN, "crc check error version %ld crc sum %lu, %lu", version_[i], crc_sum_[i], crc_sum);
                ret = OB_ERROR;
              }

              //not return OB_ERROR, just in case
              if(row_checksum != 0 && row_checksum != row_checksum_[i])
              {
                TBSYS_LOG(WARN, "row_checksum check error version %ld row_checksum %lu, %lu", version_[i], row_checksum_[i], row_checksum);
              }
            }
          }
        }
        if (!found_same_version)
        {
          if (version > min_version) update_crc_sum(min_version, version, crc_sum, row_checksum);
        }
      }
      return ret;
    }
    void ObTabletCrcHistoryHelper::get_min_max_version(int64_t& min_version, int64_t& max_version) const
    {
      min_version = version_[0];
      max_version = 0;
      for (int i = 0; i < MAX_KEEP_HIS_COUNT; i++)
      {
        if (version_[i] < min_version) min_version = version_[i];
        if (version_[i] > max_version) max_version = version_[i];
      }
    }
    DEFINE_SERIALIZE(ObTabletCrcHistoryHelper)
    {
      int ret = 0;
      int64_t tmp_pos = pos;
      for (int i = 0; i < MAX_KEEP_HIS_COUNT; i++)
      {
        if (OB_SUCCESS == ret)
        {
          ret = serialization::encode_vi64(buf, buf_len, tmp_pos, version_[i]);
        }
        if (OB_SUCCESS == ret)
        {
          ret = serialization::encode_vi64(buf, buf_len, tmp_pos, crc_sum_[i]);
        }
        if (OB_SUCCESS == ret)
        {
          ret = serialization::encode_vi64(buf, buf_len, tmp_pos, row_checksum_[i]);
        }
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }
    DEFINE_DESERIALIZE(ObTabletCrcHistoryHelper)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;
      for (int i = 0; i < MAX_KEEP_HIS_COUNT; i++)
      {
        if (OB_SUCCESS == ret)
        {
          ret = serialization::decode_vi64(buf, data_len, tmp_pos, &version_[i]);
        }
        if (OB_SUCCESS == ret)
        {
          ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t *>(&crc_sum_[i]));
        }
        if (OB_SUCCESS == ret)
        {
          ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t *>(&row_checksum_[i]));
        }
      }
      if (OB_SUCCESS == ret)
      {

        pos = tmp_pos;
      }
      return ret;
    }
    DEFINE_GET_SERIALIZE_SIZE(ObTabletCrcHistoryHelper)
    {
      int64_t len = 0;
      for (int i = 0; i < MAX_KEEP_HIS_COUNT; i++)
      {
        len += serialization::encoded_length_vi64(version_[i]);
        len += serialization::encoded_length_vi64(crc_sum_[i]);
        len += serialization::encoded_length_vi64(row_checksum_[i]);
      }
      return len;
    }
    void ObTabletCrcHistoryHelper::update_crc_sum(const int64_t version, const int64_t new_version, const uint64_t crc_sum, const uint64_t row_checksum)
    {
      for (int i = 0; i < MAX_KEEP_HIS_COUNT; i++)
      {
        if (version_[i] == version)
        {
          version_[i] = new_version;
          crc_sum_[i] = crc_sum;
          row_checksum_[i] = row_checksum;
          break;
        }
      }
    }

    int ObTabletCrcHistoryHelper::get_row_checksum(const int64_t version, uint64_t &row_checksum) const
    {
      int ret = OB_ERROR;
      for (int i = 0 ; i < MAX_KEEP_HIS_COUNT; i++)
      {
        if (version_[i] == version)
        {
          row_checksum = row_checksum_[i];
          ret = OB_SUCCESS;
        }
      }

      return ret;
    }

    void ObTabletCrcHistoryHelper::reset_crc_sum(const int64_t version)
    {
      for (int i = 0 ; i < MAX_KEEP_HIS_COUNT; i++)
      {
        if (version_[i] == version)
        {
         row_checksum_[i] = 0;
         crc_sum_[i] = 0;
        }
      }
    }
////////////////////////////////////////////////////////////////
    ObTabletInfoManager::ObTabletInfoManager()
    {
      tablet_infos_.init(MAX_TABLET_COUNT, data_holder_);
    }

    int ObTabletInfoManager::add_tablet_info(const common::ObTabletInfo& tablet_info, int32_t& out_index)
    {
      out_index = OB_INVALID_INDEX;
      int32_t ret = OB_SUCCESS;
      if (tablet_infos_.get_array_index() >= MAX_TABLET_COUNT)
      {
        TBSYS_LOG(ERROR, "too many tablet max is %ld now is %ld", MAX_TABLET_COUNT, tablet_infos_.get_array_index());
        ret = OB_ERROR;
      }
      if (OB_SUCCESS == ret)
      {
        ObTabletInfo tmp;
        if (OB_SUCCESS != (ret = tmp.deep_copy(allocator_, tablet_info)))
        {
          TBSYS_LOG(ERROR, "copy tablet error");
        }
        else if(!tablet_infos_.push_back(tmp))
        {
          TBSYS_LOG(ERROR, "too many tablet max is %ld now is %ld", MAX_TABLET_COUNT, tablet_infos_.get_array_index());
          ret = OB_ERROR;
        }
        else
        {
          out_index = static_cast<int32_t>(tablet_infos_.get_array_index() - 1);
        }
      }
      return ret;
    }
    int32_t ObTabletInfoManager::get_index(const_iterator it) const
    {
      return static_cast<int32_t>(it - begin());
    }
    //add liumz, [secondary index static_index_build] 20150319:b
    int32_t ObTabletInfoManager::get_index(const common::ObTabletInfo &tablet_info)
    {
      int32_t tablet_info_index = OB_INVALID_INDEX;
      for (const_iterator it = begin(); it != end(); it++)
      {
        if (it->equal(tablet_info))
        {
          tablet_info_index = get_index(it);
          break;
        }
      }
      return tablet_info_index;
    }
    //add:e
    ObTabletInfoManager::const_iterator ObTabletInfoManager::get_tablet_info(const int32_t index) const
    {
      return tablet_infos_.at(index);
    }
    ObTabletInfoManager::iterator ObTabletInfoManager::get_tablet_info(const int32_t index)
    {
      return tablet_infos_.at(index);
    }
    ObTabletInfoManager::const_iterator ObTabletInfoManager::begin() const
    {
      return data_holder_;
    }
    ObTabletInfoManager::iterator ObTabletInfoManager::begin()
    {
      return data_holder_;
    }
    ObTabletInfoManager::const_iterator ObTabletInfoManager::end() const
    {
      return begin() + tablet_infos_.get_array_index();
    }
    ObTabletInfoManager::iterator ObTabletInfoManager::end()
    {
      return begin() + tablet_infos_.get_array_index();
    }
    int32_t ObTabletInfoManager::get_array_size() const
    {
      return static_cast<int32_t>(tablet_infos_.get_array_index());
    }
    void ObTabletInfoManager::clear()
    {
      for (int64_t i = 0; i < tablet_infos_.get_array_index(); i++)
      {
        allocator_.free(const_cast<char*>(reinterpret_cast<const char*>(tablet_infos_.at(i)->range_.start_key_.ptr())));
        allocator_.free(const_cast<char*>(reinterpret_cast<const char*>(tablet_infos_.at(i)->range_.end_key_.ptr())));
        tablet_infos_.at(i)->range_.start_key_.assign(NULL, 0);
        tablet_infos_.at(i)->range_.end_key_.assign(NULL, 0);
      }
      tablet_infos_.clear();
      for (int64_t i = 0; i < MAX_TABLET_COUNT; i++)
      {
        crc_helper_[i].reset();
      }
    }

    ObTabletCrcHistoryHelper *ObTabletInfoManager::get_crc_helper(const int32_t index)
    {
      ObTabletCrcHistoryHelper *ret = NULL;
      if (index >= 0 && index < tablet_infos_.get_array_index())
      {
        ret = &crc_helper_[index];
      }
      return ret;
    }

    const ObTabletCrcHistoryHelper *ObTabletInfoManager::get_crc_helper(const int32_t index) const
    {
      const ObTabletCrcHistoryHelper *ret = NULL;
      if (index >= 0 && index < tablet_infos_.get_array_index())
      {
        ret = &crc_helper_[index];
      }
      return ret;
    }
    void ObTabletInfoManager::hex_dump(const int32_t index, const int32_t log_level) const
    {
      static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
      if (index >= 0 && index < tablet_infos_.get_array_index() && TBSYS_LOGGER._level >= log_level)
      {
        data_holder_[index].range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
        TBSYS_LOG(INFO, "%s", row_key_dump_buff);
        TBSYS_LOG(INFO, "row_count = %ld occupy_size = %ld",
            data_holder_[index].row_count_, data_holder_[index].occupy_size_);
        for (int i = 0; i < ObTabletCrcHistoryHelper::MAX_KEEP_HIS_COUNT; i++)
        {
          TBSYS_LOG(INFO, "crc_info %d version %ld, crc_sum %lu row_checksum %lu",
              i, crc_helper_[index].version_[i], crc_helper_[index].crc_sum_[i], crc_helper_[index].row_checksum_[i]);
        }
      }

    }

    void ObTabletInfoManager::dump_as_hex(FILE* stream) const
    {
      static char start_key_buff[OB_MAX_ROW_KEY_LENGTH * 2];
      static char end_key_buff[OB_MAX_ROW_KEY_LENGTH * 2];
      int size = static_cast<int32_t>(tablet_infos_.get_array_index());
      fprintf(stream, "size %d", size);
      for (int64_t i = 0; i < tablet_infos_.get_array_index(); i++)
      {
        fprintf(stream, "index %ld row_count %ld occupy_size %ld crcinfo %lu ",
            i, data_holder_[i].row_count_, data_holder_[i].occupy_size_, data_holder_[i].crc_sum_ );
        for (int crc_i = 0; crc_i < ObTabletCrcHistoryHelper::MAX_KEEP_HIS_COUNT; crc_i++)
        {
          fprintf(stream, "%ld %lu ", crc_helper_[i].version_[crc_i], crc_helper_[i].crc_sum_[crc_i]);
        }
        fprintf(stream, "table_id %ld border_flag %d\n",
            data_holder_[i].range_.table_id_, data_holder_[i].range_.border_flag_.get_data());

        start_key_buff[0] = 0;
        end_key_buff[0] = 0;

        if (data_holder_[i].range_.start_key_.ptr() != NULL)
        {
          data_holder_[i].range_.start_key_.to_string(start_key_buff, OB_MAX_ROW_KEY_LENGTH * 2);
        }

        if (data_holder_[i].range_.end_key_.ptr() != NULL)
        {
          data_holder_[i].range_.start_key_.to_string(end_key_buff, OB_MAX_ROW_KEY_LENGTH * 2);
        }

        fprintf(stream, "start_key %s\n end_key %s\n", start_key_buff, end_key_buff);
      }
      return;
    }
    void ObTabletInfoManager::read_from_hex(FILE* stream)
    {
      static char start_key_buff[OB_MAX_ROW_KEY_LENGTH * 2];
      static char end_key_buff[OB_MAX_ROW_KEY_LENGTH * 2];
      static char ob_start_key_buff[OB_MAX_ROW_KEY_LENGTH];
      static char ob_end_key_buff[OB_MAX_ROW_KEY_LENGTH];

      int size = 0;
      fscanf(stream, "size %d", &size);
      int64_t index = 0;
      int32_t out_index = 0;
      for (int64_t i = 0; i < size; i++)
      {
        ObTabletInfo tablet_info;
        int border_flag_data = 0;

        fscanf(stream, "index %ld row_count %ld occupy_size %ld crcinfo %lu ",
            &index, &tablet_info.row_count_, &tablet_info.occupy_size_, &tablet_info.crc_sum_);
        for (int crc_i = 0; crc_i < ObTabletCrcHistoryHelper::MAX_KEEP_HIS_COUNT; crc_i++)
        {
          fscanf(stream, "%ld %lu ", &crc_helper_[i].version_[crc_i], &crc_helper_[i].crc_sum_[crc_i]);
        }
        fscanf(stream, "table_id %ld border_flag %d\n", &tablet_info.range_.table_id_, &border_flag_data);

        tablet_info.range_.border_flag_.set_data(static_cast<int8_t>(border_flag_data));

        start_key_buff[0] = 0;
        end_key_buff[0] = 0;
        ob_start_key_buff[0] = 0;
        ob_end_key_buff[0] = 0;

        int len = 0;
        fscanf(stream, "start_key %d", &len);
        if (len > 0)
        {
          // @todo from text to rowkey
          //fscanf(stream, "%s", start_key_buff);
        }

        len = 0;
        fscanf(stream, "\nend_key %d", &len);

        if (len > 0)
        {
          // @todo from text to rowkey
          //fscanf(stream, "%s", end_key_buff);
        }
        fscanf(stream, "\n");

        str_to_hex(start_key_buff, static_cast<int32_t>(strlen(start_key_buff)), ob_start_key_buff, OB_MAX_ROW_KEY_LENGTH);
        str_to_hex(end_key_buff, static_cast<int32_t>(strlen(end_key_buff)), ob_end_key_buff, OB_MAX_ROW_KEY_LENGTH);

        // tablet_info.range_.start_key_.assign_ptr(ob_start_key_buff, strlen(start_key_buff)/2 );
        // tablet_info.range_.end_key_.assign_ptr(ob_end_key_buff, strlen(end_key_buff)/2 );

        add_tablet_info(tablet_info, out_index);
        if (out_index != index)
        {
          TBSYS_LOG(ERROR, "index %ld != out_index %d", index, out_index);
          break;
        }
      }
      return;
    }
    DEFINE_SERIALIZE(ObTabletInfoManager)
    {
      int ret = 0;
      int64_t tmp_pos = pos;
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, tablet_infos_.get_array_index());
      }
      for (int64_t i = 0; i < tablet_infos_.get_array_index(); i++)
      {
        ret = data_holder_[i].serialize(buf, buf_len, tmp_pos);
        if (OB_SUCCESS != ret)
        {
          break;
        }
        ret = crc_helper_[i].serialize(buf, buf_len, tmp_pos);
        if (OB_SUCCESS != ret)
        {
          break;
        }
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }
    DEFINE_DESERIALIZE(ObTabletInfoManager)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;
      int64_t total_size = 0;
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &total_size);
      }
      if (OB_SUCCESS == ret)
      {
        ObObj rowkey_objs1[OB_MAX_ROWKEY_COLUMN_NUMBER];
        ObObj rowkey_objs2[OB_MAX_ROWKEY_COLUMN_NUMBER];
        ObTabletInfo tmp_tablet_info;
        for (int64_t i = 0; OB_SUCCESS == ret && i < total_size; i++)
        {
          int32_t out_index = -1;
          tmp_tablet_info.range_.start_key_.assign(rowkey_objs1, OB_MAX_ROWKEY_COLUMN_NUMBER);
          tmp_tablet_info.range_.end_key_.assign(rowkey_objs2, OB_MAX_ROWKEY_COLUMN_NUMBER);
          ret = tmp_tablet_info.deserialize(buf, data_len, tmp_pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to deserialize tablet info, ret=%d, buf=%p, data_len=%ld, tmp_pos=%ld",
                ret, buf, data_len, tmp_pos);
          }
          else
          {
            add_tablet_info(tmp_tablet_info, out_index);
            ret = crc_helper_[out_index].deserialize(buf, data_len, tmp_pos);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to deserialize crc info, out_index=%d, ret=%d, buf=%p, data_len=%ld, "
                  "tmp_pos=%ld", out_index, ret, buf, data_len, tmp_pos);
            }
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }
    DEFINE_GET_SERIALIZE_SIZE(ObTabletInfoManager)
    {
      int64_t len = serialization::encoded_length_vi64(tablet_infos_.get_array_index());
      for (int64_t i = 0; i < tablet_infos_.get_array_index(); i++)
      {
        len += data_holder_[i].get_serialize_size();
        len += crc_helper_[i].get_serialize_size();

      }
      return len;
    }
  }
}
