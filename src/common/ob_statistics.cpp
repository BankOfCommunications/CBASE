/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*
*
*   Version: 0.1 2010-12-06
*
*   Authors:
*          daoan(daoan@taobao.com)
*
*
================================================================*/
#include <tblog.h>

#include "ob_statistics.h"
#include "ob_atomic.h"
#include "serialization.h"
#include "ob_new_scanner.h"

namespace oceanbase
{
  namespace common
  {
    ObStat::ObStat() :
        mod_id_(OB_INVALID_ID),
        table_id_(OB_INVALID_ID)

    {
      memset((void*)(values_), 0, sizeof(int64_t) * MAX_STATICS_PER_TABLE);
    }

    uint64_t ObStat::get_mod_id() const
    {
      return mod_id_;
    }

    uint64_t ObStat::get_table_id() const
    {
      return table_id_;
    }

    int64_t ObStat::get_value(const int32_t index) const
    {
      int64_t value = 0;
      if (index < MAX_STATICS_PER_TABLE && index >= 0)
      {
        value = values_[index];
      }
      return value;
    }

    int ObStat::set_value(const int32_t index, int64_t value)
    {
      int ret = OB_SIZE_OVERFLOW;
      if (index < MAX_STATICS_PER_TABLE && index >= 0)
      {
        atomic_exchange(reinterpret_cast<volatile uint64_t*>(&(values_[index])), value);
        ret = OB_SUCCESS;
      }
      return ret;
    }

    int ObStat::inc(const int32_t index, const int64_t inc_value)
    {
      int ret = OB_SIZE_OVERFLOW;
      if (index < MAX_STATICS_PER_TABLE && index >= 0)
      {
        if (inc_value == 1)
        {
          atomic_inc(reinterpret_cast<volatile uint64_t*>(&(values_[index])));
        }
        else
        {
          atomic_add(reinterpret_cast<volatile uint64_t*>(&(values_[index])), inc_value);
        }
        ret = OB_SUCCESS;
      }
      return ret;
    }

    void ObStat::set_mod_id(const uint64_t mod_id)
    {
      if (mod_id_ == OB_INVALID_ID)
      {
        mod_id_ = mod_id;
      }
      return;
    }

    void ObStat::set_table_id(const uint64_t table_id)
    {
      if (table_id_ == OB_INVALID_ID)
      {
        table_id_ = table_id;
      }
      return;
    }

    DEFINE_SERIALIZE(ObStat)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, mod_id_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, table_id_);
      }
      if (OB_SUCCESS == ret)
      {
        for (int32_t i = 0; i < MAX_STATICS_PER_TABLE; i++)
        {
          ret = serialization::encode_vi64(buf, buf_len, tmp_pos, values_[i]);
          if (OB_SUCCESS != ret)
          {
            break;
          }

        }
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }
    DEFINE_DESERIALIZE(ObStat)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&mod_id_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&table_id_));
      }
      if (OB_SUCCESS == ret)
      {
        int64_t value = 0;
        for (int64_t i = 0; i < MAX_STATICS_PER_TABLE; i++)
        {
          ret = serialization::decode_vi64(buf, data_len, tmp_pos, &value);
          values_[i] = value;
          if (OB_SUCCESS != ret)
          {
            break;
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }
    DEFINE_GET_SERIALIZE_SIZE(ObStat)
    {
      int64_t len = serialization::encoded_length_vi64(mod_id_);
      len += serialization::encoded_length_vi64(table_id_);
      for (int64_t i = 0; i < MAX_STATICS_PER_TABLE; i++)
      {
        len += serialization::encoded_length_vi64(values_[i]);
      }
      return len;
    }

    ObStatManager::ObStatManager(ObRole server_type)
      : server_type_(server_type)
    {
      int64_t mod = 0;
      for (mod = 0; mod < OB_MAX_MOD_NUMBER; mod++)
      {
        stat_cnt_[mod] = 0;
        table_stats_[mod].init(OB_MAX_TABLE_NUMBER, data_holder_[mod]);
      }
    }
    ObStatManager::~ObStatManager()
    {
    }

    void ObStatManager::init(const ObServer &server)
    {
      server_ = server;
    }

    ObRole ObStatManager::get_server_type() const
    {
      return server_type_;
    }
    void ObStatManager::set_server_type(const ObRole server_type)
    {
      server_type_ = server_type;
    }
    int ObStatManager::add_new_stat(const ObStat& stat)
    {
      int ret = OB_SUCCESS;
      uint64_t table_id = stat.get_table_id();
      uint64_t mod_id = stat.get_mod_id();
      if (table_id != OB_INVALID_ID && mod_id != OB_INVALID_ID)
      {
        tbsys::CThreadGuard guard(&lock_);
        for (int32_t i = 0; i < table_stats_[mod_id].get_array_index(); i++)
        {
          if (data_holder_[mod_id][i].get_table_id() == table_id)
          {
            ret = OB_ENTRY_EXIST;
            break;
          }
        }
        if (ret != OB_ENTRY_EXIST)
        {
          if (!table_stats_[mod_id].push_back(stat))
          {
            TBSYS_LOG(WARN, "too much tables");
            ret = OB_ERROR;
          }
        }
      }
      return ret;
    }
    int ObStatManager::set_value(const uint64_t mod_id, const uint64_t table_id, const int32_t index, const int64_t value)
    {
      int ret = OB_ERROR;
      bool need_add_new = true;
      OB_ASSERT(mod_id < OB_MAX_MOD_NUMBER);
      for (int32_t i = 0; table_id != OB_INVALID_ID && mod_id != OB_INVALID_ID && i < table_stats_[mod_id].get_array_index(); i++)
      {
        if (data_holder_[mod_id][i].get_table_id() == table_id)
        {
          need_add_new = false;
          ret = data_holder_[mod_id][i].set_value(index, value);
        }
      }
      if (need_add_new && table_id != OB_INVALID_ID && mod_id != OB_INVALID_ID)
      {
        ObStat stat;
        stat.set_mod_id(mod_id);
        stat.set_table_id(table_id);
        ret = stat.set_value(index, value);
        if (OB_SUCCESS == ret)
        {
          if ((ret = add_new_stat(stat)) == OB_ENTRY_EXIST)
          {
            ret = set_value(mod_id, table_id, index, value);
          }
        }
      }
      return ret;
    }
    int ObStatManager::inc(const uint64_t mod_id, const uint64_t table_id, const int32_t index, const int64_t inc_value)
    {
      int ret = OB_ERROR;
      bool need_add_new = true;
      assert(mod_id < OB_MAX_MOD_NUMBER);
      for (int32_t i = 0; table_id != OB_INVALID_ID && mod_id != OB_INVALID_ID && i < table_stats_[mod_id].get_array_index(); i++)
      {
        if (data_holder_[mod_id][i].get_table_id() == table_id)
        {
          need_add_new = false;
          ret = data_holder_[mod_id][i].inc(index, inc_value);
        }
      }
      if (need_add_new && table_id != OB_INVALID_ID && mod_id != OB_INVALID_ID)
      {
        ObStat stat;
        stat.set_mod_id(mod_id);
        stat.set_table_id(table_id);
        ret = stat.set_value(index, inc_value);
        if (OB_SUCCESS == ret)
        {
          if ((ret = add_new_stat(stat)) == OB_ENTRY_EXIST)
          {
            ret = inc(mod_id, table_id, index, inc_value);
          }
        }
      }
      return ret;
    }

    ObStatManager::const_iterator ObStatManager::begin(uint64_t mod) const
    {
      return data_holder_[mod];
    }
    ObStatManager::const_iterator ObStatManager::end(uint64_t mod) const
    {
      return data_holder_[mod] + table_stats_[mod].get_array_index();
    }

    DEFINE_SERIALIZE(ObStatManager)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;
      uint64_t mod = 0;
      ret = serialization::encode_vi64(buf, buf_len, tmp_pos, server_type_);
      //we need get size first, so new table added to stat will not occus error
      for (mod = 0; ret == OB_SUCCESS && mod < OB_MAX_MOD_NUMBER; mod++)
      {
        int32_t size = static_cast<int32_t>(table_stats_[mod].get_array_index());

        if (OB_SUCCESS == ret)
        {
          ret = serialization::encode_vi32(buf, buf_len, tmp_pos, size);
        }
        if (OB_SUCCESS == ret)
        {
          for (int32_t i = 0; i < size; i++)
          {
            ret = data_holder_[mod][i].serialize(buf, buf_len, tmp_pos);
            if (OB_SUCCESS != ret)
            {
              break;
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
    DEFINE_DESERIALIZE(ObStatManager)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;
      int32_t size = 0;
      int64_t mod = 0;
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&server_type_));
      }
      for (mod = 0; ret == OB_SUCCESS && mod < OB_MAX_MOD_NUMBER; mod++)
      {
        ret = serialization::decode_vi32(buf, data_len, tmp_pos, &size);
        if (OB_SUCCESS == ret)
        {
          for (int64_t i = 0; i < size; i++)
          {
            ret = data_holder_[mod][i].deserialize(buf, data_len, tmp_pos);
            if (OB_SUCCESS != ret)
            {
              break;
            }
          }
        }
        if (OB_SUCCESS == ret)
        {
          table_stats_[mod].init(OB_MAX_TABLE_NUMBER, data_holder_[mod], size);
        }
        else
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
    DEFINE_GET_SERIALIZE_SIZE(ObStatManager)
    {
      int64_t len = serialization::encoded_length_vi64(server_type_);
      int64_t mod = 0;
      for (mod = 0; mod < OB_MAX_MOD_NUMBER; mod++)
      {
        len += serialization::encoded_length_vi32(static_cast<int32_t>(table_stats_[mod].get_array_index()));
        for (int64_t i = 0; i < table_stats_[mod].get_array_index(); i++)
        {
          len += data_holder_[mod][i].get_serialize_size();
        }
      }
      return len;
    }

    int ObStatManager::reset()
    {
      int64_t mod = 0;
      server_type_ = OB_INVALID;
      memset(stat_cnt_, 0, sizeof(stat_cnt_));
      memset(data_holder_, 0, sizeof(data_holder_));
      for (mod = 0; mod < OB_MAX_MOD_NUMBER; mod++)
      {
        table_stats_[mod].init(OB_MAX_TABLE_NUMBER, data_holder_[mod]);
      }
      return OB_SUCCESS;
    }

    ObStatManager & ObStatManager::operator=(const ObStatManager &rhs)
    {
      int64_t mod = 0;
      int64_t idx = 0;
      if (this != &rhs)
      {
        reset();
        server_type_ = rhs.get_server_type();
        server_ = rhs.server_;
        for (mod = 0; mod < OB_MAX_MOD_NUMBER; mod++)
        {
          stat_cnt_[mod] = rhs.stat_cnt_[mod];
          map_[mod] = rhs.map_[mod];
          for (idx = 0; idx < rhs.table_stats_[mod].get_array_index(); idx++)
          {
            table_stats_[mod].push_back(rhs.data_holder_[mod][idx]);
          }
        }
      }
      return *this;
    }

    int64_t ObStatManager::addop(const int64_t lv, const int64_t rv)
    {
      return lv + rv;
    }

    int64_t ObStatManager::subop(const int64_t lv, const int64_t rv)
    {
      return lv - rv;
    }

    ObStatManager & ObStatManager::add(const ObStatManager &augend)
    {
      return operate(augend, addop);
    }

    ObStatManager & ObStatManager::subtract(const ObStatManager &minuend)
    {
      return operate(minuend, subop);
    }

    ObStatManager & ObStatManager::operate(const ObStatManager &operand, OpFunc op)
    {
      if (server_type_ == operand.server_type_)
      {
        for (int32_t mod = 0; mod < OB_MAX_MOD_NUMBER; mod++)
        {
          for (int32_t i = 0; i < table_stats_[mod].get_array_index(); i++)
          {
            ObStat *stat = table_stats_[mod].at(i);
            ObStat *operand_stat = NULL;
            int has_table = operand.get_stat(stat->get_mod_id(), stat->get_table_id(), operand_stat);
            int64_t op_value = 0;
            // if operand has status value of same table, then apply op, otherwise keep old values.
            if (OB_SUCCESS == has_table && NULL != operand_stat)
            {
              for (int32_t index = 0; index < ObStat::MAX_STATICS_PER_TABLE; ++index)
              {
                op_value = op(stat->get_value(index) , operand_stat->get_value(index));
                stat->set_value(index, op_value);
              }
            }
          }
        }
      }
      return *this;
    }

    int ObStatManager::get_stat(const uint64_t mod_id, const uint64_t table_id, ObStat* &stat) const
    {
      int ret = OB_ENTRY_NOT_EXIST;
      stat = NULL;
      if (table_id != OB_INVALID_ID && mod_id != OB_INVALID_ID)
      {
        for (int32_t i = 0; i < table_stats_[mod_id].get_array_index(); i++)
        {
          if (table_stats_[mod_id].at(i)->get_table_id() == table_id)
          {
            stat = table_stats_[mod_id].at(i);
            ret = OB_SUCCESS;
            break;
          }
        }
      }
      return ret;
    }

    const char *ObStatManager::get_name(const uint64_t mod, int32_t index) const
    {
      index = (int32_t)max(0, min(stat_cnt_[mod] - 1, index));
      return map_[mod][index];
    }

    int ObStatManager::get_scanner(ObNewScanner &scanner) const
    {
      int ret = OB_SUCCESS;
      int64_t last_used_mod = OB_INVALID_ID;
      char ipbuf[OB_IP_STR_BUFF] = {0};
      server_.ip_to_string(ipbuf, sizeof (ipbuf));
      ObString ipstr = ObString::make_string(ipbuf);
      const int32_t port = server_.get_port();
      ObString server_name = ObString::make_string(print_role(get_server_type()));
      int64_t total_stat_cnt = 0;
      /* create row_desc */
      ObRowDesc row_desc;
      int32_t column_id = OB_APP_MIN_COLUMN_ID;
      row_desc.add_column_desc(OB_ALL_SERVER_STAT_TID, column_id++);
      row_desc.add_column_desc(OB_ALL_SERVER_STAT_TID, column_id++);
      row_desc.add_column_desc(OB_ALL_SERVER_STAT_TID, column_id++);
      row_desc.add_column_desc(OB_ALL_SERVER_STAT_TID, column_id++);
      row_desc.add_column_desc(OB_ALL_SERVER_STAT_TID, column_id++);
      row_desc.set_rowkey_cell_count(4);
      ObRow row;
      ObObj obj;

      // drop mod id info
      for (int32_t mod = 0; mod < OB_MAX_MOD_NUMBER; mod++)
      {
        ObStat stat;
        total_stat_cnt += stat_cnt_[mod];
        // drop table id info
        for (int32_t i = 0; i < table_stats_[mod].get_array_index(); i++)
        {
          for (int32_t j = 0; j < stat_cnt_[mod]; j++)
          {
            stat.inc(j, table_stats_[mod].at(i)->get_value(j));
          }
        }
        for (int32_t i = 0; i < stat_cnt_[mod]; i++)
        {
          column_id = OB_APP_MIN_COLUMN_ID;
          row.set_row_desc(row_desc);
          row.reset(false, ObRow::DEFAULT_NULL);
          /* server type */
          obj.set_type(ObVarcharType);
          obj.set_varchar(server_name);
          row.set_cell(OB_ALL_SERVER_STAT_TID, column_id++, obj);
          /* server ip */
          obj.set_type(ObVarcharType);
          obj.set_varchar(ipstr);
          row.set_cell(OB_ALL_SERVER_STAT_TID, column_id++, obj);
          /* port */
          obj.set_type(ObIntType);
          obj.set_int(port);
          row.set_cell(OB_ALL_SERVER_STAT_TID, column_id++, obj);
          /* key */
          obj.set_type(ObVarcharType);
          obj.set_varchar(ObString::make_string(get_name(mod, i)));
          row.set_cell(OB_ALL_SERVER_STAT_TID, column_id++, obj);
          /* value */
          obj.set_type(ObIntType);
          obj.set_int(stat.get_value(i));
          row.set_cell(OB_ALL_SERVER_STAT_TID, column_id++, obj);

          scanner.add_row(row);
          // for last rowkey
          last_used_mod = mod;
        }
      }
      /* last rowkey */
      if (total_stat_cnt > 0)
      {
        static ObObj rk_objs[4];
        rk_objs[0].set_type(ObVarcharType);
        rk_objs[0].set_varchar(server_name);
        rk_objs[1].set_type(ObVarcharType);
        rk_objs[1].set_varchar(ipstr);
        rk_objs[2].set_type(ObIntType);
        rk_objs[2].set_int(port);
        rk_objs[3].set_type(ObVarcharType);
        rk_objs[3].set_varchar(ObString::make_string(get_name(last_used_mod, stat_cnt_[last_used_mod] - 1)));
        ObRowkey rowkey(rk_objs, 4);
        scanner.set_last_row_key(rowkey);
      }

      /* fullfilled */
      scanner.set_is_req_fullfilled(true, total_stat_cnt);

      return ret;
    }

    void ObStatManager::set_id2name(const uint64_t mod, const char *map[], int32_t len)
    {
      map_[mod] = map;
      stat_cnt_[mod] = (int32_t)min(len, ObStat::MAX_STATICS_PER_TABLE);
    }

    void ObStatManager::print_info() const
    {
      /* TODO */
      return;
    }
  }
}
