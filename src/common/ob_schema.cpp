/*===============================================================
 *   (C) 2007-2010 Taobao Inc.
 *
 *
 *   Version: 0.1 2010-09-26
 *
 *   Authors:
 *          daoan(daoan@taobao.com)
 *          maoqi(maoqi@taobao.com)
 *          fangji(fangji.hcm@taobao.com)
 *
 *
 ================================================================*/
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <algorithm>

#include <config.h>
#include <tblog.h>

#include "common/utility.h"
#include "ob_schema.h"
#include "ob_obj_type.h"
#include "common/ob_schema_service.h"
#include "ob_schema_helper.h"
#include "ob_define.h"

namespace
{
  const char* STR_TABLE_ID = "table_id";
  const char* STR_TABLE_TYPE = "table_type";
  //const char* STR_ROWKEY_SPLIT = "rowkey_split";
  //const char* STR_ROWKEY_LENGTH = "rowkey_max_length";
  const char* STR_COLUMN_INFO = "column_info";
  const char* STR_JOIN_RELATION = "join";
  const char* STR_COMPRESS_FUNC_NAME = "compress_func_name";
  const char* STR_BLOCK_SIZE = "block_size";
  const char* STR_USE_BLOOMFILTER = "use_bloomfilter";
  const char* STR_HAS_BASELINE_DATA = "has_baseline_data";
  const char* STR_CONSISTENCY_LEVEL = "consistency_level";
  const char* STR_MAX_COLUMN_ID = "max_column_id";
  const char* STR_COLUMN_TYPE_INT = "int";
  //add lijianqiang [INT_32] 20151008:b
  const char* STR_COLUMN_TYPE_INT32 = "int32";
  //add 20151008:e
  const char* STR_COLUMN_TYPE_FLOAT = "float";
  const char* STR_COLUMN_TYPE_DOUBLE = "double";
  const char* STR_COLUMN_TYPE_VCHAR = "varchar";
  const char* STR_COLUMN_TYPE_DATETIME = "datetime";
  const char* STR_COLUMN_TYPE_PRECISE_DATETIME = "precise_datetime";
  //add peiouya [DATE_TIME] 20150906:b
  const char* STR_COLUMN_TYPE_DATE = "date";
  const char* STR_COLUMN_TYPE_TIME = "time";
  //add 20150906:e
  const char* STR_COLUMN_TYPE_SEQ = "seq";
  const char* STR_COLUMN_TYPE_C_TIME = "create_time";
  const char* STR_COLUMN_TYPE_M_TIME = "modify_time";
  //add wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_14:b
  /*
    *for rs_admin check_schema,add decimal type
    */
  const char* STR_COLUMN_TYPE_DECIMAL="decimal";
  //add e
  const char* STR_SECTION_APP_NAME = "app_name";
  const char* STR_KEY_APP_NAME = "name";
  const char* STR_MAX_TABLE_ID = "max_table_id";

  const char* STR_SCHEMA_VERSION = "schema_version";

  const char* STR_COLUMN_GROUP_INFO="column_group_info";
  const char* STR_ROWKEY="rowkey";
  const int EXPIRE_ITEM=2;
  const char* STR_EXPIRE_CONDITION="expire_condition";
  const char* STR_EXPIRE_FREQUENCY="expire_frequency";
  const char* STR_MAX_SSTABLE_SIZE="max_sstable_size";
  const char* STR_QUERY_CACHE_EXPIRE_TIME="query_cache_expire_time_ms";
  const char* STR_IS_EXPIRE_EFFECT_IMMEDIATELY="expire_effect_immediately";
  const char* STR_MAX_SCAN_ROWS_PER_TABLET="max_scan_rows_per_tablet";
  const char* STR_INTERNAL_UPS_SCAN_SIZE="internal_ups_scan_size";
  //add zhaoqiong[roottable tablet management]20150302:b
  const char* STR_REPLICA_NUM="replica_num";
  // add e
  const unsigned int COLUMN_ID_RESERVED = 2;

  const int POS_COLUM_MANTAINED = 0;
  const int POS_COLUM_ID = 1;
  const int POS_COLUM_NAME = 2;
  const int POS_COLUM_TYPE = 3;
  const int POS_COLUM_SIZE = 4;

  const uint64_t ROW_KEY_COLUMN_ID = 1;
  const uint64_t MAX_ID_USED = oceanbase::common::OB_ALL_MAX_COLUMN_ID;//we limit our id in 2 byte, so update server can take advantage of this

  const int POS_COLUMN_GROUP_ID = 0;

  const int32_t OB_SCHEMA_MAGIC_NUMBER = 0x4353; //SC

}


namespace oceanbase
{
  namespace common
  {
    using namespace std;

    DEFINE_SERIALIZE(ObRowkeyColumn)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;
      if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, tmp_pos, length_)))
      {
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, tmp_pos, column_id_)))
      {
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, tmp_pos, type_)))
      {
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, tmp_pos, order_)))
      {
      }
      else
      {
        pos = tmp_pos;
      }
      return ret;
    }

    DEFINE_DESERIALIZE(ObRowkeyColumn)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;
      if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, tmp_pos, &length_)))
      {
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, tmp_pos,
              reinterpret_cast<int64_t *>(&column_id_))))
      {
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, tmp_pos,
              reinterpret_cast<int32_t*>(&type_))))
      {
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, tmp_pos,
              reinterpret_cast<int32_t*>(&order_))))
      {
      }
      else
      {
        pos = tmp_pos;
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObRowkeyColumn)
    {
      int64_t len = 0;
      len += serialization::encoded_length_vi64(length_);
      len += serialization::encoded_length_vi64(column_id_);
      len += serialization::encoded_length_vi32(type_);
      len += serialization::encoded_length_vi32(order_);
      return len;
    }

    int64_t ObRowkeyColumn::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "cid=%ld,type=%d,length=%ld.",
          column_id_, type_, length_);
      return pos;
    }

    int64_t ObRowkeyInfo::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      for (int i = 0; i < size_; ++i)
      {
        pos += columns_[i].to_string(buf + pos, buf_len - pos);
        if (pos >= buf_len) break;
      }
      return pos;
    }

    ObRowkeyInfo::ObRowkeyInfo():size_(0)
    {
      memset(columns_, 0, sizeof(ObRowkeyColumn) * OB_MAX_ROWKEY_COLUMN_NUMBER);
    }

    ObRowkeyInfo::~ObRowkeyInfo()
    {

    }

    int ObRowkeyInfo::get_column(const int64_t index, ObRowkeyColumn& column) const
    {
      int ret = OB_SUCCESS;
      if (index < 0 || index > size_ -1)
      {
        TBSYS_LOG(WARN, "Invalid argument index=%ld, split_size=%ld", index, size_);
        ret = OB_INVALID_INDEX;
      }
      else
      {
        column = columns_[index];
      }
      return ret;
    }

    const ObRowkeyColumn *ObRowkeyInfo::get_column(const int64_t index) const
    {
      const ObRowkeyColumn *ret = NULL;
      if (0 > index
          || size_ <= index)
      {
        TBSYS_LOG(WARN, "Invalid argument index=%ld, split_size=%ld", index, size_);
      }
      else
      {
        ret = &columns_[index];
      }
      return ret;
    }

    int ObRowkeyInfo::get_column_id(const int64_t index, uint64_t & column_id) const
    {
      ObRowkeyColumn column;;
      int ret = get_column(index, column);
      if (OB_SUCCESS == ret)
      {
        column_id = column.column_id_;
      }
      return ret;
    }

    int64_t ObRowkeyInfo::get_binary_rowkey_length() const
    {
      int64_t length = 0;
      for (int i = 0; i < size_; ++i)
      {
        length += columns_[i].length_;
      }
      return length;
    }

    int ObRowkeyInfo::get_index(const uint64_t column_id, int64_t &index, ObRowkeyColumn& column) const
    {
      int ret = OB_ENTRY_NOT_EXIST;
      int64_t i = 0;
      for (; i < size_; ++i)
      {
        if (columns_[i].column_id_ == column_id)
        {
          index = i;
          column = columns_[i];
          ret = OB_SUCCESS;
          break;
        }
      }
      return ret;
    }

    bool ObRowkeyInfo::is_rowkey_column(const uint64_t column_id) const
    {
      int64_t index = -1;
      ObRowkeyColumn column;
      bool ret = false;
      if (OB_SUCCESS == get_index(column_id, index, column) && index >= 0)
      {
        ret = true;
      }
      return ret;
    }

    int ObRowkeyInfo::add_column(const ObRowkeyColumn& column)
    {
      int ret = OB_SUCCESS;
      if (size_ >= OB_MAX_ROWKEY_COLUMN_NUMBER)
      {
        TBSYS_LOG(WARN, "No more column can be add, split_size=%ld, OB_MAX_ROWKEY_COLUMN_NUMBER=%ld",
            size_, OB_MAX_ROWKEY_COLUMN_NUMBER);
      }
      else
      {
        columns_[size_] = column;
        ++size_;
      }
      return ret;
    }

    int ObRowkeyInfo::set_column(int64_t idx, const ObRowkeyColumn& column)
    {
      int ret = OB_SUCCESS;
      if (0 > idx || OB_MAX_ROWKEY_COLUMN_NUMBER <= idx)
      {
        ret = OB_SIZE_OVERFLOW;
        TBSYS_LOG(WARN, "invalid idx=%ld", idx);
      }
      else
      {
        columns_[idx] = column;
        if (size_ <= idx)
        {
          size_ = 1 + idx;
        }
      }
      return ret;
    }

    DEFINE_SERIALIZE(ObRowkeyInfo)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;
      ret = serialization::encode_vi64(buf, buf_len, tmp_pos, size_);
      for (int32_t index = 0; OB_SUCCESS == ret && index < size_; ++index)
      {
        ret = columns_[index].serialize(buf, buf_len, tmp_pos);
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }

    DEFINE_DESERIALIZE(ObRowkeyInfo)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;
      ret = serialization::decode_vi64(buf, data_len, tmp_pos, &size_);
      for (int32_t index = 0; OB_SUCCESS == ret && index < size_; ++index)
      {
        ret = columns_[index].deserialize(buf, data_len, tmp_pos);
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObRowkeyInfo)
    {
      int64_t len = 0;
      len += serialization::encoded_length_vi64(size_);
      for (int32_t index = 0; index < size_; ++index)
      {
        len += columns_[index].get_serialize_size();
      }
      return len;
    }

    /*-----------------------------------------------------------------------------
     * ObColumnSchemaV2
     *-----------------------------------------------------------------------------*/

    ObColumnSchemaV2::ObColumnSchemaV2() : maintained_(false), is_nullable_(true),
    table_id_(OB_INVALID_ID),column_group_id_(OB_INVALID_ID),
    column_id_(OB_INVALID_ID),size_(0),type_(ObNullType),
    default_value_(), column_group_next_(NULL)
    {}

    ObColumnSchemaV2& ObColumnSchemaV2::operator=(const ObColumnSchemaV2& src_schema)
    {
      memcpy(this, &src_schema, sizeof(src_schema));
      return *this;
    }

    uint64_t    ObColumnSchemaV2::get_id()   const
    {
      return column_id_;
    }

    const char* ObColumnSchemaV2::get_name() const
    {
      return name_;
    }

    ColumnType  ObColumnSchemaV2::get_type() const
    {
      return type_;
    }

    int64_t  ObColumnSchemaV2::get_size() const
    {
      return (ObVarcharType == type_) ? size_ : ob_obj_type_size(type_);
    }

    uint64_t ObColumnSchemaV2::get_table_id() const
    {
      return table_id_;
    }
//add wenghaixing DECIMAL OceanBase_BankCommV0.2 2014_6_5:b
    uint32_t ObColumnSchemaV2::get_precision() const {
    	return ob_decimal_helper_.dec_precision_;
    }

    uint32_t ObColumnSchemaV2::get_scale() const {
    	return ob_decimal_helper_.dec_scale_;
    }

    void ObColumnSchemaV2::set_precision(uint32_t pre) {
    	ob_decimal_helper_.dec_precision_ = static_cast<uint8_t>(pre)
    			& META_PREC_MASK;
    }

    void ObColumnSchemaV2::set_scale(uint32_t scale) {
    	ob_decimal_helper_.dec_scale_ = static_cast<uint8_t>(scale)
    			& META_SCALE_MASK;
    	;
    }

    //add:e
    bool  ObColumnSchemaV2::is_maintained() const
    {
      return maintained_;
    }

    uint64_t ObColumnSchemaV2::get_column_group_id() const
    {
      return column_group_id_;
    }

    bool ObColumnSchemaV2::is_join_column() const
    {
      return (join_info_.join_table_ != OB_INVALID_ID);
    }

    void ObColumnSchemaV2::set_table_id(const uint64_t id)
    {
      table_id_ = id;
    }

    void ObColumnSchemaV2::set_column_id(const uint64_t id)
    {
      column_id_ = id;
    }

    void ObColumnSchemaV2::set_column_name(const char *name)
    {
      if (NULL != name && '\0' != *name)
      {
        snprintf(name_,sizeof(name_),"%s",name);
      }
    }

    void ObColumnSchemaV2::set_column_name(const ObString& name)
    {
      if (name.ptr() != NULL && static_cast<uint32_t>(name.length()) < sizeof(name_))
      {
        snprintf(name_,name.length() + 1,"%s",name.ptr());
      }
    }
    void ObColumnSchemaV2::set_column_type(const ColumnType type)
    {
      type_ = type;
    }

    void ObColumnSchemaV2::set_column_size(int64_t size) //only used when type is varchar
    {
      size_ = size;
    }

    void ObColumnSchemaV2::set_column_group_id(const uint64_t id)
    {
      column_group_id_ = id;
    }

    void ObColumnSchemaV2::set_maintained(const bool maintained)
    {
      maintained_ = maintained;
    }

    void ObColumnSchemaV2::set_join_info(const uint64_t join_table, const uint64_t* left_column_id,
        const uint64_t left_column_count, const uint64_t correlated_column)
    {
      join_info_.join_table_ = join_table;
      join_info_.left_column_count_ = left_column_count;
      join_info_.correlated_column_ = correlated_column;
      memcpy(join_info_.left_column_offset_array_, left_column_id, left_column_count * sizeof(uint64_t));
    }

    const ObColumnSchemaV2::ObJoinInfo* ObColumnSchemaV2::get_join_info() const
    {
      const ObJoinInfo *info = NULL;
      if (join_info_.join_table_ != OB_INVALID_ID)
      {
        info = &join_info_;
      }
      return info;
    }

    bool ObColumnSchemaV2::operator==(const ObColumnSchemaV2& r) const
    {
      bool ret = false;
      if (table_id_ == r.table_id_ && column_group_id_ == r.column_group_id_ &&
          column_id_ == r.column_id_)
      {
        ret = true;
      }
      return ret;
    }

    void ObColumnSchemaV2::print_info() const
    {
      TBSYS_LOG(INFO,"COLUMN:(%lu,%lu,%lu:%s)",table_id_,column_group_id_,column_id_,name_);
      TBSYS_LOG(INFO,"JOIN  :(%lu,%lu,%lu)",join_info_.join_table_,join_info_.left_column_count_,
          join_info_.correlated_column_);
    }
    //add zhaoqiong [Schema Manager] 20150327:b
    void ObColumnSchemaV2::print_debug_info() const
    {
      TBSYS_LOG(TRACE,"COLUMN:(%lu,%lu,%lu:%s)",table_id_,column_group_id_,column_id_,name_);
      TBSYS_LOG(TRACE,"JOIN  :(%lu,%lu,%lu)",join_info_.join_table_,join_info_.left_column_count_,
          join_info_.correlated_column_);
    }
    //add:e

    void ObColumnSchemaV2::print(FILE* fd) const
    {
      fprintf(fd, " column: %8ld %8ld %24s %16s %8ld\n", column_id_, column_group_id_,
          name_, strtype(type_), get_size());
      if (OB_INVALID_ID != join_info_.join_table_)
      {
        fprintf(fd, "JOIN  : %8lu %8lu %8lu\n",join_info_.join_table_,
            join_info_.left_column_count_, join_info_.correlated_column_);
      }
    }

    ColumnType ObColumnSchemaV2::convert_str_to_column_type(const char* str)
    {
      ColumnType type = ObNullType;
      if (strcmp(str, STR_COLUMN_TYPE_INT) == 0)
      {
        type = ObIntType;
      }
      //add lijianqiang [INT_32] 20151008:b
      else if (strcmp(str, STR_COLUMN_TYPE_INT32) == 0)
      {
        type = ObInt32Type;
      }
      //add 20151008:e
      else if (strcmp(str, STR_COLUMN_TYPE_FLOAT) == 0)
      {
        type = ObFloatType;
      }
      else if (strcmp(str, STR_COLUMN_TYPE_DOUBLE) == 0)
      {
        type = ObDoubleType;
      }
      else if (strcmp(str, STR_COLUMN_TYPE_VCHAR) == 0)
      {
        type = ObVarcharType;
      }
      else if (strcmp(str, STR_COLUMN_TYPE_DATETIME) == 0)
      {
        type = ObDateTimeType;
      }
      //add peiouya [DATE_TIME] 20150906:b
      else if (strcmp(str, STR_COLUMN_TYPE_DATE) == 0)
      {
        type = ObDateType;
      }
      else if (strcmp(str, STR_COLUMN_TYPE_TIME) == 0)
      {
        type = ObTimeType;
      }
      //add 20150906:e
      else if (strcmp(str, STR_COLUMN_TYPE_PRECISE_DATETIME) == 0)
      {
        type = ObPreciseDateTimeType;
      }
      else if (strcmp(str, STR_COLUMN_TYPE_SEQ) == 0)
      {
        type = ObSeqType;
      }
      else if (strcmp(str, STR_COLUMN_TYPE_C_TIME) == 0)
      {
        type = ObCreateTimeType;
      }
      else if (strcmp(str, STR_COLUMN_TYPE_M_TIME) == 0)
      {
        type = ObModifyTimeType;
      }
      //add wenghaixing DECIMAL OceanBase_BankCommV0.2 2014_7_14:b
      else if (strcmp(str, STR_COLUMN_TYPE_DECIMAL) == 0)
      {
        type = ObDecimalType;
      }
      //add e
      else
      {
        TBSYS_LOG(ERROR, "column type %s not be supported", str);
      }
      return type;
    }

    ObColumnSchemaV2* find_column_info_tmp(ObColumnSchemaV2* columns_info,const int64_t array_size,const char *column_name)
    {
      ObColumnSchemaV2 *info = NULL;
      for(int32_t i = 0; i < array_size; ++i)
      {
        if (NULL != columns_info[i].get_name() )
        {
          if ( strlen(columns_info[i].get_name()) == strlen(column_name) &&
              0 == strncmp(columns_info[i].get_name(),column_name,strlen(column_name)) )
          {
            info = &columns_info[i];
            break;
          }
        }
      }
      return info;
    }

    DEFINE_SERIALIZE(ObColumnSchemaV2)
    {
      int ret = 0;
      int64_t tmp_pos = pos;
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, maintained_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, is_nullable_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos,static_cast<int64_t>(table_id_));
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos,static_cast<int64_t>(column_group_id_));
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos,static_cast<int64_t>(column_id_));
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, size_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi32(buf, buf_len, tmp_pos, type_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vstr(buf, buf_len, tmp_pos, name_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = default_value_.serialize(buf, buf_len, tmp_pos);
      }
        //wenghaixing DECIMAL OceanBase_BankCommV0.2 2014_6_5:b
      	if(OB_SUCCESS == ret&&ObDecimalType==type_)
      	{
      		ret = serialization::encode_i8(buf, buf_len, tmp_pos,ob_decimal_helper_.dec_precision_);
      	}
      	if(OB_SUCCESS == ret&&ObDecimalType==type_)
      	{
      		ret = serialization::encode_i8(buf, buf_len, tmp_pos,ob_decimal_helper_.dec_scale_);
      	}
      	//add:e
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos,static_cast<int64_t>(join_info_.join_table_));
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos,static_cast<int64_t>(join_info_.correlated_column_));
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos,static_cast<int64_t>(join_info_.left_column_count_));
      }

      for (uint64_t i = 0; OB_SUCCESS == ret && i < join_info_.left_column_count_; ++i)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos,static_cast<int64_t>(join_info_.left_column_offset_array_[i]));
      }

      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }

    int ObColumnSchemaV2::deserialize_v3(const char* buf, const int64_t data_len, int64_t& pos)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &maintained_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t *>(&table_id_));
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t *>(&column_group_id_));
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t *>(&column_id_));
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &size_);
      }
      if (OB_SUCCESS == ret)
      {
        int32_t type = 0;
        ret = serialization::decode_vi32(buf, data_len, tmp_pos, &type);
        if (OB_SUCCESS == ret)
        {
          type_ = static_cast<ColumnType>(type);
        }
      }
      if (OB_SUCCESS == ret)
      {
        int64_t len = 0;
        serialization::decode_vstr(buf, data_len, tmp_pos,
            name_, OB_MAX_COLUMN_NAME_LENGTH, &len);
        if (-1 == len)
        {
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t *>(&join_info_.join_table_));
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t *>(&join_info_.left_column_count_));
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi32(buf, data_len, tmp_pos,
            reinterpret_cast<int32_t*>(&join_info_.left_column_offset_array_[0]));
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi32(buf, data_len, tmp_pos,
            reinterpret_cast<int32_t*>(&join_info_.left_column_offset_array_[1]));
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t *>(&join_info_.correlated_column_));
      }

      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }

    int ObColumnSchemaV2::deserialize_v4(const char* buf, const int64_t data_len, int64_t& pos)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &maintained_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &is_nullable_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t *>(&table_id_));
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t *>(&column_group_id_));
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t *>(&column_id_));
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &size_);
      }
      if (OB_SUCCESS == ret)
      {
        int32_t type = 0;
        ret = serialization::decode_vi32(buf, data_len, tmp_pos, &type);
        if (OB_SUCCESS == ret)
        {
          type_ = static_cast<ColumnType>(type);
        }
      }
      if (OB_SUCCESS == ret)
      {
        int64_t len = 0;
        serialization::decode_vstr(buf, data_len, tmp_pos,
            name_, OB_MAX_COLUMN_NAME_LENGTH, &len);
        if (-1 == len)
        {
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = default_value_.deserialize(buf, data_len, tmp_pos);
      }
//add wenghaixing DECIMAL OceanBase_BankCommV0.1 2014_5_22:b
      if(ObDecimalType==type_){
        //add wenghaixing DECIMAL OceanBase_BankCommV0.2 2014_6_5:b
        int8_t p= 0;
        int8_t s = 0;
        //add:e
      	if (OB_SUCCESS == ret) {

      		ret = serialization::decode_i8(buf, data_len,tmp_pos,
      				&p);
      		ob_decimal_helper_.dec_precision_=static_cast<uint8_t>(p) & META_PREC_MASK;;
      	}

      	if (OB_SUCCESS == ret) {

      		ret = serialization::decode_i8(buf, data_len,tmp_pos,
      				&s);
      		ob_decimal_helper_.dec_scale_=static_cast<uint8_t>(s) & META_SCALE_MASK;
      	}
      }
      	//add :e
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t *>(&join_info_.join_table_));
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t *>(&join_info_.correlated_column_));
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t *>(&join_info_.left_column_count_));
      }

      for (uint64_t i = 0; OB_SUCCESS == ret && i < join_info_.left_column_count_; ++i)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos,
            reinterpret_cast<int64_t *>(&join_info_.left_column_offset_array_[i]));
      }

      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }

    DEFINE_DESERIALIZE(ObColumnSchemaV2)
    {
      return deserialize_v4(buf, data_len, pos);
    }

    DEFINE_GET_SERIALIZE_SIZE(ObColumnSchemaV2)
    {
      int64_t len = serialization::encoded_length_bool(maintained_);
      len += serialization::encoded_length_bool(is_nullable_);
      len += serialization::encoded_length_vi64(table_id_);
      len += serialization::encoded_length_vi64(column_group_id_);
      len += serialization::encoded_length_vi64(column_id_);
      len += serialization::encoded_length_vi64(size_);
      len += serialization::encoded_length_vi32(type_);
      len += serialization::encoded_length_vstr(name_);
      len += default_value_.get_serialize_size();
      //add wenghaixing DECIMAL OceanBase_BankCommV0.2 2014_6_5:b
      if(ObDecimalType==type_)
      len += 2;
      /*
      *1 byte for precision,and 1 byte for scale
      */
      //add:e
      len += serialization::encoded_length_vi64(join_info_.join_table_);
      len += serialization::encoded_length_vi64(join_info_.correlated_column_);
      len += serialization::encoded_length_vi64(join_info_.left_column_count_);
      for (uint64_t i = 0; i < join_info_.left_column_count_; ++i)
      {
        len += serialization::encoded_length_vi64(join_info_.left_column_offset_array_[i]);
      }
      return len;
    }


    /*-----------------------------------------------------------------------------
     *  ObTableSchema
     *-----------------------------------------------------------------------------*/

    ObTableSchema::ObTableSchema() : table_id_(OB_INVALID_ID),
    rowkey_split_(0),
    block_size_(0),
    table_type_(SSTABLE_IN_DISK),
    is_pure_update_table_(false),
    is_use_bloomfilter_(false),
    is_merge_dynamic_data_(true),
    consistency_level_(NO_CONSISTENCY),
    has_baseline_data_(false),
    expire_frequency_(1),
    max_sstable_size_(0),
    query_cache_expire_time_(0),
    is_expire_effect_immediately_(0),
    max_scan_rows_per_tablet_(0),
    internal_ups_scan_size_(0),
    merge_write_sstable_version_(2),
    replica_count_(2),
	//mod zhaoqiong [Schema Manager] 20150424:b
	//version_(OB_SCHEMA_VERSION_FOUR_SECOND),
    version_(OB_SCHEMA_VERSION_SIX_FIRST),
	//mod:e
    schema_version_(0),
    create_time_column_id_(OB_INVALID_ID),
    modify_time_column_id_(OB_INVALID_ID)
    {
      name_[0] = '\0';
      expire_condition_[0] = '\0';
      comment_str_[0] = '\0';
      memset(reserved_, 0, sizeof(reserved_));
    }

    ObTableSchema& ObTableSchema::operator=(const ObTableSchema& src_schema)
    {
      memcpy(this, &src_schema, sizeof(src_schema));
      return *this;
    }

    uint64_t    ObTableSchema::get_table_id()   const
    {
      return table_id_;
    }
    bool ObTableSchema::is_merge_dynamic_data() const
    {
      return is_merge_dynamic_data_;
    }

    ObTableSchema::TableType   ObTableSchema::get_table_type() const
    {
      return table_type_;
    }

    const char* ObTableSchema::get_table_name() const
    {
      return name_;
    }

    const char* ObTableSchema::get_compress_func_name() const
    {
      return compress_func_name_;
    }

    const char* ObTableSchema::get_comment_str() const
    {
      return comment_str_;
    }

    uint64_t ObTableSchema::get_max_column_id() const
    {
      return max_column_id_;
    }

    int64_t ObTableSchema::get_version() const
    {
      return version_;
    }

    int32_t ObTableSchema::get_split_pos() const
    {
      return static_cast<int32_t>(rowkey_split_);
    }

    int32_t ObTableSchema::get_rowkey_max_length() const
    {
      return static_cast<int32_t>(rowkey_max_length_);
    }

    bool ObTableSchema::is_pure_update_table() const
    {
      return is_pure_update_table_;
    }

    bool ObTableSchema::is_use_bloomfilter()   const
    {
      return is_use_bloomfilter_;
    }

    bool ObTableSchema::has_baseline_data() const
    {
      return has_baseline_data_;
    }

    ObConsistencyLevel ObTableSchema::get_consistency_level() const
    {
      return consistency_level_;
    }

    bool ObTableSchema::is_expire_effect_immediately() const
    {
      return (is_expire_effect_immediately_ > 0);
    }

    int32_t ObTableSchema::get_block_size()    const
    {
      return block_size_;
    }

    int64_t ObTableSchema::get_max_sstable_size()    const
    {
      return max_sstable_size_;
    }

    int64_t ObTableSchema::get_expire_frequency()    const
    {
      return expire_frequency_;
    }

    int64_t ObTableSchema::get_query_cache_expire_time() const
    {
      return query_cache_expire_time_;
    }

    int64_t ObTableSchema::get_max_scan_rows_per_tablet() const
    {
      return max_scan_rows_per_tablet_;
    }

    int64_t ObTableSchema::get_internal_ups_scan_size() const
    {
      return internal_ups_scan_size_;
    }

    int64_t ObTableSchema::get_merge_write_sstable_version() const
    {
      return merge_write_sstable_version_;
    }

    int64_t ObTableSchema::get_replica_count() const
    {
      return replica_count_;
    }

    int64_t ObTableSchema::get_schema_version() const
    {
      return schema_version_;
    }

    void ObTableSchema::print(FILE* fd) const
    {
      fprintf(fd, "table=%s id=%ld, version=%ld, schema_version=%ld\n", name_, table_id_, version_, schema_version_);
      fprintf(fd, "properties: max_column_id_=%lu, rowkey_split_=%ld,\n"
          "rowkey_max_length_=%ld, block_size_=%d, table_type_=%d,"
          "is_pure_update_table_=%d,is_use_bloomfilter_=%d,"
          "has_baseline_data_=%d, merge_write_sstable_version_=%ld\n" ,
          max_column_id_, rowkey_split_, rowkey_max_length_, block_size_, table_type_,
          is_pure_update_table_, is_use_bloomfilter_, has_baseline_data_,
          merge_write_sstable_version_);
      fprintf(fd, "rowkey=");
      for (int64_t i = 0; i < rowkey_info_.get_size(); ++i)
      {
        const ObRowkeyColumn* column = rowkey_info_.get_column(i);
        fprintf(fd, "%lu(%ld)%s", column->column_id_, column->length_,
            i < rowkey_info_.get_size()-1 ? "," : "\n");
      }
      fprintf(fd, "expire_condition_=%s\n", expire_condition_);
      fprintf(fd, "comment_str_=%s\n", comment_str_);
    }

    const char* ObTableSchema::get_expire_condition() const
    {
      return expire_condition_;
    }

    const ObRowkeyInfo& ObTableSchema::get_rowkey_info() const
    {
      return rowkey_info_;
    }

    ObRowkeyInfo& ObTableSchema::get_rowkey_info()
    {
      return rowkey_info_;
    }

    uint64_t ObTableSchema::get_create_time_column_id() const
    {
      return create_time_column_id_;
    }

    uint64_t ObTableSchema::get_modify_time_column_id() const
    {
      return modify_time_column_id_;
    }

    void ObTableSchema::set_table_id(const uint64_t id)
    {
      table_id_ = id;
    }

    void ObTableSchema::set_version(const int64_t version)
    {
      version_ = version;
    }

    void ObTableSchema::set_max_column_id(const uint64_t id)
    {
      max_column_id_ = id;
    }

    void ObTableSchema::set_table_type(TableType type)
    {
      table_type_ = type;
    }

    void ObTableSchema::set_split_pos(const int64_t split_pos)
    {
      rowkey_split_ = split_pos;
    }

    void ObTableSchema::set_rowkey_max_length(const int64_t len)
    {
      rowkey_max_length_ = len;
    }

    void ObTableSchema::set_block_size(const int64_t block_size)
    {
      block_size_ = static_cast<int32_t>(block_size);
    }

    void ObTableSchema::set_max_sstable_size(const int64_t max_sstable_size)
    {
      max_sstable_size_ = max_sstable_size;
    }

    void ObTableSchema::set_table_name(const char* name)
    {
      if (name != NULL && *name != '\0')
      {
        snprintf(name_,sizeof(name_),"%s",name);
      }
    }

    void ObTableSchema::set_table_name(const ObString& name)
    {
      if (name.ptr() != NULL && static_cast<uint32_t>(name.length()) < sizeof(name_))
      {
        snprintf(name_,name.length() + 1,"%s",name.ptr());
      }
    }

    void ObTableSchema::set_compressor_name(const char* compressor)
    {
      if (compressor != NULL && *compressor != '\0')
      {
        snprintf(compress_func_name_,sizeof(compress_func_name_),"%s",compressor);
      }
    }

    void ObTableSchema::set_compressor_name(const ObString& compressor)
    {
      if (compressor.ptr() != NULL && static_cast<uint32_t>(compressor.length()) < sizeof(compress_func_name_))
      {
        snprintf(compress_func_name_,compressor.length() + 1,"%s",compressor.ptr());
      }
    }

    void ObTableSchema::set_pure_update_table(bool is_pure)
    {
      is_pure_update_table_ = is_pure;
    }

    void ObTableSchema::set_use_bloomfilter(bool use_bloomfilter)
    {
      is_use_bloomfilter_ = use_bloomfilter;
    }

    void ObTableSchema::set_has_baseline_data(bool base_data)
    {
      has_baseline_data_ = base_data;
    }

    void ObTableSchema::set_consistency_level(int64_t consistency_level)
    {
      consistency_level_ = (ObConsistencyLevel)consistency_level;
      is_merge_dynamic_data_ = (consistency_level_ != STATIC);
    }

    void ObTableSchema::set_expire_effect_immediately(
        const int64_t expire_effect_immediately)
    {
      is_expire_effect_immediately_ = expire_effect_immediately;
    }

    void ObTableSchema::set_create_time_column(uint64_t id)
    {
      create_time_column_id_ = id;
    }

    void ObTableSchema::set_expire_condition(const char* expire_condition)
    {
      if (expire_condition != NULL && *expire_condition != '\0')
      {
        snprintf(expire_condition_, sizeof(expire_condition_), "%s", expire_condition);
      }
    }

    void ObTableSchema::set_expire_condition(const ObString& expire_condition)
    {
      if (expire_condition.ptr() != NULL
          && static_cast<uint32_t>(expire_condition.length()) < sizeof(expire_condition_))
      {
        snprintf(expire_condition_, expire_condition.length() + 1, "%s", expire_condition.ptr());
      }
    }

    void ObTableSchema::set_comment_str(const char* comment_str)
    {
      if (comment_str != NULL && *comment_str != '\0')
      {
        snprintf(comment_str_, sizeof(comment_str_), "%s", comment_str);
      }
    }

    void ObTableSchema::set_expire_frequency(const int64_t expire_frequency)
    {
      expire_frequency_ = expire_frequency;
    }

    void ObTableSchema::set_rowkey_info(ObRowkeyInfo& rowkey_info)
    {
      rowkey_info_ = rowkey_info;
    }

    void ObTableSchema::set_query_cache_expire_time(const int64_t expire_time)
    {
      query_cache_expire_time_ = expire_time;
    }

    void ObTableSchema::set_max_scan_rows_per_tablet(const int64_t max_scan_rows)
    {
      max_scan_rows_per_tablet_ = max_scan_rows;
    }

    void ObTableSchema::set_internal_ups_scan_size(const int64_t scan_size)
    {
      internal_ups_scan_size_ = scan_size;
    }

    void ObTableSchema::set_merge_write_sstable_version(const int64_t version)
    {
      merge_write_sstable_version_ = version;
    }

    void ObTableSchema::set_replica_count(const int64_t count)
    {
      replica_count_ = count;
    }

    void ObTableSchema::set_schema_version(const int64_t version)
    {
      schema_version_ = version;
    }

    void ObTableSchema::set_modify_time_column(uint64_t id)
    {
      modify_time_column_id_ = id;
    }

    bool ObTableSchema::operator ==(const ObTableSchema& r) const
    {
      bool ret = false;

      if ( (table_id_ != OB_INVALID_ID && table_id_ == r.table_id_) ||
          (('\0' != *name_ && '\0' != *r.name_) && strlen(name_) == strlen(r.name_)
           && 0 == strncmp(name_,r.name_,strlen(name_))) )
      {
        ret = true;
      }
      return ret;
    }

    bool ObTableSchema::operator ==(const uint64_t table_id) const
    {
      bool ret = false;

      if (table_id != OB_INVALID_ID && table_id == table_id_)
      {
        ret = true;
      }
      return ret;
    }

    bool ObTableSchema::operator ==(const ObString& table_name) const
    {
      bool ret = false;
      if (0 == table_name.compare(name_))
      {
        ret = true;
      }
      return ret;
    }
    //modify wenghaixing [secondary index] 20141105
     //add wenghaixing [secondary index] 20141104_02
    void ObTableSchema::set_index_helper(const IndexHelper& ih)
    {
        ih_=ih;
    }
    /*
     *删除了add_index_list的接口 //repaired from messy code by zhuxh 20151014
     **/
   /* int ObTableSchema::add_index_list(uint64_t tid)
    {
        int ret=OB_ERROR;
        for(int64_t i=0;i<OB_MAX_INDEX_NUMS;i++){
            if(index_list_[i]==OB_INVALID_ID){

                index_list_[i]=tid;
                ret=OB_SUCCESS;
                break;
            }
        }
        if(OB_SUCCESS!=ret)
            TBSYS_LOG(ERROR,"add index_list_ error,one table has no more 5 index!");
            return ret;

    }
    */
    IndexHelper ObTableSchema::get_index_helper() const
    {
       return ih_;
    }
    //add e
    DEFINE_SERIALIZE(ObTableSchema)
    {
      int ret = 0;
      int64_t tmp_pos = pos;

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, table_id_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, max_column_id_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, rowkey_split_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, rowkey_max_length_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi32(buf, buf_len, tmp_pos, block_size_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi32(buf, buf_len, tmp_pos, table_type_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vstr(buf, buf_len, tmp_pos, name_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vstr(buf, buf_len, tmp_pos, compress_func_name_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, is_pure_update_table_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, is_use_bloomfilter_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = rowkey_info_.serialize(buf, buf_len, tmp_pos);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, has_baseline_data_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vstr(buf, buf_len, tmp_pos, expire_condition_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, expire_frequency_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, max_sstable_size_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, is_merge_dynamic_data_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, query_cache_expire_time_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, is_expire_effect_immediately_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, max_scan_rows_per_tablet_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, internal_ups_scan_size_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, merge_write_sstable_version_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, replica_count_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, version_);
      }
      if (OB_SUCCESS == ret)
      {
        if (version_ >= OB_SCHEMA_VERSION_FOUR_SECOND)
        {
          ret = serialization::encode_vi64(buf, buf_len, tmp_pos, schema_version_);
        }
        else
        {
          ret = serialization::encode_vi64(buf, buf_len, tmp_pos, reserved_[0]);
        }
      }
      for (int64_t i = 0; i < TABLE_SCHEMA_RESERVED_NUM && OB_SUCCESS == ret; ++i)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, reserved_[i]);
      }
      //add wenghaixing [secondary index] 20141104
     if(OB_SUCCESS==ret)
     {
       // for(int64_t i=0;i<ih_array_.count();i++){
       if (OB_SUCCESS != (ret = ih_.serialize(buf, buf_len, tmp_pos)))
       {
         TBSYS_LOG(WARN, "failed to serialize ih_array_, err=%d", ret);
         //break;
       }
       //}
     }
      //add e
      //add wenghaixing [secondary index] 20141104
      /*
      *删除了index_list_，所以序列化的接口关闭
       if(OB_SUCCESS==ret)
       {
           for(int64_t i=0;i<OB_MAX_INDEX_NUMS;i++){

               if(OB_SUCCESS!=(ret=serialization::encode_vi64(buf, buf_len, tmp_pos,index_list_[i])))
               {
                   TBSYS_LOG(WARN,"failed to serialized index_list,t_id[%ld],i[%ld]",index_list_[i],i);
               }
                TBSYS_LOG(ERROR,"test::whx index_list_[%ld]",index_list_[i]);

           }
       }
       */
      //add e
      if (OB_SUCCESS == ret)
      {
        // count
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, 0);
      }
      /*
      if (OB_SUCCESS == ret && version_ >= OB_SCHEMA_VERSION_FOUR_FIRST)
      {
        ret = serialization::encode_vstr(buf, buf_len, tmp_pos, comment_str_);
      }
      if (OB_SUCCESS == ret && version_ >= OB_SCHEMA_VERSION_FOUR_SECOND)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, consistency_level_);
      }
      */
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }

    int ObTableSchema::deserialize_v3(const char* buf, const int64_t data_len, int64_t& pos)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos,
            reinterpret_cast<int64_t *>(&table_id_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos,
            reinterpret_cast<int64_t *>(&max_column_id_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &rowkey_split_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &rowkey_max_length_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi32(buf, data_len, tmp_pos, &block_size_);
      }

      if (OB_SUCCESS == ret)
      {
        int32_t table_type = 0;
        ret = serialization::decode_vi32(buf, data_len, tmp_pos, &table_type);
        table_type_ = static_cast<TableType>(table_type);
      }
      if (OB_SUCCESS == ret)
      {
        int64_t len = 0;
        serialization::decode_vstr(buf, data_len, tmp_pos,
            name_, OB_MAX_TABLE_NAME_LENGTH, &len);
        if (-1 == len)
        {
          ret = OB_ERROR;
        }
      }
      if (OB_SUCCESS == ret)
      {
        int64_t len = 0;
        serialization::decode_vstr(buf, data_len, tmp_pos,
            compress_func_name_, OB_MAX_TABLE_NAME_LENGTH, &len);
        if (-1 == len)
        {
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &is_pure_update_table_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &is_use_bloomfilter_);
      }
      if (OB_SUCCESS == ret)
      {
        // rowkey fix len no need in v4
        bool is_row_key_fixed_len = false;
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &is_row_key_fixed_len);
      }
      if (OB_SCHEMA_VERSION < get_version())
      {
        int64_t expire_column_id = 0;
        int64_t expire_duration = 0;
        // expire_info no need in v4
        if (OB_SUCCESS == ret)
        {
          ret = serialization::decode_vi64(buf, data_len, tmp_pos, &expire_column_id);
        }
        if (OB_SUCCESS == ret)
        {
          ret = serialization::decode_vi64(buf, data_len, tmp_pos, &expire_duration);
        }
      }
      if (OB_SCHEMA_VERSION_TWO < get_version())
      {
        int64_t len = 0;
        serialization::decode_vstr(buf, data_len, tmp_pos,
            expire_condition_, OB_MAX_EXPIRE_CONDITION_LENGTH, &len);
        if (-1 == len)
        {
          ret = OB_ERROR;
        }
        if (OB_SUCCESS == ret)
        {
          ret = serialization::decode_vi64(buf, data_len, tmp_pos, &expire_frequency_);
        }
        if (OB_SUCCESS == ret)
        {
          ret = serialization::decode_vi64(buf, data_len, tmp_pos, &max_sstable_size_);
        }
        if (OB_SUCCESS == ret)
        {
          ret = serialization::decode_bool(buf, data_len, tmp_pos, &is_merge_dynamic_data_);
        }
        if (OB_SUCCESS == ret)
        {
          ret = serialization::decode_vi64(buf, data_len, tmp_pos, &query_cache_expire_time_);
        }
        if (OB_SUCCESS == ret)
        {
          ret = serialization::decode_vi64(buf, data_len, tmp_pos, &is_expire_effect_immediately_);
        }
        if (OB_SUCCESS == ret)
        {
          ret = serialization::decode_vi64(buf, data_len, tmp_pos, &max_scan_rows_per_tablet_);
        }
        if (OB_SUCCESS == ret)
        {
          ret = serialization::decode_vi64(buf, data_len, tmp_pos, &internal_ups_scan_size_);
        }
        for (int64_t i = 0; i < TABLE_SCHEMA_RESERVED_NUM && OB_SUCCESS == ret; ++i)
        {
          ret = serialization::decode_vi64(buf, data_len, tmp_pos, &reserved_[i]);
        }
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }

    int ObTableSchema::deserialize_v4(const char* buf, const int64_t data_len, int64_t& pos)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos,
            reinterpret_cast<int64_t *>(&table_id_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos,
            reinterpret_cast<int64_t *>(&max_column_id_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &rowkey_split_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &rowkey_max_length_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi32(buf, data_len, tmp_pos, &block_size_);
      }

      if (OB_SUCCESS == ret)
      {
        int32_t table_type = 0;
        ret = serialization::decode_vi32(buf, data_len, tmp_pos, &table_type);
        table_type_ = static_cast<TableType>(table_type);
      }
      if (OB_SUCCESS == ret)
      {
        int64_t len = 0;
        serialization::decode_vstr(buf, data_len, tmp_pos,
            name_, OB_MAX_TABLE_NAME_LENGTH, &len);
        if (-1 == len)
        {
          ret = OB_ERROR;
        }
      }
      if (OB_SUCCESS == ret)
      {
        int64_t len = 0;
        serialization::decode_vstr(buf, data_len, tmp_pos,
            compress_func_name_, OB_MAX_TABLE_NAME_LENGTH, &len);
        if (-1 == len)
        {
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &is_pure_update_table_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &is_use_bloomfilter_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = rowkey_info_.deserialize(buf, data_len, tmp_pos);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &has_baseline_data_);
      }
      if (OB_SUCCESS == ret)
      {
        int64_t len = 0;
        serialization::decode_vstr(buf, data_len, tmp_pos,
            expire_condition_, OB_MAX_EXPIRE_CONDITION_LENGTH, &len);
        if (-1 == len)
        {
          ret = OB_ERROR;
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &expire_frequency_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &max_sstable_size_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &is_merge_dynamic_data_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &query_cache_expire_time_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &is_expire_effect_immediately_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &max_scan_rows_per_tablet_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &internal_ups_scan_size_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &merge_write_sstable_version_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &replica_count_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &version_);
      }
      if (OB_SUCCESS == ret)
      {
        if (version_ >= OB_SCHEMA_VERSION_FOUR_SECOND)
        {
          ret = serialization::decode_vi64(buf, data_len, tmp_pos, &schema_version_);
        }
        else
        {
          ret = serialization::decode_vi64(buf, data_len, tmp_pos, &reserved_[0]);
        }
      }
      for (int64_t i = 0; i < TABLE_SCHEMA_RESERVED_NUM && OB_SUCCESS == ret; ++i)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &reserved_[i]);
      }
       //add wenghaixing [secondary index] 20141104
        //to do
      if(OB_SUCCESS==ret && get_version() >= OB_SCHEMA_VERSION_SIX_FIRST)
      {
        IndexHelper ih;
        if (OB_SUCCESS != (ret = ih.deserialize(buf, data_len, tmp_pos)))
        {
          TBSYS_LOG(WARN, "fail to deserialize index helper:ret[%d]", ret);
        }
        else
        {
          ih_=ih;
        }
      }

      //add e

      //add wenghaixing [secondary index] 20141104
      /*删除了index_list，所以反序列化的接口关闭
       if(OB_SUCCESS==ret)
        {

            for(int64_t i=0;i<OB_MAX_INDEX_NUMS;i++)
            {
                uint64_t idx_id;
                if(OB_SUCCESS!=(ret=serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&idx_id))))
                {
                    TBSYS_LOG(WARN,"failed to decode idx_id,i[%ld]",idx_id);
                    break;
                }
                else
                {
                    index_list_[i]=idx_id;
                    TBSYS_LOG(ERROR,"test::whx idx_id idx_id,[%ld]",idx_id);
                }
            }
        }
        */
      //add e
      int64_t count = 0;
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &count);
      }
      if (OB_SUCCESS == ret)
      {
        for (int64_t i = 0;i < count;++i)
        {
          ObObj value_obj;
          if (OB_SUCCESS != (ret = value_obj.deserialize(buf, data_len, tmp_pos)))
          {
            TBSYS_LOG(WARN, "deserialize value failed, ret=%d", ret);
          }
          if (OB_SUCCESS != ret)
          {
            break;
          }
        }
      }
      /*
      if (OB_SUCCESS == ret && version_ >= OB_SCHEMA_VERSION_FOUR_FIRST)
      {
        int64_t len = 0;
        serialization::decode_vstr(buf, data_len, tmp_pos, comment_str_, OB_MAX_TABLE_COMMENT_LENGTH, &len);
        if (-1 == len)
        {
          ret = OB_ERROR;
        }
      }
      if (OB_SUCCESS == ret && version_ == OB_SCHEMA_VERSION_FOUR_SECOND)
      {
        int64_t consistency_level_value = -1;
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &consistency_level_value);
        consistency_level_ = (ObConsistencyLevel)consistency_level_value;
      }
      */
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }

    DEFINE_DESERIALIZE(ObTableSchema)
    {
      int ret = OB_SUCCESS;
      if (get_version() <= OB_SCHEMA_VERSION_THREE)
      {
        ret = deserialize_v3(buf, data_len, pos);
      }
      else
      {
        ret = deserialize_v4(buf, data_len, pos);
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTableSchema)
    {
      int64_t len =  serialization::encoded_length_vi64(table_id_);
      len += serialization::encoded_length_vi64(max_column_id_);
      len += serialization::encoded_length_vi64(rowkey_split_);
      len += serialization::encoded_length_vi64(rowkey_max_length_);
      len += serialization::encoded_length_vi32(block_size_);
      len += serialization::encoded_length_vi32(table_type_);
      len += serialization::encoded_length_vstr(name_);
      len += serialization::encoded_length_vstr(compress_func_name_);
      len += serialization::encoded_length_bool(is_pure_update_table_);
      len += serialization::encoded_length_bool(is_use_bloomfilter_);
      len += rowkey_info_.get_serialize_size();
      len += serialization::encoded_length_bool(has_baseline_data_);
      len += serialization::encoded_length_vstr(expire_condition_);
      len += serialization::encoded_length_vi64(expire_frequency_);
      len += serialization::encoded_length_vi64(max_sstable_size_);
      len += serialization::encoded_length_bool(is_merge_dynamic_data_);
      len += serialization::encoded_length_vi64(query_cache_expire_time_);
      len += serialization::encoded_length_vi64(is_expire_effect_immediately_);
      len += serialization::encoded_length_vi64(max_scan_rows_per_tablet_);
      len += serialization::encoded_length_vi64(internal_ups_scan_size_);
      len += serialization::encoded_length_vi64(merge_write_sstable_version_);
      len += serialization::encoded_length_vi64(replica_count_);
      len += serialization::encoded_length_vi64(version_);
      if (OB_SCHEMA_VERSION_FOUR_SECOND <= version_)
      {
        len += serialization::encoded_length_vi64(schema_version_);
      }
      else
      {
        len += serialization::encoded_length_vi64(reserved_[0]);
      }
      len += serialization::encoded_length_vi64(reserved_[0]) * TABLE_SCHEMA_RESERVED_NUM;
      len += serialization::encoded_length_vi64(0);
      //add wenghaixing [secondary index]20150617
      if(OB_SCHEMA_VERSION_SIX_FIRST <= version_)
      {
        len += serialization::encoded_length_vi64(ih_.tbl_tid);
        len += serialization::encoded_length_i32(ih_.status);
      }
      //add e
      /*
      if (OB_SCHEMA_VERSION_FOUR_FIRST <= version_)
      {
        len += serialization::encoded_length_vstr(comment_str_);
        len += serialization::encoded_length_vi64(schema_version_);
      }
      if (OB_SCHEMA_VERSION_FOUR_SECOND == version_ )
      {
        len += serialization::encoded_length_vi64(consistency_level_);
      }
      */
      return len;
    }



    /*-----------------------------------------------------------------------------
     *  ObSchemaManagerV2
     *-----------------------------------------------------------------------------*/
    ObSchemaManagerV2::ObSchemaManagerV2(): schema_magic_(OB_SCHEMA_MAGIC_NUMBER),
    //mod zhaoqiong [Schema Manager] 20150424:b
    //version_(OB_SCHEMA_VERSION_FOUR),
    version_(OB_SCHEMA_VERSION_SIX),
    //mod:e
    timestamp_(0), max_table_id_(OB_INVALID_ID),column_nums_(0),
    table_nums_(0), columns_(NULL), column_capacity_(0),
    //add zhaoqiong [Schema Manager] 20150327:b
    table_begin_(0),table_end_(0),column_begin_(0),column_end_(0),is_completion_(true),
    //add:e
    drop_column_group_(false),hash_sorted_(false),
    //add wenghaixing [secondary index] 20141105
    index_hash_init_(false),
    //add e
    column_group_nums_(0)
    {
      app_name_[0] = '\0';
    }

    ObSchemaManagerV2::ObSchemaManagerV2(const int64_t timestamp): schema_magic_(OB_SCHEMA_MAGIC_NUMBER),
    //mod zhaoqiong [Schema Manager] 20150424:b
    //version_(OB_SCHEMA_VERSION_FOUR),
    version_(OB_SCHEMA_VERSION_SIX),
    //mod:e
    timestamp_(timestamp),
    max_table_id_(OB_INVALID_ID),column_nums_(0),
    table_nums_(0), columns_(NULL), column_capacity_(0),
    //add zhaoqiong [Schema Manager] 20150327:b
    table_begin_(0),table_end_(0),column_begin_(0),column_end_(0),is_completion_(true),
    //add:e
    drop_column_group_(false),hash_sorted_(false),
    //add wenghaixing [secondary index] 20141105
    index_hash_init_(false),
    //add e
    column_group_nums_(0)
    {
      app_name_[0] = '\0';
    }

    ObSchemaManagerV2::~ObSchemaManagerV2()
    {
      if (hash_sorted_)
      {
        column_hash_map_.destroy();
        id_hash_map_.destroy();
      }
       //add wenghaixing [secondary index] 20141105
      if(index_hash_init_)
      {
        id_index_map_.destroy();
      }
    //add e
      if (NULL != columns_)
      {
        ob_free(columns_, ObModIds::OB_SCHEMA);
        columns_ = NULL;
      }
    }

    ObSchemaManagerV2& ObSchemaManagerV2::operator=(const ObSchemaManagerV2& schema) //ugly,for hashMap
    {
      if (this != &schema)
      {
        schema_magic_ = schema.schema_magic_;
        version_ = schema.version_;
        timestamp_ = schema.timestamp_;
        max_table_id_ = schema.max_table_id_;
        table_nums_ = schema.table_nums_;
        //add zhaoqiong [Schema Manager] 20150327:b
        table_begin_ = schema.table_begin_;
        table_end_ = schema.table_end_;
        column_begin_ = schema.column_begin_;
        column_end_ = schema.column_end_;
        is_completion_ = schema.is_completion_;
        //add:e

        snprintf(app_name_,sizeof(app_name_),"%s",schema.app_name_);

        memcpy(&table_infos_,&schema.table_infos_,sizeof(table_infos_));

        if (OB_SUCCESS == prepare_column_storage(schema.column_nums_))
        {
          memcpy(columns_, schema.columns_, schema.column_nums_ * sizeof(ObColumnSchemaV2));
          column_nums_ = schema.column_nums_;
        }

        //mod zhaoqiong [Schema Manager] 20150327:b
        //ret = sort_column();
        int ret = OB_SUCCESS;
        if (is_completion_)
        {
          ret = sort_column();
          //add liuxiao [secondary index static index] 20150615
          ret=init_index_hash();
          //add e
        }
        //mod:e

        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "sort column failed:ret[%d]", ret);
        }
      }
      return *this;
    }


    //add zhaoqiong [Schema Manager] 20150423:b
    void ObSchemaManagerV2::copy_without_sort(const ObSchemaManagerV2& schema)
    {
      if (this != &schema)
      {
        schema_magic_ = schema.schema_magic_;
        version_ = schema.version_;
        timestamp_ = schema.timestamp_;
        max_table_id_ = schema.max_table_id_;
        table_nums_ = schema.table_nums_;
        table_begin_ = schema.table_begin_;
        table_end_ = schema.table_end_;
        column_begin_ = schema.column_begin_;
        column_end_ = schema.column_end_;
        is_completion_ = schema.is_completion_;

        snprintf(app_name_,sizeof(app_name_),"%s",schema.app_name_);

        memcpy(&table_infos_,&schema.table_infos_,sizeof(table_infos_));

        if (OB_SUCCESS == prepare_column_storage(schema.column_nums_))
        {
          memcpy(columns_, schema.columns_, schema.column_nums_ * sizeof(ObColumnSchemaV2));
          column_nums_ = schema.column_nums_;
        }
      }
    }
    //add:e



    ObSchemaManagerV2::ObSchemaManagerV2(const ObSchemaManagerV2& schema) : schema_magic_(OB_SCHEMA_MAGIC_NUMBER),
    //mod zhaoqiong [Schema Manager] 20150424:b
//    version_(OB_SCHEMA_VERSION_FOUR),
    version_(OB_SCHEMA_VERSION_SIX),
    //mod:e
    timestamp_(0),
    max_table_id_(OB_INVALID_ID),
    column_nums_(0),
    table_nums_(0),
    columns_(NULL),
    column_capacity_(0),
    //add zhaoqiong [Schema Manager] 20150327:b
    table_begin_(0),
    table_end_(0),
    column_begin_(0),
    column_end_(0),
    is_completion_(true),
    //add:e
    drop_column_group_(false),
    hash_sorted_(false),
    column_group_nums_(0)
    {
      app_name_[0] = '\0';
      *this = schema;
    }

    const ObTableSchema* ObSchemaManagerV2::table_begin() const
    {
      return table_infos_;
    }

    const ObTableSchema* ObSchemaManagerV2::table_end() const
    {
      return table_infos_ + table_nums_;
    }

    const ObTableSchema* ObSchemaManagerV2::get_table_schema(const char* table_name) const
    {
      const ObTableSchema *table = NULL;

      if (table_name != NULL && *table_name != '\0' && table_nums_ > 0)
      {
        ObTableSchema tmp;
        tmp.set_table_name(table_name);
        table = std::find(table_infos_,table_infos_ + table_nums_,tmp);
        if (table == (table_infos_ + table_nums_))
        {
          table = NULL;
        }
      }
      return table;
    }

    const ObTableSchema* ObSchemaManagerV2::get_table_schema(const ObString& table_name) const
    {
      const ObTableSchema *table = NULL;

      if (table_name.ptr() != NULL && table_name.length() > 0 && table_nums_ > 0)
      {
        table = std::find(table_infos_,table_infos_ + table_nums_,table_name);
        if (table == (table_infos_ + table_nums_))
        {
          table = NULL;
        }
      }
      return table;
    }


    const ObTableSchema* ObSchemaManagerV2::get_table_schema(const uint64_t table_id) const
    {
      const ObTableSchema *table = NULL;

      if (table_nums_ > 0)
      {
        ObTableSchema tmp;
        tmp.set_table_id(table_id);
        table = std::find(table_infos_,table_infos_ + table_nums_,tmp);
        if (table == (table_infos_ + table_nums_))
        {
          table = NULL;
        }
      }
      return table;
    }

    ObTableSchema* ObSchemaManagerV2::get_table_schema(const char* table_name)
    {
      ObTableSchema *table = NULL;

      if (table_name != NULL && *table_name != '\0' && table_nums_ > 0)
      {
        ObTableSchema tmp;
        tmp.set_table_name(table_name);
        table = std::find(table_infos_,table_infos_ + table_nums_,tmp);
        if (table == (table_infos_ + table_nums_))
        {
          table = NULL;
        }
      }
      return table;
    }

    ObTableSchema* ObSchemaManagerV2::get_table_schema(const uint64_t table_id)
    {
      ObTableSchema *table = NULL;

      if (table_nums_ > 0)
      {
        ObTableSchema tmp;
        tmp.set_table_id(table_id);
        table = std::find(table_infos_,table_infos_ + table_nums_,tmp);
        if (table == (table_infos_ + table_nums_))
        {
          table = NULL;
        }
      }
      return table;
    }

    int64_t ObSchemaManagerV2::get_table_query_cache_expire_time(const ObString& table_name) const
    {
      int64_t expire_second = 0;
      const ObTableSchema* table_schema = get_table_schema(table_name);

      if (NULL != table_schema)
      {
        expire_second = table_schema->get_query_cache_expire_time();
      }

      return expire_second;
    }

    uint64_t ObSchemaManagerV2::get_create_time_column_id(const uint64_t table_id) const
    {
      uint64_t id = OB_INVALID_ID;
      const ObTableSchema *table = get_table_schema(table_id);
      if (table != NULL)
      {
        id = table->get_create_time_column_id();
      }
      return id;
    }

    uint64_t ObSchemaManagerV2::get_modify_time_column_id(const uint64_t table_id) const
    {
      uint64_t id = OB_INVALID_ID;
      const ObTableSchema *table = get_table_schema(table_id);
      if (table != NULL)
      {
        id = table->get_modify_time_column_id();
      }
      return id;
    }

    struct __table_sort
    {
      __table_sort(tbsys::CConfig& config): config_(config) {}
      bool operator()(const std::string& l,const std::string& r)
      {
        uint64_t l_table_id = config_.getInt(l.c_str(),STR_TABLE_ID,0);
        uint64_t r_table_id = config_.getInt(r.c_str(),STR_TABLE_ID,0);
        return l_table_id < r_table_id;
      }
      tbsys::CConfig& config_;
    };


    bool ObSchemaManagerV2::parse_from_file(const char* file_name, tbsys::CConfig& config)
    {
      bool parse_ok = true;

      if (parse_ok && file_name != NULL && config.load(file_name) != EXIT_SUCCESS)
      {
        TBSYS_LOG(ERROR, "can not open config file, file name is %s", file_name);
        parse_ok = false;
      }

      TBSYS_LOG(DEBUG,"config:%p",&config);


      if (parse_ok)
      {
        const char* p = config.getString(STR_SECTION_APP_NAME, STR_KEY_APP_NAME, NULL);

        int length = 0;
        if (p != NULL) length = static_cast<int32_t>(strlen(p));
        if (p == NULL || length >= OB_MAX_APP_NAME_LENGTH)
        {
          TBSYS_LOG(ERROR, "parse [%s]  %s error", STR_SECTION_APP_NAME, STR_KEY_APP_NAME);
          parse_ok = false;
        }
        else
        {
          strncpy(app_name_, p, OB_MAX_APP_NAME_LENGTH);
          app_name_[OB_MAX_APP_NAME_LENGTH - 1] = '\0';
          max_table_id_ = config.getInt(STR_SECTION_APP_NAME, STR_MAX_TABLE_ID, 0);
          if (max_table_id_ > MAX_ID_USED)
          {
            TBSYS_LOG(ERROR, "we limit our table id less than %lu", MAX_ID_USED);
            parse_ok = false;
          }
          if (max_table_id_ < 1)
          {
            TBSYS_LOG(ERROR, "max_table_id is %lu", max_table_id_);
            parse_ok = false;
          }

          //mod zhaoqiong [Schema Manager] 20150424:b
//          version_ = config.getInt(STR_SECTION_APP_NAME, STR_SCHEMA_VERSION,
//                                   OB_SCHEMA_VERSION_FOUR);
//          if (version_ > OB_SCHEMA_VERSION_FOUR)
//          {
//            TBSYS_LOG(ERROR, "we limit our schema version less than %ld",
//                      OB_SCHEMA_VERSION_FOUR);
          version_ = config.getInt(STR_SECTION_APP_NAME, STR_SCHEMA_VERSION,
                                   OB_SCHEMA_VERSION_FIVE);
          if (version_ > OB_SCHEMA_VERSION_FIVE)
          {
            TBSYS_LOG(ERROR, "we limit our schema version less than %ld",
                      OB_SCHEMA_VERSION_FIVE);
            //mod:e
            parse_ok = false;
          }
          if (version_ < OB_SCHEMA_VERSION_TWO)
          {
            TBSYS_LOG(ERROR, "version_ is %d, must greater than %ld",
                version_, OB_SCHEMA_VERSION);
            parse_ok = false;
          }

          vector<string> sections;
          config.getSectionName(sections);
          table_nums_ = sections.size() - 1;
          if (table_nums_ > OB_MAX_TABLE_NUMBER)
          {
            TBSYS_LOG(ERROR, "%ld error table number", table_nums_);
            parse_ok = false;
            table_nums_ = 0;
          }
          else if (table_nums_ < 1)
          {
            TBSYS_LOG(INFO, "not table schema in file.");
            table_nums_ = 0;
          }
          // app section and sections.size() - 1 tables

          //sort the table
          std::sort(sections.begin(),sections.end(),__table_sort(config));

          uint32_t index = 0;

          for(vector<string>::iterator it = sections.begin();
              it != sections.end() && parse_ok; ++it)
          {
            if (strcmp(it->c_str(), STR_SECTION_APP_NAME) == 0)
            {
              continue;
            }
            TBSYS_LOG(DEBUG,"table name :%s ",it->c_str());

            if (!parse_one_table(it->c_str(), config, table_infos_[index]))
            {
              parse_ok = false;
              table_nums_ = 0;
              break;
            }
            table_infos_[index].set_version(version_);
            ++index;
          }

          if (parse_ok && sort_column() != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR,"sort column failed");
            parse_ok = false;
            init_index_hash();
          }

          TBSYS_LOG(DEBUG,"config:%p",&config);
          // we must get all tables, so we can check join info;
          index = 0;
          for(vector<string>::iterator it = sections.begin();
              it != sections.end() && parse_ok; ++it)
          {
            if (strcmp(it->c_str(), STR_SECTION_APP_NAME) == 0)
            {
              continue;
            }
            if ( !parse_rowkey_info(it->c_str(), config, table_infos_[index]) )
            {
              parse_ok = false;
              table_nums_ = 0;
              break;
            }
            ++index;
          }

          // we must get all tables, so we can check join info;
          index = 0;
          for(vector<string>::iterator it = sections.begin();
              it != sections.end() && parse_ok; ++it)
          {
            if (strcmp(it->c_str(), STR_SECTION_APP_NAME) == 0)
            {
              continue;
            }
            if ( !parse_join_info(it->c_str(), config, table_infos_[index]) )
            {
              parse_ok = false;
              table_nums_ = 0;
              break;
            }
            ++index;
          }

          if (parse_ok)
          {
            parse_ok = check_table_expire_condition();
          }
          if (parse_ok)
          {
            parse_ok = check_compress_name();
          }
        }

      }
      return parse_ok;
    }

    bool ObSchemaManagerV2::parse_one_table(const char* section_name, tbsys::CConfig& config, ObTableSchema& schema)
    {
      uint64_t table_id = OB_INVALID_ID;
      int type = 0;
      //ObSchema::TableType table_type = ObSchema::INVALID;
      ObTableSchema::TableType table_type = ObTableSchema::INVALID;
      int32_t rowkey_split = 0;
      int32_t rowkey_max_length = 0;
      uint64_t max_column_id = 0;

      bool parse_ok = true;
      const char* name = section_name;
      int name_len = 0;
      if (name != NULL) name_len = static_cast<int32_t>(strlen(name));
      if (name == NULL || name_len >= OB_MAX_TABLE_NAME_LENGTH)
      {
        TBSYS_LOG(ERROR, "table_name is missing");
        parse_ok = false;
      }
      if (parse_ok)
      {
        table_id = config.getInt(section_name, STR_TABLE_ID, 0);
        if (table_id == 0)
        {
          TBSYS_LOG(ERROR, "table id can not be 0");
          parse_ok = false;
        }
        if ( /*(table_id < OB_APP_MIN_TABLE_ID) ||*/ (table_id > max_table_id_))
        {
          TBSYS_LOG(ERROR, "table id %lu greater than max_table_id %lu or less than %lu",
              table_id, max_table_id_,OB_APP_MIN_TABLE_ID);
          parse_ok = false;
        }
      }
      if (parse_ok && get_table_schema(table_id) != NULL)
      {
        TBSYS_LOG(ERROR, "table id %lu have been used", table_id);
        parse_ok = false;
      }

      if (parse_ok)
      {
        type = config.getInt(section_name, STR_TABLE_TYPE, 0);

        if (type == ObTableSchema::SSTABLE_IN_DISK)
        {
          table_type = ObTableSchema::SSTABLE_IN_DISK;
        }
        else if (type == ObTableSchema::SSTABLE_IN_RAM)
        {
          table_type = ObTableSchema::SSTABLE_IN_RAM;
        }
        else
        {
          TBSYS_LOG(ERROR, "%d is a invalid table type", type);
          parse_ok = false;
        }
      }

      if (parse_ok)
      {
        //rowkey_split = config.getInt(section_name, STR_ROWKEY_SPLIT, 0);
        //rowkey_max_length = config.getInt(section_name, STR_ROWKEY_LENGTH, 0);
        max_column_id = config.getInt(section_name, STR_MAX_COLUMN_ID, 0);
        if (max_column_id < 1)
        {
          TBSYS_LOG(ERROR, "max_column_id is %lu", max_column_id);
          parse_ok = false;
        }
        if (max_column_id > MAX_ID_USED)
        {
          TBSYS_LOG(ERROR, "we limit our column id less than %lu", MAX_ID_USED);
          parse_ok = false;
        }
      }
      const char* compress_func_name = NULL;
      int32_t block_size = 0;
      int32_t is_use_bloomfilter = 0;
      int32_t has_baseline_data = 0;
      if (parse_ok)
      {
        compress_func_name = config.getString(section_name, STR_COMPRESS_FUNC_NAME,"none");
        block_size = config.getInt(section_name, STR_BLOCK_SIZE, OB_DEFAULT_SSTABLE_BLOCK_SIZE);
        is_use_bloomfilter = config.getInt(section_name, STR_USE_BLOOMFILTER, 0);
        has_baseline_data = config.getInt(section_name, STR_HAS_BASELINE_DATA, 0);
        if (block_size < 0)
        {
          TBSYS_LOG(ERROR, "block_size is %d, must >= 0",
              block_size);
          parse_ok = false;
        }
      }

      if (parse_ok )
      {
        schema.set_table_id(table_id);
        schema.set_max_column_id(max_column_id);
        schema.set_table_name(name);
        schema.set_table_type(table_type);
        schema.set_split_pos(rowkey_split);
        schema.set_rowkey_max_length(rowkey_max_length);
        schema.set_compressor_name(compress_func_name);
        schema.set_block_size(block_size);
        schema.set_use_bloomfilter(is_use_bloomfilter);
        schema.set_has_baseline_data(has_baseline_data);
      }

      if (parse_ok)
      {
        const char* expire_condition =
          config.getString(section_name, STR_EXPIRE_CONDITION, "");
        int64_t expire_frequency =
          config.getInt(section_name, STR_EXPIRE_FREQUENCY, 1);
        int64_t max_sstable_size =
          config.getInt(section_name, STR_MAX_SSTABLE_SIZE, OB_DEFAULT_MAX_TABLET_SIZE);
        int64_t consistency_level =
          config.getInt(section_name, STR_CONSISTENCY_LEVEL, 0);
        int64_t expire_time =
          config.getInt(section_name, STR_QUERY_CACHE_EXPIRE_TIME, 0);
        int64_t expire_effect =
          config.getInt(section_name, STR_IS_EXPIRE_EFFECT_IMMEDIATELY, 0);
        int64_t max_scan_rows =
          config.getInt(section_name, STR_MAX_SCAN_ROWS_PER_TABLET, 0);
        int64_t scan_size =
          config.getInt(section_name, STR_INTERNAL_UPS_SCAN_SIZE, 0);
		//add zhaoqiong[roottable tablet management]20150409:b
		int64_t replica_num =
          config.getInt(section_name, STR_REPLICA_NUM, 3); 
		//add e
        if (expire_frequency < 1)
        {
          TBSYS_LOG(ERROR, "expire_frequency is %ld, must greater than 0",
              expire_frequency);
          parse_ok = false;
        }
        else if (max_sstable_size < 0)
        {
          TBSYS_LOG(ERROR, "max_sstable_size is %ld, must >= 0",
              max_sstable_size);
          parse_ok = false;
        }
        else if (expire_time < 0)
        {
          TBSYS_LOG(ERROR, "query_cache_expire_second is %ld, must >= 0",
              expire_time);
          parse_ok = false;
        }
        else if (expire_effect < 0)
        {
          TBSYS_LOG(ERROR, "is_expire_effect_immediately is %ld, must >= 0",
              expire_time);
          parse_ok = false;
        }
        else if (max_scan_rows < 0)
        {
          TBSYS_LOG(ERROR, "max_scan_rows_per_tablet is %ld, must >= 0",
              max_scan_rows);
          parse_ok = false;
        }
        else if (scan_size < 0 || scan_size >= OB_MAX_PACKET_LENGTH) {
          TBSYS_LOG(ERROR, "internal_ups_scan_size is %ld, must >= 0",
              scan_size);
          parse_ok = false;
        }
		//add zhaoqiong[roottable tablet management]20150409:b
		else if (replica_num <= 0 || replica_num > OB_MAX_COPY_COUNT){
		  TBSYS_LOG(ERROR, "internal_replica_num is %ld, must >= 0",
              replica_num);
          parse_ok = false;
		}
		// add e
        else
        {
          schema.set_expire_condition(expire_condition);
          schema.set_expire_frequency(expire_frequency);
          schema.set_max_sstable_size(max_sstable_size);
          schema.set_consistency_level(consistency_level);
          schema.set_query_cache_expire_time(expire_time * 1000L);
          schema.set_expire_effect_immediately(expire_effect);
          schema.set_max_scan_rows_per_tablet(max_scan_rows);
          schema.set_internal_ups_scan_size(scan_size);
		  //add zhaoqiong[roottable tablet management]20150409:b
		  schema.set_replica_count(replica_num);
		  //add e
        }
      }

      if (parse_ok)
      {
        parse_ok = parse_column_info(section_name, config, schema);
      }

      return parse_ok;
    }

    bool ObSchemaManagerV2::parse_column_info(const char* section_name, tbsys::CConfig& config, ObTableSchema& schema)
    {
      bool parse_ok = true;
      char *str = NULL;
      uint64_t id = OB_INVALID_ID;
      uint64_t maintained = 0;
      ColumnType type = ObNullType;
      vector<const char*> column_info_strs;

      bool has_create_time_column = false;
      bool has_modify_time_column = false;
      bool has_create_time_id = false;
      bool has_modify_time_id = false;
      if (section_name != NULL)
      {
        column_info_strs = config.getStringList(section_name, STR_COLUMN_INFO);
      }
      if (column_info_strs.empty())
      {
        parse_ok = false;
      }
      int size = 0;

      ObColumnSchemaV2 *columns_info = new (std::nothrow) ObColumnSchemaV2 [OB_MAX_COLUMN_NUMBER];
      int32_t column_index = 0;

      if (NULL == columns_info)
      {
        TBSYS_LOG(ERROR,"alloc columns_info failed");
        parse_ok = false;
      }

      for(vector<const char*>::const_iterator it = column_info_strs.begin();
          it != column_info_strs.end() && parse_ok; ++it)
      {
        size = 0;
        str = strdup(*it);
        vector<char*> node_list;
        str = str_trim(str);
        tbsys::CStringUtil::split(str, ",", node_list);
        if (node_list.size() < POS_COLUM_TYPE + 1)
        {
          TBSYS_LOG(ERROR, "parse column |%s| error", str);
          parse_ok = false;
        }
        if (parse_ok)
        {
          maintained = strtoll(node_list[POS_COLUM_MANTAINED], NULL, 10);
          if (maintained != 0 && maintained != 1)
          {
            TBSYS_LOG(ERROR, "maintained %lu is unacceptable", maintained);
            parse_ok = false;
          }
          if (maintained == 1)
          {
            schema.set_pure_update_table(false);
          }
        }
        if (parse_ok)
        {
          id = strtoll(node_list[POS_COLUM_ID], NULL, 10);
          if (id == 0 )
          {
            TBSYS_LOG(ERROR, "column id error id = %lu", id);
            parse_ok = false;
          }
        }
        if (parse_ok)
        {
          if ((uint64_t)OB_CREATE_TIME_COLUMN_ID == id)
          {
            has_create_time_id = true;
          }
          if ((uint64_t)OB_MODIFY_TIME_COLUMN_ID == id)
          {
            has_modify_time_id = true;
          }
        }

        if (parse_ok)
        {
          type = ObColumnSchemaV2::convert_str_to_column_type(node_list[POS_COLUM_TYPE]);
          if (type == ObNullType)
          {
            TBSYS_LOG(ERROR, "column type error |%s|", node_list[POS_COLUM_TYPE]);
            parse_ok = false;
          }
        }

        if (parse_ok && type == ObVarcharType)
        {
          if (node_list.size() < POS_COLUM_SIZE + 1)
          {
            TBSYS_LOG(ERROR, "column type need size |%s|", node_list[POS_COLUM_TYPE]);
            parse_ok = false;
          }
          if (parse_ok)
          {
            size = static_cast<int32_t>(strtoll(node_list[POS_COLUM_SIZE], NULL, 10));
            if (size <= 0)
            {
              TBSYS_LOG(ERROR, "column type size error |%s|", node_list[POS_COLUM_SIZE]);
              parse_ok = false;
            }
          }
        }

        if (parse_ok && type == ObCreateTimeType)
        {
          if (has_create_time_column)
          {
            TBSYS_LOG(ERROR, "more than one column have create time type");
            parse_ok = false;
          }
          else
          {
            has_create_time_column = true;
            schema.set_create_time_column(id);
          }
        }

        if (parse_ok && type == ObModifyTimeType)
        {
          if (has_modify_time_column)
          {
            TBSYS_LOG(ERROR, "more than one column have modify time type");
            parse_ok = false;
          }
          else
          {
            has_modify_time_column = true;
            schema.set_modify_time_column(id);
          }
        }

        if (parse_ok)
        {
          ObColumnSchemaV2 column;

          column.set_table_id(schema.get_table_id());
          column.set_column_id(id);
          column.set_column_name(node_list[POS_COLUM_NAME]);
          column.set_column_type(type);
          column.set_column_size(size);
          column.set_maintained(1 == maintained ? true : false);

          if (parse_ok && column_index < OB_MAX_COLUMN_NUMBER)
          {
            columns_info[column_index++] = column;
          }
          else
          {
            parse_ok = false;
          }
        }
        free(str);
        str = NULL;
      }

      if (parse_ok)
      {
        if (!has_create_time_column && has_create_time_id)
        {
          TBSYS_LOG(WARN, "schema file has no create_time column, and the default create_time column id is used");
          parse_ok = false;
        }
        if (!has_modify_time_column && has_modify_time_id)
        {
          TBSYS_LOG(WARN, "schema file has no modify_time column, and the default modify_time column id is used");
          parse_ok = false;
        }
      }
      //parse column group info
      vector<const char *> column_group_infos;
      char *group_str = NULL;
      uint32_t column_group_id = 0;

      if (parse_ok)
      {
        column_group_infos = config.getStringList(section_name, STR_COLUMN_GROUP_INFO);
        if (column_group_infos.size() > static_cast<uint64_t>(OB_MAX_COLUMN_GROUP_NUMBER))
        {
          TBSYS_LOG(ERROR,"we limit the column groups num less than %ld",OB_MAX_COLUMN_GROUP_NUMBER);
          parse_ok = false;
        }
      }


      if (parse_ok && !column_group_infos.empty() )
      {
        int64_t total_column_in_group = 0;

        //set column who are not in any group in schema to default group
        int8_t exist_in_column_group_info[OB_MAX_COLUMN_NUMBER];
        memset(exist_in_column_group_info, 0, OB_MAX_COLUMN_NUMBER);
        for(vector<const char*>::const_iterator it = column_group_infos.begin();
            it != column_group_infos.end() && parse_ok; ++it)
        {
          group_str = strdup(*it);
          vector<char*> column_list;
          group_str = str_trim(group_str);
          tbsys::CStringUtil::split(group_str, ",", column_list);
          if (column_list.size() < 2)
          {
            TBSYS_LOG(ERROR, "parse column group |%s| error", group_str);
            parse_ok = false;
          }

          if (parse_ok)
          {
            for(uint32_t i=1;i < column_list.size() && parse_ok; ++i)
            {
              ObColumnSchemaV2 *col  = find_column_info_tmp(columns_info,column_index,column_list[i]);
              if (NULL == col)
              {
                TBSYS_LOG(ERROR,"can't find column |%s|",column_list[i]);
                parse_ok = false;
              }
              else
              {
                exist_in_column_group_info[col - columns_info] = 1;
              }
            }
          }
          free(group_str);
          group_str = NULL;
        }

        for (int32_t index = 0; index < column_index; ++index)
        {
          if (0 == exist_in_column_group_info[index])
          {
            ObColumnSchemaV2* col = columns_info + index;
            col->set_column_group_id(OB_DEFAULT_COLUMN_GROUP_ID);
            if (add_column(*col) != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR,"can't add column |%s|",col->get_name());
              parse_ok = false;
            }
            else
            {
              ++total_column_in_group;
            }
          }
        }

        for(vector<const char*>::const_iterator it = column_group_infos.begin();
            it != column_group_infos.end() && parse_ok; ++it)
        {
          group_str = strdup(*it);
          vector<char*> column_list;
          group_str = str_trim(group_str);
          tbsys::CStringUtil::split(group_str, ",", column_list);
          if (column_list.size() < 2)
          {
            TBSYS_LOG(ERROR, "parse column group |%s| error", group_str);
            parse_ok = false;
          }

          if (parse_ok)
          {
            column_group_id = static_cast<uint32_t>(strtoul(column_list[POS_COLUMN_GROUP_ID],NULL,10));
          }

          if (parse_ok)
          {
            for(uint32_t i=1;i < column_list.size() && parse_ok; ++i)
            {
              ObColumnSchemaV2 *col  = find_column_info_tmp(columns_info,column_index,column_list[i]);
              if (NULL == col)
              {
                TBSYS_LOG(ERROR,"can't find column |%s|",column_list[i]);
                parse_ok = false;
              }
              else
              {
                col->set_column_group_id(column_group_id);
                //schema.add_column(col->get_id(),*col);
                if (add_column(*col) != OB_SUCCESS)
                {
                  TBSYS_LOG(ERROR,"can't add column |%s|",column_list[i]);
                  parse_ok = false;
                }
                else
                {
                  ++total_column_in_group;
                }
              }
            }
          }
          free(group_str);
          group_str = NULL;
        }

        if (parse_ok && total_column_in_group < column_index )
        {
          TBSYS_LOG(ERROR,"there is a column not belongs any column group");
          parse_ok = false;
        }
      }
      else if (parse_ok && column_group_infos.empty() )
      {
        for(int32_t i=0; i < column_index && parse_ok; ++i)
        {
          columns_info[i].set_column_group_id(OB_DEFAULT_COLUMN_GROUP_ID);
          if ( add_column(columns_info[i]) != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR,"add column |%s| failed",columns_info[i].get_name());
            parse_ok = false;
          }
        }
      }

      if (columns_info != NULL)
      {
        delete [] columns_info;
        columns_info = NULL;
      }

      return parse_ok;
    }

    bool ObSchemaManagerV2::parse_rowkey_column(const char* column_str, ObRowkeyColumn& column, ObTableSchema& schema)
    {
      // column str like
      // column_name(column_length%column_type)
      bool parse_ok = true;
      if (NULL != column_str)
      {
        char* str = strdup(column_str);
        str = str_trim(str);
        char* s = str;
        char* p = NULL;

        p = strchr(s, '(');
        if (NULL != p)
        {
          *p = '\0';
        }

        // no compatible rowkey descriptions, check if exist in table.
        const ObColumnSchemaV2* column_schema = get_column_schema(schema.get_table_name(), s);
        if (NULL == column_schema)
        {
          TBSYS_LOG(ERROR, "column (%s) not exist, cannot be part of rowkey.", s);
          parse_ok = false;
        }
        else
        {
          column.column_id_ = column_schema->get_id();
          column.length_ = column_schema->get_size();
          column.type_ = column_schema->get_type();
        }

        if (parse_ok && NULL != p)
        {
          // parse compatible rowkey descriptions. (8%int)
          s = p + 1;

          p = strchr(s, '%');
          if (NULL == p)
          {
            TBSYS_LOG(ERROR, "rowkey compatible format error:%s", s);
            parse_ok = false;
          }

          if (parse_ok)
          {
            *p = '\0';
            column.length_ = strtol(s, NULL, 10);

            s = p + 1;
            p = strchr(s, ')');
            if (NULL == p)
            {
              TBSYS_LOG(ERROR, "rowkey compatible format error:%s", s);
              parse_ok = false;
            }
          }

          if (parse_ok)
          {
            *p = '\0';
            column.type_  = ObColumnSchemaV2::convert_str_to_column_type(s);
          }

        }

        free(str);
      }

      return parse_ok;
    }

    bool ObSchemaManagerV2::parse_rowkey_info(const char* section_name, tbsys::CConfig&  config, ObTableSchema& schema)
    {
      bool parse_ok = true;

      //parse rowkey column
      const char* rowkey_column_str = NULL;
      char* clone_column_str = NULL;
      ObRowkeyInfo  rowkey_info;
      vector<char*> column_list;

      if (NULL != section_name)
      {
        rowkey_column_str = config.getString(section_name, STR_ROWKEY);
      }
      if (NULL != rowkey_column_str)
      {
        clone_column_str = strdup(rowkey_column_str);
        tbsys::CStringUtil::split(clone_column_str, ",", column_list);
        int size = static_cast<int>(column_list.size());
        for (int index = 0; parse_ok && index < size; ++index)
        {
          ObRowkeyColumn column;
          parse_ok = parse_rowkey_column(column_list[index], column, schema);
          if (parse_ok)
          {
            schema.get_rowkey_info().add_column(column);
          }
        }
        free(clone_column_str);
      }

      return parse_ok;
    }

    const ObColumnSchemaV2* ObSchemaManagerV2::column_begin() const
    {
      return columns_;
    }
    const ObColumnSchemaV2* ObSchemaManagerV2::column_end() const
    {
      return columns_ + column_nums_;
    }

    const char* ObSchemaManagerV2::get_app_name() const
    {
      return app_name_;
    }

    int ObSchemaManagerV2::set_app_name(const char* app_name)
    {
      int ret = OB_SUCCESS;

      if (NULL == app_name || app_name[0] == '\0')
      {
        TBSYS_LOG(WARN, "app name is NULL or empty");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        snprintf(app_name_, sizeof(app_name_), "%s", app_name);
      }

      return ret;
    }

    int64_t ObSchemaManagerV2::get_column_count() const
    {
      return column_nums_;
    }

    /*add wenghaixing [secondary index]20141105
     *将二级索引的信息放入hashmap当中
     */
    int ObSchemaManagerV2::add_index_in_map(ObTableSchema *tschema)
    {
      int ret = OB_SUCCESS;
      IndexList il;
      if(NULL == tschema)
      {
        TBSYS_LOG(WARN,"NULL pointer of tschema!");
        ret=OB_ERROR;
      }
      else if(OB_INVALID_ID != tschema->get_index_helper().tbl_tid)
      {
        uint64_t tid = tschema->get_index_helper().tbl_tid;
        if(hash::HASH_NOT_EXIST == id_index_map_.get(tid,il))
        {
          if(OB_SUCCESS != il.add_index(tschema->get_table_id()))
          {
            TBSYS_LOG(WARN,"add in hash map faile tid[%ld]!",tid);
            ret = OB_ERROR;
          }
          else
          {
            id_index_map_.set(tid,il,0);
          }
        }
        else if(hash::HASH_EXIST == id_index_map_.get(tid,il))
        {
          if(OB_SUCCESS != il.add_index(tschema->get_table_id()))
          {
            TBSYS_LOG(WARN,"index_list is full!");
            ret = OB_ERROR;
          }
          else
          {
            id_index_map_.set(tid,il,1);
          }
       }
            //add wenghaixing [secondary index ] 20141105 test log
           // TBSYS_LOG(ERROR,"test,whx::whx::tid[%ld]",tid);
            //index_list l2;
           // id_index_map_.get(tid,il);
            //uint64_t id2;
           // il.get_idx_id(0,id2);
           // TBSYS_LOG(ERROR,"test,whx::whx::il_count[%ld],il(0)[%ld]",il.get_count(),id2);
            //add e
     }

        return ret;
    }
    const hash::ObHashMap<uint64_t,IndexList,hash::NoPthreadDefendMode>*  ObSchemaManagerV2::get_index_hash() const
    {
      return &id_index_map_;
    }
    const int ObSchemaManagerV2::get_index_list(uint64_t table_id, IndexList &out) const
    {
      int ret = OB_SUCCESS;
      if(!index_hash_init_)
      {
        TBSYS_LOG(ERROR,"index hash is not inited!");
        ret = OB_ERROR;
      }
      else if(-1 == id_index_map_.get(table_id,out))
      {
        TBSYS_LOG(ERROR,"get index list for drop table err");
        ret = OB_ERROR;
      }
      return ret;
    }
    //add e
    //add wenghaixing [secondary index expire info]20141229
    bool ObSchemaManagerV2::is_modify_expire_condition(uint64_t table_id, uint64_t cid)const
    {
      bool ret = false;
      if(1 == table_id&&39 == cid)
      {
        ret = true;
      }
      return ret;
    }
    //add e
    //add wenghaixing [secondary index create index fix]20150203
    int ObSchemaManagerV2::get_index_column_num(uint64_t& table_id, int64_t& num) const
    {
      int ret = OB_SUCCESS;
      num = 0;
      const ObTableSchema* schema = get_table_schema(table_id);
      //IndexHelper index_helper;
      IndexList list;
      int64_t single_index_col_num = 0;
      if(NULL == schema)
      {
        TBSYS_LOG(WARN,"failed to get table schema,table[%ld]",table_id);
        ret = OB_ERR_SCHEMA_UNSET;
      }
      else if(OB_SUCCESS != (ret = get_index_list(table_id,list)))
      {
        TBSYS_LOG(WARN, "failed to get index list for cal col num");
        ret = OB_ERROR;
      }
      else
      {
        uint64_t index_tid = OB_INVALID_ID;
        for(int64_t i = 0;i < list.get_count();i++)
        {
          single_index_col_num = 0;
          //const ObTableSchema* index_schema = NULL;
          uint64_t max_index_column_id = OB_INVALID_ID;
          list.get_idx_id(i,index_tid);
          max_index_column_id = schema->get_max_column_id();
          for(uint64_t loop = OB_APP_MIN_COLUMN_ID;loop < max_index_column_id;loop++)
          {
            if(NULL != (get_column_schema(index_tid,loop)))
            {
              single_index_col_num ++;
            }
          }
          num += single_index_col_num;
        }
      }
      TBSYS_LOG(INFO,"table[%ld] has [%ld] index columns",table_id, num);
      return ret;
    }

    //add e
    //add wenghaixing [secondary index static_index_build]20150303
    int ObSchemaManagerV2::get_init_index(uint64_t *table_id, int64_t &size) const
    {
      int ret = OB_SUCCESS;
      int64_t i = 0;
      //uint64_t ref_tid = OB_INVALID_ID;
      IndexList list;
      uint64_t idx_tid = OB_INVALID_ID;
      const ObTableSchema* schema = NULL;
      hash::ObHashMap<uint64_t,IndexList,hash::NoPthreadDefendMode>::const_iterator iter = id_index_map_.begin();
      for( ; iter != id_index_map_.end(); iter++)
      {
        list = iter->second;
        for(int64_t loop = 0; loop < list.get_count(); loop ++)
        {
          list.get_idx_id(loop, idx_tid);
          if(NULL == (schema = get_table_schema(idx_tid)))
          {
            TBSYS_LOG(ERROR, "get index table id failed, ret[%d]", ret);
            ret = OB_SCHEMA_ERROR;
            break;
          }
          else if(INDEX_INIT == schema->get_index_helper().status)
          {
            table_id[i] = idx_tid;
            i++;
          }
        }
      }

      if(OB_SUCCESS == ret)
      {
        size = i;
      }
      return ret;
    }
    //add e
    //add wenghaixing [secondary index alter_table_debug]20150611
    bool ObSchemaManagerV2::is_index_has_storing(uint64_t table_id)const
    {
      bool ret = false;
      if(NULL != get_column_schema(table_id,OB_INDEX_VIRTUAL_COLUMN_ID))
      {
        ret = true;
      }
      return ret;
    }
    //add e
    uint64_t ObSchemaManagerV2::get_max_table_id() const
    {
      return max_table_id_;
    }

    int64_t ObSchemaManagerV2::get_table_count() const
    {
      return table_nums_;
    }

    ObSchemaManagerV2::Status ObSchemaManagerV2::get_status() const
    {
      Status status = INVALID;
      if (get_version() == CORE_SCHEMA_VERSION  && get_table_count() == CORE_TABLE_COUNT)
      {
        status = CORE_TABLES;
      }
      else if (get_version() > CORE_SCHEMA_VERSION && get_table_count() > CORE_TABLE_COUNT)
      {
        status = ALL_TABLES;
      }
      return status;
    }

    int64_t ObSchemaManagerV2::get_version() const
    {
      return timestamp_;
    }

    void ObSchemaManagerV2::set_version(const int64_t version)
    {
      timestamp_ = version;
    }

    void ObSchemaManagerV2::set_max_table_id(const uint64_t max_table_id)
    {
      max_table_id_ = max_table_id;
    }

    int32_t ObSchemaManagerV2::get_code_version() const
    {
      return version_;
    }

    const ObColumnSchemaV2* ObSchemaManagerV2::get_column_schema(const int32_t index) const
    {
      const ObColumnSchemaV2 *column = NULL;
      if (index >=0 && index < column_nums_)
      {
        column = &columns_[index];
      }
      return column;
    }

    const ObColumnSchemaV2* ObSchemaManagerV2::get_column_schema(const uint64_t table_id,
        const uint64_t column_group_id,
        const uint64_t column_id) const
    {
      const ObColumnSchemaV2 *column = NULL;

      if (OB_INVALID_ID != table_id && OB_INVALID_ID != column_group_id && OB_INVALID_ID != column_id)
      {
        ObColumnIdKey k;
        ObColumnInfo v;
        int err = OB_SUCCESS;

        k.table_id_ = table_id;
        k.column_id_ = column_id;

        err = id_hash_map_.get(k,v);
        if ( -1 == err || hash::HASH_NOT_EXIST == err)
        {
        }
        else
        {
          ObColumnSchemaV2 *tmp = v.head_;
          for(; tmp != NULL;tmp = tmp->column_group_next_)
          {
            if (tmp->get_column_group_id() == column_group_id)
            {
              column = tmp;
              break;
            }
          }
        }
      }
      return column;
    }

    const ObColumnSchemaV2* ObSchemaManagerV2::get_column_schema(const uint64_t table_id,
        const uint64_t column_id,
        int32_t* idx /*=NULL*/) const
    {
      const ObColumnSchemaV2 *column = NULL;

      if (OB_INVALID_ID != table_id && OB_INVALID_ID != column_id)
      {
        ObColumnIdKey k;
        ObColumnInfo v;
        int err = OB_SUCCESS;

        k.table_id_ = table_id;
        k.column_id_ = column_id;

        err = id_hash_map_.get(k,v);
        if ( -1 == err || hash::HASH_NOT_EXIST == err)
        {
        }
        else if (v.head_ != NULL)
        {
          column = v.head_;
          if (idx != NULL)
          {
            *idx = static_cast<int32_t>(column - column_begin() - v.table_begin_index_);
          }
        }
        else
        {
          TBSYS_LOG(ERROR,"found column but v.head_ is NULL");
        }
      }
      return column;
    }

    const ObColumnSchemaV2* ObSchemaManagerV2::get_column_schema(const char* table_name,
        const char* column_name,
        int32_t* idx /*=NULL*/) const
    {
      const ObColumnSchemaV2 *column = NULL;

      if (NULL != table_name && '\0' != *table_name && NULL != column_name && '\0' != column_name)
      {
        ObColumnNameKey k;
        k.table_name_.assign_ptr(const_cast<char *>(table_name),static_cast<int32_t>(strlen(table_name)));
        k.column_name_.assign_ptr(const_cast<char *>(column_name),static_cast<int32_t>(strlen(column_name)));
        column = get_column_schema(k.table_name_,k.column_name_,idx);
      }
      return column;
    }

    const ObColumnSchemaV2* ObSchemaManagerV2::get_column_schema(const ObString& table_name,
        const ObString& column_name,
        int32_t* idx /*=NULL*/) const
    {
      const ObColumnSchemaV2 *column = NULL;

      ObColumnNameKey k;
      k.table_name_ = table_name;
      k.column_name_ = column_name;
      int err = OB_SUCCESS;

      ObColumnInfo v;
      err = column_hash_map_.get(k,v);
      if (-1 == err|| hash::HASH_NOT_EXIST  == err)
      {
      }
      else if (v.head_  != NULL)
      {
        column = v.head_;
        //column = &columns_[v.index_[0]];
        if (idx != NULL)
        {
          *idx = static_cast<int32_t>(column - column_begin() - v.table_begin_index_);
          //*idx = v.index_[0] - v.table_begin_index_;
        }
      }
      else
      {
        TBSYS_LOG(ERROR,"found column but v.head_ is null");
      }
      return column;
    }

    const ObColumnSchemaV2* ObSchemaManagerV2::get_table_schema(const uint64_t table_id, int32_t& size) const
    {
      int err = OB_SUCCESS;
      const ObColumnSchemaV2 *begin = NULL;
      const ObColumnSchemaV2 *end = NULL;
      if (OB_INVALID_ID == table_id)
      {
        TBSYS_LOG(ERROR,"table id invalid [%lu]",table_id);
        err = OB_ERROR;
      }

      ObColumnSchemaV2 target;
      size = 0;

      if (OB_SUCCESS == err)
      {
        target.set_table_id(table_id);
        target.set_column_group_id(0);
        target.set_column_id(0);
        begin = std::lower_bound(column_begin(),column_end(),target,ObColumnSchemaV2Compare());
      }

      if  (OB_SUCCESS == err && begin != NULL && begin != column_end()
          && begin->get_table_id() == table_id)
      {
        target.set_column_group_id(OB_INVALID_ID);
        target.set_column_id(OB_INVALID_ID);
        end = std::upper_bound(begin,column_end(),target,ObColumnSchemaV2Compare());
        size = static_cast<int32_t>(end - begin);
      }
      return begin != column_end() ? begin : NULL;
    }

    const ObColumnSchemaV2* ObSchemaManagerV2::get_group_schema(const uint64_t table_id,
        const uint64_t column_group_id,int32_t& size) const
    {
      int err = OB_SUCCESS;

      const ObColumnSchemaV2 *group_begin = NULL;
      const ObColumnSchemaV2 *group_end = NULL;
      if (OB_INVALID_ID == column_group_id )
      {
        TBSYS_LOG(ERROR,"column group id invalid [%lu]",column_group_id);
        err = OB_ERROR;
      }

      ObColumnSchemaV2 target;
      size = 0;

      if (OB_SUCCESS == err)
      {
        target.set_table_id(table_id);
        target.set_column_group_id(column_group_id);
        target.set_column_id(0);
        // FIXME: the compare function using column_schema_compare, not
        // ObColumnSchemaV2Compare(), because the gcc-4.4.5 has bug can't
        // handle it, it reports "'<anonymous>' may be used uninitialized in this function"
        group_begin = std::lower_bound(column_begin(),column_end(),target,column_schema_compare);
      }

      if  (OB_SUCCESS == err && group_begin != NULL && group_begin != column_end()
          && group_begin->get_table_id() == table_id
          && group_begin->get_column_group_id() == column_group_id)
      {
        target.set_column_id(OB_INVALID_ID);
        group_end = std::upper_bound(group_begin,column_end(),target,ObColumnSchemaV2Compare());
        size = static_cast<int32_t>(group_end - group_begin);
      }
      return group_begin != column_end() ? group_begin : NULL;
    }

    int ObSchemaManagerV2::ensure_column_storage()
    {
      int ret = OB_SUCCESS;

      if (NULL == columns_ && 0 == column_capacity_)
      {
        ret = prepare_column_storage(DEFAULT_MAX_COLUMNS);
      }

      if (OB_SUCCESS == ret &&
          column_nums_ >= column_capacity_
          && NULL != columns_)
      {
        ret = double_expand_storage(columns_, column_nums_, MAX_COLUMNS_LIMIT,
            column_capacity_, ObModIds::OB_SCHEMA);
      }

      return ret;
    }

    int ObSchemaManagerV2::prepare_column_storage(const int64_t column_num, bool need_reserve_space)
    {
      int ret = OB_SUCCESS;
      if (column_num > MAX_COLUMNS_LIMIT)
      {
        ret = OB_SIZE_OVERFLOW;
      }
      else if (column_capacity_ < column_num)
      {
        // free old memory, and reallocate
        if (NULL != columns_)
        {
          ob_free(columns_, ObModIds::OB_SCHEMA);
        }

        // round up for reserved some extra space.
        if (need_reserve_space)
        {
          column_capacity_ = upper_align(column_num, DEFAULT_MAX_COLUMNS);
        }
        else
        {
          column_capacity_ = column_num;
        }

        if (NULL == (columns_ = reinterpret_cast<ObColumnSchemaV2*>(
                ob_malloc(column_capacity_ * sizeof(ObColumnSchemaV2), ObModIds::OB_SCHEMA))))
        {
          TBSYS_LOG(ERROR, "allocate memory for copy schema failed. %ld", column_num);
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          memset(columns_, 0x0, column_capacity_ * sizeof(ObColumnSchemaV2));
        }
      }
      return ret;
    }

    int ObSchemaManagerV2::add_column_without_sort(ObColumnSchemaV2& column)
    {
      int ret = OB_ERROR;
      const ObTableSchema *table = get_table_schema(column.get_table_id());
      if (NULL == table)
      {
        TBSYS_LOG(ERROR,"can't find this table:%lu",column.get_table_id());
        ret = OB_ENTRY_NOT_EXIST;
      }
      else if (column.get_id() < COLUMN_ID_RESERVED )
      {
        TBSYS_LOG(ERROR,"column id %ld reserved for internal usage.", column.get_id());
        ret = OB_INVALID_ARGUMENT;
      }
      //modify wenghaixing [secondary index alter_table_debug]20150611
      //else if (column.get_id() > table->get_max_column_id())//old code
      else if (column.get_id() > table->get_max_column_id() && column.get_id() != OB_INDEX_VIRTUAL_COLUMN_ID)
      // modify e
      {
        //mod jinty [Paxos Cluster.Balance] 20160708:b
        //TBSYS_LOG(ERROR,"column id %lu greater thean max_column_id %lu",
           //column.get_id(), table->get_max_column_id());
        TBSYS_LOG(ERROR,"column id %lu greater thean max_column_id %lu,table name=[%s]",
           column.get_id(), table->get_max_column_id(),table->get_table_name());
        //mod e
        ret = OB_INVALID_ARGUMENT;
      }
      else if (column_nums_ >= MAX_COLUMNS_LIMIT)
      {
        TBSYS_LOG(ERROR, "schema column_nums_ =%ld large than %ld",
            column_nums_, MAX_COLUMNS_LIMIT);
        ret = OB_SIZE_OVERFLOW;
      }
      else if (column_nums_ >= column_capacity_ && OB_SUCCESS != (ret = ensure_column_storage()))
      {
        TBSYS_LOG(ERROR, "expand error, ret= %d", ret);
      }
      else
      {
        columns_[column_nums_++] = column;
        ret = OB_SUCCESS;
      }
      return ret;
    }

    int ObSchemaManagerV2::add_column(ObColumnSchemaV2& column)
    {
      int ret = OB_ERROR;
      const ObTableSchema *table = get_table_schema(column.get_table_id());

      if (NULL == table)
      {
        TBSYS_LOG(ERROR,"can't find this table:%lu",column.get_table_id());
        ret = OB_ENTRY_NOT_EXIST;
      }
      else if (column.get_id() < COLUMN_ID_RESERVED )
      {
        TBSYS_LOG(ERROR,"column id %ld reserved for internal usage.", column.get_id());
        ret = OB_INVALID_ARGUMENT;
      }
      else if (column.get_id() > table->get_max_column_id())
      {
        TBSYS_LOG(ERROR,"column id %lu greater thean max_column_id %lu",
            column.get_id(), table->get_max_column_id());
        ret = OB_INVALID_ARGUMENT;
      }
      else if (column_nums_ >= MAX_COLUMNS_LIMIT)
      {
        TBSYS_LOG(ERROR, "schema column_nums_ =%ld large than %ld",
            column_nums_, MAX_COLUMNS_LIMIT);
        ret = OB_SIZE_OVERFLOW;
      }
      else if (column_nums_ >= column_capacity_ && OB_SUCCESS != (ret = ensure_column_storage()))
      {
        TBSYS_LOG(ERROR, "expand error, ret= %d", ret);
      }
      else
      {
        const ObColumnSchemaV2 *c = NULL;
        if ( (c = std::find(columns_,columns_ + column_nums_,column)) != columns_ + column_nums_)
        {
          TBSYS_LOG(ERROR,"column %s has already added",column.get_name());
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          columns_[column_nums_++] = column;
          std::sort(columns_, columns_+column_nums_, ObColumnSchemaV2Compare());
          ret = OB_SUCCESS;
        }
        //if (column_nums_ > 0 &&
        //    ( (column.get_table_id() < columns_[column_nums_ - 1].get_table_id()) ||
        //      (column.get_table_id() == columns_[column_nums_ - 1].get_table_id() &&
        //       column.get_column_group_id() < columns_[column_nums_- 1].get_column_group_id()) )
        //   )
        //{
        //  TBSYS_LOG(ERROR,"table id,column group id must in order,prev:(%lu,%lu),"
        //      "i:(%lu,%lu)", columns_[column_nums_- 1].get_table_id(),
        //      columns_[column_nums_ - 1].get_column_group_id(),
        //      column.get_table_id(),
        //      column.get_column_group_id());
        //}
        //else
        //{
        //  columns_[column_nums_++] = column;
        //  ret = OB_SUCCESS;
        //}
      }
      return ret;
    }

    int ObSchemaManagerV2::add_table(ObTableSchema& table)
    {
      int ret = OB_SUCCESS;
      if (table_nums_ >= OB_MAX_TABLE_NUMBER)
      {
        ret = OB_ERROR;
      }

      for(int32_t i=0; i < table_nums_; ++i)
      {
        //if ( (0 == strncmp(table_infos_[i].get_table_name(),table.get_table_name(),
        //            strlen(table.get_table_name()))) ||
        //     ( table.get_table_id() < table_infos_[i].get_table_id())  )


        //mod zhaoqiong [Schema Manager] 20150424:b
        //if (0 == strncmp(table_infos_[i].get_table_name(),table.get_table_name(),
        //      strlen(table.get_table_name())) ||
        //    table.get_table_id() == table_infos_[i].get_table_id())
        //mod:e
        if (0 == strncmp(table_infos_[i].get_table_name(),table.get_table_name(),
              max(strlen(table.get_table_name()),strlen(table_infos_[i].get_table_name()))) ||
            table.get_table_id() == table_infos_[i].get_table_id())
        {
          TBSYS_LOG(WARN,"can't add this table:[%ld,%s], table_infos[%d][table_id=%ld, table_name=%s]",
              table.get_table_id(), table.get_table_name(), i, table_infos_[i].get_table_id(), table_infos_[i].get_table_name());
          ret = OB_INVALID_ARGUMENT;
          break;
        }
      }

      if (OB_SUCCESS == ret)
      {
        table_infos_[table_nums_++] = table;
        if ((OB_INVALID_ID == max_table_id_) || (table.get_table_id() > max_table_id_))
          max_table_id_ = table.get_table_id();
      }
      return ret;
    }

    void ObSchemaManagerV2::del_column(const ObColumnSchemaV2& column)
    {
      ObColumnSchemaV2 *c = NULL;
      if ( (c = std::find(columns_,columns_ + column_nums_,column)) != columns_ + column_nums_)
      {
        --column_nums_;
        memmove(c,c+1,sizeof(ObColumnSchemaV2) * (column_nums_ - (c - columns_)));
        int ret = sort_column();
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "sort column failed:ret[%d]", ret);
        }
      }
      else
      {
        TBSYS_LOG(WARN,"column %s does not exist",column.get_name());
      }
    }

    void ObSchemaManagerV2::set_drop_column_group(bool drop_group /* = true*/)
    {
      drop_column_group_ = drop_group;
    }

    int ObSchemaManagerV2::get_column_index(const char *table_name,
        const char* column_name,
        int32_t index_array[],int32_t& size) const
    {
      int ret = OB_SUCCESS;

      if (NULL == table_name || NULL == column_name || '\0' == *table_name || '\0' == column_name || size <= 0)
      {
        TBSYS_LOG(ERROR,"invalid argument");
        ret = OB_ERROR;
      }
      else
      {
        ObColumnNameKey key;
        key.table_name_.assign_ptr(const_cast<char *>(table_name),static_cast<int32_t>(strlen(table_name)));
        key.column_name_.assign_ptr(const_cast<char *>(column_name),static_cast<int32_t>(strlen(column_name)));

        ObColumnInfo info;
        ret = column_hash_map_.get(key,info);
        if (-1 == ret || hash::HASH_NOT_EXIST  == ret)
        {
          TBSYS_LOG(WARN,"%s:%s not found [%d]",table_name,column_name,ret);
        }
        else
        {
          int32_t i=0;
          ObColumnSchemaV2 *tmp = info.head_;
          for(; i < size && tmp != NULL; ++i)
          {
            index_array[i] = static_cast<int32_t>(tmp - columns_);
            tmp = tmp->column_group_next_;
          }
          size = i;
          ret = OB_SUCCESS;
        }
      }
      return ret;
    }

    int ObSchemaManagerV2::get_column_index(const uint64_t table_id,
        const uint64_t column_id,
        int32_t index_array[],int32_t& size) const
    {
      int ret = OB_SUCCESS;

      if (OB_INVALID_ID == table_id || OB_INVALID_ID == column_id || size <= 0)
      {
        TBSYS_LOG(ERROR,"invalid argument");
      }
      else
      {
        ObColumnIdKey key;
        key.table_id_ = table_id;
        key.column_id_ = column_id;

        ObColumnInfo info;
        ret = id_hash_map_.get(key,info);
        if (-1 == ret || hash::HASH_NOT_EXIST  == ret)
        {
          TBSYS_LOG(WARN,"%lu:%lu not found [%d]",table_id,column_id,ret);
        }
        else
        {
          ObColumnSchemaV2 *tmp = info.head_;
          int32_t i=0;
          for(; i < size && tmp != NULL; ++i)
          {
            index_array[i] = static_cast<int32_t>(tmp - columns_);
            tmp = tmp->column_group_next_;
          }
          size = i;
          ret = OB_SUCCESS;
        }
      }
      return ret;
    }

    int ObSchemaManagerV2::get_column_schema(const uint64_t table_id, const uint64_t column_id,
        ObColumnSchemaV2* columns[],int32_t& size) const
    {
      int ret = OB_SUCCESS;

      if (OB_INVALID_ID == table_id || OB_INVALID_ID == column_id || size <= 0)
      {
        TBSYS_LOG(ERROR,"invalid argument");
      }
      else
      {
        ObColumnIdKey key;
        key.table_id_ = table_id;
        key.column_id_ = column_id;

        ObColumnInfo info;
        ret = id_hash_map_.get(key,info);
        if (-1 == ret || hash::HASH_NOT_EXIST  == ret)
        {
        }
        else
        {
          ObColumnSchemaV2 *tmp = info.head_;
          int32_t i=0;
          for(; i < size && tmp != NULL; ++i)
          {
            columns[i] = tmp;
            tmp = tmp->column_group_next_;
          }
          size = i;
          ret = OB_SUCCESS;
        }
      }
      return ret;

    }

    int ObSchemaManagerV2::get_column_schema(const char *table_name, const char* column_name,
        ObColumnSchemaV2* columns[],int32_t& size) const
    {
      int ret = OB_SUCCESS;

      if (NULL == table_name || NULL == column_name || '\0' == *table_name || '\0' == column_name || size <= 0)
      {
        TBSYS_LOG(ERROR,"invalid argument");
        ret = OB_ERROR;
      }
      else
      {
        ObColumnNameKey key;
        key.table_name_.assign_ptr(const_cast<char *>(table_name),static_cast<int32_t>(strlen(table_name)));
        key.column_name_.assign_ptr(const_cast<char *>(column_name),static_cast<int32_t>(strlen(column_name)));
        ret = get_column_schema(key.table_name_,key.column_name_,columns,size);
      }
      return ret;
    }

    int ObSchemaManagerV2::get_column_schema(const ObString& table_name,
        const ObString& column_name,
        ObColumnSchemaV2* columns[],int32_t& size) const
    {
      int ret = OB_SUCCESS;

      if (size <= 0)
      {
        TBSYS_LOG(ERROR,"invalid argument");
        ret = OB_ERROR;
      }
      else
      {
        ObColumnNameKey key;
        key.table_name_ = table_name;
        key.column_name_ = column_name;

        ObColumnInfo info;
        ret = column_hash_map_.get(key,info);
        if (-1 == ret || hash::HASH_NOT_EXIST  == ret)
        {
        }
        else
        {
          ObColumnSchemaV2 *tmp = info.head_;
          int32_t i=0;
          for(; i < size && tmp != NULL; ++i)
          {
            columns[i] = tmp;
            tmp = tmp->column_group_next_;
          }
          size = i;
          ret = OB_SUCCESS;
        }
      }
      return ret;
    }

    bool ObSchemaManagerV2::is_compatible(const ObSchemaManagerV2& schema_manager) const
    {
      bool compatible = true;

      /**
       * schema compatible check based on table id, column id, column
       * type and max column id, doesn't care the table name and
       * column name.
       */
      if (strncmp(app_name_, schema_manager.get_app_name(), OB_MAX_APP_NAME_LENGTH) != 0)
      {
        compatible = false;
      }
      for (int64_t i = 0; compatible && i < column_nums_; ++i)
      {
        const ObColumnSchemaV2 *column_left = get_column_schema(static_cast<int32_t>(i));
        if (NULL == column_left)
        {
          TBSYS_LOG(WARN, "should no be here. i=%ld", i);
          compatible = false;
          break;
        }
        uint64_t left_table_id = column_left->get_table_id();
        uint64_t left_column_id = column_left->get_id();
        const ObTableSchema *left_table_schema = get_table_schema(left_table_id);
        if (NULL == left_table_schema)
        {
          TBSYS_LOG(WARN, "should no be here, table_schema is NULL. table_id=%ld", left_table_id);
          compatible = false;
          break;
        }
        const ObColumnSchemaV2 *column_right = schema_manager.get_column_schema(left_table_id, left_column_id);
        if (NULL == column_right)
        {
          TBSYS_LOG(INFO, "column is delete. table_name=%s, table_id=%lu, "
              "column_name=%s, column_id=%lu",
              left_table_schema->get_table_name(), left_table_id,
              column_left->get_name(), column_left->get_id());
          continue;
        }
        uint64_t right_column_id = column_right->get_id();
        if (left_column_id != right_column_id)
        {
          TBSYS_LOG(WARN, "column id has been change. column_name=%s, old_column_id=%lu, new_column_id=%lu, be cautious!",
              left_table_schema->get_table_name(), left_column_id, right_column_id);
          compatible = false;
          break;
        }
        if (column_left->get_type() != column_right->get_type())
        {
          TBSYS_LOG(WARN, "column type has been change. column_name=%s, old_type=%d, new_type=%d",
              column_left->get_name(), column_left->get_type(), column_right->get_type());
          compatible = false;
          break;
        }
        if (column_left->get_type() == ObVarcharType)
        {
          if (column_left->get_size() > column_right->get_size())
          {
            TBSYS_LOG(WARN, "varchar column name = %s has change str length. old_length=%ld, new_lenght=%ld, should not less than old",
                column_left->get_name(), column_left->get_size(), column_right->get_size());
            compatible = false;
            break;
          }
        }
      }
      int64_t right_column_count = schema_manager.get_column_count();
      //判断右表新增的列。同样对column_id有要求 //repaired from messy code by zhuxh 20151014
      for (int64_t i = 0; i < right_column_count; i++)
      {
        const ObColumnSchemaV2 *right_column = schema_manager.get_column_schema((int32_t)i);
        if (NULL == right_column)
        {
          TBSYS_LOG(WARN, "right column is null. i=%ld", i);
          compatible = false;
          break;
        }
        const ObTableSchema *right_table_schema= schema_manager.get_table_schema(right_column->get_table_id());
        if (NULL == right_table_schema)
        {
          TBSYS_LOG(WARN, "right table schema is null. table_id=%ld", right_column->get_table_id());
          compatible = false;
          break;
        }
        const ObTableSchema *left_table_schema = get_table_schema(right_column->get_table_id());
        const ObColumnSchemaV2 *left_column = get_column_schema(right_column->get_table_id(), right_column->get_id());
        if (NULL == left_table_schema)
        {
          TBSYS_LOG(INFO, "new table add. table_name=%s, table_id=%lu",
              right_table_schema->get_table_name(), right_column->get_table_id());
          continue;
        }
        if (NULL == left_column)
        {
          TBSYS_LOG(INFO, "table add new column. table_name=%s, table_id=%lu, "
              "column_name=%s, column_id=%lu",
              right_table_schema->get_table_name(), right_column->get_table_id(),
              right_column->get_name(), right_column->get_id());
          if (right_column->get_id() <= left_table_schema->get_max_column_id())
          {
            TBSYS_LOG(WARN, "column id =%lu not legal. should bigger than %lu",
                right_column->get_id(), left_table_schema->get_max_column_id());
            compatible = false;
            break;
          }
        }
      }
      return compatible;
    }

    bool ObSchemaManagerV2::check_join_column(const int32_t column_index,
        const char* column_name, const char* join_column_name,
        ObTableSchema& schema, const ObTableSchema& join_table_schema, uint64_t& column_offset)
    {
      bool parse_ok = true;
      uint64_t join_column_id = 0;
      ObRowkeyColumn column;
      int64_t index = 0;

      const ObColumnSchemaV2* cs = get_column_schema(schema.get_table_name(), column_name);
      const ObColumnSchemaV2* jcs = get_column_schema(join_table_schema.get_table_name(), join_column_name);

      if (NULL == cs || NULL == jcs)
      {
        TBSYS_LOG(ERROR, "column(%s,%s) not a valid column.", column_name, join_column_name);
        parse_ok = false;
      }
      else if (cs->get_type() != jcs->get_type())
      {
        //the join should be happen between too columns have the same type
        TBSYS_LOG(ERROR, "join column have different types (%s,%d), (%s,%d) ",
            column_name, cs->get_type(), join_column_name, jcs->get_type());
        parse_ok = false;
      }
      else if (OB_SUCCESS != join_table_schema.get_rowkey_info().get_column_id(column_index, join_column_id))
      {
        TBSYS_LOG(ERROR, "join table (%s) has not rowkey column on index(%d)",
            join_table_schema.get_table_name(), column_index);
        parse_ok = false;
      }
      else if (join_column_id != jcs->get_id())
      {
        TBSYS_LOG(ERROR, "join column(%s,%ld) not match join table rowkey column(%ld)",
            join_table_schema.get_table_name(), jcs->get_id(), join_column_id);
        parse_ok = false;
      }
      else if (OB_SUCCESS != schema.get_rowkey_info().get_index(cs->get_id(), index, column))
      {
        TBSYS_LOG(ERROR, "left column (%s,%ld) not a rowkey column of left table(%s)",
            column_name, cs->get_id(), schema.get_table_name());
        parse_ok = false;
      }
      else
      {
        column_offset = index;
      }

      return parse_ok;
    }

    bool ObSchemaManagerV2::parse_join_info(const char* section_name, tbsys::CConfig& config, ObTableSchema& schema)
    {
      uint64_t left_column_offset_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
      uint64_t left_column_count = 0;
      uint64_t table_id_joined = OB_INVALID_ID;
      const ObTableSchema *table_joined = NULL;

      memset(left_column_offset_array, 0, sizeof(left_column_offset_array));

      bool parse_ok = true;
      char *str = NULL;
      vector<const char*> join_info_strs;
      vector<char*> node_list;
      if (section_name != NULL)
      {
        join_info_strs = config.getStringList(section_name, STR_JOIN_RELATION);
      }
      if (!join_info_strs.empty())
      {
        char *s = NULL;
        int len = 0;
        char *p = NULL;
        for(vector<const char*>::iterator it = join_info_strs.begin();
            parse_ok && it != join_info_strs.end(); ++it)
        {
          str = strdup(*it);
          s = str;
          len = static_cast<int32_t>(strlen(s));

          // str like [r1$jr1,r2$jr2]%joined_table_name:f1$jf1,f2$jf2,...
          if (*s != '[')
          {
            TBSYS_LOG(ERROR, "join info (%s) incorrect, first character must be [", str);
            parse_ok = false;
          }
          else
          {
            ++s;
          }

          if (parse_ok)
          {
            // find another bracket
            p = strchr(s, ']');
            if (NULL == p)
            {
              TBSYS_LOG(ERROR, "join info (%s) incorrect, cannot found ]", str);
              parse_ok = false;
            }
            else
            {
              // s now be the join rowkey columns array.
              *p = '\0';
            }
          }

          if (parse_ok)
          {
            node_list.clear();
            s = str_trim(s);
            tbsys::CStringUtil::split(s, ",", node_list);
            if (node_list.empty())
            {
              TBSYS_LOG(ERROR, "join info (%s) incorrect, left join columns not exist.", str);
              parse_ok = false;
            }
            else
            {
              // skip join rowkey columns string, now s -> %joined_table_name:f1$jf1...
              s = p + 1;
            }
          }

          if (parse_ok && *s != '%')
          {
            TBSYS_LOG(ERROR, "%s format error, should be rowkey", str);
            parse_ok = false;
          }

          if (parse_ok)
          {
            // skip '%', find join table name.
            s++;
            p = strchr(s, ':');
            if (NULL == p)
            {
              TBSYS_LOG(ERROR, "%s format error, could not find ':'", str);
              parse_ok = false;
            }
            else
            {
              // now s is the joined table name.
              *p = '\0';
            }
          }

          if (parse_ok)
          {
            table_joined = get_table_schema(s);
            if (NULL != table_joined)
            {
              table_id_joined = table_joined->get_table_id();
            }

            if (NULL == table_joined || table_id_joined == OB_INVALID_ID)
            {
              TBSYS_LOG(ERROR, "%s table not exist ", s);
              parse_ok = false;
            }
          }

          // parse join rowkey columns.
          if (parse_ok)
          {
            char* cp = NULL;
            uint64_t column_offset = 0;
            for(uint32_t i = 0; parse_ok && i < node_list.size(); ++i)
            {
              cp = strchr(node_list[i], '$');
              if (NULL == cp)
              {
                TBSYS_LOG(ERROR, "error can not find '$' (%s) ", node_list[i]);
                parse_ok = false;
                break;
              }
              else
              {
                *cp = '\0';
                ++cp;
                // now node_list[i] is left column, cp is join table rowkey column;
                parse_ok = check_join_column(i, node_list[i], cp, schema, *table_joined, column_offset);
                if (parse_ok)
                {
                  left_column_offset_array[i] = column_offset;
                }
              }
            }

            if (parse_ok)
            {
              left_column_count = node_list.size();
            }
          }


          // parse join columns
          if (parse_ok)
          {
            s = p + 1;
            s = str_trim(s);
            node_list.clear();
            tbsys::CStringUtil::split(s, ",", node_list);
            if (node_list.empty())
            {
              TBSYS_LOG(ERROR, "%s can not find correct info", str);
              parse_ok = false;
            }
          }

          uint64_t ltable_id = OB_INVALID_ID;

          if (parse_ok)
          {
            ltable_id = schema.get_table_id();
            uint64_t lid = OB_INVALID_ID;
            uint64_t rid = OB_INVALID_ID;
            char *fp = NULL;
            for(uint32_t i = 0; parse_ok && i < node_list.size(); ++i)
            {
              lid = OB_INVALID_ID;
              rid = OB_INVALID_ID;
              fp = strchr(node_list[i], '$');
              if (NULL == fp)
              {
                TBSYS_LOG(ERROR, "error can not find '$' %s ", node_list[i]);
                parse_ok = false;
                break;
              }
              *fp = '\0';
              fp++;


              int32_t l_column_index[OB_MAX_COLUMN_GROUP_NUMBER];
              int32_t r_column_index[OB_MAX_COLUMN_GROUP_NUMBER];

              int32_t l_column_size = sizeof(l_column_index) / sizeof(l_column_index[0]);
              int32_t r_column_size = sizeof(r_column_index) / sizeof(r_column_index[0]);

              get_column_index(schema.get_table_name(), node_list[i], l_column_index, l_column_size);
              get_column_index(table_joined->get_table_name(), fp, r_column_index, r_column_size);

              if (l_column_size <= 0 || r_column_size <= 0 )
              {
                TBSYS_LOG(ERROR, "error column name %s %s ", node_list[i], fp);
                parse_ok = false;
                break;
              }

              for(int32_t l_index = 0; l_index < l_column_size && parse_ok; ++l_index)
              {
                //TODO check column
                ObColumnSchemaV2* lcs = &columns_[ l_column_index[l_index] ];
                ObColumnSchemaV2* rcs = &columns_[ r_column_index[0] ];  //just need the id of the right column

                if (lcs->get_type() != rcs->get_type())
                {
                  //the join should be happen between too columns have the same type
                  //date_and_time_todo  [DATE_TIME]
                  if (lcs->get_type() == ObPreciseDateTimeType &&
                      (rcs->get_type() == ObCreateTimeType || rcs->get_type() == ObModifyTimeType))
                  {
                    //except that ObPreciseDateTimeType join with ObCreateTimeType or ObModifyTimeType
                  }
                  else
                  {
                    TBSYS_LOG(ERROR, "join column have different types %s %s ", node_list[i], p);
                    parse_ok = false;
                    break;
                  }
                }
                if (lcs->get_type() == ObCreateTimeType || lcs->get_type() == ObModifyTimeType)
                {
                  TBSYS_LOG(ERROR, "column %s can not be jonined, it hase type ObCreateTimeType or ObModifyTimeType", node_list[i]);
                  parse_ok = false;
                  break;
                }
                lcs->set_join_info(table_id_joined,left_column_offset_array,left_column_count,rcs->get_id());
              }
            }
          }
          free(str);
          str = NULL;
        }
      }
      if (str) free(str);
      return parse_ok;
    }

    void ObSchemaManagerV2::print_info() const
    {
      for(int64_t i=0; i<column_nums_; ++i)
      {
        columns_[i].print_info();
      }
    }
    //add zhaoqiong [Schema Manager] 20150327:b
    void ObSchemaManagerV2::print_debug_info() const
    {
      for(int64_t i=0; i<column_nums_; ++i)
      {
        columns_[i].print_debug_info();
      }
    }
    //add:e
    const char* convert_column_type_to_str(ColumnType type)
    {
      if (type == ObIntType)
      {
        return STR_COLUMN_TYPE_INT;
      }
      //add lijianqiang [INT_32] 20151008:b
      else if (type == ObInt32Type)
      {
        return STR_COLUMN_TYPE_INT32;
      }
      //add 20151008:e
      else if (type == ObVarcharType)
      {
        return STR_COLUMN_TYPE_VCHAR;
      }
      else if (type == ObDateTimeType)
      {
        return STR_COLUMN_TYPE_DATETIME;
      }
      else if (type == ObPreciseDateTimeType)
      {
        return STR_COLUMN_TYPE_PRECISE_DATETIME;
      }
      //add peiouya [DATE_TIME] 20150906:b
      else if (type == ObDateType)
      {
        return STR_COLUMN_TYPE_DATE;
      }
      else if (type == ObTimeType)
      {
        return STR_COLUMN_TYPE_TIME;
      }
      //add 20150906:e
      else if (type == ObCreateTimeType)
      {
        return STR_COLUMN_TYPE_C_TIME;
      }
      else if (type == ObModifyTimeType)
      {
        return STR_COLUMN_TYPE_M_TIME;
      }
      //add wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_14:b
      else if (type==ObDecimalType)
      {
        return STR_COLUMN_TYPE_DECIMAL;
      }
      //add e
      else
      {
        TBSYS_LOG(ERROR,"column type %d not be supported", type);
        return NULL;
      }
    }


    void ObSchemaManagerV2::print(FILE* fd) const
    {
      fprintf(fd, "version: %ld\n", timestamp_);
      fprintf(fd, "max_table_id: %ld\n", max_table_id_);
      fprintf(fd, "app_name: %s\n", app_name_);
      fprintf(fd, "total_tables: %ld\n", table_nums_);
      fprintf(fd, "tatal_columns: %ld\n", column_nums_);
      const ObTableSchema* it = table_begin();
      for (; it != table_end(); ++it)
      {
        fprintf(fd, "----------------------------------------------------------------\n");
        it->print(fd);
        int32_t col_num = 0;
        const ObColumnSchemaV2* col = ObSchemaManagerV2::get_table_schema(it->get_table_id(), col_num);
        for (int32_t i = 0; i < col_num; ++i, ++col)
        {
          //fprintf(fd, "column=%s id=%ld type=%s\n", col->get_name(), col->get_id(), strtype(col->get_type()));
          col->print(fd);
        }
      }
    }

    //add wenghaixing [secondary index] 20141105
    int ObSchemaManagerV2::init_index_hash()
    {
      int ret = OB_SUCCESS;
      if(!index_hash_init_)
      {
        if(OB_SUCCESS != (ret = id_index_map_.create(hash::cal_next_prime(OB_MAX_TABLE_NUMBER))))
        {
          TBSYS_LOG(ERROR,"create id_index_map_ failed!");
          return ret;//do not return like this in other code_block!
        }
        else
        {
          TBSYS_LOG(DEBUG,"create id_index_map_ success!");
        }
      }
      else
      {
        id_index_map_.clear();
      }
      if(OB_SUCCESS == ret)
      {
        for(int64_t i = 0;i<table_nums_;i++)
        {
          //add wenghaixing [secondary index] 20141105
          if(OB_SUCCESS != (ret = add_index_in_map(table_infos_+i)))
          {
            TBSYS_LOG(WARN, "failed to add index[%ld], err=%d",table_infos_[i].get_table_id() ,ret);
          }
          //add e
        }
      }
      if(OB_SUCCESS == ret)
      {
        index_hash_init_=true;
      }
      return ret;
    }
    //add e
    //add wenghaixing [secondary index upd] 20141126
    int ObSchemaManagerV2::column_hint_index(uint64_t table_id, uint64_t cid, IndexList &out)const
    {
      int ret = OB_SUCCESS;
      IndexList il;
      if(hash::HASH_NOT_EXIST == id_index_map_.get(table_id,il))
      {
      }
      else
      {
        for(int64_t i = 0;i<il.get_count();i++)
        {
          if(OB_SUCCESS != ret)
          {
            break;
          }
          else
          {
            uint64_t tid = OB_INVALID_ID;
            const ObColumnSchemaV2* index_column_schema = NULL;
            const ObTableSchema *main_table_schema = NULL;
            il.get_idx_id(i,tid);
            index_column_schema=get_column_schema(tid,cid);
            main_table_schema = get_table_schema(tid);
            if(NULL == index_column_schema || NULL == main_table_schema)
            {
              TBSYS_LOG(DEBUG,"index_column_schema is NULL.tid[%ld],cid[%ld]",tid,cid);
            }
            else if(ERROR == main_table_schema->get_index_helper().status)
            {
              continue;
            }
            else if(OB_SUCCESS != (ret = out.add_index(tid)))
            {
              TBSYS_LOG(ERROR,"index_list is full,and tid[%ld] index cannot add in",tid);
            }
          }

         }
       }
       return ret;
    }
    //add e
     //add wenghaixing [secondary index col checksum] 20141210
    int ObSchemaManagerV2::column_hint_index(uint64_t table_id, uint64_t cid, bool &hint)const
    {
      int ret = OB_SUCCESS;
      IndexList il;
      hint = false;
      if(hash::HASH_NOT_EXIST==id_index_map_.get(table_id,il))
      {
      }
      else
      {
        for(int64_t i = 0;i<il.get_count();i++)
        {
          if(OB_SUCCESS != ret)
          {
            break;
          }
          else
          {
            uint64_t tid = OB_INVALID_ID;
            const ObColumnSchemaV2* index_column_schema = NULL;
            il.get_idx_id(i,tid);
            index_column_schema = get_column_schema(tid,cid);
            if(NULL == index_column_schema)
            {
              TBSYS_LOG(DEBUG,"index_column_schema is NULL.tid[%ld],cid[%ld]",tid,cid);
            }
            else
            {
              hint=true;
            }
          }
        }
      }
      return ret;
    }
    //add e
    //add wenghaixing [secondary index drop table_with_index]20141222
    int ObSchemaManagerV2::get_index_list_for_drop(uint64_t table_id, IndexList &out)const
    {
      int ret = OB_SUCCESS;
      if(!index_hash_init_)
      {
        TBSYS_LOG(ERROR,"index hash is not inited!");
        ret = OB_ERROR;
      }
      else if(-1 == id_index_map_.get(table_id,out))
      {
        TBSYS_LOG(ERROR,"get index list for drop table err");
        ret = OB_ERROR;
      }
      return ret;
    }

    //add e
    int ObSchemaManagerV2::sort_column()
    {
      int ret = OB_SUCCESS;
      //add zhaoqiong [Schema Manager] 20150327:b
      std::sort(table_infos_,table_infos_+table_nums_,ObTableSchemaCompare());
      //add:e
      std::sort(columns_, columns_+column_nums_, ObColumnSchemaV2Compare());
      if (!hash_sorted_)
      {
        TBSYS_LOG(DEBUG,"create hash table");
        if ((ret = column_hash_map_.create(hash::cal_next_prime(512))) != OB_SUCCESS )
        {
          TBSYS_LOG(ERROR,"create column_hash_map_ failed");
        }
        else if ((ret = id_hash_map_.create(hash::cal_next_prime(512))) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"create id_hash_map_ failed");
          column_hash_map_.destroy();
        }
      }
      else
      {
        TBSYS_LOG(DEBUG,"rebuild hash table,clear first");
        column_hash_map_.clear();
        id_hash_map_.clear();
      }

      column_group_nums_ = 0;

      if (OB_SUCCESS == ret && drop_column_group_)
      {
        TBSYS_LOG(INFO,"drop all column group info");
        for(int64_t i=0; i < column_nums_; ++i)
        {
          columns_[i].set_column_group_id(0); //drop all column group
        }

        TBSYS_LOG(DEBUG,"old column nums:%ld",column_nums_);
        //get rid of column that belongs to more one column group
        sort(columns_, columns_ + column_nums_,ObColumnSchemaV2Compare());
        ObColumnSchemaV2 *new_end = unique(columns_,columns_ + column_nums_);
        column_nums_ = new_end - column_begin();
        TBSYS_LOG(DEBUG,"new column nums:%ld",column_nums_);
      }

      if (OB_SUCCESS == ret)
      {
        ObColumnNameKey name_key;
        ObColumnIdKey   id_key;
        ObColumnInfo info;
        ObTableSchema* table_schema;

        int err = OB_SUCCESS;
        int32_t table_begin_offset = 0;
        const char *table_name = NULL;

        for(int i = 0; i < column_nums_ && OB_SUCCESS == ret; ++i)
        {
          columns_[i].column_group_next_ = NULL; //rebuild the list

          table_schema = get_table_schema(columns_[i].get_table_id());
          if (NULL == table_schema)
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "get_table_schema(id=%ld)=>NULL, this should not happen", columns_[i].get_table_id());
            break;
          }
          table_name = table_schema->get_table_name();

          name_key.table_name_.assign_ptr(const_cast<char *>(table_name),static_cast<int32_t>(strlen(table_name)));
          name_key.column_name_.assign_ptr(const_cast<char *>(columns_[i].get_name()), static_cast<int32_t>(strlen(columns_[i].get_name() )));

          id_key.table_id_ = columns_[i].get_table_id();
          id_key.column_id_ = columns_[i].get_id();

          info.head_ = &columns_[i];

          if (i > 0 && columns_[i].get_table_id() != columns_[i - 1].get_table_id())
          {
            table_begin_offset = i;
          }

          info.table_begin_index_ = table_begin_offset;

          ret = column_hash_map_.set(name_key,info);
          err = id_hash_map_.set(id_key,info);

          if ( -1 == ret || -1 == err)
          {
            TBSYS_LOG(ERROR,"insert column into hash set failed");
            ret = OB_ERROR;
          }
          else if (hash::HASH_EXIST == ret && hash::HASH_EXIST == err)
          {
            ret = column_hash_map_.get(name_key,info);
            err = id_hash_map_.get(id_key,info);
            if (-1 == ret || -1 == err)
            {
              TBSYS_LOG(ERROR,"get (%s,%s) failed",name_key.table_name_.ptr(),name_key.column_name_.ptr());
              ret = OB_ERROR;
            }
            else
            {
              ObColumnSchemaV2* tmp = info.head_;

              while(tmp->column_group_next_ != NULL)
              {
                tmp = tmp->column_group_next_;
              }
              tmp->column_group_next_ = &columns_[i];

              ret = column_hash_map_.set(name_key,info,1); //overwrite
              err = id_hash_map_.set(id_key,info,1);
            }
          }
          else if (ret != err)
          {
            TBSYS_LOG(ERROR,"name->id & id->name no match: [ret:%d,err:%d]",ret,err);
            ret = OB_ERROR;
          }
          else
          {
            TBSYS_LOG(DEBUG,"insert (%s,%s),(%lu,%lu) into hash succ",name_key.table_name_.ptr(),
                name_key.column_name_.ptr(), id_key.table_id_,id_key.column_id_);
            //succ
          }

          if (-1 == ret || -1 == err)
          {
            TBSYS_LOG(ERROR,"error happend in sort column ");
            ret = OB_ERROR;
          }
          else
          {
            ret = OB_SUCCESS;
          }

          if (column_group_nums_ > 0)
          {
            if (columns_[i].get_table_id() > column_groups_[column_group_nums_ - 1].table_id_ ||
                columns_[i].get_column_group_id() > column_groups_[column_group_nums_ - 1].column_group_id_)
            {
              column_groups_[column_group_nums_].table_id_ = columns_[i].get_table_id();
              column_groups_[column_group_nums_].column_group_id_ = columns_[i].get_column_group_id();
              ++column_group_nums_;
            }
          }
          else
          {
            column_groups_[column_group_nums_].table_id_ = columns_[i].get_table_id();
            column_groups_[column_group_nums_].column_group_id_ = columns_[i].get_column_group_id();
            ++column_group_nums_;
          }


          if (columns_[i].get_type() == ObCreateTimeType ||columns_[i].get_type() == ObModifyTimeType)
          {
            ObTableSchema *table = get_table_schema(columns_[i].get_table_id());
            if (NULL != table)
            {
              if (columns_[i].get_type() == ObCreateTimeType)
                table->set_create_time_column(columns_[i].get_id());
              else
                table->set_modify_time_column(columns_[i].get_id());
            }
          }
        } //end for
      }

      if (OB_SUCCESS == ret)
      {
        hash_sorted_ = true;
      }
      else
      {
        TBSYS_LOG(WARN,"sort column failed");
      }

      return ret;
    }

    int ObSchemaManagerV2::replace_system_variable(
        char* expire_condition, const int64_t buf_size) const
    {
      int ret = OB_SUCCESS;
      struct tm* tm = NULL;
      time_t sys_date = static_cast<time_t>(tbsys::CTimeUtil::getTime() / 1000 / 1000);
      char replace_str_buf[32];

      if (NULL == expire_condition || buf_size <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, expire_condition=%p, buf_size=%ld",
            expire_condition, buf_size);
        ret = OB_ERROR;
      }
      else
      {
        tm = localtime(&sys_date);
        if (NULL == tm)
        {
          TBSYS_LOG(WARN, "failed to transfer system date to tm, sys_date=%ld",
              sys_date);
          ret = OB_ERROR;
        }
        else
        {
          /*
          strftime(replace_str_buf, sizeof(replace_str_buf),
              "#%Y-%m-%d %H:%M:%S#", tm);
          ret = replace_str(expire_condition, buf_size, SYS_DATE, replace_str_buf);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to replace $SYS_DATE in expire condition");
          }
          */
          //modify peiouya [Expire_condition_modify] 20140909:b
          if ((NULL != strstr(expire_condition,SYS_DATE)) && (NULL != strstr(expire_condition,SYS_DAY)))
          {
            ret = OB_ERROR;
            TBSYS_LOG(WARN, "$SYS_DATE and $SYS_DAY, the two can only and must choose one!");
          }
          else if (NULL != strstr(expire_condition,SYS_DATE))
          {
            strftime(replace_str_buf, sizeof(replace_str_buf),
              "#%Y-%m-%d %H:%M:%S#", tm);
            ret = replace_str(expire_condition, buf_size, SYS_DATE, replace_str_buf);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to replace $SYS_DATE in expire condition");
            }
          }
          else
          {
            strftime(replace_str_buf, sizeof(replace_str_buf),
              "#%Y-%m-%d#", tm);
            ret = replace_str(expire_condition, buf_size, SYS_DAY, replace_str_buf);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to replace $SYS_DAY in expire condition");
            }
          }
            //modify 20140909:b
        }
      }

      return ret;
    }

    int ObSchemaManagerV2::check_expire_dependent_columns(
        const ObString& expr, const ObTableSchema& table_schema,
        ObExpressionParser& parser) const
    {
      int ret               = OB_SUCCESS;
      int i                 = 0;
      int64_t type          = 0;
      int64_t postfix_size  = 0;
      ObString key_col_name;
      ObArrayHelper<ObObj> expr_array;
      ObObj post_expr[OB_MAX_COMPOSITE_SYMBOL_COUNT];
      const ObColumnSchemaV2* column_schema = NULL;
      ObString table_name;

      expr_array.init(OB_MAX_COMPOSITE_SYMBOL_COUNT, post_expr);
      //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
      //ret = parser.parse(expr, expr_array);
      ret = parser.parse(expr, expr_array, true);
      //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
      if (OB_SUCCESS == ret)
      {
        postfix_size = expr_array.get_array_index();
      }
      else
      {
        TBSYS_LOG(WARN, "parse infix expression to postfix expression "
            "error, ret=%d", ret);
      }

      if (OB_SUCCESS == ret)
      {
        i = 0;
        while(i < postfix_size - 1)
        {
          if (OB_SUCCESS != expr_array.at(i)->get_int(type))
          {
            TBSYS_LOG(WARN, "unexpected data type. int expected, but actual type is %d",
                expr_array.at(i)->get_type());
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          else
          {
            if (ObExpression::COLUMN_IDX == type)
            {
              if (OB_SUCCESS != expr_array.at(i+1)->get_varchar(key_col_name))
              {
                TBSYS_LOG(WARN, "unexpected data type. varchar expected, "
                    "but actual type is %d",
                    expr_array.at(i+1)->get_type());
                ret = OB_ERR_UNEXPECTED;
                break;
              }
              else
              {
                table_name.assign_ptr(const_cast<char*>(table_schema.get_table_name()),
                    static_cast<int32_t>(strlen(table_schema.get_table_name())));
                column_schema = get_column_schema(table_name, key_col_name);
                if (NULL == column_schema)
                {
                  TBSYS_LOG(ERROR, "expire condition includes invalid column name, "
                      "table_name=%s, column_name=%.*s, expire_condition=%.*s",
                      table_schema.get_table_name(), key_col_name.length(), key_col_name.ptr(),
                      expr.length(), expr.ptr());
                  ret = OB_ERROR;
                  break;
                }
              }
            }/* only column name needs to decode. other type is ignored */
            i += 2; // skip <type, data> (2 objects as an element)
          }
        }
      }

      return ret;
    }

    //add liumz, [for delete_index/replace_index] 20150129:b
    bool ObSchemaManagerV2::is_have_modifiable_index(uint64_t table_id) const
    {
      bool ret = false;
      IndexList idx_list;
      uint64_t tmp_tid = OB_INVALID_ID;
      if(index_hash_init_)
      {
        if(hash::HASH_EXIST == id_index_map_.get(table_id, idx_list))
        {
          for(int64_t i=0; i<idx_list.get_count(); i++)
          {
            const ObTableSchema *main_table_schema = NULL;
            idx_list.get_idx_id(i, tmp_tid);
            if (NULL == (main_table_schema = get_table_schema(tmp_tid)))
            {
              TBSYS_LOG(WARN,"fail to get table schema for table[%ld]", tmp_tid);
            }
            else
            {
              IndexStatus status = main_table_schema->get_index_helper().status;
              if(AVALIBALE == status || WRITE_ONLY == status || INDEX_INIT == status || NOT_AVALIBALE == status)
              {
                ret=true;
                break;
              }
            }
          }
        }
      }
      return ret;
    }
    int ObSchemaManagerV2::get_all_modifiable_index_tid(uint64_t main_tid, uint64_t *tid_array, uint64_t &count) const   //获得tid为main_tid的表的所有的可用的索引表，存到数组tid_array里面
    {
      int ret = OB_SUCCESS;
      IndexList idx_list;
      uint64_t tmp_tid = OB_INVALID_ID;
      if(index_hash_init_)
      {
        if(hash::HASH_EXIST == id_index_map_.get(main_tid, idx_list))
        {
          for(int64_t i=0; i<idx_list.get_count(); i++)
          {
            const ObTableSchema *main_table_schema = NULL;
            idx_list.get_idx_id(i, tmp_tid);
            if (NULL == (main_table_schema = get_table_schema(tmp_tid)))
            {
              ret = OB_ERR_ILLEGAL_ID;
              TBSYS_LOG(WARN,"fail to get table schema for table[%ld]", main_tid);
            }
            else
            {
              IndexStatus status = main_table_schema->get_index_helper().status;
              if(status == AVALIBALE || status == WRITE_ONLY || INDEX_INIT == status || NOT_AVALIBALE == status)
              {
                tid_array[count++]=tmp_tid;
              }
            }
          }
        }
      }
      else
      {
        ret = OB_NOT_INIT;
      }
      return ret;
    }
    //add:e

    //add liuxiao [secondary index col checksum]20150527
    int ObSchemaManagerV2::get_all_avaiable_index_list(ObArray<uint64_t> &index_id_list) const
    //获得tid为main_tid的表的所有的可用的索引表id，存到数组tid_array里面
    {
      int ret = OB_SUCCESS;
      uint64_t idx_id = OB_INVALID_ID;
      const ObTableSchema *main_table_schema = NULL;
      hash::ObHashMap<uint64_t,IndexList,hash::NoPthreadDefendMode>::const_iterator iter = id_index_map_.begin();
      for (;iter != id_index_map_.end(); ++iter)
      {
        IndexList tmp_list=iter->second;
        for(int64_t i = 0; i< tmp_list.get_count(); i++)
        {
          tmp_list.get_idx_id(i,idx_id);
          if (NULL == (main_table_schema = get_table_schema(idx_id)))
          {
            ret = OB_ERR_ILLEGAL_ID;
            TBSYS_LOG(WARN,"fail to get table schema for table[%ld]", idx_id);
          }
          else
          {
            if(main_table_schema->get_index_helper().status == AVALIBALE)
            {
              index_id_list.push_back(idx_id);
            }
          }
        }
      }
      return ret;
    }
    int ObSchemaManagerV2::get_all_init_index_tid(ObArray<uint64_t> &index_id_list) const
    {
      int ret = OB_SUCCESS;
      uint64_t idx_id = OB_INVALID_ID;
      const ObTableSchema *index_table_schema = NULL;
      hash::ObHashMap<uint64_t,IndexList,hash::NoPthreadDefendMode>::const_iterator iter = id_index_map_.begin();
      for (;iter != id_index_map_.end(); ++iter)
      {
        IndexList tmp_list=iter->second;
        for(int64_t i = 0; i< tmp_list.get_count(); i++)
        {
          tmp_list.get_idx_id(i,idx_id);
          if (NULL == (index_table_schema = get_table_schema(idx_id)))
          {
            ret = OB_SCHEMA_ERROR;
            TBSYS_LOG(WARN,"fail to get table schema for index table[%lu]", idx_id);
          }
          else
          {
            if(index_table_schema->get_index_helper().status == INDEX_INIT)
            {
              index_id_list.push_back(idx_id);
            }
          }
        }
      }
      return ret;
    }
    bool ObSchemaManagerV2::is_have_init_index(uint64_t table_id) const
    //获得tid为main_tid的表的所有的可用的索引表，存到数组tid_array里面
    {
      bool ret=false;
      IndexList il;
      uint64_t tmp_tid = OB_INVALID_ID;
      if(index_hash_init_)
      {
        if(hash::HASH_EXIST == id_index_map_.get(table_id,il))
        {
          for(int64_t i = 0;i < il.get_count();i++)
          {
            const ObTableSchema *main_table_schema = NULL;
            il.get_idx_id(i,tmp_tid);
            if (NULL == (main_table_schema = get_table_schema(tmp_tid)))
            {
              TBSYS_LOG(WARN,"fail to get table schema for table[%ld]", tmp_tid);
            }
            else
            {
              if(main_table_schema->get_index_helper().status == INDEX_INIT)
              {
                ret=true;
                break;
              }
            }
          }
        }
      }
      return ret;
    }

    //add:e
    //add liumz, [secondary index static_index_build] 20150529:b
    int ObSchemaManagerV2::get_all_staging_index_tid(ObArray<uint64_t> &index_id_list) const
    {
      int ret = OB_SUCCESS;
      uint64_t idx_id = OB_INVALID_ID;
      const ObTableSchema *index_table_schema = NULL;
      hash::ObHashMap<uint64_t,IndexList,hash::NoPthreadDefendMode>::const_iterator iter = id_index_map_.begin();
      for (;iter != id_index_map_.end(); ++iter)
      {
        IndexList tmp_list=iter->second;
        for(int64_t i = 0; i< tmp_list.get_count(); i++)
        {
          tmp_list.get_idx_id(i,idx_id);
          if (NULL == (index_table_schema = get_table_schema(idx_id)))
          {
            ret = OB_SCHEMA_ERROR;
            TBSYS_LOG(WARN,"fail to get table schema for index table[%lu]", idx_id);
          }
          else
          {
            if(index_table_schema->get_index_helper().status == NOT_AVALIBALE)
            {
              if (OB_SUCCESS != (index_id_list.push_back(idx_id)))
              {
                TBSYS_LOG(WARN, "push index table[%lu] into index_id_list failed", idx_id);
              }
            }
          }
        }
      }
      return ret;
    }
    //add e
    //add liumz, [secondary index static_index_build] 20150629:b
    int ObSchemaManagerV2::get_all_index_tid(ObArray<uint64_t> &index_id_list) const
    {
      int ret = OB_SUCCESS;
      uint64_t idx_id = OB_INVALID_ID;
      hash::ObHashMap<uint64_t,IndexList,hash::NoPthreadDefendMode>::const_iterator iter = id_index_map_.begin();
      for (;iter != id_index_map_.end(); ++iter)
      {
        IndexList tmp_list=iter->second;
        for(int64_t i = 0; i< tmp_list.get_count(); i++)
        {
          tmp_list.get_idx_id(i,idx_id);
          if (OB_SUCCESS != (index_id_list.push_back(idx_id)))
          {
            TBSYS_LOG(WARN, "push index table[%lu] into index_id_list failed", idx_id);
          }
        }
      }
      return ret;
    }
    //add e

    //add wenghaixing [secondary index drop table_with_index]20141222
    int ObSchemaManagerV2::get_all_modifiable_index_num(uint64_t main_tid,int64_t &count) const
    {
      int ret = OB_SUCCESS;
      IndexList idx_list;
      uint64_t tmp_tid = OB_INVALID_ID;
      count = 0;
      if(index_hash_init_)
      {
        if(hash::HASH_EXIST == id_index_map_.get(main_tid, idx_list))
        {
          for(int64_t i=0; i<idx_list.get_count(); i++)
          {
            const ObTableSchema *main_table_schema = NULL;
            idx_list.get_idx_id(i, tmp_tid);
            if (NULL == (main_table_schema = get_table_schema(tmp_tid)))
            {
              ret = OB_ERR_ILLEGAL_ID;
              TBSYS_LOG(WARN,"fail to get table schema for table[%ld]", main_tid);
            }
            else
            {
              IndexStatus status = main_table_schema->get_index_helper().status;
              if(status == AVALIBALE || status == WRITE_ONLY || INDEX_INIT == status || NOT_AVALIBALE == status)
              {
                count++;
              }
            }
          }
        }
      }
      else
        ret = OB_NOT_INIT;
      return ret;
    }
    //add e
    //add fanqiushi_index
    //判断tid为table_id的表是否有可用的索引 //repaired from messy code by zhuxh 20151014
    bool ObSchemaManagerV2::is_have_available_index(uint64_t table_id) const
    {
           bool ret=false;
           IndexList il;
            uint64_t tmp_tid=OB_INVALID_ID;
           if(index_hash_init_)
           {
                if(hash::HASH_EXIST==id_index_map_.get(table_id,il))
                {
                    for(int64_t i=0;i<il.get_count();i++)
                    {
                        const ObTableSchema *main_table_schema = NULL;
                        il.get_idx_id(i,tmp_tid);
                        if (NULL == (main_table_schema = get_table_schema(tmp_tid)))
                        {
                          //ret = OB_ERR_ILLEGAL_ID;
                          TBSYS_LOG(WARN,"fail to get table schema for table[%ld]", tmp_tid);
                        }
                        //mod liumz, [bugfix: break loop if ret == true]:20150604
                        /*else
                        {
                            if(main_table_schema->get_index_helper().status==AVALIBALE)
                              ret=true;
                        }*/
                        else if(main_table_schema->get_index_helper().status==AVALIBALE)
                        {
                          ret = true;
                          break;
                        }
                        //mod:e
                    }
                    //TBSYS_LOG(ERROR,"test::fanqs,,table_id=%ld,,il.get_count()=%ld",table_id,il.get_count());
                   // if(il.get_count()>0)
                        //ret = true;
                }
           }
           return ret;
    }
    int ObSchemaManagerV2::get_all_avalibale_index_tid(uint64_t main_tid,uint64_t *tid_array,uint64_t &count) const   //获得tid为main_tid的表的所有的可用的索引表，存到数组tid_array里面
    {
        int ret=OB_SUCCESS;
        IndexList il;
        uint64_t tmp_tid=OB_INVALID_ID;
        uint64_t tmp_count=0;
        if(index_hash_init_)
        {
             if(hash::HASH_EXIST==id_index_map_.get(main_tid,il))
             {
                 for(int64_t i=0;i<il.get_count();i++)
                 {
                     const ObTableSchema *main_table_schema = NULL;
                     il.get_idx_id(i,tmp_tid);
                     if (NULL == (main_table_schema = get_table_schema(tmp_tid)))
                     {
                       ret = OB_ERR_ILLEGAL_ID;
                       TBSYS_LOG(WARN,"fail to get table schema for table[%ld]", main_tid);
                     }
                     else
                     {
                         if(main_table_schema->get_index_helper().status==AVALIBALE)
                         {
                             tid_array[tmp_count]=tmp_tid;
                             tmp_count++;
                         }

                     }
                 }
             }
        }
        else
            ret=OB_NOT_INIT;

        count=tmp_count;
        return ret;
    }

    bool ObSchemaManagerV2::is_this_table_avalibale(uint64_t tid) const   //判断tid为参数的表是否是可用的索引表
    {
        bool ret=false;
        const ObTableSchema *main_table_schema = NULL;
        if (NULL == (main_table_schema = get_table_schema(tid)))
        {
          //ret = OB_ERR_ILLEGAL_ID;
          TBSYS_LOG(WARN,"fail to get table schema for table[%ld]", tid);
        }
        else
        {
            if(main_table_schema->get_index_helper().status==AVALIBALE)
            {
                ret=true;
            }

        }
        return ret;
    }

    bool ObSchemaManagerV2::is_cid_in_index(uint64_t cid,uint64_t tid,uint64_t *index_tid_array) const
    {
        bool ret=false;
        IndexList il;
        uint64_t tmp_tid=OB_INVALID_ID;
        int32_t array_index=0;
        if(index_hash_init_)
        {
             if(hash::HASH_EXIST==id_index_map_.get(tid,il))
             {
                 for(int64_t i=0;i<il.get_count();i++)
                 {
                     const ObTableSchema *index_table_schema = NULL;
                     il.get_idx_id(i,tmp_tid);
                     //TBSYS_LOG(ERROR,"test::fanqs4,,tmp_tid=%ld",tmp_tid);
                     if (NULL == (index_table_schema = get_table_schema(tmp_tid)))
                     {
                       ret = false;
                       TBSYS_LOG(WARN,"fail to get table schema for table[%ld]", tmp_tid);
                     }
                     else
                     {
                          const ObRowkeyInfo *rowkey_info = &index_table_schema->get_rowkey_info();
                          uint64_t index_first_cid=OB_INVALID_ID;
                          rowkey_info->get_column_id(0,index_first_cid);
                          //TBSYS_LOG(ERROR,"test::fanqs4,,enter this,index_first_cid=%ld,cid=%ld",index_first_cid,cid);
                          if(index_first_cid==cid&&index_table_schema->get_index_helper().status==AVALIBALE)
                          {
                             index_tid_array[array_index]=tmp_tid;
                             //TBSYS_LOG(ERROR,"test::fanqs4,,enter this,tmp_tid=%ld,tid=%ld,,,array_index=%d",tmp_tid,tid,array_index);
                             array_index++;
                             ret=true;
                            // break;
                          }
                        // if(main_table_schema->get_index_helper().status==AVALIBALE)
                         //{
                             //tid_array[i]=tmp_tid;
                            // count=i+1;
                         //}

                     }
                 }
                 //TBSYS_LOG(ERROR,"test::fanqs4,,index_tid_array[0]=%ld,,array_index=%d",index_tid_array[0],array_index);
             }
        }
        else
            ret=false;
        return ret;
    }

    //add:e
    bool ObSchemaManagerV2::check_compress_name() const
    {
      int ret = OB_SUCCESS;
      bool bret = true;
      const char* compress_name = NULL;
      for (int64_t i = 0; i < table_nums_ && OB_SUCCESS == ret; ++i)
      {
        if (NULL != (compress_name = table_infos_[i].get_compress_func_name())
            && compress_name[0] != '\0')
        {
          if (0 != strcmp(compress_name, "lzo_1.0")
              && 0 != strcmp(compress_name, "none")
              && 0 != strcmp(compress_name, "snappy_1.0"))
          {
            bret = false;
            TBSYS_LOG(ERROR, "table compress function name check failed. table_name=%s, compress_name=%s",
                table_infos_[i].get_table_name(), compress_name);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "fail to get tabel compress name, table_name=%s, compress_name=%p", table_infos_[i].get_table_name(), compress_name);
          bret = false;
          ret = OB_ERROR;
        }
      }
      return bret;
    }

    bool ObSchemaManagerV2::check_table_expire_condition() const
    {
      int ret   = OB_SUCCESS;
      bool bret = true;
      const char* expire_condition = NULL;
      char infix_condition_expr[OB_MAX_EXPIRE_CONDITION_LENGTH];
      ObString cond_expr;
      ObExpressionParser* parser = new (std::nothrow) ObExpressionParser();

      if (NULL == parser)
      {
        TBSYS_LOG(WARN, "failed to new expression parser");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }

      for (int64_t i = 0; i < table_nums_ && OB_SUCCESS == ret; ++i)
      {
        if (NULL != (expire_condition = table_infos_[i].get_expire_condition())
            && expire_condition[0] != '\0')
        {
          if (static_cast<int64_t>(strlen(expire_condition)) >= OB_MAX_EXPIRE_CONDITION_LENGTH)
          {
            TBSYS_LOG(WARN, "expire condition too large, expire_condition_len=%zu, "
                "max_condition_len=%ld, table_name=%s",
                strlen(expire_condition), OB_MAX_EXPIRE_CONDITION_LENGTH,
                table_infos_[i].get_table_name());
            ret = OB_ERROR;
          }
          else
          {
            strcpy(infix_condition_expr, expire_condition);
            ret = replace_system_variable(infix_condition_expr,
                OB_MAX_EXPIRE_CONDITION_LENGTH);
            if (OB_SUCCESS == ret)
            {
              cond_expr.assign_ptr(infix_condition_expr,
                  static_cast<int32_t>(strlen(infix_condition_expr)));
              ret = check_expire_dependent_columns(cond_expr,
                  table_infos_[i], *parser);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "failed to check expire dependent columns, infix_expr=%s",
                    infix_condition_expr);
              }
            }
          }
        }
      }

      if (OB_SUCCESS != ret)
      {
        bret = false;
      }

      if (NULL != parser)
      {
        delete parser;
        parser = NULL;
      }

      return bret;
    }

    bool ObSchemaManagerV2::ObColumnGroupHelperCompare::operator() (const ObColumnGroupHelper& l,
        const ObColumnGroupHelper& r) const
    {
      bool ret = false;
      if (l.table_id_ < r.table_id_ ||
          (l.table_id_ == r.table_id_ && l.column_group_id_ < r.column_group_id_))
      {
        ret = true;
      }
      //bool ret = true;
      //if (l.table_id_ > r.table_id_ ||
      //    (l.table_id_ == r.table_id_ && l.column_group_id_ > r.column_group_id_) )
      //{
      //  ret = false;
      //}
      return ret;
    }

    int ObSchemaManagerV2::get_column_groups(uint64_t table_id,uint64_t column_groups[],int32_t& size) const
    {
      int ret = OB_SUCCESS;
      const ObColumnGroupHelper *begin = NULL;

      if ( OB_INVALID_ID == table_id || size < 0)
      {
        TBSYS_LOG(ERROR,"invalid argument");
        ret = OB_ERROR;
      }
      else
      {
        const ObColumnGroupHelper *end = NULL;

        ObColumnGroupHelper target;
        target.table_id_ = table_id;
        target.column_group_id_ = 0;

        begin = std::lower_bound(column_groups_,column_groups_ + column_group_nums_,target,ObColumnGroupHelperCompare());

        if  (begin != NULL && begin != column_groups_ + column_group_nums_
            && begin->table_id_ == table_id)
        {
          target.column_group_id_ = OB_INVALID_ID;
          end = std::upper_bound(begin,column_groups_ + column_group_nums_,target,ObColumnGroupHelperCompare());
          size = static_cast<int32_t>(size > end - begin ? end - begin : size);
          for(int32_t i=0; i<size; ++i)
          {
            column_groups[i] = (begin + i)->column_group_id_;
          }
        }
        else
        {
          size = 0;
          TBSYS_LOG(WARN,"not found column group in table %lu,column_group_nums_:%ld",table_id,column_group_nums_);
          ret = OB_ERROR;
        }
      }
      return ret;
    }

    int64_t ObSchemaManagerV2::ObColumnNameKey::hash() const
    {
      return table_name_.hash() + column_name_.hash();
    }

    bool ObSchemaManagerV2::ObColumnNameKey::operator==(const ObColumnNameKey& key) const
    {
      bool ret = false;
      if (table_name_ == key.table_name_ && column_name_ == key.column_name_)
      {
        ret = true;
      }
      return ret;
    }

    int64_t ObSchemaManagerV2::ObColumnIdKey::hash() const
    {
      hash::hash_func<uint64_t> h;
      return h(table_id_) + h(column_id_);
    }

    bool ObSchemaManagerV2::ObColumnIdKey::operator==(const ObColumnIdKey& key) const
    {
      bool ret = false;
      if (table_id_ == key.table_id_ && column_id_ == key.column_id_)
      {
        ret = true;
      }
      return ret;
    }

    DEFINE_SERIALIZE(ObSchemaManagerV2)
    {
      int ret = 0;
      int64_t tmp_pos = pos;

      if (schema_magic_ != OB_SCHEMA_MAGIC_NUMBER)
      {
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i32(buf, buf_len, tmp_pos, schema_magic_);
      }

      if (OB_SUCCESS == ret)
      {
        //mod zhaoqiong [Schema Manager] 20150424:b
        // CAUTION! only support serialize ObSchemaManagerV2 with version 5
//        ret = serialization::encode_vi32(buf, buf_len, tmp_pos, OB_SCHEMA_VERSION_FOUR);
        ret = serialization::encode_vi32(buf, buf_len, tmp_pos, OB_SCHEMA_VERSION_SIX);
        //mod:e
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, timestamp_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, static_cast<int64_t>(max_table_id_));
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, column_nums_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, table_nums_);
      }


      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vstr(buf, buf_len, tmp_pos, app_name_);
      }
      //add zhaoqiong [Schema Manager] 20150327:b
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, table_begin_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, table_end_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, column_begin_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, column_end_);
      }
      //add:e

      if (OB_SUCCESS == ret)
      {
        //mod zhaoqiong [Schema Manager] 20150327:b
        //for (int64_t i = 0; i < table_nums_; i++)
        for (int64_t i = table_begin_; i < table_end_; i++)
        //mod:e
        {
          ret = table_infos_[i].serialize(buf, buf_len, tmp_pos);
          if (OB_SUCCESS != ret)
          {
            break;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        //mod zhaoqiong [Schema Manager] 20150327:b
        //for (int64_t i = 0; i < column_nums_; i++)
        for (int64_t i = column_begin_; i < column_end_; i++)
        //mod:e
        {
          ret = columns_[i].serialize(buf, buf_len, tmp_pos);
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

    DEFINE_DESERIALIZE(ObSchemaManagerV2)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;
      //add zhaoqiong [Schema Manager] 20150327:b
      int64_t table_begin =0;
      int64_t column_begin = 0;
      //add:e
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i32(buf, data_len, tmp_pos, &schema_magic_);
      }

      if (OB_SUCCESS == ret)
      {
        if (schema_magic_ != OB_SCHEMA_MAGIC_NUMBER) //old schema
        {
          TBSYS_LOG(ERROR,"schema magic numer is wrong, schema_magic_=%x, OB_SCHEMA_MAGIC_NUMBER=%x",
              schema_magic_, OB_SCHEMA_MAGIC_NUMBER);
          ret = OB_ERROR;
        }
        else
        {
          if (OB_SUCCESS == ret)
          {
            ret = serialization::decode_vi32(buf, data_len, tmp_pos, &version_);
          }

          if (OB_SUCCESS == ret)
          {
            ret = serialization::decode_vi64(buf, data_len, tmp_pos, &timestamp_);
          }

          if (OB_SUCCESS == ret)
          {
            ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t *>(&max_table_id_));
          }
          if (OB_SUCCESS == ret)
          {
            ret = serialization::decode_vi64(buf, data_len, tmp_pos, &column_nums_);
          }

          if (OB_SUCCESS == ret)
          {
            ret = serialization::decode_vi64(buf, data_len, tmp_pos, &table_nums_);
          }

          if (OB_SUCCESS == ret)
          {
            int64_t len = 0;
            serialization::decode_vstr(buf, data_len, tmp_pos,
                app_name_, OB_MAX_APP_NAME_LENGTH, &len);
            if (-1 == len)
            {
              ret = OB_ERROR;
            }
          }

          if (OB_SUCCESS == ret)
          {
            if (table_nums_ < 0 || table_nums_ > OB_MAX_TABLE_NUMBER)
            {
              TBSYS_LOG(ERROR, "bugs, table_nums_ %ld error", table_nums_);
              ret = OB_ERROR;
            }
          }
          //add zhaoqiong [Schema Manager] 20150327:b
          if (OB_SUCCESS == ret && version_ >= OB_SCHEMA_VERSION_FIVE)
          {
            ret = serialization::decode_vi64(buf, data_len, tmp_pos, &table_begin);
            TBSYS_LOG(DEBUG, "schema table_begin= %ld ",table_begin);
            if (table_begin != table_end_) //discontinuous
            {
              TBSYS_LOG(ERROR,"schema is discontinuous, expect_table_begin_=%ld, table_begin_=%ld",table_end_,table_begin_);
              ret = OB_ERROR;
            }
          }
          if (OB_SUCCESS == ret && version_ >= OB_SCHEMA_VERSION_FIVE)
          {
            ret = serialization::decode_vi64(buf, data_len, tmp_pos, &table_end_);
            TBSYS_LOG(DEBUG, "schema table_end= %ld ",table_end_);
            if (table_end_ > table_nums_) // illegal
            {
              TBSYS_LOG(ERROR,"schema is illegal, table_end_=%ld, table_nums_=%ld",table_end_,table_nums_);
              ret = OB_ERROR;
            }
          }

          if (OB_SUCCESS == ret && version_ >= OB_SCHEMA_VERSION_FIVE)
          {
            ret = serialization::decode_vi64(buf, data_len, tmp_pos, &column_begin);
            TBSYS_LOG(DEBUG, "schema column_begin= %ld ",column_begin);
            if (column_begin != column_end_) //discontinuous
            {
              TBSYS_LOG(ERROR,"schema is discontinuous, expect_column_begin_=%ld, column_begin_=%ld",column_end_,column_begin_);
              ret = OB_ERROR;
            }
          }
          if (OB_SUCCESS == ret && version_ >= OB_SCHEMA_VERSION_FIVE)
          {
            ret = serialization::decode_vi64(buf, data_len, tmp_pos, &column_end_);
            TBSYS_LOG(DEBUG, "schema column_end= %ld ",column_end_);
            if (column_end_ > column_nums_) // illegal
            {
              TBSYS_LOG(ERROR,"schema is illegal, column_end_=%ld, column_nums_=%ld",column_end_,column_nums_);
              ret = OB_ERROR;
            }
          }
          //add:e
          //add zhaoqiong [Schema Manager] 20150424:b
          if (OB_SUCCESS == ret && version_ >= OB_SCHEMA_VERSION_FIVE)
          {
            for (int64_t i = table_begin; i < table_end_; ++i)
            {
              table_infos_[i].set_version(version_);
              ret = table_infos_[i].deserialize(buf, data_len, tmp_pos);
              if (OB_SUCCESS != ret)
                break;
            }
          }
          //add:e
          //mod zhaoqiong [Schema Manager] 20150424:b
          //if (OB_SUCCESS == ret)
          else if (OB_SUCCESS == ret && version_ < OB_SCHEMA_VERSION_FIVE)
            //mod:e
          {
            for (int64_t i = 0; i < table_nums_; ++i)
            {
              table_infos_[i].set_version(version_);
              ret = table_infos_[i].deserialize(buf, data_len, tmp_pos);
              if (OB_SUCCESS != ret)
                break;

            }
          }
          //mod zhaoqiong [Schema Manager] 20150327:b
//          if (OB_SUCCESS == ret)
//          {
//            ret = prepare_column_storage(column_nums_);
//          }
          //exp:first deserialize, need alloc all column memory
          //the followed deserialize do not need alloc any more
          if (OB_SUCCESS == ret && 0 == column_begin)
          {
            ret = prepare_column_storage(column_nums_);
          }
          //mod:e

          //del zhaoqiong [Schema Manager] 20150424:b
//          if (OB_SUCCESS == ret)
//          {
//            for (int64_t i = 0; i < column_nums_; ++i)
//            {
//              if (version_ <= OB_SCHEMA_VERSION_THREE)
//              {
//                ret = columns_[i].deserialize_v3(buf, data_len, tmp_pos);
//              }
//              else
//              {
//                ret = columns_[i].deserialize_v4(buf, data_len, tmp_pos);
//              }
//              if (OB_SUCCESS != ret)
//                break;
//            }
//          }
          //del:e



          //add zhaoqiong [Schema Manager] 20150424:b
          if (OB_SUCCESS == ret && version_ <= OB_SCHEMA_VERSION_THREE)
          {
            for (int64_t i = 0; i < column_nums_; ++i)
            {
              ret = columns_[i].deserialize_v3(buf, data_len, tmp_pos);
              if (OB_SUCCESS != ret)
                break;
            }

          }
          else if (OB_SUCCESS == ret && version_ <= OB_SCHEMA_VERSION_FOUR)
          {
            for (int64_t i = 0; i < column_nums_; ++i)
            {
              ret = columns_[i].deserialize_v4(buf, data_len, tmp_pos);
              if (OB_SUCCESS != ret)
                break;
            }
          }
          else if (OB_SUCCESS == ret && version_ > OB_SCHEMA_VERSION_FOUR)
          {
            for (int64_t i = column_begin; i < column_end_; ++i)
            {
              ret = columns_[i].deserialize_v4(buf, data_len, tmp_pos);
              if (OB_SUCCESS != ret)
                break;
            }
          }

          if (OB_SUCCESS == ret && version_ > OB_SCHEMA_VERSION_FOUR)
          {
            pos = tmp_pos;
            if (table_end_ < table_nums_)
            {
              is_completion_ = false;
            }
            else
            {
              is_completion_ = true;
              sort_column();
              //add liuxiao [secondary index static index] 20150615
              init_index_hash();
              //add e
            }
          }
          else if( OB_SUCCESS == ret)
          {
            sort_column();
          }
          //del zhaoqiong [Schema Manager] 20150327:b
//          if (OB_SUCCESS == ret)
//          {
//            sort_column();
//          }          
          //del:e
		  //add wenghaixing [secondary index]20141105
          //add e
        }
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObSchemaManagerV2)
    {
      int64_t len = serialization::encoded_length_i32(schema_magic_); //magic number use fixed length
      len += serialization::encoded_length_vi32(version_);
      len += serialization::encoded_length_vi64(timestamp_);
      len += serialization::encoded_length_vi64(static_cast<int64_t>(max_table_id_));
      len += serialization::encoded_length_vi64(column_nums_);
      len += serialization::encoded_length_vi64(table_nums_);
      len += serialization::encoded_length_vstr(app_name_);
      //add zhaoqiong [Schema Manager] 20150327:b
      len += serialization::encoded_length_vi64(table_begin_);
      len += serialization::encoded_length_vi64(table_end_);
      len += serialization::encoded_length_vi64(column_begin_);
      len += serialization::encoded_length_vi64(column_end_);
      //add:e
      //mod zhaoqiong [Schema Manager] 20150327:b
//      for (int64_t i = 0; i < table_nums_; ++i)
//      {
//        len += table_infos_[i].get_serialize_size();
//      }

//      for (int64_t i = 0; i < column_nums_; ++i)
//      {
//        len += columns_[i].get_serialize_size();
//      }
      if (version_ > OB_SCHEMA_VERSION_FOUR)
      {
        for (int64_t i = table_begin_; i < table_end_; ++i)
        {
          len += table_infos_[i].get_serialize_size();
        }

        for (int64_t i = column_begin_; i < column_end_; ++i)
        {
          len += columns_[i].get_serialize_size();
        }
      }
      else
      {
        for (int64_t i = 0; i < table_nums_; ++i)
        {
          len += table_infos_[i].get_serialize_size();
        }

        for (int64_t i = 0; i < column_nums_; ++i)
        {
          len += columns_[i].get_serialize_size();
        }

      }
	  //mod:e

      return len;
    }

    int ObSchemaManagerV2::add_new_table_schema(const ObArray<TableSchema>& schema_array)
    {
      int ret = OB_SUCCESS;
      for (int i = 0; i < schema_array.count() && OB_SUCCESS == ret; i++)
      {
        TableSchema *tschema = (TableSchema*)(&schema_array.at(i));
        ObTableSchema old_tschema;
        old_tschema.set_table_id(tschema->table_id_);
        old_tschema.set_max_column_id(tschema->max_used_column_id_);
        //modfiy dolphin [database manager]@20150617:b
        //old_tschema.set_table_name(tschema->table_name_);
        ObString dt;
        char buf[OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1] = {0};
        dt.assign_buffer(buf,OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1);
        dt.concat(tschema->dbname_,tschema->table_name_);
        old_tschema.set_table_name(dt);
        //modifye:e

        ObTableSchema::TableType table_type = ObTableSchema::INVALID;
        if (TableSchema::DISK == tschema->load_type_)
        {
          table_type = ObTableSchema::SSTABLE_IN_DISK;
        }
        else if (TableSchema::MEMORY == tschema->load_type_)
        {
          table_type = ObTableSchema::SSTABLE_IN_RAM;
        }
        old_tschema.set_table_type(table_type);
        old_tschema.set_split_pos(tschema->rowkey_split_);            //
        old_tschema.set_rowkey_max_length(tschema->max_rowkey_length_);
        old_tschema.set_merge_write_sstable_version(tschema->merge_write_sstable_version_);
        old_tschema.set_schema_version(tschema->schema_version_);
        old_tschema.set_compressor_name(tschema->compress_func_name_); //
        old_tschema.set_block_size(tschema->tablet_block_size_);
        old_tschema.set_max_sstable_size(tschema->tablet_max_size_);
        //add zhaoqiong[roottable tablet management]20150302:b
        if ((tschema->replica_num_ >0 && tschema->replica_num_<= OB_MAX_COPY_COUNT)
                //add wangdonghui [create table with expire info] 20170510 :b
                || tschema->replica_num_ == OB_DEFAULT_COPY_COUNT
                //add :e
                )
        {
            old_tschema.set_replica_count(tschema->replica_num_);
        }
        else
        {
            ret = OB_ERROR;
            continue;
        }
		//add e
        old_tschema.set_use_bloomfilter(tschema->is_use_bloomfilter_); //
        old_tschema.set_consistency_level(tschema->consistency_level_);
        old_tschema.set_pure_update_table(tschema->is_pure_update_table_); // @deprecated
        old_tschema.set_create_time_column(tschema->create_time_column_id_);
        old_tschema.set_modify_time_column(tschema->modify_time_column_id_);
        //add wenghaixing [secondary index] 20141104_02
        old_tschema.set_index_helper(tschema->get_index_helper());
        //add e
        //add wenghaixing [secondary index create fix]20141226
        old_tschema.set_replica_count(tschema->replica_num_);
        //add e
        if (tschema->expire_condition_[0] != '\0')
        {
          TBSYS_LOG(INFO, "expire condition =%s", tschema->expire_condition_);
          old_tschema.set_expire_condition(tschema->expire_condition_);
        }
        if (tschema->comment_str_[0] != '\0')
        {
          old_tschema.set_comment_str(tschema->comment_str_);
        }
        if (OB_SUCCESS != (ret = add_table(old_tschema)))
        {
          TBSYS_LOG(WARN, "failed to add table, err=%d", ret);
        }
        else
        {
          TBSYS_LOG(DEBUG, "add table to schema_manager succ. table_id = %lu", tschema->table_id_);
          ObRowkeyInfo rowkey_info;
          ObRowkeyColumn rowkey_column;
          uint64_t left_column_offset_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
          int64_t right_rowkey_column_count = 0;
          if (0 < tschema->join_info_.count())
          {
            TBSYS_LOG(DEBUG, "this is a wide_table. add join info");
            if (OB_SUCCESS != (ret = ObSchemaHelper::get_left_offset_array(schema_array, i,
                    tschema->join_info_.at(0).right_table_id_,left_column_offset_array, right_rowkey_column_count)))
            {
              TBSYS_LOG(WARN, "fail to get offset array.");
            }
            //for debug
            for (int i = 0; i < right_rowkey_column_count; i++)
            {
              TBSYS_LOG(DEBUG, "left_column_offset_array[%d] = %ld", i, left_column_offset_array[i]);
            }
          }
          int count = 0;
          for (int64_t i = 0; i < tschema->columns_.count() && ret == OB_SUCCESS; i++)
          {
            ObColumnSchemaV2 old_tcolumn;
            const ColumnSchema &tcolumn = tschema->columns_.at(i);
            old_tcolumn.set_table_id(tschema->table_id_);
            //add wenghaixing DECIMAL OceanBase_BankCommV0.2 2014_6_5:b
            if (tcolumn.data_type_ == ObDecimalType) {
                old_tcolumn.set_precision(
                    static_cast<uint32_t>(tcolumn.data_precision_));
                old_tcolumn.set_scale(
                    static_cast<uint32_t>(tcolumn.data_scale_));
            }
              //add:e
            old_tcolumn.set_column_id(tcolumn.column_id_);
            old_tcolumn.set_column_name(tcolumn.column_name_);
            old_tcolumn.set_column_type(tcolumn.data_type_);
            old_tcolumn.set_column_size(tcolumn.data_length_);
            old_tcolumn.set_maintained(true); // @deprecated
            old_tcolumn.set_column_group_id(tcolumn.column_group_id_);
            //add peiouya [NotNULL_check] [JHOBv0.1] 20131208:b
			old_tcolumn.set_nullable(tcolumn.nullable_);
            //add 20131208:e
            // @todo join info
            if (0 < tcolumn.join_table_id_)
            {
              count++;
              TBSYS_LOG(DEBUG, "%d, column_name=%s, column_id=%lu have join info, join_table=%lu, join_column_id=%lu",
                  count, tcolumn.column_name_, tcolumn.column_id_, tcolumn.join_table_id_, tcolumn.join_column_id_);
              old_tcolumn.set_join_info(tcolumn.join_table_id_, left_column_offset_array,
                  right_rowkey_column_count, tcolumn.join_column_id_);
            }
            if (0 < tcolumn.rowkey_id_)
            {
              rowkey_column.column_id_ = tcolumn.column_id_;
              rowkey_column.type_ = tcolumn.data_type_;
              rowkey_column.length_ = tcolumn.length_in_rowkey_;
              rowkey_column.order_ = static_cast<ObRowkeyColumn::Order>(tcolumn.order_in_rowkey_);
              if (OB_SUCCESS != (ret = rowkey_info.set_column(tcolumn.rowkey_id_-1, rowkey_column)))
              {
                TBSYS_LOG(WARN, "failed to set rowkey column, err=%d", ret);
                break;
              }
            }
            if (OB_SUCCESS == ret)
            {
              if (OB_SUCCESS != (ret = add_column_without_sort(old_tcolumn)))
              {
                TBSYS_LOG(WARN, "failed to add column, err=%d", ret);
                break;
              }
            }
          } // end for
          if (OB_SUCCESS == ret)
          {
            ObTableSchema* p_old_tschema = this->get_table_schema(/**modify dolphin [database manager]@20150616 tschema->table_name_ */old_tschema.get_table_name());
            if (NULL == p_old_tschema)
            {
              TBSYS_LOG(WARN, "failed to get table schema");
            }
            else
            {
              p_old_tschema->set_rowkey_info(rowkey_info);
            }
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG, "add new table_schema success");
      }
      else
      {
        TBSYS_LOG(WARN, "fail to add new table schema");
      }
      return ret;
    }

    // convert the new table schema into old one and insert it into the schema_manager
    // zhuweng.yzf@taobao.com
    int ObSchemaManagerV2::add_new_table_schema(const TableSchema& tschema)
    {
      int ret = OB_SUCCESS;
      ObArray<TableSchema> array_wrapper;
      ret = array_wrapper.push_back(tschema);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "push back to array fail");
      }
      else
      {
        ret = add_new_table_schema(array_wrapper);
      }
      return ret;
    }

    //add zhaoqiong[Schema Manager]20150310:b
    int ObSchemaManagerV2::drop_one_table(uint64_t table_id)
    {
        int ret = OB_SUCCESS;
        if (table_nums_ >= OB_MAX_TABLE_NUMBER|| table_nums_ <= 0)
        {
            ret = OB_ERROR;
        }
        ObTableSchema* tschema = get_table_schema(table_id);
        if (NULL == tschema)
        {
            TBSYS_LOG(WARN,"can't drop this table's ObTableSchema:[%ld]",table_id);
            ret = OB_ENTRY_NOT_EXIST;
        }
        if (OB_SUCCESS == ret)
        {
            if (tschema->get_table_id() == max_table_id_)
            {
                max_table_id_ = OB_INVALID_ID;
                for (int64_t i = 0; i < table_nums_ && OB_SUCCESS == ret; ++i)
                {
                    ObTableSchema *tmp = &table_infos_[i];
                    if (tmp == tschema)
                        continue;
                    if ((OB_INVALID_ID == max_table_id_) || (tmp->get_table_id() > max_table_id_))
                        max_table_id_ = tmp->get_table_id();
                }
                TBSYS_LOG(DEBUG,"update max table_id = %ld",max_table_id_);
            }
            memmove(tschema,tschema+1,sizeof(ObTableSchema) * (table_nums_ - (tschema - table_infos_)));
            table_nums_--;

          int32_t csize = 0;
          ObColumnSchemaV2 *tcolumn = const_cast<ObColumnSchemaV2 *>(get_table_schema(table_id,csize));
          if ( NULL == tcolumn)
          {
              TBSYS_LOG(WARN,"can't drop this table's ObColumnSchema:[%ld]",table_id);
              ret = OB_ENTRY_NOT_EXIST;
          }
          if (OB_SUCCESS == ret)
          {
              memmove(tcolumn,tcolumn+csize,sizeof(ObColumnSchemaV2) * (column_nums_ - (tcolumn + csize - columns_)));
              column_nums_ -= csize;
              TBSYS_LOG(DEBUG,"drop this table schema success:[%ld]",table_id);
          }
        }
         if (OB_ENTRY_NOT_EXIST == ret)
          {
            ret = OB_SUCCESS;
            TBSYS_LOG(WARN,"this table may not be created:[%ld]",table_id);
          }
        return ret;
    }

    int ObSchemaManagerV2::drop_tables(const ObArray<int64_t>& table_array)
    {
        int ret = OB_SUCCESS;
        for (int i = 0; i < table_array.count() && OB_SUCCESS == ret; i++)
        {
          uint64_t drop_table_id = table_array.at(i);
          if (OB_SUCCESS != (ret = drop_one_table(drop_table_id)))
          {
            TBSYS_LOG(WARN, "failed to drop table, err=%d", ret);
          }
        }
        return ret;
    }

    void ObSchemaManagerV2::reset()
    {
      table_begin_ = 0;
      table_end_ = 0;
      table_nums_ = 0;
      column_begin_ = 0;
      column_end_ = 0;
      column_nums_ = 0;
    }

    //add e



    int ObSchemaManagerV2::write_to_file(const char* file_name)
    {
      int ret = OB_SUCCESS;
      TBSYS_LOG(INFO, "write schema to file. file_name=%s", file_name);
      FILE *fd = NULL;
      if (NULL == file_name || (NULL == (fd = fopen(file_name, "w"))))
      {
        TBSYS_LOG(WARN, "can't open file. file_name=%p", file_name);
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == ret)
      {
        if (0 >= (ret = fprintf(fd, "[%s]\n", STR_SECTION_APP_NAME)))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
        }
        else if (0 >= (ret = fprintf(fd, "%s=%s\n", STR_KEY_APP_NAME, app_name_)))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
        }
        else if (0 >= (ret = fprintf(fd, "%s=%lu\n", STR_MAX_TABLE_ID, max_table_id_)))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
        }
        else if (0 >= (ret = fprintf(fd, "%s=%d\n", STR_SCHEMA_VERSION, version_)))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
        }
        else
        {
          ret = OB_SUCCESS;
        }
      }
      if (OB_SUCCESS == ret)
      {
        for (int64_t i = 0; i < table_nums_; i++)
        {
          ret = write_table_to_file(fd, i);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fail to write table to file. table index =%ld, ret=%d", i, ret);
            break;
          }
        }
      }
      if (NULL != fd)
      {
        fclose(fd);
      }
      return ret;
    }

    int ObSchemaManagerV2::write_table_to_file(FILE *fd, const int64_t table_index)
    {
      int ret = OB_SUCCESS;
      if (table_index >= table_nums_ || NULL == fd)
      {
        TBSYS_LOG(WARN, "invalid index. index=%ld, table_num=%ld, fd=%p",
            table_index, table_nums_, fd);
        ret = OB_INVALID_ARGUMENT;
      }

      const ObTableSchema *table_schema = NULL;
      if (OB_SUCCESS == ret)
      {
        table_schema = table_begin() + table_index;
        if (NULL == table_schema)
        {
          TBSYS_LOG(WARN, "table_schema = %p, error here.!", table_schema);
          ret = OB_ERROR;
        }
      }
      if (OB_SUCCESS == ret)
      {
        if (0 >= (ret = fprintf(fd, "\n\n[%s]\n", table_schema->get_table_name())))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
        }
        else if (0 >= (ret = fprintf(fd, "%s=%lu\n", STR_TABLE_ID, table_schema->get_table_id())))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
        }
        else if (0 >= (ret = fprintf(fd, "%s=%d\n", STR_TABLE_TYPE, table_schema->get_table_type())))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
        }
        else if (0 >= (ret = fprintf(fd, "%s=%ld\n", STR_MAX_COLUMN_ID, table_schema->get_max_column_id())))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
        }
        else if (0 >= (ret = fprintf(fd, "%s=%s\n", STR_COMPRESS_FUNC_NAME, table_schema->get_compress_func_name())))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
        }
        else if (0 >= (ret = fprintf(fd, "%s=%d\n", STR_BLOCK_SIZE, table_schema->get_block_size())))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
        }
        else if (0 >= (ret = fprintf(fd, "%s=%d\n", STR_USE_BLOOMFILTER, table_schema->is_use_bloomfilter())))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
        }
        else if (0 >= (ret = fprintf(fd, "%s=%d\n", STR_HAS_BASELINE_DATA, table_schema->has_baseline_data())))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
        }
        else if (0 >= (ret = fprintf(fd, "%s=%ld\n", STR_EXPIRE_FREQUENCY, table_schema->get_expire_frequency())))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
        }
        else if (0 >= (ret = fprintf(fd, "%s=%d\n", STR_CONSISTENCY_LEVEL, table_schema->get_consistency_level())))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
        }
        else if (0 >= (ret = fprintf(fd, "%s=%ld\n", STR_QUERY_CACHE_EXPIRE_TIME, table_schema->get_query_cache_expire_time())))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
        }
        else if (0 >= (ret = fprintf(fd, "%s=%d\n", STR_IS_EXPIRE_EFFECT_IMMEDIATELY, table_schema->is_expire_effect_immediately())))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
        }
        else if (0 >= (ret = fprintf(fd, "%s=%ld\n", STR_MAX_SCAN_ROWS_PER_TABLET, table_schema->get_max_scan_rows_per_tablet())))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
        }
        else if (0 >= (ret = fprintf(fd, "%s=%ld\n", STR_INTERNAL_UPS_SCAN_SIZE, table_schema->get_internal_ups_scan_size())))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
        }
        else if (0 >= (ret = fprintf(fd, "%s=%s\n", STR_EXPIRE_CONDITION, table_schema->get_expire_condition())))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
        }
        else if (0 >= (ret = fprintf(fd, "%s=%ld\n", STR_MAX_SSTABLE_SIZE, table_schema->get_max_sstable_size())))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
        }
		//add zhaoqiong[roottable tablet management]20150302:b
        else if (0 >= (ret = fprintf(fd, "%s=%ld\n", STR_REPLICA_NUM, table_schema->get_replica_count())))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
        }
		//add e
        else
        {
          ret = OB_SUCCESS;
        }
      }

      //row_key
      if (OB_SUCCESS == ret)
      {
        ret = write_rowkey_info_to_file(fd, table_schema->get_table_id(), table_schema->get_rowkey_info());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to write rowkey info to file. ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        bool have_join_info = false;
        char join_buff[OB_MAX_PACKET_LENGTH]; //for join_info
        int64_t pos = 0;
        const ObColumnSchemaV2* column_schema = NULL;
        for (uint64_t i = 0; i <= table_schema->get_max_column_id(); i++)
        {
          column_schema = get_column_schema(table_schema->get_table_id(), i);
          if (NULL != column_schema)
          {
            ret = write_column_info_to_file(fd, column_schema);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "fail to write column info to file. ret=%d", ret);
              break;
            }
            else
            {
              const ObColumnSchemaV2::ObJoinInfo* join_info = column_schema->get_join_info();
              if (join_info != NULL)
              {
                const ObTableSchema *join_table_schema = get_table_schema(join_info->join_table_);
                const ObColumnSchemaV2 *join_column_schema = get_column_schema(join_info->join_table_, join_info->correlated_column_);
                if (NULL == join_table_schema || NULL == join_column_schema)
                {
                  TBSYS_LOG(WARN, "table schema =%p, column_schema=%p, table_id=%ld, column_id=%ld",
                      join_table_schema, join_column_schema, join_info->join_table_, join_info->correlated_column_);
                  ret = OB_ERROR;
                  break;
                }
                if (!have_join_info)
                {
                  have_join_info = true;
                  //[r1$jr1,r2$jr2]%joined_table_name:f1$jf1,f2$jf2,
                  pos += snprintf(join_buff + pos, OB_MAX_PACKET_LENGTH, "%s=[", STR_JOIN_RELATION);
                  for (uint64_t i = 0 ; i < join_info->left_column_count_; i ++)
                  {
                    uint64_t left_column_id = 0;
                    table_schema->get_rowkey_info().get_column_id(join_info->left_column_offset_array_[i], left_column_id);
                    const ObColumnSchemaV2 *left_column= get_column_schema(table_schema->get_table_id(), left_column_id);
                    ObRowkeyInfo join_rowkey = join_table_schema->get_rowkey_info();
                    uint64_t rowkey_column_id = 0;
                    join_rowkey.get_column_id(i, rowkey_column_id);
                    const ObColumnSchemaV2 *rowkey_column_schema = get_column_schema(join_info->join_table_, rowkey_column_id);
                    pos += snprintf(join_buff + pos, OB_MAX_PACKET_LENGTH, "%s$%s,", left_column->get_name(), rowkey_column_schema->get_name());
                  }
                  pos += snprintf(join_buff + pos - 1, OB_MAX_PACKET_LENGTH, "]%%%s:", join_table_schema->get_table_name());
                  pos = pos - 1;
                  pos += snprintf(join_buff + pos, OB_MAX_PACKET_LENGTH, "%s$%s", column_schema->get_name(), join_column_schema->get_name());
                }
                else
                {
                  pos += snprintf(join_buff + pos, OB_MAX_PACKET_LENGTH, ",%s$%s", column_schema->get_name(), join_column_schema->get_name());
                }
              }
            }
          }
        }
        join_buff[pos] = '\0';
        if (OB_SUCCESS == ret)
        {
          fprintf(fd, "%s\n", join_buff);
        }
      }
      return ret;
    }
    int ObSchemaManagerV2::write_rowkey_info_to_file(FILE *fd, const uint64_t table_id, const ObRowkeyInfo &rowkey)
    {
      int ret = OB_SUCCESS;
      char rowkey_buf[OB_MAX_PACKET_LENGTH];
      memset(rowkey_buf, '\0', OB_MAX_PACKET_LENGTH);
      int64_t pos = 0;
      pos += snprintf(rowkey_buf + pos, OB_MAX_PACKET_LENGTH, "%s=", STR_ROWKEY);
      for (int64_t i = 0; i < rowkey.get_size(); i++)
      {
        const ObRowkeyColumn *rowkey_column = rowkey.get_column(i);
        if (rowkey_column == NULL)
        {
          TBSYS_LOG(WARN, "invalid column. column point is NULL");
          ret = OB_ERROR;
          break;
        }
        uint64_t column_id = rowkey_column->column_id_;
        const ObColumnSchemaV2* column_schema = get_column_schema(table_id, column_id);
        if (column_schema == NULL)
        {
          TBSYS_LOG(WARN, "cann't find the column_schema. table_id=%ld, column_id=%ld",
              table_id, column_id);
          ret = OB_ERROR;
          break;
        }


        pos += snprintf(rowkey_buf + pos, OB_MAX_PACKET_LENGTH, "%s(%ld%%%s),",
            column_schema->get_name(), rowkey_column->length_, convert_column_type_to_str(rowkey_column->type_));
      }
      rowkey_buf[pos - 1] = '\0';
      fprintf(fd, "%s\n", rowkey_buf);
      return ret;
    }
    int ObSchemaManagerV2::write_column_info_to_file(FILE *fd, const ObColumnSchemaV2 *column_schema)
    {
      int ret = OB_SUCCESS;
      if (NULL == fd || NULL == column_schema)
      {
        TBSYS_LOG(WARN, "invalid argument. fd=%p, column_schema=%p", fd, column_schema);
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == ret)
      {
        if (ObVarcharType != column_schema->get_type())
        {
          if (0 >= (ret = fprintf(fd, "%s=%d,%ld,%s,%s\n", STR_COLUMN_INFO, column_schema->is_maintained(),
                  column_schema->get_id(),
                  column_schema->get_name(),
                  convert_column_type_to_str(column_schema->get_type()))))
          {
            TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
            ret = OB_ERROR;
          }
          else
          {
            ret = OB_SUCCESS;
          }
        }
        else
        {
          if (0 >= (ret = fprintf(fd, "%s=%d,%ld,%s,%s,%ld\n", STR_COLUMN_INFO, column_schema->is_maintained(),
                  column_schema->get_id(),
                  column_schema->get_name(),
                  convert_column_type_to_str(column_schema->get_type()),
                  column_schema->get_size())))
          {
            TBSYS_LOG(WARN, "fprintf buf content to file fail. ret=%d", ret);
            ret = OB_ERROR;
          }
          else
          {
            ret = OB_SUCCESS;
          }
        }
      }
      return ret;
    }
int ObSchemaManagerV2::change_table_id(const uint64_t table_id, const uint64_t new_table_id)
    {
      int ret = OB_SUCCESS;
      //tableschema
      ObTableSchema *table_schema = NULL;
      if (NULL == (table_schema = const_cast<ObTableSchema*>(get_table_schema(table_id))))
      {
        TBSYS_LOG(WARN, "fail to find table_schema. table_id=%ld", table_id);
        ret = OB_ERROR;
      }
      else
      {
        table_schema->set_table_id(new_table_id);
      }
      //columnschema
      //join_tables
      if (OB_SUCCESS == ret)
      {
        for (int64_t i = 0; i < column_nums_; i++)
        {
          ObColumnSchemaV2 *column = const_cast<ObColumnSchemaV2 *>(columns_ + i);
          ObColumnSchemaV2::ObJoinInfo *join_info = NULL;
          if (NULL != column)
          {
            if (NULL != (join_info = const_cast<ObColumnSchemaV2::ObJoinInfo *>(column->get_join_info())))
            {
              if (join_info->join_table_ == table_id)
              {
                join_info->join_table_ = new_table_id;
              }
            }
            if (column->get_table_id() == table_id)
            {
              column->set_table_id(new_table_id);
            }
          }
          else
          {
            TBSYS_LOG(WARN, "error happened. i=%ld, column_num=%ld", i, column_nums_);
            break;
          }
        }
      }
      //column_groups
      if (OB_SUCCESS == ret)
      {
        for (int64_t i = 0; i < column_group_nums_; i++)
        {
          if (column_groups_[i].table_id_ == table_id)
          {
            column_groups_[i].table_id_ = new_table_id;
          }
        }
      }
      //sort
      if (OB_SUCCESS == ret)
      {
        ret = sort_column();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to sort column. ert=%d", ret);
        }
      }
      return ret;
    }

//add wenghaixing[decimal] for fix delete bug 2014/10/10
int ObSchemaManagerV2::get_cond_val_info(uint64_t table_id,uint64_t column_id,ObObjType &type,uint32_t &p,uint32_t &s, int32_t* idx /*=NULL*/)const{
        int ret=OB_SUCCESS;
        const ObColumnSchemaV2 *column = NULL;

        if (OB_INVALID_ID != table_id && OB_INVALID_ID != column_id)
        {
          ObColumnIdKey k;
          ObColumnInfo v;
          int err = OB_SUCCESS;

          k.table_id_ = table_id;
          k.column_id_ = column_id;

          err = id_hash_map_.get(k,v);
          if ( -1 == err || hash::HASH_NOT_EXIST == err)
          {
          }
          else if (v.head_ != NULL)
          {
            column = v.head_;
            if (idx != NULL)
            {
              *idx = static_cast<int32_t>(column - column_begin() - v.table_begin_index_);
            }
          }
          else
          {
            TBSYS_LOG(ERROR,"found column but v.head_ is NULL");
            ret=OB_ERROR;
          }
        }
        else {
            ret=OB_ERROR;
        }

        if(ret==OB_SUCCESS){
            type=column->get_type();
            p=column->get_precision();
            s=column->get_scale();
        }
        return ret;

   }
//add e
//add zhaoqiong [Schema Manager] 20150327:b
int ObSchemaManagerV2::apply_schema_mutator(const ObSchemaMutator &mutator)
{
  int ret = OB_SUCCESS;
  if (mutator.get_refresh_schema())
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "mutator contain refresh schema operation, need get full schema");
  }
  else if (timestamp_ != mutator.get_start_version())
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN,"mutator not match current schema,timestamp=%ld, mutator start_version=%ld",
              timestamp_, mutator.get_start_version());
  }
  else if (OB_SUCCESS != (ret = drop_tables(mutator.get_droped_tables())))
  {
    TBSYS_LOG(WARN, "can not drop schema");
  }
  else if (OB_SUCCESS != (ret = add_new_table_schema(mutator.get_add_table_schema())))
  {
    TBSYS_LOG(WARN, "can not add schema");
  }
  else if (OB_SUCCESS != (ret = sort_column()))
  {
    TBSYS_LOG(WARN, "can not sort column");
  }
  //add liuxiao [secondary index static index] 20150615
  else if (OB_SUCCESS != (ret = init_index_hash()))
  {
      TBSYS_LOG(WARN, "can not init_index_hash");
  }
  //add e
  else
  {
    timestamp_ = mutator.get_end_version();
    TBSYS_LOG(INFO, "apply schema mutator(version[%ld->%ld])", mutator.get_start_version(), mutator.get_end_version());
  }
  return ret;
}

int ObSchemaManagerV2::determine_serialize_pos(int64_t table_begin_pos,int64_t column_begin_pos) const
{
  int ret = OB_SUCCESS;
  if (table_begin_pos < 0 || table_begin_pos >= table_nums_)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR,"invalid table_begin_pos, table_begin_pos=%ld,table_nums_=%ld",
              table_begin_pos, table_nums_);
  }
  else if (column_begin_pos < 0 || column_begin_pos >= column_nums_)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR,"invalid column_begin_pos, column_begin_pos=%ld,column_nums_=%ld",
              column_begin_pos, column_nums_);
  }
  else
  {
    table_begin_ = table_begin_pos;
    column_begin_ = column_begin_pos;

    int64_t len = serialization::encoded_length_i32(schema_magic_); //magic number use fixed length
    len += serialization::encoded_length_vi32(version_);
    len += serialization::encoded_length_vi64(timestamp_);
    len += serialization::encoded_length_vi64(static_cast<int64_t>(max_table_id_));
    len += serialization::encoded_length_vi64(column_nums_);
    len += serialization::encoded_length_vi64(table_nums_);
    len += serialization::encoded_length_vstr(app_name_);
    len += serialization::encoded_length_vi64(table_begin_);
    len += serialization::encoded_length_vi64(table_end_);
    len += serialization::encoded_length_vi64(column_begin_);
    len += serialization::encoded_length_vi64(column_end_);

    int64_t serialize_size = len;
    bool finished = false;
    int64_t last_table_end_pos = table_begin_;
    int64_t last_column_end_pos = column_begin_;
    uint64_t current_table_id = -1;

    for (int64_t i = last_table_end_pos; i < table_nums_; ++i)
    {
      current_table_id = table_infos_[i].get_table_id();
      len += table_infos_[i].get_serialize_size();

      for (int64_t j = last_column_end_pos; j < column_nums_; ++j)
      {
        if (columns_[j].get_table_id() == current_table_id)
        {
          len += columns_[j].get_serialize_size();
          if (len > MAX_SERIALIZE_SIZE)
          {
            finished = true;
            break;
         }
        }
        else if (j == last_column_end_pos)
        {
          TBSYS_LOG(ERROR, "error occurs,table[%ld] has no columns",current_table_id);
        }
        else
        {
         serialize_size = len;
         last_column_end_pos = j;
         break;
        }
      }//end column for
      if (finished)
      {
        table_end_ = last_table_end_pos;
        column_end_ = last_column_end_pos;
        TBSYS_LOG(INFO, "wait for serialization in next round, table index begins at %ld,column index begins at %ld,serialize_size=%ld ",
                  table_end_,column_end_, serialize_size);
        break;
      }
      else
      {
        last_table_end_pos ++;
      }
    }//end for table

    if (!finished)
    {
      if (last_table_end_pos != table_nums_)
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "error occurs,totally serialize %ld tables, not equal to table_nums_=%ld",
                  last_table_end_pos,table_nums_);
      }
      else
      {
        serialize_size = len;
        table_end_ = table_nums_;
        column_end_ = column_nums_;
        TBSYS_LOG(INFO, " finish serialization in current round, serialize %ld tables, %ld columns in total, serialize_size=%ld",
                  table_end_,column_end_, serialize_size);
      }
    }
  }

  return ret;
}

void ObSchemaManagerV2::set_serialize_whole() const
{
  table_begin_ = 0;
  column_begin_ = 0;
  table_end_ = table_nums_;
  column_end_ = column_nums_;
}
int ObSchemaManagerV2::prepare_for_next_serialize()
{
  int ret = OB_SUCCESS;
  if (table_end_ <= 0 || column_end_ <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR,"maybe not after first serialize, this func should use after first serialize, table_end_=%ld,column_end_=%ld",
              table_end_, column_end_);
  }
  else if (OB_SUCCESS != (ret = determine_serialize_pos(table_end_,column_end_)))
  {
    TBSYS_LOG(ERROR,"determine_serialize_pos error");
  }
  return ret;
}

bool ObSchemaManagerV2::need_next_serialize() const
{
  return (table_end_ < table_nums_ ? true : false);
}

/*-----------------------------------------------------------------------------
 *  ObSchemaMutator
 *-----------------------------------------------------------------------------*/
ObSchemaMutator::ObSchemaMutator():start_version_(0),end_version_(0),refresh_new_schema_(false)
{
}

ObSchemaMutator::~ObSchemaMutator()
{
}

void ObSchemaMutator::set_version_range(int64_t start_version,int64_t end_version)
{
  start_version_ = start_version;
  end_version_ = end_version;
}

DEFINE_SERIALIZE(ObSchemaMutator)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;

  ret = serialization::encode_bool(buf, buf_len, tmp_pos, refresh_new_schema_);
  if (OB_SUCCESS != ret || refresh_new_schema_)
  {
    //refresh_new_schema_ need fetch full schema, do not need other info
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, tmp_pos, start_version_)))
  {
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, tmp_pos, end_version_)))
  {
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, tmp_pos, droped_tables_.count())))
  {
  }
  else
  {
    for (int64_t i = 0; OB_SUCCESS == ret && i < droped_tables_.count(); ++i)
    {
      if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, tmp_pos, droped_tables_.at(i))))
      {
        TBSYS_LOG(WARN, "failed to serialize column, err=%d", ret);
        break;
      }
    }
  }
  if (OB_SUCCESS != ret || refresh_new_schema_)
  {
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, tmp_pos, add_table_schema_.count())))
  {
  }
  else
  {
    for (int64_t i = 0; OB_SUCCESS == ret && i < add_table_schema_.count(); ++i)
    {
      if (OB_SUCCESS != (ret = add_table_schema_.at(i).serialize(buf, buf_len, tmp_pos)))
      {
        TBSYS_LOG(WARN, "failed to serialize column, err=%d", ret);
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

DEFINE_DESERIALIZE(ObSchemaMutator)
{
  int ret = OB_SUCCESS;
  int64_t drop_table_count = 0;
  int64_t create_table_count = 0;

  ret = serialization::decode_bool(buf, data_len, pos, &refresh_new_schema_);

  if (OB_SUCCESS != ret || refresh_new_schema_)
  {

  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &start_version_)))
  {
    TBSYS_LOG(WARN, "deserialize error here");
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &end_version_)))
  {
    TBSYS_LOG(WARN, "deserialize error here");
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &drop_table_count)))
  {
    TBSYS_LOG(WARN, "deserialize error here");
  }
  else
  {
    int64_t droped_table_id;
    for (int64_t i = 0; i < drop_table_count; ++i)
    {
      if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &droped_table_id)))
      {
        TBSYS_LOG(WARN, "failed to deserialize column, err=%d", ret);
        break;
      }
      else if (OB_SUCCESS != (ret = droped_tables_.push_back(droped_table_id)))
      {
        TBSYS_LOG(WARN, "failed to push into array, err=%d", ret);
        break;
      }
    }
  }

  if (OB_SUCCESS != ret || refresh_new_schema_)
  {
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &create_table_count)))
  {
    TBSYS_LOG(WARN, "deserialize error here");
  }
  else
  {
    for (int64_t i = 0; i < create_table_count; ++i)
    {
      TableSchema table_schema;
      if (OB_SUCCESS != (ret = table_schema.deserialize(buf, data_len, pos)))
      {
        TBSYS_LOG(WARN, "failed to deserialize column, err=%d", ret);
        break;
      }
      else if (OB_SUCCESS != (ret = add_table_schema_.push_back(table_schema)))
      {
        TBSYS_LOG(WARN, "failed to push into array, err=%d", ret);
        break;
      }
    }
  }
  return ret;
}
//add:e
  } // end namespace common
}   // end namespace oceanbase
