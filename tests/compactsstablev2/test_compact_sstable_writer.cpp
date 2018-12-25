#include "gtest/gtest.h"
#include "common/ob_define.h"
#include "compactsstablev2/ob_compact_sstable_writer.h"
#include "common/compress/ob_compressor.h"

using namespace oceanbase;
using namespace common;
using namespace compactsstablev2;

class TestCompactSSTableWriter : public ::testing::Test
{
public:
  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }

  /**
   *make version range
   *@param version_range:version_range
   *@param version_flag:0(vaild version)
   *                    1(invaild major frozen time)
   *                    2(invalid major transaction id)
   *                    3(invalid next commit log id)
   *                    4(invalid start minor version)
   *                    5(invalid end minor version)
   *                    6(invalid is final minor version)
   *                    7(invalid major version)
   */
  int make_version_range(const ObCompactStoreType row_store_type,
      ObFrozenMinorVersionRange& version_range,
      const int version_flag)
  {
    int ret = OB_SUCCESS;

    if (DENSE_SPARSE == row_store_type)
    {
      version_range.major_version_ = 10;
      version_range.major_frozen_time_ = 2;
      version_range.next_transaction_id_ = 3;
      version_range.next_commit_log_id_ = 4;
      version_range.start_minor_version_ = 5;
      version_range.end_minor_version_ = 6;
      version_range.is_final_minor_version_ = 1;
    }
    else if (DENSE_DENSE == row_store_type)
    {
      version_range.reset();
      version_range.major_version_ = 10;
    }

    if (0 == version_flag)
    {
    }
    else if (1 == version_flag)
    {
      version_range.major_frozen_time_ = -1;
    }
    else if (2 == version_flag)
    {
      version_range.major_frozen_time_ = -1;
    }
    else if (3 == version_flag)
    {
      version_range.next_transaction_id_ = -1;
    }
    else if (4 == version_flag)
    {
      version_range.next_commit_log_id_ = -1;
    }
    else if (5 == version_flag)
    {
      version_range.start_minor_version_ = -1;
    }
    else if (6 == version_flag)
    {
      version_range.is_final_minor_version_ = -1;
    }
    else if (7 == version_flag)
    {
      version_range.major_version_ = -1;
    }
    else
    {
      TBSYS_LOG(ERROR, "invalid version flag:version_flag=%d",
          version_flag);
      ret = OB_ERROR;
    }

    return ret;
  }

  /**
   *make compressor name
   *@param comp_name:compressor name
   *@param comp_flag:0(not compress),1(invalid),2(lzo_1.0),
   *                 3(snappy),4(none)
   */
  int make_comp_name(ObString& comp_name, const int comp_flag)
  {
    int ret = OB_SUCCESS;
    static char s_comp_name_buf[1024];
    memset(s_comp_name_buf, 0, 1024);

    if (comp_flag < 0 || comp_flag > 4)
    {
      TBSYS_LOG(ERROR, "invlid comp_flag:comp_flag=%d", comp_flag);
      ret = OB_ERROR;
    }
    else if (0 == comp_flag)
    {
      comp_name.assign_ptr(NULL, 0);
    }
    else if (1 == comp_flag)
    {
      memcpy(s_comp_name_buf, "invalid", 10);
      comp_name.assign_ptr(s_comp_name_buf, 10);
    }
    else if (2 == comp_flag)
    {
      memcpy(s_comp_name_buf, "lzo_1.0", 10);
      comp_name.assign_ptr(s_comp_name_buf, 10);
    }
    else if (3 == comp_flag)
    {
      memcpy(s_comp_name_buf, "snappy", 10);
      comp_name.assign_ptr(s_comp_name_buf, 10);
    }
    else if (4 == comp_flag)
    {
      memcpy(s_comp_name_buf, "none", 10);
      comp_name.assign_ptr(s_comp_name_buf, 10);
    }
    else
    {
      TBSYS_LOG(ERROR, "invlid comp_flag:comp_flag=%d", comp_flag);
      ret = OB_ERROR;
    }

    return ret;
  }

  /**
   * create compressor/decompressor
   * @param comp_flag:0(not compress),1(invalid),2(lzo_1.0),3(snappy),
   *                  4(none)
   * TODO: release the compressor
   */
  int make_compressor(ObCompressor*& compressor, const int comp_flag)
  {
    int ret = OB_SUCCESS;
    static ObCompressor* compressor_lzo = NULL;
    static ObCompressor* compressor_none = NULL;

    if (NULL != compressor_lzo)
    {
      destroy_compressor(compressor_lzo);
    }

    compressor_lzo = create_compressor("lzo_1.0");
    if (NULL == compressor_lzo)
    {
      TBSYS_LOG(ERROR, "create compressor error:NULL==compressor_");
      ret = OB_ERROR;
    }

    if (NULL != compressor_none)
    {
      destroy_compressor(compressor_none);
    }

    compressor_none = create_compressor("none");
    if (NULL == compressor_none)
    {
      TBSYS_LOG(ERROR, "create compressor error:NULL==compressor_");
      ret = OB_ERROR;
    }

    if (OB_SUCCESS == ret)
    {
      if (1 == comp_flag)
      {
        compressor = NULL;
      }
      else if (2 == comp_flag)
      {
        compressor = compressor_lzo;
      }
      else if (4 == comp_flag)
      {
        compressor = compressor_none;
      }
      else
      {
        TBSYS_LOG(ERROR, "not support:comp_flag=%d", comp_flag);
        ret = OB_ERROR;
      }
    }

    return ret;
  }

  /**
   *make file path
   *@param file_path:file path
   *@param file_num:>=0(valid filename),-1(invalid filename)
   *                -2(empty filepath)
   */
  int make_file_path(ObString& file_path, const int64_t file_num)
  {
    int ret = OB_SUCCESS;

    static char s_file_path_buf[1024];
    memset(s_file_path_buf, 0, 1024);

    if (file_num >= 0)
    {
      snprintf(s_file_path_buf, 1024, "%ld%s", file_num, ".sst");
      file_path.assign_ptr(s_file_path_buf, 10);
    }
    else if (-1 == file_num)
    {
      snprintf(s_file_path_buf, 1024, "tt/1.sst");
      file_path.assign_ptr(s_file_path_buf, 10);
    }
    else if (-2 == file_num)
    {
      file_path.assign_ptr(s_file_path_buf, 0);
    }
    else
    {
      TBSYS_LOG(ERROR, "invalid file_num:file_num=%ld", file_num);
      ret = OB_ERROR;
    }

    return ret;
  }

  /**
   *make column def
   *@param def: column def
   *@param table_id: table id
   *@param column_id: column id
   *@param value_type: value type
   *@param rowkey_seq: rowkey seq
   */
  void make_column_def(ObSSTableSchemaColumnDef& def,
      const uint64_t table_id, const uint64_t column_id,
      const int value_type, const int64_t rowkey_seq)
  {
    def.table_id_ = table_id;
    def.column_id_ = column_id;
    def.column_value_type_ = value_type;
    def.rowkey_seq_ = rowkey_seq;
  }

  /**
   *make schema
   *@param shcema: schema
   *@param table_id: table id
   *@param flag:
   *   --0(rowkey<2-11>,rowvalue<12-21>,ObIntType)
   */
  void make_schema(compactsstablev2::ObSSTableSchema& schema,
      const uint64_t table_id, const int flag)
  {
    int ret = OB_SUCCESS;
    schema.reset();
    ObSSTableSchemaColumnDef def;

    if (0 == flag)
    {
      for (int64_t j = 0; j < 10; j ++)
      {
        //rowkey seq:1---10
        //column id:2----11
        make_column_def(def, table_id,
            static_cast<uint64_t>(j + 2), ObIntType, j + 1);
        ret = schema.add_column_def(def);
        ASSERT_EQ(OB_SUCCESS, ret);
      }

      for (int64_t j = 10; j < 20; j ++)
      {
        //rowkey seq:0
        //column id:12----21
        make_column_def(def, table_id, static_cast<uint64_t>(j + 2),
            ObIntType, 0);
        ret = schema.add_column_def(def);
        ASSERT_EQ(OB_SUCCESS, ret);
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "invalid flag:flag=%d", flag);
      ret = OB_ERROR;
    }
  }

  /**
   *make range
   *@param range: table range
   *@param table_id: table id
   *@param start: stard key id
   *@param end: end key id
   *@param start_flag: 0(min),-1(uninclusive start),-2(inclusvie start)
   *@param end_flag: 0(max),-1(uninclusive end),-2(inclusive end)
   */
  int make_range(ObNewRange& range, const uint64_t table_id,
      const int64_t start, const int64_t end,
      const int start_flag, const int end_flag)
  {
    int ret = OB_SUCCESS;

    static ObObj s_startkey_buf_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
    static ObObj s_endkey_buf_array[OB_MAX_ROWKEY_COLUMN_NUMBER];

    ObObj obj;
    int64_t remain = 0;
    int64_t yushu = 0;

    //range.table_id_
    range.reset();
    range.table_id_ = table_id;

    //range.start_key_
    if (0 == start_flag)
    {
      range.start_key_.set_min_row();
      range.border_flag_.unset_inclusive_start();
      range.border_flag_.set_min_value();
    }
    else
    {
      remain = start;
      for (int64_t i = 0; i < 10; i ++)
      {
        yushu = remain % 10;
        obj.set_int(yushu);
        s_startkey_buf_array[12 - 3 - i] = obj;
        remain = (remain - yushu) / 10;
      }

      if (-1 == start_flag)
      {
        range.border_flag_.unset_inclusive_start();
      }
      else if (-2 == start_flag)
      {
        range.border_flag_.set_inclusive_start();
      }
      else
      {
        TBSYS_LOG(ERROR, "invalid end_flag:start_flag=%d", start_flag);
        ret = OB_ERROR;
      }

      range.start_key_.assign(s_startkey_buf_array, 10);
    }

    //end key
    if (0 == end_flag)
    {
      range.end_key_.set_max_row();
      range.border_flag_.unset_inclusive_end();
      range.border_flag_.set_max_value();
    }
    else
    {
      remain = end;
      for (int64_t i = 0; i < 10; i ++)
      {
        yushu = remain % 10;
        obj.set_int(yushu);
        s_endkey_buf_array[12 - 3 - i] = obj;
        remain = (remain - yushu) / 10;
      }

      if (-1 == end_flag)
      {
        range.border_flag_.unset_inclusive_end();
      }
      else if (-2 == end_flag)
      {
        range.border_flag_.set_inclusive_end();
      }
      else
      {
        TBSYS_LOG(ERROR, "invalid end_flag:end_flag=%d", end_flag);
        ret = OB_ERROR;
      }
      range.end_key_.assign(s_endkey_buf_array, 10);
    }

    return ret;
  }

  /**
   *make rowkey
   *@param rowkey: rowkey
   *@param data: rowkey num
   *@param flag: 0(normal), 1(min), 2(max)
   */
  int make_rowkey(ObRowkey& rowkey, const int64_t data,
      const int flag = 0)
  {
    int ret = OB_SUCCESS;

    static ObObj s_rowkey_buf_array[OB_MAX_ROWKEY_COLUMN_NUMBER];

    int64_t remain = 0;
    int64_t yushu = 0;
    ObObj obj;

    if (0 == flag)
    {
      remain = data;
      for (int64_t i = 0; i < 10; i ++)
      {
        yushu = remain % 10;
        obj.set_int(yushu);
        s_rowkey_buf_array[12 - 3 - i] = obj;
        remain = (remain - yushu) / 10;
      }
      rowkey.assign(s_rowkey_buf_array, 10);
    }
    else if (1 == flag)
    {
      rowkey.set_min_row();
    }
    else if (2 == flag)
    {
      rowkey.set_max_row();
    }
    else
    {
      TBSYS_LOG(ERROR, "invalid flag:flag=%d", flag);
      ret = OB_ERROR;
    }

    return ret;
  }

  /**
   *make row
   *@param row: row
   *@param table_id: table id
   *@param data: rowkey num
   *@param flag: 0(normal,write),1(del row,write),2(no row value,write)
   *             3(del row,scan,single obj),4(del row,scan,mult obj),
   *             5(del row,write,mult obj),6(no row value,scan),
   *             7(null, scan)
   */
  void make_row(ObRow& row, const uint64_t table_id, const int64_t data,
      const int flag)
  {
    int ret = OB_SUCCESS;
    static ObRowDesc s_desc;
    s_desc.reset();

    ObObj obj;
    int64_t remain = 0;
    int64_t yushu = 0;
    int64_t i = 0;

    if (0 == flag)
    {//normal
      for (i = 0; i < 20; i ++)
      {
        ret = s_desc.add_column_desc(table_id, i + 2);
        ASSERT_EQ(OB_SUCCESS, ret);
      }
      s_desc.set_rowkey_cell_count(10);
      row.set_row_desc(s_desc);

      remain = data;
      for (i = 0; i < 10; i ++)
      {
        yushu = remain % 10;
        obj.set_int(yushu);
        ret = row.set_cell(table_id, 12 - 1 - i, obj);
        ASSERT_EQ(OB_SUCCESS, ret);
        remain = (remain - yushu) / 10;
      }

      for (i = 10; i < 20; i ++)
      {
        obj.set_int(i - 10);
        ret = row.set_cell(table_id, i + 2, obj);
        ASSERT_EQ(OB_SUCCESS, ret);
      }
    }
    else if (1 == flag)
    {//del row
      for (i = 0; i < 10; i ++)
      {
        ret = s_desc.add_column_desc(table_id, i + 2);
        ASSERT_EQ(OB_SUCCESS, ret);
      }
      ret = s_desc.add_column_desc(table_id, OB_ACTION_FLAG_COLUMN_ID);
      ASSERT_EQ(OB_SUCCESS, ret);
      s_desc.set_rowkey_cell_count(10);
      row.set_row_desc(s_desc);

      remain = data;
      for (i = 0; i < 10; i ++)
      {
        yushu = remain % 10;
        obj.set_int(yushu);
        ret = row.set_cell(table_id, 12 - 1 - i, obj);
        ASSERT_EQ(OB_SUCCESS, ret);
        remain = (remain - yushu) / 10;
      }

      obj.set_ext(ObActionFlag::OP_DEL_ROW);
      ret = row.set_cell(table_id, OB_ACTION_FLAG_COLUMN_ID, obj);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    else if (2 == flag)
    {//empty rowvalue
      for (i = 0; i < 10; i ++)
      {
        ret = s_desc.add_column_desc(table_id, i + 2);
        ASSERT_EQ(OB_SUCCESS, ret);
      }
      s_desc.set_rowkey_cell_count(10);
      row.set_row_desc(s_desc);

      remain = data;
      for (i = 0; i < 10; i ++)
      {
        yushu = remain % 10;
        obj.set_int(yushu);
        ret = row.set_cell(table_id, 12 - 1 - i, obj);
        ASSERT_EQ(OB_SUCCESS, ret);
        remain = (remain - yushu) / 10;
      }
    }
    else if (3 == flag)
    {//del row(scan)
      ret = s_desc.add_column_desc(table_id, OB_ACTION_FLAG_COLUMN_ID);
      ASSERT_EQ(OB_SUCCESS, ret);
      s_desc.set_rowkey_cell_count(0);
      row.set_row_desc(s_desc);

      obj.set_ext(ObActionFlag::OP_DEL_ROW);
      ret = row.set_cell(table_id, OB_ACTION_FLAG_COLUMN_ID, obj);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    else if (4 == flag)
    {//del row(scan, mult obj)
      for (i = 10; i < 20; i ++)
      {
        ret = s_desc.add_column_desc(table_id, i + 2);
        ASSERT_EQ(OB_SUCCESS, ret);
      }
      ret = s_desc.add_column_desc(table_id, OB_ACTION_FLAG_COLUMN_ID);
      ASSERT_EQ(OB_SUCCESS, ret);

      s_desc.set_rowkey_cell_count(0);
      row.set_row_desc(s_desc);

      for (i = 10; i < 20; i ++)
      {
        obj.set_int(i - 10);
        ret = row.set_cell(table_id, i + 2, obj);
        ASSERT_EQ(OB_SUCCESS, ret);
      }

      obj.set_ext(ObActionFlag::OP_DEL_ROW);
      ret = row.set_cell(table_id, OB_ACTION_FLAG_COLUMN_ID, obj);
    }
    else if (5 == flag)
    {//del row(write, multi obj)
      for (i = 0; i < 10; i ++)
      {
        ret = s_desc.add_column_desc(table_id, i + 2);
        ASSERT_EQ(OB_SUCCESS, ret);
      }

      ret = s_desc.add_column_desc(table_id, OB_ACTION_FLAG_COLUMN_ID);
      ASSERT_EQ(OB_SUCCESS, ret);

      for (i = 10; i < 20; i ++)
      {
        ret = s_desc.add_column_desc(table_id, i + 2);
        ASSERT_EQ(OB_SUCCESS, ret);
      }

      s_desc.set_rowkey_cell_count(10);
      row.set_row_desc(s_desc);

      remain = data;
      for (i = 0; i < 10; i ++)
      {
        yushu = remain % 10;
        obj.set_int(yushu);
        ret = row.set_cell(table_id, 12 - 1 - i, obj);
        ASSERT_EQ(OB_SUCCESS, ret);
        remain = (remain - yushu) / 10;
      }

      obj.set_ext(ObActionFlag::OP_DEL_ROW);
      ret = row.set_cell(table_id, OB_ACTION_FLAG_COLUMN_ID, obj);

      for (i = 10; i < 20; i ++)
      {
        obj.set_int(i - 10);
        ret = row.set_cell(table_id, i + 2, obj);
        ASSERT_EQ(OB_SUCCESS, ret);
      }

      ASSERT_EQ(OB_SUCCESS, ret);
    }
    else if (6 == flag)
    {//empty row scan
      for (i = 10; i < 20; i ++)
      {
        ret = s_desc.add_column_desc(table_id, i + 2);
        ASSERT_EQ(OB_SUCCESS, ret);
      }

      s_desc.set_rowkey_cell_count(0);
      row.set_row_desc(s_desc);

      for (i = 10; i < 20; i ++)
      {
        obj.set_ext(ObActionFlag::OP_NOP);
        ret = row.set_cell(table_id, i + 2, obj);
        ASSERT_EQ(OB_SUCCESS, ret);
      }
    }
    else if (7 == flag)
    {
      ret = s_desc.add_column_desc(table_id, 32);
      ASSERT_EQ(OB_SUCCESS, ret);

      s_desc.set_rowkey_cell_count(0);
      row.set_row_desc(s_desc);

      obj.set_null();
      ret = row.set_cell(table_id, 32, obj);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    else
    {
      TBSYS_LOG(ERROR, "invalid flag:flag=%d", flag);
      ret = OB_ERROR;
    }
  }

  /**
   * delete file
   * @param i: file id
   */
  void delete_file(const int64_t i)
  {
    ObString file_path;
    make_file_path(file_path, i);
    remove(file_path.ptr());
  }

  /**
   * check block
   * @param fd: file fd
   * @param compressor: compressor
   * @param table_id: table id
   * @param start_num: start num
   * @param row_count: row count
   * @param size: file seek size
   */
  void check_block(const int fd, ObCompressor* compressor,
      const ObCompactStoreType store_type,
      const uint64_t table_id, const int64_t start_num,
      const int64_t row_count, int64_t& size)
  {
    int ret = OB_SUCCESS;

    ObRecordHeaderV2 record_header;
    ObRecordHeaderV2* record_header_ptr = NULL;
    ObSSTableBlockHeader* block_header_ptr = NULL;
    ObSSTableBlockRowIndex* row_index_ptr = NULL;

    int64_t rowkey_len = 21;
    int64_t rowvalue_len = 0;
    int64_t total_len = 0;
    if (DENSE_SPARSE == store_type)
    {
      rowvalue_len = 61;
    }
    else if (DENSE_DENSE == store_type)
    {
      rowvalue_len = 21;
    }
    total_len = rowkey_len + rowvalue_len;

    int64_t read_size = 0;
    const char* payload_ptr = NULL;
    int64_t payload_size = 0;
    int64_t pos = 0;
    char comp_buf[1024 * 1024];
    char uncomp_buf[1024 * 1024];
    int64_t uncomp_len = 0;

    char cur_row_buf[1024];
    ObCompactCellWriter cur_row;
    ObRow row;
    int64_t row_offset = 0;
    int64_t cur_row_num = start_num;

    read_size = read(fd, comp_buf, sizeof(ObRecordHeaderV2));
    ASSERT_EQ(static_cast<int64_t>(sizeof(ObRecordHeaderV2)), read_size);
    record_header_ptr = (ObRecordHeaderV2*)(comp_buf);

    ASSERT_EQ(static_cast<int16_t>(OB_SSTABLE_BLOCK_DATA_MAGIC),
        record_header_ptr->magic_);
    ASSERT_EQ(static_cast<int16_t>(sizeof(ObRecordHeaderV2)),
        record_header_ptr->header_length_);
    ASSERT_EQ(0x2, record_header_ptr->version_);
    ASSERT_EQ(0, record_header_ptr->reserved16_);
    ASSERT_EQ(static_cast<int64_t>(total_len * row_count + 4 * (row_count + 1)
        + sizeof(ObSSTableBlockHeader)),
        record_header_ptr->data_length_);

    size = sizeof(ObRecordHeaderV2) + record_header_ptr->data_zlength_;

    lseek(fd, (-1)*sizeof(ObRecordHeaderV2), SEEK_CUR);
    read_size = read(fd, comp_buf, size);
    ASSERT_EQ(read_size, size);

    ret = ObRecordHeaderV2::check_record(comp_buf,
        size, OB_SSTABLE_BLOCK_DATA_MAGIC, record_header,
        payload_ptr, payload_size);
    ASSERT_EQ(OB_SUCCESS, ret);

    if (NULL == compressor)
    {
      memcpy(uncomp_buf, payload_ptr, payload_size);
    }
    else
    {
      ret = compressor->decompress(payload_ptr, payload_size, uncomp_buf,
          1024 * 1024, uncomp_len);
      ASSERT_EQ(OB_SUCCESS, ret);
    }

    block_header_ptr = (ObSSTableBlockHeader*)(uncomp_buf);
    //ASSERT_EQ(total_len * row_count + sizeof(ObSSTableBlockHeader),
    //    block_header_ptr->row_index_offset_);
    ASSERT_EQ(row_count, block_header_ptr->row_count_);
    ASSERT_EQ(0, block_header_ptr->reserved64_);
    pos += sizeof(ObSSTableBlockHeader);

    int row_flag = 0;
    for (int64_t i = 0; i < row_count; i ++)
    {
      cur_row.init(cur_row_buf, 1024, store_type);
      make_row(row, table_id, cur_row_num, row_flag);
      cur_row_num ++;
      ret = cur_row.append_row(row);
      ASSERT_EQ(OB_SUCCESS, ret);

      cur_row.reset();
      for (int64_t j = 0; j < total_len; j ++)
      {
        ASSERT_EQ(cur_row_buf[j], uncomp_buf[pos + j]);
      }
      pos += total_len;
    }

    row_offset = 1 * sizeof(ObSSTableBlockHeader);
    for (int64_t j = 0; j < row_count; j ++)
    {
      row_index_ptr = (ObSSTableBlockRowIndex*)(uncomp_buf + pos);
      ASSERT_EQ(row_offset, row_index_ptr->row_offset_);

      row_offset += total_len;
      pos += sizeof(ObSSTableBlockRowIndex);
    }

    row_index_ptr = (ObSSTableBlockRowIndex*)(uncomp_buf + pos);
    ASSERT_EQ(row_offset, row_index_ptr->row_offset_);
  }

  /**
   * check block index
   * @param fd: file id
   * @param store_type: store type
   * @param block_cnt: block count
   * @param block_flag: (0)
   * @param row_count: last block row count
   * @param size: file seek size
   */
  void check_block_index(const int fd,
      const ObCompactStoreType store_type, const int64_t block_cnt,
      const int64_t block_flag, const int64_t row_count, int64_t& size)
  {
    (void) block_flag;
    int ret = OB_SUCCESS;

    ObRecordHeaderV2 record_header;
    ObRecordHeaderV2* record_header_ptr = NULL;
    ObSSTableBlockIndex* block_index_ptr = NULL;

    int64_t rowkey_len = 21;
    int64_t rowvalue_len = 0;
    int64_t total_len = 0;
    int64_t block_row_count = 0;
    if (DENSE_SPARSE == store_type)
    {
      rowvalue_len = 61;
      block_row_count = 12;
    }
    else if (DENSE_DENSE == store_type)
    {
      rowvalue_len = 21;
      block_row_count = 24;
    }
    total_len = rowkey_len + rowvalue_len;

    int64_t read_size = 0;
    const char* payload_ptr = NULL;
    int64_t payload_size = 0;
    int64_t pos = 0;
    char comp_buf[1024 * 1024];
    char uncomp_buf[10 * 1024 * 1024];

    int64_t data_offset = 0;
    int64_t endkey_offset = 0;

    read_size = read(fd, comp_buf, sizeof(ObRecordHeaderV2));
    ASSERT_EQ(static_cast<int64_t>(sizeof(ObRecordHeaderV2)), read_size);
    record_header_ptr = (ObRecordHeaderV2*)(comp_buf);

    ASSERT_EQ(static_cast<int16_t>(OB_SSTABLE_BLOCK_INDEX_MAGIC),
        record_header_ptr->magic_);
    ASSERT_EQ(static_cast<int16_t>(sizeof(ObRecordHeaderV2)),
        record_header_ptr->header_length_);
    ASSERT_EQ(0x2, record_header_ptr->version_);
    ASSERT_EQ(0, record_header_ptr->reserved16_);
    ASSERT_EQ(static_cast<int64_t>((block_cnt + 1) * sizeof(ObSSTableBlockIndex)),
        record_header_ptr->data_length_);

    size = sizeof(ObRecordHeaderV2) + record_header_ptr->data_zlength_;

    lseek(fd, (-1)*sizeof(ObRecordHeaderV2), SEEK_CUR);
    read_size = read(fd, comp_buf, size);
    ASSERT_EQ(read_size, size);

    ret = ObRecordHeaderV2::check_record(comp_buf,
        size, OB_SSTABLE_BLOCK_INDEX_MAGIC, record_header,
        payload_ptr, payload_size);
    ASSERT_EQ(OB_SUCCESS, ret);

    memcpy(uncomp_buf, payload_ptr, payload_size);

    for (int64_t i = 0; i < block_cnt - 1; i ++)
    {
      block_index_ptr = (ObSSTableBlockIndex*)(uncomp_buf + pos);
      //ASSERT_EQ(data_offset, block_index_ptr->block_data_offset_);
      ASSERT_EQ(endkey_offset, block_index_ptr->block_endkey_offset_);
      data_offset += total_len * (block_row_count)
        + 4 * (block_row_count + 1)
        + sizeof(ObSSTableBlockHeader) + sizeof(ObRecordHeaderV2);
      endkey_offset += 21;
      pos += sizeof(ObSSTableBlockIndex);
    }

    block_index_ptr = (ObSSTableBlockIndex*)(uncomp_buf + pos);
    //ASSERT_EQ(data_offset, block_index_ptr->block_data_offset_);
    ASSERT_EQ(endkey_offset, block_index_ptr->block_endkey_offset_);
    data_offset += total_len * (row_count) + 4 * (row_count + 1)
      + sizeof(ObSSTableBlockHeader) + sizeof(ObRecordHeaderV2);
    endkey_offset += 21;
    pos += sizeof(ObSSTableBlockIndex);

    block_index_ptr = (ObSSTableBlockIndex*)(uncomp_buf + pos);
    //ASSERT_EQ(data_offset, block_index_ptr->block_data_offset_);
    ASSERT_EQ(endkey_offset, block_index_ptr->block_endkey_offset_);
  }

  /**
   * check sstable file
   * @param fd: file fd
   * @param compressor: compressor
   * @param start_row_num: start row num
   * @param schema: schema
   * @param version_range: version range
   * @param range_start_num: range start num
   * @param range_end_num: range end num
   * @param range_start_flag:
   * ---0(min),-1(uninclusive start),-2(inclusive start)
   * ---0(max),-1(uninclusive end),-2(inclusive end)
   * @param store_type: store type
   * @param table_id: table id
   * @param table_count: table count
   * @param block_size: block size
   */
  void check_sstable_file(const int fd, ObCompressor* compressor,
      const ObString& compressor_str,
      const int64_t* start_row_num,
      const ObSSTableSchema& schema,
      const ObFrozenMinorVersionRange& version_range,
      const int64_t* range_start_num, const int64_t* range_end_num,
      const int* range_start_flag, const int* range_end_flag,
      const ObCompactStoreType& store_type,
      const uint64_t* table_id, const int64_t table_count,
      const int64_t block_size)
  {
    int ret = OB_SUCCESS;

    int64_t file_size = 0;
    ObRecordHeaderV2 record_header;
    ObSSTableTrailerOffset trailer_offset;
    ObSSTableHeader sstable_header;
    ObSSTableTableIndex* table_index_ptr = NULL;
    ObSSTableBlockIndex** block_index_ptr = NULL;
    ObSSTableBlockHeader* block_header_ptr = NULL;
    ObSSTableBlockRowIndex* row_index_ptr = NULL;
    char** block_endkey_ptr = NULL;

    //char buf[2 * 1024 * 1024];
    char* buf = (char*)ob_malloc(10 * 1024 * 1024, ObModIds::TEST);
    int64_t read_offset = 0;
    int64_t read_size = 0;
    int64_t real_read_size = 0;
    const char* payload_ptr = NULL;
    int64_t payload_size = 0;
    //char uncomp_buf[1024 * 1024];
    char* uncomp_buf = (char*)ob_malloc(1024 * 1024, ObModIds::TEST);
    int64_t uncomp_buf_size = 1024 * 1024;
    int64_t uncomp_buf_len = 0;
    int64_t ii = 0;

    int64_t cur_table_offset = 0;
    char cur_row_buf[1024];
    ObCompactCellWriter row_writer;
    row_writer.init(cur_row_buf, 1024, store_type);
    ObRow row;
    ObRowkey rowkey;

    //sstable size
    file_size = get_file_size(fd);
    ASSERT_NE(0, file_size);

    //sstable trailer offset
    read_offset = file_size - sizeof(ObSSTableTrailerOffset);
    read_size = sizeof(ObSSTableTrailerOffset);
    lseek(fd, read_offset, SEEK_SET);
    real_read_size = read(fd, buf, read_size);
    ASSERT_EQ(read_size, real_read_size);
    memcpy(&trailer_offset, buf, read_size);
    ASSERT_EQ(static_cast<int64_t>(file_size - sizeof(ObSSTableTrailerOffset)
        - sizeof(ObSSTableHeader) - sizeof(ObRecordHeaderV2)),
        trailer_offset.offset_);

    //sstable header
    read_offset = trailer_offset.offset_;
    read_size = sizeof(ObSSTableHeader) + sizeof(ObRecordHeaderV2);
    lseek(fd, read_offset, SEEK_SET);
    real_read_size = read(fd, buf, read_size);
    ret = ObRecordHeaderV2::check_record(buf, real_read_size,
        OB_SSTABLE_HEADER_MAGIC, record_header,
        payload_ptr, payload_size);
    ASSERT_EQ(OB_SUCCESS, ret);
    memcpy(&sstable_header, payload_ptr, payload_size);
    ASSERT_EQ(static_cast<int64_t>(sizeof(ObSSTableHeader)), sstable_header.header_size_);
    ASSERT_EQ(1 * ObSSTableHeader::SSTABLE_HEADER_VERSION,
        sstable_header.header_version_);
    ASSERT_EQ(version_range.major_version_,
        sstable_header.major_version_);
    ASSERT_EQ(version_range.major_frozen_time_,
        sstable_header.major_frozen_time_);
    ASSERT_EQ(version_range.next_transaction_id_,
        sstable_header.next_transaction_id_);
    ASSERT_EQ(version_range.next_commit_log_id_,
        sstable_header.next_commit_log_id_);
    ASSERT_EQ(version_range.start_minor_version_,
        sstable_header.start_minor_version_);
    ASSERT_EQ(version_range.end_minor_version_,
        sstable_header.end_minor_version_);
    ASSERT_EQ(version_range.is_final_minor_version_,
        sstable_header.is_final_minor_version_);
    ASSERT_EQ(store_type, sstable_header.row_store_type_);
    ASSERT_EQ(0, sstable_header.reserved16_);
    ASSERT_EQ(static_cast<uint64_t>(1001), sstable_header.first_table_id_);
    ASSERT_EQ(table_count, sstable_header.table_count_);
    ASSERT_EQ(static_cast<int64_t>(sizeof(ObSSTableTableIndex)),
        sstable_header.table_index_unit_size_);
    ASSERT_EQ(sstable_header.table_count_ * 20,
        sstable_header.schema_array_unit_count_);
    ASSERT_EQ(static_cast<int>(sizeof(ObSSTableTableSchemaItem)),
        sstable_header.schema_array_unit_size_);
    ASSERT_EQ(block_size, sstable_header.block_size_);
    for (int64_t i = 0; i < compressor_str.length(); i ++)
    {
      ASSERT_EQ(compressor_str.ptr()[i],
          sstable_header.compressor_name_[i]);
    }
    for (int64_t i = 0; i < 64; i ++)
    {
      ASSERT_EQ(0, sstable_header.reserved64_[i]);
    }

    //sstable schema
    read_offset = sstable_header.schema_record_offset_;
    read_size = sstable_header.schema_array_unit_count_
      * sizeof(ObSSTableTableSchemaItem) + sizeof(ObRecordHeaderV2);
    lseek(fd, read_offset, SEEK_SET);
    real_read_size = read(fd, buf, read_size);
    ASSERT_EQ(real_read_size, read_size);
    ret = ObRecordHeaderV2::check_record(buf, real_read_size,
        OB_SSTABLE_SCHEMA_MAGIC, record_header,
        payload_ptr, payload_size);
    ASSERT_EQ(OB_SUCCESS, ret);
    int64_t def_column_count = sstable_header.schema_array_unit_count_;
    int64_t tmp_def_column_count;
    const ObSSTableSchemaColumnDef* def = schema.get_column_def_array(
        tmp_def_column_count);
    const ObSSTableTableSchemaItem* def_item
      = (const ObSSTableTableSchemaItem*)(payload_ptr);
    ASSERT_TRUE(NULL != def);
    //ASSERT_EQ(schema.get_column_count(), def_column_count);
    for (int64_t i = 0; i < def_column_count; i ++)
    {
      ASSERT_EQ(def_item[i].table_id_, static_cast<uint64_t>(1001 + (i/20)));
      ASSERT_EQ(def_item[i].column_id_, def[i%20].column_id_);
      ASSERT_EQ(def_item[i].rowkey_seq_, def[i%20].rowkey_seq_);
      ASSERT_EQ(def_item[i].column_attr_, 0);
      ASSERT_EQ(def_item[i].column_value_type_,
          def[i%20].column_value_type_);
      for (int64_t j = 0; j < 3; j ++)
      {
        ASSERT_EQ(def_item[i].reserved16_[j], 0);
      }
    }

    //table index
    read_offset = sstable_header.table_index_offset_;
    read_size = sstable_header.table_count_
      * sstable_header.table_index_unit_size_
      + sizeof(ObRecordHeaderV2);
    lseek(fd, read_offset, SEEK_SET);
    real_read_size = read(fd, buf, read_size);
    ASSERT_EQ(real_read_size, read_size);
    ret = ObRecordHeaderV2::check_record(buf, real_read_size,
        OB_SSTABLE_TABLE_INDEX_MAGIC, record_header,
        payload_ptr, payload_size);
    ASSERT_EQ(OB_SUCCESS, ret);
    table_index_ptr = (ObSSTableTableIndex*)ob_malloc(
      sizeof(ObSSTableTableIndex) * sstable_header.table_count_, ObModIds::TEST);
    memcpy(table_index_ptr, payload_ptr, payload_size);
    for (int64_t i = 0; i < sstable_header.table_count_; i ++)
    {
      ASSERT_EQ(static_cast<int64_t>(sizeof(ObSSTableTableIndex)), table_index_ptr[i].size_);
      ASSERT_EQ(1 * ObSSTableTableIndex::TABLE_INDEX_VERSION,
          table_index_ptr[i].version_);
      ASSERT_EQ(table_id[i], table_index_ptr[i].table_id_);
      ASSERT_EQ(20, table_index_ptr[i].column_count_);
      ASSERT_EQ(10, table_index_ptr[i].columns_in_rowkey_);
      ASSERT_EQ(SSTABLE_BLOOMFILTER_HASH_COUNT,
          table_index_ptr[i].bloom_filter_hash_count_);
      for (int64_t j = 0; j < 8; j ++)
      {
        ASSERT_EQ(0, table_index_ptr[i].reserved_[j]);
      }
    }

    //table bloomfilter
    for (int64_t i = 0; i < sstable_header.table_count_; i ++)
    {
      read_offset = table_index_ptr[i].bloom_filter_offset_;
      read_size = table_index_ptr[i].bloom_filter_size_;
      lseek(fd, read_offset, SEEK_SET);
      real_read_size = read(fd, buf, read_size);
      ASSERT_EQ(real_read_size, read_size);
      ret = ObRecordHeaderV2::check_record(buf, real_read_size,
          OB_SSTABLE_TABLE_BLOOMFILTER_MAGIC,
          record_header, payload_ptr, payload_size);
      ASSERT_EQ(OB_SUCCESS, ret);
    }

    //table range
    for (int64_t i = 0; i < sstable_header.table_count_; i ++)
    {
      read_offset = table_index_ptr[i].range_keys_offset_;
      read_size = 1
        + table_index_ptr[i].range_start_key_length_
        + table_index_ptr[i].range_end_key_length_
        + sizeof(ObRecordHeaderV2);
      lseek(fd, read_offset, SEEK_SET);
      real_read_size = read(fd, buf, read_size);
      ASSERT_EQ(real_read_size, read_size);
      ret = ObRecordHeaderV2::check_record(buf, real_read_size,
          OB_SSTABLE_TABLE_RANGE_MAGIC,
          record_header, payload_ptr, payload_size);
      ASSERT_EQ(OB_SUCCESS, ret);
      int64_t pos = 0;
      int8_t* tmp_data = (int8_t*)payload_ptr;
      ObNewRange tmp_range;
      make_range(tmp_range, table_id[i], range_start_num[i],
          range_end_num[i], range_start_flag[i], range_end_flag[i]);
      ASSERT_EQ(*tmp_data, tmp_range.border_flag_.get_data());
      pos ++;
      row_writer.reset();
      row_writer.append_rowkey(tmp_range.start_key_);
      for (int64_t j = 0; j < table_index_ptr[i].range_start_key_length_;
          j ++)
      {
        ASSERT_EQ(cur_row_buf[j], payload_ptr[pos + j]);
      }
      pos += table_index_ptr[i].range_start_key_length_;
      row_writer.reset();
      row_writer.append_rowkey(tmp_range.end_key_);
      for (int64_t j = 0; j < table_index_ptr[i].range_end_key_length_;
          j ++)
      {
        ASSERT_EQ(cur_row_buf[j], payload_ptr[pos + j]);
      }
    }

    //block index
    block_index_ptr = (ObSSTableBlockIndex**)ob_malloc(
      sstable_header.table_count_ * sizeof(ObSSTableBlockIndex*), ObModIds::TEST);
    ASSERT_TRUE(NULL != block_index_ptr);
    for (int64_t i = 0; i < sstable_header.table_count_; i ++)
    {
      if (table_index_ptr[i].block_count_ != 0)
      {
        block_index_ptr[i] = (ObSSTableBlockIndex*)ob_malloc(
            (table_index_ptr[i].block_count_ + 1) *
            sizeof(ObSSTableBlockIndex), ObModIds::TEST);
        read_offset = table_index_ptr[i].block_index_offset_;
        read_size = table_index_ptr[i].block_index_size_;
        lseek(fd, read_offset, SEEK_SET);
        real_read_size = read(fd, buf, read_size);
        ASSERT_EQ(real_read_size, read_size);
        ret = ObRecordHeaderV2::check_record(buf, real_read_size,
            OB_SSTABLE_BLOCK_INDEX_MAGIC,
            record_header, payload_ptr, payload_size);
        ASSERT_EQ(OB_SUCCESS, ret);
        memcpy(block_index_ptr[i], payload_ptr, payload_size);
      }
    }

    //block endkey
    block_endkey_ptr = (char**)ob_malloc(
      sstable_header.table_count_ * sizeof(char*), ObModIds::TEST);
    ASSERT_TRUE(NULL != block_endkey_ptr);
    for (int64_t i = 0; i < sstable_header.table_count_; i ++)
    {
      if (table_index_ptr[i].block_count_ != 0)
      {
        read_offset = table_index_ptr[i].block_endkey_offset_;
        read_size = table_index_ptr[i].block_endkey_size_;
        lseek(fd, read_offset, SEEK_SET);
        real_read_size = read(fd, buf, read_size);
        ASSERT_EQ(real_read_size, read_size);
        ret = ObRecordHeaderV2::check_record(buf, real_read_size,
            OB_SSTABLE_BLOCK_ENDKEY_MAGIC,
            record_header, payload_ptr, payload_size);
        ASSERT_EQ(OB_SUCCESS, ret);
        block_endkey_ptr[i] = (char*)ob_malloc(payload_size, ObModIds::TEST);
        memcpy(block_endkey_ptr[i], payload_ptr, payload_size);
      }
    }

    //block
    for (int64_t i = 0; i < sstable_header.table_count_; i ++)
    {
      cur_table_offset = table_index_ptr[i].block_data_offset_;
      ii = start_row_num[0];
      for (int64_t j = 0; j < table_index_ptr[i].block_count_; j ++)
      {
        read_offset = block_index_ptr[i][j].block_data_offset_;
        read_size = block_index_ptr[i][j + 1].block_data_offset_
          - block_index_ptr[i][j].block_data_offset_;
        lseek(fd, read_offset + table_index_ptr[i].block_data_offset_,
            SEEK_SET);
        real_read_size = read(fd, buf, read_size);
        ASSERT_EQ(real_read_size, read_size);
        ret = ObRecordHeaderV2::check_record(buf, real_read_size,
            OB_SSTABLE_BLOCK_DATA_MAGIC,
            record_header, payload_ptr, payload_size);
        ASSERT_EQ(OB_SUCCESS, ret);
        if (record_header.data_length_ == record_header.data_zlength_)
        {
          memcpy(uncomp_buf, payload_ptr, payload_size);
          uncomp_buf_len = payload_size;
        }
        else
        {
          ret = compressor->decompress(payload_ptr, payload_size,
              uncomp_buf, uncomp_buf_size, uncomp_buf_len);
          ASSERT_EQ(OB_SUCCESS, ret);
        }

        int64_t pos = 0;
        block_header_ptr = (ObSSTableBlockHeader*)uncomp_buf;
        ASSERT_EQ(0, block_header_ptr->reserved64_);
        pos = sizeof(ObSSTableBlockHeader);
        for (int64_t k = 0; k < block_header_ptr->row_count_; k ++)
        {
          make_row(row, table_index_ptr[i].table_id_, ii, 0);
          row_writer.reset();
          row_writer.append_row(row);
          for (int64_t z = 0; z < row_writer.size(); z ++)
          {
            ASSERT_EQ(cur_row_buf[z], uncomp_buf[pos + z]);
          }

          row_index_ptr = (ObSSTableBlockRowIndex*)(&uncomp_buf[
            block_header_ptr->row_index_offset_
            + k * sizeof(ObSSTableBlockRowIndex)]);
          ASSERT_EQ(pos, row_index_ptr->row_offset_);
          pos += row_writer.size();

          if (k == block_header_ptr->row_count_ - 1)
          {
            row_writer.reset();
            make_rowkey(rowkey, ii, 0);
            row_writer.append_rowkey(rowkey);
            char* tmp_buf = (char*)(block_endkey_ptr[i]
              + block_index_ptr[i][j].block_endkey_offset_);
            ASSERT_EQ(row_writer.size(), block_index_ptr[i][j + 1].block_endkey_offset_ - block_index_ptr[i][j].block_endkey_offset_);
            for (int64_t z = 0; z < row_writer.size(); z++)
            {
              ASSERT_EQ(cur_row_buf[z], tmp_buf[z]);
            }
          }
          ii ++;
        }
        row_index_ptr = (ObSSTableBlockRowIndex*)(&uncomp_buf[
          block_header_ptr->row_index_offset_
          + block_header_ptr->row_count_
          * sizeof(ObSSTableBlockRowIndex)]);
        ASSERT_EQ(pos, row_index_ptr->row_offset_);
      }
    }

    ob_free(buf);
    ob_free(uncomp_buf);
  }
};


/**
 * test construct
 */
TEST_F(TestCompactSSTableWriter, construct)
{
  ObCompactSSTableWriter writer;

  EXPECT_FALSE(writer.sstable_inited_);
  EXPECT_FALSE(writer.table_inited_);
  EXPECT_FALSE(writer.sstable_file_inited_);
  EXPECT_FALSE(writer.sstable_first_table_);
  EXPECT_FALSE(writer.not_table_first_row_);
  EXPECT_TRUE(NULL == writer.compressor_);
  EXPECT_TRUE(writer.dio_flag_);
  EXPECT_TRUE(writer.filesys_ == &writer.default_filesys_);
  EXPECT_FALSE(writer.split_buf_inited_);
  EXPECT_FALSE(writer.split_flag_);
  ASSERT_EQ(0, writer.def_sstable_size_);
  ASSERT_EQ(0, writer.min_split_sstable_size_);
  ASSERT_EQ(0, writer.cur_offset_);
  ASSERT_EQ(0, writer.cur_table_offset_);
  ASSERT_EQ(static_cast<uint64_t>(0), writer.sstable_checksum_);
  ASSERT_EQ(0, writer.sstable_table_init_count_);
  ASSERT_EQ(0, writer.sstable_trailer_offset_.offset_);
  ASSERT_EQ(0, writer.query_struct_.sstable_size_);
}

TEST_F(TestCompactSSTableWriter, destruct)
{
}

/**
 * test set_sstable_param1
 * --DENSE_SPARSE
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param1)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(DENSE_SPARSE, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 2);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_SPARSE;
  const int64_t table_count = 5;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  EXPECT_TRUE(writer.sstable_inited_);
  EXPECT_FALSE(writer.table_inited_);
  EXPECT_FALSE(writer.sstable_file_inited_);
  EXPECT_FALSE(writer.sstable_first_table_);
  EXPECT_FALSE(writer.not_table_first_row_);
  EXPECT_TRUE(NULL != writer.compressor_);
  EXPECT_TRUE(writer.dio_flag_);
  EXPECT_TRUE(writer.filesys_ == &writer.default_filesys_);
  EXPECT_FALSE(writer.split_buf_inited_);
  EXPECT_FALSE(writer.split_flag_);
  ASSERT_EQ(0, writer.def_sstable_size_);
  ASSERT_EQ(0, writer.min_split_sstable_size_);
  ASSERT_EQ(0, writer.cur_offset_);
  ASSERT_EQ(0, writer.cur_table_offset_);
  ASSERT_EQ(static_cast<uint64_t>(0), writer.sstable_checksum_);
  ASSERT_EQ(5, writer.sstable_table_init_count_);
  ASSERT_EQ(0, writer.sstable_trailer_offset_.offset_);
  ASSERT_EQ(0, writer.query_struct_.sstable_size_);
}

/**
 * test set_sstable_param2
 * --DENSE_DENSE
 * --not split
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param2)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(DENSE_DENSE, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 2);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  EXPECT_TRUE(writer.sstable_inited_);
  EXPECT_FALSE(writer.table_inited_);
  EXPECT_FALSE(writer.sstable_file_inited_);
  EXPECT_FALSE(writer.sstable_first_table_);
  EXPECT_FALSE(writer.not_table_first_row_);
  EXPECT_TRUE(NULL != writer.compressor_);
  EXPECT_TRUE(writer.dio_flag_);
  EXPECT_TRUE(writer.filesys_ == &writer.default_filesys_);
  EXPECT_FALSE(writer.split_buf_inited_);
  EXPECT_FALSE(writer.split_flag_);
  ASSERT_EQ(0, writer.def_sstable_size_);
  ASSERT_EQ(0, writer.min_split_sstable_size_);
  ASSERT_EQ(0, writer.cur_offset_);
  ASSERT_EQ(0, writer.cur_table_offset_);
  ASSERT_EQ(static_cast<uint64_t>(0), writer.sstable_checksum_);
  ASSERT_EQ(1, writer.sstable_table_init_count_);
  ASSERT_EQ(0, writer.sstable_trailer_offset_.offset_);
  ASSERT_EQ(0, writer.query_struct_.sstable_size_);
}

/**
 * test set_sstable_param3
 * --DENSE_DENSE
 * --split
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param3)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(DENSE_DENSE, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 2);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 100;
  const int64_t min_split_sstable_size = 50;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  EXPECT_TRUE(writer.sstable_inited_);
  EXPECT_FALSE(writer.table_inited_);
  EXPECT_FALSE(writer.sstable_file_inited_);
  EXPECT_FALSE(writer.sstable_first_table_);
  EXPECT_FALSE(writer.not_table_first_row_);
  EXPECT_TRUE(NULL != writer.compressor_);
  EXPECT_TRUE(writer.dio_flag_);
  EXPECT_TRUE(writer.filesys_ == &writer.default_filesys_);
  EXPECT_FALSE(writer.split_buf_inited_);
  EXPECT_TRUE(writer.split_flag_);
  ASSERT_EQ(100, writer.def_sstable_size_);
  ASSERT_EQ(50, writer.min_split_sstable_size_);
  ASSERT_EQ(0, writer.cur_offset_);
  ASSERT_EQ(0, writer.cur_table_offset_);
  ASSERT_EQ(static_cast<uint64_t>(0), writer.sstable_checksum_);
  ASSERT_EQ(1, writer.sstable_table_init_count_);
  ASSERT_EQ(0, writer.sstable_trailer_offset_.offset_);
  ASSERT_EQ(0, writer.query_struct_.sstable_size_);
}

/**
 * test set_sstable_param4
 * compress string == NULL
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param4)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(DENSE_DENSE, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 100;
  const int64_t min_split_sstable_size = 50;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  EXPECT_TRUE(writer.sstable_inited_);
  EXPECT_FALSE(writer.table_inited_);
  EXPECT_FALSE(writer.sstable_file_inited_);
  EXPECT_FALSE(writer.sstable_first_table_);
  EXPECT_FALSE(writer.not_table_first_row_);
  EXPECT_TRUE(NULL == writer.compressor_);
  EXPECT_TRUE(writer.dio_flag_);
  EXPECT_TRUE(writer.filesys_ == &writer.default_filesys_);
  EXPECT_FALSE(writer.split_buf_inited_);
  EXPECT_TRUE(writer.split_flag_);
  ASSERT_EQ(100, writer.def_sstable_size_);
  ASSERT_EQ(50, writer.min_split_sstable_size_);
  ASSERT_EQ(0, writer.cur_offset_);
  ASSERT_EQ(0, writer.cur_table_offset_);
  ASSERT_EQ(static_cast<uint64_t>(0), writer.sstable_checksum_);
  ASSERT_EQ(1, writer.sstable_table_init_count_);
  ASSERT_EQ(0, writer.sstable_trailer_offset_.offset_);
  ASSERT_EQ(0, writer.query_struct_.sstable_size_);
}

/**
 * test set_sstable_param5
 * sstable reinited
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param5)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(DENSE_DENSE, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 100;
  const int64_t min_split_sstable_size = 50;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

/**
 * test set_sstable_param6
 * invalid row store type
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param6)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(DENSE_DENSE, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 100;
  const int64_t min_split_sstable_size = 50;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

/**
 * test set_sstable_param7
 * valid version range
 * --store_type==DENSE_SPARSE
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param7)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(DENSE_SPARSE, version_range, 1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_SPARSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);
}

/**
 * test set_sstable_param8
 * --invalid version range
 * --store_type==DENSE_SPARSE
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param8)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(DENSE_DENSE, version_range, 7);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_SPARSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

/**
 * test set_sstable_param9
 * --invalid table count
 * --store_type==DENSE_SPARSE
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param9)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(DENSE_SPARSE, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_SPARSE;
  const int64_t table_count = -1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

/**
 * test set_sstable_param10
 * --invalid block size
 * --store_type==DENSE_SPARSE
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param10)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(DENSE_SPARSE, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_SPARSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 0;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

/**
 * test set_sstable_param11
 * --min/def sstable size error
 *  --min_split_sstable_size > def_sstable_size
 * --store_type==DENSE_SPARSE
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param11)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(DENSE_SPARSE, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 50;
  const int64_t min_split_sstable_size = 100;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

/**
 * test set_sstable_param12
 * --min/def sstable size error
 *  --min_split_sstable_size != 0 && def_sstable_size == 0
 * --store_type==DENSE_SPARSE
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param12)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(DENSE_SPARSE, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 100;
  const int64_t min_split_sstable_size = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

/**
 * test set_sstable_param13
 * --invalid compressor
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param13)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(DENSE_DENSE, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 1);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_ERROR, ret);
}

/**
 * test set table info1
 * --success(one table)
 */
TEST_F(TestCompactSSTableWriter, set_table_info1)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObNewRange range;
  ObSSTableSchema schema;

  ret = make_version_range(DENSE_DENSE, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 2);
  ASSERT_EQ(OB_SUCCESS, ret);


  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  const uint64_t table_id = 1001;
  const int range_flag = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  make_schema(schema, table_id, range_flag);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  EXPECT_TRUE(writer.sstable_inited_);
  EXPECT_TRUE(writer.table_inited_);
  EXPECT_FALSE(writer.sstable_file_inited_);
  EXPECT_TRUE(writer.sstable_first_table_);
  EXPECT_FALSE(writer.not_table_first_row_);
  EXPECT_TRUE(NULL != writer.compressor_);
  EXPECT_TRUE(writer.dio_flag_);
  EXPECT_TRUE(writer.filesys_ == &writer.default_filesys_);
  EXPECT_FALSE(writer.split_buf_inited_);
  EXPECT_FALSE(writer.split_flag_);
  ASSERT_EQ(0, writer.def_sstable_size_);
  ASSERT_EQ(0, writer.min_split_sstable_size_);
  ASSERT_EQ(0, writer.cur_offset_);
  ASSERT_EQ(0, writer.cur_table_offset_);
  ASSERT_EQ(static_cast<uint64_t>(0), writer.sstable_checksum_);
  ASSERT_EQ(1, writer.sstable_table_init_count_);
  ASSERT_EQ(0, writer.sstable_trailer_offset_.offset_);
  ASSERT_EQ(0, writer.query_struct_.sstable_size_);
}

/**
 * tst set table info2
 * --fail(sstable is not init)
 */
TEST_F(TestCompactSSTableWriter, set_table_info2)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObSSTableSchema schema;
  ObNewRange range;
  const uint64_t table_id = 1001;
  const int range_flag = 0;

  make_schema(schema, table_id, range_flag);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_NOT_INIT, ret);
}

/**
 * test set sstable filepath1
 * --success
 */
TEST_F(TestCompactSSTableWriter, set_sstable_filepath1)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObNewRange range;
  ObSSTableSchema schema;
  ObString file_path;

  ret = make_version_range(DENSE_DENSE, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 2);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  const uint64_t table_id = 1001;
  const int schema_flag = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  make_schema(schema, table_id, schema_flag);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  const int64_t file_num = 1;
  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_SUCCESS, ret);

  EXPECT_TRUE(writer.sstable_inited_);
  EXPECT_TRUE(writer.table_inited_);
  EXPECT_TRUE(writer.sstable_file_inited_);
  EXPECT_TRUE(writer.sstable_first_table_);
  EXPECT_FALSE(writer.not_table_first_row_);
  EXPECT_TRUE(NULL != writer.compressor_);
  EXPECT_TRUE(writer.dio_flag_);
  EXPECT_TRUE(writer.filesys_ == &writer.default_filesys_);
  EXPECT_FALSE(writer.split_buf_inited_);
  EXPECT_FALSE(writer.split_flag_);
  ASSERT_EQ(0, writer.def_sstable_size_);
  ASSERT_EQ(0, writer.min_split_sstable_size_);
  ASSERT_EQ(0, writer.cur_offset_);
  ASSERT_EQ(0, writer.cur_table_offset_);
  ASSERT_EQ(static_cast<uint64_t>(0), writer.sstable_checksum_);
  ASSERT_EQ(1, writer.sstable_table_init_count_);
  ASSERT_EQ(0, writer.sstable_trailer_offset_.offset_);
  ASSERT_EQ(0, writer.query_struct_.sstable_size_);

  delete_file(file_num);
}

/**
 * test set sstable filepath2
 * --fail(sstable not init)
 */
TEST_F(TestCompactSSTableWriter, set_sstable_filepath2)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObString file_path;

  const int64_t file_num = 1;
  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_NOT_INIT, ret);
}

/**
 * test set sstable filepath3
 * --table not inited
 */
TEST_F(TestCompactSSTableWriter, set_sstable_filepath3)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObNewRange range;
  ObSSTableSchema schema;
  ObString file_path;

  ret = make_version_range(DENSE_DENSE, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 2);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  const int64_t file_num = 1;
  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_NOT_INIT, ret);
}

/**
 * test set sstable filepath4
 * --sstable file init twice
 */
TEST_F(TestCompactSSTableWriter, set_sstable_filepath4)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObNewRange range;
  ObSSTableSchema schema;
  ObString file_path;

  ret = make_version_range(DENSE_DENSE, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 2);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  const uint64_t table_id = 1001;
  const int schema_flag = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  make_schema(schema, table_id, schema_flag);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  const int64_t file_num = 1;
  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_INIT_TWICE, ret);

  delete_file(file_num);
}

/**
 * test set sstable filepath5
 * --empty file name
 */
TEST_F(TestCompactSSTableWriter, set_sstable_filepath5)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObNewRange range;
  ObSSTableSchema schema;
  ObString file_path;

  ret = make_version_range(DENSE_DENSE, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 2);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  const uint64_t table_id = 1001;
  const int schema_flag = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  make_schema(schema, table_id, schema_flag);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  const int64_t file_num = -2;
  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  delete_file(file_num);
}

/**
 * test set sstable filepath6
 * --empty file name
 */
TEST_F(TestCompactSSTableWriter, set_sstable_filepath6)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObNewRange range;
  ObSSTableSchema schema;
  ObString file_path;

  ret = make_version_range(DENSE_DENSE, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 2);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  const uint64_t table_id = 1001;
  const int schema_flag = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  make_schema(schema, table_id, schema_flag);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  const int64_t file_num = -1;
  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_filepath(file_path);
  EXPECT_NE(OB_ERROR, ret);

  delete_file(file_num);
}

/**
 * test append_row1
 * --DENSE_DENSE
 * --one row
 */
TEST_F(TestCompactSSTableWriter, append_row1)
{
  int ret = OB_SUCCESS;
  ObCompressor* compressor = NULL;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObSSTableSchema schema;
  ObNewRange range;
  ObString file_path;
  ObRow row;
  const ObCompactStoreType store_type = DENSE_DENSE;
  uint64_t table_id = 1001;
  const int64_t table_count = 1;
  const int64_t block_size = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  const int64_t file_num = 1;
  const int schema_flag = 0;
  bool is_split = false;

  ret = make_version_range(DENSE_DENSE, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_compressor(compressor, 2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, block_size, comp_name, def_sstable_size,
      min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  make_schema(schema, table_id, schema_flag);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < 1; i ++)
  {
    make_row(row, table_id, i, 0);

    ret = writer.append_row(row, is_split);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ret = writer.finish();
  ASSERT_EQ(OB_SUCCESS, ret);

  //open file
  int fd = 0;
  if (-1 == (fd = open(file_path.ptr(), O_RDONLY)))
  {
    TBSYS_LOG(WARN, "open error");
  }

  int64_t start_row_num[1] = {0};
  const int64_t range_start_num = 0;
  const int64_t range_end_num = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  check_sstable_file(fd, compressor, comp_name, start_row_num,
      schema, version_range, &range_start_num, &range_end_num,
      &range_start_flag, &range_end_flag,
      store_type, &table_id, table_count, block_size);

  //close and delete file
  close(fd);
  delete_file(file_num);
}

/**
 * test append_row2
 * --DENSE_DENSE
 * --one block(24 row)
 */
TEST_F(TestCompactSSTableWriter, append_row2)
{
  int ret = OB_SUCCESS;
  ObCompressor* compressor = NULL;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObSSTableSchema schema;
  ObNewRange range;
  ObString file_path;
  ObRow row;
  const ObCompactStoreType store_type = DENSE_DENSE;
  uint64_t table_id = 1001;
  const int64_t table_count = 1;
  const int64_t block_size = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  const int64_t file_num = 1;
  const int schema_flag = 0;
  bool is_split = false;

  ret = make_version_range(store_type, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_compressor(compressor, 2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, block_size, comp_name, def_sstable_size,
      min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  make_schema(schema, table_id, schema_flag);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < 24; i ++)
  {
    make_row(row, table_id, i, 0);

    ret = writer.append_row(row, is_split);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ret = writer.finish();
  ASSERT_EQ(OB_SUCCESS, ret);

  //open file
  int fd = 0;
  if (-1 == (fd = open(file_path.ptr(), O_RDONLY)))
  {
    TBSYS_LOG(WARN, "open error");
  }

  int64_t start_row_num[1] = {0};
  const int64_t range_start_num = 0;
  const int64_t range_end_num = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  check_sstable_file(fd, compressor, comp_name, start_row_num,
      schema, version_range, &range_start_num, &range_end_num,
      &range_start_flag, &range_end_flag, store_type,
      &table_id, table_count, block_size);

  //close and delete file
  close(fd);
  delete_file(file_num);
}

/**
 * test append_row3
 * --DENSE_DENSE
 * --three block(24 * 2 + 2 row)
 */
TEST_F(TestCompactSSTableWriter, append_row3)
{
  int ret = OB_SUCCESS;
  ObCompressor* compressor = NULL;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObSSTableSchema schema;
  ObNewRange range;
  ObString file_path;
  ObRow row;
  const ObCompactStoreType store_type = DENSE_DENSE;
  uint64_t table_id = 1001;
  const int64_t table_count = 1;
  const int64_t block_size = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  const int64_t file_num = 1;
  const int schema_flag = 0;
  bool is_split = false;

  ret = make_version_range(store_type, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_compressor(compressor, 2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, block_size, comp_name, def_sstable_size,
      min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  make_schema(schema, table_id, schema_flag);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < 50; i ++)
  {
    make_row(row, table_id, i, 0);

    ret = writer.append_row(row, is_split);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ret = writer.finish();
  ASSERT_EQ(OB_SUCCESS, ret);

  //open file
  int fd = 0;
  if (-1 == (fd = open(file_path.ptr(), O_RDONLY)))
  {
    TBSYS_LOG(WARN, "open error");
  }

  int64_t start_row_num[1] = {0};
  const int64_t range_start_num = 0;
  const int64_t range_end_num = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  check_sstable_file(fd, compressor, comp_name, start_row_num,
      schema, version_range, &range_start_num, &range_end_num,
      &range_start_flag, &range_end_flag, store_type,
      &table_id, table_count, block_size);

  //close and delete file
  close(fd);
  delete_file(file_num);
}

/**
 * test append_row7
 * --DENSE_SPARSE
 * --three table
 */
TEST_F(TestCompactSSTableWriter, append_row4)
{
  int ret = OB_SUCCESS;
  ObCompressor* compressor = NULL;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObSSTableSchema schema;
  ObNewRange range;
  ObString file_path;
  ObRow row;
  const ObCompactStoreType store_type = DENSE_SPARSE;
  uint64_t table_id[3] = {1001, 1002, 1003};
  const int64_t table_count = 3;
  const int64_t block_size = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  const int64_t file_num = 1;
  const int schema_flag = 0;
  bool is_split = false;

  ret = make_version_range(store_type, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_compressor(compressor, 2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, block_size, comp_name, def_sstable_size,
      min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id[0], 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  make_schema(schema, table_id[0], schema_flag);

  ret = writer.set_table_info(table_id[0], schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < 100; i ++)
  {
    make_row(row, table_id[0], i, 0);

    ret = writer.append_row(row, is_split);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  //table_id:1002
  ret = make_range(range, table_id[1], 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  make_schema(schema, table_id[1], schema_flag);

  ret = writer.set_table_info(table_id[1], schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < 100; i ++)
  {
    make_row(row, table_id[1], i, 0);

    ret = writer.append_row(row, is_split);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  //table_id:1003
  ret = make_range(range, table_id[2], 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  make_schema(schema, table_id[2], schema_flag);

  ret = writer.set_table_info(table_id[2], schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < 100; i ++)
  {
    make_row(row, table_id[2], i, 0);

    ret = writer.append_row(row, is_split);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ret = writer.finish();
  ASSERT_EQ(OB_SUCCESS, ret);

  //open file
  int fd = 0;
  if (-1 == (fd = open(file_path.ptr(), O_RDONLY)))
  {
    TBSYS_LOG(WARN, "open error");
  }

  int64_t start_row_num[3] = {0, 0, 0};
  const int64_t range_start_num[3] = {0, 0, 0};
  const int64_t range_end_num[3] = {0, 0, 0};
  const int range_start_flag[3] = {0, 0, 0};
  const int range_end_flag[3] = {0, 0, 0};
  check_sstable_file(fd, compressor, comp_name, start_row_num,
      schema, version_range, range_start_num, range_end_num,
      range_start_flag, range_end_flag,
      store_type, table_id, table_count, block_size);

  //close and delete file
  close(fd);
  delete_file(file_num);
}

/**
 * test append_row5
 * --DENSE_DENSE
 * --not compress
 * --split
 * --(200 row)
 */
TEST_F(TestCompactSSTableWriter, append_row5)
{
  int ret = OB_SUCCESS;
  ObCompressor* compressor = NULL;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObSSTableSchema schema;
  ObNewRange range;
  ObString file_path;
  ObRow row;
  const ObCompactStoreType store_type = DENSE_DENSE;
  uint64_t table_id = 1001;
  const int64_t table_count = 1;
  const int64_t block_size = 1024;
  const int64_t def_sstable_size = 4000;
  const int64_t min_split_sstable_size = 2000;
  int64_t file_num = 1;
  const int schema_flag = 0;
  bool is_split = false;

  ret = make_version_range(store_type, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 4);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_compressor(compressor, 4);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, block_size, comp_name, def_sstable_size,
      min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  make_schema(schema, table_id, schema_flag);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < 300; i ++)
  {
    make_row(row, table_id, i, 0);

    ret = writer.append_row(row, is_split);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (true == is_split)
    {
      file_num ++;
      ret = make_file_path(file_path, file_num);
      ASSERT_EQ(OB_SUCCESS, ret);
      TBSYS_LOG(WARN, "i=%ld", i);

      ret = writer.set_sstable_filepath(file_path);
      ASSERT_EQ(OB_SUCCESS, ret);
      is_split = false;
    }
  }

  ret = writer.finish();
  ASSERT_EQ(OB_SUCCESS, ret);

  //open file
  int fd = 0;
  ret = make_file_path(file_path, 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  if (-1 == (fd = open(file_path.ptr(), O_RDONLY)))
  {
    TBSYS_LOG(WARN, "open error");
  }
  int64_t start_row_num1[1] = {0};
  int64_t range_start_num = 0;
  int64_t range_end_num = 87;
  int range_start_flag = 0;
  int range_end_flag = -2;
  check_sstable_file(fd, compressor, comp_name, start_row_num1,
      schema, version_range, &range_start_num, &range_end_num,
      &range_start_flag, &range_end_flag, store_type,
      &table_id, table_count, block_size);
  close(fd);
  delete_file(1);

  ret = make_file_path(file_path, 2);
  ASSERT_EQ(OB_SUCCESS, ret);
  if (-1 == (fd = open(file_path.ptr(), O_RDONLY)))
  {
    TBSYS_LOG(WARN, "open error");
  }
  int64_t start_row_num2[1] = {88};
  range_start_num = 87;
  range_end_num = 175;
  range_start_flag = -1;
  range_end_flag = -2;
  check_sstable_file(fd, compressor, comp_name, start_row_num2,
      schema, version_range, &range_start_num, &range_end_num,
      &range_start_flag, &range_end_flag, store_type,
      &table_id, table_count, block_size);
  close(fd);
  delete_file(2);

  ret = make_file_path(file_path, 3);
  ASSERT_EQ(OB_SUCCESS, ret);
  if (-1 == (fd = open(file_path.ptr(), O_RDONLY)))
  {
    TBSYS_LOG(WARN, "open error");
  }
  int64_t start_row_num3[1] = {176};
  range_start_num = 175;
  range_end_num = 0;
  range_start_flag = -1;
  range_end_flag = 0;
  check_sstable_file(fd, compressor, comp_name, start_row_num3,
      schema, version_range, &range_start_num, &range_end_num,
      &range_start_flag, &range_end_flag, store_type,
      &table_id, table_count, block_size);
  close(fd);
  delete_file(3);
}

/**
 * test append_row6
 * --DENSE_DENSE
 * --empty sstable
 */
TEST_F(TestCompactSSTableWriter, append_row6)
{
  int ret = OB_SUCCESS;
  ObCompressor* compressor = NULL;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObSSTableSchema schema;
  ObNewRange range;
  ObString file_path;
  ObRow row;
  const ObCompactStoreType store_type = DENSE_DENSE;
  uint64_t table_id = 1001;
  const int64_t table_count = 1;
  const int64_t block_size = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  const int64_t file_num = 1;
  const int schema_flag = 0;
  bool is_split = false;

  ret = make_version_range(store_type, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_compressor(compressor, 2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, block_size, comp_name, def_sstable_size,
      min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  make_schema(schema, table_id, schema_flag);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < 0; i ++)
  {
    make_row(row, table_id, i, 0);

    ret = writer.append_row(row, is_split);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ret = writer.finish();
  ASSERT_EQ(OB_SUCCESS, ret);

  //open file
  int fd = 0;
  if (-1 == (fd = open(file_path.ptr(), O_RDONLY)))
  {
    TBSYS_LOG(WARN, "open error");
  }

  int64_t start_row_num[1] = {0};
  const int64_t range_start_num = 0;
  const int64_t range_end_num = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  check_sstable_file(fd, compressor, comp_name, start_row_num,
      schema, version_range, &range_start_num, &range_end_num,
      &range_start_flag, &range_end_flag, store_type,
      &table_id, table_count, block_size);

  //close and delete file
  close(fd);
  delete_file(file_num);
}

TEST_F(TestCompactSSTableWriter, append_row7)
{
  int ret = OB_SUCCESS;
  ObCompressor* compressor = NULL;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObSSTableSchema schema;
  ObNewRange range;
  ObString file_path;
  ObRow row;
  const ObCompactStoreType store_type = DENSE_DENSE;
  uint64_t table_id = 1001;
  const int64_t table_count = 1;
  const int64_t block_size = 64 * 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  const int64_t file_num = 1;
  const int schema_flag = 0;
  bool is_split = false;

  ret = make_version_range(store_type, version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_compressor(compressor, 2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, block_size, comp_name, def_sstable_size,
      min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  make_schema(schema, table_id, schema_flag);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_SUCCESS, ret);


  for (int64_t i = 0; i < 1000000; i ++)
  {
    //TBSYS_LOG(WARN, "i=%ld", i);
    make_row(row, table_id, i, 0);
    ret = writer.append_row(row, is_split);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ret = writer.finish();
  ASSERT_EQ(OB_SUCCESS, ret);

  delete_file(file_num);
}

/*
TEST_F(TestCompactSSTableWriter, get_table_range)
{//success or fail()
 //after set table info
 //after finish sstable
 //after switch table
 //after switch sstable

  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer1;
  ObCompactSSTableWriter writer2;
  ObCompactSSTableWriter writer3;
  ObCompactSSTableWriter writer4;
  ObFrozenMinorVersionRange version_range1;
  ObFrozenMinorVersionRange version_range2;
  ObFrozenMinorVersionRange version_range3;
  ObFrozenMinorVersionRange version_range4;
  ObString comp_name1;
  ObString comp_name2;
  ObString comp_name3;
  ObString comp_name4;
  ObSSTableSchema schema1;
  ObSSTableSchema schema2;
  ObSSTableSchema schema3;
  ObSSTableSchema schema4;
  ObNewRange range1;
  ObNewRange range2;
  ObNewRange range3;
  ObNewRange range4;
  const ObNewRange* result_range1 = NULL;
  const ObNewRange* result_range2 = NULL;
  const ObNewRange* result_range3 = NULL;
  const ObNewRange* result_range4 = NULL;
  ObNewRange expect_range1;
  ObNewRange expect_range2;
  ObNewRange expect_range3;
  ObNewRange expect_range4;
  ObString file_path2;
  ObString file_path3;
  ObString file_path4;
  ObRow row;
  int64_t i = 0;
  bool is_split = false;

  //after set table info(min-->max)(old/new)
  make_version_range(version_range1, 1001);
  make_comp_name(comp_name1, 0);
  make_schema(schema1, 1001);
  make_range(range1, 1001, -1, -1);

  ret = writer1.set_sstable_param(version_range1, DENSE_SPARSE,
      1, 1024, comp_name1, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer1.set_table_info(1001, schema1, range1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer1.get_table_range(result_range1, 1);
  ASSERT_EQ(OB_SUCCESS, ret);

  make_range(expect_range1, 1001, -1, -1);
  ASSERT_EQ(expect_range1.border_flag_.get_data(),
      result_range1->border_flag_.get_data());
  EXPECT_TRUE(expect_range1.equal(*result_range1));

  ret = writer1.get_table_range(result_range1, 0);
  EXPECT_NE(OB_SUCCESS, ret);

  //after finish sstable(old/new)(not split)
  make_version_range(version_range2, 1001);
  make_comp_name(comp_name2, 0);
  make_schema(schema2, 1001);
  make_range(range2, 1001, -1, -1);
  make_file_path(file_path2, 1);

  ret = writer2.set_sstable_param(version_range2, DENSE_SPARSE,
      1, 1024, comp_name2, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer2.set_table_info(1001, schema2, range2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer2.set_sstable_filepath(file_path2);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (i = 0; i < 10; i ++)
  {
    make_row(row, 1001, i);
    ret = writer2.append_row(row, is_split);
    row.reset(false, ObRow::DEFAULT_NULL);
    ASSERT_EQ(OB_SUCCESS, ret);
    EXPECT_FALSE(is_split);
  }

  ret = writer2.finish();
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer2.get_table_range(result_range2, 0);
  ASSERT_EQ(OB_SUCCESS, ret);
  make_range(expect_range2, 1001, -1, -1);
  ASSERT_EQ(expect_range2.border_flag_.get_data(),
      result_range2->border_flag_.get_data());
  EXPECT_TRUE(expect_range2.equal(*result_range2));

  ret = writer2.get_table_range(result_range2, 1);
  EXPECT_NE(OB_SUCCESS, ret);

  remove(file_path2.ptr());

  //after switch table
  make_version_range(version_range3, 1001);
  make_comp_name(comp_name3, 0);
  make_schema(schema3, 1001, 0);
  make_range(range3, 1001, 0, 100);
  make_file_path(file_path3, 1);

  ret = writer3.set_sstable_param(version_range3, DENSE_SPARSE,
      3, 1024, comp_name3, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer3.set_table_info(1001, schema3, range3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer3.set_sstable_filepath(file_path3);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (i = 1; i < 30; i ++)
  {
    make_row(row, 1001, i);
    ret = writer3.append_row(row, is_split);
    row.reset(false, ObRow::DEFAULT_NULL);
    ASSERT_EQ(OB_SUCCESS, ret);
    EXPECT_FALSE(is_split);
  }

  ret = writer3.get_table_range(result_range3, 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  make_range(expect_range3, 1001, 0, 100);
  ASSERT_EQ(expect_range3.border_flag_.get_data(),
      result_range3->border_flag_.get_data());
  EXPECT_TRUE(expect_range3.equal(*result_range3));

  ret = writer3.get_table_range(result_range3, 0);
  EXPECT_NE(OB_SUCCESS, ret);

  make_schema(schema3, 1002, 0);
  make_range(range3, 1002, 100, 200);
  ret = writer3.set_table_info(1002, schema3, range3);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (i = 101; i < 30; i ++)
  {
    make_row(row, 1002, i);
    ret = writer3.append_row(row, is_split);
    row.reset(false, ObRow::DEFAULT_NULL);
    ASSERT_EQ(OB_SUCCESS, ret);
    EXPECT_FALSE(is_split);
  }

  ret = writer3.get_table_range(result_range3, 0);
  ASSERT_EQ(OB_SUCCESS, ret);
  make_range(expect_range3, 1001, 0, 100);
  ASSERT_EQ(expect_range3.border_flag_.get_data(),
      result_range3->border_flag_.get_data());
  EXPECT_TRUE(expect_range3.equal(*result_range3));

  ret = writer3.get_table_range(result_range3, 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  make_range(expect_range3, 1002, 100, 200);
  ASSERT_EQ(expect_range3.border_flag_.get_data(),
      result_range3->border_flag_.get_data());
  EXPECT_TRUE(expect_range3.equal(*result_range3));

  make_schema(schema3, 1003, 0);
  make_range(range3, 1003, 200, 300);
  ret = writer3.set_table_info(1003, schema3, range3);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (i = 201; i < 30; i ++)
  {
    make_row(row, 1003, i);
    ret = writer3.append_row(row, is_split);
    row.reset(false, ObRow::DEFAULT_NULL);
    ASSERT_EQ(OB_SUCCESS, ret);
    EXPECT_FALSE(is_split);
  }

  ret = writer3.get_table_range(result_range3, 0);
  ASSERT_EQ(OB_SUCCESS, ret);
  make_range(expect_range3, 1002, 100, 200);
  ASSERT_EQ(expect_range3.border_flag_.get_data(),
      result_range3->border_flag_.get_data());
  EXPECT_TRUE(expect_range3.equal(*result_range3));

  ret = writer3.get_table_range(result_range3, 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  make_range(expect_range3, 1003, 200, 300);
  ASSERT_EQ(expect_range3.border_flag_.get_data(),
      result_range3->border_flag_.get_data());
  EXPECT_TRUE(expect_range3.equal(*result_range3));

  ret = writer3.finish();
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer3.get_table_range(result_range3, 0);
  ASSERT_EQ(OB_SUCCESS, ret);
  make_range(expect_range3, 1003, 200, 300);
  ASSERT_EQ(expect_range3.border_flag_.get_data(),
      result_range3->border_flag_.get_data());
  EXPECT_TRUE(expect_range3.equal(*result_range3));

  ret = writer3.get_table_range(result_range3, 1);
  EXPECT_NE(OB_SUCCESS, ret);

  remove(file_path3.ptr());

  //after switch sstable
  make_version_range(version_range4, 1001);
  make_comp_name(comp_name4, 0);
  make_schema(schema4, 1001);
  make_range(range4, 1001, -1, -1);
  make_file_path(file_path4, 1);

  ret = writer4.set_sstable_param(version_range4, DENSE_SPARSE,
      1, 1024, comp_name4, 4000, 2000);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer4.set_table_info(1001, schema4, range4);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t file_cnt = 1;
  make_file_path(file_path4, file_cnt);
  ret = writer4.set_sstable_filepath(file_path4);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (i = 0; i < 200; i ++)
  {
    make_row(row, 1001, i);
    ret = writer4.append_row(row, is_split);
    row.reset(false, ObRow::DEFAULT_NULL);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (is_split)
    {
      file_cnt ++;
      make_file_path(file_path4, file_cnt);
      ret = writer4.set_sstable_filepath(file_path4);
      ASSERT_EQ(OB_SUCCESS, ret);

      if (2 == file_cnt)
      {
        ret = writer4.get_table_range(result_range4, 0);
        ASSERT_EQ(OB_SUCCESS, ret);
        make_range(expect_range4, 1001, -1, 47);
        ASSERT_EQ(expect_range4.border_flag_.get_data(),
            result_range4->border_flag_.get_data());
        EXPECT_TRUE(expect_range4.equal(*result_range4));

        ret = writer4.get_table_range(result_range4, 1);
        ASSERT_EQ(OB_SUCCESS, ret);
        make_range(expect_range4, 1001, 47, -1);
        ASSERT_EQ(expect_range4.border_flag_.get_data(),
            result_range4->border_flag_.get_data());
        EXPECT_TRUE(expect_range4.equal(*result_range4));
      }
      else if (3 == file_cnt)
      {
        ret = writer4.get_table_range(result_range4, 0);
        ASSERT_EQ(OB_SUCCESS, ret);
        make_range(expect_range4, 1001, 47, 95);
        ASSERT_EQ(expect_range4.border_flag_.get_data(),
            result_range4->border_flag_.get_data());
        EXPECT_TRUE(expect_range4.equal(*result_range4));

        ret = writer4.get_table_range(result_range4, 1);
        ASSERT_EQ(OB_SUCCESS, ret);
        make_range(expect_range4, 1001, 95, -1);
        ASSERT_EQ(expect_range4.border_flag_.get_data(),
            result_range4->border_flag_.get_data());
        EXPECT_TRUE(expect_range4.equal(*result_range4));
      }
      else if (4 == file_cnt)
      {
        ret = writer4.get_table_range(result_range4, 0);
        ASSERT_EQ(OB_SUCCESS, ret);
        make_range(expect_range4, 1001, 95, 143);
        ASSERT_EQ(expect_range4.border_flag_.get_data(),
            result_range4->border_flag_.get_data());
        EXPECT_TRUE(expect_range4.equal(*result_range4));

        ret = writer4.get_table_range(result_range4, 1);
        ASSERT_EQ(OB_SUCCESS, ret);
        make_range(expect_range4, 1001, 143, -1);
        ASSERT_EQ(expect_range4.border_flag_.get_data(),
            result_range4->border_flag_.get_data());
        EXPECT_TRUE(expect_range4.equal(*result_range4));
      }
    }
  }

  ret = writer4.finish();
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer4.get_table_range(result_range4, 0);
  ASSERT_EQ(OB_SUCCESS, ret);
  make_range(expect_range4, 1001, 143, -1);
  ASSERT_EQ(expect_range4.border_flag_.get_data(),
      result_range4->border_flag_.get_data());
  EXPECT_TRUE(expect_range4.equal(*result_range4));

  ret = writer4.get_table_range(result_range4, 1);
  EXPECT_NE(OB_SUCCESS, ret);

  for (i = 1; i <= file_cnt; i ++)
  {
    make_file_path(file_path4, i);
    remove(file_path4.ptr());
  }
  (void) result_range4;
}
*/

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
