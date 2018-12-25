#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_COMPACT_ROW_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_COMPACT_ROW_H_
namespace oceanbase
{
  namespace compactsstablev2
  {
    class ObCompactRow
    {
    public:
      static const int64_t COL_ID_SIZE = 4;                                      //column id size(4 Byte)
      static const uint64_t MAX_COL_ID = common::OB_ALL_MAX_COLUMN_ID;           //max column id 2^16 -1
      static const int64_t MAX_COL_NUM = 1024L;
      static const int64_t MAX_BUF_SIZE = 2 * 1024 * 1024;                       //2M
      
    public:
      ObCompactRow();
      ~ObCompactRow();
    
      int get_obj(common::ObObj& obj);
      int get_rowkey(common::ObRowkey& rowkey, commom::ObObj* rowkey_buf);
      int set_rowkey(const common::ObRowKey& rowkey);
      int set_rowvalue(const common::ObRow& rowvalue);

      int assign_row(const char* buf, const int64_t key_size, const int64_t value_size);

      int next_cell();
      int get_cell();

      int set_end();

      int get_cur_colum_id();

    private:
      int get_follow_size(const ObCompactObjMeta* meta, int32_t& follow_size);
    
    private:
      ObBufferReader buf_reader_;
      uint64_t column_id_;
      ObObj value_;
      char* buf_;
      int32_t buf_pos_;
      int32_t buf_size_;
      bool self_alloc_;           //内存是否是自己分配的

      int32_t cur_iter_;
      
      bool row_inited_;
      ObCompactStoreType store_type_;  //存储类型
      int32_t step_;              //是否扫描rowkey or rowvalue (0:rowkey  1:rowvalue)
    };
  }//end namesapce compactsstablev2
}
#endif
