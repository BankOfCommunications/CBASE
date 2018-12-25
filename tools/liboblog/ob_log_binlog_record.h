////===================================================================
 //
 // ob_log_binlog_record.h liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-12-26 by XiuMing (wanhong.wwh@alibaba-inc.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //   ObLogBinlogRecord declaration file
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================

#ifndef OCEANBASE_LIBOBLOG_BINLOG_RECORD_H_
#define OCEANBASE_LIBOBLOG_BINLOG_RECORD_H_

#include "common/ob_define.h"
#include <BR.h>                   // IBinlogRecord
#include <DRCMessageFactory.h>    // createBinlogRecord
#include "tbsys.h"                // TBSYS_LOG

#define BR_CALL(f, args...) \
  do {\
    if (NULL == data_) \
    { \
      TBSYS_LOG(ERROR, "%s() fails, IBinlogRecord has not been created", __FUNCTION__); \
      abort(); \
    } \
    data_->f(args); \
  } while (0)

#define BR_CALL_WITH_RETVAL(f, args...) \
{ \
    if (NULL == data_) \
    { \
      TBSYS_LOG(ERROR, "%s() fails, IBinlogRecord has not been created", __FUNCTION__); \
      abort(); \
    } \
  return data_->f(args);\
}

class ITableMeta;

namespace oceanbase
{
  namespace liboblog
  {
    class ObLogDmlStmt;
    class ObLogMutator;
    class ObLogBinlogRecord : public IBinlogRecord
    {
      // Class constant vairables
      public:
        static const std::string BINLOG_RECORD_TYPE;

      // Member vairables
      private:
        IBinlogRecord     *data_;         ///< real BinlogRecord
        ObLogBinlogRecord *next_;
        int64_t           precise_timestamp_;   ///< precise timestamp in micro seconds

      // Constructor and Deconstructor
      public:
        ObLogBinlogRecord() : data_(NULL), next_(NULL), precise_timestamp_(0)
        {
          bool creating_binlog_record = true;

          data_ = DRCMessageFactory::createBinlogRecord(BINLOG_RECORD_TYPE, creating_binlog_record);
          if (NULL == data_)
          {
            TBSYS_LOG(ERROR, "DRCMessageFactory::createBinlogRecord fails");
            abort();
          }

          clear();
        }

        virtual ~ObLogBinlogRecord()
        {
          if (NULL != data_)
          {
            clear();

            DRCMessageFactory::destroy(data_);
            data_ = NULL;
          }

          next_ = NULL;
          precise_timestamp_ = 0;
        }

      // Specific interfaces
      public:
        void set_next(ObLogBinlogRecord *next) {next_ = next;};
        ObLogBinlogRecord *get_next() {return next_;};

        /// @brief Initialize HEARTBEAT BinlogRecord
        /// @param timestamp HEARTBEAT timestamp
        /// @param query_back whether to query back
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int init_heartbeat(int64_t timestamp, bool query_back);

        /// @brief Initialize transaction barrier: BEGIN/COMMIT
        /// @param type record type
        /// @param mutator sourc data container
        /// @param query_back whether to query back
        /// @param br_index_in_trans BinlogRecord index in transaction
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int init_trans_barrier(const RecordType type,
            const ObLogMutator &mutator,
            bool query_back,
            uint64_t br_index_in_trans);

        /// @brief Initialize BinlogRecord of HEARTBEAT type
        /// @param dml_stmt DML statement information
        /// @param query_back whether to query back
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int init_dml(const ObLogDmlStmt *dml_stmt, bool query_back);

        inline void set_precise_timestamp(int64_t precise_timestamp) { precise_timestamp_ = precise_timestamp; }
        inline int64_t get_precise_timestamp() const { return precise_timestamp_; }

      // Private member functions
      private:
        /// NOTE: If query back, binlog record data is full and all retrieved;
        ///       else binlog record is full and some columns are faked.
        int get_src_category_(bool query_back)
        {
          return query_back ? SRC_FULL_RETRIEVED : SRC_FULL_FAKED;
        }

        RecordType get_record_type_(const common::ObDmlType dml_type) const;

        void init_(uint64_t id,
            RecordType type,
            int src_category,
            int64_t timestamp,
            uint64_t checkpoint,
            bool first_in_log_event,
            ITableMeta *table_meta,
            uint64_t br_index_in_trans);

      //////////////////////////// IBinlogRecord Interfaces //////////////////////////
      /* setter and getter */
      public:
        void setUserData(void *data) { BR_CALL(setUserData, data); }
        void *getUserData() { BR_CALL_WITH_RETVAL(getUserData); }

        // 必须: 获取记录类型的来源
        void setSrcType(int type) { BR_CALL(setSrcType, type); }
        int getSrcType() const { BR_CALL_WITH_RETVAL(getSrcType); }

        // 必须: 设置和获取记录获取方式
        void setSrcCategory(int cartegory) { BR_CALL(setSrcCategory, cartegory); }
        int getSrcCategory() const { BR_CALL_WITH_RETVAL(getSrcCategory); };

        // 必须: 设置和返回该日志记录创建时间
        void setTimestamp(long timestamp) { BR_CALL(setTimestamp, timestamp); }
        time_t getTimestamp() { BR_CALL_WITH_RETVAL(getTimestamp); }

        // 必须: 设置和获取该记录的操作类型insert/delete/update/replace/heartbeat...
        int setRecordType(int aType) { BR_CALL_WITH_RETVAL(setRecordType, aType); }
        int recordType() { BR_CALL_WITH_RETVAL(recordType); }

        // 必须: 获取ITableMeta中设置的该表的编码
        const char* recordEncoding() { BR_CALL_WITH_RETVAL(recordEncoding); }

        // 必须: 设置和获取记录来源的db名
        void setDbname(const char *db) { BR_CALL(setDbname, db); }
        const char *dbname() const { BR_CALL_WITH_RETVAL(dbname); }

        // 必须: 设置和获取记录来源的table名
        void setTbname(const char *table) { BR_CALL(setTbname, table); }
        const char* tbname() const { BR_CALL_WITH_RETVAL(tbname); }

        // creating必须, parse不需要: 获取记录对应表的meta信息
        void setTableMeta(ITableMeta *tblMeta) { BR_CALL(setTableMeta, tblMeta); }
        ITableMeta* getTableMeta() { BR_CALL_WITH_RETVAL(getTableMeta); }

        // 必须: 设置和获取记录的位点的字符串形式 %d@%d
        // file: 64位位点;
        // offset: BinlogRecord在事务内的编号
        void setCheckpoint(uint64_t file, uint64_t offset) { BR_CALL(setCheckpoint, file, offset); }
        const char* getCheckpoint() { BR_CALL_WITH_RETVAL(getCheckpoint);  }

        /**
         * 获取2个32位的位点描述, 根据mysql和ob的不同
         * mysql: getCheckpoint1获得的是文件编号, getCheckpoint2 记录文件内偏移
         * ob: getCheckpoint1: 64位位点, getCheckpoint2: binlog record在事务内的编号
         */
        uint64_t getCheckpoint1() { BR_CALL_WITH_RETVAL(getCheckpoint1); }
        uint64_t getCheckpoint2() { BR_CALL_WITH_RETVAL(getCheckpoint2); }

        uint64_t getFileNameOffset() { BR_CALL_WITH_RETVAL(getFileNameOffset); }
        uint64_t getFileOffset() { BR_CALL_WITH_RETVAL(getFileOffset); }

        // 必选: 设置该条记录是否是一组同类型记录的开始(精卫特需)
        void setFirstInLogevent(bool b) { BR_CALL(setFirstInLogevent, b); }
        bool firstInLogevent() { BR_CALL_WITH_RETVAL(firstInLogevent); }

        // 可选: 设置和获取记录的id
        void setId(uint64_t id) { BR_CALL(setId, id); }
        uint64_t id() { BR_CALL_WITH_RETVAL(id); }

        // 可选: 设置和获取来源目的库的ip-port
        void setInstance(const char* instance) { BR_CALL(setInstance, instance); }
        const char* instance() const { BR_CALL_WITH_RETVAL(instance); }

        // 可选: 设置获取额外信息，该信息会保存在pkValue的位置上
        void setExtraInfo(const char* info) { BR_CALL(setExtraInfo, info); }
        const char* extraInfo() const { BR_CALL_WITH_RETVAL(extraInfo); }

        // 可选: 添加和获取一系列的时间点用作性能分析
        const bool isTimemarked() const { BR_CALL_WITH_RETVAL(isTimemarked); }
        void setTimemarked(bool marked) { BR_CALL(setTimemarked, marked); }
        void addTimemark(long time) { BR_CALL(addTimemark, time); }
        std::vector<long>& getTimemark() { BR_CALL_WITH_RETVAL(getTimemark); }
        long* getTimemark(size_t &length) { BR_CALL_WITH_RETVAL(getTimemark, length); }
        void curveTimemark() { BR_CALL(curveTimemark); }

        /* column data */
      public:
        /**
         * 添加一个字段的值，这个值必须是一个解析好的字符串形式，同时
         * 遵循的规则是:
         * 1. insert类型没有旧值,只有新值,此时旧值可能用作标志位
         * 2. delete类型只有旧值,没有新值,此时新值可能用作标志为
         * 3. update/replace类型新旧值一一对应
         * 4. 字段的排列必须与ITableMeta中字段的排列一致且一一对应
         */
        int putOld(std::string *val) { BR_CALL_WITH_RETVAL(putOld, val); }
        int putNew(std::string *val) { BR_CALL_WITH_RETVAL(putNew, val); };

        // 清除所有更新前后还没有持久化的新旧字段
        void clearOld() { BR_CALL(clearOld); }
        void clearNew() { BR_CALL(clearNew); }

        /**
         * 清除所有内容以便重用, 不释放用户内存
         */
        void clear()
        {
          BR_CALL(clear);
          next_ = NULL;
          setTableMeta(NULL);
          setRecordType(EUNKNOWN);
          precise_timestamp_ = 0;
        }

        /**
         * 清除所有内容，包括用户内存
         */
        void clearWithUserMemory()
        {
          BR_CALL(clearWithUserMemory);
          next_ = NULL;
          precise_timestamp_ = 0;
        }

        // 获取持久化前后全部的新值
        const std::vector<std::string *>& newCols() { BR_CALL_WITH_RETVAL(newCols); }
        IStrArray* parsedNewCols() const { BR_CALL_WITH_RETVAL(parsedNewCols); }

        // 获取持久化前后全部的旧值
        const std::vector<std::string *>& oldCols() { BR_CALL_WITH_RETVAL(oldCols); }
        IStrArray* parsedOldCols() const { BR_CALL_WITH_RETVAL(parsedOldCols); }

        // 获取持久化后全部字段名和类型
        IStrArray* parsedColNames() const { BR_CALL_WITH_RETVAL(parsedColNames); }
        IStrArray* parsedColEncodings() const { BR_CALL_WITH_RETVAL(parsedColEncodings); }
        const uint8_t *parsedColTypes() const { BR_CALL_WITH_RETVAL(parsedColTypes); }

        // 获取ITableMeta中的主键/rowkey名的索引数组
        const int* pkKeys(size_t &size) const { BR_CALL_WITH_RETVAL(pkKeys, size); }

        // 获取ITableMeta中的unique key名的索引数组
        const int* ukKeys(size_t &size) const { BR_CALL_WITH_RETVAL(ukKeys, size); }

        void setRecordEncoding(const char* encoding) { BR_CALL(setRecordEncoding, encoding); }
      /* tostring and parse */
      public:
        /** 不提供parse方法，接入只会用到toString
         * 解析只读记录,从序列化的数据流中获取BinlogRecord对象
         * @param ptr   数据区
         * @param size  ptr所占字节数
         * @return 0:成功；<0: 失败
         */
        int parse(const void *ptr, size_t size) { BR_CALL_WITH_RETVAL(parse, ptr, size); }

        /**
         * 判断是否解析成功
         *
         * @return true 成功; false 失败
         */
        bool parsedOK() { BR_CALL_WITH_RETVAL(parsedOK); }

        /**
         * 只读数据区实际有效的字节数，parse()的时候允许size比比有效数据长
         * @return 字节数
         */
        size_t getRealSize() { BR_CALL_WITH_RETVAL(getRealSize); }

        /**
         * 序列化为字节流
         *
         * @return 字节流的首地址指针
         */
        const std::string* toString() { BR_CALL_WITH_RETVAL(toString); }
    }; // end class ObLogBinlogRecord
  } // end namespace liboblog
} // end namespace oceanbase

#endif // end OCEANBASE_LIBOBLOG_BINLOG_RECORD_H_
