#ifndef __OB_CHECK_H__
#define  __OB_CHECK_H__

#include "common/utility.h"
#include "common/ob_define.h"
#include "ob_check_param.h"
#include "tokenizer.h"
#include "file_reader.h"
#include "oceanbase_db.h"
#include <vector>
#include "file_appender.h"
#include "tbsys.h" //add by pangtz:20141205
using namespace oceanbase::common;
using namespace oceanbase::api;

//add by pangtz:20141203 [全局错误代码数组]
/*
 * error_arr[0] == true,表示 TABLE_NOT_EXIST（表不存在）
 * error_arr[1] == true,表示 DATAFLIE_NOT_EXIST（数据文件不存在）
 * error_arr[2] == true,表示 COLUMN_NUM_ERROR（字段数错误）
 * error_arr[3] == true,表示 DATA_ERROR（脏数据）
 * error_arr[4] == true,表示 NOT_NULL_CONSTRAIN_ERROR（非空约束问题）
 * error_arr[5] == true,表示 SYSTEM_ERROR（系统问题）
 * error_arr[6] == true,表示 LOCK_CONFLICT（锁冲突）
 * error_arr[7] == true,表示 VARCHAR_OVER_FLOW（字符串超长）
 * error_arr[8] == true,表示 OVER_MAX_ROWKEY_NUM（主键数超出最大限制）
 * error_arr[9] == true,表示 LINE_TOO_LONG（一行数据过长）
 * error_arr[10] == true,表示 RS_ERROR（rs异常）
 * error_arr[11] == true,表示 CONF_FILE_ERROR（配置文件错误）
 * error_arr[12] == true,表示 DELETE_ERROR（数据删除错误）
 * error_arr[13] == true,表示 TABLE_CONF_NOT_EXIST（配置文件不存在）
 * error_arr[14] == true,表示 MEMORY_ERROR_NOT_ENOUGH（服务器内存不足）
 *  
 */
extern bool error_arr[15];
//add e

//add by pangtz:20141203 [ob_import输屏日志类]
class ObImportLogInfo{
	public:
enum ImportErrorType{
	NORMAL = 0,
	TABLE_NOT_EXIST = 1,
	DATAFLIE_NOT_EXIST = 2,
	COLUMN_NUM_ERROR = 3,
	DATA_ERROR = 4,
	NOT_NULL_CONSTRAIN_ERROR = 5,
	SYSTEM_ERROR = 6,
	LOCK_CONFLICT = 7,
	VARCHAR_OVER_FLOW = 8,
	OVER_MAX_ROWKEY_NUM = 9,
	LINE_TOO_LONG = 10,
	RS_ERROR = 11,
	CONF_FILE_ERROR = 12,
	DELETE_ERROR = 13,
	TABLE_CONF_NOT_EXIST = 14,
	MEMORY_ERROR_NOT_ENOUGH = 15
};

		ObImportLogInfo();
		~ObImportLogInfo(){}
	
		inline void set_datafile_name(string name){datafile_name = name;}
		inline std::string get_datafile_name(){return datafile_name;} 
		inline void set_table_name(string name){table_name = name;}
		inline std::string get_table_name(){return table_name;} 
		inline void set_begin_time(){import_begin_time_ =  tbsys::CTimeUtil::getTime();}
		inline void set_end_time(){import_end_time_ = tbsys::CTimeUtil::getTime();}
		inline const char *get_begin_time()
		{
			std::string begin_time_str = const_cast<char *>(time2str(import_begin_time_));
			return begin_time_str.substr(0,19).data();
		}
		inline const char *get_end_time()
		{
			std::string end_time_str = const_cast<char *>(time2str(import_end_time_));
			return end_time_str.substr(0,19).data();
		}
		inline int64_t get_during_time(){return (import_end_time_ - import_begin_time_)/1000000;}
		inline void set_processed_lineno(int64_t proc_lineno){processed_lineno_ = proc_lineno;}
		inline void set_bad_lineno(int64_t bad_lineno){bad_lineno_ = bad_lineno;}
        inline int64_t get_processed_lineno() const {return processed_lineno_;}
		inline int64_t get_bad_lineno() const {return bad_lineno_;}
		inline int64_t get_succ_lineno() const {return (processed_lineno_ - bad_lineno_);}
		inline void set_final_ret(int ret)
		{
		    if(ret == 0 && bad_lineno_ == 0){
		       final_ret_ = 0;
		    }else if(ret == 0 && bad_lineno_ != 0){
		       final_ret_ = 2;
		    }else if(ret != 0){
				final_ret_ = 1;
			}

		}
		inline int get_final_ret(){return final_ret_;}

		void print_error_log();
	private:
		int64_t import_begin_time_;
		int64_t import_end_time_;
		int64_t during_time_;
		int64_t processed_lineno_;
		int64_t bad_lineno_;
		int64_t succ_lineno_;
		std::string datafile_name;
		std::string table_name;
		int final_ret_;
};
//add e

class TestRowBuilder;

class ObRowBuilder {
  public:
    friend class TestRowBuilder;
  public:
    enum RowkeyDataType{
      INT8,
      INT64,
      INT32,
      VARCHAR,
      DATETIME,
      INT16
    };

  static const int kMaxRowkeyDesc = OB_MAX_ROWKEY_COLUMN_NUMBER;
  public:
    ObRowBuilder(ObSchemaManagerV2 *schema, const TableParam &param);
    ~ObRowBuilder();
    
    int set_column_desc(const std::vector<ColumnDesc> &columns);

    //int set_rowkey_desc(const std::vector<RowkeyDesc> &rowkeys);

    bool check_valid();

    int build_tnx(RecordBlock &block, DbTranscation *tnx, AppendableFile *bad_file) const;
 //modify by pangtz:20141127 给函数添加token_nr参数
    int create_rowkey(ObRowkey &rowkey, TokenInfo *tokens,int token_nr) const;
	//modify:end
	//modify by pangtz:20141127 给函数添加token_nr参数
    int setup_content(RowMutator *mutator, TokenInfo *tokens, int token_nr) const;
	//modify:end
    int make_obobj(const ColumnInfo &column_info, ObObj &obj, TokenInfo *tokens) const;
	//add by pangtz:20141126 重载函数，使得可以对一个char*串做make_obobj
	int make_obobj(const ColumnInfo &column_info, ObObj &obj, const char *accdate) const;
	//add:end
    inline int get_lineno() const
    {
      return atomic_read(&lineno_);
    }

	//add by pangtz:20141203 [bad_lineno_设置和获取]
	inline void add_bad_lineno()
	{
		 atomic_add(1, &bad_lineno_);
	}
	inline int64_t get_bad_lineno()
	{
		return atomic_read(&bad_lineno_);
	}
	//add e
	

  private:
    ObSchemaManagerV2 *schema_;
    ColumnInfo columns_desc_[OB_MAX_COLUMN_NUMBER];
    int columns_desc_nr_;

    //RowkeyDesc rowkey_desc_[kMaxRowkeyDesc];
    int64_t rowkey_desc_nr_;
    int64_t rowkey_offset_[kMaxRowkeyDesc];
    mutable atomic_t lineno_;
	//add by pangtz:20141203 [记录bad记录数]
	mutable atomic_t bad_lineno_;
	//add e
    int64_t rowkey_max_size_;
    const TableParam &table_param_;
    AppendableFile *bad_file_;
    char *line_buffer_;
    static const int64_t LINE_BUFFER_SIZE = 1024 * 1024 * 2; //2M
	
};

#endif
