// =====================================================================================
//
//       Filename:  file_writer.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  2014年11月14日 09时40分53秒
//       Revision:  none
//       Compiler:  g++
//
//         Author:  liyongfeng
//   Organization:  ecnu DaSE
//
// =====================================================================================
#ifndef __FILE_WRITER_H__
#define __FILE_WRITER_H__

#include <stdio.h> 
#include <stdint.h>
#include <stdlib.h>
#include <tbsys.h>

extern int64_t kDefaultMaxFileSize;
extern int64_t kDefaultMaxRecordNum;

class FileWriter {
public:
	static const mode_t DATA_FILE_MODE = 0644;
	static const int64_t DEFAULT_MAX_FILE_SIZE = 1024 * 1024 * 1024;//1GB
	static const int64_t DEFAULT_MAX_REOCRD_NUM = 1000 * 100 * 100;//1000万
	
	public:
	FileWriter();
	~FileWriter();

	int init_file_name(const char *filename);
	void set_max_file_size(const int64_t &max_file_size);
	void set_max_record_count(const int64_t &max_record_num);
	int rotate_data_file(const char *filename, const char *fmt = NULL);
	int write_buffer(const char *buffer, const int64_t &length, const int64_t &num);
	int write_records(const char *buffer, const int64_t &length, const int64_t &num = 1);
	
	private:

	char *filename_;

	int32_t flag_;
	int64_t max_file_size_;
	int64_t cur_file_size_;
	int64_t cur_record_num_;
	int64_t max_record_num_;
	FILE *file_;
    tbsys::CThreadMutex file_lock_;
};
#endif
