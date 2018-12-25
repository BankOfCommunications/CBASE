// =====================================================================================
//
//       Filename:  db_tablet_info.cpp
//
//    Description:  
//
//        Version:  1.0
//        Created:  2014年11月11日 10时53分47秒
//       Revision:  none
//       Compiler:  g++
//
//         Author:  liyongfeng
//   Organization:  ecnu DaSE
//
// =====================================================================================

#include "db_tablet_info.h"

namespace oceanbase {
	namespace api {
		static const char *kCsPort = "port";
		static const char *kMsPort = "ms_port";
		static const char *kIpv4 = "ipv4";
		static const char *kTabletVersion = "tablet_version";

		static const char *kOccupySize = "occupy_size";
		static const char *kRecordCount = "record_count";

		ObExportTabletInfo::ObExportTabletInfo()
		{
			occupy_size_ = 0;
			record_count_ = 0;
		}

		ObExportTabletInfo::~ObExportTabletInfo()
		{

		}

		ObExportTabletInfo::ObExportTabletInfo(const ObExportTabletInfo &src)
		{
			for (int i = 0; i < kTabletMaxReplicaNr; ++i) {
				slice_[i] = src.slice_[i];
			}

			start_key_ = src.start_key_;
			start_key_buffer_ = src.start_key_buffer_;
			end_key_ = src.end_key_;
			end_key_buffer_ = src.end_key_buffer_;

			occupy_size_ = src.occupy_size_;
			record_count_ = src.record_count_;
			
            tablet_index_ = src.tablet_index_;//add qianzm [export_by_tablets] 20160415
		}

		ObExportTabletInfo& ObExportTabletInfo::operator=(const ObExportTabletInfo &src)
		{
			for (int i = 0; i < kTabletMaxReplicaNr; ++i) {
				slice_[i] = src.slice_[i];
			}

			start_key_ = src.start_key_;
			start_key_buffer_ = src.start_key_buffer_;
			end_key_ = src.end_key_;
			end_key_buffer_ = src.end_key_buffer_;

			occupy_size_ = src.occupy_size_;
			record_count_ = src.record_count_;

            tablet_index_= src.tablet_index_;//add qianzm [export_by_tablets] 20160415
			
			return *this;
		}

		int ObExportTabletInfo::build_tablet_info(const ObRowkey &start_key, DbRecord *recp)
		{
			int ret = OB_SUCCESS;
			ObRowkey end_key;
			ret = recp->get_rowkey(end_key);
			if (ret != OB_SUCCESS) {
				TBSYS_LOG(ERROR, "can't find rowkey from record, ret=[%d]", ret);
				return ret;
			}

			return build_tablet_info(start_key, end_key, recp);
		}

		int ObExportTabletInfo::build_tablet_info(const ObRowkey &start_key, const ObRowkey &end_key, DbRecord *recp)
		{
			int ret = OB_SUCCESS;
			if ((ret = assign_start_key(start_key)) != OB_SUCCESS) {
				TBSYS_LOG(ERROR, "assign tablet start rowkey failed, ret=[%d]", ret);
			} else if ((ret = assign_end_key(end_key)) != OB_SUCCESS) {
				TBSYS_LOG(ERROR, "assign tablet end rowkey failed, ret=[%d]", ret);
			} else if ((ret = parse_from_record(recp)) != OB_SUCCESS) {
				TBSYS_LOG(ERROR, "parse tablet location info failed, ret=[%d]", ret);
			}

			return ret;
		}

		int ObExportTabletInfo::parse_one_cell(const common::ObCellInfo *cell)
		{
			int ret = OB_SUCCESS;
			int64_t value;

			if ((ret = cell->value_.get_int(value)) != common::OB_SUCCESS) {
				TBSYS_LOG(ERROR, "ObCellInfo, get_int() failed, ret=[%d]", ret);
				return ret;
			}

			common::ObString column = cell->column_name_;

			if (!column.compare(kOccupySize)) {
				occupy_size_ = value;
			} else if (!column.compare(kRecordCount)) {
				record_count_ = value;
			} else {
				if (column.length() < 2) {//length must be higher than 2
					TBSYS_LOG(ERROR, "invalid column in cell, column name=[%s]", column.ptr());
					return common::OB_ERROR;
				}

				// column is "1_port", "1_ms_port", "1_ipv4", "1_tablet_version", "2_port", "2_ms_port", "2_ipv4", "2_tablet_version", ...
				int idx = *column.ptr() - '1';
				//mod by qianzm 20160223:b
				//if (idx < 0 || idx > 2) {//idx must be [0, 2]
				if (idx < 0 || idx > 5) {//idx must be [0, 5],原来的副本数为3,现在的副本数为6
				//mod e
					TBSYS_LOG(ERROR, "tablet slice location index is wrong, idx=[%d]", idx);
					return common::OB_ERROR;
				}

				//skip 'idx+1' + '_'
				ObString key;
				key.assign_ptr(column.ptr() + 2, column.length() - 2);//pick up keyword

				if (!key.compare(kCsPort)) {
					slice_[idx].cs_port = (short unsigned int)value;
				} else if (!key.compare(kMsPort)) {
					slice_[idx].ms_port = (short unsigned int)value;
				} else if (!key.compare(kIpv4)) {
					slice_[idx].ip_v4 = (int32_t)value;
				} else if (!key.compare(kTabletVersion)) {
					slice_[idx].tablet_version = value;
				} else {
					TBSYS_LOG(ERROR, "invalid column in cell, column name=[%s]", column.ptr());
					return common::OB_ERROR;
				}
			}

			return ret;
		}

		int ObExportTabletInfo::parse_from_record(DbRecord *recp)
		{
			int ret = common::OB_SUCCESS;

			DbRecord::Iterator iter = recp->begin();
			while (iter != recp->end()) {
				ret = parse_one_cell(&(iter->second));
				if (ret != OB_SUCCESS) {
					TBSYS_LOG(ERROR, "parse one cell failed, ret=[%d]", ret);
					break;
				}
  				iter++;
			}

			return ret;
		}

		int ObExportTabletInfo::assign_start_key(const ObRowkey &start_key)
		{
			int ret = start_key.deep_copy(start_key_, start_key_buffer_);
			if (ret != OB_SUCCESS) {
				TBSYS_LOG(ERROR, "deep copy start rowkey failed, ret=[%d]", ret);
			}

			return ret;
		}

		int ObExportTabletInfo::assign_end_key(const ObRowkey &end_key)
		{
			int ret = end_key.deep_copy(end_key_, end_key_buffer_);
			if (ret != OB_SUCCESS) {
				TBSYS_LOG(ERROR, "deep copy end rowkey failed, ret=[%d]", ret);
			}

			return ret;
		}

	}		 
}
