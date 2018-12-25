// =====================================================================================
//
//       Filename:  db_tablet_info.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  2014年11月04日 08时59分21秒
//       Revision:  none
//       Compiler:  g++
//
//         Author:  liyongfeng
//   Organization:  ecnu DaSE
//
// =====================================================================================

#ifndef __DB_TABLET_INFO_H__
#define __DB_TABLET_INFO_H__

#include "common/utility.h"
#include "db_record.h"

namespace oceanbase {
	namespace api {
		using namespace oceanbase::common;
        //mod qianzm 20160718:b
        //const int kTabletMaxReplicaNr = 3;
        const int kTabletMaxReplicaNr = 6;
        //mod 20160718:e

		struct ObExportTabletSliceLocation {
			ObExportTabletSliceLocation()
				:ip_v4(0), cs_port(0), ms_port(0), tablet_version(0)
			{ }
			int32_t ip_v4;
			unsigned short cs_port;
			unsigned short ms_port;
			int64_t tablet_version;
		};

		class ObExportRefBuffer {
		public:
			ObExportRefBuffer() : ptr_(NULL), ref_(NULL) { }
			~ObExportRefBuffer() {
				unref();
			}

			ObExportRefBuffer(const ObExportRefBuffer &other) {
				if (other.ref_ != NULL) {
					__sync_add_and_fetch(&other.ref_->ref_count, 1);
					ptr_ = other.ptr_;
					ref_ = other.ref_;
				} else {
					ptr_ = NULL;
					ref_ = NULL;
				}
			}

			ObExportRefBuffer& operator=(const ObExportRefBuffer &other) {
				if (ptr_ != other.ptr_) {
					ObExportRefBuffer tmp(other);
					swap(tmp);
				}
				return *this;
			}

			inline char* alloc(int64_t sz) {
				ptr_ = (char *)ob_malloc(sz, ObModIds::TEST);
				if (ptr_ != NULL) {
					ref();
				}
				return ptr_;
			}

			inline void free(char* ptr) {
				ob_free(ptr);
			}

			void swap(ObExportRefBuffer &other) {
				std::swap(ptr_, other.ptr_);
				std::swap(ref_, other.ref_);
			}
		private:
			struct ObExportRefCtrBlock {
				ObExportRefCtrBlock() : ref_count(1) { }
				int64_t ref_count;
			};
			void ref() {
				if (ref_ != NULL) {
					__sync_add_and_fetch(&ref_->ref_count, 1);
				} else {
					ref_ = new(std::nothrow) ObExportRefCtrBlock();
				}
			}
			void unref() {
				if (ref_ != NULL) {
					int64_t ref_count = __sync_add_and_fetch(&ref_->ref_count, -1);
					if (ref_count <= 0) {
						delete ref_;
						free(ptr_);
						ref_ = NULL;
						ptr_ = NULL;
					}
				}
			}
		private:
			char *ptr_;
			ObExportRefCtrBlock *ref_;
		};

		class ObExportTabletInfo {
			public:
			ObExportTabletInfo();
			ObExportTabletInfo(const ObExportTabletInfo &src);

			virtual ~ObExportTabletInfo();

			ObExportTabletInfo &operator=(const ObExportTabletInfo &src);

			int build_tablet_info(const ObRowkey &start_key, DbRecord *recp);
			int build_tablet_info(const ObRowkey &start_key, const ObRowkey &end_key, DbRecord *recp);

			void dump_slice();

			int parse_from_record(DbRecord *recp);

			ObExportTabletSliceLocation slice_[kTabletMaxReplicaNr];

			int assign_start_key(const ObRowkey &start_key);
			int assign_end_key(const ObRowkey &end_key);
			const ObRowkey &get_start_key() const { return start_key_; }
			const ObRowkey &get_end_key() const { return end_key_; }
            //add qianzm [export_by_tablets] 20160415:b
			void set_tablet_index(int tablet_index){ tablet_index_ = tablet_index ; }
            int get_tablet_index(){ return tablet_index_; }
			//add 20160415:e

			int64_t occupy_size_;
			int64_t record_count_;
		private:
			int parse_one_cell(const common::ObCellInfo *cell);

			ObExportRefBuffer start_key_buffer_;
			ObRowkey start_key_;
			ObExportRefBuffer end_key_buffer_;
			ObRowkey end_key_;
            int tablet_index_;//add qianzm [export_by_tablets] 20160415
		};
	}
}

#endif
